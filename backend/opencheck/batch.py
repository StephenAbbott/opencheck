"""Batch / portfolio screening — a thin loop over the single-LEI pipeline.

Phase 164. Everything in OpenCheck has been one LEI at a time; the people
who most need it — a compliance officer with a counterparty list, a
journalist with a leak, an EITI secretariat with ninety licence-holders —
arrive with a list. This module is deliberately *not* a second pipeline:
each row is exactly ``routers.lookup._lookup_impl`` for that LEI (so it
shares the replay cache, the degradation channel, the GLEIF throttle and
every source adapter unchanged), and the batch is the loop, the cap and
the concurrency limit around it.

Two numbers govern the shape, and both are stated on the page rather than
hidden:

* **Cap of 20 rows** (``MAX_ROWS``). A cold anchor costs 4–6 GLEIF calls,
  so twenty rows ≈ 80–120 calls ≈ two minutes at the Phase 143 throttle
  (50/min). Fifty rows, the first draft's cap, would have been five.
* **Two pipelines in flight** (``OPENCHECK_BATCH_CONCURRENCY``). More than
  that and one batch monopolises the shared GLEIF budget that every human
  lookup queues behind.

Rows come back **as they finish, not in input order**, and a row that could
not be checked — GLEIF rate-limited past the Phase 162 max-wait, an unknown
LEI, an adapter fan-out that died — is reported as ``row_failed`` with
``degraded: true``, never silently dropped and never rendered as a clean
row. The verdict-sentence rule from Phase 132 holds for the table too:
OpenCheck does not grade companies, so rows are never ranked by severity.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import re
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from fastapi import HTTPException

from . import identifiers
from .config import get_settings
from .gleif_throttle import GleifRateLimitedError

#: Hard cap on rows per batch. Stated on the page and in the 422 detail.
MAX_ROWS = 20

#: Tolerant paste: LEIs separated by newlines, commas, semicolons, spaces
#: or tabs, so a column copied out of a spreadsheet just works.
_SPLIT = re.compile(r"[\s,;]+")


@dataclass(frozen=True)
class RejectedToken:
    """A pasted token that will not be screened, and why — shown in place."""

    token: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {"token": self.token, "reason": self.reason}


@dataclass
class ParsedList:
    """Outcome of :func:`parse_lei_list`."""

    leis: list[str] = field(default_factory=list)
    rejected: list[RejectedToken] = field(default_factory=list)
    #: Valid, unique LEIs beyond the cap that were NOT taken — so the UI can
    #: say "20 of 27 taken" instead of silently truncating.
    overflow: int = 0
    cap: int = MAX_ROWS

    @property
    def accepted(self) -> int:
        return len(self.leis)


def parse_lei_list(text: str, *, cap: int = MAX_ROWS) -> ParsedList:
    """Split pasted text into checksum-valid, de-duplicated LEIs.

    Every token is classified rather than the paste being refused whole:
    a wrong-length token, a shape miss, a check-digit failure and a
    duplicate each land in ``rejected`` with a reason a reader can act on.
    Order is preserved so the table can mirror the paste.
    """
    parsed = ParsedList(cap=cap)
    seen: set[str] = set()
    for raw in _SPLIT.split(text or ""):
        if not raw:
            continue
        lei = identifiers.normalise_lei(raw)
        if len(lei) != 20:
            parsed.rejected.append(
                RejectedToken(raw, f"{len(lei)} characters — an LEI has 20")
            )
            continue
        if not identifiers.LEI_STRICT_SHAPE.match(lei):
            parsed.rejected.append(
                RejectedToken(raw, "not an LEI: 18 letters or digits then two digits")
            )
            continue
        if identifiers.lei_check_digit_error(lei):
            parsed.rejected.append(
                RejectedToken(raw, "check digits do not match — a typo?")
            )
            continue
        if lei in seen:
            parsed.rejected.append(RejectedToken(raw, "duplicate"))
            continue
        seen.add(lei)
        if len(parsed.leis) >= cap:
            parsed.overflow += 1
            continue
        parsed.leis.append(lei)
    return parsed


def batch_concurrency() -> int:
    """Pipelines in flight per batch (``OPENCHECK_BATCH_CONCURRENCY``)."""
    return max(1, int(get_settings().batch_concurrency))


async def _one(
    lei: str, deepen_top: int, refresh: bool, sem: asyncio.Semaphore
) -> tuple[str, dict[str, Any], Any]:
    """Run one LEI through the single-lookup pipeline under the semaphore.

    Returns ``("row_done", row, response)`` or ``("row_failed", {...},
    None)``. Failures are *rows*, not exceptions: one unknown LEI must not
    abort the other nineteen, and a rate-limited anchor is reported so the
    reader knows that company was not checked. The full ``LookupResponse``
    rides along for callers that need more than the row — the combined
    export (Phase 167) needs every row's BODS statements.
    """
    from .mcp.shaping import shape_batch_row
    from .routers.lookup import _lookup_impl

    async with sem:
        try:
            resp = await _lookup_impl(lei, deepen_top=deepen_top, refresh=refresh)
        except HTTPException as exc:
            return (
                "row_failed",
                {
                    "lei": lei,
                    "status": exc.status_code,
                    "reason": str(exc.detail),
                    # 503 is the Phase 143/162 throttle refusal — momentary;
                    # everything else (404 unknown, 400 shape) is durable.
                    "retryable": exc.status_code == 503,
                    "degraded": True,
                },
                None,
            )
        except GleifRateLimitedError as exc:  # pragma: no cover — _lookup_impl maps it
            return (
                "row_failed",
                {
                    "lei": lei,
                    "status": 503,
                    "reason": str(exc) or "GLEIF is rate-limiting OpenCheck",
                    "retryable": True,
                    "degraded": True,
                },
                None,
            )
        except Exception as exc:  # noqa: BLE001 — a row, never a batch abort
            return (
                "row_failed",
                {
                    "lei": lei,
                    "status": 500,
                    "reason": f"{type(exc).__name__}: {exc}",
                    "retryable": False,
                    "degraded": True,
                },
                None,
            )
    return ("row_done", shape_batch_row(resp), resp)


async def iter_batch(
    leis: list[str],
    *,
    deepen_top: int = 5,
    refresh: bool = False,
    concurrency: int | None = None,
) -> AsyncIterator[tuple[str, dict[str, Any], Any]]:
    """Screen ``leis`` and yield ``(event, payload, response)`` per row.

    The engine under :func:`run_batch` (which drops the response) and the
    combined export (which keeps it). ``response`` is the ``LookupResponse``
    for ``row_done`` and ``None`` for ``row_failed`` and ``batch_done``.
    """
    sem = asyncio.Semaphore(concurrency or batch_concurrency())
    tasks = [
        asyncio.ensure_future(_one(lei, deepen_top, refresh, sem)) for lei in leis
    ]
    done = failed = 0
    try:
        for fut in asyncio.as_completed(tasks):
            event, payload, resp = await fut
            if event == "row_done":
                done += 1
            else:
                failed += 1
            yield (event, payload, resp)
    finally:
        # A client that disconnects mid-batch must not leave nineteen
        # pipelines running against the shared upstream budgets.
        for t in tasks:
            if not t.done():
                t.cancel()
    yield (
        "batch_done",
        {
            "requested": len(leis),
            "done": done,
            "failed": failed,
            # Rows that completed but with a screening check that did not
            # fully run are counted by the caller from each row's
            # ``degraded`` flag; ``failed`` rows are degraded by definition.
        },
        None,
    )


async def run_batch(
    leis: list[str],
    *,
    deepen_top: int = 5,
    refresh: bool = False,
    concurrency: int | None = None,
) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    """Screen ``leis`` and yield ``(event, payload)`` as each row finishes.

    Events: ``row_done`` / ``row_failed`` per LEI (completion order), then
    one ``batch_done`` with the totals. Callers parse and cap the list
    first (:func:`parse_lei_list`) — this function trusts its input.
    """
    async for event, payload, _resp in iter_batch(
        leis, deepen_top=deepen_top, refresh=refresh, concurrency=concurrency
    ):
        yield (event, payload)


# ---------------------------------------------------------------------------
# Combined export (Phase 167)
# ---------------------------------------------------------------------------

#: The table as it is written to ``rows.csv`` — the same columns, in the
#: same order, as the page's client-side CSV (``frontend/src/lib/batch.ts``),
#: so a reader who has both files can line them up.
CSV_COLUMNS = [
    "lei",
    "legal_name",
    "jurisdiction",
    "register_status",
    "verdict",
    "risk_count",
    "risk_codes",
    "context_count",
    "context_codes",
    "sources_applicable",
    "sources_answered",
    "degraded",
    "degraded_sources",
    "state",
    "reason",
    "report_url",
]


@dataclass
class BatchResult:
    """Everything the combined export needs, in paste order."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)
    #: Every row's BODS statements, de-duplicated by ``statementId`` across
    #: rows: two companies in one group share the parent's entity statement
    #: (both derive it from the same LEI), and it must appear once.
    statements: list[dict[str, Any]] = field(default_factory=list)
    #: Registered source ids that returned data for at least one row.
    contributing_ids: list[str] = field(default_factory=list)
    #: Per-bundle licence notices adapters attached, de-duplicated.
    license_notices: list[dict[str, Any]] = field(default_factory=list)
    #: ``statementId``s that appeared in more than one row's bundle.
    duplicate_statement_count: int = 0

    @property
    def totals(self) -> dict[str, int]:
        return {
            "requested": len(self.rows) + len(self.failed),
            "done": len(self.rows),
            "failed": len(self.failed),
            "degraded": sum(1 for r in self.rows if r.get("degraded")) + len(self.failed),
        }


async def collect_batch(
    leis: list[str], *, deepen_top: int = 5, refresh: bool = False
) -> BatchResult:
    """Run the batch to completion and merge every row's bundle.

    Rows come back in paste order regardless of completion order. A
    failed row contributes no statements and no sources — it is named in
    ``failed`` (and in ``rows.csv``) so the bundle never reads as a
    complete answer for a list it only partly covered.
    """
    order = {lei: i for i, lei in enumerate(leis)}
    rows: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    responses: dict[str, Any] = {}
    async for event, payload, resp in iter_batch(
        leis, deepen_top=deepen_top, refresh=refresh
    ):
        if event == "row_done":
            rows.append(payload)
            responses[payload["lei"]] = resp
        elif event == "row_failed":
            failed.append(payload)
    rows.sort(key=lambda r: order.get(r["lei"], len(order)))
    failed.sort(key=lambda r: order.get(r["lei"], len(order)))

    result = BatchResult(rows=rows, failed=failed)
    seen: set[str] = set()
    sources: set[str] = set()
    notices: dict[str, dict[str, Any]] = {}
    for row in rows:
        resp = responses[row["lei"]]
        for stmt in resp.bods or []:
            sid = stmt.get("statementId")
            if sid and sid in seen:
                result.duplicate_statement_count += 1
                continue
            if sid:
                seen.add(sid)
            result.statements.append(stmt)
        for h in resp.hits or []:
            if not h.is_stub:
                sources.add(h.source_id)
        for n in resp.license_notices or []:
            key = json.dumps(n, sort_keys=True, default=str)
            notices.setdefault(key, n)
    result.contributing_ids = sorted(sources)
    result.license_notices = list(notices.values())
    return result


def _csv_row_done(row: dict[str, Any], origin: str) -> list[Any]:
    status = row.get("register_status") or {}
    cov = row.get("coverage") or {}
    return [
        row["lei"],
        row.get("legal_name") or "",
        row.get("jurisdiction") or "",
        status.get("liveness") or "",
        row.get("verdict") or "",
        row.get("risk_count", 0),
        " ".join(row.get("risk_codes") or []),
        row.get("context_count", 0),
        " ".join(row.get("context_codes") or []),
        cov.get("applicable", ""),
        cov.get("answered", ""),
        "true" if row.get("degraded") else "false",
        " ".join(row.get("degraded_sources") or []),
        "degraded" if row.get("degraded") else "done",
        "",
        f"{origin}{row.get('report_url', '')}",
    ]


def _csv_row_failed(row: dict[str, Any], origin: str) -> list[Any]:
    return [
        row["lei"], "", "", "", "", "", "", "", "", "", "", "true", "",
        "not checked", row.get("reason") or "", f"{origin}/?lei={row['lei']}",
    ]


def rows_csv(result: BatchResult, *, origin: str = "https://opencheck.world") -> str:
    """``rows.csv``: every screened LEI, one line each, failed rows included
    with their reason — the same columns the page's CSV button writes."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(CSV_COLUMNS)
    for r in result.rows:
        w.writerow(_csv_row_done(r, origin))
    for r in result.failed:
        w.writerow(_csv_row_failed(r, origin))
    return buf.getvalue()
