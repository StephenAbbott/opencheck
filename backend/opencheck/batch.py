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
) -> tuple[str, dict[str, Any]]:
    """Run one LEI through the single-lookup pipeline under the semaphore.

    Returns ``("row_done", row)`` or ``("row_failed", {...})``. Failures
    are *rows*, not exceptions: one unknown LEI must not abort the other
    nineteen, and a rate-limited anchor is reported so the reader knows
    that company was not checked.
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
            )
    return ("row_done", shape_batch_row(resp))


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
    sem = asyncio.Semaphore(concurrency or batch_concurrency())
    tasks = [
        asyncio.ensure_future(_one(lei, deepen_top, refresh, sem)) for lei in leis
    ]
    done = failed = 0
    try:
        for fut in asyncio.as_completed(tasks):
            event, payload = await fut
            if event == "row_done":
                done += 1
            else:
                failed += 1
            yield (event, payload)
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
    )
