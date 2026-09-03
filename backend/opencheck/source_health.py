"""Source health as the sources page reads it (Phase 161).

The weekly sweep (``scripts/source_health.py``, Mondays 07:30 UTC) is the only
thing in OpenCheck that knows whether a source is *answering* — as opposed to
being *configured*, which is all ``/sources`` could say ("live ready" means a
key is set; a source can be live-ready and refuse every request for a month).
Until this phase the sweep's report lived in a 90-day CI artifact and a
rolling GitHub issue, which is to say nowhere a reader of opencheck.world
would find it.

The workflow now uploads ``source-health.json`` and a rolling
``source-health-history.json`` to the ``source-health-latest`` GitHub release
(the entity-pages arrangement: a URL an ephemeral-filesystem host can read
without a rebuild), and this module reads them back for ``GET /source-health``.

Three rules:

- **The report is read, never re-derived.** The sweep asserts provenance,
  shape and statement counts against one known-good subject per source, with
  cache reads disabled and a single retry; nothing at request time could
  reproduce that, so nothing here probes a source. The page shows the last
  sweep's verdict and says when it was reached.
- **What is served is a shaping of the report, not the report.** Statuses,
  reasons, known gaps, liveness, latency and statement totals; not observed
  field names or result sizes, which are for the engineer reading the
  artifact. The sweep already redacts credentials from reasons.
- **A missing report is reported as missing.** ``available: false`` with a
  reason, never an empty-but-healthy table: the page then shows the catalogue
  exactly as it did before this phase. A report that cannot be *refreshed* is
  served stale and says so — last Monday's verdict is more useful than none,
  and the page prints its date.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

STATUSES = ("ok", "degraded", "fail", "skipped")
HISTORY_FILENAME = "source-health-history.json"
HISTORY_SHOWN = 8
"""How many past sweeps a source's history carries — two months of Mondays."""

REFRESH_AFTER_S = 3600.0
"""The sweep runs weekly; re-reading the asset hourly is already generous."""

FETCH_TIMEOUT_S = 20.0


@dataclass
class _Cached:
    fetched_at: float
    payload: dict[str, Any]


_cache: _Cached | None = None
_lock: asyncio.Lock | None = None


def reset_for_tests() -> None:
    global _cache, _lock
    _cache = None
    _lock = None


def _history_location(report_location: str) -> str:
    """The history file sits beside the report, under a fixed name."""
    head, sep, _ = report_location.rpartition("/")
    return f"{head}{sep}{HISTORY_FILENAME}" if sep else HISTORY_FILENAME


def _statement_total(counts: dict[str, Any] | None) -> int | None:
    """Entity + person + relationship statements, which is what the sweep
    counted; the ``interest:*`` keys are a histogram *within* relationships
    and would double-count."""
    if not counts:
        return None
    total = 0
    for key in ("entity", "person", "relationship"):
        value = counts.get(key)
        if isinstance(value, int):
            total += value
    return total


def shape(report: dict[str, Any] | None, history: dict[str, Any] | None) -> dict[str, Any]:
    """The ``/source-health`` payload from the sweep's own report.

    ``history`` is the rolling file the workflow maintains —
    ``{"runs": [{"generated_at": ..., "statuses": {source_id: status}}]}``,
    oldest first — and is optional: a source's ``history`` is then just the
    current status.
    """
    if not report or not isinstance(report.get("sources"), dict):
        return {"available": False, "reason": "no sweep report"}

    runs = [r for r in ((history or {}).get("runs") or []) if isinstance(r, dict)]
    generated_at = report.get("generated_at")
    # The current run belongs at the end of the history whether or not the
    # workflow managed to append it before uploading.
    if not runs or runs[-1].get("generated_at") != generated_at:
        runs.append(
            {
                "generated_at": generated_at,
                "statuses": {sid: row.get("status") for sid, row in report["sources"].items()},
            }
        )
    runs = runs[-HISTORY_SHOWN:]

    collapses = report.get("statement_collapses") or {}
    sources: dict[str, Any] = {}
    for source_id, row in report["sources"].items():
        if not isinstance(row, dict):
            continue
        status = row.get("status")
        if status not in STATUSES:
            continue
        sources[source_id] = {
            "status": status,
            "reason": row.get("reason") or "",
            "known_gap": row.get("known_gap") or "",
            "liveness": row.get("liveness"),
            "retrieved_at": row.get("retrieved_at"),
            "latency_ms": row.get("latency_ms"),
            "attempts": row.get("attempts") or 0,
            "statement_total": _statement_total(row.get("statement_counts")),
            "statement_collapse": collapses.get(source_id),
            "history": [
                (run.get("statuses") or {}).get(source_id)
                for run in runs
                if source_id in (run.get("statuses") or {})
            ],
        }

    counts = report.get("counts") or {}
    return {
        "available": True,
        "generated_at": generated_at,
        "compared_against": report.get("compared_against"),
        "registry_size": report.get("registry_size"),
        "probed": report.get("probed"),
        "counts": {status: int(counts.get(status) or 0) for status in STATUSES},
        "sweeps": [run.get("generated_at") for run in runs],
        "sources": sources,
    }


async def _read(location: str) -> dict[str, Any] | None:
    """A JSON document from a local path or a URL; ``None`` when absent."""
    if location.startswith(("http://", "https://")):
        import httpx

        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT_S, follow_redirects=True) as client:
            resp = await client.get(location, headers={"Accept": "application/json"})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    path = Path(location)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


async def _fetch() -> dict[str, Any]:
    from .config import get_settings

    settings = get_settings()
    location = settings.source_health_file or settings.source_health_url
    if not location:
        return {"available": False, "reason": "source health not configured"}
    report = await _read(location)
    if report is None:
        return {"available": False, "reason": "no sweep has published a report yet"}
    history: dict[str, Any] | None = None
    try:
        history = await _read(_history_location(location))
    except Exception as exc:  # noqa: BLE001 — history is decoration on the report
        log.info("source-health history unavailable: %s", exc)
    return shape(report, history)


async def load(*, now: float | None = None) -> dict[str, Any]:
    """The shaped payload, refreshed at most hourly and served stale on error."""
    global _cache, _lock
    if _lock is None:
        _lock = asyncio.Lock()
    clock = time.monotonic() if now is None else now
    if _cache is not None and clock - _cache.fetched_at < REFRESH_AFTER_S:
        return _cache.payload
    async with _lock:
        if _cache is not None and clock - _cache.fetched_at < REFRESH_AFTER_S:
            return _cache.payload
        try:
            payload = await _fetch()
        except Exception as exc:  # noqa: BLE001 — a fetch failure must not 500 the sources page
            log.warning("source-health refresh failed: %s", exc)
            if _cache is not None:
                stale = dict(_cache.payload, stale=True)
                _cache = _Cached(clock, stale)
                return stale
            payload = {"available": False, "reason": f"could not read the sweep report: {type(exc).__name__}"}
        _cache = _Cached(clock, payload)
        return payload
