"""pipelinestats — how the lookup gate is used, and how long runs take (Phase 238).

Motivation (23 Sept 2026): two large companies opened together in two tabs,
and one was refused with "OpenCheck is running as many checks as it can at
once". For a run to wait the full ``lookup_queue_wait_s`` without a slot,
every one of the ``lookup_max_concurrent`` slots had to be held for the
whole wait — and nothing recorded who held them, how often the gate refused
anyone, or how long a run takes. The investigation (the Notion ticket, and
``claude/pipeline-gate-investigation-2026-09-24.md`` in the project) could
rule the watchlist worker and FullCheck out from other counters, but could
not name what was running.

What is recorded, all from ``routers.lookup``'s gate and flight:

* **Who starts runs.** Every fresh run is attributed to a *caller kind* —
  a closed vocabulary derived from the request path by
  :func:`caller_kind_for_path` in ``lookup_budget.ClientScopeMiddleware``
  (``stream``, ``api``, ``batch``, ``expand``, ``export``, ``narrative``,
  ``watch``, ``mcp``, ``other``) or ``server`` for work with no request at
  all (the watchlist worker). Counted as started / queued / refused, and as
  **slot-seconds** — the time each kind held a slot — which is what answers
  "who was occupying the gate".
* **The gate.** Current and peak slots in use and queue length, and the
  queue waits of admitted and refused runs, as percentiles.
* **Run time.** Wall-clock from admission to the last event, as
  percentiles, and per source the time into the run at which it answered
  (``source_completed`` / ``source_error``) plus how often it timed out.

**Privacy.** The keys are the caller-kind vocabulary above and adapter ids.
The recorders take a kind string, a source id and numbers — never an LEI,
an IP or a path — so nothing identifying can reach them.
``test_pipelinestats.py`` pins that. Samples are kept in bounded windows
(the last :data:`_WINDOW` values), so a percentile describes recent traffic
and memory stays flat.

Same contract as :mod:`opencheck.signalstats`, where the snapshot is served
(the ``pipelines`` key of ``/signalstats``): in-process, reset on deploy,
and every entry point fails soft — instrumentation must never break a run.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import Counter, deque
from typing import Any

log = logging.getLogger("opencheck.pipelinestats")

#: The caller kinds. Anything else is folded into ``other`` so the counters
#: cannot grow keys from input.
CALLER_KINDS = (
    "stream",
    "api",
    "batch",
    "expand",
    "export",
    "narrative",
    "watch",
    "mcp",
    "server",
    "other",
)

#: Path prefix → caller kind. Matched on a segment boundary, longest first,
#: so ``/lookup-stream`` is never read as ``/lookup``.
_PATH_KINDS = (
    ("/lookup-stream", "stream"),
    ("/lookup", "api"),
    ("/batch-stream", "batch"),
    ("/batch-export", "batch"),
    ("/expand-layer", "expand"),
    ("/expand", "expand"),
    ("/export-network", "export"),
    ("/export", "export"),
    ("/narrative", "narrative"),
    ("/watch", "watch"),
    ("/mcp", "mcp"),
)

#: How many recent samples a percentile is taken over.
_WINDOW = 500
#: Adapter ids tracked per source, at most (a registry is ~50).
_MAX_SOURCES = 200


def caller_kind_for_path(path: str | None) -> str:
    """The caller kind for an HTTP request path. Only the path's first
    segment is read, and the answer is always one of :data:`CALLER_KINDS`."""
    if not path:
        return "other"
    for prefix, kind in _PATH_KINDS:
        if path == prefix or path.startswith(prefix + "/"):
            return kind
    return "other"


def _kind(kind: str | None) -> str:
    if kind is None:
        return "server"
    return kind if kind in CALLER_KINDS else "other"


def _percentiles(values: deque[float] | list[float]) -> dict[str, Any]:
    data = sorted(values)
    if not data:
        return {"n": 0, "p50": None, "p90": None, "max": None}

    def pick(q: float) -> float:
        # Nearest-rank: the smallest value with at least q of the data at or
        # below it. No interpolation, so every figure is a real observation.
        idx = max(0, min(len(data) - 1, math.ceil(q * len(data)) - 1))
        return round(data[idx], 1)

    return {"n": len(data), "p50": pick(0.5), "p90": pick(0.9), "max": round(data[-1], 1)}


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started = time.time()
        self.started_by: Counter[str] = Counter()
        self.queued_by: Counter[str] = Counter()
        self.refused_by: Counter[str] = Counter()
        self.slot_seconds_by: Counter[str] = Counter()
        self.active = 0
        self.queued = 0
        self.peak_active = 0
        self.peak_queued = 0
        self.wait_admitted: deque[float] = deque(maxlen=_WINDOW)
        self.wait_refused: deque[float] = deque(maxlen=_WINDOW)
        self.run_seconds: deque[float] = deque(maxlen=_WINDOW)
        self.runs_completed = 0
        self.runs_errored = 0
        self.source_seconds: dict[str, deque[float]] = {}
        self.source_timeouts: Counter[str] = Counter()
        self.sources_truncated = False


_state = _State()


def reset() -> None:
    """Clear everything. Tests only."""
    global _state
    _state = _State()


def record_queued(kind: str | None, queued_now: int) -> None:
    """A run found no free slot and joined the queue (``queued_now``
    includes it)."""
    try:
        with _state.lock:
            _state.queued_by[_kind(kind)] += 1
            _state.queued = queued_now
            _state.peak_queued = max(_state.peak_queued, queued_now)
    except Exception as exc:  # noqa: BLE001
        log.debug("pipelinestats.record_queued failed, ignoring: %s", exc)


def record_admitted(kind: str | None, waited_s: float, active_now: int, queued_now: int) -> None:
    """A run got a slot after ``waited_s`` (0 when one was free)."""
    try:
        with _state.lock:
            _state.started_by[_kind(kind)] += 1
            _state.wait_admitted.append(max(0.0, waited_s))
            _state.active = active_now
            _state.queued = queued_now
            _state.peak_active = max(_state.peak_active, active_now)
    except Exception as exc:  # noqa: BLE001
        log.debug("pipelinestats.record_admitted failed, ignoring: %s", exc)


def record_refused(kind: str | None, waited_s: float, queued_now: int) -> None:
    """A run waited ``waited_s`` and was refused a slot (the 503)."""
    try:
        with _state.lock:
            _state.refused_by[_kind(kind)] += 1
            _state.wait_refused.append(max(0.0, waited_s))
            _state.queued = queued_now
    except Exception as exc:  # noqa: BLE001
        log.debug("pipelinestats.record_refused failed, ignoring: %s", exc)


def record_released(kind: str | None, held_s: float, active_now: int, queued_now: int) -> None:
    """A run gave its slot back after holding it ``held_s``."""
    try:
        with _state.lock:
            _state.slot_seconds_by[_kind(kind)] += max(0.0, held_s)
            _state.active = active_now
            _state.queued = queued_now
    except Exception as exc:  # noqa: BLE001
        log.debug("pipelinestats.record_released failed, ignoring: %s", exc)


def record_source(source_id: Any, seconds_into_run: float, *, timed_out: bool = False) -> None:
    """A source answered (or failed) ``seconds_into_run`` after admission."""
    try:
        if not isinstance(source_id, str) or not source_id:
            return
        with _state.lock:
            window = _state.source_seconds.get(source_id)
            if window is None:
                if len(_state.source_seconds) >= _MAX_SOURCES:
                    _state.sources_truncated = True
                    return
                window = _state.source_seconds[source_id] = deque(maxlen=_WINDOW)
            window.append(max(0.0, seconds_into_run))
            if timed_out:
                _state.source_timeouts[source_id] += 1
    except Exception as exc:  # noqa: BLE001
        log.debug("pipelinestats.record_source failed, ignoring: %s", exc)


def record_run(seconds: float, *, completed: bool) -> None:
    """A run ended ``seconds`` after admission — with ``done`` or not."""
    try:
        with _state.lock:
            if completed:
                _state.runs_completed += 1
                _state.run_seconds.append(max(0.0, seconds))
            else:
                _state.runs_errored += 1
    except Exception as exc:  # noqa: BLE001
        log.debug("pipelinestats.record_run failed, ignoring: %s", exc)


#: How many sources the ``slowest_sources`` list names.
_SLOWEST = 8


def stats() -> dict[str, Any]:
    """The ``pipelines`` section of ``/signalstats``."""
    with _state.lock:
        by_kind = {
            kind: {
                "started": _state.started_by.get(kind, 0),
                "queued": _state.queued_by.get(kind, 0),
                "refused": _state.refused_by.get(kind, 0),
                "slot_seconds": round(_state.slot_seconds_by.get(kind, 0.0), 1),
            }
            for kind in CALLER_KINDS
            if _state.started_by.get(kind)
            or _state.queued_by.get(kind)
            or _state.refused_by.get(kind)
        }
        sources = {
            sid: {**_percentiles(window), "timeouts": _state.source_timeouts.get(sid, 0)}
            for sid, window in _state.source_seconds.items()
        }
        out = {
            "started": sum(_state.started_by.values()),
            "refused": sum(_state.refused_by.values()),
            "queued": sum(_state.queued_by.values()),
            "active_now": _state.active,
            "queued_now": _state.queued,
            "peak_active": _state.peak_active,
            "peak_queued": _state.peak_queued,
            "by_caller": by_kind,
            "queue_wait_s": {
                "admitted": _percentiles(_state.wait_admitted),
                "refused": _percentiles(_state.wait_refused),
            },
            "runs_completed": _state.runs_completed,
            "runs_errored": _state.runs_errored,
            "run_seconds": _percentiles(_state.run_seconds),
            "sources_truncated": _state.sources_truncated,
        }
    slowest = sorted(
        (item for item in sources.items() if item[1]["p90"] is not None),
        key=lambda item: item[1]["p90"],
        reverse=True,
    )[:_SLOWEST]
    out["slowest_sources"] = [{"source_id": sid, **row} for sid, row in slowest]
    out["sources"] = sources
    return out
