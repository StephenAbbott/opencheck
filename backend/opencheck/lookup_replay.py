"""How a lookup run is admitted, shared, cached and folded (Phases 47, 234, 238).

Split out of ``routers/lookup.py`` in Phase 246, unchanged except for the two
names that still live there: a flight drives ``lookup._lookup_pipeline``
through the module, so a test that replaces the pipeline replaces it here too,
and ``fold_lookup_events`` imports ``LookupResponse`` when it runs.
``routers/lookup.py`` re-exports every name defined here — the replay cache
and the in-flight table are the same objects under both addresses.

* **The replay cache** — completed runs, kept 15 minutes, replayed by both
  lookup endpoints, saved reports and share cards.
* **The gate and the flights** — one run per ``(lei, deepen_top)`` at a time,
  at most ``lookup_max_concurrent`` at once, first come first served.
* **The fold** — ``fold_lookup_events`` turns a run's events into a
  ``LookupResponse``; the live ``/lookup``, a saved report and its exports all
  go through it, so they cannot drift.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, NamedTuple

from fastapi import HTTPException

from . import (
    listing as _listing,
    lookup_budget as _lookup_budget,
    pipelinestats as _pipelinestats,
)
from .bods import unique_statements
from .config import get_settings
from .knowability import statement_for as knowability_statement_for
from .sources import SearchKind

if TYPE_CHECKING:
    from .routers.lookup import LookupResponse

#: One event of a lookup run: ``(name, payload)``.
LookupEvent = tuple[str, Any]


# --- replay cache --------------------------------------------------------------
#
# Completed lookup runs are kept in memory for a short window so a page
# refresh, a shared URL, or an SSE reconnect replays instantly instead of
# re-querying every source. Only runs that reached the "done" event are
# cached; per-source retries and ?refresh=true invalidate/bypass.
#
# Replays are never allowed to masquerade as live runs: a replayed stream is
# prefixed with a "replayed" event carrying the wall-clock completion time of
# the original run, and the sync /lookup response mirrors it as
# ``replayed`` / ``fetched_at`` so the UI can badge the result and offer a
# fresh check.


class _ReplayEntry(NamedTuple):
    stored: float  # monotonic clock, for the TTL check
    fetched_at: str  # wall-clock UTC ISO 8601 completion time, for display
    events: list[LookupEvent]


_REPLAY_TTL_SECONDS = 15 * 60.0
_REPLAY_MAX_ENTRIES = 64
_REPLAY_CACHE: dict[str, _ReplayEntry] = {}


def _invalidate_replay(lei: str) -> None:
    prefix = f"{lei.strip().upper()}:"
    for key in [k for k in _REPLAY_CACHE if k.startswith(prefix)]:
        _REPLAY_CACHE.pop(key, None)


# --- Phase 234: one run per LEI at a time, a bounded number at once ----------
#
# A fresh run is a *flight*: a task, detached from whoever asked for it, that
# drives ``_lookup_pipeline`` and buffers its events. The first caller starts
# it (and is charged for it — ``lookup_budget.charge``); anyone asking for the
# same ``(lei, deepen_top)`` while it is in the air follows the same buffer
# from the start, free. Before this, two tabs, a watchlist baseline and a
# FullCheck hop on one LEI ran the forty-source fan-out four times.
#
# Flights also pass through one process-wide gate (``lookup_max_concurrent``)
# so a burst queues instead of stacking pipelines until Render's memory limit
# does the queueing for us. A run that cannot get a slot within
# ``lookup_queue_wait_s`` ends in a 503 error event and its charge is
# refunded.
#
# Phase 238: the queue is first come, first served, a run started from the
# interactive stream waits ``lookup_stream_queue_wait_s`` (5 min) rather than
# 60 s, and while it waits the flight carries ``queued`` events with its place
# in the queue — the loading grid says "waiting for a free slot" instead of
# failing. Those events are about the wait, not the run, so they never reach
# the replay cache or a saved report (``_TRANSIENT_EVENTS``). Every admission,
# refusal, wait and run time is counted in ``pipelinestats`` and served as the
# ``pipelines`` section of ``/signalstats``.
#
# A follower that goes away (an SSE tab closed) does not cancel the flight:
# the run completes and lands in the replay cache, which is where the next
# visitor finds it. The budget and the gate bound how many such runs exist.

#: ``deepen_top`` is accepted in this range everywhere. Every value is its own
#: replay-cache key, so an unclamped value from an in-process caller (the MCP
#: tools took any int) forced a fresh run per value and evicted other readers'
#: runs from the 64-entry cache.
DEEPEN_TOP_MAX = 10


def clamp_deepen_top(deepen_top: Any) -> int:
    try:
        value = int(deepen_top)
    except (TypeError, ValueError):
        value = 5
    return max(0, min(DEEPEN_TOP_MAX, value))


class _PipelineBusyError(Exception):
    def __init__(self, waited_s: float = 0.0) -> None:
        super().__init__()
        self.waited_s = waited_s


#: How often a queued run re-reads its place in the queue, and tells the
#: reader with a ``queued`` event if it has moved (Phase 238).
_QUEUE_POLL_S = 3.0


class _Waiter:
    __slots__ = ("future",)

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.future: asyncio.Future[None] = loop.create_future()


class _PipelineGate:
    """Counts pipelines in flight and queues the rest, first come first served.

    Phase 238 replaced an ``asyncio.Condition`` here: every release woke every
    waiter to race for the slot, so a run that arrived a second ago could take
    it from one that had waited fifty, and nothing could say where a run stood.
    Now a released slot is handed straight to the oldest waiter, and a
    waiter's position is its place in :attr:`waiters` — what the ``queued``
    event reports to the loading grid.

    Bound to one event loop (the test suite runs several), so it is rebuilt
    when the loop changes.
    """

    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.active = 0
        self.limit = 0
        self.waiters: deque[_Waiter] = deque()

    @property
    def queued(self) -> int:
        return len(self.waiters)

    def position(self, waiter: _Waiter) -> int:
        """1 = next to run; 0 = not queued."""
        try:
            return self.waiters.index(waiter) + 1
        except ValueError:
            return 0

    async def acquire(
        self,
        limit: int,
        max_wait: float,
        on_queued: Callable[[int], Awaitable[None]] | None = None,
    ) -> float:
        """Take a slot and return the seconds spent waiting for it.

        ``on_queued(position)`` is awaited when the run joins the queue and
        whenever its position changes. Raises :class:`_PipelineBusyError`
        after ``max_wait`` seconds without a slot.
        """
        self.limit = limit
        if limit <= 0 or (self.active < limit and not self.waiters):
            self.active += 1
            return 0.0
        waiter = _Waiter(self.loop)
        self.waiters.append(waiter)
        started = self.loop.time()
        deadline = started + max(max_wait, 0.0)
        last = 0
        try:
            while not waiter.future.done():
                pos = self.position(waiter)
                if on_queued is not None and pos != last:
                    last = pos
                    await on_queued(pos)
                    continue  # the push yielded; re-read before sleeping
                remaining = deadline - self.loop.time()
                if remaining <= 0:
                    break
                # ``asyncio.wait``, not ``wait_for``: it never cancels the
                # future on a timeout, and a cancellation of this task always
                # arrives as CancelledError — ``wait_for`` can swallow one
                # that lands as the slot is handed over.
                await asyncio.wait((waiter.future,), timeout=min(remaining, _QUEUE_POLL_S))
        except BaseException:
            self._abandon(waiter)
            raise
        if waiter.future.done():
            return self.loop.time() - started
        self._abandon(waiter)
        raise _PipelineBusyError(self.loop.time() - started)

    def _abandon(self, waiter: _Waiter) -> None:
        """A waiter leaving without running: drop it from the queue, or pass
        on the slot it was handed as it left."""
        if waiter.future.done() and not waiter.future.cancelled():
            self.release()
            return
        waiter.future.cancel()
        try:
            self.waiters.remove(waiter)
        except ValueError:
            pass

    def release(self) -> None:
        self.active -= 1
        while self.waiters and (self.limit <= 0 or self.active < self.limit):
            waiter = self.waiters.popleft()
            if waiter.future.done():
                continue
            self.active += 1
            waiter.future.set_result(None)


_GATE: _PipelineGate | None = None


def _gate() -> _PipelineGate:
    global _GATE
    if _GATE is None or _GATE.loop is not asyncio.get_running_loop():
        _GATE = _PipelineGate()
    return _GATE


def pipelines_running() -> int:
    """Pipelines in flight in this process (``/memstats`` and tests)."""
    return _GATE.active if _GATE is not None else 0


def pipelines_queued() -> int:
    """Runs waiting for a slot in this process (tests)."""
    return _GATE.queued if _GATE is not None else 0


class _Flight:
    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.events: list[LookupEvent] = []
        self.done = False
        self.exc: BaseException | None = None
        self.cond = asyncio.Condition()
        self.task: asyncio.Task[None] | None = None

    async def push(self, event: LookupEvent) -> None:
        async with self.cond:
            self.events.append(event)
            self.cond.notify_all()

    async def finish(self) -> None:
        async with self.cond:
            self.done = True
            self.cond.notify_all()

    async def follow(self) -> AsyncIterator[LookupEvent]:
        i = 0
        while True:
            async with self.cond:
                await self.cond.wait_for(lambda: i < len(self.events) or self.done)
                batch = self.events[i:]
                finished = self.done
            for event in batch:
                yield event
            i += len(batch)
            if finished and i >= len(self.events):
                break
        if self.exc is not None and not isinstance(self.exc, asyncio.CancelledError):
            raise self.exc


_IN_FLIGHT: dict[str, _Flight] = {}

#: Error statuses after which a run's charge is given back: nothing upstream
#: was spent on a malformed LEI (400), or on a run refused a slot (503).
_REFUNDED_STATUSES = frozenset({400, 503})

#: Events about the *wait*, not the run (Phase 238). Followers see them while
#: the run is in the air; the replay cache — and so a saved report — keeps
#: only what the run found.
_TRANSIENT_EVENTS = frozenset({"queued"})


def _queue_wait_for(kind: str | None) -> float:
    """How long a fresh run started by ``kind`` may queue for a slot. The
    interactive stream waits longest, because the loading grid shows the
    reader that it is waiting and where it stands (Phase 238)."""
    settings = get_settings()
    if kind == "stream":
        return max(settings.lookup_stream_queue_wait_s, settings.lookup_queue_wait_s)
    return settings.lookup_queue_wait_s


async def _run_flight(
    key: str, lei: str, deepen_top: int, flight: _Flight, stored: float, charged: str | None
) -> None:
    settings = get_settings()
    completed_at: str | None = None
    gate = _gate()
    kind = _lookup_budget.current_caller_kind()
    max_wait = _queue_wait_for(kind)
    joined_queue = False

    async def _on_queued(position: int) -> None:
        nonlocal joined_queue
        if not joined_queue:
            joined_queue = True
            _pipelinestats.record_queued(kind, gate.queued)
        await flight.push((
            "queued",
            {
                "position": position,
                "running": gate.active,
                "limit": gate.limit,
                "max_wait_s": int(max_wait),
            },
        ))

    try:
        try:
            waited = await gate.acquire(settings.lookup_max_concurrent, max_wait, _on_queued)
        except _PipelineBusyError as exc:
            _pipelinestats.record_refused(kind, exc.waited_s, gate.queued)
            _lookup_budget.refund(charged)
            await flight.push((
                "error",
                {
                    "status": 503,
                    "detail": (
                        "OpenCheck is running as many checks as it can at once. "
                        "Try again in a minute."
                    ),
                    "retry_after_s": 30,
                },
            ))
            return
        admitted = time.monotonic()
        _pipelinestats.record_admitted(kind, waited, gate.active, gate.queued)
        finished = False
        try:
            # Through the module, not a name bound at import: routers.lookup
            # imports this one, and a test replaces ``lookup._lookup_pipeline``.
            from .routers import lookup as _lookup

            async for event in _lookup._lookup_pipeline(lei, deepen_top=deepen_top):
                name, payload = event
                if name in ("source_completed", "source_error") and isinstance(payload, dict):
                    _pipelinestats.record_source(
                        payload.get("source_id"),
                        time.monotonic() - admitted,
                        timed_out=payload.get("error_type") == "timeout",
                    )
                if name == "done":
                    # Phase 216: the run names itself on its last event, so a
                    # client holding a live (not replayed) run can still say
                    # which run it is looking at when it asks to save it.
                    # Stamped here, once, and buffered with the stamp — a
                    # replay repeats the same value.
                    completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    event = ("done", {**payload, "run_completed_at": completed_at})
                    finished = True
                elif name == "error" and payload.get("status") in _REFUNDED_STATUSES:
                    _lookup_budget.refund(charged)
                await flight.push(event)
        finally:
            held = time.monotonic() - admitted
            gate.release()
            _pipelinestats.record_released(kind, held, gate.active, gate.queued)
            _pipelinestats.record_run(held, completed=finished)
        if completed_at is not None:
            while len(_REPLAY_CACHE) >= _REPLAY_MAX_ENTRIES:
                _REPLAY_CACHE.pop(next(iter(_REPLAY_CACHE)), None)
            _REPLAY_CACHE[key] = _ReplayEntry(
                stored=stored,
                fetched_at=completed_at,
                events=[e for e in flight.events if e[0] not in _TRANSIENT_EVENTS],
            )
    except BaseException as exc:  # noqa: BLE001 — handed to every follower
        flight.exc = exc
        if isinstance(exc, asyncio.CancelledError):
            raise
    finally:
        if _IN_FLIGHT.get(key) is flight:
            _IN_FLIGHT.pop(key, None)
        await flight.finish()


def _replay_key(lei: str, deepen_top: int) -> str:
    return f"{lei.strip().upper()}:{clamp_deepen_top(deepen_top)}"


async def _lookup_pipeline_cached(
    lei: str, deepen_top: int = 5, refresh: bool = False
) -> AsyncIterator[LookupEvent]:
    """Replay a cached completed run, join one in flight, or start one.

    Only starting one costs the caller anything (Phase 234): the run is
    charged to the client's lookup budget first, and a spent budget ends the
    stream with a 429 ``error`` event carrying ``retry_after_s``.
    """
    deepen_top = clamp_deepen_top(deepen_top)
    key = _replay_key(lei, deepen_top)
    now = time.monotonic()

    if not refresh:
        entry = _REPLAY_CACHE.get(key)
        if entry is not None and now - entry.stored < _REPLAY_TTL_SECONDS:
            # Provenance first, so the UI knows before any result arrives.
            yield (
                "replayed",
                {
                    "fetched_at": entry.fetched_at,
                    "age_seconds": round(now - entry.stored, 1),
                },
            )
            for event in entry.events:
                yield event
            return

    loop = asyncio.get_running_loop()
    flight = _IN_FLIGHT.get(key)
    if flight is None or flight.loop is not loop or flight.done:
        try:
            charged = await _lookup_budget.charge()
        except _lookup_budget.BudgetExceededError as exc:
            yield (
                "error",
                {"status": 429, "detail": str(exc), "retry_after_s": exc.retry_after_s},
            )
            return
        # A caller that waited for budget may find the run done or started
        # by someone else meanwhile; take that instead and give the charge back.
        flight = _IN_FLIGHT.get(key)
        if flight is not None and flight.loop is loop and not flight.done:
            _lookup_budget.refund(charged)
        else:
            flight = _Flight()
            _IN_FLIGHT[key] = flight
            flight.task = asyncio.create_task(
                _run_flight(
                    key, lei.strip().upper(), deepen_top, flight, time.monotonic(), charged
                )
            )

    async for event in flight.follow():
        yield event


def replay_entry(lei: str, deepen_top: int = 5) -> _ReplayEntry | None:
    """The held completed run for ``(lei, deepen_top)``, or ``None`` when
    there is none or it has aged out of the replay window.

    Phase 216: a saved report is copied from here and nowhere else — the
    server's own record of what it streamed, never a payload a client posts.
    """
    entry = _REPLAY_CACHE.get(_replay_key(lei, deepen_top))
    if entry is None or time.monotonic() - entry.stored >= _REPLAY_TTL_SECONDS:
        return None
    return entry


def _knowability_payload(jurisdiction: str, today: date | None = None) -> dict[str, Any]:
    """The ``knowability`` event: the subject-jurisdiction statement as JSON.

    ``as_of`` records the day the sentence was rendered, so a reader of a
    saved report can see the date the "a change is announced for …" clause
    was judged against. Region-suffixed codes (``US-DE``) fall back to the
    country inside ``statement_for``."""
    today = today or date.today()
    st = knowability_statement_for(jurisdiction, today)
    payload = st.model_dump(mode="json")
    payload["as_of"] = today.isoformat()
    return payload


def fold_lookup_events(lei: str, events: Iterable[LookupEvent]) -> LookupResponse:
    """Collect a lookup's event stream into one ``LookupResponse``.

    Factored out of ``_lookup_impl`` in Phase 216 so a saved report — whose
    payload *is* the stored event list — folds into the same response the
    live pipeline produces: the PDF, Markdown and MCP views of a saved report
    cannot drift from the live ones. A ``hit`` payload may be a ``SourceHit``
    (live, in-process) or its JSON dict (read back from a saved report).
    """
    norm_lei = lei.strip().upper()
    hits: list[Any] = []
    errors: dict[str, str] = {}
    links: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    degraded_sources: list[dict[str, Any]] = []
    source_liveness: dict[str, dict[str, Any]] = {}
    graph_shape: dict[str, Any] = {}
    verdict: str | None = None
    subject_profile: dict[str, Any] | None = None
    knowability: dict[str, Any] | None = None
    knowability_chain: dict[str, Any] | None = None
    listing: dict[str, Any] | None = None
    oa_screening: list[dict[str, Any]] = []
    bods_all: list[dict[str, Any]] = []
    same_pairs: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    license_notices: list[dict[str, str]] = []
    legal_name: str | None = None
    jurisdiction: str | None = None
    derived: dict[str, str] = {}
    replayed = False
    fetched_at: str | None = None
    sources_applicable: list[str] = []
    run_completed_at: str | None = None

    for event, payload in events:
        if event == "replayed":
            replayed = True
            fetched_at = payload["fetched_at"]
        elif event == "sources_applicable":
            sources_applicable = list(payload.get("source_ids") or [])
        elif event == "error":
            raise HTTPException(
                status_code=payload["status"], detail=payload["detail"]
            )
        elif event == "gleif_done":
            legal_name = payload["legal_name"]
            jurisdiction = payload["jurisdiction"]
            derived = payload["derived_identifiers"]
        elif event == "hit":
            hits.append(payload)
        elif event == "source_error":
            errors[payload["source_id"]] = payload["error"]
        elif event == "deepen_error":
            errors.setdefault(payload["source_id"], payload["error"])
        elif event == "deepen_result":
            bods_all.extend(payload["bods"])
        elif event == "cross_source_links":
            links = payload["links"]
        elif event == "possibly_same_entities":
            same_pairs = payload["pairs"]
        elif event == "subject_profile":
            subject_profile = payload.get("profile")
        elif event == "knowability":
            knowability = payload
        elif event == "knowability_chain":
            knowability_chain = payload
        elif event == "listing":
            listing = payload
        elif event == "risk_signals":
            signals = payload["signals"]
            degraded_sources = payload.get("degraded_sources") or []
            verdict = payload.get("verdict")
            oa_screening = payload.get("openaleph_screening") or []
            source_liveness = payload.get("source_liveness") or {}
            graph_shape = payload.get("graph_shape") or {}
        elif event == "done":
            bods_issues = payload["bods_issues"]
            license_notices = payload["license_notices"]
            run_completed_at = payload.get("run_completed_at")

    from .routers.lookup import LookupResponse  # routers.lookup imports this module

    return LookupResponse(
        query=norm_lei,
        kind=SearchKind.ENTITY,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=signals,
        # Two results from one source can map the same party (two OpenSanctions
        # records naming one subsidiary); the export is one statement per id.
        # Folded here, not in the pipeline, so a saved report stored before
        # Phase 235 renders and exports without repeats too.
        # Phase 236: the primary listing goes onto the subject's GLEIF entity
        # statement here, from the frozen event — so a saved report's export
        # carries the listing that was true on the day it ran.
        bods=_listing.apply_to_bods(unique_statements(bods_all), norm_lei, listing),
        bods_issues=bods_issues,
        license_notices=license_notices,
        possibly_same_entities=same_pairs,
        degraded_sources=degraded_sources,
        openaleph_screening=oa_screening,
        source_liveness=source_liveness,
        graph_shape=graph_shape,
        verdict=verdict,
        subject_profile=subject_profile,
        knowability=knowability,
        knowability_chain=knowability_chain,
        listing=listing,
        lei=norm_lei,
        legal_name=legal_name,
        jurisdiction=jurisdiction,
        derived_identifiers=derived,
        replayed=replayed,
        fetched_at=fetched_at,
        sources_applicable=sources_applicable,
        run_completed_at=run_completed_at,
    )
