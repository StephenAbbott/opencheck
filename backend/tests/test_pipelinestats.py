"""Phase 238 — the lookup gate: counted, first come first served, and patient
with the interactive stream.

Follow-up to Phase 234. On 23 Sept 2026 two large companies opened together
were refused with "running as many checks as it can at once", and nothing
could say who held the slots, how often the gate refused anyone or how long a
run takes. These tests pin:

* the ``pipelines`` section of ``/signalstats`` — started / queued / refused
  and slot-seconds per caller kind, queue waits, run times, per-source timings
  and timeouts — aggregate only, never an LEI;
* the caller kind comes from the request path, set by the middleware;
* the queue is FIFO and reports each run's position as a ``queued`` event;
* a run started from ``/lookup-stream`` waits ``lookup_stream_queue_wait_s``,
  everyone else ``lookup_queue_wait_s``;
* ``queued`` events never reach the replay cache (so never a saved report);
* a waiter that goes away leaves the queue and leaks no slot.

The pipeline is a fake throughout, so these are offline.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck import lookup_budget, lookup_replay, pipelinestats, signalstats
from opencheck.config import get_settings
from opencheck.routers import lookup as lk

_LEI_A = "213800LH1BZH3DI6G760"
_LEI_B = "5493001KJTIIGC8Y1R12"
_LEI_C = "529900T8BM49AURSDO55"

_BROWSER = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0 Safari/537.36"
)


class _HeldPipeline:
    """A fake ``_lookup_pipeline`` whose runs each wait for their own LEI's
    release event, so a test decides exactly when each slot frees."""

    def __init__(self, *, sources: tuple[tuple[str, str | None], ...] = ()) -> None:
        self.started: list[str] = []
        self.gates: dict[str, asyncio.Event] = {}
        self.hold: set[str] = set()
        self.sources = sources

    def gate(self, lei: str) -> asyncio.Event:
        return self.gates.setdefault(lei, asyncio.Event())

    async def __call__(self, lei: str, deepen_top: int = 5) -> AsyncIterator[Any]:
        self.started.append(lei)
        yield ("source_started", {"source_id": "gleif", "source_name": "GLEIF"})
        for source_id, error_type in self.sources:
            if error_type is None:
                yield ("source_completed", {"source_id": source_id, "hit_count": 1})
            else:
                yield ("source_error", {"source_id": source_id, "error": "x", "error_type": error_type})
        if lei in self.hold:
            await self.gate(lei).wait()
        yield ("done", {"lei": lei, "bods_issues": [], "license_notices": []})


@pytest.fixture
def one_slot(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OPENCHECK_LOOKUP_MAX_CONCURRENT", "1")
    monkeypatch.setenv("OPENCHECK_LOOKUP_QUEUE_WAIT_S", "0.05")
    monkeypatch.setenv("OPENCHECK_LOOKUP_STREAM_QUEUE_WAIT_S", "5")
    monkeypatch.setattr(lookup_replay, "_QUEUE_POLL_S", 0.01)  # read there (Phase 246)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def _drain(lei: str, **kw: Any) -> list[Any]:
    return [e async for e in lk._lookup_pipeline_cached(lei, **kw)]


async def _until(predicate, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


# ---------------------------------------------------------------------------
# Caller kinds and the numbers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path, kind",
    [
        ("/lookup-stream", "stream"),
        ("/lookup", "api"),
        ("/lookup-source", "other"),  # not a pipeline run, and not /lookup
        ("/batch-stream", "batch"),
        ("/expand-layer", "expand"),
        ("/expand", "expand"),
        ("/export/pdf", "export"),
        ("/narrative", "narrative"),
        ("/watch/items", "watch"),
        ("/mcp", "mcp"),
        ("/mcp/", "mcp"),
        ("/entity/213800LH1BZH3DI6G760", "other"),
        ("", "other"),
        (None, "other"),
    ],
)
def test_caller_kind_comes_from_the_first_path_segment(path, kind) -> None:
    assert pipelinestats.caller_kind_for_path(path) == kind


def test_percentiles_are_nearest_rank_real_observations() -> None:
    assert pipelinestats._percentiles([]) == {"n": 0, "p50": None, "p90": None, "max": None}
    values = [float(v) for v in range(1, 11)]  # 1..10
    assert pipelinestats._percentiles(values) == {"n": 10, "p50": 5.0, "p90": 9.0, "max": 10.0}
    assert pipelinestats._percentiles([7.25]) == {"n": 1, "p50": 7.2, "p90": 7.2, "max": 7.2}


def test_an_unknown_kind_is_folded_into_other_and_none_is_server() -> None:
    pipelinestats.record_admitted("../etc/passwd", 0.0, 1, 0)
    pipelinestats.record_admitted(None, 0.0, 1, 0)
    by = pipelinestats.stats()["by_caller"]
    assert set(by) == {"other", "server"}


async def test_a_run_is_counted_with_its_kind_wait_time_and_sources(monkeypatch) -> None:
    fake = _HeldPipeline(sources=(("opensanctions", None), ("cvr_denmark", "timeout"), ("inpi", "http")))
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    with lookup_budget.caller_scope("api"):
        await _drain(_LEI_A)
    p = signalstats.stats()["pipelines"]
    assert p["started"] == 1 and p["refused"] == 0 and p["queued"] == 0
    assert p["by_caller"]["api"]["started"] == 1
    assert p["by_caller"]["api"]["slot_seconds"] >= 0
    assert p["runs_completed"] == 1 and p["run_seconds"]["n"] == 1
    assert p["queue_wait_s"]["admitted"]["n"] == 1
    assert p["active_now"] == 0 and p["peak_active"] == 1
    assert set(p["sources"]) == {"opensanctions", "cvr_denmark", "inpi"}
    assert p["sources"]["cvr_denmark"]["timeouts"] == 1
    assert p["sources"]["inpi"]["timeouts"] == 0  # an error, not a timeout
    assert len(p["slowest_sources"]) == 3


async def test_the_numbers_never_carry_an_lei(monkeypatch) -> None:
    monkeypatch.setattr(lk, "_lookup_pipeline", _HeldPipeline(sources=(("gleif", None),)))
    with lookup_budget.client_scope("198.51.100.7"), lookup_budget.caller_scope("stream"):
        await _drain(_LEI_A)
    blob = json.dumps(signalstats.stats())
    assert _LEI_A not in blob and "198.51.100.7" not in blob
    assert '"stream"' in blob  # not passing by recording nothing


# ---------------------------------------------------------------------------
# The queue
# ---------------------------------------------------------------------------


async def test_the_queue_is_first_come_first_served_and_reports_position(one_slot, monkeypatch) -> None:
    fake = _HeldPipeline()
    fake.hold = {_LEI_A, _LEI_B, _LEI_C}
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    with lookup_budget.caller_scope("stream"):
        a = asyncio.create_task(_drain(_LEI_A))
        await _until(lambda: lk.pipelines_running() == 1)
        b = asyncio.create_task(_drain(_LEI_B))
        await _until(lambda: lk.pipelines_queued() == 1)
        c = asyncio.create_task(_drain(_LEI_C))
        await _until(lambda: lk.pipelines_queued() == 2)
        fake.gate(_LEI_A).set()
        await a
        await _until(lambda: fake.started == [_LEI_A, _LEI_B])
        flight_c = lk._IN_FLIGHT[f"{_LEI_C}:5"]
        await _until(lambda: [p["position"] for e, p in flight_c.events if e == "queued"] == [2, 1])
        fake.gate(_LEI_B).set()
        fake.gate(_LEI_C).set()
        events_b, events_c = await b, await c
    assert fake.started == [_LEI_A, _LEI_B, _LEI_C]
    positions_c = [p["position"] for e, p in events_c if e == "queued"]
    assert positions_c == [2, 1]  # it moved up when B got the slot
    assert [p["position"] for e, p in events_b if e == "queued"] == [1]
    queued = next(p for e, p in events_b if e == "queued")
    assert queued["limit"] == 1 and queued["running"] == 1 and queued["max_wait_s"] == 5
    assert events_b[-1][0] == "done" and events_c[-1][0] == "done"
    assert lk.pipelines_running() == 0 and lk.pipelines_queued() == 0
    p = pipelinestats.stats()
    assert p["by_caller"]["stream"] == {
        "started": 3,
        "queued": 2,
        "refused": 0,
        "slot_seconds": p["by_caller"]["stream"]["slot_seconds"],
    }
    assert p["peak_queued"] == 2


async def test_queued_events_never_reach_the_replay_cache(one_slot, monkeypatch) -> None:
    fake = _HeldPipeline()
    fake.hold = {_LEI_A}
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    with lookup_budget.caller_scope("stream"):
        a = asyncio.create_task(_drain(_LEI_A))
        await _until(lambda: lk.pipelines_running() == 1)
        b = asyncio.create_task(_drain(_LEI_B))
        await _until(lambda: lk.pipelines_queued() == 1)
        fake.gate(_LEI_A).set()
        await a
        live = await b
    assert any(e == "queued" for e, _ in live)
    held = lk.replay_entry(_LEI_B)
    assert held is not None
    assert [e for e, _ in held.events] == ["source_started", "done"]
    replayed = await _drain(_LEI_B)
    assert "queued" not in [e for e, _ in replayed]


async def test_only_the_interactive_stream_waits_longer(one_slot, monkeypatch) -> None:
    fake = _HeldPipeline()
    fake.hold = {_LEI_A}
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    with lookup_budget.caller_scope("api"):
        running = asyncio.create_task(_drain(_LEI_A))
        await _until(lambda: lk.pipelines_running() == 1)
        busy = await _drain(_LEI_B)  # waits 0.05 s, then refused
    assert busy[-1][0] == "error" and busy[-1][1]["status"] == 503
    assert [e for e, _ in busy] == ["queued", "error"]
    with lookup_budget.caller_scope("stream"):
        patient = asyncio.create_task(_drain(_LEI_C))
        await asyncio.sleep(0.2)  # four times the API wait
        assert not patient.done()
        fake.gate(_LEI_A).set()
        await running
        events = await patient
    assert events[-1][0] == "done"
    p = pipelinestats.stats()
    assert p["refused"] == 1 and p["by_caller"]["api"]["refused"] == 1
    assert p["queue_wait_s"]["refused"]["n"] == 1
    assert p["by_caller"]["stream"]["started"] == 1


async def test_the_stream_never_waits_less_than_everyone_else(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_LOOKUP_QUEUE_WAIT_S", "90")
    monkeypatch.setenv("OPENCHECK_LOOKUP_STREAM_QUEUE_WAIT_S", "10")
    get_settings.cache_clear()
    try:
        assert lk._queue_wait_for("stream") == 90
        assert lk._queue_wait_for("mcp") == 90
        assert lk._queue_wait_for(None) == 90
    finally:
        get_settings.cache_clear()


async def test_a_waiter_that_goes_away_leaves_the_queue_and_leaks_no_slot(one_slot) -> None:
    gate = lk._gate()
    await gate.acquire(1, 1.0)
    waiting = asyncio.create_task(gate.acquire(1, 5.0))
    await _until(lambda: gate.queued == 1)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert gate.queued == 0
    gate.release()
    assert gate.active == 0
    # The slot is free for the next caller at once.
    assert await gate.acquire(1, 0.0) == 0.0
    gate.release()


async def test_a_slot_handed_to_a_leaving_waiter_is_passed_on(one_slot) -> None:
    gate = lk._gate()
    await gate.acquire(1, 1.0)
    first = asyncio.create_task(gate.acquire(1, 5.0))
    second = asyncio.create_task(gate.acquire(1, 5.0))
    await _until(lambda: gate.queued == 2)
    gate.release()  # hands the slot to `first` …
    first.cancel()  # … which leaves before it can use it
    with pytest.raises(asyncio.CancelledError):
        await first
    await asyncio.wait_for(second, 1.0)  # so `second` gets it
    assert gate.active == 1 and gate.queued == 0
    gate.release()
    assert gate.active == 0


# ---------------------------------------------------------------------------
# End to end: the middleware names the caller
# ---------------------------------------------------------------------------


def test_the_middleware_attributes_runs_to_their_route(monkeypatch) -> None:
    from opencheck.app import app

    monkeypatch.setattr(lk, "_lookup_pipeline", _HeldPipeline())
    client = TestClient(app)
    r = client.get(f"/lookup-stream?lei={_LEI_A}", headers={"User-Agent": _BROWSER})
    assert r.status_code == 200
    r = client.get(f"/lookup?lei={_LEI_B}", headers={"User-Agent": _BROWSER})
    assert r.status_code == 200
    body = client.get("/signalstats").json()
    by = body["pipelines"]["by_caller"]
    assert by["stream"]["started"] == 1
    assert by["api"]["started"] == 1
