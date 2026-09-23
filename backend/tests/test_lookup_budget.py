"""Phase 234 — one per-client budget of full lookups, whoever asks for them.

Pins the ticket's "done when": no route or MCP tool can run more full lookups
a minute than ``/lookup`` allows (one budget, charged at the moment a fresh
pipeline starts), one run per LEI at a time (single-flight), a bounded number
at once (the gate), ``deepen_top`` clamped, failed FullCheck hops reported
rather than swallowed, and one client unable to exhaust GLEIF, watchlist or
saved-report capacity.

The pipeline itself is replaced by a fake generator throughout, so these
tests are offline and exercise only the machinery around it.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from opencheck import lookup_budget
from opencheck.config import get_settings
from opencheck.routers import lookup as lk

_LEI_A = "213800LH1BZH3DI6G760"
_LEI_B = "5493001KJTIIGC8Y1R12"
_LEI_C = "529900T8BM49AURSDO55"
_LEI_D = "7LTWFZYICNSX8D621K86"


@pytest.fixture
def budget_on(monkeypatch: pytest.MonkeyPatch):
    """Rate limiting on (the suite turns it off), a lookup budget of 2/minute."""
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_ENABLED", "1")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_LOOKUP", "2/minute")
    get_settings.cache_clear()
    lookup_budget.reset_for_tests()
    yield
    get_settings.cache_clear()
    lookup_budget.reset_for_tests()


class _FakePipeline:
    """Stands in for ``_lookup_pipeline``: counts runs, can be held open."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []
        self.release = asyncio.Event()
        self.hold = False

    async def __call__(self, lei: str, deepen_top: int = 5) -> AsyncIterator[Any]:
        self.calls.append((lei, deepen_top))
        yield ("source_started", {"source_id": "gleif", "source_name": "GLEIF"})
        if self.hold:
            await self.release.wait()
        yield ("done", {"lei": lei, "bods_issues": [], "license_notices": []})


async def _drain(lei: str, **kw: Any) -> list[Any]:
    return [e async for e in lk._lookup_pipeline_cached(lei, **kw)]


# ---------------------------------------------------------------------------
# The budget
# ---------------------------------------------------------------------------


def test_budget_refuses_past_the_lookup_tier_and_names_the_wait(budget_on) -> None:
    b = lookup_budget._Budget()
    assert b.try_charge("198.51.100.1", now=100.0) is None
    assert b.try_charge("198.51.100.1", now=101.0) is None
    wait = b.try_charge("198.51.100.1", now=102.0)
    assert wait == pytest.approx(58.0)
    # Another client is untouched, and the first frees when its oldest ages out.
    assert b.try_charge("198.51.100.2", now=102.0) is None
    assert b.try_charge("198.51.100.1", now=160.5) is None


def test_refund_gives_the_charge_back(budget_on) -> None:
    b = lookup_budget._Budget()
    b.try_charge("c", now=1.0)
    b.try_charge("c", now=2.0)
    b.refund("c")
    assert b.try_charge("c", now=3.0) is None


async def test_no_client_means_server_work_and_is_never_charged(budget_on) -> None:
    for _ in range(5):
        assert await lookup_budget.charge() is None


async def test_charge_waits_when_asked_to(budget_on, monkeypatch) -> None:
    waits = iter([0.05, None])
    monkeypatch.setattr(lookup_budget._budget, "try_charge", lambda client: next(waits))
    with lookup_budget.client_scope("c"), lookup_budget.waiting(5):
        assert await lookup_budget.charge() == "c"
    with lookup_budget.client_scope("c"):
        monkeypatch.setattr(lookup_budget._budget, "try_charge", lambda client: 30.0)
        with pytest.raises(lookup_budget.BudgetExceededError) as exc:
            await lookup_budget.charge()
        assert exc.value.retry_after_s == 30


# ---------------------------------------------------------------------------
# Fresh runs cost; replays and joined runs do not
# ---------------------------------------------------------------------------


async def test_only_a_fresh_run_is_charged(budget_on, monkeypatch) -> None:
    fake = _FakePipeline()
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    with lookup_budget.client_scope("198.51.100.9"):
        await _drain(_LEI_A)
        replay = await _drain(_LEI_A)  # replay: free
        assert replay[0][0] == "replayed"
        await _drain(_LEI_B)  # second fresh run
        refused = await _drain(_LEI_C)  # third: over 2/minute
    assert len(fake.calls) == 2
    assert refused == [("error", {"status": 429, "detail": refused[0][1]["detail"], "retry_after_s": refused[0][1]["retry_after_s"]})]
    assert refused[0][1]["retry_after_s"] >= 1


async def test_concurrent_callers_share_one_run(budget_on, monkeypatch) -> None:
    fake = _FakePipeline()
    fake.hold = True
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    with lookup_budget.client_scope("198.51.100.9"):
        first = asyncio.create_task(_drain(_LEI_A))
        await asyncio.sleep(0.01)
        second = asyncio.create_task(_drain(_LEI_A, refresh=True))
        await asyncio.sleep(0.01)
        fake.release.set()
        a, b = await asyncio.gather(first, second)
    assert len(fake.calls) == 1
    assert a == b and a[-1][0] == "done"
    assert lookup_budget._budget.used("198.51.100.9") == 1


async def test_a_follower_that_leaves_does_not_cancel_the_run(budget_on, monkeypatch) -> None:
    fake = _FakePipeline()
    fake.hold = True
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    gen = lk._lookup_pipeline_cached(_LEI_A)
    await gen.__anext__()
    await gen.aclose()  # the SSE tab closed
    fake.release.set()
    for _ in range(50):
        if lk.replay_entry(_LEI_A) is not None:
            break
        await asyncio.sleep(0.01)
    assert lk.replay_entry(_LEI_A) is not None


async def test_a_malformed_lei_costs_nothing(budget_on) -> None:
    with lookup_budget.client_scope("c"):
        for _ in range(4):
            events = await _drain("not-an-lei")
            assert events[-1][0] == "error" and events[-1][1]["status"] == 400
    assert lookup_budget._budget.used("c") == 0


async def test_lookup_impl_raises_429_with_retry_after(budget_on, monkeypatch) -> None:
    fake = _FakePipeline()
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    monkeypatch.setattr(lk, "fold_lookup_events", lambda lei, events: SimpleNamespace(lei=lei))
    with lookup_budget.client_scope("c"):
        await lk._lookup_impl(_LEI_A)
        await lk._lookup_impl(_LEI_B)
        with pytest.raises(HTTPException) as exc:
            await lk._lookup_impl(_LEI_C)
    assert exc.value.status_code == 429
    assert int(exc.value.headers["Retry-After"]) >= 1


# ---------------------------------------------------------------------------
# deepen_top and the concurrency gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value, expected", [(50, 10), (-3, 0), (7, 7), ("x", 5), (None, 5)])
def test_deepen_top_is_clamped(value, expected) -> None:
    assert lk.clamp_deepen_top(value) == expected


async def test_an_out_of_range_deepen_top_reuses_the_clamped_run(monkeypatch) -> None:
    fake = _FakePipeline()
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    await _drain(_LEI_A, deepen_top=99)
    await _drain(_LEI_A, deepen_top=10_000)
    assert fake.calls == [(_LEI_A, 10)]
    assert set(lk._REPLAY_CACHE) == {f"{_LEI_A}:10"}
    assert lk.replay_entry(_LEI_A, 500) is not None


async def test_the_gate_refuses_a_run_that_cannot_get_a_slot(budget_on, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_LOOKUP_MAX_CONCURRENT", "1")
    monkeypatch.setenv("OPENCHECK_LOOKUP_QUEUE_WAIT_S", "0.05")
    get_settings.cache_clear()
    fake = _FakePipeline()
    fake.hold = True
    monkeypatch.setattr(lk, "_lookup_pipeline", fake)
    with lookup_budget.client_scope("c"):
        running = asyncio.create_task(_drain(_LEI_A))
        await asyncio.sleep(0.01)
        assert lk.pipelines_running() == 1
        busy = await _drain(_LEI_B)
        fake.release.set()
        await running
    assert busy[-1][0] == "error" and busy[-1][1]["status"] == 503
    assert len(fake.calls) == 1
    # The refused run's charge was given back.
    assert lookup_budget._budget.used("c") == 1
    assert lk.pipelines_running() == 0


# ---------------------------------------------------------------------------
# /expand-layer: charged per hop, partial layers, failures reported
# ---------------------------------------------------------------------------


def _layer(lei: str) -> list[dict]:
    return [{"statementId": f"e-{lei}", "recordType": "entity", "recordDetails": {}}]


def test_expand_layer_defers_what_the_budget_does_not_cover(budget_on, monkeypatch) -> None:
    from opencheck.app import app

    async def _fake_lookup(*, lei, deepen_top=3):
        await lookup_budget.charge()  # what the real pipeline entry does
        return SimpleNamespace(lei=lei, bods=_layer(lei), risk_signals=[])

    async def _charging(*, lei, deepen_top=3):
        try:
            return await _fake_lookup(lei=lei, deepen_top=deepen_top)
        except lookup_budget.BudgetExceededError as exc:
            raise HTTPException(429, str(exc), headers={"Retry-After": str(exc.retry_after_s)}) from exc

    monkeypatch.setattr(lk, "_lookup_impl", _charging)
    items = [{"anchor": f"a{i}", "lei": lei} for i, lei in enumerate([_LEI_A, _LEI_B, _LEI_C, _LEI_D])]
    r = TestClient(app).post("/expand-layer", json={"items": items})
    assert r.status_code == 200
    body = r.json()
    assert len(body["expanded"]) == 2 and len(body["deferred"]) == 2
    assert set(body["expanded"]) | set(body["deferred"]) == {"a0", "a1", "a2", "a3"}
    assert body["retry_after_s"] >= 1
    assert body["hops"]["lei"] == 2
    assert body["failed"] == []


def test_expand_layer_reports_a_failed_hop_instead_of_drawing_no_owners(monkeypatch) -> None:
    from opencheck.app import app

    async def _fake_lookup(*, lei, deepen_top=3):
        if lei == _LEI_B:
            raise HTTPException(404, "LEI not found in GLEIF.")
        if lei == _LEI_C:
            raise RuntimeError("upstream fell over")
        return SimpleNamespace(lei=lei, bods=_layer(lei), risk_signals=[])

    monkeypatch.setattr(lk, "_lookup_impl", _fake_lookup)
    items = [{"anchor": "a", "lei": _LEI_A}, {"anchor": "b", "lei": _LEI_B}, {"anchor": "c", "lei": _LEI_C}]
    body = TestClient(app).post("/expand-layer", json={"items": items}).json()
    failed = {f["anchor"]: f for f in body["failed"]}
    assert set(failed) == {"b", "c"}
    assert failed["b"]["status"] == 404 and "not found" in failed["b"]["reason"]
    assert failed["c"]["status"] == 500 and failed["c"]["reason"].startswith("RuntimeError")
    # Failed hops were answered for (not deferred), so the client does not retry them forever.
    assert set(body["expanded"]) == {"a", "b", "c"} and body["deferred"] == []


def test_routes_that_run_a_lookup_are_on_the_lookup_tier() -> None:
    from opencheck.app import app
    from opencheck.ratelimit import limiter, lookup_tier

    wanted = {"/expand", "/expand-layer", "/watch/items"}
    seen = set()
    for route in app.routes:
        if getattr(route, "path", None) not in wanted:
            continue
        name = f"{route.endpoint.__module__}.{route.endpoint.__name__}"
        groups = limiter._dynamic_route_limits.get(name) or []
        providers = {g._LimitGroup__limit_provider for g in groups}
        assert lookup_tier in providers, route.path
        seen.add(route.path)
    assert seen == wanted


# ---------------------------------------------------------------------------
# The MCP mount
# ---------------------------------------------------------------------------


def _mcp_call(client: TestClient, tool: str, ip: str = "198.51.100.40", rid: int = 1):
    return client.post(
        "/mcp",
        headers={
            "accept": "application/json, text/event-stream",
            "content-type": "application/json",
            "x-forwarded-for": ip,
        },
        json={
            "jsonrpc": "2.0",
            "id": rid,
            "method": "tools/call",
            "params": {"name": tool, "arguments": {"lei": _LEI_A} if tool == "opencheck_lookup" else {}},
        },
    )


def test_the_app_serves_mcp_behind_the_guard() -> None:
    pytest.importorskip("mcp")
    from opencheck.app import app
    from opencheck.mcp.guard import McpRateGuard

    routes = [r for r in app.router.routes if getattr(r, "path", None) == "/mcp"]
    assert routes and all(isinstance(r.endpoint, McpRateGuard) for r in routes)


@pytest.fixture
def mcp_client(monkeypatch):
    """A fresh MCP app behind the guard and the client middleware, exactly as
    ``app.py`` assembles them. Built fresh because the streamable-HTTP session
    manager can be run only once per instance, and the suite's other
    lifespans have used the app's."""
    pytest.importorskip("mcp")
    from contextlib import asynccontextmanager

    from starlette.applications import Starlette
    from starlette.middleware import Middleware

    from opencheck.mcp import server
    from opencheck.mcp.guard import guard_routes

    seen: list[str | None] = []

    async def _fake(**_kw: Any) -> dict[str, Any]:
        seen.append(lookup_budget.current_client())
        return {"ok": True}

    for name in ("opencheck_lookup", "opencheck_list_sources"):
        monkeypatch.setattr(server.mcp._tool_manager._tools[name], "fn", _fake)
    monkeypatch.setattr(server.mcp, "_session_manager", None)
    mcp_app = server.mcp.streamable_http_app()
    manager = server.mcp.session_manager

    @asynccontextmanager
    async def _lifespan(_app):
        async with manager.run():
            yield

    app = Starlette(
        routes=guard_routes(list(mcp_app.routes)),
        middleware=[Middleware(lookup_budget.ClientScopeMiddleware)],
        lifespan=_lifespan,
    )
    with TestClient(app) as c:
        yield c, seen


def test_mcp_tool_calls_are_limited_at_the_rest_tier(budget_on, monkeypatch, mcp_client) -> None:
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_LOOKUP", "1/minute")
    get_settings.cache_clear()
    client, seen = mcp_client
    assert _mcp_call(client, "opencheck_lookup").status_code == 200
    refused = _mcp_call(client, "opencheck_lookup", rid=2)
    assert refused.status_code == 429
    assert int(refused.headers["retry-after"]) >= 1
    assert refused.json()["id"] == 2 and "error" in refused.json()
    # A tool with no REST fan-out counterpart is not on the lookup tier …
    assert _mcp_call(client, "opencheck_list_sources", rid=3).status_code == 200
    # … and another address has its own budget.
    assert _mcp_call(client, "opencheck_lookup", ip="198.51.100.41").status_code == 200
    # The tools ran as their callers: the work they start is charged there.
    assert seen == ["198.51.100.40", "198.51.100.40", "198.51.100.41"]


# ---------------------------------------------------------------------------
# GLEIF headroom
# ---------------------------------------------------------------------------


async def test_discretionary_gleif_calls_leave_the_reserve_to_lookups(monkeypatch) -> None:
    from opencheck import gleif_throttle as gt

    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "5")
    monkeypatch.setenv("OPENCHECK_GLEIF_LOOKUP_RESERVE", "2")
    monkeypatch.setenv("OPENCHECK_GLEIF_THROTTLE_MAX_WAIT_S", "0")
    get_settings.cache_clear()
    t = gt.GleifThrottle()
    try:
        for _ in range(3):
            with gt.discretionary():
                await t.acquire()
        # 3 sent + 2 reserved = the limit: a discretionary call is refused
        # at once, without waiting and without sending …
        with gt.discretionary(), pytest.raises(gt.GleifRateLimitedError):
            await t.acquire()
        assert t.in_flight_window == 3
        # … while a lookup still gets the two reserved slots.
        await t.acquire()
        await t.acquire()
        assert t.in_flight_window == 5
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Batch rows
# ---------------------------------------------------------------------------


async def test_a_budget_refusal_is_a_retryable_batch_row(monkeypatch) -> None:
    from opencheck import batch

    async def _refuse(lei, deepen_top=5, refresh=False):
        raise HTTPException(429, "budget spent", headers={"Retry-After": "30"})

    monkeypatch.setattr(lk, "_lookup_impl", _refuse)
    event, row, _ = await batch._one(_LEI_A, 5, False, asyncio.Semaphore(1))
    assert event == "row_failed" and row["status"] == 429 and row["retryable"] is True


# ---------------------------------------------------------------------------
# Saved reports and watchlists: per-client quotas
# ---------------------------------------------------------------------------


def test_saved_report_quota_is_per_client(budget_on, monkeypatch) -> None:
    from opencheck import saved_reports as sr

    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_PER_IP", "2/day")
    get_settings.cache_clear()
    with lookup_budget.client_scope("198.51.100.5"):
        for _ in range(2):
            sr.check_client_quota()
            sr.spend_client_quota()
        with pytest.raises(sr.ClientQuotaError) as exc:
            sr.check_client_quota()
        assert exc.value.status == 429 and exc.value.retry_after_s >= 1
    with lookup_budget.client_scope("198.51.100.6"):
        sr.check_client_quota()
    sr.check_client_quota()  # server-started work: no client, no quota
