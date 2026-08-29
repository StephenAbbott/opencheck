"""Phase 143 — the process-wide GLEIF throttle and the 429 degradation chain.

GLEIF rate-limits by IP (60 req/min shared across everything one deployment
sends it). These tests pin the three layers built after the 2026-08-29
production saturation:

* ``GleifThrottle`` — the sliding-window budget + shared penalty box;
* ``GleifThrottledTransport`` — the build_client wrapper that enforces the
  budget for ``api.gleif.org`` only and retries a 429 once, honouring
  Retry-After;
* the adapter's fallback chain — stale cache, then the entity-pages Golden
  Copy snapshot, then ``GleifRateLimitedError`` which ``/lookup`` turns into
  a friendly 503 instead of the old ``502 GLEIF fetch failed``.

The suite-wide conftest disables the throttle
(``OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE=0``); tests here re-enable it
per-fixture.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import entity_pages as ep
from opencheck import provenance
from opencheck.app import app
from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.gleif_throttle import (
    GleifRateLimitedError,
    GleifThrottle,
    GleifThrottledTransport,
    get_throttle,
    reset_throttle_for_tests,
)
from opencheck.http import build_client
from opencheck.sources.gleif import GleifAdapter

_API = "https://api.gleif.org/api/v1"
_LEI = "21380068P1DRHMJ8KU70"
_PARENT_LEI = "PARENT0000000000XX01"
_CHILD_LEI = "CHILD00000000000XX02"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    """Tmp data root, live mode on, throttle OFF unless a test turns it on."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_FILE", raising=False)
    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "0")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    reset_throttle_for_tests()
    yield
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    reset_throttle_for_tests()


def _enable_throttle(monkeypatch, per_minute: int, max_wait_s: float) -> None:
    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", str(per_minute))
    monkeypatch.setenv("OPENCHECK_GLEIF_THROTTLE_MAX_WAIT_S", str(max_wait_s))
    get_settings.cache_clear()
    reset_throttle_for_tests()


def _entity_store_db(tmp_path: Path) -> Path:
    """A minimal entity-pages SQLite: subject + parent + one child."""
    db = tmp_path / "entity_pages.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(ep.SCHEMA)
    rows = [
        (_LEI, "SHELL PLC", "shell-plc", "ACTIVE", "ISSUED", "GB", "H0PO",
         "LONDON", "", "GB", "2012-06-06", "2026-08-01", None, _PARENT_LEI, None),
        (_PARENT_LEI, "SHELL GROUP HOLDINGS", "shell-group-holdings", "ACTIVE",
         "ISSUED", "GB", "H0PO", "LONDON", "", "GB", "2010-01-01", "2026-08-01",
         None, None, None),
        (_CHILD_LEI, "SHELL SUBSIDIARY B.V.", "shell-subsidiary-bv", "ACTIVE",
         "ISSUED", "NL", "B6ES", "DEN HAAG", "", "NL", "2015-05-05",
         "2026-08-01", None, _LEI, _LEI),
    ]
    conn.executemany(
        "INSERT INTO entities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('source_publish_date', '2026-08-03 08:00:00')"
    )
    conn.commit()
    conn.close()
    return db


def _age_cache_entry(tmp_path: Path, key: str, age_days: float) -> None:
    """Rewrite a live-tier cache entry's ``_cached_at`` so it reads as stale."""
    path = tmp_path / "cache" / "live" / f"{key}.json"
    wrapper = json.loads(path.read_text())
    wrapper["_cached_at"] = time.time() - age_days * 86_400
    path.write_text(json.dumps(wrapper))


# ---------------------------------------------------------------------------
# GleifThrottle — the budget itself
# ---------------------------------------------------------------------------


async def test_throttle_disabled_is_a_noop() -> None:
    throttle = GleifThrottle()
    for _ in range(200):  # far beyond any real budget, instant when disabled
        await throttle.acquire()
    assert throttle.in_flight_window == 0  # disabled acquire records nothing


async def test_throttle_grants_up_to_limit_then_raises(monkeypatch) -> None:
    _enable_throttle(monkeypatch, per_minute=3, max_wait_s=0.1)
    throttle = GleifThrottle()
    for _ in range(3):
        await throttle.acquire()
    assert throttle.in_flight_window == 3
    with pytest.raises(GleifRateLimitedError):
        await throttle.acquire()


async def test_penalty_blocks_even_with_budget_free(monkeypatch) -> None:
    _enable_throttle(monkeypatch, per_minute=50, max_wait_s=0.1)
    throttle = GleifThrottle()
    throttle.penalise(5.0)
    with pytest.raises(GleifRateLimitedError):
        await throttle.acquire()


async def test_short_penalty_is_waited_out(monkeypatch) -> None:
    _enable_throttle(monkeypatch, per_minute=50, max_wait_s=2.0)
    throttle = GleifThrottle()
    throttle.penalise(0.2)
    start = time.monotonic()
    await throttle.acquire()  # must wait ~0.2s, not raise
    assert time.monotonic() - start >= 0.15
    assert throttle.in_flight_window == 1


# ---------------------------------------------------------------------------
# GleifThrottledTransport — scope, retry, penalty
# ---------------------------------------------------------------------------


def _mock_transport(responses: list[httpx.Response]):
    """Inner transport yielding canned responses; records the requests."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return responses[min(len(seen), len(responses)) - 1]

    return httpx.MockTransport(handler), seen


async def test_transport_ignores_other_hosts(monkeypatch) -> None:
    _enable_throttle(monkeypatch, per_minute=1, max_wait_s=0.1)
    inner, seen = _mock_transport([httpx.Response(200, json={})])
    transport = GleifThrottledTransport(inner)
    async with httpx.AsyncClient(transport=transport) as client:
        for _ in range(3):  # would exhaust a budget of 1 immediately
            response = await client.get("https://api.example.org/thing")
            assert response.status_code == 200
    assert len(seen) == 3
    assert get_throttle().in_flight_window == 0  # no GLEIF budget consumed


async def test_transport_retries_a_429_once_honouring_retry_after(
    monkeypatch,
) -> None:
    _enable_throttle(monkeypatch, per_minute=50, max_wait_s=5.0)
    inner, seen = _mock_transport(
        [
            httpx.Response(429, headers={"Retry-After": "0"}),
            httpx.Response(200, json={"data": []}),
        ]
    )
    transport = GleifThrottledTransport(inner)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get(f"{_API}/lei-records/{_LEI}")
    assert response.status_code == 200
    assert len(seen) == 2  # exactly one retry


async def test_transport_gives_up_after_second_429_and_penalises(
    monkeypatch,
) -> None:
    _enable_throttle(monkeypatch, per_minute=50, max_wait_s=5.0)
    inner, seen = _mock_transport(
        [
            httpx.Response(429, headers={"Retry-After": "0"}),
            httpx.Response(429, headers={"Retry-After": "9"}),
        ]
    )
    transport = GleifThrottledTransport(inner)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get(f"{_API}/lei-records/{_LEI}")
    assert response.status_code == 429  # handed back for the fallback chain
    assert len(seen) == 2  # never a third attempt
    # The second 429's Retry-After went into the shared penalty box: with a
    # short max wait, the next acquire refuses rather than queueing behind it.
    monkeypatch.setenv("OPENCHECK_GLEIF_THROTTLE_MAX_WAIT_S", "0.1")
    get_settings.cache_clear()
    with pytest.raises(GleifRateLimitedError):
        await get_throttle().acquire()


async def test_budget_exhaustion_raises_without_sending(monkeypatch) -> None:
    _enable_throttle(monkeypatch, per_minute=1, max_wait_s=0.1)
    inner, seen = _mock_transport([httpx.Response(200, json={})])
    transport = GleifThrottledTransport(inner)
    async with httpx.AsyncClient(transport=transport) as client:
        assert (await client.get(f"{_API}/lei-records/{_LEI}")).status_code == 200
        with pytest.raises(GleifRateLimitedError):
            await client.get(f"{_API}/lei-records/{_LEI}")
    assert len(seen) == 1  # the refused request was never sent


def test_build_client_installs_wrapper_only_when_enabled(monkeypatch) -> None:
    client = build_client()
    assert not isinstance(client._transport, GleifThrottledTransport)
    _enable_throttle(monkeypatch, per_minute=50, max_wait_s=15.0)
    client = build_client()
    assert isinstance(client._transport, GleifThrottledTransport)


# ---------------------------------------------------------------------------
# Adapter fallback chain — stale cache, snapshot store, 503
# ---------------------------------------------------------------------------


async def test_429_falls_back_to_stale_cache(
    httpx_mock: HTTPXMock, tmp_path: Path
) -> None:
    """Expired cache entries are better than a dead lookup: on 429 the
    adapter re-reads them with the TTL waived and the bundle is served."""
    cache = Cache()
    cache.put(
        f"gleif/lei/{_LEI}",
        {"data": {"id": _LEI, "attributes": {"lei": _LEI, "entity": {
            "legalName": {"name": "SHELL PLC (STALE CACHE)"}}}}},
    )
    cache.put(f"gleif/lei/{_LEI}/direct-parent", {"data": None})
    cache.put(f"gleif/lei/{_LEI}/ultimate-parent", {"data": None})
    cache.put(
        f"gleif/lei/{_LEI}/direct-children-p1-s100",
        {"data": [], "meta": {"pagination": {"total": 0}}},
    )
    for key in (
        f"gleif/lei/{_LEI}",
        f"gleif/lei/{_LEI}/direct-parent",
        f"gleif/lei/{_LEI}/ultimate-parent",
        f"gleif/lei/{_LEI}/direct-children-p1-s100",
    ):
        _age_cache_entry(tmp_path, key, age_days=9)  # past both TTLs

    httpx_mock.add_response(status_code=429, is_reusable=True)  # every live re-fetch is refused

    bundle = await GleifAdapter().fetch(_LEI)
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == (
        "SHELL PLC (STALE CACHE)"
    )
    assert "snapshot_fallback" not in bundle


async def test_429_falls_back_to_entity_store_snapshot(
    httpx_mock: HTTPXMock, tmp_path: Path, monkeypatch
) -> None:
    """No cache at all → the Golden Copy snapshot serves the anchor, with
    provenance resolving as ``snapshot`` dated to the DB's publish date."""
    monkeypatch.setenv(
        "OPENCHECK_ENTITY_PAGES_DB_FILE", str(_entity_store_db(tmp_path))
    )
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    httpx_mock.add_response(status_code=429, is_reusable=True)

    with provenance.recording() as recorder:
        bundle = await GleifAdapter().fetch(_LEI)
    resolved = recorder.resolve()

    assert bundle["snapshot_fallback"] is True
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == "SHELL PLC"
    assert bundle["record"]["attributes"]["registration"]["status"] == "ISSUED"
    # Parent resolved from the store, with its name.
    parent = bundle["direct_parent"]
    assert parent["attributes"]["lei"] == _PARENT_LEI
    assert parent["attributes"]["entity"]["legalName"]["name"] == (
        "SHELL GROUP HOLDINGS"
    )
    # Children come from the store's parent index, exact total included.
    assert bundle["direct_children_total"] == 1
    assert bundle["direct_children"][0]["attributes"]["lei"] == _CHILD_LEI
    # Honest badge: snapshot, dated to the Golden Copy publish date.
    assert resolved.liveness == "snapshot"
    assert resolved.retrieved_at_iso() == "2026-08-03T00:00:00Z"


async def test_429_with_no_fallback_raises_rate_limited(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(status_code=429, is_reusable=True)
    with pytest.raises(GleifRateLimitedError):
        await GleifAdapter().fetch(_LEI)


def test_lookup_returns_friendly_503_when_rate_limited(
    httpx_mock: HTTPXMock,
) -> None:
    """The endpoint contract: a rate-limited anchor with no fallback is a
    503 with retry advice — not the old ``502 GLEIF fetch failed``."""
    httpx_mock.add_response(status_code=429, is_reusable=True)
    response = TestClient(app).get("/lookup", params={"lei": _LEI})
    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "rate-limiting" in detail
    assert "retry" in detail.lower()


# ---------------------------------------------------------------------------
# Phase 144 — serve the snapshot sooner than the full throttle wait
# ---------------------------------------------------------------------------


async def test_snapshot_available_reads_the_store(tmp_path, monkeypatch) -> None:
    assert GleifAdapter._snapshot_available(_LEI) is False  # no store configured
    monkeypatch.setenv(
        "OPENCHECK_ENTITY_PAGES_DB_FILE", str(_entity_store_db(tmp_path))
    )
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    assert GleifAdapter._snapshot_available(_LEI) is True
    assert GleifAdapter._snapshot_available("MISSING00000000000XX") is False


async def test_saturated_budget_serves_snapshot_within_the_bound(
    tmp_path, monkeypatch
) -> None:
    """With a snapshot in the store and the shared budget saturated, the
    anchor is served after ``OPENCHECK_GLEIF_SNAPSHOT_AFTER_S`` — not after
    the throttle's much longer max wait. Phase 143 measured 15–21s anchor
    stalls in production; this pins the cap that removes them."""
    monkeypatch.setenv(
        "OPENCHECK_ENTITY_PAGES_DB_FILE", str(_entity_store_db(tmp_path))
    )
    monkeypatch.setenv("OPENCHECK_GLEIF_SNAPSHOT_AFTER_S", "0.3")
    _enable_throttle(monkeypatch, per_minute=50, max_wait_s=30.0)
    ep.reset_store_for_tests()
    get_throttle().penalise(15.0)  # budget saturated: acquire would block 15s

    start = time.monotonic()
    bundle = await GleifAdapter().fetch(_LEI)
    elapsed = time.monotonic() - start

    assert bundle["snapshot_fallback"] is True
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == "SHELL PLC"
    assert elapsed < 3.0  # the 30s max wait never applied


async def test_fast_live_fetch_wins_over_an_available_snapshot(
    httpx_mock: HTTPXMock, tmp_path, monkeypatch
) -> None:
    """The bound is a ceiling, not a preference: when GLEIF answers promptly
    the anchor stays live even though a snapshot row exists."""
    monkeypatch.setenv(
        "OPENCHECK_ENTITY_PAGES_DB_FILE", str(_entity_store_db(tmp_path))
    )
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}",
        json={"data": {"id": _LEI, "attributes": {"lei": _LEI, "entity": {
            "legalName": {"name": "SHELL PLC (LIVE)"}}}}},
    )
    for path in (
        "direct-parent",
        "direct-parent-reporting-exception",
        "ultimate-parent",
        "ultimate-parent-reporting-exception",
    ):
        httpx_mock.add_response(
            url=f"{_API}/lei-records/{_LEI}/{path}", status_code=404
        )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}/direct-children?page[size]=100&page[number]=1",
        json={"data": [], "meta": {"pagination": {"total": 0}}},
    )

    bundle = await GleifAdapter().fetch(_LEI)

    assert "snapshot_fallback" not in bundle
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == (
        "SHELL PLC (LIVE)"
    )
