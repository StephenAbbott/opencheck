"""Phase 258 — where the shared GLEIF budget goes (opencheck/gleifstats.py).

The Golden Copy ticket's gate asked whether ``/isins`` was "a material share
of the remaining GLEIF calls" and nothing could answer it: no counter knew
which route or which GLEIF endpoint a request was for, and a Phase 234
refusal left no trace. These pin the counting at the one place every GLEIF
request passes — the throttled transport — and that nothing but closed
vocabularies can reach a counter.
"""

from __future__ import annotations

import json

import httpx
import pytest

from opencheck import gleif_throttle as gt
from opencheck import gleifstats
from opencheck.config import get_settings
from opencheck.routers import health

LEI = "21380068P1DRHMJ8KU70"


@pytest.mark.parametrize(
    ("path", "endpoint"),
    [
        (f"/api/v1/lei-records/{LEI}/isins", "isins"),
        (f"/api/v1/lei-records/{LEI}", "lei_record"),
        (f"/api/v1/lei-records/{LEI}/direct-parent", "parent"),
        (f"/api/v1/lei-records/{LEI}/ultimate-parent-reporting-exception", "parent_exception"),
        (f"/api/v1/lei-records/{LEI}/direct-parent-relationship", "parent_relationship"),
        (f"/api/v1/lei-records/{LEI}/ultimate-children", "children"),
        (f"/api/v1/lei-records/{LEI}/direct-child-relationships", "child_relationships"),
        (f"/api/v1/lei-records/{LEI}/field-modifications", "field_modifications"),
        ("/api/v1/lei-records", "search"),
        ("/api/v1/autocompletions", "search"),
        ("/api/v1/registration-authorities", "other"),
    ],
)
def test_endpoint_classification(path, endpoint):
    assert gleifstats.endpoint_for(path) == endpoint


@pytest.mark.parametrize(
    ("path", "route"),
    [
        ("/securities", "securities"),
        ("/subsidiaries", "subsidiaries"),
        ("/subsidiaries/declared", "subsidiaries"),
        (f"/og/{LEI}.png", "share"),
        (f"/share/{LEI}", "share"),
        ("/resolve-national-id", "national_id"),
        ("/search", "search"),
        ("/history", "history"),
        (f"/entity/{LEI}", "entity"),
        ("/lookup-stream", "stream"),
        ("/lookup", "api"),
        ("/mcp", "mcp"),
        ("/sources", "other"),
        (None, "other"),
    ],
)
def test_route_classification(path, route):
    assert gleifstats.route_for_path(path) == route
    assert route in gleifstats.ROUTES


class _Inner(httpx.AsyncBaseTransport):
    def __init__(self, statuses: list[int]) -> None:
        self.statuses = list(statuses)
        self.sent = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.sent += 1
        status = self.statuses.pop(0) if self.statuses else 200
        return httpx.Response(status, headers={"Retry-After": "0"}, request=request)


@pytest.fixture
def throttle_on(monkeypatch):
    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "5")
    monkeypatch.setenv("OPENCHECK_GLEIF_LOOKUP_RESERVE", "2")
    monkeypatch.setenv("OPENCHECK_GLEIF_THROTTLE_MAX_WAIT_S", "0")
    get_settings.cache_clear()
    gt.reset_throttle_for_tests()
    yield
    gt.reset_throttle_for_tests()
    get_settings.cache_clear()


async def _get(transport, path: str) -> httpx.Response:
    async with httpx.AsyncClient(transport=transport) as client:
        return await client.get(f"https://api.gleif.org{path}")


async def test_sent_and_429_are_counted_by_route_and_endpoint(throttle_on):
    transport = gt.GleifThrottledTransport(_Inner([200, 429, 200]))
    with gleifstats.route_scope("securities"):
        await _get(transport, f"/api/v1/lei-records/{LEI}/isins")
    with gleifstats.route_scope("stream"):
        await _get(transport, f"/api/v1/lei-records/{LEI}")  # 429, then retried → 200
    s = gleifstats.stats()
    assert s["by_route"]["securities"]["sent"] == 1
    assert s["by_route"]["stream"]["sent"] == 2
    assert s["by_route"]["stream"]["http_429"] == 1
    assert s["by_endpoint"]["isins"]["sent"] == 1
    assert s["sent_total"] == 3
    assert s["isins_share"] == round(1 / 3, 4)
    assert s["sent_by_route_endpoint"]["securities|isins"] == 1


async def test_a_reserve_refusal_is_counted_as_held_for_lookups(throttle_on):
    transport = gt.GleifThrottledTransport(_Inner([]))
    for _ in range(3):
        await _get(transport, f"/api/v1/lei-records/{LEI}")
    with gleifstats.route_scope("securities"), gt.discretionary():
        with pytest.raises(gt.GleifRateLimitedError):
            await _get(transport, f"/api/v1/lei-records/{LEI}/isins")
    s = gleifstats.stats()
    assert s["by_route"]["securities"]["refused_held_for_lookups"] == 1
    assert s["by_route"]["securities"]["sent"] == 0
    assert s["refused_total"] == 1


async def test_a_refusal_after_a_429_is_counted_as_rate_limited(throttle_on):
    gt.get_throttle().penalise(30)
    transport = gt.GleifThrottledTransport(_Inner([]))
    with gleifstats.route_scope("subsidiaries"), gt.discretionary():
        with pytest.raises(gt.GleifRateLimitedError):
            await _get(transport, f"/api/v1/lei-records/{LEI}/direct-children")
    assert gleifstats.stats()["by_route"]["subsidiaries"]["refused_rate_limited"] == 1


async def test_no_request_is_server_work(throttle_on):
    transport = gt.GleifThrottledTransport(_Inner([]))
    await _get(transport, f"/api/v1/lei-records/{LEI}")
    assert gleifstats.stats()["by_route"]["server"]["sent"] == 1


async def test_other_hosts_are_not_counted(throttle_on):
    transport = gt.GleifThrottledTransport(_Inner([]))
    async with httpx.AsyncClient(transport=transport) as client:
        await client.get("https://api.openfigi.com/v3/mapping")
    assert gleifstats.stats()["sent_total"] == 0


def test_unknown_names_cannot_create_keys():
    gleifstats.record_call("an LEI 21380068P1DRHMJ8KU70", "sent", route="Shell PLC")
    gleifstats.record_call("isins", "not-an-outcome")
    gleifstats.record_securities("file")
    gleifstats.record_securities("Shell PLC")
    s = gleifstats.stats()
    assert set(s["by_route"]) <= gleifstats.ROUTES
    assert set(s["by_endpoint"]) <= gleifstats.ENDPOINT_NAMES
    assert set(s["securities_served"]) == gleifstats.SECURITIES_SERVED
    assert s["securities_served"]["file"] == 1
    dumped = json.dumps(s)
    assert LEI not in dumped and "Shell" not in dumped


async def test_signalstats_serves_the_gleif_section():
    gleifstats.record_securities("cache")
    body = json.loads((await health.signal_stats()).body)
    assert body["gleif"]["securities_served"]["cache"] == 1
    assert "isin_index" in body["gleif"]


def test_the_middleware_sets_the_route(monkeypatch):
    """End to end: a request to /securities spends its GLEIF call as
    'securities', whatever the pipeline caller kinds say."""
    from fastapi.testclient import TestClient

    from opencheck import securities as svc
    from opencheck.app import app

    seen: list[str] = []

    async def fake_assemble(lei, *, page=1, page_size=20):
        seen.append(gleifstats.current_route())
        return {
            "lei": lei, "available": False, "total": 0, "page": page,
            "page_size": page_size, "securities": [], "sanctioned": [],
            "sources": [], "license_notices": [],
        }

    monkeypatch.setattr("opencheck.routers.securities.assemble_securities", fake_assemble)
    with TestClient(app) as tc:
        assert tc.get("/securities", params={"lei": LEI}).status_code == 200
    assert seen == ["securities"]
    assert svc  # imported for the patch target's module to be loaded
