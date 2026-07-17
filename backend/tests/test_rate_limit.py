"""Per-IP rate limiting (opencheck/ratelimit.py).

The suite-wide conftest disables the limiter (OPENCHECK_RATE_LIMIT_ENABLED=0),
so these tests re-enable it per-fixture by flipping ``limiter.enabled`` and
shrink the budgets via monkeypatched env vars — the tier callables re-read
``get_settings()`` on every request, so ``get_settings.cache_clear()`` makes
tiny test budgets take effect immediately.

Endpoints used here are chosen to fail (or answer) fast without any network:
``/license-matrix`` is a pure dict, ``/narrative`` 404s immediately while
narratives are disabled, ``/subsidiaries`` 400s on a malformed LEI. Errors
deliberately count against the budget.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.requests import Request

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.ratelimit import client_ip, limiter


def _mk_request(headers: dict[str, str], client_host: str | None = "203.0.113.9") -> Request:
    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "client": (client_host, 12345) if client_host else None,
    }
    return Request(scope)


# ---------------------------------------------------------------------------
# key_func — client IP resolution behind Render's proxy chain
# ---------------------------------------------------------------------------


def test_client_ip_prefers_true_client_ip() -> None:
    req = _mk_request(
        {"true-client-ip": "198.51.100.7", "x-forwarded-for": "192.0.2.1, 10.0.0.1"}
    )
    assert client_ip(req) == "198.51.100.7"


def test_client_ip_falls_back_to_first_forwarded_for() -> None:
    req = _mk_request({"x-forwarded-for": " 192.0.2.1 , 10.0.0.1, 10.0.0.2"})
    assert client_ip(req) == "192.0.2.1"


def test_client_ip_falls_back_to_socket_peer() -> None:
    assert client_ip(_mk_request({})) == "203.0.113.9"


def test_client_ip_never_empty() -> None:
    assert client_ip(_mk_request({"x-forwarded-for": " , "}, client_host=None)) == "unknown"


# ---------------------------------------------------------------------------
# Enforcement — tiny budgets, limiter enabled per-fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def limited_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """TestClient with the limiter ON and tiny per-tier budgets."""
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_DEFAULT", "3/minute")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_LOOKUP", "2/minute")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_HEAVY", "1/minute")
    get_settings.cache_clear()
    limiter.reset()
    limiter.enabled = True
    try:
        yield TestClient(app)
    finally:
        limiter.enabled = False
        limiter.reset()
        get_settings.cache_clear()


def test_suite_default_is_disabled() -> None:
    """conftest.py turns the limiter off for the rest of the suite — pinned
    here because endpoint functions are called directly (no Request) in other
    test files, which only works while the slowapi wrapper is a pass-through."""
    assert limiter.enabled is False


def test_default_tier_enforced_with_retry_after(limited_client: TestClient) -> None:
    for i in range(3):
        r = limited_client.get("/license-matrix")
        assert r.status_code == 200, f"request {i + 1} should be within budget"
    r = limited_client.get("/license-matrix")
    assert r.status_code == 429
    assert "Rate limit exceeded" in r.json()["detail"]
    assert "Retry-After" in r.headers
    assert r.headers["X-RateLimit-Limit"] == "3"


def test_per_ip_isolation_via_forwarded_for(limited_client: TestClient) -> None:
    """Exhausting one client's budget must not affect another client."""
    a = {"X-Forwarded-For": "192.0.2.1"}
    b = {"X-Forwarded-For": "192.0.2.2"}
    for _ in range(3):
        assert limited_client.get("/license-matrix", headers=a).status_code == 200
    assert limited_client.get("/license-matrix", headers=a).status_code == 429
    assert limited_client.get("/license-matrix", headers=b).status_code == 200


def test_true_client_ip_beats_forwarded_for(limited_client: TestClient) -> None:
    """Same X-Forwarded-For, different True-Client-IP → separate buckets."""
    a = {"True-Client-IP": "198.51.100.1", "X-Forwarded-For": "192.0.2.9"}
    b = {"True-Client-IP": "198.51.100.2", "X-Forwarded-For": "192.0.2.9"}
    for _ in range(3):
        assert limited_client.get("/license-matrix", headers=a).status_code == 200
    assert limited_client.get("/license-matrix", headers=a).status_code == 429
    assert limited_client.get("/license-matrix", headers=b).status_code == 200


def test_heavy_tier_and_errors_count(limited_client: TestClient) -> None:
    """/narrative sits on the heavy tier (1/minute here) and its immediate
    error (unconfigured in tests: 404 disabled / 503 no API key) still spends
    budget — garbage requests are exactly the abuse the limiter guards
    against."""
    r = limited_client.get("/narrative", params={"lei": "5493001KJTIIGC8Y1R12"})
    assert r.status_code in (404, 503)  # unconfigured — but the request counted
    r = limited_client.get("/narrative", params={"lei": "5493001KJTIIGC8Y1R12"})
    assert r.status_code == 429


def test_tiers_are_independent(limited_client: TestClient) -> None:
    """Spending the default tier budget must not touch another route's bucket."""
    for _ in range(3):
        assert limited_client.get("/license-matrix").status_code == 200
    assert limited_client.get("/license-matrix").status_code == 429
    # Different route, own bucket (default tier, 400s fast on a bad LEI):
    r = limited_client.get("/subsidiaries", params={"lei": "nope"})
    assert r.status_code == 400


def test_health_and_sources_exempt(limited_client: TestClient) -> None:
    for _ in range(10):  # far beyond every budget above
        assert limited_client.get("/health").status_code == 200
        assert limited_client.get("/sources").status_code == 200


# ---------------------------------------------------------------------------
# Coverage — every public opencheck route must carry a tier decorator
# ---------------------------------------------------------------------------

# Deliberately unlimited routes (cheap, probe-style, or infra):
_EXEMPT_PATHS = {
    "/health",  # Render's health check probes this continuously
    "/sources",  # static inventory, backs the frontend footer
    "/.well-known/mcp.json",  # ARD discovery descriptor (static dict)
}


def test_every_public_route_has_a_rate_limit() -> None:
    """New routes must opt into a tier (or be added to _EXEMPT_PATHS with a
    reason). Guards against silently shipping an unlimited endpoint."""
    marked: set[str] = set(
        limiter._Limiter__marked_for_limiting  # type: ignore[attr-defined]
    )
    missing = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue  # mounted MCP / starlette internals
        endpoint = route.endpoint
        if not endpoint.__module__.startswith("opencheck"):
            continue  # FastAPI's own docs/openapi routes
        if route.path in _EXEMPT_PATHS:
            continue
        name = f"{endpoint.__module__}.{endpoint.__name__}"
        if name not in marked:
            missing.append(route.path)
    assert not missing, f"routes without @limiter.limit(<tier>): {missing}"
