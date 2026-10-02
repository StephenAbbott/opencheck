"""The suite runs offline (Phase 266).

Two halves: the lifespan's boot downloads are off under test
(``OPENCHECK_WARM_CACHES_ON_START=0``, set in ``conftest.py``), and a socket
guard (``tests/_network_guard.py``) fails any test that reaches past the
loopback interface. These tests pin both, and prove the guard can fail.
"""

from __future__ import annotations

import asyncio
import importlib
import socket

import pytest
from fastapi.testclient import TestClient

from opencheck.config import get_settings
from tests import _network_guard

# Every function ``app._warm_caches_background`` calls, as (module, name).
# The test below fails if the warm-up grows a step this list does not name.
WARM_UPS: dict[str, tuple[str, str]] = {
    "climatetrace": ("opencheck.sources.climatetrace", "warm_caches"),
    "securities": ("opencheck.securities", "warm_index"),
    "entity_pages": ("opencheck.entity_pages", "warm_entity_pages_db"),
    "apr_serbia": ("opencheck.sources.apr_serbia", "warm_index"),
    "asp_moldova": ("opencheck.sources.asp_moldova", "warm_index"),
    "onrc_romania": ("opencheck.sources.onrc_romania", "warm_index"),
    "chilecompra": ("opencheck.sources.chilecompra", "warm_index"),
    "meip": ("opencheck.meip", "warm_meip_db"),
    "psc_graph": ("opencheck.psc_graph", "warm_psc_graph_db"),
    "isin_index": ("opencheck.isin_index", "warm_index"),
}


def stub_every_warm_up(
    monkeypatch: pytest.MonkeyPatch, *, keep: tuple[str, ...] = (), calls: list[str] | None = None
) -> None:
    """Replace every boot download with a no-op that records its name."""
    for key, (module, name) in WARM_UPS.items():
        if key in keep:
            continue

        def _stub(_key: str = key) -> dict:
            if calls is not None:
                calls.append(_key)
            return {}

        monkeypatch.setattr(importlib.import_module(module), name, _stub)


@pytest.fixture
def warm_up_setting(monkeypatch: pytest.MonkeyPatch):
    def _set(value: str) -> None:
        monkeypatch.setenv("OPENCHECK_WARM_CACHES_ON_START", value)
        get_settings.cache_clear()

    yield _set
    get_settings.cache_clear()


def test_the_suite_runs_with_the_warm_up_off() -> None:
    assert get_settings().warm_caches_on_start is False


def test_the_warm_up_is_on_by_default_outside_the_suite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENCHECK_WARM_CACHES_ON_START", raising=False)
    get_settings.cache_clear()
    try:
        assert get_settings().warm_caches_on_start is True
    finally:
        get_settings.cache_clear()


def test_the_lifespan_skips_the_warm_up_when_off(monkeypatch, warm_up_setting) -> None:
    import opencheck.app as app_module

    started: list[str] = []

    async def _spy() -> None:
        started.append("warm-up")

    monkeypatch.setattr(app_module, "_warm_caches_background", _spy)
    warm_up_setting("0")
    with TestClient(app_module.app) as client:
        assert client.get("/health").status_code == 200
    assert started == []


def test_the_lifespan_runs_every_warm_up_when_on(monkeypatch, warm_up_setting) -> None:
    import opencheck.app as app_module

    calls: list[str] = []
    stub_every_warm_up(monkeypatch, calls=calls)
    warm_up_setting("1")

    asyncio.run(app_module._warm_caches_background())
    assert calls == list(WARM_UPS), (
        "app._warm_caches_background calls a step WARM_UPS does not name, or "
        "in another order — add it here so the suite can stub it"
    )


# ---------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------


def test_the_guard_is_installed() -> None:
    assert _network_guard.installed()


def test_the_guard_refuses_a_public_address_and_records_it() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        with pytest.raises(_network_guard.NetworkBlocked):
            sock.connect(("192.0.2.1", 443))  # TEST-NET-1, never routable
    finally:
        sock.close()
    attempts = _network_guard.drain()  # drained so this test itself passes
    assert attempts and "192.0.2.1" in attempts[0]


def test_the_guard_refuses_dns_for_a_public_name() -> None:
    with pytest.raises(_network_guard.NetworkBlocked):
        socket.getaddrinfo("api.gleif.org", 443)
    assert any("api.gleif.org" in a for a in _network_guard.drain())


def test_a_refused_request_is_an_oserror_so_code_takes_its_outage_path() -> None:
    import httpx

    with pytest.raises(httpx.ConnectError):
        httpx.get("https://api.gleif.org/api/v1/lei-records?page[size]=1", timeout=2)
    assert _network_guard.drain()


def test_the_guard_allows_loopback() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect(server.getsockname())
        assert socket.getaddrinfo("localhost", 80)
    finally:
        client.close()
        server.close()
    assert _network_guard.drain() == []
