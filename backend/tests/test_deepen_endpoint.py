"""Integration tests for /deepen across Phase 2 adapters."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck.app import app
from opencheck.config import get_settings

_GLEIF = "https://api.gleif.org/api/v1"
_OS = "https://api.opensanctions.org"
_AL = "https://search.openaleph.org/api/2"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_deepen_stub_path_has_license_block() -> None:
    """Even the Phase 0 stub path returns the adapter's static license."""
    client = TestClient(app)
    r = client.get("/deepen", params={"source": "opensanctions", "hit_id": "NK-stub"})
    assert r.status_code == 200
    body = r.json()
    assert body["license"] == "CC-BY-NC-4.0"
    # OpenSanctions is NC — warning must be surfaced even without fetching live.
    assert body["license_notice"] is not None
    assert "CC-BY-NC-4.0" in body["license_notice"]


def test_deepen_gleif_live_maps_to_bods(
    monkeypatch, httpx_mock: HTTPXMock
) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    lei = "213800LBDB8WB3QGVN21"
    httpx_mock.add_response(
        url=f"{_GLEIF}/lei-records/{lei}",
        json={
            "data": {
                "id": lei,
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": "BP P.L.C."},
                        "jurisdiction": "GB",
                    },
                },
            }
        },
    )
    httpx_mock.add_response(
        url=f"{_GLEIF}/lei-records/{lei}/direct-parent",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_GLEIF}/lei-records/{lei}/direct-parent-reporting-exception",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_GLEIF}/lei-records/{lei}/ultimate-parent",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_GLEIF}/lei-records/{lei}/ultimate-parent-reporting-exception",
        status_code=404,
    )

    client = TestClient(app)
    r = client.get("/deepen", params={"source": "gleif", "hit_id": lei})
    assert r.status_code == 200
    body = r.json()
    assert body["license"] == "CC0-1.0"
    assert body["license_notice"] is None
    assert len(body["bods"]) == 1
    assert body["bods"][0]["recordType"] == "entity"
    assert body["bods_issues"] == []


def test_deepen_opensanctions_live_flags_nc_license(
    monkeypatch, httpx_mock: HTTPXMock
) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    get_settings.cache_clear()

    httpx_mock.add_response(
        url=f"{_OS}/entities/NK-rosneft",
        json={
            "id": "NK-rosneft",
            "schema": "Company",
            "caption": "Rosneft Oil Company",
            "properties": {"name": ["Rosneft Oil Company"]},
        },
    )

    client = TestClient(app)
    r = client.get(
        "/deepen", params={"source": "opensanctions", "hit_id": "NK-rosneft"}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["license"] == "CC-BY-NC-4.0"
    assert "CC-BY-NC-4.0" in body["license_notice"]
    assert len(body["bods"]) == 1
    assert body["bods"][0]["recordType"] == "entity"


def test_deepen_rejects_disabled_openaleph_source() -> None:
    """OpenAleph was removed from the registry while the LEI flow is
    the supported entry point — /deepen should 404 cleanly rather than
    leak the adapter's response."""
    client = TestClient(app)
    r = client.get("/deepen", params={"source": "openaleph", "hit_id": "aleph-nc"})
    assert r.status_code == 404
    assert "unknown source" in r.json()["detail"].lower()
