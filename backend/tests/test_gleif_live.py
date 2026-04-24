"""Live GLEIF adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.gleif import GleifAdapter

_API = "https://api.gleif.org/api/v1"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_entity_search_maps_lei_records(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/lei-records?filter[fulltext]=bp&page[size]=10",
        json={
            "data": [
                {
                    "type": "lei-records",
                    "id": "213800LBDB8WB3QGVN21",
                    "attributes": {
                        "lei": "213800LBDB8WB3QGVN21",
                        "entity": {
                            "legalName": {"name": "BP P.L.C."},
                            "jurisdiction": "GB",
                            "status": "ACTIVE",
                            "registeredAs": "00102498",
                        },
                    },
                }
            ]
        },
    )

    adapter = GleifAdapter()
    hits = await adapter.search("bp", SearchKind.ENTITY)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name == "BP P.L.C."
    assert hit.hit_id == "213800LBDB8WB3QGVN21"
    assert hit.identifiers["lei"] == "213800LBDB8WB3QGVN21"
    assert hit.identifiers["registered_as_gb"] == "00102498"


async def test_fetch_lei_bundle_with_parents(httpx_mock: HTTPXMock) -> None:
    lei = "213800LBDB8WB3QGVN21"
    parent_lei = "PARENTXXXXXXXXXXXXXX"

    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}",
        json={
            "data": {
                "id": lei,
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": "BP P.L.C."},
                        "jurisdiction": "GB",
                        "legalAddress": {
                            "addressLines": ["1 St James's Square"],
                            "city": "London",
                            "postalCode": "SW1Y 4PD",
                            "country": "GB",
                        },
                    },
                },
            }
        },
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/direct-parent",
        json={
            "data": {
                "id": parent_lei,
                "attributes": {
                    "lei": parent_lei,
                    "entity": {
                        "legalName": {"name": "BP Group Holdings"},
                        "jurisdiction": "GB",
                    },
                },
            }
        },
    )
    # Ultimate parent 404s, and the exception probe also 404s — this is
    # the "no parent and no exception declared" case.
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/ultimate-parent",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/ultimate-parent-reporting-exception",
        status_code=404,
    )

    adapter = GleifAdapter()
    bundle = await adapter.fetch(lei)

    assert bundle["lei"] == lei
    assert bundle["record"]["id"] == lei
    assert bundle["direct_parent"]["id"] == parent_lei
    assert bundle["ultimate_parent"] is None
    assert bundle["ultimate_parent_exception"] is None


async def test_fetch_surfaces_reporting_exception(httpx_mock: HTTPXMock) -> None:
    """When the direct-parent endpoint 404s, fall back to the exception endpoint."""
    lei = "213800LBDB8WB3QGVN21"

    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}",
        json={
            "data": {
                "id": lei,
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": "Family Trust Holdings"},
                        "jurisdiction": "GB",
                    },
                },
            }
        },
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/direct-parent",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/direct-parent-reporting-exception",
        json={
            "data": {
                "type": "reporting-exceptions",
                "attributes": {
                    "lei": lei,
                    "exceptionCategory": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                    "exceptionReason": "NATURAL_PERSONS",
                },
            }
        },
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/ultimate-parent",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/ultimate-parent-reporting-exception",
        status_code=404,
    )

    adapter = GleifAdapter()
    bundle = await adapter.fetch(lei)

    assert bundle["direct_parent"] is None
    assert bundle["direct_parent_exception"]["attributes"]["exceptionReason"] == (
        "NATURAL_PERSONS"
    )


async def test_stub_path_when_allow_live_false(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = GleifAdapter()
    hits = await adapter.search("anything", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True


async def test_gleif_rejects_person_search() -> None:
    adapter = GleifAdapter()
    hits = await adapter.search("Alice", SearchKind.PERSON)
    assert hits == []
