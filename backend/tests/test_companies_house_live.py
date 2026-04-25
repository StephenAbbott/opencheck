"""Live Companies House adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.companies_house import CompaniesHouseAdapter

_API = "https://api.company-information.service.gov.uk"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    """Force the adapter into live mode and isolate the cache per test."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_entity_search_maps_items(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/companies?q=bp&items_per_page=10",
        json={
            "items": [
                {
                    "company_number": "00102498",
                    "title": "BP P.L.C.",
                    "company_status": "active",
                    "address_snippet": "1 St James's Square, London, SW1Y 4PD",
                }
            ],
            "total_results": 1,
        },
    )

    adapter = CompaniesHouseAdapter()
    hits = await adapter.search("bp", SearchKind.ENTITY)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.hit_id == "00102498"
    assert hit.name == "BP P.L.C."
    assert hit.identifiers["gb_coh"] == "00102498"
    assert "active" in hit.summary


async def test_person_search_maps_officers(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/officers?q=smith&items_per_page=10",
        json={
            "items": [
                {
                    "title": "Jane SMITH",
                    "appointment_count": 3,
                    "date_of_birth": {"year": 1975, "month": 8},
                    "links": {"self": "/officers/abc123/appointments"},
                }
            ]
        },
    )

    adapter = CompaniesHouseAdapter()
    hits = await adapter.search("smith", SearchKind.PERSON)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name == "Jane SMITH"
    assert "3 appointment" in hit.summary
    assert "1975" in hit.summary
    assert hit.hit_id == "abc123"


async def test_search_hits_cache_on_second_call(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/companies?q=bp&items_per_page=10",
        json={"items": []},
    )

    adapter = CompaniesHouseAdapter()
    await adapter.search("bp", SearchKind.ENTITY)
    # Second call must not hit HTTP — pytest-httpx would fail if it did
    # (only one response was registered).
    await adapter.search("bp", SearchKind.ENTITY)


async def test_fetch_company_bundle_returns_profile_officers_pscs(
    httpx_mock: HTTPXMock,
) -> None:
    number = "00102498"
    httpx_mock.add_response(
        url=f"{_API}/company/{number}",
        json={"company_number": number, "company_name": "BP P.L.C."},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/officers",
        json={"items": []},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control",
        json={"items": []},
    )

    adapter = CompaniesHouseAdapter()
    bundle = await adapter.fetch(number)

    assert bundle["company_number"] == number
    assert bundle["profile"]["company_name"] == "BP P.L.C."
    assert "officers" in bundle
    assert "pscs" in bundle


async def test_fetch_officer_bundle_returns_appointments(
    httpx_mock: HTTPXMock,
) -> None:
    """Officer ids dispatch to /officers/{id}/appointments."""
    officer_id = "zS_RY9pRYlJ9XwGJEOFtkJgrf8s"
    httpx_mock.add_response(
        url=f"{_API}/officers/{officer_id}/appointments",
        json={
            "name": "Jane SMITH",
            "date_of_birth": {"year": 1975, "month": 8},
            "items": [
                {
                    "appointed_to": {
                        "company_name": "ACME LTD",
                        "company_number": "00102498",
                    },
                    "officer_role": "director",
                    "appointed_on": "2020-01-15",
                }
            ],
        },
    )

    adapter = CompaniesHouseAdapter()
    bundle = await adapter.fetch(officer_id)

    assert bundle["officer_id"] == officer_id
    assert bundle["appointments"]["name"] == "Jane SMITH"
    assert len(bundle["appointments"]["items"]) == 1


async def test_stub_path_when_allow_live_false(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = CompaniesHouseAdapter()
    hits = await adapter.search("anything", SearchKind.ENTITY)

    assert len(hits) == 1
    assert hits[0].is_stub is True
