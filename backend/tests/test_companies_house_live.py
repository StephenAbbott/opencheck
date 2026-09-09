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
        url=f"{_API}/company/{number}/officers?items_per_page=100&start_index=0",
        json={"items": []},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control?items_per_page=100&start_index=0",
        json={"items": []},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control-statements?items_per_page=100&start_index=0",
        json={"items": [{"statement": "no-individual-or-entity-with-signficant-control",
                         "notified_on": "2016-04-06", "etag": "s1"}]},
    )

    adapter = CompaniesHouseAdapter()
    bundle = await adapter.fetch(number)

    assert bundle["company_number"] == number
    assert bundle["profile"]["company_name"] == "BP P.L.C."
    assert "officers" in bundle
    assert "pscs" in bundle
    assert bundle["psc_statements"]["items"][0]["statement"] == (
        "no-individual-or-entity-with-signficant-control"
    )


async def test_fetch_officer_bundle_returns_appointments(
    httpx_mock: HTTPXMock,
) -> None:
    """Officer ids dispatch to /officers/{id}/appointments."""
    officer_id = "zS_RY9pRYlJ9XwGJEOFtkJgrf8s"
    httpx_mock.add_response(
        url=f"{_API}/officers/{officer_id}/appointments?items_per_page=100&start_index=0",
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


# ---------------------------------------------------------------------------
# Phase 192: list endpoints are paginated
#
# Companies House answers /officers with 35 items when asked for no page
# size. Lloyds Bank PLC (00002065) has 116 officers on file, of whom 16 are
# serving, and the report said "35 officers listed" — the page size, read as
# a fact about the bank. The numbers in these tests are that company's.
# ---------------------------------------------------------------------------

_PAGE = "?items_per_page=100&start_index=0"


def _officer(i: int, *, resigned: bool) -> dict:
    return {
        "name": f"OFFICER, Number {i}",
        "officer_role": "director",
        "appointed_on": "2001-01-01",
        **({"resigned_on": "2010-01-01"} if resigned else {}),
    }


def _company_pages(httpx_mock: HTTPXMock, number: str, officer_pages: list[dict]) -> None:
    """Mock a whole company: profile, the given officer pages, empty PSCs."""
    httpx_mock.add_response(
        url=f"{_API}/company/{number}", json={"company_number": number, "company_name": "LLOYDS"}
    )
    for start, page in zip(range(0, len(officer_pages) * 100, 100), officer_pages):
        httpx_mock.add_response(
            url=f"{_API}/company/{number}/officers?items_per_page=100&start_index={start}",
            json=page,
        )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control{_PAGE}",
        json={"items": [], "total_results": 0},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control-statements{_PAGE}",
        status_code=404,
        json={"errors": [{"error": "not-found"}]},
    )


async def test_officers_are_fetched_past_the_registers_default_page(
    httpx_mock: HTTPXMock,
) -> None:
    """116 officers on file come back as 116, not as one page of them."""
    all_officers = [_officer(i, resigned=i >= 16) for i in range(116)]
    _company_pages(
        httpx_mock,
        "00002065",
        [
            {"items": all_officers[:100], "total_results": 116, "active_count": 16},
            {"items": all_officers[100:], "total_results": 116, "active_count": 16},
        ],
    )

    bundle = await CompaniesHouseAdapter().fetch("00002065")

    assert len(bundle["officers"]["items"]) == 116
    # The merged payload keeps the register's own envelope and says it is whole.
    assert bundle["officers"]["total_results"] == 116
    assert bundle["officers"]["active_count"] == 16
    assert bundle["officers"]["items_per_page"] == 116
    assert bundle["officers"]["start_index"] == 0


async def test_a_list_that_fits_one_page_costs_one_call(httpx_mock: HTTPXMock) -> None:
    """Pagination is not a second call: a short list ends on its own page."""
    _company_pages(
        httpx_mock, "00102498", [{"items": [_officer(0, resigned=False)], "total_results": 1}]
    )

    await CompaniesHouseAdapter().fetch("00102498")

    officer_calls = [r for r in httpx_mock.get_requests() if r.url.path.endswith("/officers")]
    assert len(officer_calls) == 1


async def test_a_truncated_cached_page_is_not_served(
    httpx_mock: HTTPXMock, tmp_path
) -> None:
    """A record cached before this phase holds Companies House's own 35-item
    page. It says so — ``total_results`` is larger than the list it carries —
    so it is re-fetched whole rather than served, which is what spares the
    namespace an expiry that would re-spend every other register call."""
    from opencheck.cache import Cache

    Cache().put(
        "companies_house/company/00002065/officers",
        {"items": [_officer(i, resigned=False) for i in range(35)], "total_results": 116},
    )
    all_officers = [_officer(i, resigned=i >= 16) for i in range(116)]
    _company_pages(
        httpx_mock,
        "00002065",
        [
            {"items": all_officers[:100], "total_results": 116},
            {"items": all_officers[100:], "total_results": 116},
        ],
    )

    bundle = await CompaniesHouseAdapter().fetch("00002065")

    assert len(bundle["officers"]["items"]) == 116


async def test_a_list_longer_than_the_cap_says_it_was_cut(
    httpx_mock: HTTPXMock, monkeypatch
) -> None:
    """The bound exists so one lookup cannot crawl an unbounded list. A list
    cut by it is marked, because an unmarked short list is the defect."""
    monkeypatch.setattr("opencheck.sources.companies_house._MAX_LIST_PAGES", 2)
    page = {"items": [_officer(i, resigned=False) for i in range(100)], "total_results": 5000}
    _company_pages(httpx_mock, "00002065", [page, page])

    bundle = await CompaniesHouseAdapter().fetch("00002065")

    assert len(bundle["officers"]["items"]) == 200
    assert bundle["officers"]["opencheck_page_cap_reached"] is True
