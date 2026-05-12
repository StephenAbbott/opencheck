"""Live OpenAleph adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.openaleph import OpenAlephAdapter

_API = "https://search.openaleph.org/api/2"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_entity_search_maps_results(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities?q=acme&filter:schema=LegalEntity&limit=10",
        json={
            "results": [
                {
                    "id": "aleph-123",
                    "schema": "Company",
                    "properties": {
                        "name": ["Acme Holdings"],
                        "leiCode": ["LEI0000000000000ACME"],
                    },
                    "collection": {
                        "id": 42,
                        "foreign_id": "icij-leaks",
                        "label": "ICIJ leaks",
                    },
                }
            ]
        },
    )

    adapter = OpenAlephAdapter()
    hits = await adapter.search("acme", SearchKind.ENTITY)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name == "Acme Holdings"
    assert hit.hit_id == "aleph-123"
    assert hit.identifiers["lei"] == "LEI0000000000000ACME"
    assert "ICIJ leaks" in hit.summary


async def test_fetch_pulls_collection_metadata(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities/aleph-123",
        json={
            "id": "aleph-123",
            "schema": "Company",
            "properties": {"name": ["Acme Holdings"]},
            "collection": {"id": "42"},
        },
    )
    httpx_mock.add_response(
        url=f"{_API}/collections/42",
        json={"id": 42, "label": "ICIJ leaks", "license": "CC BY-NC 4.0"},
    )

    adapter = OpenAlephAdapter()
    bundle = await adapter.fetch("aleph-123")
    assert bundle["entity"]["id"] == "aleph-123"
    assert bundle["collection"]["license"] == "CC BY-NC 4.0"


async def test_auth_header_sent_when_key_set(
    httpx_mock: HTTPXMock, monkeypatch
) -> None:
    monkeypatch.setenv("OPENALEPH_API_KEY", "secret")
    get_settings.cache_clear()

    httpx_mock.add_response(
        url=f"{_API}/entities?q=acme&filter:schema=LegalEntity&limit=10",
        match_headers={"Authorization": "ApiKey secret"},
        json={"results": []},
    )

    adapter = OpenAlephAdapter()
    await adapter.search("acme", SearchKind.ENTITY)


async def test_stub_path_when_allow_live_false(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = OpenAlephAdapter()
    hits = await adapter.search("anything", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True


# ---------------------------------------------------------------------------
# fetch_by_lei
# ---------------------------------------------------------------------------

_ERICSSON_ENTITY = {
    "id": "aleph-ericsson-001",
    "schema": "Company",
    "caption": "Ericsson AB",
    "properties": {
        "name": ["Ericsson AB"],
        "leiCode": ["549300MLH00Y3BN4HD49"],
        "jurisdiction": ["se"],
        "registrationNumber": ["556056-6258"],
    },
    "collection": {"id": 7, "foreign_id": "orbis", "label": "Bureau van Dijk Orbis"},
}


async def test_fetch_by_lei_returns_hits(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.leiCode=549300MLH00Y3BN4HD49"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_lei("549300MLH00Y3BN4HD49")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"
    assert hits[0].name == "Ericsson AB"
    assert hits[0].is_stub is False
    assert hits[0].identifiers["lei"] == "549300MLH00Y3BN4HD49"


async def test_fetch_by_lei_empty_result(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.leiCode=UNKNOWNLEI00000000XX"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_lei("UNKNOWNLEI00000000XX")
    assert hits == []


async def test_fetch_by_lei_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_lei("549300MLH00Y3BN4HD49")
    assert hits == []


# ---------------------------------------------------------------------------
# fetch_by_oc_url
# ---------------------------------------------------------------------------


async def test_fetch_by_oc_url_returns_hits(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    oc_url = "https://opencorporates.com/companies/se/556056-6258"
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.opencorporatesUrl={quote(oc_url)}"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_oc_url("se/556056-6258")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"


async def test_fetch_by_oc_url_empty_result(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    oc_url = "https://opencorporates.com/companies/gb/99999999"
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.opencorporatesUrl={quote(oc_url)}"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_oc_url("gb/99999999")
    assert hits == []


async def test_fetch_by_oc_url_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_oc_url("se/556056-6258")
    assert hits == []


# ---------------------------------------------------------------------------
# fetch_by_registration
# ---------------------------------------------------------------------------


async def test_fetch_by_registration_returns_hits(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=(
            f"{_API}/entities"
            f"?filter:properties.registrationNumber={quote('556056-6258')}"
            f"&filter:properties.jurisdiction={quote('se')}"
            f"&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("se", "556056-6258")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"
    assert hits[0].name == "Ericsson AB"


async def test_fetch_by_registration_uppercases_jurisdiction_lowercased(
    httpx_mock: HTTPXMock,
) -> None:
    """Jurisdiction is always sent lowercase regardless of caller input."""
    from urllib.parse import quote
    httpx_mock.add_response(
        url=(
            f"{_API}/entities"
            f"?filter:properties.registrationNumber={quote('00102498')}"
            f"&filter:properties.jurisdiction={quote('gb')}"
            f"&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("GB", "00102498")
    assert hits == []


async def test_fetch_by_registration_empty_result(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=(
            f"{_API}/entities"
            f"?filter:properties.registrationNumber={quote('ZZZZZZZZ')}"
            f"&filter:properties.jurisdiction={quote('fr')}"
            f"&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("fr", "ZZZZZZZZ")
    assert hits == []


async def test_fetch_by_registration_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("se", "556056-6258")
    assert hits == []


# ---------------------------------------------------------------------------
# fetch_by_name (strategy 4 — name-based fallback)
# ---------------------------------------------------------------------------

async def test_fetch_by_name_returns_hits(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Ericsson AB')}&filter:schema=LegalEntity&limit=5",
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Ericsson AB")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"
    assert hits[0].name == "Ericsson AB"


async def test_fetch_by_name_empty_result(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Nonexistent Corp')}&filter:schema=LegalEntity&limit=5",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Nonexistent Corp")
    assert hits == []


async def test_fetch_by_name_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Ericsson AB")
    assert hits == []
