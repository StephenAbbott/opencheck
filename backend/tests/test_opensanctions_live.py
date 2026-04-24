"""Live OpenSanctions adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.opensanctions import OpenSanctionsAdapter

_API = "https://api.opensanctions.org"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_entity_search_maps_results(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/default?q=rosneft&schema=LegalEntity&limit=10",
        match_headers={"Authorization": "ApiKey test-key"},
        json={
            "results": [
                {
                    "id": "NK-rosneft",
                    "schema": "Company",
                    "caption": "Rosneft Oil Company",
                    "properties": {
                        "leiCode": ["253400VC22A0KFSOPB29"],
                        "wikidataId": ["Q219617"],
                    },
                    "datasets": ["eu_fsf", "us_ofac_sdn"],
                    "topics": ["sanction"],
                }
            ]
        },
    )

    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("rosneft", SearchKind.ENTITY)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name == "Rosneft Oil Company"
    assert hit.hit_id == "NK-rosneft"
    assert hit.identifiers["lei"] == "253400VC22A0KFSOPB29"
    assert hit.identifiers["wikidata_qid"] == "Q219617"
    assert "sanction" in hit.summary


async def test_person_search(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/default?q=putin&schema=Person&limit=10",
        json={
            "results": [
                {
                    "id": "NK-putin",
                    "schema": "Person",
                    "caption": "Vladimir Putin",
                    "properties": {"wikidataId": ["Q7747"]},
                    "topics": ["role.pep", "sanction"],
                    "datasets": ["eu_fsf"],
                }
            ]
        },
    )

    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("putin", SearchKind.PERSON)
    assert len(hits) == 1
    assert hits[0].identifiers["wikidata_qid"] == "Q7747"


async def test_fetch_entity_bundle(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities/NK-rosneft",
        json={
            "id": "NK-rosneft",
            "schema": "Company",
            "caption": "Rosneft Oil Company",
            "properties": {"name": ["Rosneft Oil Company"]},
        },
    )

    adapter = OpenSanctionsAdapter()
    bundle = await adapter.fetch("NK-rosneft")
    assert bundle["entity_id"] == "NK-rosneft"
    assert bundle["entity"]["caption"] == "Rosneft Oil Company"


async def test_stub_path_when_no_key(monkeypatch) -> None:
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    get_settings.cache_clear()

    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("anything", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True
