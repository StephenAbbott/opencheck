"""Live EveryPolitician adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.everypolitician import EveryPoliticianAdapter

_API = "https://api.opensanctions.org"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_search_returns_politicians(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/peps?q=putin&schema=Person&limit=10",
        json={
            "results": [
                {
                    "id": "Q7747-pep",
                    "schema": "Person",
                    "caption": "Vladimir Putin",
                    "properties": {
                        "name": ["Vladimir Putin"],
                        "position": ["President of Russia"],
                        "country": ["ru"],
                        "wikidataId": ["Q7747"],
                    },
                }
            ]
        },
    )

    adapter = EveryPoliticianAdapter()
    hits = await adapter.search("putin", SearchKind.PERSON)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name == "Vladimir Putin"
    assert hit.identifiers["wikidata_qid"] == "Q7747"
    assert "President" in hit.summary
    assert "RU" in hit.summary


async def test_search_rejects_entity_kind() -> None:
    """EveryPolitician is persons-only — entity searches return nothing."""
    adapter = EveryPoliticianAdapter()
    hits = await adapter.search("acme", SearchKind.ENTITY)
    assert hits == []


async def test_fetch_returns_full_entity(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities/Q7747-pep",
        json={
            "id": "Q7747-pep",
            "schema": "Person",
            "caption": "Vladimir Putin",
            "properties": {
                "name": ["Vladimir Putin"],
                "position": ["President of Russia"],
                "wikidataId": ["Q7747"],
                "birthDate": ["1952-10-07"],
            },
        },
    )

    adapter = EveryPoliticianAdapter()
    bundle = await adapter.fetch("Q7747-pep")

    assert bundle["entity_id"] == "Q7747-pep"
    assert bundle["entity"]["caption"] == "Vladimir Putin"


async def test_stub_path_when_allow_live_false(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = EveryPoliticianAdapter()
    hits = await adapter.search("anything", SearchKind.PERSON)
    assert len(hits) == 1
    assert hits[0].is_stub is True


async def test_stub_path_when_no_api_key(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    get_settings.cache_clear()

    adapter = EveryPoliticianAdapter()
    hits = await adapter.search("anything", SearchKind.PERSON)
    assert len(hits) == 1
    assert hits[0].is_stub is True
