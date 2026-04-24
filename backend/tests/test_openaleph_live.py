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
