"""Live GLEIF adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import json
import time

import pytest
from pytest_httpx import HTTPXMock

from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.gleif import GleifAdapter, _RELATIONSHIP_CACHE_MAX_AGE_DAYS

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
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/direct-children?page[size]=10&page[number]=1",
        json={"data": [], "meta": {"pagination": {"total": 0}}},
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
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/direct-children?page[size]=10&page[number]=1",
        json={"data": [], "meta": {"pagination": {"total": 0}}},
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


# ---------------------------------------------------------------------------
# Relationship cache TTL tests
# ---------------------------------------------------------------------------

_LEI = "213800LBDB8WB3QGVN21"
_PARENT_LEI = "PARENTXXXXXXXXXXXXXX"

_RECORD_FIXTURE = {
    "data": {
        "id": _LEI,
        "attributes": {
            "lei": _LEI,
            "entity": {
                "legalName": {"name": "BP P.L.C."},
                "jurisdiction": "GB",
            },
        },
    }
}

_PARENT_FIXTURE = {
    "data": {
        "id": _PARENT_LEI,
        "attributes": {
            "lei": _PARENT_LEI,
            "entity": {
                "legalName": {"name": "BP Group Holdings"},
                "jurisdiction": "GB",
            },
        },
    }
}


def _write_stale_cache(tmp_path, cache_key: str, payload) -> None:
    """Write a cache entry backdated by 2 × _RELATIONSHIP_CACHE_MAX_AGE_DAYS."""
    cache = Cache(root=tmp_path)
    cache.put(cache_key, payload)
    live_path = tmp_path / "cache" / "live" / f"{cache_key}.json"
    wrapper = json.loads(live_path.read_text())
    wrapper["_cached_at"] = time.time() - (_RELATIONSHIP_CACHE_MAX_AGE_DAYS * 2 * 86_400)
    live_path.write_text(json.dumps(wrapper))


async def test_stale_parent_cache_is_refetched_when_gleif_now_returns_404(
    httpx_mock: HTTPXMock, tmp_path
) -> None:
    """A parent relationship cached > _RELATIONSHIP_CACHE_MAX_AGE_DAYS ago must
    be re-fetched.  If GLEIF now returns 404 (relationship became inactive),
    the bundle should have no parent rather than serving stale data.
    """
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    # Pre-populate a stale direct-parent cache entry.
    _write_stale_cache(tmp_path, f"gleif/lei/{_LEI}/direct-parent", _PARENT_FIXTURE)

    # GLEIF will re-serve the main record but return 404 for the parent
    # (relationship now inactive).
    httpx_mock.add_response(url=f"{_API}/lei-records/{_LEI}", json=_RECORD_FIXTURE)
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}/direct-parent", status_code=404
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}/direct-parent-reporting-exception",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}/ultimate-parent", status_code=404
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}/ultimate-parent-reporting-exception",
        status_code=404,
    )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}/direct-children?page[size]=10&page[number]=1",
        json={"data": [], "meta": {"pagination": {"total": 0}}},
    )

    adapter = GleifAdapter()
    bundle = await adapter.fetch(_LEI)

    # Stale parent was discarded; fresh fetch confirmed no parent.
    assert bundle["direct_parent"] is None
    assert bundle["direct_parent_exception"] is None

    monkeypatch.undo()
    get_settings.cache_clear()


async def test_fresh_parent_cache_is_served_without_refetch(
    httpx_mock: HTTPXMock, tmp_path
) -> None:
    """A parent relationship cached < _RELATIONSHIP_CACHE_MAX_AGE_DAYS ago must
    be served from cache without hitting the GLEIF API.
    """
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    # Pre-populate a fresh direct-parent cache (just written = current time).
    cache = Cache(root=tmp_path)
    cache.put(f"gleif/lei/{_LEI}/direct-parent", _PARENT_FIXTURE)
    cache.put(f"gleif/lei/{_LEI}/ultimate-parent", None)
    cache.put(f"gleif/lei/{_LEI}/ultimate-parent-exception", None)
    cache.put(f"gleif/lei/{_LEI}/direct-children-p1", {"data": [], "meta": {"pagination": {"total": 0}}})

    # Only the main record fetch hits the network; parent/children come from cache.
    httpx_mock.add_response(url=f"{_API}/lei-records/{_LEI}", json=_RECORD_FIXTURE)

    adapter = GleifAdapter()
    bundle = await adapter.fetch(_LEI)

    assert bundle["direct_parent"]["id"] == _PARENT_LEI
    # No unexpected HTTP requests were made (httpx_mock would raise if they were).

    monkeypatch.undo()
    get_settings.cache_clear()
