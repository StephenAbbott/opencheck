"""Smoke tests for Phase 0 source adapter stubs."""

from __future__ import annotations

import pytest

from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind


@pytest.fixture(autouse=True)
def _isolated_data_root(monkeypatch, tmp_path):
    """Point the cache at a tmp dir so the shipped demo fixtures
    don't shadow the stub path under test."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_registry_has_expected_sources() -> None:
    assert set(REGISTRY.keys()) == {
        "companies_house",
        "gleif",
        "opencorporates",
        "brightquery",
        "opensanctions",
        "everypolitician",
        "wikidata",
        "opentender",
    }


def test_source_info_fields_are_populated() -> None:
    for adapter in REGISTRY.values():
        info = adapter.info
        assert info.id == adapter.id
        assert info.name
        assert info.homepage.startswith("http")
        assert info.license
        assert info.attribution
        assert info.supports, f"{adapter.id} declares no supported kinds"


# Adapters that are entered via a specific identifier (e.g. LEI, ocid) rather
# than free-text search. Their search() method intentionally returns [] because
# they are called directly via fetch() in the LEI-lookup flow (app.py).
_IDENTIFIER_KEYED = {"opencorporates", "brightquery"}


@pytest.mark.parametrize(
    "source_id",
    [sid for sid in REGISTRY if sid not in _IDENTIFIER_KEYED],
)
async def test_adapter_search_returns_stubs_for_supported_kinds(source_id: str) -> None:
    adapter = REGISTRY[source_id]
    for kind in adapter.info.supports:
        hits = await adapter.search("Rosneft", kind)
        assert hits, f"{source_id} returned no stub hits for {kind}"
        for hit in hits:
            assert hit.source_id == source_id
            assert hit.is_stub is True
            assert hit.name


async def test_entity_only_adapter_rejects_person_search() -> None:
    # GLEIF is entity-only; EveryPolitician is person-only.
    gleif = REGISTRY["gleif"]
    assert await gleif.search("Alice Example", SearchKind.PERSON) == []

    ep = REGISTRY["everypolitician"]
    assert await ep.search("Acme Ltd", SearchKind.ENTITY) == []


async def test_fetch_returns_stub_payload() -> None:
    adapter = REGISTRY["companies_house"]
    payload = await adapter.fetch("00000000")
    assert payload["is_stub"] is True
    assert payload["source_id"] == "companies_house"
    assert payload["hit_id"] == "00000000"
