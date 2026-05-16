"""Tests for the Open Ownership BODS bulk data adapters.

These tests exercise the stub path (no Parquet/FTS files configured) and
verify the adapter structure, SourceInfo fields, and search/fetch contracts.

Live-path tests (requiring actual Parquet files) are marked with
``pytest.mark.skipif`` and can be activated by setting the env vars
``BODS_GLEIF_PARQUET_DIR``, ``BODS_GLEIF_FTS_DB``,
``BODS_UK_PSC_PARQUET_DIR``, ``BODS_UK_PSC_FTS_DB``.
"""

from __future__ import annotations

import os

import pytest

from opencheck.bods import map_bods_gleif, map_bods_uk_psc
from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.bods_gleif import BODSGleifAdapter
from opencheck.sources.bods_uk_psc import BODSUKPSCAdapter


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("BODS_GLEIF_PARQUET_DIR", raising=False)
    monkeypatch.delenv("BODS_GLEIF_FTS_DB", raising=False)
    monkeypatch.delenv("BODS_UK_PSC_PARQUET_DIR", raising=False)
    monkeypatch.delenv("BODS_UK_PSC_FTS_DB", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Registry registration
# ---------------------------------------------------------------------------


def test_bods_gleif_registered_in_registry() -> None:
    assert "bods_gleif" in REGISTRY
    assert isinstance(REGISTRY["bods_gleif"], BODSGleifAdapter)


def test_bods_uk_psc_registered_in_registry() -> None:
    assert "bods_uk_psc" in REGISTRY
    assert isinstance(REGISTRY["bods_uk_psc"], BODSUKPSCAdapter)


# ---------------------------------------------------------------------------
# SourceInfo fields
# ---------------------------------------------------------------------------


def test_bods_gleif_info() -> None:
    adapter = BODSGleifAdapter()
    info = adapter.info
    assert info.id == "bods_gleif"
    assert info.name
    assert info.homepage.startswith("http")
    assert info.license == "CC-BY-4.0"
    assert info.attribution
    assert info.supports == [SearchKind.ENTITY]
    assert info.requires_api_key is False
    assert info.live_available is False  # no paths configured


def test_bods_uk_psc_info() -> None:
    adapter = BODSUKPSCAdapter()
    info = adapter.info
    assert info.id == "bods_uk_psc"
    assert info.name
    assert info.homepage.startswith("http")
    assert info.license == "OGL-UK-3.0"
    assert info.attribution
    assert SearchKind.ENTITY in info.supports
    assert SearchKind.PERSON in info.supports
    assert info.requires_api_key is False
    assert info.live_available is False


# ---------------------------------------------------------------------------
# Stub search
# ---------------------------------------------------------------------------


async def test_bods_gleif_stub_entity_search() -> None:
    adapter = BODSGleifAdapter()
    hits = await adapter.search("Rosneft", SearchKind.ENTITY)
    assert hits, "Expected at least one stub hit"
    hit = hits[0]
    assert hit.source_id == "bods_gleif"
    assert hit.kind == SearchKind.ENTITY
    assert hit.is_stub is True
    assert hit.name


async def test_bods_gleif_rejects_person_search() -> None:
    adapter = BODSGleifAdapter()
    hits = await adapter.search("Alice Example", SearchKind.PERSON)
    assert hits == [], "GLEIF is entity-only; should return [] for PERSON"


async def test_bods_uk_psc_stub_entity_search() -> None:
    adapter = BODSUKPSCAdapter()
    hits = await adapter.search("Barclays", SearchKind.ENTITY)
    assert hits, "Expected at least one stub hit"
    hit = hits[0]
    assert hit.source_id == "bods_uk_psc"
    assert hit.kind == SearchKind.ENTITY
    assert hit.is_stub is True
    assert hit.name


async def test_bods_uk_psc_stub_person_search() -> None:
    adapter = BODSUKPSCAdapter()
    hits = await adapter.search("Alice Smith", SearchKind.PERSON)
    assert hits, "Expected at least one stub hit"
    hit = hits[0]
    assert hit.source_id == "bods_uk_psc"
    assert hit.kind == SearchKind.PERSON
    assert hit.is_stub is True
    assert hit.name


# ---------------------------------------------------------------------------
# Stub fetch
# ---------------------------------------------------------------------------


async def test_bods_gleif_stub_fetch() -> None:
    adapter = BODSGleifAdapter()
    payload = await adapter.fetch("some-statementid")
    assert payload["source_id"] == "bods_gleif"
    assert payload["hit_id"] == "some-statementid"
    assert payload["is_stub"] is True


async def test_bods_uk_psc_stub_fetch() -> None:
    adapter = BODSUKPSCAdapter()
    payload = await adapter.fetch("some-statementid")
    assert payload["source_id"] == "bods_uk_psc"
    assert payload["hit_id"] == "some-statementid"
    assert payload["is_stub"] is True


# ---------------------------------------------------------------------------
# Passthrough mappers
# ---------------------------------------------------------------------------


def test_map_bods_gleif_passthrough_empty() -> None:
    result = list(map_bods_gleif({}))
    assert result == []


def test_map_bods_gleif_passthrough_statements() -> None:
    stmts = [{"statementType": "entityStatement", "statementId": "abc"}]
    result = list(map_bods_gleif({"bods_statements": stmts}))
    assert result == stmts


def test_map_bods_uk_psc_passthrough_empty() -> None:
    result = list(map_bods_uk_psc({}))
    assert result == []


def test_map_bods_uk_psc_passthrough_statements() -> None:
    stmts = [
        {"statementType": "entityStatement", "statementId": "ent-1"},
        {"statementType": "personStatement", "statementId": "per-1"},
        {"statementType": "relationshipStatement", "statementId": "rel-1"},
    ]
    result = list(map_bods_uk_psc({"bods_statements": stmts}))
    assert result == stmts


# ---------------------------------------------------------------------------
# Live tests (only run when Parquet + FTS files are configured)
# ---------------------------------------------------------------------------

_GLEIF_LIVE = bool(
    os.environ.get("BODS_GLEIF_PARQUET_DIR")
    and os.environ.get("BODS_GLEIF_FTS_DB")
)
_UK_PSC_LIVE = bool(
    os.environ.get("BODS_UK_PSC_PARQUET_DIR")
    and os.environ.get("BODS_UK_PSC_FTS_DB")
)


@pytest.mark.skipif(not _GLEIF_LIVE, reason="BODS GLEIF Parquet not configured")
async def test_bods_gleif_live_search() -> None:
    adapter = BODSGleifAdapter()
    hits = await adapter.search("Deutsche Bank", SearchKind.ENTITY)
    assert hits, "Expected live GLEIF results for Deutsche Bank"
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name
    assert "bods_gleif_statementid" in hit.identifiers


@pytest.mark.skipif(not _GLEIF_LIVE, reason="BODS GLEIF Parquet not configured")
async def test_bods_gleif_live_fetch() -> None:
    adapter = BODSGleifAdapter()
    hits = await adapter.search("Deutsche Bank", SearchKind.ENTITY)
    assert hits
    payload = await adapter.fetch(hits[0].hit_id)
    assert payload["is_stub"] is False
    assert "bods_statements" in payload
    stmts = payload["bods_statements"]
    assert stmts
    entity_stmts = [s for s in stmts if s.get("statementType") == "entityStatement"]
    assert entity_stmts, "Expected at least one entityStatement"
    assert entity_stmts[0]["recordDetails"]["name"]


@pytest.mark.skipif(not _UK_PSC_LIVE, reason="BODS UK PSC Parquet not configured")
async def test_bods_uk_psc_live_entity_search() -> None:
    adapter = BODSUKPSCAdapter()
    hits = await adapter.search("Barclays", SearchKind.ENTITY)
    assert hits, "Expected live UK PSC entity results for Barclays"
    assert hits[0].is_stub is False


@pytest.mark.skipif(not _UK_PSC_LIVE, reason="BODS UK PSC Parquet not configured")
async def test_bods_uk_psc_live_person_search() -> None:
    adapter = BODSUKPSCAdapter()
    hits = await adapter.search("Smith", SearchKind.PERSON)
    assert hits, "Expected live UK PSC person results"
    assert hits[0].is_stub is False
    assert hits[0].kind == SearchKind.PERSON
