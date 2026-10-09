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


def test_bods_gleif_not_in_registry_when_deactivated() -> None:
    # bods_gleif is temporarily deactivated (OOM on Render free tier).
    # The adapter class still exists; verify it is instantiable.
    assert "bods_gleif" not in REGISTRY
    assert isinstance(BODSGleifAdapter(), BODSGleifAdapter)


def test_bods_uk_psc_not_in_registry_when_deactivated() -> None:
    # bods_uk_psc is temporarily deactivated (OOM on Render free tier).
    # The adapter class still exists; verify it is instantiable.
    assert "bods_uk_psc" not in REGISTRY
    assert isinstance(BODSUKPSCAdapter(), BODSUKPSCAdapter)


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


def test_map_bods_gleif_republishes_statements() -> None:
    """Phase 317 (dates-audit Q3): an OpenCheck publication of Open Ownership's
    data — v0.4 shapes, OpenCheck's publication block, OO's recordId and
    statementDate kept, the OO statement named in an annotation."""
    stmts = [{"statementType": "entityStatement", "statementId": "abc",
              "statementDate": "2025-02-28",
              "publicationDetails": {"publicationDate": "2025-03-01"}}]
    (out,) = list(map_bods_gleif({"bods_statements": stmts}))
    assert out["recordType"] == "entity" and "statementType" not in out
    assert out["recordId"] == "abc" and out["statementId"] != "abc"
    assert out["statementDate"] == "2025-02-28"
    assert out["publicationDetails"]["publisher"] == {"name": "OpenCheck"}
    assert out["source"]["opencheckSourceId"] == "bods_gleif"
    assert "abc" in out["annotations"][0]["description"]
    assert "2025-03-01" in out["annotations"][0]["description"]


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
    assert [r["recordType"] for r in result] == ["entity", "person", "relationship"]
    assert [r["recordId"] for r in result] == ["ent-1", "per-1", "rel-1"]


def test_republished_relationships_take_v04_references_and_close_when_ended() -> None:
    rel = {
        "statementType": "relationshipStatement", "statementId": "rel-1",
        "recordDetails": {
            "isComponent": False,
            "subject": {"describedByEntityStatement": "ent-1"},
            "interestedParty": {"describedByPersonStatement": "per-1"},
            "interests": [{"type": "shareholding", "startDate": "2016-04-06",
                           "endDate": "2019-01-01"}],
        },
    }
    (out,) = list(map_bods_uk_psc({"bods_statements": [rel]}))
    assert out["recordDetails"]["subject"] == "ent-1"
    assert out["recordDetails"]["interestedParty"] == "per-1"
    assert out["declarationSubject"] == "ent-1"
    assert out["recordStatus"] == "closed"
    assert out["publicationDetails"]["publicationDate"]


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
