"""Tests for the EITI SOE (State-Owned Enterprises) adapter.

Fixture-driven — no network, no committed artifact required. Exercises the
offline LEI match, the bundle shape/validation, and the gzip index load path.
"""

from __future__ import annotations

import gzip
import json

import pytest

from opencheck.sources import eiti_soe
from opencheck.sources.base import SearchKind
from opencheck.sources.schemas.eiti_soe import EitiSoeBundle

# 20-char dummy LEIs (format-valid length; not real records).
_LEI_MATCH = "5493001KJTIIGC8Y1R12"
_LEI_MISS = "213800WSGIIZCXF1P572"

_FIXTURE = {
    _LEI_MATCH: {
        "lei": _LEI_MATCH,
        "match_method": "opencorporates_id",
        "match_confidence": "high",
        "soe": {
            "company_name": "Ghana National Petroleum Corporation",
            "country": "GH",
            "iso_alpha2": "GH",
            "sector": "Oil & Gas",
            "commodities": ["oil", "gas"],
            "company_type": "State-owned enterprise",
            "government_entity": "Ministry of Energy",
            "opencorporates_id": "gh/CS000000001",
            "eiti_id_company": "eiti-co-123",
            "eiti_id_government": "eiti-gov-9",
            "audited_financial_statement": "https://example.org/gnpc.pdf",
            "public_listing_or_website": "https://gnpcghana.com",
            "years": ["2018", "2019"],
            "soe_list": True,
        },
    }
}


@pytest.fixture(autouse=True)
def _inject_index(monkeypatch):
    """Point the adapter at the in-memory fixture and never allow_live."""
    monkeypatch.setattr(eiti_soe, "_index", dict(_FIXTURE))

    class _Settings:
        allow_live = False

    monkeypatch.setattr(eiti_soe, "get_settings", lambda: _Settings())
    yield
    eiti_soe._reset_index_for_tests()


@pytest.fixture
def adapter():
    return eiti_soe.EitiSoeAdapter()


async def test_info_is_cdd_no_key(adapter):
    info = adapter.info
    assert info.id == "eiti_soe"
    assert info.category == "cdd"
    assert info.requires_api_key is False
    assert SearchKind.ENTITY in info.supports


async def test_search_is_empty(adapter):
    assert await adapter.search("anything", SearchKind.ENTITY) == []


async def test_fetch_by_lei_match(adapter):
    bundle = await adapter.fetch_by_lei(_LEI_MATCH)
    assert bundle is not None
    assert bundle["source_id"] == "eiti_soe"
    assert bundle["lei"] == _LEI_MATCH
    assert bundle["is_state_owned"] is True
    assert bundle["entity_name"] == "Ghana National Petroleum Corporation"
    assert bundle["match_confidence"] == "high"
    assert bundle["country"] == "GH"
    assert bundle["commodities"] == ["oil", "gas"]
    assert bundle["is_stub"] is False
    # No live enrichment when allow_live is off.
    assert bundle["payments"] == []
    # Bundle validates against the declared schema.
    EitiSoeBundle.model_validate(bundle)


async def test_fetch_by_lei_case_insensitive(adapter):
    assert await adapter.fetch_by_lei(_LEI_MATCH.lower()) is not None


async def test_fetch_by_lei_miss(adapter):
    assert await adapter.fetch_by_lei(_LEI_MISS) is None


async def test_fetch_deepen_and_stub(adapter):
    bundle = await adapter.fetch(_LEI_MATCH)
    assert bundle["lei"] == _LEI_MATCH
    stub = await adapter.fetch(_LEI_MISS)
    assert stub["is_stub"] is True


async def test_does_not_assert_lei_as_published_identifier(adapter):
    """The SOE DB does not publish the LEI (it is derived at build time), so the
    bundle exposes it as the anchor `lei` but the source publishes eiti/OC ids —
    the hit builder must not assert `lei` in SourceHit.identifiers (see the
    corroboration rule in CLAUDE.md). This test documents the contract the
    routers/lookup.py hit builder must honour."""
    bundle = await adapter.fetch_by_lei(_LEI_MATCH)
    assert bundle["eiti_id_company"] == "eiti-co-123"
    assert bundle["opencorporates_id"] == "gh/CS000000001"


def test_gzip_index_load_path(tmp_path, monkeypatch):
    """The real load path reads a gzipped {'meta', 'index'} artifact."""
    path = tmp_path / "eiti_soe_index.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump({"meta": {"resolved_lei": 1}, "index": _FIXTURE}, f)
    monkeypatch.setattr(eiti_soe, "_INDEX_PATH", path)
    eiti_soe._reset_index_for_tests()
    idx = eiti_soe._get_index()
    assert _LEI_MATCH in idx
    eiti_soe._reset_index_for_tests()


# ---------------------------------------------------------------------------
# BODS mapper — the shape that drives the STATE_CONTROLLED signal
# ---------------------------------------------------------------------------


async def test_mapper_emits_state_control_shape(adapter):
    """map_eiti_soe must emit a registeredEntity SOE, a stateBody government,
    and a controlByLegalFramework relationship whose interestedParty is the
    stateBody — the exact shape risk._state_controlled_signals reads to raise
    STATE_CONTROLLED (so the state-ownership signal needs no bespoke risk rule).
    """
    from opencheck.bods import map_eiti_soe

    bundle = await adapter.fetch_by_lei(_LEI_MATCH)
    statements = list(map_eiti_soe(bundle))

    entities = [s for s in statements if s["recordType"] == "entity"]
    rels = [s for s in statements if s["recordType"] == "relationship"]
    assert len(entities) == 2
    assert len(rels) == 1

    etypes = {
        e["recordDetails"]["entityType"]["type"] for e in entities
    }
    assert etypes == {"registeredEntity", "stateBody"}

    state_body = next(
        e for e in entities
        if e["recordDetails"]["entityType"]["type"] == "stateBody"
    )
    soe = next(
        e for e in entities
        if e["recordDetails"]["entityType"]["type"] == "registeredEntity"
    )
    rel = rels[0]
    assert rel["recordDetails"]["subject"] == soe["statementId"]
    assert rel["recordDetails"]["interestedParty"] == state_body["statementId"]
    assert rel["recordDetails"]["interests"][0]["type"] == "controlByLegalFramework"

    # Corroboration rule: the SOE database does not publish the LEI, so no BODS
    # identifier may carry an LEI scheme.
    for ident in soe["recordDetails"]["identifiers"]:
        assert ident.get("scheme") != "XI-LEI"
        assert ident.get("id") != _LEI_MATCH


async def test_mapper_skips_stub_and_nameless(adapter):
    from opencheck.bods import map_eiti_soe

    assert list(map_eiti_soe({"is_stub": True})) == []
    assert list(map_eiti_soe({"lei": _LEI_MATCH})) == []  # no entity_name


async def test_mapper_gates_state_control_on_low_confidence():
    """A low-confidence (name-only) match must NOT emit the stateBody + control
    relationship — that would raise a false STATE_CONTROLLED on a possibly-wrong
    entity. The SOE entity is still emitted so its enrichment surfaces."""
    from opencheck.bods import map_eiti_soe

    bundle = {
        "lei": _LEI_MATCH,
        "entity_name": "Some SOE",
        "country": "GH",
        "government_entity": "Ministry of Energy",
        "eiti_id_company": "eiti-co-1",
        "eiti_id_government": "eiti-gov-1",
        "match_confidence": "low",
        "is_stub": False,
    }
    low = list(map_eiti_soe(bundle))
    assert [s["recordType"] for s in low] == ["entity"]
    assert low[0]["recordDetails"]["entityType"]["type"] == "registeredEntity"

    # medium / high still emit the full state-control shape.
    bundle["match_confidence"] = "medium"
    kinds = {s["recordType"] for s in map_eiti_soe(bundle)}
    assert kinds == {"entity", "relationship"}
