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
        "match_method": "gleif_name_exact",
        "match_confidence": "medium",
        "gleif_legal_name": "GHANA NATIONAL PETROLEUM CORPORATION",
        "soe": {
            "company_name": "Ghana National Petroleum Corporation",
            "name_variants": [
                "Ghana National Petroleum Corporation",
                "GHANA NATIONAL PETROLEUM CORP (GNPC)",
            ],
            "country_name": "Ghana",
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
    # Never "high" from this source. Nothing EITI publishes corroborates the
    # match — two strings agreeing is not corroboration — so the builder grades
    # every row medium and the adapter carries that through.
    assert bundle["match_confidence"] == "medium"
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


async def test_hit_builder_asserts_no_identifiers_at_all(adapter):
    """The hit carries an empty identifier set — and this calls the **real**
    hit builder rather than describing what it ought to do.

    Three separate reasons, and all three have to hold:

    * ``lei`` is OpenCheck-derived at index-build time (the corroboration rule).
    * ``eiti_soe_id`` was published here until EITI regenerated its whole
      company id space in the new database — UUIDv4 to a UUIDv5 over a
      normalised name. A key a source can regenerate wholesale is a
      deduplication key, and asserting one lets the reconciler claim
      corroboration from a string that does not resolve anywhere.
    * ``ocid`` never was asserted, and now could not be: EITI publishes no
      OpenCorporates id for a single one of its 194 state-owned enterprises.

    The previous version of this test read two fields off the bundle and
    asserted nothing about the hit at all, so it would have passed unchanged
    through the identifier decision it existed to guard.
    """
    from opencheck.routers.hit_builders import _bh_eiti_soe, _LookupCtx

    bundle = await adapter.fetch_by_lei(_LEI_MATCH)
    hit = _bh_eiti_soe(bundle, _LookupCtx(lei=_LEI_MATCH, legal_name="GNPC"))

    assert hit.identifiers == {}
    # The id is still in the bundle: the live payments query is keyed on it.
    assert bundle["eiti_id_company"] == "eiti-co-123"


async def test_bundle_carries_every_spelling_eiti_holds(adapter):
    """The new database keys on a UUIDv5 over a normalised name, so one company
    can arrive under several spellings. They are kept — they are what the GLEIF
    search is run against, and what a reader needs to recognise the company."""
    bundle = await adapter.fetch_by_lei(_LEI_MATCH)
    assert bundle["name_variants"] == [
        "Ghana National Petroleum Corporation",
        "GHANA NATIONAL PETROLEUM CORP (GNPC)",
    ]
    assert bundle["gleif_legal_name"] == "GHANA NATIONAL PETROLEUM CORPORATION"


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

    # Corroboration rule: **no identifiers at all**, not merely no LEI. The
    # `XI-EITI` scheme this used to emit carried `eiti_id_company`, and every
    # statement asserting one now names a key that cannot be looked up in the
    # database it came from.
    assert soe["recordDetails"]["identifiers"] == []


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


# ---------------------------------------------------------------------------
# The builder, on a fixture, through its real entry points
# ---------------------------------------------------------------------------


def _load_builder():
    """Import ``scripts/build_eiti_soe_index.py`` by path.

    The point is to exercise the shipped file rather than a reimplementation of
    it: a test that arranges the thing it is meant to prove proves nothing.
    """
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "scripts" / "build_eiti_soe_index.py"
    spec = importlib.util.spec_from_file_location("_build_eiti_soe_index", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _FakeClient:
    """Stands in for httpx at the one seam the roster builder uses."""

    def __init__(self, rows):
        self._rows = rows

    def get(self, url, params=None, headers=None):  # noqa: D401
        sql = (params or {}).get("sql", "")
        rows = [{"n": len(self._rows)}] if "count(*)" in sql else self._rows

        class _Resp:
            status_code = 200

            @staticmethod
            def raise_for_status():
                return None

            @staticmethod
            def json():
                return {"rows": rows}

        return _Resp()


def test_builder_collapses_name_variants_onto_one_company():
    """Two spellings, one ``eiti_id_company`` → one roster entry holding both.

    This is the whole reason the roster is keyed on the id: EITI's own
    ``view_soeList`` holds 251 distinct ``soe_name`` values for 194 distinct
    companies, and the old builder — which keyed on the normalised name —
    counted those as separate SOEs and searched GLEIF for each.
    """
    builder = _load_builder()
    rows = [
        {
            "eiti_id_company": "eiti_id_company:abc",
            "soe_name": "Sonangol E.P.",
            "country_iso3": "AGO",
            "country_name": "Angola",
            "sectors": "Oil & Gas",
            "year": 2019,
            "audited_statement_url": "Not available",
            "public_listing_url": "https://sonangol.co.ao",
        },
        {
            "eiti_id_company": "eiti_id_company:abc",
            "soe_name": "SONANGOL EP",
            "country_iso3": "AGO",
            "country_name": "Angola",
            "sectors": "Oil & Gas",
            "year": 2021,
            "audited_statement_url": "https://example.org/afs.pdf",
            "public_listing_url": "Not available",
        },
    ]
    roster = builder._roster_new(_FakeClient(rows), sleep=0)

    assert list(roster) == ["eiti_id_company:abc"]
    entry = roster["eiti_id_company:abc"]
    assert entry["names"] == ["Sonangol E.P.", "SONANGOL EP"]
    assert entry["years"] == ["2019", "2021"]
    # "Not available" is EITI's way of saying nothing, and must never reach a
    # card as if it were a URL.
    assert entry["afs"] == "https://example.org/afs.pdf"
    assert entry["listing"] == "https://sonangol.co.ao"


def test_builder_refuses_a_truncated_harvest():
    """The page cap is a server setting, not a contract.

    ``_fetch_all`` checks what it harvested against the server's own
    ``count(*)``. An earlier SOE build silently kept 176 rows of 5,332 because
    nothing compared the two.
    """
    builder = _load_builder()

    class _ShortClient(_FakeClient):
        def get(self, url, params=None, headers=None):
            sql = (params or {}).get("sql", "")
            if "count(*)" in sql:
                rows = [{"n": 99}]        # the server says 99 …
            else:
                rows = self._rows          # … and hands back 2

            class _Resp:
                status_code = 200

                @staticmethod
                def raise_for_status():
                    return None

                @staticmethod
                def json():
                    return {"rows": rows}

            return _Resp()

    with pytest.raises(RuntimeError, match="harvested 2 rows but count"):
        builder._fetch_all(
            _ShortClient([{"a": 1}, {"a": 2}]), "view_soeList", "a", sleep=0
        )


def test_builder_never_strips_a_legal_form_suffix():
    """``Teck`` matched TECK GmbH as a plain exact match in the sibling builder.

    Suffix-stripping is the one "improvement" that would make this matcher
    dangerous, so the normaliser's behaviour is pinned rather than described.
    """
    builder = _load_builder()
    assert builder._norm_name("Sonangol E.P.") == "sonangol e p"
    assert builder._norm_name("Teck") != builder._norm_name("Teck GmbH")
    assert builder._norm_name("Glencore") != builder._norm_name("Glencore AG")
    # Case, accents and punctuation still fold.
    assert builder._norm_name("Société  Nationale!") == builder._norm_name("societe nationale")


def test_builder_treats_eiti_placeholders_as_absent():
    builder = _load_builder()
    for placeholder in ("Not available", "n/v", "N/A", "", "  "):
        assert builder._clean(placeholder) == ""
    assert builder._clean(" https://example.org ") == "https://example.org"


async def test_mapper_names_the_state_when_eiti_does_not_name_the_body(adapter):
    """The repointed roster carries no `government_entity`, and the graph must
    not lose its state-control edge because of it.

    The new database's `metadata_gov_entities` holds revenue-collecting
    agencies with no link saying which body owns which enterprise, so the
    controlling party falls back to the state EITI files the company under —
    named as such, with the relationship saying in words that EITI does not name
    the organ. Dropping the edge instead would silently switch off
    `STATE_CONTROLLED`, the one signal this adapter exists to raise.
    """
    from opencheck.bods import map_eiti_soe

    bundle = await adapter.fetch_by_lei(_LEI_MATCH)
    bundle["government_entity"] = None
    bundle["country_name"] = "Ghana"

    statements = list(map_eiti_soe(bundle))
    assert len(statements) == 3

    state_body = next(
        s for s in statements
        if s["recordType"] == "entity"
        and s["recordDetails"]["entityType"]["type"] == "stateBody"
    )
    assert state_body["recordDetails"]["name"] == "Government of Ghana"

    rel = next(s for s in statements if s["recordType"] == "relationship")
    details = rel["recordDetails"]["interests"][0]["details"]
    assert "does not name the controlling government body" in details
    # The inference is labelled, not disguised as an EITI assertion.
    assert "controlled by Government of Ghana (EITI SOE database)" not in details


async def test_mapper_prefers_the_body_eiti_names(adapter):
    from opencheck.bods import map_eiti_soe

    bundle = await adapter.fetch_by_lei(_LEI_MATCH)
    bundle["government_entity"] = "Ministry of Energy"

    rel = next(
        s for s in map_eiti_soe(bundle) if s["recordType"] == "relationship"
    )
    details = rel["recordDetails"]["interests"][0]["details"]
    assert details == (
        "State-owned enterprise controlled by Ministry of Energy "
        "(EITI SOE database)."
    )
