"""Tests for the Nigeria CAC beneficial ownership adapter + mapper.

Two layers:
* Fixture-driven unit tests (no committed artifact) for the adapter contract
  and the BODS mapping (interest types, person vs entity BOC, shared-owner
  dedup, the corroboration rule).
* A regression test that runs the **real committed** index
  (opencheck/data/cac_nigeria_psc.json) through the mapper and asserts every
  statement validates — so a bad data edit is caught in CI.
"""

from __future__ import annotations

import pytest

from opencheck.bods import map_cac_nigeria, validate_shape
from opencheck.routers.lookup import _bh_cac_nigeria, _build_result_hit, _LookupCtx
from opencheck.sources import REGISTRY, cac_nigeria
from opencheck.sources.base import SearchKind

# 20-char format-valid dummy LEIs.
_LEI_A = "5493001KJTIIGC8Y1R12"
_LEI_B = "213800WSGIIZCXF1P572"
_LEI_MISS = "9999009999999999XX99"

# Two subjects sharing a corporate owner ("Shared Holdings Ltd") to exercise
# cross-subject statement dedup, plus a person owner and a control-only owner.
_FIXTURE = {
    _LEI_A: {
        "company": "ALPHA PLC",
        "rc": "111111",
        "lei": _LEI_A,
        "lei_status": "ISSUED",
        "status": "ACTIVE",
        "pscs": [
            {
                "owner_name": "Shared Holdings Ltd", "owner_kind": "entity",
                "owner_rc": None, "owner_jurisdiction": "NG", "nationality": None,
                "notified": "2023-05-07", "shares": True, "share_pct_direct": 96.6,
                "share_pct_indirect": 0, "voting": False, "voting_pct_direct": None,
                "voting_pct_indirect": None, "appoint_board": True,
                "sig_influence_company": False, "sig_influence_trust_firm": False,
            },
        ],
    },
    _LEI_B: {
        "company": "BETA PLC",
        "rc": "222222",
        "lei": _LEI_B,
        "lei_status": "LAPSED",
        "status": "ACTIVE",
        "pscs": [
            {
                "owner_name": "Shared Holdings Ltd", "owner_kind": "entity",
                "owner_rc": None, "owner_jurisdiction": "NG", "nationality": None,
                "notified": "2023-07-29", "shares": True, "share_pct_direct": 66.9,
                "share_pct_indirect": 0, "voting": True, "voting_pct_direct": 66.87,
                "voting_pct_indirect": 0, "appoint_board": True,
                "sig_influence_company": True, "sig_influence_trust_firm": True,
            },
            {
                "owner_name": "Jane Founder", "owner_kind": "person",
                "owner_rc": None, "owner_jurisdiction": None, "nationality": "NIGERIA",
                "notified": "2023-07-29", "shares": True, "share_pct_direct": 5.4,
                "share_pct_indirect": 0, "voting": True, "voting_pct_direct": 5.38,
                "voting_pct_indirect": None, "appoint_board": True,
                "sig_influence_company": True, "sig_influence_trust_firm": True,
            },
        ],
    },
}


@pytest.fixture(autouse=True)
def _inject_index(monkeypatch):
    monkeypatch.setattr(cac_nigeria, "_index", dict(_FIXTURE))
    yield
    cac_nigeria._reset_index_for_tests()


@pytest.fixture
def adapter():
    return cac_nigeria.CacNigeriaAdapter()


def _ctx(lei, name):
    return _LookupCtx(
        lei=lei, legal_name=name, jurisdiction="NG", registered_as="",
        derived={}, ocid=None, spglobal=None, qid=None,
    )


async def test_info(adapter):
    info = adapter.info
    assert info.id == "cac_nigeria"
    assert info.category == "cdd"
    assert info.requires_api_key is False
    assert info.is_national_register is True
    assert info.country == "NG"
    assert SearchKind.ENTITY in info.supports
    # Licence is a public register, not non-commercial → no NC warning.
    assert "nc" not in info.license.lower()


async def test_search_is_empty(adapter):
    assert await adapter.search("anything", SearchKind.ENTITY) == []


async def test_fetch_by_lei_match_miss_case(adapter):
    b = await adapter.fetch_by_lei(_LEI_A)
    assert b is not None and b["source_id"] == "cac_nigeria"
    assert b["is_stub"] is False and b["record"]["company"] == "ALPHA PLC"
    assert await adapter.fetch_by_lei(_LEI_A.lower()) is not None
    assert await adapter.fetch_by_lei(_LEI_MISS) is None
    stub = await adapter.fetch(_LEI_MISS)
    assert stub["is_stub"] is True


async def test_mapper_shapes_and_validate(adapter):
    b = await adapter.fetch_by_lei(_LEI_B)
    stmts = list(map_cac_nigeria(b))
    assert validate_shape(stmts) == []
    kinds = [s["recordType"] for s in stmts]
    # subject + Shared Holdings entity + Jane person + 2 relationships
    assert kinds.count("entity") == 2
    assert kinds.count("person") == 1
    assert kinds.count("relationship") == 2


async def test_person_vs_entity_beneficial_ownership_flag(adapter):
    stmts = list(map_cac_nigeria(await adapter.fetch_by_lei(_LEI_B)))
    by_id = {s["statementId"]: s for s in stmts}
    for s in stmts:
        if s["recordType"] != "relationship":
            continue
        party = by_id[s["recordDetails"]["interestedParty"]]
        boc = {i["beneficialOwnershipOrControl"] for i in s["recordDetails"]["interests"]}
        if party["recordType"] == "person":
            assert boc == {True}
        else:
            assert boc == {False}


async def test_five_conditions_map_to_interest_types(adapter):
    stmts = list(map_cac_nigeria(await adapter.fetch_by_lei(_LEI_B)))
    # Jane Founder has all five conditions set.
    rel = next(
        s for s in stmts
        if s["recordType"] == "relationship"
        and any(
            p["recordType"] == "person"
            for p in stmts
            if p["statementId"] == s["recordDetails"]["interestedParty"]
        )
    )
    types = [i["type"] for i in rel["recordDetails"]["interests"]]
    assert "shareholding" in types
    assert "votingRights" in types
    assert "appointmentOfBoard" in types
    assert types.count("otherInfluenceOrControl") == 2  # conditions 4 and 5
    # Share percentage carried through.
    sh = next(i for i in rel["recordDetails"]["interests"] if i["type"] == "shareholding")
    assert sh["share"]["exact"] == 5.4


async def test_shared_owner_dedup_across_subjects(adapter):
    a = list(map_cac_nigeria(await adapter.fetch_by_lei(_LEI_A)))
    b = list(map_cac_nigeria(await adapter.fetch_by_lei(_LEI_B)))

    def owner_id(stmts):
        return next(
            s["statementId"] for s in stmts
            if s["recordType"] == "entity"
            and s["recordDetails"]["name"] == "Shared Holdings Ltd"
        )

    assert owner_id(a) == owner_id(b)


async def test_hit_builder_asserts_rc_not_lei(adapter):
    b = await adapter.fetch_by_lei(_LEI_A)
    hit = _bh_cac_nigeria(b, _ctx(_LEI_A, "ALPHA PLC"))
    assert hit.source_id == "cac_nigeria"
    assert hit.identifiers == {"ng_cac_rc": "111111"}
    assert "lei" not in hit.identifiers  # corroboration rule
    assert hit.is_stub is False
    # Subtitle counts PSC filings (register rows), not persons/owners: declared
    # parties may be companies or be listed by control rather than ownership.
    assert "PSC filing" in hit.summary
    assert "with significant control" not in hit.summary


async def test_build_result_hit_gates_on_record(adapter):
    b = await adapter.fetch_by_lei(_LEI_A)
    assert _build_result_hit("cac_nigeria", b, _ctx(_LEI_A, "ALPHA PLC")) is not None
    assert _build_result_hit("cac_nigeria", None, _ctx(_LEI_MISS, "x")) is None


async def test_source_block_is_official_register(adapter):
    stmts = list(map_cac_nigeria(await adapter.fetch_by_lei(_LEI_A)))
    src = stmts[0]["source"]
    assert src["type"] == ["officialRegister"]
    assert "Corporate Affairs Commission" in src["description"]


def test_registered_in_registry():
    assert REGISTRY.get("cac_nigeria") is not None


# ---------------------------------------------------------------------------
# Regression: the real committed index maps cleanly (catches bad data edits)
# ---------------------------------------------------------------------------


async def test_committed_index_maps_and_validates():
    """Run the shipped opencheck/data/cac_nigeria_psc.json through the mapper."""
    cac_nigeria._reset_index_for_tests()  # ignore the fixture; load the real file
    adapter = cac_nigeria.CacNigeriaAdapter()
    index = cac_nigeria._get_index()
    assert len(index) == 10, "expected 10 curated Nigerian entities"
    total_rel = 0
    for lei in index:
        bundle = await adapter.fetch_by_lei(lei)
        stmts = list(map_cac_nigeria(bundle))
        assert validate_shape(stmts) == [], f"{lei} produced invalid BODS"
        assert any(s["recordType"] == "entity" for s in stmts)
        total_rel += sum(1 for s in stmts if s["recordType"] == "relationship")
        # Hit builder must never assert the LEI (corroboration rule).
        hit = _bh_cac_nigeria(bundle, _ctx(lei, index[lei]["company"]))
        assert "lei" not in hit.identifiers
    assert total_rel >= 10
    cac_nigeria._reset_index_for_tests()
