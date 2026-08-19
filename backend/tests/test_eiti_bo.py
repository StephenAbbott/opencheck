"""Tests for the pooled EITI national BO registers adapter + mapper.

Two layers, mirroring test_cac_nigeria.py:
* Fixture-driven unit tests (no committed artifact) covering the adapter
  contract, each register's sub-mapper (DRC tabular rows, the Armenia
  BODS v0.2→v0.4 upconversion, the Nigeria CAC/NEITI subset), the share
  semantics annotations, and the corroboration rule.
* A regression test that runs the **real committed** pooled index
  (opencheck/data/eiti_bo_index.json.gz) through the mapper and asserts every
  statement validates — so a bad harvest/build is caught in CI.
"""

from __future__ import annotations

import pytest

from opencheck.bods import map_eiti_bo, validate_shape
from opencheck.routers.lookup import _bh_eiti_bo, _build_result_hit, _LookupCtx
from opencheck.sources import REGISTRY, eiti_bo
from opencheck.sources.base import SearchKind

# 20-char format-valid dummy LEIs.
_LEI_DRC = "5493001KJTIIGC8Y1R12"
_LEI_AM = "213800WSGIIZCXF1P572"
_LEI_NG = "5493001RKR55V4X61F71"
_LEI_MISS = "9999009999999999XX99"

_DRC_RECORD = {
    "lei": _LEI_DRC,
    "lei_registration_status": "ISSUED",
    "register_id": "drc_itie",
    "country": "CD",
    "company": "KOLWEZI TEST MINING",
    "local_ids": {"cd_nif": "A0000001X"},
    "source_date": "2026-08-19",
    "retrieved": "2026-08-19T12:00:00+00:00",
    "match": {"method": "name", "confidence": "medium"},
    "drc": {
        "name": "KOLWEZI TEST MINING",
        "acronym": "KTM",
        "nif": "A0000001X",
        "sector": "MINIER",
        "country": "RDC",
        "pct_semantics": "fraction-of-1",
        "owners": [
            {
                # Honorific + fraction share + PEP: the annotated paths.
                "name": "HON. JEAN MUTOMBO", "sex": None,
                "nationality_fr": "CONGOLAISE", "residence": None,
                "control_type": None, "n_shares": 100,
                "pct_shares_raw": 0.7, "pct_shares": 70.0,
                "pct_voting_raw": 0.7, "pct_voting": 70.0,
                "pep": True, "pep_role": "DEPUTE NATIONAL",
                "acquired": "01/12/2006",
            },
            {
                # No interest data at all → unknownInterest fallback.
                "name": "MR.LI WEI", "sex": None,
                "nationality_fr": "CHINOISE", "residence": None,
                "control_type": None, "n_shares": 0,
                "pct_shares_raw": 0.0, "pct_shares": None,
                "pct_voting_raw": 0.0, "pct_voting": None,
                "pep": False, "pep_role": None, "acquired": None,
            },
        ],
    },
}

# A minimal but chain-complete BODS v0.2 declaration: subject company ← holding
# entity ← person, with a v0.2 kebab-case interest type and a string share.
_AM_BODS_02 = [
    {
        "statementID": "aaaa1111-0000-0000-0000-000000000001",
        "statementType": "entityStatement",
        "statementDate": "2026-02-01",
        "entityType": "registeredEntity",
        "name": "«ՓՈՐՁԱՐԿՈՒՄ ՄԱՅՆԻՆԳ»",
        "alternateNames": ["TEST MINING CJSC"],
        "identifiers": [{"id": "01234567", "scheme": "ARM-TAXID"}],
        "addresses": [
            {"type": "registered", "address": "Yerevan, Test st. 1", "country": "AM"}
        ],
        "publicationDetails": {
            "publicationDate": "2026-02-01", "bodsVersion": "0.2",
            "publisher": {"name": "---"},
        },
    },
    {
        "statementID": "aaaa1111-0000-0000-0000-000000000002",
        "statementType": "entityStatement",
        "statementDate": "2026-02-01",
        "entityType": "registeredEntity",
        "name": "ՀՈԼԴԻՆԳ ԹԵՍԹ",
        "alternateNames": ["HOLDING TEST LLC"],
        "identifiers": [{"id": "7700000000", "scheme": "RUS-TAXID"}],
        "publicationDetails": {
            "publicationDate": "2026-02-01", "bodsVersion": "0.2",
            "publisher": {"name": "---"},
        },
    },
    {
        "statementID": "bbbb2222-0000-0000-0000-000000000001",
        "statementType": "personStatement",
        "statementDate": "2026-02-01",
        "personType": "knownPerson",
        "names": [
            {"fullName": "Թեստ Անուն", "type": "individual"},
            {"fullName": "Test Anun", "type": "transliteration"},
        ],
        "nationalities": [{"code": "AM"}],
        "hasPepStatus": True,
        "publicationDetails": {
            "publicationDate": "2026-02-01", "bodsVersion": "0.2",
            "publisher": {"name": "---"},
        },
    },
    {
        "statementID": "cccc3333-0000-0000-0000-000000000001",
        "statementType": "ownershipOrControlStatement",
        "statementDate": "2026-02-01",
        "subject": {"describedByEntityStatement": "aaaa1111-0000-0000-0000-000000000001"},
        "interestedParty": {"describedByEntityStatement": "aaaa1111-0000-0000-0000-000000000002"},
        "interests": [
            {
                "type": "shareholding", "interestLevel": "direct",
                "beneficialOwnershipOrControl": False,
                "share": {"exact": "100.0000"}, "startDate": "2021-06-23",
            }
        ],
        "publicationDetails": {
            "publicationDate": "2026-02-01", "bodsVersion": "0.2",
            "publisher": {"name": "---"},
        },
    },
    {
        "statementID": "cccc3333-0000-0000-0000-000000000002",
        "statementType": "ownershipOrControlStatement",
        "statementDate": "2026-02-01",
        "subject": {"describedByEntityStatement": "aaaa1111-0000-0000-0000-000000000002"},
        "interestedParty": {"describedByPersonStatement": "bbbb2222-0000-0000-0000-000000000001"},
        "interests": [
            {
                "type": "voting-rights", "interestLevel": "indirect",
                "beneficialOwnershipOrControl": True,
                "share": {"exact": "51.5"},
            }
        ],
        "publicationDetails": {
            "publicationDate": "2026-02-01", "bodsVersion": "0.2",
            "publisher": {"name": "---"},
        },
    },
]

_AM_RECORD = {
    "lei": _LEI_AM,
    "lei_registration_status": "LAPSED",
    "register_id": "armenia_eregister",
    "country": "AM",
    "company": "«ՓՈՐՁԱՐԿՈՒՄ ՄԱՅՆԻՆԳ» ՓԲԸ",
    "company_latin": "Test Mining CJSC",
    "local_ids": {"am_regnum": "11.110.00001", "am_tin": "01234567"},
    "source_date": "2026-02-01",
    "retrieved": "2026-08-19T12:00:00+00:00",
    "match": {"method": "registration-number (registeredAs)", "confidence": "high"},
    "armenia": {
        "eregister_id": "1000000",
        "declaration_uuid": "dddd4444-0000-0000-0000-000000000001",
        "declaration_date": "2026-02-01 10:00",
        "declaration_url": (
            "https://old.e-register.am/en/companies/1000000/declaration/"
            "dddd4444-0000-0000-0000-000000000001"
        ),
        "declarations_on_register": 3,
        "bods_v02": _AM_BODS_02,
    },
}

_NG_RECORD = {
    "lei": _LEI_NG,
    "lei_registration_status": "ISSUED",
    "register_id": "nigeria_cac",
    "country": "NG",
    "company": "TEST CEMENT PLC",
    "local_ids": {"ng_cac_rc": "999999"},
    "source_date": "2026-08-12T00:00:00",
    "retrieved": "2026-08-12T00:00:00",
    "match": {"method": "cac_nigeria index (GLEIF RA000469)", "confidence": "high"},
    "neiti_filter_evidence": (
        "Covered by NEITI solid-minerals audit reports (data vintage ~2023)."
    ),
    "nigeria": {
        "company": "TEST CEMENT PLC",
        "rc": "999999",
        "lei": _LEI_NG,
        "pscs": [
            {
                "owner_name": "Jane Founder", "owner_kind": "person",
                "owner_rc": None, "owner_jurisdiction": None, "nationality": "NIGERIA",
                "notified": "2023-07-29", "shares": True, "share_pct_direct": 60.0,
                "share_pct_indirect": 0, "voting": True, "voting_pct_direct": 60.0,
                "voting_pct_indirect": None, "appoint_board": False,
                "sig_influence_company": False, "sig_influence_trust_firm": False,
            },
            {
                "owner_name": "Holdings Ltd", "owner_kind": "entity",
                "owner_rc": "111222", "owner_jurisdiction": "NG", "nationality": None,
                "notified": "2023-07-29", "shares": True, "share_pct_direct": 40.0,
                "share_pct_indirect": 0, "voting": False, "voting_pct_direct": None,
                "voting_pct_indirect": None, "appoint_board": False,
                "sig_influence_company": False, "sig_influence_trust_firm": False,
            },
        ],
    },
}

_FIXTURE_INDEX = {_LEI_DRC: _DRC_RECORD, _LEI_AM: _AM_RECORD, _LEI_NG: _NG_RECORD}
_FIXTURE_META = {
    "built": "2026-08-19T12:00:00+00:00",
    "entities": 3,
    "registers": {
        "drc_itie": {
            "name": "ITIE-RDC — Registre des propriétaires effectifs",
            "url": "https://www.itierdc.net/donnees/",
            "licence": "No licence stated; included with attribution.",
            "companies_harvested": 54, "lei_matched": 1,
        },
        "armenia_eregister": {
            "name": "Armenia State Register — beneficial ownership declarations",
            "url": "https://old.e-register.am/",
            "licence": "No licence stated; included with attribution.",
            "companies_harvested": 27, "lei_matched": 1,
        },
        "nigeria_cac": {
            "name": "Nigeria CAC — PSC register (NEITI solid-minerals subset)",
            "url": "https://bor.cac.gov.ng",
            "licence": "Public register.",
            "companies_harvested": 10, "lei_matched": 1,
        },
        "indonesia_ahu": {
            "name": "Indonesia AHU — Pemilik Manfaat register",
            "url": "https://ahu.go.id/pencarian/profil-pemilik-manfaat",
            "licence": "No licence stated.",
            "companies_harvested": 0, "lei_matched": 0,
        },
    },
}


@pytest.fixture(autouse=True)
def _inject_index(monkeypatch):
    monkeypatch.setattr(eiti_bo, "_index", dict(_FIXTURE_INDEX))
    monkeypatch.setattr(eiti_bo, "_meta", dict(_FIXTURE_META))
    yield
    eiti_bo._reset_index_for_tests()


@pytest.fixture
def adapter():
    return eiti_bo.EitiBoAdapter()


def _ctx(lei, name):
    return _LookupCtx(
        lei=lei, legal_name=name, jurisdiction="CD", registered_as="",
        derived={}, ocid=None, spglobal=None, qid=None,
    )


async def test_info(adapter):
    info = adapter.info
    assert info.id == "eiti_bo"
    assert info.category == "cdd"
    assert info.requires_api_key is False
    assert SearchKind.ENTITY in info.supports
    # Pooled multi-country source — not a single national register.
    assert info.is_national_register is False
    # Public registers included with attribution, no NC restriction.
    assert "non-commercial" not in info.license.lower()
    assert "cc-by-nc" not in info.license.lower()
    assert "attribution" in info.license.lower()


async def test_search_is_empty(adapter):
    assert await adapter.search("anything", SearchKind.ENTITY) == []


async def test_fetch_by_lei_match_miss_case(adapter):
    b = await adapter.fetch_by_lei(_LEI_DRC)
    assert b is not None and b["source_id"] == "eiti_bo"
    assert b["is_stub"] is False
    assert b["register_id"] == "drc_itie"
    assert b["register_name"].startswith("ITIE-RDC")
    assert await adapter.fetch_by_lei(_LEI_DRC.lower()) is not None
    assert await adapter.fetch_by_lei(_LEI_MISS) is None
    stub = await adapter.fetch(_LEI_MISS)
    assert stub["is_stub"] is True


# ---------------------------------------------------------------------------
# DRC sub-mapper
# ---------------------------------------------------------------------------


async def test_drc_mapping_shapes_and_validate(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_DRC)))
    assert validate_shape(stmts) == []
    kinds = [s["recordType"] for s in stmts]
    assert kinds.count("entity") == 1
    assert kinds.count("person") == 2
    assert kinds.count("relationship") == 2


async def test_drc_pep_honorific_share_and_dates(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_DRC)))
    people = [s for s in stmts if s["recordType"] == "person"]
    jean = next(
        p for p in people if p["recordDetails"]["names"][0]["fullName"] == "JEAN MUTOMBO"
    )
    # Honorific stripped, with a transformation annotation carrying the raw.
    ann = jean.get("annotations") or []
    assert any("HON. JEAN MUTOMBO" in a["description"] for a in ann)
    # PEP flag with the register's Fonction PPE as the reason.
    pe = jean["recordDetails"]["politicalExposure"]
    assert pe["status"] == "isPep"
    assert pe["details"][0]["reason"] == "DEPUTE NATIONAL"
    # Nationality from the register's French adjective.
    assert jean["recordDetails"]["nationalities"][0]["code"] == "CD"

    rels = [s for s in stmts if s["recordType"] == "relationship"]
    jean_rel = next(
        r for r in rels
        if r["recordDetails"]["interestedParty"] == jean["statementId"]
    )
    types = {i["type"]: i for i in jean_rel["recordDetails"]["interests"]}
    # Fraction-of-1 semantics normalised to percentages…
    assert types["shareholding"]["share"]["exact"] == 70.0
    assert types["votingRights"]["share"]["exact"] == 70.0
    # …with the raw register value annotated.
    assert any(
        "0.7" in a["description"] and a["motivation"] == "transformation"
        for a in (jean_rel.get("annotations") or [])
    )
    # DD/MM/YYYY acquisition date → ISO startDate.
    assert types["shareholding"]["startDate"] == "2006-12-01"
    # The owner with no interest data falls back to unknownInterest.
    other_rel = next(r for r in rels if r is not jean_rel)
    assert other_rel["recordDetails"]["interests"][0]["type"] == "unknownInterest"


# ---------------------------------------------------------------------------
# Armenia sub-mapper (BODS v0.2 → v0.4 upconversion)
# ---------------------------------------------------------------------------


async def test_armenia_upconversion_shapes_and_validate(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_AM)))
    assert validate_shape(stmts) == []
    kinds = [s["recordType"] for s in stmts]
    assert kinds.count("entity") == 2
    assert kinds.count("person") == 1
    assert kinds.count("relationship") == 2
    # Every output statement is v0.4, published by OpenCheck.
    assert all(s["publicationDetails"]["bodsVersion"] == "0.4" for s in stmts)


async def test_armenia_chain_references_and_interests(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_AM)))
    ids = {s["statementId"] for s in stmts}
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    for r in rels:
        assert r["recordDetails"]["subject"] in ids
        assert r["recordDetails"]["interestedParty"] in ids
    # v0.2 "voting-rights"/interestLevel/string-share → v0.4 codes and floats.
    person_rel = next(
        r for r in rels
        if any(
            p["statementId"] == r["recordDetails"]["interestedParty"]
            and p["recordType"] == "person"
            for p in stmts
        )
    )
    i = person_rel["recordDetails"]["interests"][0]
    assert i["type"] == "votingRights"
    assert i["directOrIndirect"] == "indirect"
    assert i["share"]["exact"] == 51.5
    assert i["beneficialOwnershipOrControl"] is True


async def test_armenia_subject_identifiers_and_provenance(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_AM)))
    subject = next(
        s for s in stmts
        if s["recordType"] == "entity"
        and any(
            i.get("scheme") == "AM-REG"
            for i in s["recordDetails"].get("identifiers", [])
        )
    )
    schemes = {i["scheme"] for i in subject["recordDetails"]["identifiers"]}
    assert {"ARM-TAXID", "AM-REG", "AM-TIN"} <= schemes
    assert subject["recordDetails"]["jurisdiction"]["code"] == "AM"
    # The upconversion is recorded, naming the original v0.2 statement.
    assert any(
        "BODS v0.2" in a["description"] for a in subject.get("annotations") or []
    )
    # source url points at the specific declaration.
    assert "declaration" in subject["source"]["url"]


async def test_armenia_person_keeps_register_transliteration_and_pep(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_AM)))
    person = next(s for s in stmts if s["recordType"] == "person")
    names = person["recordDetails"]["names"]
    assert any(
        n["type"] == "transliteration" and n["fullName"] == "Test Anun" for n in names
    )
    assert person["recordDetails"]["politicalExposure"]["status"] == "isPep"


# ---------------------------------------------------------------------------
# Nigeria sub-mapper (CAC ∩ NEITI subset)
# ---------------------------------------------------------------------------


async def test_nigeria_mapping_and_neiti_evidence(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_NG)))
    assert validate_shape(stmts) == []
    subject = stmts[0]
    assert subject["recordDetails"]["name"] == "TEST CEMENT PLC"
    # The (dated) NEITI extractives filter evidence is annotated on the subject.
    assert any(
        "NEITI" in a["description"] for a in subject.get("annotations") or []
    )
    # BOC flag: person True, entity False (CAC interest mapping reused).
    by_id = {s["statementId"]: s for s in stmts}
    for r in (s for s in stmts if s["recordType"] == "relationship"):
        party = by_id[r["recordDetails"]["interestedParty"]]
        boc = {i["beneficialOwnershipOrControl"] for i in r["recordDetails"]["interests"]}
        assert boc == {party["recordType"] == "person"}


# ---------------------------------------------------------------------------
# Hit builder + dispatch gating
# ---------------------------------------------------------------------------


async def test_hit_builder_asserts_register_ids_not_lei(adapter):
    b = await adapter.fetch_by_lei(_LEI_AM)
    hit = _bh_eiti_bo(b, _ctx(_LEI_AM, "Test Mining CJSC"))
    assert hit.source_id == "eiti_bo"
    assert hit.identifiers == {"am_regnum": "11.110.00001", "am_tin": "01234567"}
    assert "lei" not in hit.identifiers  # corroboration rule
    # Latin display name preferred over the register's Armenian-script name.
    assert hit.name == "Test Mining CJSC"
    assert "Armenia State Register" in hit.summary
    assert "register data 2026-02-01" in hit.summary

    b = await adapter.fetch_by_lei(_LEI_DRC)
    hit = _bh_eiti_bo(b, _ctx(_LEI_DRC, "KOLWEZI TEST MINING"))
    assert hit.identifiers == {"cd_nif": "A0000001X"}
    assert "2 beneficial owners" in hit.summary
    assert "1 PEP" in hit.summary
    assert "possible name match" in hit.summary  # medium-confidence match


async def test_build_result_hit_gates_on_record(adapter):
    b = await adapter.fetch_by_lei(_LEI_NG)
    assert _build_result_hit("eiti_bo", b, _ctx(_LEI_NG, "TEST CEMENT PLC")) is not None
    assert _build_result_hit("eiti_bo", None, _ctx(_LEI_MISS, "x")) is None


async def test_source_block_is_official_register(adapter):
    stmts = list(map_eiti_bo(await adapter.fetch_by_lei(_LEI_DRC)))
    src = stmts[0]["source"]
    assert src["type"] == ["officialRegister"]
    assert "beneficial ownership registers" in src["description"]


def test_registered_in_registry():
    assert REGISTRY.get("eiti_bo") is not None


# ---------------------------------------------------------------------------
# Regression: the real committed index maps cleanly (catches bad harvests)
# ---------------------------------------------------------------------------


async def test_committed_index_maps_and_validates():
    """Run the shipped opencheck/data/eiti_bo_index.json.gz through the mapper."""
    eiti_bo._reset_index_for_tests()  # ignore the fixture; load the real file
    adapter = eiti_bo.EitiBoAdapter()
    index, meta = eiti_bo._load()
    assert len(index) >= 3, "expected at least 3 LEI-matched pooled entities"
    registers = meta.get("registers") or {}
    # The manifest documents every pooled register — including the deferred
    # Indonesia slot and the 0-LEI DRC outcome — with harvest counts.
    assert {"drc_itie", "armenia_eregister", "nigeria_cac", "indonesia_ahu"} <= set(
        registers
    )
    for reg in registers.values():
        assert isinstance(reg.get("companies_harvested"), int)
        assert isinstance(reg.get("lei_matched"), int)
    for lei, record in index.items():
        bundle = await adapter.fetch_by_lei(lei)
        stmts = list(map_eiti_bo(bundle))
        assert validate_shape(stmts) == [], f"{lei} produced invalid BODS"
        assert any(s["recordType"] == "entity" for s in stmts)
        assert any(s["recordType"] == "relationship" for s in stmts)
        hit = _bh_eiti_bo(bundle, _ctx(lei, record.get("company") or lei))
        assert "lei" not in hit.identifiers  # corroboration rule
        assert hit.summary  # every register branch produces a summary
    eiti_bo._reset_index_for_tests()
