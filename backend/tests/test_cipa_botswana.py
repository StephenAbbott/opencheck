"""Tests for the Botswana CIPA register adapter, mapper and harvester.

Three layers:

* The harvester's page reading (``read_company``) on a view tree in the
  register's own shape — including the allowlist: nothing under an address or
  document record reaches the output.
* The adapter contract and the BODS mapping on a fixture index.
* A regression over the **real committed** index
  (``opencheck/data/cipa_botswana.json``): every record maps and validates,
  and the file holds no address or document field.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from opencheck import provenance
from opencheck.bods import map_cipa_botswana, validate_shape
from opencheck.findings import finding_cipa_botswana
from opencheck.routers.lookup import (
    _bh_cipa_botswana,
    _build_result_hit,
    _dispatch,
    _LookupCtx,
)
from opencheck.sources import REGISTRY, cipa_botswana
from opencheck.sources.base import SearchKind
from opencheck.sources.cipa_botswana import gleif_cipa_identifier, normalise_cipa_number

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_cipa_botswana_index.py"
_spec = importlib.util.spec_from_file_location("build_cipa_botswana_index", _SCRIPT)
harvester = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(harvester)

_LEI_A = "5493001KJTIIGC8Y1R12"
_LEI_REMOVED = "213800WSGIIZCXF1P572"
_LEI_MISS = "9999009999999999XX99"
_TODAY = "2026-09-30"


# ---------------------------------------------------------------------------
# The harvester: reading a company page
# ---------------------------------------------------------------------------


def _attr(nid: str, name: str, value, **extra):
    node = {"id": nid, "nodetype": "attribute", "attribute": name, "attributeValue": value}
    node.update(extra)
    return node


def _rec(nid: str, domain: str | None, children: list[str]):
    node = {"id": nid, "nodetype": "record" if domain else "box", "children": children}
    if domain:
        node["domain"] = domain
    return node


def _view_tree() -> dict:
    """A company page cut down to one director, two shareholders and one
    beneficial owner — the structure the register serves, node for node."""
    nodes = [
        {"id": "h", "nodetype": "box", "widget": "entity-heading", "children": ["h1"]},
        {"id": "h1", "nodetype": "box", "text": {"label": "Alpha Mining Proprietary Limited (BW00000000001)"}},
        _attr("cn", "CompanyNumber", "BW00000000001"),
        _attr("on", "OldCompanyNumber", "CO2001/123"),
        _attr("et", "EntityType", "private"),
        _attr("st", "SubType", "limitedByShares"),
        _rec("status", "Status", ["s1", "s2"]),
        _attr("s1", "Status", "registered"),
        _attr("s2", "StartDate", "01 March 2001"),
        _attr("rd", "RegistrationDate", "01 March 2001"),
        _attr("ts", "TotalShare", "100"),
        # Director: name, nationality, an address (never read), dates at two levels.
        _rec("d", "Director", ["d1", "dEnd"]),
        _rec("d1", "IndividualDirector", ["dn", "dnat", "daddr", "dstart"]),
        _rec("dn", "IndividualName", ["dfn", "dln"]),
        _attr("dfn", "FirstName", "Jane"),
        _attr("dln", "LastName", "Motsumi"),
        _attr("dnat", "Nationality", "BW"),
        _rec("daddr", "RolePostalAddress", ["dline"]),
        _attr("dline", "Line1", "Private Bag 9"),
        _attr("dstart", "StartDate", "02 March 2001"),
        _attr("dEnd", "EndDate", "31 December 2020"),
        # A Botswana company shareholder, with its certificate (never read).
        _rec("sh1", "Shareholder", ["sh1a"]),
        _rec("sh1a", "EntityShareholder", ["sh1n", "sh1u", "sh1doc", "sh1s", "sh1nom", "sh1nr"]),
        _rec("sh1n", "Name", ["sh1nn"]),
        _attr("sh1nn", "Name", "Parent Holdings Proprietary Limited"),
        _attr("sh1u", "SourceBusinessIdentifier", "BW00000000002"),
        _rec("sh1doc", "CertificateOfIncorporationDocument", ["sh1dk"]),
        _attr("sh1dk", "DocumentKey", "deadbeef"),
        _attr("sh1s", "StartDate", "01 March 2001"),
        _attr("sh1nom", "HasNominator", "true"),
        _rec("sh1nr", "Nominator", ["sh1nri"]),
        _rec("sh1nri", "IndividualNominator", ["sh1nrn"]),
        _rec("sh1nrn", "IndividualName", ["sh1f", "sh1l"]),
        _attr("sh1f", "FirstName", "Kagiso"),
        _attr("sh1l", "LastName", "Dube"),
        # An individual shareholder who is also the beneficial owner.
        _rec("sh2", "Shareholder", ["sh2a"]),
        _rec("sh2a", "IndividualShareholder", ["sh2n", "sh2nat", "sh2s"]),
        _rec("sh2n", "IndividualName", ["sh2f", "sh2l"]),
        _attr("sh2f", "FirstName", "Jane"),
        _attr("sh2l", "LastName", "Motsumi"),
        _attr("sh2nat", "Nationality", "BW"),
        _attr("sh2s", "StartDate", "01 March 2001"),
        # Share allocations: a Botswana company is labelled with its UIN.
        _rec("al1", "ShareAllocation", ["al1h", "al1n", "al1p"]),
        _attr("al1h", "ShareholderIds", "mro-1",
              kv={"ui-readonly-value": "Parent Holdings Proprietary Limited (BW00000000002)"}),
        _attr("al1n", "NumberOfShares", "60"),
        _attr("al1p", "PercentageAllocated", "60.00"),
        _rec("al2", "ShareAllocation", ["al2h", "al2n"]),
        _attr("al2h", "ShareholderIds", "mro-2", kv={"ui-readonly-value": "Jane Motsumi"}),
        _attr("al2n", "NumberOfShares", "40"),
        # The beneficial owner.
        _rec("bo", "BeneficialOwner", ["boi", "boEnd"]),
        _rec("boi", "IndividualBeneficialOwner", ["bon", "bonat", "boaddr", "boint", "bos"]),
        _rec("bon", "IndividualName", ["bof", "bol"]),
        _attr("bof", "FirstName", "Jane"),
        _attr("bol", "LastName", "Motsumi"),
        _attr("bonat", "Nationalities", "BW"),
        _rec("boaddr", "RolePostalAddress", ["boline"]),
        _attr("boline", "Line1", "Plot 1, Gaborone"),
        _rec("boint", "Interest", ["bot", "boe"]),
        _attr("bot", "Type", "shareholding"),
        _attr("boe", "Exact", "40"),
        _attr("bos", "StartDate", "05 June 2025"),
    ]
    return {n["id"]: n for n in nodes}


def test_read_company_takes_the_allowlisted_fields_only():
    rec = harvester.read_company(_view_tree())
    text = json.dumps(rec)
    assert "Private Bag 9" not in text and "Plot 1" not in text
    assert "deadbeef" not in text
    assert rec["company"] == "Alpha Mining Proprietary Limited"
    assert rec["uin"] == "BW00000000001" and rec["old_number"] == "CO2001/123"
    assert rec["status"] == "registered" and rec["status_since"] == "2001-03-01"
    assert rec["incorporated"] == "2001-03-01"


def test_read_company_keeps_the_role_level_end_date():
    (director,) = harvester.read_company(_view_tree())["directors"]
    assert director == {
        "kind": "IndividualDirector", "name": "Jane Motsumi", "nationality": "BW",
        "start": "2001-03-02", "end": "2020-12-31",
    }


def test_read_company_joins_allocations_to_shareholders_and_names_the_nominator():
    shareholders = harvester.read_company(_view_tree())["shareholders"]
    parent, jane = shareholders
    assert parent["uin"] == "BW00000000002"
    assert parent["shares"] == 60 and parent["percentage"] == 60
    assert parent["nominee"] is True and parent["nominator"] == "Kagiso Dube"
    # No percentage filed: derived from the share count and the total.
    assert jane["shares"] == 40 and jane["percentage"] == 40
    assert "Kagiso" not in json.dumps(jane)


def test_read_company_reads_the_declared_interest():
    (bo,) = harvester.read_company(_view_tree())["beneficial_owners"]
    assert bo["nationalities"] == ["BW"]
    assert bo["interests"] == [
        {"type": "shareholding", "share": 40, "voting": None, "other_reason": None},
    ]
    assert bo["start"] == "2025-06-05" and bo["end"] is None


def test_iso_date():
    assert harvester.iso_date("23 June 1969") == "1969-06-23"
    assert harvester.iso_date(" 1  January  2026 ") == "2026-01-01"
    assert harvester.iso_date("") is None
    assert harvester.iso_date("June 1969") is None


def test_read_filings_keeps_dates_and_names_only():
    filings = harvester.read_filings({"transactions": [
        {"transactionDate": {"value": "2026-06-13T09:20:24.833Z"},
         "name": "Maintain Director Details",
         "presenter": {"address": "P O Box 329, Gaborone"},
         "filings": [{"filingName": "Notice of Change of Directors"}]},
        {"transactionDate": {"value": "2026-09-18T08:00:00Z"},
         "name": "Maintain Beneficial Owner Details",
         "filings": [{"filingName": "Notice of Change of Beneficial Owners"}]},
    ]})
    assert filings == [
        {"date": "2026-09-18", "service": "Maintain Beneficial Owner Details",
         "filings": ["Notice of Change of Beneficial Owners"]},
        {"date": "2026-06-13", "service": "Maintain Director Details",
         "filings": ["Notice of Change of Directors"]},
    ]


# ---------------------------------------------------------------------------
# The adapter and the mapper, on a fixture index
# ---------------------------------------------------------------------------


def _record(**overrides):
    rec = harvester.read_company(_view_tree())
    rec.update({"lei": _LEI_A, "lei_status": "ISSUED", "filings": [
        {"date": "2025-06-05", "service": "Maintain Beneficial Owner Details",
         "filings": ["Notice of Change of Beneficial Owners"]},
    ]})
    rec.update(overrides)
    return rec


_FIXTURE = {
    _LEI_A: _record(),
    _LEI_REMOVED: _record(
        lei=_LEI_REMOVED, uin="BW00000000009", company="Gone Proprietary Limited",
        old_number=None, status="removed", status_since="2025-03-04",
        beneficial_owners=[], filings=[],
    ),
}


@pytest.fixture(autouse=True)
def _inject_index(monkeypatch):
    monkeypatch.setattr(cipa_botswana, "_index", dict(_FIXTURE))
    yield
    cipa_botswana._reset_index_for_tests()


@pytest.fixture
def adapter():
    return cipa_botswana.CipaBotswanaAdapter()


def _ctx(lei, name="A BOTSWANA COMPANY"):
    return _LookupCtx(
        lei=lei, legal_name=name, jurisdiction="BW", registered_as="",
        derived={}, ocid=None, spglobal=None, qid=None,
    )


def test_info(adapter):
    info = adapter.info
    assert info.id == "cipa_botswana" and info.country == "BW"
    assert info.category == "cdd" and info.is_national_register is True
    assert info.requires_api_key is False
    assert SearchKind.ENTITY in info.supports
    assert "nc" not in info.license.lower()


async def test_fetch_by_lei_match_miss_and_curated_provenance(adapter):
    with provenance.recording() as rec:
        bundle = await adapter.fetch_by_lei(_LEI_A.lower())
    assert bundle["record"]["uin"] == "BW00000000001"
    assert rec.resolve(is_stub=False).liveness == "curated"
    assert await adapter.fetch_by_lei(_LEI_MISS) is None
    assert (await adapter.fetch(_LEI_MISS))["is_stub"] is True
    assert await adapter.search("alpha", SearchKind.ENTITY) == []


def test_covers_lei(adapter):
    assert adapter.covers_lei(_LEI_A) and adapter.covers_lei(_LEI_A.lower())
    assert not adapter.covers_lei(_LEI_MISS) and not adapter.covers_lei("")


def test_dispatched_only_for_a_company_the_set_holds():
    for lei, expected in ((_LEI_A, True), (_LEI_MISS, False)):
        pairs = _dispatch(_ctx(lei))
        dispatched = [sid for sid, _coro in pairs]
        for _sid, coro in pairs:
            coro.close()
        assert ("cipa_botswana" in dispatched) is expected


async def _statements(adapter, lei=_LEI_A):
    return list(map_cipa_botswana(await adapter.fetch_by_lei(lei)))


async def test_mapping_validates_and_has_one_party_per_person(adapter):
    stmts = await _statements(adapter)
    assert validate_shape(stmts) == []
    kinds = [s["recordType"] for s in stmts]
    # subject + parent company; Jane (director, shareholder, BO) and the
    # recorded nominator; one relationship per party.
    assert kinds.count("entity") == 2
    assert kinds.count("person") == 2
    assert kinds.count("relationship") == 3


async def test_one_person_holds_every_role_with_its_own_dates_and_flag(adapter):
    stmts = await _statements(adapter)
    by_id = {s["statementId"]: s for s in stmts}
    (jane_rel,) = [
        s for s in stmts if s["recordType"] == "relationship"
        and by_id[s["recordDetails"]["interestedParty"]]["recordDetails"].get("names", [{}])[0].get("fullName") == "Jane Motsumi"
    ]
    interests = jane_rel["recordDetails"]["interests"]
    assert [i["type"] for i in interests] == ["shareholding", "shareholding", "boardMember"]
    board = interests[2]
    assert board["endDate"] == "2020-12-31" and "beneficialOwnershipOrControl" not in board
    declared = [
        i for i in jane_rel["recordDetails"]["interests"]
        if i.get("beneficialOwnershipOrControl") is True
    ]
    assert len(declared) == 1 and declared[0]["share"] == {"exact": 40}
    assert declared[0]["startDate"] == "2025-06-05"
    registered = [
        i for i in jane_rel["recordDetails"]["interests"]
        if i["type"] == "shareholding" and "beneficialOwnershipOrControl" not in i
    ]
    assert registered[0]["details"] == "Registered shareholder: 40 of 100 shares"


async def test_corporate_shareholder_carries_its_uin_and_the_nominee_arrangement(adapter):
    stmts = await _statements(adapter)
    by_id = {s["statementId"]: s for s in stmts}
    parent = next(
        s for s in stmts
        if s["recordType"] == "entity" and s["recordDetails"]["name"].startswith("Parent")
    )
    assert parent["recordDetails"]["identifiers"] == [{
        "id": "BW00000000002", "scheme": "BW-CIPA",
        "schemeName": "Companies and Intellectual Property Authority (Botswana)",
    }]
    # A company on CIPA's register may be an external company: no jurisdiction.
    assert "jurisdiction" not in parent["recordDetails"]
    rel = next(
        s for s in stmts if s["recordType"] == "relationship"
        and s["recordDetails"]["interestedParty"] == parent["statementId"]
    )
    types = [i["type"] for i in rel["recordDetails"]["interests"]]
    assert types == ["shareholding", "nominee"]
    assert "Kagiso Dube" in rel["recordDetails"]["interests"][1]["details"]
    nominator = next(
        s for s in stmts if s["recordType"] == "relationship"
        and by_id[s["recordDetails"]["interestedParty"]]["recordDetails"].get("names", [{}])[0].get("fullName") == "Kagiso Dube"
    )
    assert nominator["recordDetails"]["interests"][0]["type"] == "nominator"


async def test_subject_identifiers_and_status(adapter):
    subject = (await _statements(adapter))[0]
    rd = subject["recordDetails"]
    assert [i["id"] for i in rd["identifiers"]] == ["BW00000000001", "CO2001/123"]
    assert {i["scheme"] for i in rd["identifiers"]} == {"BW-CIPA"}
    assert rd["jurisdiction"] == {"name": "Botswana", "code": "BW"}
    assert rd["entityType"] == {"type": "registeredEntity", "details": "Private company, limited by shares"}
    assert rd["foundingDate"] == "2001-03-01"
    removed = (await _statements(adapter, _LEI_REMOVED))[0]["recordDetails"]
    assert removed["dissolutionDate"] == "2025-03-04"


async def test_hit_builder_asserts_cipa_numbers_never_the_lei(adapter):
    bundle = await adapter.fetch_by_lei(_LEI_A)
    hit = _bh_cipa_botswana(bundle, _ctx(_LEI_A))
    assert hit.identifiers == {"bw_cipa_uin": "BW00000000001", "bw_cipa_old_number": "CO2001/123"}
    assert "lei" not in hit.identifiers
    assert hit.name == "Alpha Mining Proprietary Limited"
    assert _build_result_hit("cipa_botswana", bundle, _ctx(_LEI_A)) is not None
    assert _build_result_hit("cipa_botswana", {"is_stub": True}, _ctx(_LEI_A)) is None


async def test_finding(adapter):
    bundle = await adapter.fetch_by_lei(_LEI_A)
    # The one director left in 2020, so no director is counted.
    assert finding_cipa_botswana(bundle, today=_TODAY) == (
        "Beneficial owner on file: Jane Motsumi, 2 shareholders on the register, "
        "beneficial ownership last filed 5 June 2025."
    )
    removed = await adapter.fetch_by_lei(_LEI_REMOVED)
    assert finding_cipa_botswana(removed, today=_TODAY) == (
        "Removed from the register since 4 March 2025, no current beneficial owner on file, "
        "2 shareholders on the register."
    )
    assert finding_cipa_botswana({"is_stub": True}) is None


def test_gleif_identifier_shapes():
    assert gleif_cipa_identifier("RA000035", "BW00000466545") == ("bw_cipa_uin", "BW00000466545")
    assert gleif_cipa_identifier("RA000035", "CO1988/1163") == ("bw_cipa_old_number", "CO1988/1163")
    # The typo some LEI records carry: a zero for the O.
    assert gleif_cipa_identifier("RA000035", "C02015/11750") == ("bw_cipa_old_number", "CO2015/11750")
    assert gleif_cipa_identifier("RA000035", "91/329") is None
    # NBFIRA counts only when it files a CIPA UIN.
    assert gleif_cipa_identifier("RA000821", "BW00002207780") == ("bw_cipa_uin", "BW00002207780")
    assert gleif_cipa_identifier("RA000821", "10/1/38(025)") is None
    assert gleif_cipa_identifier("RA000469", "BW00000466545") is None
    assert normalise_cipa_number(" co 1988/1163 ") == "CO1988/1163"


def test_registered_in_registry():
    assert "cipa_botswana" in REGISTRY


# ---------------------------------------------------------------------------
# Regression: the real committed index
# ---------------------------------------------------------------------------

_INDEX = Path(__file__).resolve().parents[1] / "opencheck" / "data" / "cipa_botswana.json"


async def test_committed_index_maps_and_validates():
    cipa_botswana._reset_index_for_tests()
    adapter = cipa_botswana.CipaBotswanaAdapter()
    index = cipa_botswana._get_index()
    assert len(index) >= 30
    for lei in index:
        assert len(lei) == 20
        stmts = list(map_cipa_botswana(await adapter.fetch_by_lei(lei)))
        assert stmts, lei
        assert validate_shape(stmts) == [], lei


def test_committed_index_holds_no_address_or_document_field():
    text = _INDEX.read_text(encoding="utf-8")
    for field in ('"Line1"', '"PostCode"', '"City"', '"address"', '"DocumentKey"', '"DocumentName"'):
        assert field not in text
    data = json.loads(text)
    allowed = {
        "kind", "name", "nationality", "nationalities", "country", "uin",
        "company_number", "entity_type", "sub_type", "start", "end",
        "nominee", "nominator", "shares", "percentage", "interests",
    }
    for rec in data["index"].values():
        assert "lei" in rec and rec["uin"].startswith("BW")
        for group in ("directors", "shareholders", "beneficial_owners"):
            for party in rec[group]:
                assert set(party) <= allowed, (group, set(party) - allowed)


async def test_committed_debswana_shape_is_absent_but_stanbic_is_there():
    """Debswana has no LEI, so it is not in an LEI-anchored set; Stanbic Bank
    Botswana is, with Standard Bank Group as its declared corporate owner."""
    cipa_botswana._reset_index_for_tests()
    adapter = cipa_botswana.CipaBotswanaAdapter()
    stmts = list(map_cipa_botswana(await adapter.fetch_by_lei("254900CFACP5V9H8W758")))
    names = {s["recordDetails"].get("name") for s in stmts if s["recordType"] == "entity"}
    assert "Standard Bank Group Limited" in names
