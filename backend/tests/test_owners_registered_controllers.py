"""``opencheck_owners``' fourth list: legal entities a register names as
controlling the company (MCP testing, 7 Nov 2026, issue 2).

Taqa Bratani Limited's answer said "No party is calculated at or above the
threshold. 0 current declared beneficial owner(s)" while Companies House names
The Abu Dhabi Government as its person with significant control and the same
lookup's ``disclosure_status`` said "Beneficial owners: Declared (1)". The
PSC is a relevant legal entity: its interest says
``beneficialOwnershipOrControl: false`` by the regime's own definition, so the
beneficial-owner list rightly leaves it out — and nothing listed it.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from opencheck import integrated_ownership as io
from opencheck import owners as O
from opencheck.bods.statements import set_beneficial_ownership

AS_OF = "2026-11-07"
NAMES = {
    "companies_house": "UK Companies House", "gleif": "GLEIF", "wikidata": "Wikidata",
    "ariregister": "Estonian e-Business Register (e-Äriregister)", "cac_nigeria": "Nigeria CAC",
    "sec_edgar": "SEC EDGAR (Schedule 13D/13G)",
}


def src(s):
    return {"description": NAMES[s], "opencheckSourceId": s}


def ent(i, n, s, lei=None, jur=None):
    rd = {"entityType": {"type": "registeredEntity"}, "name": n}
    if lei:
        rd["identifiers"] = [{"scheme": "XI-LEI", "id": lei}]
    if jur:
        rd["jurisdiction"] = {"name": jur[0], "code": jur[1]}
    return {"statementId": i, "recordId": i, "recordType": "entity", "recordDetails": rd, "source": src(s)}


def per(i, n, s):
    return {"statementId": i, "recordId": i, "recordType": "person",
            "recordDetails": {"personType": "knownPerson", "names": [{"type": "legal", "fullName": n}]},
            "source": src(s)}


def rel(i, party, s, interest, *, subj="T", end=None):
    it = dict(interest)
    if end:
        it["endDate"] = end
    return {"statementId": i, "recordId": i, "recordType": "relationship",
            "recordDetails": {"isComponent": False, "subject": subj, "interestedParty": party,
                              "interests": [it]}, "source": src(s)}


def flagged(source, kind, **interest):
    """An interest flagged by the source's own regime policy, as its mapper does."""
    return set_beneficial_ownership(dict(interest), source, record_kind=kind)


def answer(bods, jurisdiction="GB", disclosure=None):
    rule = O.resolve_rule(jurisdiction)

    async def no_hop(lei, anchor):
        raise AssertionError("depth 1 must not hop")

    net = asyncio.run(O.grow(list(bods), ["T"], None, rule, 1, no_hop, AS_OF, None))
    return O.owners_answer(lei="LEI", legal_name="TAQA BRATANI LIMITED", jurisdiction=jurisdiction,
                           net=net, rule=rule, depth=1, as_of=AS_OF, disclosure=disclosure)


def _taqa():
    """Production shape of 7 Nov 2026: a corporate PSC on Companies House,
    GLEIF's consolidating parent, and a director."""
    rle = flagged("companies_house", "psc_corporate_rle", type="otherInfluenceOrControl",
                  details="significant-influence-or-control", startDate="2016-04-06")
    return [
        ent("T", "TAQA BRATANI LIMITED", "companies_house", lei="213800E11LI1SCETU492"),
        ent("ADG", "The Abu Dhabi Government", "companies_house", jur=("United Arab Emirates", "AE")),
        ent("TAQA", "Abu Dhabi National Energy Company PJSC", "gleif", lei="5493008BVKGY4JLLHM93"),
        per("D", "A Director", "companies_house"),
        rel("psc", "ADG", "companies_house", rle),
        rel("parent", "TAQA", "gleif", {"type": "otherInfluenceOrControl"}),
        rel("dir", "D", "companies_house", {"type": "seniorManagingOfficial"}),
    ]


def test_the_policy_flags_a_uk_rle_false():
    # The list keys on this; if the regime ever changes it, this test says so.
    assert _taqa()[4]["recordDetails"]["interests"][0]["beneficialOwnershipOrControl"] is False


def test_a_corporate_psc_is_a_registered_controller_never_a_beneficial_owner():
    out = answer(_taqa())
    assert out["declared_beneficial_owners"] == []
    [c] = out["registered_controllers"]
    assert c["name"] == "The Abu Dhabi Government"
    assert c["source"] == "UK Companies House"
    assert c["kind"] == "entity" and c["jurisdiction"] == "AE" and c["current"] is True
    assert "Named on a beneficial-ownership register as a controlling legal entity" in out["summary"]
    assert "The Abu Dhabi Government (UK Companies House)" in out["summary"]


def test_gleif_consolidation_and_officers_are_not_registered_controllers():
    out = answer(_taqa())
    names = {c["name"] for c in out["registered_controllers"]}
    assert "Abu Dhabi National Energy Company PJSC" not in names  # consolidation, not a BO register
    assert "A Director" not in names


def test_a_non_register_source_flagging_an_entity_false_is_not_listed():
    # Wikidata's owner_entity is assert_false too, but Wikidata is not a register.
    it = flagged("wikidata", "owner_entity", type="shareholding", share={"exact": 10})
    out = answer([ent("T", "Target", "companies_house"), ent("O", "Owner Co", "wikidata"),
                  rel("w", "O", "wikidata", it)])
    assert out["registered_controllers"] == []


def test_a_person_named_without_the_flag_is_not_listed():
    # Estonia's J/C fallback board member: a person, flagged false, a role.
    it = flagged("ariregister", "kasusaaja_fallback", type="seniorManagingOfficial")
    out = answer([ent("T", "Target", "ariregister"), per("B", "Board Member", "ariregister"),
                  rel("f", "B", "ariregister", it)], jurisdiction="EE")
    assert out["registered_controllers"] == []


def test_a_ceased_rle_is_listed_as_not_current():
    rle = flagged("companies_house", "psc_corporate_rle", type="shareholding", share={"minimum": 75, "maximum": 100})
    out = answer([ent("T", "Target", "companies_house"), ent("H", "Holdco Ltd", "companies_house"),
                  rel("p", "H", "companies_house", rle, end="2020-01-31")])
    [c] = out["registered_controllers"]
    assert c["current"] is False and c["ended_on"] == "2020-01-31"
    assert "Named on a beneficial-ownership register" not in out["summary"]


def test_a_nigerian_corporate_psc_is_listed():
    it = flagged("cac_nigeria", "psc_corporate", type="shareholding", share={"exact": 51})
    out = answer([ent("T", "Target", "cac_nigeria"), ent("C", "Parent Plc", "cac_nigeria"),
                  rel("c", "C", "cac_nigeria", it)], jurisdiction="NG")
    assert [c["name"] for c in out["registered_controllers"]] == ["Parent Plc"]


def test_an_rle_that_is_also_a_calculated_owner_is_in_both():
    rle = flagged("companies_house", "psc_corporate_rle", type="shareholding", share={"exact": 100})
    out = answer([ent("T", "Target", "companies_house"), ent("H", "Holdco Ltd", "companies_house"),
                  rel("p", "H", "companies_house", rle)])
    assert [b["name"] for b in out["in_both"]] == ["Holdco Ltd"]


def test_an_empty_answer_says_what_the_sources_declare():
    # Every list empty, but the lookup's disclosure_status has a declaration
    # (Eli Lilly before the CIK join): the summary must not read as "no owners".
    disclosure = {"by_source": [
        {"source": "sec_edgar", "source_name": "SEC EDGAR", "values": ["declared"],
         "line": "Shareholders: Declared (3)"},
        {"source": "gleif", "source_name": "GLEIF", "values": ["exempt"], "line": "Parent: Exempt"},
    ]}
    out = answer([ent("T", "Target", "companies_house")], jurisdiction="US", disclosure=disclosure)
    assert "What the sources say about owners: SEC EDGAR — Shareholders: Declared (3)." in out["summary"]
    assert "GLEIF" not in out["summary"]


def test_a_non_empty_answer_does_not_repeat_the_disclosure_lines():
    disclosure = {"by_source": [{"source": "companies_house", "source_name": "UK Companies House",
                                 "values": ["declared"], "line": "Beneficial owners: Declared (1)"}]}
    out = answer(_taqa(), disclosure=disclosure)
    assert "What the sources say about owners" not in out["summary"]


def test_the_declared_owner_count_names_them():
    it = flagged("companies_house", "psc_individual", type="shareholding", share={"minimum": 75, "maximum": 100})
    out = answer([ent("T", "Target", "companies_house"), per("P", "Pat Doe", "companies_house"),
                  rel("p", "P", "companies_house", it)])
    assert "1 current declared beneficial owner(s): Pat Doe (UK Companies House)" in out["summary"]


def test_the_mcp_tool_carries_the_list(monkeypatch):
    pytest.importorskip("mcp")
    from opencheck.mcp import server
    from opencheck.routers import lookup as L

    bods = _taqa()

    async def fake(lei, deepen_top=5, **_):
        return SimpleNamespace(lei=lei, legal_name="TAQA BRATANI LIMITED", jurisdiction="GB", bods=bods,
                               integrated_ownership=io.for_lookup(lei, bods, AS_OF), license_notices=[])

    monkeypatch.setattr(L, "_lookup_impl", fake)
    out = asyncio.run(server.opencheck_owners(lei="213800E11LI1SCETU492"))
    assert [c["name"] for c in out["registered_controllers"]] == ["The Abu Dhabi Government"]
