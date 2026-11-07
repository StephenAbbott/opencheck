"""One state, one row and one node (MCP testing, 7 Nov 2026, issue 4).

Equinor's ``opencheck_owners`` listed FINANSDEPARTEMENTET (OpenSanctions),
Government of Norway (GEM) and the Ministry of Trade, Industry and Fisheries
(Wikidata) as three 67% owners, and the map drew three nodes. Stephen: fold
states only, by country — as ``STATE_CONTROLLED`` has since Phase 240 — never
companies by name. The figures are computed per body and listed per source;
a state's row never adds them.
"""

from __future__ import annotations

import asyncio
import re

from opencheck import owners as O

AS_OF = "2026-11-07"
NAMES = {"opensanctions": "OpenSanctions", "wikidata": "Wikidata",
         "climatetrace": "Global Energy Monitor / Climate TRACE"}


def ent(i, n, s, etype="registeredEntity", country=None):
    rd = {"entityType": {"type": etype}, "name": n}
    if country:
        rd["jurisdiction"] = {"name": country, "code": country}
    return {"statementId": i, "recordId": i, "recordType": "entity", "recordDetails": rd,
            "source": {"description": NAMES[s], "opencheckSourceId": s}}


def rel(i, party, share, s, t="shareholding"):
    it = {"type": t}
    if share is not None:
        it["share"] = {"exact": share}
    return {"statementId": i, "recordId": i, "recordType": "relationship",
            "recordDetails": {"isComponent": False, "subject": "T", "interestedParty": party,
                              "interests": [it]},
            "source": {"description": NAMES[s], "opencheckSourceId": s}}


def _equinor():
    return [
        ent("T", "EQUINOR ASA", "opensanctions"),
        ent("FIN", "FINANSDEPARTEMENTET", "opensanctions", "stateBody", "NO"),
        ent("GOV", "Government of Norway", "climatetrace", "state", "NO"),
        ent("MIN", "Ministry of Trade, Industry and Fisheries", "wikidata", "stateBody", "NO"),
        ent("GPF", "Government Pension Fund Norway", "wikidata"),
        rel("a", "FIN", 67, "opensanctions"),
        rel("b", "GOV", 67, "climatetrace"),
        rel("c", "MIN", 67, "wikidata"),
        rel("d", "GPF", 3.45, "wikidata"),
    ]


def _net(bods):
    async def no_hop(lei, anchor):
        raise AssertionError

    return asyncio.run(O.grow(list(bods), ["T"], None, O.resolve_rule("NO"), 1, no_hop, AS_OF, None))


def test_one_row_per_state_with_each_body_and_source():
    net = _net(_equinor())
    ans = O.owners_answer(lei="L", legal_name="EQUINOR ASA", jurisdiction="NO", net=net,
                          rule=O.resolve_rule("NO"), depth=1, as_of=AS_OF)
    [norway] = [o for o in ans["calculated_owners"] if o["kind"] == "state"]
    assert norway["name"] == "Norway (state)" and norway["party"] == "state:NO"
    assert sorted(norway["bodies"]) == ["FINANSDEPARTEMENTET", "Government of Norway",
                                        "Ministry of Trade, Industry and Fisheries"]
    assert sorted((f["source"], f["text"], f["as"]) for f in norway["figures"]) == [
        ("Global Energy Monitor / Climate TRACE", "67%", "Government of Norway"),
        ("OpenSanctions", "67%", "FINANSDEPARTEMENTET"),
        ("Wikidata", "67%", "Ministry of Trade, Industry and Fisheries"),
    ]
    assert "201%" not in ans["summary"] and "134%" not in ans["summary"]
    assert "Norway (state) (67% OpenSanctions as FINANSDEPARTEMENTET" in ans["summary"]


def test_a_company_is_never_folded_by_name():
    bods = _equinor() + [ent("GPF2", "Government Pension Fund Norway", "opensanctions"),
                         rel("e", "GPF2", 3.4, "opensanctions")]
    net = _net(bods)
    m, _svg = O.ownership_map(lei="L", legal_name="EQUINOR ASA", net=net, as_of=AS_OF)
    owners = {e["owner"] for e in m["edges"]}
    assert "Government Pension Fund Norway" in owners
    # Two pension-fund records stay two nodes: one name, no identifier.
    assert sum(1 for e in m["edges"] if e["owner"] == "Government Pension Fund Norway") == 2


def test_a_state_body_with_no_country_stays_its_own_party():
    bods = [ent("T", "Target", "opensanctions"), ent("X", "Some Ministry", "wikidata", "stateBody"),
            rel("x", "X", 51, "wikidata")]
    assert O.state_groups(bods) == {}


def test_the_map_draws_one_state_node_and_one_line_with_each_source():
    net = _net(_equinor())
    m, svg = O.ownership_map(lei="L", legal_name="EQUINOR ASA", net=net, as_of=AS_OF)
    state_rows = [e for e in m["edges"] if e["owner"] == "Norway (state)"]
    assert sorted(e["owner_as"] for e in state_rows) == [
        "FINANSDEPARTEMENTET", "Government of Norway", "Ministry of Trade, Industry and Fisheries"]
    assert svg.count(">Norway (state)<") == 1
    lines = re.findall(r'<line x1="[\d.]+" y1="[\d.]+" x2="[\d.]+" y2="[\d.]+" stroke', svg)
    assert len(lines) == 3  # Norway (state), the pension fund, and the legend's sample
    label = " ".join(re.findall(r'<tspan x="\d+" y="\d+">([^<]*)</tspan>', svg))
    assert "(as FINANSDEPARTEMENTET)" in label
    assert "(as Government of Norway)" not in label  # the node says it already


def test_an_ownership_and_a_control_edge_between_one_pair_are_one_line():
    bods = [ent("T", "Target", "opensanctions"),
            ent("G", "Government of Estonia", "opensanctions", "stateBody", "EE"),
            ent("W", "Government of Estonia", "wikidata", "stateBody", "EE"),
            rel("o", "G", 100, "opensanctions"),
            rel("w", "W", None, "wikidata", t="otherInfluenceOrControl")]
    net = _net(bods)
    _m, svg = O.ownership_map(lei="L", legal_name="Target", net=net, as_of=AS_OF)
    drawn = re.findall(r'<line x1="[\d.]+" y1="[\d.]+" x2="[\d.]+" y2="[\d.]+" stroke="(#[0-9a-f]+)"', svg)
    assert len(drawn) == 2  # one edge, plus the legend's ownership sample
    assert "Controls — Wikidata" in svg


# --- the post-deploy re-test, 7 Nov 2026 -------------------------------------

def _eesti():
    """Two sources' records of one body that share an identifier, so they are
    one party: each source's figure keeps the name that source filed under."""
    NAMES["ariregister"] = "Estonian e-Business Register (e-Äriregister)"
    ident = [{"scheme": "EE-RIK", "id": "70000350"}]
    rik = ent("RIK", "Rahandusministeerium", "ariregister", "stateBody", "EE")
    gov = ent("GOV", "Government of Estonia", "opensanctions", "state", "EE")
    for e in (rik, gov):
        e["recordDetails"]["identifiers"] = ident
    return [ent("T", "Eesti Energia AS", "opensanctions"), rik, gov,
            rel("a", "RIK", 100, "ariregister"), rel("b", "GOV", 100, "opensanctions")]


def test_each_figure_names_the_body_its_source_filed():
    net = _net(_eesti())
    ans = O.owners_answer(lei="L", legal_name="Eesti Energia AS", jurisdiction="EE", net=net,
                          rule=O.resolve_rule("EE"), depth=1, as_of=AS_OF)
    [estonia] = [o for o in ans["calculated_owners"] if o["kind"] == "state"]
    assert {f["source_id"]: f["as"] for f in estonia["figures"]} == {
        "ariregister": "Rahandusministeerium", "opensanctions": "Government of Estonia"}
    assert sorted(estonia["bodies"]) == ["Government of Estonia", "Rahandusministeerium"]
    # The map's edge rows, read from the filings, say the same.
    m, _svg = O.ownership_map(lei="L", legal_name="Eesti Energia AS", net=net, as_of=AS_OF)
    assert {r["source"]: r.get("owner_as") for r in m["edges"]}["Estonian e-Business Register (e-Äriregister)"] \
        == "Rahandusministeerium"


def test_a_folded_state_edge_is_typed_state():
    m, _svg = O.ownership_map(lei="L", legal_name="EQUINOR ASA", net=_net(_equinor()), as_of=AS_OF)
    kinds = {r["owner"]: r["owner_kind"] for r in m["edges"]}
    assert kinds["Norway (state)"] == "state"
    assert kinds["Government Pension Fund Norway"] == "entity"
