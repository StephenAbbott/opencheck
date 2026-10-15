"""Phase 340 — the question-shaped MCP tools: ``opencheck_owners`` and
``opencheck_ownership_map``.

The tests on the ticket, in order: the rule (decision 2), the ASDA bands
("may be above", never "above"), Equinor's three sources never summed,
Estonia's exactly-25% person, declared against calculated, depth and its
limit, the map, and the isolation guard.
"""

from __future__ import annotations

import ast
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from opencheck import owners as O
from opencheck import integrated_ownership as io

_BACKEND = Path(__file__).resolve().parents[1]
_CASES = _BACKEND.parent / "frontend" / "src" / "lib" / "integratedOwnershipCases.json"

AS_OF = "2026-10-15"
NAMES = {"opensanctions": "OpenSanctions", "wikidata": "Wikidata", "climatetrace": "Global Energy Monitor / Climate TRACE",
         "companies_house": "UK Companies House", "ariregister": "Estonian e-Business Register (e-Äriregister)", "gleif": "GLEIF"}


def src(s):
    return {"description": NAMES[s], "opencheckSourceId": s}


def ent(i, n, s="opensanctions", lei=None, etype="registeredEntity"):
    rd = {"entityType": {"type": etype}, "name": n}
    if lei:
        rd["identifiers"] = [{"scheme": "XI-LEI", "id": lei}]
    return {"statementId": i, "recordId": i, "recordType": "entity", "recordDetails": rd, "source": src(s)}


def per(i, n, s="opensanctions", ident=None):
    rd = {"personType": "knownPerson", "names": [{"type": "legal", "fullName": n}]}
    if ident:
        rd["identifiers"] = [{"scheme": ident[0], "id": ident[1]}]
    return {"statementId": i, "recordId": i, "recordType": "person", "recordDetails": rd, "source": src(s)}


def rel(i, subj, party, share=None, s="opensanctions", t="shareholding", bo=None, end=None, extra=()):
    it = {"type": t}
    if share is not None:
        it["share"] = share
    if bo is not None:
        it["beneficialOwnershipOrControl"] = bo
    if end:
        it["endDate"] = end
    return {"statementId": i, "recordId": i, "recordType": "relationship",
            "recordDetails": {"isComponent": False, "subject": subj, "interestedParty": party,
                              "interests": [it, *extra]}, "source": src(s)}


X = lambda p: {"exact": p}  # noqa: E731
BAND = lambda lo, hi: {"exclusiveMinimum": lo, "maximum": hi}  # noqa: E731


async def _no_hop(lei, anchor):
    raise AssertionError("depth 1 must not hop")


def net_for(bods, subject_ids=("T",), depth=1, hop=_no_hop, rule=None, subject_lei=None):
    rule = rule or O.resolve_rule("GB")
    return asyncio.run(O.grow(list(bods), list(subject_ids), None, rule, depth, hop, AS_OF, subject_lei))


def answer(bods, jurisdiction="GB", subject_ids=("T",), **kw):
    rule = O.resolve_rule(jurisdiction, kw.pop("threshold", None), kw.pop("rule", None))
    depth = kw.pop("depth", 1)
    net = net_for(bods, subject_ids, depth=depth, rule=rule, **kw)
    return O.owners_answer(lei="LEI", legal_name="Target", jurisdiction=jurisdiction,
                           net=net, rule=rule, depth=depth, as_of=AS_OF)


# --- the rule (decision 2) ----------------------------------------------------


@pytest.mark.parametrize("jur,op,value,words", [
    ("EE", ">", 25.0, "more than 25% — Estonia"),
    ("UA", ">=", 25.0, "25% or more — Ukraine"),
    ("SK", ">=", 25.0, "25% or more — Slovakia"),
    ("US-DC", ">", 10.0, "more than 10%"),
    ("GB-ENG", ">", 25.0, "more than 25% — United Kingdom"),
    ("NO", ">=", 25.0, "no national rule recorded"),
    (None, ">=", 25.0, "no national rule recorded"),
])
def test_the_default_rule_is_the_national_one_else_25_or_more(jur, op, value, words):
    r = O.resolve_rule(jur)
    assert (r.operator, r.threshold) == (op, value)
    assert words in r.text


def test_the_caller_can_set_the_threshold_and_the_reading():
    assert O.resolve_rule("EE", rule="amlr").operator == ">="
    r = O.resolve_rule("EE", threshold=10)
    assert (r.operator, r.threshold) == (">", 10.0) and "set by the caller" in r.text
    with pytest.raises(ValueError):
        O.resolve_rule("EE", rule="strict")
    with pytest.raises(ValueError):
        O.resolve_rule("EE", threshold=120)
    with pytest.raises(ValueError):
        O.resolve_rule("NO", rule="national")  # nothing recorded to apply


# --- bands, Equinor, Estonia --------------------------------------------------


def _cases():
    return {c["name"]: c for c in json.loads(_CASES.read_text(encoding="utf-8"))["cases"]}


def test_the_asda_bands_are_above_and_a_straddling_band_is_never_above():
    c = _cases()["the ASDA shape: a chain of PSC bands gives each layer its interval"]
    a = answer(c["statements"], "GB")
    pos = {o["name"]: o["position"] for o in a["calculated_owners"]}
    assert pos == {"Holdco 1": "above", "Holdco 2": "above"}
    # A 20–30% band straddles "more than 25%": may be above, never above.
    bods = [ent("T", "Target"), per("P", "Pat"), rel("r", "T", "P", {"minimum": 20, "maximum": 30})]
    (o,) = answer(bods, "GB")["calculated_owners"]
    assert o["position"] == "may_be_above"
    assert "may_be_above" in answer(bods, "GB")["hint"]


def test_a_band_wholly_below_the_threshold_is_left_out():
    bods = [ent("T", "Target"), per("P", "Pat"), rel("r", "T", "P", {"minimum": 10, "maximum": 20})]
    assert answer(bods, "GB")["calculated_owners"] == []


def test_an_unstated_share_may_be_above():
    bods = [ent("T", "Target"), per("P", "Pat"), rel("r", "T", "P")]
    (o,) = answer(bods, "GB")["calculated_owners"]
    assert o["position"] == "may_be_above"


def test_equinor_three_sources_three_figures_never_summed():
    c = _cases()["the Equinor shape: three sources, one stake, three figures never summed"]
    a = answer(c["statements"], "NO", subject_ids=c["subject_ids"])
    # The answer is computed with the lookup's canon (entities that share an
    # identifier); these three state bodies share none, so three rows of 67%.
    texts = sorted(f["text"] for o in a["calculated_owners"] for f in o["figures"])
    assert texts == ["67%", "67%", "67%"]
    assert "201%" not in json.dumps(a) and "134%" not in json.dumps(a)
    # The fixture's holders are plain entities with no LEI: each chain stops, and says why.
    assert {s["reason"] for s in a["chain_stops"]} == {
        "no LEI to follow; its owners are not in the sources OpenCheck reached"
    }


def test_estonia_exactly_25_is_out_under_more_than_25_and_in_under_the_amlr():
    bods = [ent("T", "Target OÜ", "ariregister"), per("A", "Anna", "ariregister"),
            per("B", "Bert", "ariregister"),
            rel("r1", "T", "A", X(25), "ariregister"), rel("r2", "T", "B", X(75), "ariregister")]
    national = answer(bods, "EE")
    assert [o["name"] for o in national["calculated_owners"]] == ["Bert"]
    amlr = answer(bods, "EE", rule="amlr")
    assert [o["name"] for o in amlr["calculated_owners"]] == ["Bert", "Anna"]
    assert "Estonia" in national["rule"]["text"] and "AML Regulation" in amlr["rule"]["text"]


# --- declared against calculated -------------------------------------------------


def test_a_psc_band_flagged_as_bo_is_in_both_lists():
    bods = [ent("T", "Target Ltd", "companies_house"), per("P", "Pat Doe", "companies_house"),
            rel("r", "T", "P", BAND(75, 100), "companies_house", bo=True)]
    a = answer(bods, "GB")
    assert [d["name"] for d in a["declared_beneficial_owners"]] == ["Pat Doe"]
    assert a["declared_beneficial_owners"][0]["interests"]
    assert a["in_both"] == [{
        "name": "Pat Doe", "kind": "person", "position": "above",
        "calculated": ["75–100% (UK Companies House)"], "declared_by": ["UK Companies House"],
    }]


def test_people_match_across_registers_on_an_identifier_never_a_name():
    bods = [ent("T", "Target"), per("P1", "Pat Doe", "opensanctions", ident=("EE-ARIREGISTER-HASH", "abc")),
            per("P2", "Pat Doe", "ariregister", ident=("EE-ARIREGISTER-HASH", "abc")),
            per("P3", "Pat Doe", "ariregister"),
            rel("r1", "T", "P1", X(60)),
            rel("r2", "T", "P2", None, "ariregister", t="otherInfluenceOrControl", bo=True),
            rel("r3", "T", "P3", None, "ariregister", t="otherInfluenceOrControl", bo=True)]
    a = answer(bods, "EE")
    assert len(a["declared_beneficial_owners"]) == 2
    (both,) = a["in_both"]
    assert both["declared_by"] == ["Estonian e-Business Register (e-Äriregister)"]
    # P3 shares only the name: it is declared, but not matched.


def test_an_ended_declaration_is_listed_as_not_current():
    bods = [ent("T", "Target", "companies_house"), per("P", "Pat", "companies_house"),
            rel("r", "T", "P", BAND(25, 50), "companies_house", bo=True, end="2020-01-01")]
    (d,) = answer(bods, "GB")["declared_beneficial_owners"]
    assert d["current"] is False and d["ended_on"] == "2020-01-01"


def test_voting_only_and_former_holders_are_listed_without_a_figure():
    c = _cases()["a voting path above a shareholder"]
    a = answer(c["statements"], "GB")
    assert [u["status"] for u in a["listed_without_a_figure"]] == ["voting_rights_only"]


# --- depth ------------------------------------------------------------------------


def _chain():
    return [ent("T", "Target", lei="TTTTTTTTTTTTTTTTTT00"), ent("A", "Holdco A", lei="AAAAAAAAAAAAAAAAAA00"),
            ent("B", "Holdco B", lei="BBBBBBBBBBBBBBBBBB00"),
            rel("r1", "T", "A", X(60)), rel("r2", "T", "B", X(40))]


def test_depth_1_says_where_the_chain_stops():
    a = answer(_chain(), "GB")
    reasons = {s["name"]: s["reason"] for s in a["chain_stops"]}
    assert set(reasons) == {"Holdco A", "Holdco B"}
    assert all("raise depth" in r for r in reasons.values())
    assert a["depth"]["extra_lookups"] == 0


def test_depth_2_looks_up_each_corporate_owner_and_multiplies_through():
    calls = []

    async def hop(lei, anchor):
        calls.append((lei, anchor))
        if lei == "AAAAAAAAAAAAAAAAAA00":
            return [per("P", "Pat"), rel("rA", anchor, "P", X(100))]
        return []

    a = answer(_chain(), "GB", depth=2, hop=hop)
    assert sorted(calls) == [("AAAAAAAAAAAAAAAAAA00", "A"), ("BBBBBBBBBBBBBBBBBB00", "B")]
    pat = next(o for o in a["calculated_owners"] if o["name"] == "Pat")
    assert pat["figures"][0]["text"] == "60%" and pat["figures"][0]["route"] == "through Holdco A"
    stops = {s["name"]: s["reason"] for s in a["chain_stops"]}
    assert stops == {"Holdco B": "its own lookup found no owners in any source"}
    assert a["depth"] == {"requested": 2, "layers_reached": 2, "extra_lookups": 2,
                          "hop_limit_reached": False, "max_extra_lookups": O.MAX_HOPS}


def test_the_hop_limit_holds_and_is_said(monkeypatch):
    monkeypatch.setattr(O, "MAX_HOPS", 1)

    async def hop(lei, anchor):
        return []

    a = answer(_chain(), "GB", depth=3, hop=hop)
    assert a["depth"]["extra_lookups"] == 1 and a["depth"]["hop_limit_reached"] is True
    assert any("lookup limit" in s["reason"] for s in a["chain_stops"])


def test_a_failed_owner_lookup_is_said_not_raised():
    async def hop(lei, anchor):
        raise RuntimeError("boom")

    a = answer(_chain(), "GB", depth=2, hop=hop)
    assert all("did not complete" in s["reason"] for s in a["chain_stops"])


def test_a_state_body_is_the_top_of_its_chain():
    bods = [ent("T", "Target"), ent("S", "Ministry", etype="stateBody", lei="SSSSSSSSSSSSSSSSSS00"),
            rel("r", "T", "S", X(67))]
    assert answer(bods, "NO")["chain_stops"] == []


# --- the map ------------------------------------------------------------------------


def _map_bods():
    return [ent("T", "Target", "companies_house"), ent("A", "Holdco A", "companies_house"),
            per("P", "Pat", "companies_house"), per("D", "Director", "companies_house"),
            per("E", "Former", "companies_house"),
            rel("r1", "T", "A", BAND(75, 100), "companies_house"),
            rel("r2", "A", "P", BAND(50, 75), "companies_house"),
            rel("r3", "T", "D", t="boardMember", s="companies_house"),
            rel("r4", "T", "E", BAND(25, 50), "companies_house", end="2019-01-01"),
            rel("r5", "T", "A", X(80), "opensanctions")]


def test_the_map_draws_current_ownership_edges_with_their_source():
    net = net_for(_map_bods())
    a, svg = O.ownership_map(lei="LEI", legal_name="Target", net=net, as_of=AS_OF)
    owners = [(r["owner"], r["owned"], r["source"], r["status"]) for r in a["edges"]]
    assert ("Holdco A", "Target", "UK Companies House", "current") in owners
    assert ("Holdco A", "Target", "OpenSanctions", "current") in owners
    assert ("Pat", "Holdco A", "UK Companies House", "current") in owners
    assert ("Former", "Target", "UK Companies House", "ended") in owners
    assert not any(r["owner"] == "Director" for r in a["edges"])  # roles left out
    assert a["edges_drawn"] == a["current_edges"] == 3
    assert svg and svg.startswith("<svg") and "Former" not in svg
    for name in ("Holdco A", "Pat", "OpenSanctions", "UK Companies House"):
        assert name in svg
    assert a["fullcheck_url"] == "https://opencheck.world/?lei=LEI&mode=full"


def test_the_map_filters_to_one_source():
    net = net_for(_map_bods())
    a, svg = O.ownership_map(lei="LEI", legal_name="Target", net=net, source="opensanctions", as_of=AS_OF)
    assert {r["source"] for r in a["edges"]} == {"OpenSanctions"}


def test_a_company_with_no_owners_has_no_image():
    net = net_for([ent("T", "Target")])
    a, svg = O.ownership_map(lei="LEI", legal_name="Target", net=net, as_of=AS_OF)
    assert svg is None and a["edges"] == [] and "no ownership" in a["image"]


# --- the MCP tools --------------------------------------------------------------


@pytest.fixture
def fake_lookup(monkeypatch):
    pytest.importorskip("mcp")
    from opencheck.routers import lookup as L

    bods = [ent("T", "Target Ltd", "companies_house", lei="TTTTTTTTTTTTTTTTTT00"),
            per("P", "Pat Doe", "companies_house"),
            rel("r", "T", "P", BAND(75, 100), "companies_house", bo=True)]

    async def fake(lei, deepen_top=5, **_):
        payload = io.for_lookup(lei, bods, AS_OF)
        return SimpleNamespace(lei=lei, legal_name="Target Ltd", jurisdiction="GB", bods=bods,
                               integrated_ownership=payload, license_notices=[])

    monkeypatch.setattr(L, "_lookup_impl", fake)
    return bods


def test_opencheck_owners_tool(fake_lookup):
    from opencheck.mcp import server

    out = asyncio.run(server.opencheck_owners(lei="TTTTTTTTTTTTTTTTTT00"))
    assert out["rule"]["operator"] == ">" and "United Kingdom" in out["rule"]["text"]
    assert [o["name"] for o in out["calculated_owners"]] == ["Pat Doe"]
    assert [b["name"] for b in out["in_both"]] == ["Pat Doe"]
    assert "not a determination" in out["note"]
    assert asyncio.run(server.opencheck_owners(lei="TTTTTTTTTTTTTTTTTT00", depth=4))["status"] == 400
    assert asyncio.run(server.opencheck_owners(lei="TTTTTTTTTTTTTTTTTT00", rule="x"))["status"] == 400
    assert asyncio.run(server.opencheck_owners(lei="nope"))["status"] == 400


def test_opencheck_ownership_map_tool_returns_json_and_an_svg_image(fake_lookup):
    from mcp.server.fastmcp import Image

    from opencheck.mcp import server

    out = asyncio.run(server.opencheck_ownership_map(lei="TTTTTTTTTTTTTTTTTT00"))
    text, image = out
    data = json.loads(text.text)
    assert data["edges"][0]["owner"] == "Pat Doe"
    assert isinstance(image, Image)
    assert image._mime_type == "image/svg+xml" and image.data.startswith(b"<svg")


def test_both_tools_are_listed_and_rate_limited():
    pytest.importorskip("mcp")
    from opencheck.mcp import TOOL_NAMES
    from opencheck.mcp.guard import _LOOKUP, TOOL_TIERS

    assert {"opencheck_owners", "opencheck_ownership_map"} <= set(TOOL_NAMES)
    assert TOOL_TIERS["opencheck_owners"] is _LOOKUP
    assert TOOL_TIERS["opencheck_ownership_map"] is _LOOKUP


# --- never a signal ---------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(
    [_BACKEND / "opencheck" / m for m in ("risk.py", "verdict.py", "signalstats.py")]
    + list((_BACKEND / "opencheck" / "narrative").glob("*.py"))
), ids=lambda p: p.name)
def test_no_risk_module_reads_the_owners_answer(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names = [node.module or ""] + [a.name for a in node.names]
        elif isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        else:
            continue
        assert not any(n.split(".")[-1] == "owners" for n in names), path.name
