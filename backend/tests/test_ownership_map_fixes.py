"""The MCP ownership map, road-tested on UNILEVER PLC (15 Oct 2026).

The map drew three "Controls" edges into a widely held listed company and
none of them was an owner: GLEIF's direct- and ultimate-parent reporting
exceptions (NON_CONSOLIDATING), each as its own "Non-consolidating parent"
node, and the company secretary (OpenCorporates' position "secretary", typed
``otherInfluenceOrControl`` since Phase 197). Notion: "OpenCheck MCP
ownership map fixes".

Stephen's decisions (15 Oct 2026): officers are marked in the BODS, so every
reader treats them as a role; an undisclosed parent (NON_PUBLIC) is still
drawn, every other exception is listed in words.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from opencheck import integrated_ownership as io
from opencheck import owners as O
from opencheck.bods import map_companies_house, map_opencorporates
from opencheck.bods.mapper import _gleif_exception_statements, gleif_reporting_exception
from opencheck.bods.mappers.wikidata import _oc_match_position
from opencheck.bods.officer_roles import OFFICER_POSITION_KEY, officer_position
from opencheck.reporting import diagram

from .test_bods_ch_officer import _officer_bundle
from .test_bods_opencorporates import _minimal_bundle, _officer
from .test_owners_phase340 import AS_OF, X, ent, net_for, rel

LEI = "549300MKFYEKVRWML317"


def _exceptions(reason: str = "NON_CONSOLIDATING") -> list[dict]:
    out = []
    for kind, category in (("direct", "DIRECT"), ("ultimate", "ULTIMATE")):
        out += _gleif_exception_statements(
            LEI, "T", kind,
            {"reason": reason, "category": f"{category}_ACCOUNTING_CONSOLIDATION_PARENT"},
        )
    return out


def _oc_secretary() -> tuple[list[dict], str]:
    """OpenCorporates' record of the company, with its secretary."""
    stmts = list(map_opencorporates(_minimal_bundle(
        ocid="gb/00041424", name="UNILEVER PLC",
        officers=[_officer("PRAKASH KAKKAD", "secretary", officer_id="9")],
    )))
    subject = next(s["statementId"] for s in stmts if s["recordType"] == "entity")
    return stmts, subject


def _unilever() -> tuple[list[dict], list[str]]:
    oc, oc_subject = _oc_secretary()
    return [ent("T", "UNILEVER PLC", "gleif", lei=LEI), *_exceptions(), *oc], ["T", oc_subject]


# --- issue 1: GLEIF reporting exceptions are filings, not parties ------------------


def test_the_unilever_shape_draws_nothing_and_says_why():
    bods, subjects = _unilever()
    net = net_for(bods, subjects)
    a, svg = O.ownership_map(lei=LEI, legal_name="UNILEVER PLC", net=net, as_of=AS_OF)
    assert svg is None
    assert a["edges"] == [] and a["current_edges"] == 0
    assert a["reporting_exceptions"] == [{
        "company": "UNILEVER PLC",
        "parent_levels": ["direct", "ultimate"],
        "reason": "NON_CONSOLIDATING",
        "meaning": "the entity is controlled by legal entities not subject to preparing "
                   "consolidated financial statements",
    }]
    assert a["summary"].startswith("No source names a current owner or controller of UNILEVER PLC.")
    assert "direct and ultimate parents of UNILEVER PLC: NON_CONSOLIDATING" in a["summary"]
    assert a["summary"].endswith("It names no party.")


def test_an_undisclosed_parent_is_still_drawn():
    bods = [ent("T", "Target", "gleif", lei=LEI), *_exceptions("NON_PUBLIC")]
    a, svg = O.ownership_map(lei=LEI, legal_name="Target", net=net_for(bods), as_of=AS_OF)
    assert a["reporting_exceptions"] == []
    assert {r["owner"] for r in a["edges"]} == {"Undisclosed parent (GLEIF reporting exception)"}
    assert svg and "Undisclosed parent" in svg


def test_the_direct_and_ultimate_undisclosed_parent_is_one_node_and_one_edge():
    # 288 WHITESANDS UK LIMITED (2138008HG9U53HXBAJ27), production 16 Oct 2026:
    # GLEIF files NON_PUBLIC at both levels and the map drew two
    # "Undisclosed parent" nodes beside its one real owner.
    bods = [ent("T", "288 WHITESANDS UK LIMITED", "gleif", lei=LEI), *_exceptions("NON_PUBLIC"),
            ent("P", "Stephen Allen", "companies_house"),
            rel("r", "T", "P", X(40), "companies_house")]
    a, svg = O.ownership_map(
        lei=LEI, legal_name="288 WHITESANDS UK LIMITED", net=net_for(bods), as_of=AS_OF
    )
    owners = [r["owner"] for r in a["edges"]]
    assert sorted(owners) == ["Stephen Allen", "Undisclosed parent (GLEIF reporting exception)"]
    assert a["current_edges"] == 2
    assert svg and svg.count("<title>Undisclosed parent") == 1
    assert a["undisclosed_parents"] == [{
        "company": "288 WHITESANDS UK LIMITED",
        "parent_levels": ["direct", "ultimate"],
        "reason": "NON_PUBLIC",
        "meaning": a["undisclosed_parents"][0]["meaning"],
    }]
    assert a["summary"] == (
        "2 current ownership or control edges above 288 WHITESANDS UK LIMITED. "
        "GLEIF records the direct and ultimate parent of 288 WHITESANDS UK LIMITED "
        "as withheld (NON_PUBLIC): a parent exists but is not disclosed. "
        "It is drawn once, as an undisclosed parent."
    )


def test_a_withheld_parent_at_one_level_says_that_level_only():
    bods = [ent("T", "Target", "gleif", lei=LEI),
            *_gleif_exception_statements(LEI, "T", "direct", {"reason": "NON_PUBLIC"})]
    a, _ = O.ownership_map(lei=LEI, legal_name="Target", net=net_for(bods), as_of=AS_OF)
    assert a["undisclosed_parents"][0]["parent_levels"] == ["direct"]
    assert "the direct parent of Target as withheld (NON_PUBLIC)" in a["summary"]


def test_a_real_owner_is_drawn_beside_the_exception_sentence():
    bods = [ent("T", "Target", "gleif", lei=LEI), *_exceptions(),
            ent("H", "Holdco", "companies_house"), rel("r", "T", "H", X(60), "companies_house")]
    a, svg = O.ownership_map(lei=LEI, legal_name="Target", net=net_for(bods), as_of=AS_OF)
    assert [r["owner"] for r in a["edges"]] == ["Holdco"]
    assert a["summary"].startswith("1 current ownership or control edge above Target.")
    named = O.ownership_map(lei=LEI, legal_name="BP P.L.C.", net=net_for(bods), as_of=AS_OF)[0]
    assert "above BP P.L.C. GLEIF" in named["summary"]  # one full stop, not two
    assert svg and "Holdco" in svg and "Non-consolidating" not in svg


def test_only_opencheck_s_own_gleif_bridges_are_read_as_exceptions():
    bridge_rel = next(s for s in _exceptions() if s["recordType"] == "relationship")
    assert gleif_reporting_exception(bridge_rel)["reason"] == "NON_CONSOLIDATING"
    other = json.loads(json.dumps(bridge_rel))
    other["source"] = {"description": "UK Companies House", "opencheckSourceId": "companies_house"}
    assert gleif_reporting_exception(other) is None
    assert gleif_reporting_exception(rel("r", "T", "H", X(60), "gleif")) is None


# --- issue 2: an officer appointment is a role, wherever it is read ---------------


def test_the_secretary_is_not_drawn_as_a_controller():
    oc, subject = _oc_secretary()
    bods = [ent("H", "Holdco", "companies_house"),
            rel("r", subject, "H", X(60), "companies_house"), *oc]
    net = net_for(bods, [subject])
    a, svg = O.ownership_map(lei=None, legal_name="UNILEVER PLC", net=net, as_of=AS_OF)
    assert [r["owner"] for r in a["edges"]] == ["Holdco"]
    assert svg and "KAKKAD" not in svg


def test_opencorporates_marks_an_officer_whose_position_has_no_role_code():
    oc, _ = _oc_secretary()
    rel_stmt = next(s for s in oc if s["recordType"] == "relationship")
    interest = rel_stmt["recordDetails"]["interests"][0]
    assert interest["type"] == "otherInfluenceOrControl"  # Phase 197, unchanged
    assert interest[OFFICER_POSITION_KEY] == "secretary"
    director = list(map_opencorporates(_minimal_bundle(officers=[_officer("Jo", "director")])))
    d_rel = next(s for s in director if s["recordType"] == "relationship")
    d = d_rel["recordDetails"]["interests"][0]
    assert d["type"] == "seniorManagingOfficial" and OFFICER_POSITION_KEY not in d


@pytest.mark.parametrize("position,expected", [
    ("Secretario del Consejo", "otherInfluenceOrControl"),
    ("Secretaria Executiva", "otherInfluenceOrControl"),
    ("Directör", "seniorManagingOfficial"),
    ("Directeur administratif", "seniorManagingOfficial"),
])
def test_the_regex_fallbacks_follow_the_exact_matches(position, expected):
    assert _oc_match_position(position) == expected


def test_companies_house_marks_a_secretary_appointment():
    stmts = list(map_companies_house(_officer_bundle()))
    interests = [
        s["recordDetails"]["interests"][0] for s in stmts if s["recordType"] == "relationship"
    ]
    secretary = next(i for i in interests if i["type"] == "otherInfluenceOrControl")
    assert officer_position(secretary)
    director = next(i for i in interests if i["type"] == "boardMember")
    assert officer_position(director) is None


def test_the_diagram_reads_the_marker():
    sec = {"type": "otherInfluenceOrControl", OFFICER_POSITION_KEY: "secretary"}
    assert diagram.categorise([sec]) == "role"
    assert diagram.interest_label(sec) == "Secretary"
    # A secretary who also holds shares is drawn by the shares.
    assert diagram.categorise([sec, {"type": "shareholding"}]) == "ownership"
    assert diagram.categorise([{"type": "otherInfluenceOrControl"}]) == "control"


# --- issue 3: long names wrap, never run off the picture ----------------------------


def test_a_long_node_name_wraps_onto_two_lines_with_its_full_name_kept():
    name = "THE MUNICIPAL WASTE PAPER RECEPTACLE COMPANY OF THE CITY OF NEW YORK"
    lines = diagram.node_label_lines(name)
    assert len(lines) == 2 and all(len(ln) <= 26 for ln in lines)
    assert lines[-1].endswith("…")
    svg = diagram._node_svg(130, 118, "entity", name)
    assert f"<title>{name}</title>" in svg and svg.count("<tspan") == 2
    short = diagram._node_svg(130, 118, "entity", "UNILEVER PLC")
    assert "<title>" not in short and short.count("<tspan") == 1


# --- through the protocol ------------------------------------------------------------


@pytest.fixture
def unilever_lookup(monkeypatch):
    pytest.importorskip("mcp")
    from opencheck.routers import lookup as L

    bods, _ = _unilever()

    async def fake(lei, deepen_top=5, **_):
        payload = io.for_lookup(lei, bods, AS_OF)
        return SimpleNamespace(lei=lei, legal_name="UNILEVER PLC", jurisdiction="GB", bods=bods,
                               integrated_ownership=payload, license_notices=[])

    monkeypatch.setattr(L, "_lookup_impl", fake)


def test_the_map_tool_answers_unilever_in_words(unilever_lookup):
    from opencheck.mcp.server import mcp

    out = asyncio.run(mcp.call_tool("opencheck_ownership_map", {"lei": LEI}))
    blocks = out if isinstance(out, list) else out[0]
    assert [b.type for b in blocks] == ["text"]
    data = json.loads(blocks[0].text)
    assert data["edges"] == []
    assert data["reporting_exceptions"][0]["parent_levels"] == ["direct", "ultimate"]
    assert "No source names a current owner" in data["summary"]
