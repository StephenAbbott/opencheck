"""The drawn map names a source by its short name (the post-deploy re-test of
the MCP testing ticket, 7 Nov 2026): Eli Lilly's figure lines were cut at
"SEC EDGAR — U.S. Securities and Exchange Commissi…" and Taqa Bratani's MEIP
label wrapped to four lines. The map's JSON keeps every source's full name."""

from __future__ import annotations

import asyncio

import pytest

from opencheck import owners as O

AS_OF = "2026-11-07"
SEC = "SEC EDGAR — U.S. Securities and Exchange Commission"
MEIP = "OECD-UNSD Multinational Enterprise Information Platform (MEIP), 'Group Register' sheet."


@pytest.mark.parametrize("label, short", [
    (SEC, "SEC EDGAR"),
    (MEIP, "OECD-UNSD MEIP"),
    ("OECD-UNSD Multinational Enterprise Information Platform (MEIP)", "OECD-UNSD MEIP"),
    ("CVR — Det Centrale Virksomhedsregister (Danish Business Authority)", "CVR"),
    ("Estonian e-Business Register (e-Äriregister)", "Estonian e-Business Register (e-Äriregister)"),
    ("Global Energy Monitor / Climate TRACE", "Global Energy Monitor / Climate TRACE"),
    ("OpenSanctions", "OpenSanctions"),
])
def test_short_source(label, short):
    assert O.short_source(label) == short


def _src(desc, sid):
    return {"description": desc, "opencheckSourceId": sid}


def _map():
    s = _src(SEC, "sec_edgar")
    bods = [
        {"statementId": "T", "recordId": "T", "recordType": "entity", "source": s,
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Target"}},
        {"statementId": "E", "recordId": "E", "recordType": "entity", "source": s,
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Lilly Endowment, Inc."}},
        {"statementId": "r", "recordId": "r", "recordType": "relationship", "source": s,
         "recordDetails": {"isComponent": False, "subject": "T", "interestedParty": "E",
                           "interests": [{"type": "shareholding", "share": {"exact": 9.6}}]}},
    ]

    async def no_hop(lei, anchor):
        raise AssertionError

    net = asyncio.run(O.grow(bods, ["T"], None, O.resolve_rule("US"), 1, no_hop, AS_OF, None))
    return O.ownership_map(lei="LEI", legal_name="Target", net=net, as_of=AS_OF)


def test_the_drawing_uses_the_short_name_and_the_json_the_full_one():
    ans, svg = _map()
    assert ">9.6% (SEC EDGAR)<" in svg
    assert "Commissi…" not in svg
    assert ans["integrated_figures"]["Lilly Endowment, Inc."] == [f"9.6% ({SEC})"]
    assert ans["edges"][0]["source"] == SEC
