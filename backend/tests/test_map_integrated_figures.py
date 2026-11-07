"""The map's ``integrated_figures`` keeps every node's figures when two nodes
share a name (MCP testing, 7 Nov 2026, issue 3: Rosneft's "Rosneftegaz JSC"
from OpenSanctions lost its "at least 44.69%" to GEM's "40.4%")."""

from __future__ import annotations

import asyncio

from opencheck import owners as O

AS_OF = "2026-11-07"
NAMES = {"opensanctions": "OpenSanctions", "climatetrace": "Global Energy Monitor / Climate TRACE"}


def ent(i, n, s):
    return {"statementId": i, "recordId": i, "recordType": "entity",
            "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": n},
            "source": {"description": NAMES[s], "opencheckSourceId": s}}


def rel(i, party, share, s):
    return {"statementId": i, "recordId": i, "recordType": "relationship",
            "recordDetails": {"isComponent": False, "subject": "T", "interestedParty": party,
                              "interests": [{"type": "shareholding", "share": {"exact": share}}]},
            "source": {"description": NAMES[s], "opencheckSourceId": s}}


def test_two_nodes_with_one_name_keep_both_figures():
    bods = [ent("T", "Rosneft", "opensanctions"),
            ent("A", "Rosneftegaz JSC", "opensanctions"), ent("B", "Rosneftegaz JSC", "climatetrace"),
            rel("a", "A", 44.69, "opensanctions"), rel("b", "B", 40.4, "climatetrace")]

    async def no_hop(lei, anchor):
        raise AssertionError

    net = asyncio.run(O.grow(bods, ["T"], None, O.resolve_rule("RU"), 1, no_hop, AS_OF, None))
    m, svg = O.ownership_map(lei="L", legal_name="Rosneft", net=net, as_of=AS_OF)
    assert sorted(m["integrated_figures"]["Rosneftegaz JSC"]) == [
        "40.4% (Global Energy Monitor / Climate TRACE)", "44.69% (OpenSanctions)"]
    # The image already drew both; the JSON now says what the image says.
    assert "44.69% (OpenSanctions)" in svg and "40.4% (Global Energy Monitor / Climate TRACE)" in svg
