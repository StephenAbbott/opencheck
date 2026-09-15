"""Phase 210 — a relationship's parties are recordIds, and the consumers know.

BODS v0.4 references a party by its ``recordId``; every OpenCheck mapper sets
``statementId == recordId`` so the distinction never showed. The OECD's MEIP
statements (Phase 208) are the first with a hash ``statementId`` and a
``meip-entity-N`` ``recordId``, and the first live lookup drew A/S Norske
Shell and SHELL PLC as two unlinked nodes. These tests hold a bundle in that
shape against every backend consumer that resolves an edge.
"""

from __future__ import annotations

import json

from opencheck.bods.ftm import map_to_ftm
from opencheck.bods.neo4j import to_cypher
from opencheck.bods.refs import party_ref, resolver, statement_index
from opencheck.bods.senzing import map_to_senzing
from opencheck.reporting.diagram import source_diagram
from opencheck.risk import _layers_signal, _upstream_entity_ids

HEAD_SID = "3f6e3c2a-7b1e-5c9a-9d0b-2e4f8a1c6d70"
MID_SID = "5b5b5b5b-1111-5222-8333-444455556666"
SUB_SID = "a1d2b8ee-4c55-5a1a-8e37-0c9b2f0d1a11"


def _entity(sid: str, rid: str, name: str, lei: str) -> dict:
    return {
        "statementId": sid,
        "recordId": rid,
        "recordType": "entity",
        # The OECD stamps the group head on every statement, members included.
        "declarationSubject": "meip-entity-100913",
        "recordDetails": {
            "name": name,
            "entityType": {"type": "registeredEntity"},
            "jurisdiction": {"code": "GB"},
            "identifiers": [{"id": lei, "scheme": "XI-LEI"}],
        },
    }


def _rel(sid: str, rid: str, subject_rid: str, ip_rid: str) -> dict:
    return {
        "statementId": sid,
        "recordId": rid,
        "recordType": "relationship",
        "declarationSubject": subject_rid,
        "recordDetails": {
            "subject": subject_rid,
            "interestedParty": ip_rid,
            "interests": [{"type": "unknownInterest", "directOrIndirect": "unknown"}],
        },
        "source": {"description": "OECD-UNSD MEIP"},
    }


# Three-hop chain in the publisher's shape: SUB -> MID -> HEAD.
BUNDLE = [
    _entity(SUB_SID, "meip-entity-100959", "A/S Norske Shell", "213800F4ETX85XLF5K47"),
    _rel("r-1", "meip-rel-1", "meip-entity-100959", "meip-entity-5"),
    _entity(MID_SID, "meip-entity-5", "Shell Petroleum N.V.", "724500KDSUZ5NW8YBM07"),
    _rel("r-2", "meip-rel-2", "meip-entity-5", "meip-entity-100913"),
    _entity(HEAD_SID, "meip-entity-100913", "SHELL PLC", "21380068P1DRHMJ8KU70"),
]


def test_resolver_maps_every_spelling_to_the_statement_id() -> None:
    resolve = resolver(BUNDLE)
    assert resolve("meip-entity-100913") == HEAD_SID
    assert resolve(HEAD_SID) == HEAD_SID
    assert resolve({"describedByEntityStatement": "meip-entity-5"}) == MID_SID
    assert resolve({"reason": "noBeneficialOwners"}) == ""
    assert resolve("meip-entity-404") == "meip-entity-404", "unknown stays dangling"
    assert party_ref("") is None


def test_declaration_subject_ranks_below_record_id() -> None:
    """The OECD's alias names the head on every statement; as a peer of
    recordId it pointed the head's id at the first subsidiary in the file."""
    assert statement_index(BUNDLE)["meip-entity-100913"]["statementId"] == HEAD_SID
    assert resolver(BUNDLE)("meip-entity-100913") == HEAD_SID


def test_index_never_lets_a_record_id_shadow_a_statement_id() -> None:
    idx = statement_index([
        {"statementId": "X", "recordId": "Y", "recordType": "entity", "recordDetails": {}},
        {"statementId": "Y", "recordId": "Y", "recordType": "entity", "recordDetails": {}},
    ])
    assert idx["Y"]["statementId"] == "Y"


def test_risk_walks_the_chain_through_record_id_references() -> None:
    assert _upstream_entity_ids(SUB_SID, BUNDLE) == {MID_SID, HEAD_SID}
    # The layer count walks the same edges: three corporate layers (the
    # subject's own counted, as this rule has always done), a path that
    # starts at the subject and reaches the head. Before Phase 210 every one
    # of these edges resolved to nothing and the signal was silently absent.
    sig = _layers_signal("some_register", "213800F4ETX85XLF5K47", BUNDLE)
    assert sig is not None and sig.evidence["layers"] == 3
    assert sig.evidence["longest_path"][0] == SUB_SID
    assert sig.evidence["longest_path"][-1] == HEAD_SID


def test_exports_link_the_parties_they_name() -> None:
    cypher = to_cypher(BUNDLE)
    assert f"MATCH (a {{id: '{HEAD_SID}'}}), (b {{id: '{MID_SID}'}})" in cypher
    assert "meip-entity" not in cypher.split("MATCH")[1], "edges use the node ids"

    ftm = map_to_ftm(BUNDLE)
    links = [e for e in ftm if e["schema"] in ("Ownership", "UnknownLink", "Directorship")]
    assert len(links) == 2
    owners = {(e["properties"].get("owner") or e["properties"].get("subject"))[0] for e in links}
    assert owners == {MID_SID, HEAD_SID}

    senzing = map_to_senzing(BUNDLE)
    head = next(r for r in senzing if r["RECORD_ID"] == HEAD_SID)
    pointers = [f for f in head["FEATURES"] if "REL_POINTER_KEY" in f]
    assert pointers and pointers[0]["REL_POINTER_KEY"] == MID_SID


def test_pdf_diagram_names_both_ends() -> None:
    by_id = {**{s["statementId"]: s for s in BUNDLE}, **statement_index(BUNDLE)}
    rels = [s for s in BUNDLE if s["recordType"] == "relationship"]
    diagram = source_diagram(rels, by_id, source_name="OECD-UNSD MEIP")
    assert diagram.has_relationships
    # Rows are (party, interest, subject) — labels, never "a party".
    assert [(r[0], r[2]) for r in diagram.rows] == [
        ("Shell Petroleum N.V.", "A/S Norske Shell"),
        ("SHELL PLC", "Shell Petroleum N.V."),
    ]
    assert "A/S Norske Shell" in diagram.svg and "SHELL PLC" in diagram.svg
    assert "a party" not in json.dumps(diagram.rows)
