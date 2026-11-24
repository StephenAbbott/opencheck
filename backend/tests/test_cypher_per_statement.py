"""The Cypher export: one edge per relationship statement, with its provenance.

From the Notion ticket "Check out relate from Neo4J" (step 3). The old export
MERGEd one ``OWNS_OR_CONTROLS`` edge per pair of nodes, so two sources' edges
between the same pair became one edge carrying the last source's interest, and
nothing on the edge said which source, whether it had ended or whether it was
ownership at all. These tests pin the per-statement shape.
"""

from __future__ import annotations

import re

from opencheck.bods.neo4j import (
    EDGE_PROPERTIES,
    NOT_OWNERSHIP_BASIS,
    edge_properties,
    to_cypher,
)
from opencheck.bods.statements import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)
from opencheck.integrated_ownership import EXCLUDED_SOURCES


def _edge_lines(cypher: str) -> list[str]:
    return [
        line for line in cypher.splitlines()
        if "OWNS_OR_CONTROLS" in line and not line.startswith("//")
    ]


def _pair(source_id: str, local: str, interests: list[dict]) -> list[dict]:
    company = make_entity_statement(source_id=source_id, local_id="co", name="Acme Ltd")
    owner = make_person_statement(source_id=source_id, local_id="jane", full_name="Jane Roe")
    rel = make_relationship_statement(
        source_id=source_id, local_id=local,
        subject_statement_id=company["recordId"],
        interested_party_statement_id=owner["recordId"],
        interests=interests,
    )
    return [company, owner, rel]


def test_the_not_ownership_sources_are_integrated_ownerships_exclusions() -> None:
    assert set(NOT_OWNERSHIP_BASIS) == set(EXCLUDED_SOURCES)


def test_every_property_written_is_described() -> None:
    bundle = _pair("companies_house", "r1", [
        {"type": "shareholding", "share": {"exclusiveMinimum": 25, "maximum": 50},
         "beneficialOwnershipOrControl": True, "startDate": "2020-01-01"},
    ])
    props = edge_properties(bundle[2], "2026-11-24")
    assert set(props) <= set(EDGE_PROPERTIES)


def test_two_sources_between_one_pair_are_two_edges() -> None:
    a = _pair("companies_house", "r1", [{"type": "shareholding", "share": {"exact": 60}}])
    b = _pair("opencorporates", "r1", [{"type": "votingRights", "share": {"exact": 40}}])
    # Same two parties: reuse the first bundle's nodes for the second edge.
    b_rel = make_relationship_statement(
        source_id="opencorporates", local_id="r1",
        subject_statement_id=a[0]["recordId"],
        interested_party_statement_id=a[1]["recordId"],
        interests=[{"type": "votingRights", "share": {"exact": 40}}],
    )
    cy = to_cypher([*a, b_rel])
    edges = _edge_lines(cy)
    assert len(edges) == 2
    assert {re.search(r"r\.sourceId = '([^']+)'", e).group(1) for e in edges} == {
        "companies_house", "opencorporates",
    }
    # Each edge is MERGEd on its own statement id, never on the pair.
    for e, rel in zip(edges, (a[2], b_rel), strict=True):
        assert f"{{statementId: '{rel['statementId']}'}}" in e
    assert "r.shareExact = 60.0" in cy and "r.shareExact = 40.0" in cy
    del b


def test_an_ended_relationship_says_so_and_when() -> None:
    bundle = _pair("companies_house", "r1", [
        {"type": "shareholding", "startDate": "2019-01-01", "endDate": "2021-06-30"},
    ])
    (edge,) = _edge_lines(to_cypher(bundle, as_of="2026-11-24"))
    assert "r.ended = true" in edge
    assert "r.endDate = '2021-06-30'" in edge
    assert "r.endedAsOf = '2026-11-24'" in edge
    assert "r.startDate = '2019-01-01'" in edge
    assert "r.undated = true" not in edge


def test_ended_is_judged_on_the_given_day() -> None:
    bundle = _pair("companies_house", "r1", [
        {"type": "shareholding", "startDate": "2019-01-01", "endDate": "2030-01-01"},
    ])
    (edge,) = _edge_lines(to_cypher(bundle, as_of="2026-11-24"))
    assert "r.ended = false" in edge


def test_an_undated_relationship_is_marked() -> None:
    bundle = _pair("companies_house", "r1", [{"type": "shareholding"}])
    (edge,) = _edge_lines(to_cypher(bundle, as_of="2026-11-24"))
    assert "r.undated = true" in edge


def test_a_band_keeps_both_ends_and_exclusivity() -> None:
    bundle = _pair("companies_house", "r1", [
        {"type": "shareholding",
         "share": {"exclusiveMinimum": 25, "maximum": 50}},
    ])
    (edge,) = _edge_lines(to_cypher(bundle, as_of="2026-11-24"))
    # BODS v0.4: an exclusive bound is its own number, not a flag.
    assert "r.shareExclusiveMin = 25.0" in edge and "r.shareMax = 50.0" in edge
    assert "r.shareMin" not in edge
    assert "r.shareInterest = 'shareholding'" in edge


def test_gleif_level_2_is_marked_not_ownership() -> None:
    parent = make_entity_statement(source_id="gleif", local_id="p", name="Parent plc")
    child = make_entity_statement(source_id="gleif", local_id="c", name="Child Ltd")
    rel = make_relationship_statement(
        source_id="gleif", local_id="c:direct-parent:p",
        subject_statement_id=child["recordId"],
        interested_party_statement_id=parent["recordId"],
        interests=[{"type": "otherInfluenceOrControl", "details": "consolidated"}],
    )
    (edge,) = _edge_lines(to_cypher([parent, child, rel], as_of="2026-11-24"))
    assert "r.notOwnership = true" in edge
    assert "accounting consolidation" in edge


def test_a_register_edge_is_not_marked() -> None:
    bundle = _pair("companies_house", "r1", [{"type": "shareholding"}])
    (edge,) = _edge_lines(to_cypher(bundle, as_of="2026-11-24"))
    assert "notOwnership" not in edge


def test_nodes_carry_their_source() -> None:
    bundle = _pair("companies_house", "r1", [{"type": "shareholding"}])
    cy = to_cypher(bundle)
    node = next(line for line in cy.splitlines() if line.startswith("MERGE (n:Entity"))
    assert "n.sourceId = 'companies_house'" in node
    assert "n.license = " in node


def test_the_script_uses_nothing_outside_core_cypher() -> None:
    cy = to_cypher(_pair("companies_house", "r1", [{"type": "shareholding"}]))
    statements = "\n".join(line for line in cy.splitlines() if not line.startswith("//"))
    assert "apoc." not in statements
    assert "LOAD CSV" not in statements
    assert "IN TRANSACTIONS" not in statements
