"""Phase 274 — two BODS v0.4 defects in what OpenCheck emits.

1. **Address types are scoped by record kind.** BODS v0.4 allows a person
   ``residence | service | alternative`` and an entity
   ``registered | business | alternative``. The FtM mapper filed every address
   as ``registered``, so every OpenSanctions / OpenAleph / EveryPolitician
   person with an address failed JSON-schema validation in production (Igor
   Sechin on Rosneft Deutschland, 28 Sept 2026). An FtM address says nothing
   about what it is for, so both kinds now get ``alternative``, and the
   registry-wide mapper guard fails any test whose mapper emits an address type
   its record kind does not allow.

2. **An unspecified party is a finding, not noise.** ``to_cypher`` dropped any
   relationship whose ``subject`` or ``interestedParty`` was an
   ``UnspecifiedRecord`` rather than a reference, so a chain ending in an
   undisclosed owner simply was not in the Neo4j graph while the RDF export
   said ``bods:Unspecified``. Cypher now draws a per-statement
   ``:UnspecifiedParty`` placeholder on either side, and RDF handles the
   subject side it used to drop.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest
from rdflib import Dataset, Literal
from rdflib.namespace import RDF, RDFS

from opencheck.bods import (
    make_entity_statement,
    make_relationship_statement,
    to_cypher,
    to_rdf,
)
from opencheck.bods.mapper import map_ftm
from opencheck.bods.mappers.ftm import FTM_ADDRESS_TYPE
from opencheck.bods.rdf import BODS, REC, STMT
from opencheck.bods.validator import VALID_ADDRESS_TYPES_BY_RECORD, address_type_issues
from tests import _entity_subtype_guard as guard
from tests.test_bods_libcovebods import _to_list, validate_bods_statements

_OS_URL = lambda i: f"https://www.opensanctions.org/entities/{i}/"  # noqa: E731


# ---------------------------------------------------------------------------
# 1. Address types
# ---------------------------------------------------------------------------


def _schema_address_enum(record_file: str) -> set[str]:
    import libcovebods

    path = pathlib.Path(libcovebods.__file__).parent / "data" / "schema-0-4-0" / record_file
    schema = json.loads(path.read_text("utf-8"))
    found: list[set[str]] = []

    def walk(node: Any, under_addresses: bool) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "enum" and under_addresses and isinstance(value, list):
                    found.append(set(value))
                walk(value, under_addresses or key == "addresses")
        elif isinstance(node, list):
            for value in node:
                walk(value, under_addresses)

    walk(schema, False)
    assert len(found) == 1, f"expected one addresses[].type enum in {record_file}, got {found}"
    return found[0]


def test_address_type_table_matches_the_vendored_v04_schema() -> None:
    assert VALID_ADDRESS_TYPES_BY_RECORD["person"] == _schema_address_enum("person-record.json")
    assert VALID_ADDRESS_TYPES_BY_RECORD["entity"] == _schema_address_enum("entity-record.json")
    # The one code the FtM mapper uses must be valid on both kinds.
    assert all(FTM_ADDRESS_TYPE in allowed for allowed in VALID_ADDRESS_TYPES_BY_RECORD.values())


def _sechin() -> dict[str, Any]:
    """The production shape: OpenSanctions Q525666 on Rosneft Deutschland."""
    return {
        "id": "Q525666",
        "schema": "Person",
        "caption": "Игор Сечин",
        "properties": {
            "name": ["Игор Сечин"],
            "address": ["Moscow Russia", "Moscow"],
            "nationality": ["ru"],
            "wikidataId": ["Q525666"],
            "innCode": ["772000000000"],
        },
    }


def test_ftm_person_addresses_are_alternative_and_validate_clean() -> None:
    statements = _to_list(map_ftm(_sechin(), source_id="opensanctions", source_url_builder=_OS_URL))
    person = statements[0]
    assert person["recordType"] == "person"
    assert person["recordDetails"]["addresses"] == [
        {"type": "alternative", "address": "Moscow Russia"},
        {"type": "alternative", "address": "Moscow"},
    ]
    report = validate_bods_statements(statements)
    assert report["json_errors"] == [], report["json_errors"]
    assert report["additional_errors"] == [], report["additional_errors"]


def test_ftm_entity_address_entity_is_alternative_and_validates_clean() -> None:
    statements = _to_list(
        map_ftm(
            {
                "id": "NK-co",
                "schema": "Company",
                "caption": "Acme Holdings",
                "properties": {
                    "name": ["Acme Holdings"],
                    "jurisdiction": ["cy"],
                    "addressEntity": [
                        {
                            "schema": "Address",
                            "properties": {
                                "street": ["1 Main St"],
                                "city": ["Limassol"],
                                "country": ["cy"],
                            },
                        }
                    ],
                },
            },
            source_id="openaleph",
            source_url_builder=lambda i: f"https://aleph.occrp.org/entities/{i}",
        )
    )
    entity = statements[0]
    assert entity["recordType"] == "entity"
    (address,) = entity["recordDetails"]["addresses"]
    assert address["type"] == "alternative"
    assert address["address"] == "1 Main St, Limassol, cy"
    report = validate_bods_statements(statements)
    assert report["json_errors"] == [], report["json_errors"]


@pytest.mark.parametrize(
    ("record_type", "address_type"),
    [
        ("person", "registered"),
        ("person", "business"),
        ("entity", "residence"),
        ("entity", "service"),
        ("person", "placeOfBirth"),
    ],
)
def test_address_type_outside_the_record_kind_is_reported(record_type: str, address_type: str) -> None:
    stmt = {"recordType": record_type, "recordDetails": {"addresses": [{"type": address_type}]}}
    (issue,) = address_type_issues(stmt)
    assert repr(address_type) in issue


@pytest.mark.parametrize(
    "stmt",
    [
        {"recordType": "person", "recordDetails": {"addresses": [{"type": "residence"}, {"address": "x"}]}},
        {"recordType": "entity", "recordDetails": {"addresses": [{"type": "business"}]}},
        {"recordType": "relationship", "recordDetails": {}},
        {"recordType": "person", "recordDetails": {}},
    ],
)
def test_valid_or_untyped_addresses_have_no_issue(stmt: dict[str, Any]) -> None:
    assert address_type_issues(stmt) == []


def test_guard_catches_a_person_registered_address() -> None:
    def map_fake(bundle: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "statementId": "p1",
                "recordType": "person",
                "recordDetails": {"addresses": [{"type": "registered", "address": "Moscow"}]},
            }
        ]

    guard.wrap(map_fake)({})
    (violation,) = guard.drain()
    assert violation[0] == "map_fake"
    assert "'registered'" in violation[2]


def test_guard_leaves_a_publisher_verbatim_bundle_alone() -> None:
    def map_meip(bundle: dict[str, Any]) -> list[dict[str, Any]]:  # name is the key
        return [
            {
                "statementId": "p1",
                "recordType": "person",
                "recordDetails": {"addresses": [{"type": "registered", "address": "Moscow"}]},
            }
        ]

    guard.wrap(map_meip)({})
    assert guard.drain() == []


# ---------------------------------------------------------------------------
# 2. Unspecified parties in the graph exports
# ---------------------------------------------------------------------------

_REASON = "interestedPartyHasNotProvidedInformation"


def _subject() -> dict[str, Any]:
    return make_entity_statement(source_id="companies_house", local_id="acme", name="Acme Ltd")


def _unspecified_owner(subject: dict[str, Any], local_id: str = "r1") -> dict[str, Any]:
    """Built the way the mappers build it (``interested_party_unspecified``)."""
    return make_relationship_statement(
        source_id="companies_house",
        local_id=local_id,
        subject_statement_id=subject["statementId"],
        interested_party_unspecified={
            "reason": _REASON,
            "description": "The PSC has not provided the company's information.",
        },
        interests=[{"type": "shareholding", "beneficialOwnershipOrControl": True}],
    )


def _unspecified_subject(party: dict[str, Any]) -> dict[str, Any]:
    return {
        "statementId": "rel-subj",
        "recordId": "rel-subj",
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": {"reason": "unknown", "description": "Held through an undisclosed vehicle."},
            "interestedParty": party["recordId"],
            "interests": [{"type": "votingRights"}],
        },
    }


def _edges(cypher: str) -> list[str]:
    return [line for line in cypher.splitlines() if "OWNS_OR_CONTROLS" in line]


def test_cypher_keeps_an_edge_to_an_unspecified_owner() -> None:
    subject = _subject()
    rel = _unspecified_owner(subject)
    cy = to_cypher([subject, rel])

    node_id = f"{rel['statementId']}#interestedParty"
    (merge,) = [line for line in cy.splitlines() if line.startswith("MERGE (u:UnspecifiedParty")]
    assert f"{{id: '{node_id}'}}" in merge
    assert f"u.reason = '{_REASON}'" in merge
    assert "u.description = 'The PSC has not provided the company\\'s information.'" in merge
    assert "u.side = 'interestedParty'" in merge

    (edge,) = _edges(cy)
    assert f"(a {{id: '{node_id}'}})" in edge
    assert f"(b {{id: '{subject['statementId']}'}})" in edge
    assert "r.interest = 'shareholding'" in edge
    # The placeholder is MERGEd before the edge that MATCHes it.
    assert cy.index(merge) < cy.index(edge)


def test_cypher_keeps_an_edge_from_an_unspecified_subject() -> None:
    owner = _subject()
    cy = to_cypher([owner, _unspecified_subject(owner)])
    assert "MERGE (u:UnspecifiedParty {id: 'rel-subj#subject'})" in cy
    assert "u.reason = 'unknown'" in cy
    (edge,) = _edges(cy)
    assert f"(a {{id: '{owner['statementId']}'}}), (b {{id: 'rel-subj#subject'}})" in edge


def test_cypher_placeholders_are_per_statement_not_per_reason() -> None:
    """Two chains ending in the same reason must not meet in one hub node."""
    a = make_entity_statement(source_id="companies_house", local_id="a", name="A Ltd")
    b = make_entity_statement(source_id="companies_house", local_id="b", name="B Ltd")
    cy = to_cypher([a, b, _unspecified_owner(a, "ra"), _unspecified_owner(b, "rb")])
    placeholders = [line for line in cy.splitlines() if line.startswith("MERGE (u:UnspecifiedParty")]
    assert len(placeholders) == 2
    assert len({line.split("{id: ")[1].split("}")[0] for line in placeholders}) == 2
    assert len(_edges(cy)) == 2


def test_cypher_still_skips_a_relationship_with_a_missing_party() -> None:
    subject = _subject()
    rel = _unspecified_owner(subject)
    del rel["recordDetails"]["interestedParty"]
    cy = to_cypher([subject, rel])
    assert _edges(cy) == []
    assert "UnspecifiedParty {" not in cy


def _rdf_graph(statements: list[dict[str, Any]], sid: str):
    ds = Dataset()
    ds.parse(data=to_rdf(statements), format="trig")
    return ds.graph(STMT[sid])


def test_rdf_unspecified_subject_becomes_bods_unspecified() -> None:
    owner = _subject()
    g = _rdf_graph([owner, _unspecified_subject(owner)], "rel-subj")
    node = g.value(REC["rel-subj"], BODS.subject)
    assert node is not None
    assert (node, RDF.type, BODS.Unspecified) in g
    assert (node, RDFS.label, Literal("unknown")) in g
    assert (node, RDFS.comment, Literal("Held through an undisclosed vehicle.")) in g
    assert (REC["rel-subj"], BODS.interestedParty, REC[owner["recordId"]]) in g


def test_cypher_and_rdf_agree_about_unspecified_parties() -> None:
    """The two graph exports now say the same thing about the same bundle."""
    subject = _subject()
    bundle = [subject, _unspecified_owner(subject), _unspecified_subject(subject)]

    cy = to_cypher(bundle)
    cypher_placeholders = sum(
        1 for line in cy.splitlines() if line.startswith("MERGE (u:UnspecifiedParty")
    )

    ds = Dataset()
    ds.parse(data=to_rdf(bundle), format="trig")
    rdf_unspecified = {q[0] for q in ds.quads((None, RDF.type, BODS.Unspecified, None))}

    assert len(_edges(cy)) == 2
    assert cypher_placeholders == len(rdf_unspecified) == 2
