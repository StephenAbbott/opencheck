"""Phase 276 — the Senzing and FtM exports keep unspecified-party relationships.

Phase 274 made the Cypher export draw a party BODS cannot name — an
``UnspecifiedRecord`` (``{reason, description}``) in a relationship's
``subject`` or ``interestedParty`` — as a per-statement placeholder, and fixed
RDF's subject side. The Senzing and FtM exports still discarded the whole
relationship, and an unspecified *subject* in Senzing was worse than silent: it
folded a ``REL_POINTER`` with an **empty key** onto the owner's record, a
pointer that lands on no anchor.

Each export needed its own shape, because each consumer does something
different with a placeholder:

* **Senzing resolves records automatically.** A placeholder with a name or an
  identifier would be merged with every other placeholder into one entity,
  inventing links between unrelated companies. So a placeholder carries only
  ``REL_*`` features — the spec's "Relationship" category, not used for
  matching — and its reason as payload. Nothing on it can resolve.
* **FtM needs a ``LegalEntity`` at the owner end** of an ``Ownership``, and
  every LegalEntity schema is matchable (Aleph xref, yente). The placeholder is
  named after the company it is unspecified for, so names do not collide
  across companies; the subject side is a non-matchable ``Asset``. FtM's id
  grammar rejects Cypher's ``#side`` suffix, so the ids are
  ``<statementId>-unspecified-<side>``.

Both are keyed per relationship statement, never per reason — the Phase 274
decision for Cypher.
"""

from __future__ import annotations

from typing import Any

import pytest
from rdflib import Dataset
from rdflib.namespace import RDF

from opencheck.bods import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    to_cypher,
    to_rdf,
)
from opencheck.bods.ftm import map_to_ftm
from opencheck.bods.rdf import BODS
from opencheck.bods.refs import unspecified_party
from opencheck.bods.senzing import map_to_senzing

_REASON = "interestedPartyHasNotProvidedInformation"
_DESCRIPTION = "The PSC has not provided the company's information."


def _company(local_id: str = "acme", name: str = "Acme Ltd") -> dict[str, Any]:
    return make_entity_statement(source_id="companies_house", local_id=local_id, name=name)


def _unspecified_owner(subject: dict[str, Any], local_id: str = "r1") -> dict[str, Any]:
    """Built the way the Companies House mapper builds it."""
    return make_relationship_statement(
        source_id="companies_house",
        local_id=local_id,
        subject_statement_id=subject["statementId"],
        interested_party_unspecified={"reason": _REASON, "description": _DESCRIPTION},
        interests=[{"type": "shareholding", "beneficialOwnershipOrControl": True}],
    )


def _unspecified_subject(party: dict[str, Any], interest: str = "votingRights") -> dict[str, Any]:
    return {
        "statementId": "rel-subj",
        "recordId": "rel-subj",
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": {"reason": "unknown", "description": "Held through an undisclosed vehicle."},
            "interestedParty": party["recordId"],
            "interests": [{"type": interest}],
        },
    }


def _ticket_bundle() -> list[dict[str, Any]]:
    """The ticket's test shape: one unspecified owner, one unspecified subject."""
    company = _company()
    owner = make_person_statement(
        source_id="companies_house", local_id="jane", full_name="Jane Doe"
    )
    return [company, owner, _unspecified_owner(company), _unspecified_subject(owner)]


# ---------------------------------------------------------------------------
# refs.unspecified_party — the one reader of the record's shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"reason": "unknown", "description": "x"}, {"reason": "unknown", "description": "x"}),
        ({"unspecified": {"reason": "unknown"}}, {"reason": "unknown"}),  # older wrapped shape
        ({}, {}),  # an unspecified record with nothing in it is still unspecified
        ("rec-1", None),  # a v0.4 reference
        ({"describedByEntityStatement": "rec-1"}, None),  # a legacy wrapped reference
        (None, None),
    ],
)
def test_unspecified_party_reads_both_shapes_and_rejects_references(raw, expected) -> None:
    assert unspecified_party(raw) == expected


# ---------------------------------------------------------------------------
# Senzing
# ---------------------------------------------------------------------------


def _by_id(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {r["RECORD_ID"]: r for r in records}


def _resolvable(record: dict[str, Any]) -> list[dict[str, Any]]:
    """Every feature Senzing could match on: anything that is not REL_*."""
    return [f for f in record["FEATURES"] if not all(k.startswith("REL_") for k in f)]


def test_senzing_keeps_both_unspecified_relationships() -> None:
    bundle = _ticket_bundle()
    company, owner, rel_owner, _ = bundle
    records = _by_id(map_to_senzing(bundle))

    party_ph = records[f"{rel_owner['statementId']}#interestedParty"]
    assert party_ph["UNSPECIFIED_PARTY"] == "interestedParty"
    assert party_ph["UNSPECIFIED_REASON"] == _REASON
    assert party_ph["UNSPECIFIED_DESCRIPTION"] == _DESCRIPTION
    assert party_ph["BODS_RELATIONSHIP_ID"] == rel_owner["statementId"]
    (pointer,) = [f for f in party_ph["FEATURES"] if "REL_POINTER_KEY" in f]
    assert pointer["REL_POINTER_KEY"] == company["statementId"]
    assert pointer["REL_POINTER_ROLE"] == "OWNER_OF"

    subject_ph = records["rel-subj#subject"]
    assert subject_ph["UNSPECIFIED_REASON"] == "unknown"
    assert subject_ph["FEATURES"] == [
        {"REL_ANCHOR_DOMAIN": "OPENCHECK", "REL_ANCHOR_KEY": "rel-subj#subject"}
    ]
    owner_pointers = [f for f in records[owner["statementId"]]["FEATURES"] if "REL_POINTER_KEY" in f]
    assert [(p["REL_POINTER_KEY"], p["REL_POINTER_ROLE"]) for p in owner_pointers] == [
        ("rel-subj#subject", "VOTING_RIGHTS_IN")
    ]


def test_senzing_unspecified_subject_no_longer_points_at_an_empty_key() -> None:
    """The pre-276 output: REL_POINTER_KEY "" on the owner's record."""
    owner = _company("owner", "Owner Holdings")
    records = map_to_senzing([owner, _unspecified_subject(owner)])
    keys = [f["REL_POINTER_KEY"] for r in records for f in r["FEATURES"] if "REL_POINTER_KEY" in f]
    assert keys and "" not in keys


def test_senzing_placeholders_carry_nothing_resolvable() -> None:
    """Two placeholders in one bundle cannot resolve together — or with anything."""
    a, b = _company("a", "A Ltd"), _company("b", "B Ltd")
    records = map_to_senzing([a, b, _unspecified_owner(a, "ra"), _unspecified_owner(b, "rb")])
    placeholders = [r for r in records if "UNSPECIFIED_PARTY" in r]

    assert len(placeholders) == 2  # per statement, not per reason
    assert len({p["RECORD_ID"] for p in placeholders}) == 2
    for p in placeholders:
        assert _resolvable(p) == []  # no RECORD_TYPE, name, identifier, address
        # Payload must be scalars (the spec's rule for root-level attributes).
        assert all(not isinstance(v, (dict, list)) for k, v in p.items() if k != "FEATURES")


def test_senzing_every_pointer_lands_on_an_anchor() -> None:
    """The spec's integrity rule, across real and placeholder records — which
    also means a dangling reference no longer emits a pointer to nowhere."""
    bundle = _ticket_bundle()
    company = bundle[0]
    bundle.append(
        make_relationship_statement(
            source_id="companies_house",
            local_id="ghost",
            subject_statement_id="not-in-this-bundle",
            interested_party_statement_id=company["statementId"],
            interests=[{"type": "shareholding"}],
        )
    )
    records = map_to_senzing(bundle)
    anchors = {f["REL_ANCHOR_KEY"] for r in records for f in r["FEATURES"] if "REL_ANCHOR_KEY" in f}
    pointers = {f["REL_POINTER_KEY"] for r in records for f in r["FEATURES"] if "REL_POINTER_KEY" in f}
    assert pointers and pointers <= anchors
    assert "not-in-this-bundle" not in pointers
    for r in records:  # and at most one anchor per record
        assert sum("REL_ANCHOR_KEY" in f for f in r["FEATURES"]) <= 1


def test_senzing_placeholder_is_licensed_by_the_relationship_source() -> None:
    company = _company()
    records = _by_id(map_to_senzing([company, _unspecified_owner(company)]))
    placeholder = next(r for r in records.values() if "UNSPECIFIED_PARTY" in r)
    assert placeholder.get("DATA_LICENSE") == records[company["statementId"]].get("DATA_LICENSE")
    assert placeholder.get("DATA_LICENSE")


# ---------------------------------------------------------------------------
# FtM
# ---------------------------------------------------------------------------


def test_ftm_keeps_both_unspecified_relationships() -> None:
    bundle = _ticket_bundle()
    company, owner, rel_owner, _ = bundle
    entities = {e["id"]: e for e in map_to_ftm(bundle)}

    owner_ph = entities[f"{rel_owner['statementId']}-unspecified-interestedParty"]
    assert owner_ph["schema"] == "LegalEntity"
    assert owner_ph["properties"]["name"] == ["Unspecified interested party in Acme Ltd"]
    assert owner_ph["properties"]["notes"] == [
        f"Unspecified interestedParty (reason: {_REASON}) — {_DESCRIPTION}"
    ]
    ownership = entities[rel_owner["statementId"]]
    assert ownership["schema"] == "Ownership"
    assert ownership["properties"]["owner"] == [owner_ph["id"]]
    assert ownership["properties"]["asset"] == [company["statementId"]]
    assert ownership["properties"]["description"] == owner_ph["properties"]["notes"]

    subject_ph = entities["rel-subj-unspecified-subject"]
    assert subject_ph["schema"] == "Asset"  # not matchable
    assert subject_ph["properties"]["name"] == [
        "Unspecified subject of an interest held by Jane Doe"
    ]
    link = entities["rel-subj"]
    assert link["properties"]["owner"] == [owner["statementId"]]
    assert link["properties"]["asset"] == [subject_ph["id"]]


def test_ftm_placeholders_precede_links_and_are_per_statement() -> None:
    a, b = _company("a", "A Ltd"), _company("b", "B Ltd")
    out = map_to_ftm([a, b, _unspecified_owner(a, "ra"), _unspecified_owner(b, "rb")])
    placeholders = [e for e in out if e["id"].endswith("-unspecified-interestedParty")]
    assert len(placeholders) == 2
    # Names differ per company — two undisclosed owners are not one candidate pair.
    assert len({p["properties"]["name"][0] for p in placeholders}) == 2
    order = [e["id"] for e in out]
    for e in out:
        if e["schema"] == "Ownership":
            assert order.index(e["properties"]["owner"][0]) < order.index(e["id"])


def test_ftm_unspecified_subject_of_a_directorship_is_an_organization() -> None:
    director = make_person_statement(source_id="companies_house", local_id="d", full_name="D")
    entities = {e["id"]: e for e in map_to_ftm([director, _unspecified_subject(director, "boardMember")])}
    assert entities["rel-subj-unspecified-subject"]["schema"] == "Organization"
    assert entities["rel-subj"]["schema"] == "Directorship"


def test_ftm_output_is_schema_valid_and_references_resolve() -> None:
    """Every entity loads as FtM, every id survives FtM's id grammar, and every
    entity-valued property points at an emitted entity of an allowed schema."""
    ftm = pytest.importorskip("followthemoney")
    from followthemoney.types import registry

    out = map_to_ftm(_ticket_bundle())
    schemas = {e["id"]: ftm.model.get(e["schema"]) for e in out}
    checked = 0
    for e in out:
        assert registry.entity.clean(e["id"]) == e["id"]
        proxy = ftm.model.get_proxy(e, cleaned=False)
        for prop, value in proxy.itervalues():
            if prop.type != registry.entity:
                continue
            assert value in schemas, (e["id"], prop.name, value)
            assert schemas[value].is_a(prop.range), (e["id"], prop.name, schemas[value])
            checked += 1
        for required in proxy.schema.required:
            assert proxy.get(required), (e["id"], required)
    assert checked == 4  # two links, two ends each — the test is not vacuous


# ---------------------------------------------------------------------------
# All four exports now agree
# ---------------------------------------------------------------------------


def test_cypher_rdf_senzing_and_ftm_agree_about_unspecified_parties() -> None:
    bundle = _ticket_bundle()

    cypher = sum(
        1 for line in to_cypher(bundle).splitlines() if line.startswith("MERGE (u:UnspecifiedParty")
    )
    ds = Dataset()
    ds.parse(data=to_rdf(bundle), format="trig")
    rdf = len({q[0] for q in ds.quads((None, RDF.type, BODS.Unspecified, None))})
    senzing = sum(1 for r in map_to_senzing(bundle) if "UNSPECIFIED_PARTY" in r)
    ftm = sum(1 for e in map_to_ftm(bundle) if "-unspecified-" in e["id"])

    assert cypher == rdf == senzing == ftm == 2
