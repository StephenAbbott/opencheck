"""The content-addressed statementId (phase B of the record-id ticket).

BODS v0.4: a ``statementId`` is unique per statement and a ``recordId`` is
stable across a record's versions. OpenCheck's factories used to set the
two equal for entities and people, so a record that changed kept its id and
two spellings of one record could not be told apart. Now

* ``recordId`` = ``_stable_id(source, kind, local id)`` — unchanged, so every
  key in the codebase still works;
* ``statementId`` = ``_stable_id(recordId, "content", digest)`` where the
  digest covers ``recordDetails`` (minus ``publicListing``) and
  ``recordStatus`` — never annotations, ``statementDate`` or ``retrievedAt``
  (Stephen, 23 Oct 2026);
* a relationship names its parties by ``recordId``;
* the risk engine's predicted evidence ids are recordIds, resolved to
  statementIds in ``resolve_signal_references``;
* every ``map_*`` is ``content_addressed``, so an edit after the factory
  still leaves with an id that names its content.
"""

from __future__ import annotations

from typing import Any

from opencheck.bods.refs import resolve_signal_references, resolver
from opencheck.bods.statements import (
    BODSBundle,
    _stable_id,
    content_addressed,
    content_statement_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    refresh_statement_id,
    stale_statement_id,
)
from opencheck.bods.unique import unique_statements


def _entity(name: str = "ACME LIMITED", **kw: Any) -> dict[str, Any]:
    return make_entity_statement(source_id="companies_house", local_id="1", name=name, **kw)


# ---------------------------------------------------------------------------
# The id and its scope
# ---------------------------------------------------------------------------


def test_record_id_is_the_key_and_statement_id_is_derived_from_it() -> None:
    e = _entity()
    assert e["recordId"] == _stable_id("companies_house", "entity", "1")
    assert e["statementId"] == content_statement_id(e["recordId"], e["recordDetails"], "new")
    assert e["statementId"] != e["recordId"]
    p = make_person_statement(source_id="companies_house", local_id="p", full_name="Jane")
    assert p["recordId"] == _stable_id("companies_house", "person", "p")
    assert p["statementId"] != p["recordId"]


def test_a_changed_record_gets_a_new_statement_id_and_keeps_its_record_id() -> None:
    a, b = _entity("ACME LIMITED"), _entity("ACME PLC")
    assert a["recordId"] == b["recordId"]
    assert a["statementId"] != b["statementId"]


def test_the_same_content_gives_the_same_statement_id() -> None:
    assert _entity()["statementId"] == _entity()["statementId"]


def test_annotations_dates_and_retrieval_are_outside_the_hash() -> None:
    a = _entity(statement_date="2020-01-01")
    b = _entity(statement_date="2024-06-30")
    assert a["statementId"] == b["statementId"]
    c = dict(a)
    c["annotations"] = [{"motivation": "commenting", "description": "x", "statement": "y"}]
    c["source"] = {**a["source"], "retrievedAt": "2026-10-23T10:00:00Z"}
    assert not stale_statement_id(c)


def test_public_listing_is_outside_the_hash() -> None:
    """The listing is stamped on a copy of the subject's statement after the
    sources settle; the graph node must keep its id."""
    a = _entity()
    b = dict(a)
    b["recordDetails"] = {**a["recordDetails"], "publicListing": {"hasPublicListing": True}}
    assert not stale_statement_id(b)


def test_record_status_is_inside_the_hash() -> None:
    common = dict(
        source_id="companies_house", local_id="r",
        subject_statement_id="s", interested_party_statement_id="i",
        interests=[{"type": "shareholding", "endDate": "2020-01-01"}],
    )
    new = make_relationship_statement(**{**common, "interests": [{"type": "shareholding"}]})
    closed = make_relationship_statement(**common)  # closed by the Phase 317 rule
    assert new["recordId"] == closed["recordId"]
    assert new["statementId"] != closed["statementId"]
    assert closed["recordStatus"] == "closed"


# ---------------------------------------------------------------------------
# Refresh and the guard
# ---------------------------------------------------------------------------


def test_refresh_recomputes_in_place_and_leaves_other_publishers_alone() -> None:
    e = _entity()
    before = e["statementId"]
    e["recordDetails"]["name"] = "ACME PLC"
    assert stale_statement_id(e)
    assert refresh_statement_id(e) is e
    assert e["statementId"] != before and not stale_statement_id(e)
    oo = {"recordId": "oo-123", "statementId": "oo-123", "recordDetails": {"name": "X"}}
    assert refresh_statement_id(oo)["statementId"] == "oo-123"
    assert not stale_statement_id(oo)


def test_content_addressed_refreshes_a_generator_a_list_and_a_bundle() -> None:
    def edited() -> dict[str, Any]:
        e = _entity()
        e["recordDetails"]["name"] = "ACME PLC"  # the id is now stale
        return e

    @content_addressed
    def gen(bundle: dict[str, Any]):
        yield edited()

    @content_addressed
    def as_list(bundle: dict[str, Any]) -> list[dict[str, Any]]:
        return [edited()]

    @content_addressed
    def as_bundle(bundle: dict[str, Any]) -> BODSBundle:
        return BODSBundle(statements=[edited()])

    import inspect

    assert inspect.isgeneratorfunction(gen)  # laziness kept (Phase 314)
    for out in (list(gen({})), as_list({}), list(as_bundle({}))):
        assert len(out) == 1 and not stale_statement_id(out[0])
    assert gen.__opencheck_content_addressed__


def test_every_registered_mapper_is_content_addressed() -> None:
    import opencheck.bods as bods_pkg
    from opencheck.sources import REGISTRY

    missing = [
        sid for sid in REGISTRY
        if hasattr(bods_pkg, f"map_{sid}")
        and not getattr(getattr(bods_pkg, f"map_{sid}"), "__opencheck_content_addressed__", False)
    ]
    assert missing == []


# ---------------------------------------------------------------------------
# References and dedup
# ---------------------------------------------------------------------------


def test_relationships_name_parties_by_record_id_and_resolve_to_statements() -> None:
    subject, owner = _entity(), make_entity_statement(
        source_id="companies_house", local_id="2", name="OWNER"
    )
    rel = make_relationship_statement(
        source_id="companies_house", local_id="r",
        subject_statement_id=subject["recordId"],
        interested_party_statement_id=owner["recordId"], interested_party_type="entity",
        interests=[{"type": "shareholding"}],
    )
    assert rel["recordDetails"]["subject"] == subject["recordId"]
    assert rel["declarationSubject"] == subject["recordId"]
    resolve = resolver([subject, owner, rel])
    assert resolve(rel["recordDetails"]["subject"]) == subject["statementId"]
    assert resolve(rel["recordDetails"]["interestedParty"]) == owner["statementId"]


def test_unique_statements_dedupes_parties_on_record_id() -> None:
    """Two versions of one record in a bundle are one party: the later one
    wins, as the old statementId dedup did when the two ids were equal."""
    old, new = _entity("ACME LIMITED"), _entity("ACME PLC")
    out = unique_statements([old, new])
    assert len(out) == 1
    rel_a = make_relationship_statement(
        source_id="companies_house", local_id="r", subject_statement_id="s",
        interested_party_statement_id="i", interests=[{"type": "shareholding"}],
    )
    rel_b = make_relationship_statement(
        source_id="companies_house", local_id="r", subject_statement_id="s",
        interested_party_statement_id="i",
        interests=[{"type": "shareholding", "endDate": "2020-01-01"}],
    )
    # Relationships still dedupe on statementId: a closed version beside the
    # open one is two statements about one record, both published.
    assert len(unique_statements([rel_a, rel_b])) == 2


def test_resolve_signal_references_maps_every_evidence_field() -> None:
    subject, owner = _entity(), make_entity_statement(
        source_id="companies_house", local_id="2", name="OWNER"
    )
    bods = [subject, owner]
    rid, oid = subject["recordId"], owner["recordId"]
    signals = [
        {"code": "SANCTIONED", "evidence": {"statement_id": rid}},
        {"code": "RELATED_PEP", "evidence": {
            "subject_statement_id": oid, "subject_statement_ids": [oid, "dangling"],
        }},
        {"code": "TRUST_OR_ARRANGEMENT", "evidence": {"matches": [{"statement_id": oid, "x": 1}]}},
        {"code": "FATF_GREY_LIST", "evidence": {"jurisdictions": [{"statement_id": rid}]}},
        {"code": "COMPLEX_OWNERSHIP_LAYERS", "evidence": {
            "longest_path": [rid, oid], "ended_relationship_statement_ids": [oid],
        }},
        {"code": "NO_EVIDENCE"},
        {"code": "ALREADY", "evidence": {"statement_id": subject["statementId"]}},
    ]
    out = resolve_signal_references(signals, bods)
    sid, osid = subject["statementId"], owner["statementId"]
    assert out[0]["evidence"]["statement_id"] == sid
    assert out[1]["evidence"] == {"subject_statement_id": osid, "subject_statement_ids": [osid, "dangling"]}
    assert out[2]["evidence"]["matches"] == [{"statement_id": osid, "x": 1}]
    assert out[3]["evidence"]["jurisdictions"] == [{"statement_id": sid}]
    assert out[4]["evidence"]["longest_path"] == [sid, osid]
    assert out[4]["evidence"]["ended_relationship_statement_ids"] == [osid]
    assert out[5] is signals[5] and out[6] is signals[6]  # untouched, not copied
    assert signals[0]["evidence"]["statement_id"] == rid  # the input is not mutated


def test_the_risk_engine_predicts_the_ftm_record_id() -> None:
    """The OpenSanctions subject-level signals used to carry a two-part id no
    mapper ever wrote; now they carry the record's recordId, which the
    resolver maps to its statementId."""
    from opencheck.bods.mappers.ftm import map_opensanctions
    from opencheck.risk import _opensanctions_topic_signals_from_entity

    entity = {
        "id": "Q123", "schema": "Company", "caption": "ACME",
        "properties": {"name": ["ACME"], "topics": ["sanction"]},
    }
    bods = list(map_opensanctions({"entity": entity}))
    subject = next(s for s in bods if s["recordType"] == "entity")
    signals = [s.to_dict() for s in _opensanctions_topic_signals_from_entity("Q123", entity)]
    assert signals and signals[0]["evidence"]["statement_id"] == subject["recordId"]
    resolved = resolve_signal_references(signals, bods)
    assert resolved[0]["evidence"]["statement_id"] == subject["statementId"]
