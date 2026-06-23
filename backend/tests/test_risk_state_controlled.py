"""Tests for the STATE_CONTROLLED risk signal.

Fires (BODS-derived, source-agnostic) when a controlling owner is modelled as a
``state`` / ``stateBody`` entity — the structure map_wikidata emits per the BODS
SOE modelling requirement. Medium confidence, presence-only, corroborating.
"""

from __future__ import annotations

from opencheck.risk import (
    STATE_CONTROLLED,
    _state_controlled_signals,
    assess_bundle,
)


def _entity(sid: str, *, entity_type: str = "registeredEntity", name: str = "Acme") -> dict:
    return {
        "statementId": sid,
        "recordType": "entity",
        "recordDetails": {"entityType": {"type": entity_type}, "name": name},
    }


def _rel(sid: str, subject: str, ip: str) -> dict:
    return {
        "statementId": sid,
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": subject,
            "interestedParty": ip,
            "interests": [{"type": "otherInfluenceOrControl"}],
        },
    }


def _bundle(owner_type: str):
    return [
        _entity("E1", name="Subject Co"),
        _entity("S1", entity_type=owner_type, name="Ministry of Energy"),
        _rel("R1", "E1", "S1"),
    ]


def test_statebody_owner_fires_state_controlled():
    sigs = _state_controlled_signals("wikidata", "Q1", _bundle("stateBody"))
    assert len(sigs) == 1
    s = sigs[0]
    assert s.code == STATE_CONTROLLED
    assert s.confidence == "medium"
    assert s.evidence["statement_id"] == "S1"          # the state node (overlay)
    assert s.evidence["subject_statement_id"] == "E1"  # the controlled entity
    assert "Ministry of Energy" in s.evidence["state_owners"]


def test_state_entity_type_also_fires():
    sigs = _state_controlled_signals("wikidata", "Q1", _bundle("state"))
    assert [s.code for s in sigs] == [STATE_CONTROLLED]


def test_private_owner_does_not_fire():
    # A foundation owner (registeredEntity) is not a state body → no signal.
    sigs = _state_controlled_signals("wikidata", "Q1", _bundle("registeredEntity"))
    assert sigs == []


def test_unreferenced_state_entity_does_not_fire():
    # A state entity with no relationship pointing at it must not fire (defensive).
    bods = [_entity("E1", name="Subject Co"), _entity("S1", entity_type="state", name="A State")]
    assert _state_controlled_signals("wikidata", "Q1", bods) == []


def test_wired_into_assess_bundle():
    sigs = assess_bundle("wikidata", {"hit_id": "Q1"}, _bundle("stateBody"), hit_id="Q1")
    assert STATE_CONTROLLED in {s.code for s in sigs}
