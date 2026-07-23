"""FtM nested edge-entity mapping — model-driven handling via _FTM_EDGE_SCHEMAS.

Regression tests for the person-side nested-edge bug: yente nests *edge
entities* under reverse property names on BOTH sides of an edge, so a Person
payload carries Directorship entities under ``directorshipDirector``, Associate
under ``associates``, Family under ``familyPerson``, etc. The old code treated
those keys as "legacy flat props" whose entries were the party itself, which
emitted phantom BODS entities named after edge captions ("director of Acme
Ltd") and dropped the real counterparty.

Fixtures mirror the yente shapes already pinned for ``positionOccupancies`` in
``test_person_check.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck.bods.mapper import (
    _FTM_EDGE_SCHEMAS,
    map_ftm,
)


def _names(bundle) -> set[str]:
    out: set[str] = set()
    for s in bundle.statements:
        if s.get("recordType") in ("entity", "person"):
            details = s.get("recordDetails") or {}
            name = details.get("name")
            if isinstance(name, str):
                out.add(name)
            for n in details.get("names") or []:
                if isinstance(n, dict) and n.get("fullName"):
                    out.add(n["fullName"])
    return out


def _rels(bundle) -> list[dict[str, Any]]:
    return [
        s["recordDetails"]
        for s in bundle.statements
        if s.get("recordType") == "relationship"
    ]


def _statement_by_name(bundle, name: str) -> dict[str, Any] | None:
    for s in bundle.statements:
        if s.get("recordType") not in ("entity", "person"):
            continue
        details = s.get("recordDetails") or {}
        if details.get("name") == name:
            return s
        for n in details.get("names") or []:
            if isinstance(n, dict) and n.get("fullName") == name:
                return s
    return None


def _person_payload(**extra_props: Any) -> dict[str, Any]:
    props: dict[str, Any] = {"name": ["Jane Doe"]}
    props.update(extra_props)
    return {
        "id": "Q123",
        "schema": "Person",
        "caption": "Jane Doe",
        "properties": props,
    }


# ---------------------------------------------------------------------------
# directorshipDirector — person side of a Directorship edge (the bug)
# ---------------------------------------------------------------------------

_DIRECTORSHIP = {
    "id": "dirship-1",
    "schema": "Directorship",
    "caption": "director of Acme Ltd",
    "properties": {
        "role": ["director"],
        "director": ["Q123"],
        "startDate": ["2015-01-01"],
        "organization": [
            {
                "id": "co-1",
                "schema": "Company",
                "caption": "Acme Ltd",
                "properties": {"name": ["Acme Ltd"]},
            }
        ],
    },
}


def test_directorship_director_emits_real_organisation() -> None:
    bundle = map_ftm(
        _person_payload(directorshipDirector=[_DIRECTORSHIP]),
        source_id="opensanctions",
    )
    assert "Acme Ltd" in _names(bundle)


def test_directorship_director_no_phantom_edge_entity() -> None:
    """The Directorship wrapper must never become a BODS entity statement."""
    bundle = map_ftm(
        _person_payload(directorshipDirector=[_DIRECTORSHIP]),
        source_id="opensanctions",
    )
    assert "director of Acme Ltd" not in _names(bundle)


def test_directorship_director_person_is_interested_party_of_org() -> None:
    bundle = map_ftm(
        _person_payload(directorshipDirector=[_DIRECTORSHIP]),
        source_id="opensanctions",
    )
    org = _statement_by_name(bundle, "Acme Ltd")
    assert org is not None
    rels = _rels(bundle)
    assert len(rels) == 1
    rel = rels[0]
    # Subject = the organisation; interested party = the person (subject
    # payload) holding the directorship.
    assert rel["subject"] == org["statementId"]
    person_sid = bundle.statements[0]["statementId"]
    assert rel["interestedParty"] == person_sid


def test_directorship_director_interest_from_role_not_appointment() -> None:
    bundle = map_ftm(
        _person_payload(directorshipDirector=[_DIRECTORSHIP]),
        source_id="opensanctions",
    )
    (rel,) = _rels(bundle)
    (interest,) = rel["interests"]
    # role "director" → boardMember via _FTM_ROLE_TO_INTEREST_TYPE; the old
    # legacy path emitted appointmentOfBoard, which is wrong for directors.
    assert interest["type"] == "boardMember"
    assert interest["details"] == "director"
    assert interest["startDate"] == "2015-01-01"


def test_directorship_unknown_role_defaults_to_senior_managing_official() -> None:
    edge = {
        "id": "dirship-2",
        "schema": "Directorship",
        "properties": {
            "role": ["supervisory weirdness"],
            "organization": [
                {
                    "id": "co-2",
                    "schema": "Company",
                    "properties": {"name": ["Beta GmbH"]},
                }
            ],
        },
    }
    bundle = map_ftm(
        _person_payload(directorshipDirector=[edge]), source_id="opensanctions"
    )
    (rel,) = _rels(bundle)
    assert rel["interests"][0]["type"] == "seniorManagingOfficial"


# ---------------------------------------------------------------------------
# Family / Associate — screening context, never BODS relationships
# ---------------------------------------------------------------------------

_ASSOCIATE = {
    "id": "assoc-1",
    "schema": "Associate",
    "caption": "associate of John Smith",
    "properties": {
        "person": ["Q123"],
        "associate": [
            {
                "id": "p-2",
                "schema": "Person",
                "caption": "John Smith",
                "properties": {"name": ["John Smith"]},
            }
        ],
    },
}

_FAMILY = {
    "id": "fam-1",
    "schema": "Family",
    "caption": "spouse of Mary Doe",
    "properties": {
        "person": ["Q123"],
        "relationship": ["spouse"],
        "relative": [
            {
                "id": "p-3",
                "schema": "Person",
                "caption": "Mary Doe",
                "properties": {"name": ["Mary Doe"]},
            }
        ],
    },
}


def test_associate_edge_emits_no_statements() -> None:
    bundle = map_ftm(
        _person_payload(associates=[_ASSOCIATE]), source_id="opensanctions"
    )
    # Only the subject person statement — no phantom entity, no relationship.
    assert len(bundle.statements) == 1
    assert "associate of John Smith" not in _names(bundle)


def test_family_edge_emits_no_statements() -> None:
    bundle = map_ftm(
        _person_payload(familyPerson=[_FAMILY]), source_id="opensanctions"
    )
    assert len(bundle.statements) == 1


# ---------------------------------------------------------------------------
# Membership / Representation / UnknownLink — newly mapped edge schemas
# ---------------------------------------------------------------------------


def test_membership_maps_to_other_influence_or_control() -> None:
    edge = {
        "id": "mem-1",
        "schema": "Membership",
        "properties": {
            "role": ["executive committee"],
            "organization": [
                {
                    "id": "org-1",
                    "schema": "Organization",
                    "properties": {"name": ["Sample Council"]},
                }
            ],
        },
    }
    bundle = map_ftm(
        _person_payload(membershipMember=[edge]), source_id="opensanctions"
    )
    assert "Sample Council" in _names(bundle)
    (rel,) = _rels(bundle)
    assert rel["interests"][0]["type"] == "otherInfluenceOrControl"
    assert rel["interests"][0]["details"] == "executive committee"


def test_representation_maps_to_nominee() -> None:
    # Subject is the agent (reverse prop ``agencyClient``); client nested.
    edge = {
        "id": "rep-1",
        "schema": "Representation",
        "properties": {
            "role": ["registered agent"],
            "agent": ["E-1"],
            "client": [
                {
                    "id": "co-9",
                    "schema": "Company",
                    "properties": {"name": ["Client Holdings"]},
                }
            ],
        },
    }
    payload = {
        "id": "E-1",
        "schema": "Company",
        "caption": "Agent Services Ltd",
        "properties": {"name": ["Agent Services Ltd"], "agencyClient": [edge]},
    }
    bundle = map_ftm(payload, source_id="opensanctions")
    client = _statement_by_name(bundle, "Client Holdings")
    assert client is not None
    (rel,) = _rels(bundle)
    assert rel["interests"][0]["type"] == "nominee"
    # Agent (the subject) is the interested party; client is the subject.
    assert rel["subject"] == client["statementId"]
    assert rel["interestedParty"] == bundle.statements[0]["statementId"]


def test_unknown_link_maps_to_unknown_interest() -> None:
    edge = {
        "id": "ul-1",
        "schema": "UnknownLink",
        "properties": {
            "role": ["linked"],
            "subject": ["Q123"],
            "object": [
                {
                    "id": "co-7",
                    "schema": "Company",
                    "properties": {"name": ["Mystery Corp"]},
                }
            ],
        },
    }
    bundle = map_ftm(
        _person_payload(unknownLinkTo=[edge]), source_id="opensanctions"
    )
    (rel,) = _rels(bundle)
    assert rel["interests"][0]["type"] == "unknownInterest"


# ---------------------------------------------------------------------------
# Generic mechanics
# ---------------------------------------------------------------------------


def test_edge_nested_under_unexpected_key_is_still_handled() -> None:
    """The handler keys off ``schema``, not the property name it sits under."""
    bundle = map_ftm(
        _person_payload(somethingNew=[_DIRECTORSHIP]), source_id="opensanctions"
    )
    assert "Acme Ltd" in _names(bundle)
    assert len(_rels(bundle)) == 1


def test_same_edge_under_two_keys_is_deduplicated() -> None:
    bundle = map_ftm(
        _person_payload(
            directorshipDirector=[_DIRECTORSHIP], alsoHere=[_DIRECTORSHIP]
        ),
        source_id="opensanctions",
    )
    assert len(_rels(bundle)) == 1


def test_edge_with_both_sides_nested_links_the_two_parties() -> None:
    """Neither endpoint is the subject — the edge still links its parties."""
    edge = {
        "id": "own-9",
        "schema": "Ownership",
        "properties": {
            "percentage": ["40"],
            "owner": [
                {
                    "id": "co-a",
                    "schema": "Company",
                    "properties": {"name": ["Owner AS"]},
                }
            ],
            "asset": [
                {
                    "id": "co-b",
                    "schema": "Company",
                    "properties": {"name": ["Asset AS"]},
                }
            ],
        },
    }
    bundle = map_ftm(
        _person_payload(ownershipOwner=[edge]), source_id="opensanctions"
    )
    owner = _statement_by_name(bundle, "Owner AS")
    asset = _statement_by_name(bundle, "Asset AS")
    assert owner is not None and asset is not None
    (rel,) = _rels(bundle)
    assert rel["interestedParty"] == owner["statementId"]
    assert rel["subject"] == asset["statementId"]
    assert rel["interests"][0]["share"] == {"exact": 40.0}


def _ownership_edge(pct: str = "60") -> dict[str, Any]:
    return {
        "id": "own-bo",
        "schema": "Ownership",
        "properties": {
            "percentage": [pct],
            "owner": ["Q123"],
            "asset": [
                {
                    "id": "co-owned",
                    "schema": "Company",
                    "properties": {"name": ["Owned Ltd"]},
                }
            ],
        },
    }


def test_ownership_does_not_assert_beneficial_ownership_by_default() -> None:
    """FtM Ownership is registered/legal ownership unless a BO dataset says
    otherwise (R3): the flag must be unset, not True."""
    payload = _person_payload(ownershipOwner=[_ownership_edge()])
    bundle = map_ftm(payload, source_id="opensanctions")
    (rel,) = _rels(bundle)
    (interest,) = rel["interests"]
    assert interest["type"] == "shareholding"
    assert "beneficialOwnershipOrControl" not in interest
    # directOrIndirect is retained; share still captured.
    assert interest["directOrIndirect"] == "direct"
    assert interest["share"] == {"exact": 60.0}


def test_ownership_asserts_bo_when_openownership_dataset_present_on_edge() -> None:
    edge = _ownership_edge()
    edge["datasets"] = ["openownership"]
    payload = _person_payload(ownershipOwner=[edge])
    bundle = map_ftm(payload, source_id="opensanctions")
    (rel,) = _rels(bundle)
    assert rel["interests"][0]["beneficialOwnershipOrControl"] is True


def test_ownership_asserts_bo_when_subject_carries_bo_dataset() -> None:
    """The dataset signal can live on the subject entity, not just the edge."""
    payload = _person_payload(ownershipOwner=[_ownership_edge()])
    payload["datasets"] = ["openownership"]
    bundle = map_ftm(payload, source_id="opensanctions")
    (rel,) = _rels(bundle)
    assert rel["interests"][0]["beneficialOwnershipOrControl"] is True


def test_out_of_scope_edges_emit_nothing() -> None:
    edge = {
        "id": "pay-1",
        "schema": "Payment",
        "properties": {
            "beneficiary": [
                {
                    "id": "co-z",
                    "schema": "Company",
                    "properties": {"name": ["Payee Ltd"]},
                }
            ],
        },
    }
    bundle = map_ftm(
        _person_payload(paymentPayer=[edge]), source_id="opensanctions"
    )
    assert len(bundle.statements) == 1


# ---------------------------------------------------------------------------
# Vendored-table drift (also enforced by scripts/check_ftm_edges.py in CI)
# ---------------------------------------------------------------------------


def test_vendored_edge_table_matches_followthemoney_model() -> None:
    ftm = pytest.importorskip("followthemoney")
    model_edges = {
        s.name: (s.source_prop.name, s.target_prop.name)
        for s in ftm.model.schemata.values()
        if s.edge and s.source_prop is not None and s.target_prop is not None
    }
    vendored = {
        name: (src, tgt) for name, (src, tgt, _policy) in _FTM_EDGE_SCHEMAS.items()
    }
    assert vendored == model_edges
