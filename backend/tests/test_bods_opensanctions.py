"""Tests for OpenSanctions → BODS mapping via map_opensanctions / map_ftm.

Focuses on the nested Ownership/Directorship schema objects introduced by the
GEM (Global Energy Monitor) datasets, where OpenSanctions embeds full entity
graphs under ``ownershipOwner``, ``ownershipAsset``, and
``directorshipOrganization`` properties.
"""
from __future__ import annotations

import pytest

from opencheck.bods import map_opensanctions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bundle(entity: dict) -> dict:
    """Wrap a raw FtM entity dict in the adapter fetch output shape."""
    return {"source_id": "opensanctions", "entity_id": entity["id"], "entity": entity}


def _stmts_by_type(bods_bundle, record_type: str) -> list[dict]:
    return [s for s in bods_bundle.statements if s.get("recordType") == record_type]


# ---------------------------------------------------------------------------
# ownershipOwner — subject is the owner
# ---------------------------------------------------------------------------


def _bp_like_entity() -> dict:
    """Minimal FtM Company with ownershipOwner entries (BP-style)."""
    return {
        "id": "NK-bp",
        "schema": "Company",
        "caption": "BP PLC",
        "properties": {
            "name": ["BP PLC"],
            "country": ["gb"],
            "leiCode": ["213800LH1BZH3DI6G760"],
            "ownershipOwner": [
                {
                    "id": "gem-own-001",
                    "caption": "Ownership",
                    "schema": "Ownership",
                    "properties": {
                        "owner": ["NK-bp"],
                        "asset": [
                            {
                                "id": "NK-bp-int",
                                "caption": "Bp International Limited",
                                "schema": "Company",
                                "properties": {
                                    "name": ["Bp International Limited"],
                                    "country": ["gb"],
                                    "registrationNumber": ["00542515"],
                                },
                            }
                        ],
                        "percentage": ["100.00"],
                        "sourceUrl": ["https://www.gov.uk/get-information-about-a-company"],
                    },
                },
                # Second subsidiary — no percentage
                {
                    "id": "gem-own-002",
                    "caption": "Ownership",
                    "schema": "Ownership",
                    "properties": {
                        "owner": ["NK-bp"],
                        "asset": [
                            {
                                "id": "NK-pan-am",
                                "caption": "Pan American Energy",
                                "schema": "Company",
                                "properties": {
                                    "name": ["Pan American Energy"],
                                    "country": ["ar"],
                                },
                            }
                        ],
                        "percentage": ["50.00"],
                    },
                },
            ],
        },
    }


def test_ownership_owner_emits_subsidiary_entity_statements():
    bundle = _make_bundle(_bp_like_entity())
    result = map_opensanctions(bundle)

    entity_stmts = _stmts_by_type(result, "entity")
    entity_names = {
        s["recordDetails"]["name"]
        for s in entity_stmts
        if isinstance(s.get("recordDetails"), dict)
    }
    assert "BP PLC" in entity_names
    assert "Bp International Limited" in entity_names
    assert "Pan American Energy" in entity_names


def test_ownership_owner_emits_relationship_statements():
    bundle = _make_bundle(_bp_like_entity())
    result = map_opensanctions(bundle)

    rel_stmts = _stmts_by_type(result, "relationship")
    assert len(rel_stmts) == 2


def test_ownership_owner_relationship_interest_type_and_percentage():
    bundle = _make_bundle(_bp_like_entity())
    result = map_opensanctions(bundle)

    # Find the relationship for Bp International (100%)
    rel_stmts = _stmts_by_type(result, "relationship")
    # Both must be shareholding. This BP fixture carries no BO-asserting
    # dataset (it is registry/GLEIF-style *legal* ownership), so the
    # beneficialOwnershipOrControl flag must be left unset rather than
    # claiming a registered holding is a beneficial one (R3).
    for rel in rel_stmts:
        interests = rel["recordDetails"]["interests"]
        assert len(interests) == 1
        assert interests[0]["type"] == "shareholding"
        assert "beneficialOwnershipOrControl" not in interests[0]

    # One must have exact share 100.0
    pcts = {
        rel["recordDetails"]["interests"][0].get("share", {}).get("exact")
        for rel in rel_stmts
    }
    assert 100.0 in pcts
    assert 50.0 in pcts


def test_ownership_owner_subject_is_interested_party():
    """The subject entity (BP) should be the interestedParty, not the subject."""
    bundle = _make_bundle(_bp_like_entity())
    result = map_opensanctions(bundle)

    # Find the BP entity statement
    entity_stmts = _stmts_by_type(result, "entity")
    bp_stmt = next(
        s for s in entity_stmts
        if isinstance(s.get("recordDetails"), dict)
        and s["recordDetails"].get("name") == "BP PLC"
    )
    bp_sid = bp_stmt["statementId"]

    rel_stmts = _stmts_by_type(result, "relationship")
    for rel in rel_stmts:
        assert rel["recordDetails"]["interestedParty"] == bp_sid, (
            "BP should be the interestedParty in ownershipOwner relationships"
        )


# ---------------------------------------------------------------------------
# ownershipAsset — subject is the asset (is owned by others)
# ---------------------------------------------------------------------------


def _bp_owned_entity() -> dict:
    """Minimal FtM Company with ownershipAsset entries."""
    return {
        "id": "NK-bp",
        "schema": "Company",
        "caption": "BP PLC",
        "properties": {
            "name": ["BP PLC"],
            "country": ["gb"],
            "ownershipAsset": [
                {
                    "id": "gem-own-100",
                    "caption": "Ownership",
                    "schema": "Ownership",
                    "properties": {
                        "owner": [
                            {
                                "id": "NK-blackrock",
                                "caption": "BlackRock",
                                "schema": "Company",
                                "properties": {
                                    "name": ["BlackRock Inc"],
                                    "country": ["us"],
                                    "leiCode": ["549300LRIF3NWCU26A80"],
                                },
                            }
                        ],
                        "asset": ["NK-bp"],
                        "percentage": ["9.20"],
                    },
                },
                {
                    "id": "gem-own-101",
                    "caption": "Ownership",
                    "schema": "Ownership",
                    "properties": {
                        "owner": [
                            {
                                "id": "NK-guaranty",
                                "caption": "Guaranty Nominees",
                                "schema": "Company",
                                "properties": {
                                    "name": ["Guaranty Nominees Ltd"],
                                    "country": ["gb"],
                                },
                            }
                        ],
                        "asset": ["NK-bp"],
                        "percentage": ["26.20"],
                    },
                },
            ],
        },
    }


def test_ownership_asset_emits_owner_entity_statements():
    bundle = _make_bundle(_bp_owned_entity())
    result = map_opensanctions(bundle)

    entity_stmts = _stmts_by_type(result, "entity")
    names = {
        s["recordDetails"]["name"]
        for s in entity_stmts
        if isinstance(s.get("recordDetails"), dict)
    }
    assert "BlackRock Inc" in names
    assert "Guaranty Nominees Ltd" in names


def test_ownership_asset_emits_two_relationship_statements():
    bundle = _make_bundle(_bp_owned_entity())
    result = map_opensanctions(bundle)
    assert len(_stmts_by_type(result, "relationship")) == 2


def test_ownership_asset_subject_is_subject_in_ooc():
    """BP (the asset) should be the subject of the OOC statement."""
    bundle = _make_bundle(_bp_owned_entity())
    result = map_opensanctions(bundle)

    entity_stmts = _stmts_by_type(result, "entity")
    bp_stmt = next(
        s for s in entity_stmts
        if isinstance(s.get("recordDetails"), dict)
        and s["recordDetails"].get("name") == "BP PLC"
    )
    bp_sid = bp_stmt["statementId"]

    rel_stmts = _stmts_by_type(result, "relationship")
    for rel in rel_stmts:
        assert rel["recordDetails"]["subject"] == bp_sid, (
            "BP should be the subject (asset) in ownershipAsset relationships"
        )
        assert rel["recordDetails"]["interests"][0]["type"] == "shareholding"


def test_ownership_asset_percentage_captured():
    bundle = _make_bundle(_bp_owned_entity())
    result = map_opensanctions(bundle)

    pcts = {
        rel["recordDetails"]["interests"][0].get("share", {}).get("exact")
        for rel in _stmts_by_type(result, "relationship")
    }
    assert 9.2 in pcts
    assert 26.2 in pcts


# ---------------------------------------------------------------------------
# directorshipOrganization — subject is the organisation; directors are IPs
# ---------------------------------------------------------------------------


def _rosneft_like_entity() -> dict:
    return {
        "id": "NK-rosneft",
        "schema": "Company",
        "caption": "Rosneft Oil Company",
        "properties": {
            "name": ["Rosneft Oil Company"],
            "country": ["ru"],
            "directorshipOrganization": [
                {
                    "id": "ru-dir-001",
                    "caption": "Directorship",
                    "schema": "Directorship",
                    "properties": {
                        "role": ["CEO"],
                        "startDate": ["2012-06-09"],
                        "director": [
                            {
                                "id": "NK-sechin",
                                "caption": "Igor Sechin",
                                "schema": "Person",
                                "properties": {
                                    "name": ["Igor Sechin"],
                                    "nationality": ["ru"],
                                },
                            }
                        ],
                        "organization": ["NK-rosneft"],
                    },
                },
                {
                    "id": "ru-dir-002",
                    "caption": "Directorship",
                    "schema": "Directorship",
                    "properties": {
                        "role": ["Board Member"],
                        "director": [
                            {
                                "id": "NK-board-member-1",
                                "caption": "Board Person",
                                "schema": "Person",
                                "properties": {
                                    "name": ["Board Person"],
                                },
                            }
                        ],
                        "organization": ["NK-rosneft"],
                    },
                },
            ],
        },
    }


def test_directorship_emits_person_statements():
    bundle = _make_bundle(_rosneft_like_entity())
    result = map_opensanctions(bundle)

    person_stmts = _stmts_by_type(result, "person")
    # Person statements store names under recordDetails.names[].fullName
    names = set()
    for s in person_stmts:
        rd = s.get("recordDetails") or {}
        for n in rd.get("names") or []:
            names.add(n.get("fullName", ""))
    assert "Igor Sechin" in names
    assert "Board Person" in names


def test_directorship_emits_relationship_statements():
    bundle = _make_bundle(_rosneft_like_entity())
    result = map_opensanctions(bundle)
    assert len(_stmts_by_type(result, "relationship")) == 2


def test_directorship_interest_types():
    bundle = _make_bundle(_rosneft_like_entity())
    result = map_opensanctions(bundle)

    rel_stmts = _stmts_by_type(result, "relationship")
    interest_types = {
        rel["recordDetails"]["interests"][0]["type"] for rel in rel_stmts
    }
    # CEO → seniorManagingOfficial; Board Member → boardMember
    assert "seniorManagingOfficial" in interest_types
    assert "boardMember" in interest_types


def test_directorship_start_date_captured():
    bundle = _make_bundle(_rosneft_like_entity())
    result = map_opensanctions(bundle)

    rel_stmts = _stmts_by_type(result, "relationship")
    ceo_rel = next(
        r for r in rel_stmts
        if r["recordDetails"]["interests"][0]["type"] == "seniorManagingOfficial"
    )
    assert ceo_rel["recordDetails"]["interests"][0].get("startDate") == "2012-06-09"


def test_directorship_role_in_details():
    bundle = _make_bundle(_rosneft_like_entity())
    result = map_opensanctions(bundle)

    rel_stmts = _stmts_by_type(result, "relationship")
    for rel in rel_stmts:
        interest = rel["recordDetails"]["interests"][0]
        assert "details" in interest


def test_directorship_subject_is_organisation():
    """Rosneft should be the subject of the OOC statement."""
    bundle = _make_bundle(_rosneft_like_entity())
    result = map_opensanctions(bundle)

    entity_stmts = _stmts_by_type(result, "entity")
    rosneft = next(
        s for s in entity_stmts
        if isinstance(s.get("recordDetails"), dict)
        and s["recordDetails"].get("name") == "Rosneft Oil Company"
    )
    rosneft_sid = rosneft["statementId"]

    for rel in _stmts_by_type(result, "relationship"):
        assert rel["recordDetails"]["subject"] == rosneft_sid


# ---------------------------------------------------------------------------
# Stub / empty payloads — should not crash
# ---------------------------------------------------------------------------


def test_stub_payload_returns_empty_bundle():
    bundle = {"source_id": "opensanctions", "hit_id": "NK-stub", "is_stub": True}
    result = map_opensanctions(bundle)
    assert result.statements == []


def test_no_ownership_data_returns_single_entity_statement():
    bundle = _make_bundle(
        {
            "id": "NK-minimal",
            "schema": "Company",
            "caption": "Minimal Corp",
            "properties": {"name": ["Minimal Corp"]},
        }
    )
    result = map_opensanctions(bundle)
    assert len(result.statements) == 1
    assert result.statements[0]["recordType"] == "entity"


def test_string_only_asset_is_skipped():
    """When asset is a plain string ID (not a dict), no extra statements emitted."""
    bundle = _make_bundle(
        {
            "id": "NK-parent",
            "schema": "Company",
            "caption": "Parent Corp",
            "properties": {
                "name": ["Parent Corp"],
                "ownershipOwner": [
                    {
                        "id": "gem-own-str",
                        "schema": "Ownership",
                        "properties": {
                            "owner": ["NK-parent"],
                            "asset": ["NK-child-string-only"],  # plain string — skip
                            "percentage": ["100.00"],
                        },
                    }
                ],
            },
        }
    )
    result = map_opensanctions(bundle)
    # Only the subject entity statement, no relationship
    assert len(result.statements) == 1
    assert result.statements[0]["recordType"] == "entity"


# ---------------------------------------------------------------------------
# Combined ownershipOwner + ownershipAsset
# ---------------------------------------------------------------------------


def test_combined_owner_and_asset_all_statements_present():
    entity = {
        "id": "NK-midco",
        "schema": "Company",
        "caption": "Mid Co",
        "properties": {
            "name": ["Mid Co"],
            "ownershipOwner": [
                {
                    "id": "gem-own-down",
                    "schema": "Ownership",
                    "properties": {
                        "owner": ["NK-midco"],
                        "asset": [
                            {
                                "id": "NK-sub",
                                "schema": "Company",
                                "caption": "Sub Co",
                                "properties": {"name": ["Sub Co"]},
                            }
                        ],
                        "percentage": ["100.00"],
                    },
                }
            ],
            "ownershipAsset": [
                {
                    "id": "gem-own-up",
                    "schema": "Ownership",
                    "properties": {
                        "owner": [
                            {
                                "id": "NK-parent",
                                "schema": "Company",
                                "caption": "Parent Co",
                                "properties": {"name": ["Parent Co"]},
                            }
                        ],
                        "asset": ["NK-midco"],
                        "percentage": ["80.00"],
                    },
                }
            ],
        },
    }
    bundle = _make_bundle(entity)
    result = map_opensanctions(bundle)

    names = {
        s["recordDetails"]["name"]
        for s in _stmts_by_type(result, "entity")
        if isinstance(s.get("recordDetails"), dict)
    }
    assert names == {"Mid Co", "Sub Co", "Parent Co"}
    assert len(_stmts_by_type(result, "relationship")) == 2
