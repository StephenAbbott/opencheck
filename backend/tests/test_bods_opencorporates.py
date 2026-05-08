"""Tests for the OpenCorporates → BODS v0.4 mapper."""

from __future__ import annotations

import pytest

from opencheck.bods import validate_shape
from opencheck.bods.mapper import (
    _oc_match_position,
    _oc_parse_network_relationships,
    map_opencorporates,
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _minimal_bundle(
    ocid: str = "gb/00102498",
    name: str = "Test Co Ltd",
    officers: list | None = None,
    network: dict | None = None,
) -> dict:
    return {
        "source_id": "opencorporates",
        "hit_id": ocid,
        "ocid": ocid,
        "company": {
            "name": name,
            "company_number": ocid.split("/")[-1],
            "jurisdiction_code": ocid.split("/")[0],
            "incorporation_date": "2000-01-01",
            "opencorporates_url": f"https://opencorporates.com/companies/{ocid}",
        },
        "officers": officers or [],
        "network": network,
    }


def _officer(
    name: str,
    position: str,
    officer_id: str = "123",
    end_date: str | None = None,
    officer_type: str | None = None,
) -> dict:
    data: dict = {
        "id": officer_id,
        "name": name,
        "position": position,
    }
    if end_date:
        data["end_date"] = end_date
    if officer_type:
        data["type"] = officer_type
    return {"officer": data}


# ---------------------------------------------------------------------
# Officer position → interest type mapping
# ---------------------------------------------------------------------


@pytest.mark.parametrize("position,expected", [
    # Board appointments
    ("director", "appointmentOfBoard"),
    ("managing director", "appointmentOfBoard"),
    ("non-executive director", "appointmentOfBoard"),
    # Board chair
    ("chairman", "boardChair"),
    ("chair", "boardChair"),
    ("chairperson", "boardChair"),
    # Board members
    ("board member", "boardMember"),
    # Senior managing officials
    ("secretary", "seniorManagingOfficial"),
    ("company secretary", "seniorManagingOfficial"),
    ("chief executive officer", "seniorManagingOfficial"),
    ("ceo", "seniorManagingOfficial"),
    ("treasurer", "seniorManagingOfficial"),
    # Nominees
    ("nominee director", "nominee"),
    ("nominee", "nominee"),
    # Trust roles
    ("trustee", "trustee"),
    ("settlor", "settlor"),
    ("protector", "protector"),
    # Ownership
    ("shareholder", "shareholding"),
    ("owner", "shareholding"),
    # Substring matches
    ("Executive Director (Finance)", "appointmentOfBoard"),
    ("Joint Company Secretary", "seniorManagingOfficial"),
    ("Independent Non-Executive Director", "appointmentOfBoard"),
    # Regex fallbacks
    ("Directeur Général", "appointmentOfBoard"),
    ("Management Chair", "boardChair"),
    # Empty / unknown
    ("", "otherInfluenceOrControl"),
    ("customs agent", "otherInfluenceOrControl"),
])
def test_oc_match_position(position: str, expected: str) -> None:
    assert _oc_match_position(position) == expected, (
        f"position={position!r}: expected {expected!r}, "
        f"got {_oc_match_position(position)!r}"
    )


# ---------------------------------------------------------------------
# Basic bundle mapping
# ---------------------------------------------------------------------


def test_map_opencorporates_produces_entity() -> None:
    bundle = _minimal_bundle()
    statements = list(map_opencorporates(bundle))
    entities = [s for s in statements if s["recordType"] == "entity"]
    assert len(entities) == 1
    assert entities[0]["recordDetails"]["name"] == "Test Co Ltd"


def test_map_opencorporates_entity_identifiers() -> None:
    bundle = _minimal_bundle(ocid="gb/00102498")
    statements = list(map_opencorporates(bundle))
    entity = next(s for s in statements if s["recordType"] == "entity")
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "OpenCorporates" in schemes
    assert "OC-GB" in schemes


def test_map_opencorporates_entity_jurisdiction() -> None:
    bundle = _minimal_bundle(ocid="gb/00102498")
    statements = list(map_opencorporates(bundle))
    entity = next(s for s in statements if s["recordType"] == "entity")
    jur = entity["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "GB"
    assert "United Kingdom" in jur["name"]


def test_map_opencorporates_passes_validator_no_officers() -> None:
    bundle = _minimal_bundle()
    issues = validate_shape(map_opencorporates(bundle))
    assert issues == [], issues


def test_map_opencorporates_empty_company_returns_empty() -> None:
    bundle = {"source_id": "opencorporates", "company": {}, "officers": []}
    assert list(map_opencorporates(bundle)) == []


# ---------------------------------------------------------------------
# Officer mapping
# ---------------------------------------------------------------------


def test_map_opencorporates_director_interest_type() -> None:
    bundle = _minimal_bundle(officers=[_officer("Jane Smith", "Director")])
    statements = list(map_opencorporates(bundle))
    rel = next(s for s in statements if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["type"] == "appointmentOfBoard"
    assert rel["recordDetails"]["interests"][0]["beneficialOwnershipOrControl"] is False


def test_map_opencorporates_secretary_interest_type() -> None:
    bundle = _minimal_bundle(officers=[_officer("Bob Jones", "Company Secretary")])
    statements = list(map_opencorporates(bundle))
    rel = next(s for s in statements if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["type"] == "seniorManagingOfficial"


def test_map_opencorporates_resigned_officer_skipped() -> None:
    bundle = _minimal_bundle(officers=[
        _officer("Jane Smith", "Director", end_date="2023-01-01"),
        _officer("Bob Jones", "Secretary"),
    ])
    statements = list(map_opencorporates(bundle))
    people = [s for s in statements if s["recordType"] == "person"]
    assert len(people) == 1
    assert people[0]["recordDetails"]["names"][0]["fullName"] == "Bob Jones"


def test_map_opencorporates_corporate_officer_emits_entity() -> None:
    """A corporate officer should emit an entity statement, not a person statement."""
    bundle = _minimal_bundle(officers=[
        _officer("Acme Holdings Ltd", "Director", officer_type="Company")
    ])
    statements = list(map_opencorporates(bundle))
    entities = [s for s in statements if s["recordType"] == "entity"]
    # focal company entity + corporate officer entity
    assert len(entities) == 2
    people = [s for s in statements if s["recordType"] == "person"]
    assert len(people) == 0


def test_map_opencorporates_passes_validator_with_officers() -> None:
    bundle = _minimal_bundle(officers=[
        _officer("Jane Smith", "Director"),
        _officer("Bob Jones", "Company Secretary", officer_id="456"),
    ])
    issues = validate_shape(map_opencorporates(bundle))
    assert issues == [], issues


# ---------------------------------------------------------------------
# Network relationship parsing — _oc_parse_network_relationships
# ---------------------------------------------------------------------


def _network_style_a(rel_type: str = "subsidiary") -> dict:
    """Style A: network["relationships"] → list of {"relationship": {...}}"""
    return {
        "relationships": [
            {
                "relationship": {
                    "relationship_type": rel_type,
                    "source": {
                        "company": {
                            "name": "Parent Corp",
                            "jurisdiction_code": "gb",
                            "company_number": "99999999",
                        }
                    },
                    "target": {
                        "company": {
                            "name": "Test Co Ltd",
                            "jurisdiction_code": "gb",
                            "company_number": "00102498",
                        }
                    },
                    "percentage_min_share_ownership": 75.0,
                    "percentage_max_share_ownership": 100.0,
                    "start_date": "2015-06-01",
                    "end_date": None,
                }
            }
        ]
    }


def _network_style_b(rel_type: str = "control_statement") -> dict:
    """Style B: network["network"] → flat list (no 'relationship' wrapper)."""
    return {
        "network": [
            {
                "relationship_type": rel_type,
                "source": {
                    "name": "Parent Corp",
                    "jurisdiction_code": "us",
                    "company_number": "12345",
                },
                "target": {
                    "name": "Sub Inc",
                    "jurisdiction_code": "us",
                    "company_number": "67890",
                },
                "start_date": "2018-01-01",
                "end_date": None,
            }
        ]
    }


def test_parse_network_style_a_subsidiary() -> None:
    parsed = _oc_parse_network_relationships(_network_style_a(), "gb/00102498")
    assert len(parsed) == 1
    rel = parsed[0]
    assert rel["relationship_type"] == "subsidiary"
    assert rel["source"]["company_number"] == "99999999"
    assert rel["target"]["company_number"] == "00102498"
    assert rel["percentage_min_share_ownership"] == 75.0
    assert rel["percentage_max_share_ownership"] == 100.0
    assert rel["start_date"] == "2015-06-01"


def test_parse_network_style_b_flat() -> None:
    parsed = _oc_parse_network_relationships(_network_style_b(), "us/67890")
    assert len(parsed) == 1
    assert parsed[0]["relationship_type"] == "control_statement"
    assert parsed[0]["source"]["company_number"] == "12345"


def test_parse_network_skips_historical_relationships() -> None:
    network = {
        "relationships": [
            {
                "relationship": {
                    "relationship_type": "subsidiary",
                    "source": {"company_number": "AAA", "jurisdiction_code": "gb", "name": "A"},
                    "target": {"company_number": "BBB", "jurisdiction_code": "gb", "name": "B"},
                    "end_date": "2020-01-01",
                }
            },
            {
                "relationship": {
                    "relationship_type": "subsidiary",
                    "source": {"company_number": "CCC", "jurisdiction_code": "gb", "name": "C"},
                    "target": {"company_number": "BBB", "jurisdiction_code": "gb", "name": "B"},
                    "end_date": None,
                }
            },
        ]
    }
    parsed = _oc_parse_network_relationships(network, "gb/BBB")
    assert len(parsed) == 1
    assert parsed[0]["source"]["company_number"] == "CCC"


def test_parse_network_empty_returns_empty() -> None:
    assert _oc_parse_network_relationships({}, "gb/00102498") == []
    assert _oc_parse_network_relationships({"relationships": []}, "gb/00102498") == []


# ---------------------------------------------------------------------
# Network relationships → BODS statements
# ---------------------------------------------------------------------


def test_map_opencorporates_network_subsidiary_emits_relationship() -> None:
    bundle = _minimal_bundle(network=_network_style_a("subsidiary"))
    statements = list(map_opencorporates(bundle))
    rels = [s for s in statements if s["recordType"] == "relationship"]
    assert len(rels) == 1
    interests = rels[0]["recordDetails"]["interests"]
    assert interests[0]["type"] == "shareholding"
    assert interests[0]["beneficialOwnershipOrControl"] is True
    assert interests[0]["share"] == {"minimum": 75.0, "maximum": 100.0}


def test_map_opencorporates_network_emits_two_entities_for_related_companies() -> None:
    """Parent entity + focal entity = 2 entities; no duplicate for focal company."""
    bundle = _minimal_bundle(network=_network_style_a("subsidiary"))
    statements = list(map_opencorporates(bundle))
    entities = [s for s in statements if s["recordType"] == "entity"]
    # focal (Test Co Ltd) + Parent Corp
    assert len(entities) == 2
    names = {e["recordDetails"]["name"] for e in entities}
    assert "Parent Corp" in names
    assert "Test Co Ltd" in names


def test_map_opencorporates_network_control_statement_no_percentages() -> None:
    """control_statement with no percentage data falls back to otherInfluenceOrControl."""
    network = {
        "relationships": [
            {
                "relationship": {
                    "relationship_type": "control_statement",
                    "source": {"company_number": "CTRL01", "jurisdiction_code": "gb", "name": "Controller"},
                    "target": {"company_number": "00102498", "jurisdiction_code": "gb", "name": "Test Co Ltd"},
                    "end_date": None,
                }
            }
        ]
    }
    bundle = _minimal_bundle(network=network)
    statements = list(map_opencorporates(bundle))
    rel = next(s for s in statements if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["type"] == "otherInfluenceOrControl"


def test_map_opencorporates_network_no_duplicate_focal_entity() -> None:
    """If the focal company appears as target in the network, it must not be emitted twice."""
    bundle = _minimal_bundle(network=_network_style_a("subsidiary"))
    statements = list(map_opencorporates(bundle))
    focal_sids = [
        s["statementId"] for s in statements
        if s["recordType"] == "entity"
        and s["recordDetails"]["name"] == "Test Co Ltd"
    ]
    assert len(focal_sids) == 1, f"Focal entity emitted {len(focal_sids)} times"


def test_map_opencorporates_network_passes_validator() -> None:
    bundle = _minimal_bundle(network=_network_style_a("subsidiary"))
    issues = validate_shape(map_opencorporates(bundle))
    assert issues == [], issues


def test_map_opencorporates_network_with_officers_passes_validator() -> None:
    """Officers + network together must produce valid BODS output."""
    bundle = _minimal_bundle(
        officers=[_officer("Jane Smith", "Director")],
        network=_network_style_a("subsidiary"),
    )
    issues = validate_shape(map_opencorporates(bundle))
    assert issues == [], issues


def test_map_opencorporates_network_none_skipped_gracefully() -> None:
    """When network is None (Supplement not available) no crash, just officers."""
    bundle = _minimal_bundle(
        officers=[_officer("Jane Smith", "Director")],
        network=None,
    )
    statements = list(map_opencorporates(bundle))
    types = [s["recordType"] for s in statements]
    assert "entity" in types
    assert "person" in types
    assert "relationship" in types
