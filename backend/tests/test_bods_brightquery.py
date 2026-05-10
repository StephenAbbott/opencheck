"""Tests for the BrightQuery → BODS v0.4 mapper."""

from __future__ import annotations

import pytest

from opencheck.bods import validate_shape
from opencheck.bods.mapper import map_brightquery

# ---------------------------------------------------------------------------
# Sample data fixtures (based on actual BrightQuery Senzing format)
# ---------------------------------------------------------------------------

_COMPANY_DATABRICKS = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "100002416308",
    "bq_dataset": "COMPANY",
    "FEATURES": [
        {"NAME_ORG": "DATABRICKS, INC.", "NAME_TYPE": "PRIMARY"},
        {"RECORD_TYPE": "ORGANIZATION"},
        {
            "ADDR_CITY": "San Francisco",
            "ADDR_COUNTRY": "USA",
            "ADDR_LINE1": "160 Spear St Fl 13",
            "ADDR_POSTAL_CODE": "94105",
            "ADDR_STATE": "CA",
            "ADDR_TYPE": "BUSINESS",
        },
        {"REL_ANCHOR_DOMAIN": "BQ", "REL_ANCHOR_KEY": 100002416308},
        {"WEBSITE_ADDRESS": "https://www.databricks.com"},
        {"OTHER_ID_NUMBER": "1587468", "OTHER_ID_TYPE": "CIK"},
        {"OTHER_ID_NUMBER": "5040256649", "OTHER_ID_TYPE": "PERMID"},
        {"OTHER_ID_NUMBER": "JJZCW7PZP8Q4", "OTHER_ID_TYPE": "SAM_UEI"},
        {"OTHER_ID_NUMBER": "7NYJ8", "OTHER_ID_TYPE": "SAM_CAGE"},
        {"OTHER_ID_NUMBER": "247369843", "OTHER_ID_TYPE": "CAPIQ"},
        {"BQ_ID": "100002416308"},
    ],
}

_COMPANY_WITH_LEI = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "100012761940",
    "bq_dataset": "COMPANY",
    "FEATURES": [
        {"NAME_ORG": "AERO SPACE CONTROLS CORPORATION"},
        {"RECORD_TYPE": "ORGANIZATION"},
        {
            "ADDR_CITY": "Billerica",
            "ADDR_COUNTRY": "USA",
            "ADDR_LINE1": "123 Industrial Way",
            "ADDR_STATE": "MA",
            "ADDR_TYPE": "BUSINESS",
        },
        {"OTHER_ID_NUMBER": "5493001KJTIIGC8Y1R12", "OTHER_ID_TYPE": "LEI"},
        {"OTHER_ID_NUMBER": "LBL1NMG1JM58", "OTHER_ID_TYPE": "SAM_UEI"},
        {"OTHER_ID_NUMBER": "29289", "OTHER_ID_TYPE": "SAM_CAGE"},
        {"OTHER_ID_NUMBER": "411997883", "OTHER_ID_TYPE": "CAPIQ"},
        {"BQ_ID": "100012761940"},
    ],
}

_PERSON_ALI_GHODSI = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "8902115872",
    "bq_dataset": "PEOPLE_BUSINESS",
    "FEATURES": [
        {"NAME_FULL": "ALI GHODSI"},
        {"NAME_FIRST": "ALI", "NAME_LAST": "GHODSI"},
        {"RECORD_TYPE": "PERSON"},
        {"ADDR_COUNTRY": "USA", "ADDR_STATE": "CA"},
        {"GROUP_ASSN_ID_NUMBER": "100002416308", "GROUP_ASSN_ID_TYPE": "BQ_ID"},
        {"REL_POINTER_DOMAIN": "BQ", "REL_POINTER_KEY": 100002416308, "REL_POINTER_ROLE": "Executive"},
        {"LINKEDIN": "https://www.linkedin.com/in/alighodsi"},
    ],
}

_PERSON_NO_NAME = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "0000000001",
    "bq_dataset": "PEOPLE_BUSINESS",
    "FEATURES": [
        {"RECORD_TYPE": "PERSON"},
        {"GROUP_ASSN_ID_NUMBER": "100002416308", "GROUP_ASSN_ID_TYPE": "BQ_ID"},
        {"REL_POINTER_KEY": 100002416308, "REL_POINTER_ROLE": "Director"},
    ],
}


def _bundle(
    company: dict | None = None,
    people: list | None = None,
    lei: str = "2549003GGLG529SNTL29",
    bq_id: str = "100002416308",
    name: str = "",
) -> dict:
    return {
        "source_id": "brightquery",
        "hit_id": lei,
        "is_stub": False,
        "lei": lei,
        "bq_id": bq_id,
        "name": name,
        "company": company or _COMPANY_DATABRICKS,
        "people": people if people is not None else [],
    }


# ---------------------------------------------------------------------------
# Entity statement
# ---------------------------------------------------------------------------


def test_map_brightquery_produces_entity() -> None:
    stmts = list(map_brightquery(_bundle()))
    entities = [s for s in stmts if s["recordType"] == "entity"]
    assert len(entities) == 1


def test_map_brightquery_entity_name() -> None:
    stmts = list(map_brightquery(_bundle()))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    assert entity["recordDetails"]["name"] == "DATABRICKS, INC."


def test_map_brightquery_entity_jurisdiction_us() -> None:
    stmts = list(map_brightquery(_bundle()))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    jur = entity["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "US"
    assert "United States" in jur["name"]


def test_map_brightquery_entity_identifiers_include_lei() -> None:
    lei = "2549003GGLG529SNTL29"
    stmts = list(map_brightquery(_bundle(lei=lei)))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "XI-LEI" in schemes
    assert "BRIGHTQUERY" in schemes


def test_map_brightquery_entity_identifiers_cik() -> None:
    stmts = list(map_brightquery(_bundle()))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "US-SEC" in schemes  # CIK


def test_map_brightquery_entity_identifiers_sam_uei() -> None:
    stmts = list(map_brightquery(_bundle()))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    id_map = {i["scheme"]: i["id"] for i in entity["recordDetails"]["identifiers"]}
    assert id_map.get("US-SAM-UEI") == "JJZCW7PZP8Q4"


def test_map_brightquery_entity_address_registered() -> None:
    stmts = list(map_brightquery(_bundle()))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    addrs = entity["recordDetails"].get("addresses", [])
    assert len(addrs) == 1
    assert "160 Spear St Fl 13" in addrs[0]["address"]
    assert "San Francisco" in addrs[0]["address"]


def test_map_brightquery_address_country_normalised() -> None:
    """'USA' should be normalised to 'US' in the BODS country field."""
    stmts = list(map_brightquery(_bundle()))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    addrs = entity["recordDetails"].get("addresses", [])
    assert addrs[0]["country"] == {"name": "United States", "code": "US"}


def test_map_brightquery_empty_company_returns_empty() -> None:
    bundle = {"source_id": "brightquery", "company": {}, "people": []}
    assert list(map_brightquery(bundle)) == []


# ---------------------------------------------------------------------------
# Person and relationship statements
# ---------------------------------------------------------------------------


def test_map_brightquery_person_emitted() -> None:
    stmts = list(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI])))
    people = [s for s in stmts if s["recordType"] == "person"]
    assert len(people) == 1


def test_map_brightquery_person_name_title_cased() -> None:
    stmts = list(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI])))
    person = next(s for s in stmts if s["recordType"] == "person")
    assert person["recordDetails"]["names"][0]["fullName"] == "Ali Ghodsi"


def test_map_brightquery_relationship_emitted() -> None:
    stmts = list(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI])))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    assert len(rels) == 1


def test_map_brightquery_relationship_interest_type() -> None:
    stmts = list(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI])))
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    interests = rel["recordDetails"]["interests"]
    assert interests[0]["type"] == "otherInfluenceOrControl"
    assert interests[0]["beneficialOwnershipOrControl"] is False


def test_map_brightquery_relationship_role_in_details() -> None:
    stmts = list(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI])))
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0].get("details") == "Executive"


def test_map_brightquery_relationship_links_entity_to_person() -> None:
    stmts = list(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI])))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    person = next(s for s in stmts if s["recordType"] == "person")
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["subject"] == entity["statementId"]
    assert rel["recordDetails"]["interestedParty"] == person["statementId"]


def test_map_brightquery_nameless_person_skipped() -> None:
    """A people record with no name should be silently skipped."""
    stmts = list(map_brightquery(_bundle(people=[_PERSON_NO_NAME])))
    people = [s for s in stmts if s["recordType"] == "person"]
    assert len(people) == 0


def test_map_brightquery_multiple_people() -> None:
    second_person = {
        "DATA_SOURCE": "BRIGHTQUERY",
        "RECORD_ID": "8902115873",
        "bq_dataset": "PEOPLE_BUSINESS",
        "FEATURES": [
            {"NAME_FULL": "JANE DOE"},
            {"RECORD_TYPE": "PERSON"},
            {"REL_POINTER_KEY": 100002416308, "REL_POINTER_ROLE": "Director"},
        ],
    }
    stmts = list(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI, second_person])))
    people = [s for s in stmts if s["recordType"] == "person"]
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    assert len(people) == 2
    assert len(rels) == 2


# ---------------------------------------------------------------------------
# BODS validator compliance
# ---------------------------------------------------------------------------


def test_map_brightquery_entity_only_passes_validator() -> None:
    issues = validate_shape(map_brightquery(_bundle()))
    assert issues == [], issues


def test_map_brightquery_with_person_passes_validator() -> None:
    issues = validate_shape(map_brightquery(_bundle(people=[_PERSON_ALI_GHODSI])))
    assert issues == [], issues


def test_map_brightquery_company_with_lei_passes_validator() -> None:
    bundle = _bundle(
        company=_COMPANY_WITH_LEI,
        lei="5493001KJTIIGC8Y1R12",
        bq_id="100012761940",
    )
    issues = validate_shape(map_brightquery(bundle))
    assert issues == [], issues
