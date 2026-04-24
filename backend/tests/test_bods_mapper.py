"""Tests for the Companies House → BODS v0.4 mapper."""

from __future__ import annotations

from opencheck.bods import (
    map_companies_house,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    validate_shape,
)


# ---------------------------------------------------------------------
# Statement factories
# ---------------------------------------------------------------------


def test_entity_statement_shape() -> None:
    s = make_entity_statement(
        source_id="companies_house",
        local_id="00102498",
        name="BP P.L.C.",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[
            {"id": "00102498", "scheme": "GB-COH", "schemeName": "Companies House"}
        ],
    )
    assert s["recordType"] == "entity"
    assert s["recordStatus"] == "new"
    assert s["recordDetails"]["entityType"]["type"] == "registeredEntity"
    assert s["recordDetails"]["incorporatedInJurisdiction"] == {
        "name": "United Kingdom",
        "code": "GB",
    }
    assert s["recordDetails"]["name"] == "BP P.L.C."
    assert s["statementId"].startswith("opencheck-")
    assert s["recordId"].startswith("opencheck-")
    assert s["statementId"] != s["recordId"]


def test_person_statement_shape() -> None:
    s = make_person_statement(
        source_id="companies_house",
        local_id="jane-smith",
        full_name="Jane Smith",
        nationalities=[{"name": "British"}],
        birth_date="1975-08",
    )
    assert s["recordType"] == "person"
    assert s["recordDetails"]["personType"] == "knownPerson"
    assert s["recordDetails"]["names"][0]["fullName"] == "Jane Smith"
    assert s["recordDetails"]["birthDate"] == "1975-08"


def test_stable_ids_are_deterministic() -> None:
    a = make_entity_statement(
        source_id="companies_house", local_id="00102498", name="BP P.L.C."
    )
    b = make_entity_statement(
        source_id="companies_house", local_id="00102498", name="BP P.L.C."
    )
    assert a["statementId"] == b["statementId"]
    assert a["recordId"] == b["recordId"]


def test_relationship_statement_shape() -> None:
    entity = make_entity_statement(
        source_id="companies_house", local_id="00102498", name="BP"
    )
    person = make_person_statement(
        source_id="companies_house", local_id="p1", full_name="Jane Smith"
    )
    rel = make_relationship_statement(
        source_id="companies_house",
        local_id="00102498:p1",
        subject_statement_id=entity["statementId"],
        interested_party_statement_id=person["statementId"],
        interests=[
            {
                "type": "shareholding",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": True,
                "share": {"minimum": 50, "maximum": 75, "exclusiveMinimum": True},
            }
        ],
    )
    rd = rel["recordDetails"]
    assert rd["subject"]["describedByEntityStatement"] == entity["statementId"]
    assert rd["interestedParty"]["describedByPersonStatement"] == person["statementId"]
    assert rd["interests"][0]["type"] == "shareholding"


# ---------------------------------------------------------------------
# Companies House bundle mapping
# ---------------------------------------------------------------------


def _sample_bundle() -> dict:
    return {
        "company_number": "00102498",
        "profile": {
            "company_number": "00102498",
            "company_name": "BP P.L.C.",
            "date_of_creation": "1909-04-14",
            "registered_office_address": {
                "address_line_1": "1 St James's Square",
                "locality": "London",
                "postal_code": "SW1Y 4PD",
                "country": "England",
            },
        },
        "officers": {"items": []},
        "pscs": {
            "items": [
                {
                    "kind": "individual-person-with-significant-control",
                    "name": "Jane SMITH",
                    "name_elements": {"forename": "Jane", "surname": "Smith"},
                    "date_of_birth": {"year": 1975, "month": 8},
                    "nationality": "British",
                    "etag": "abc123",
                    "natures_of_control": [
                        "ownership-of-shares-50-to-75-percent",
                        "voting-rights-50-to-75-percent",
                    ],
                    "address": {
                        "address_line_1": "10 Downing Street",
                        "locality": "London",
                        "country": "United Kingdom",
                    },
                },
                {
                    "kind": "corporate-entity-person-with-significant-control",
                    "name": "Acme Holdings Ltd",
                    "etag": "def456",
                    "identification": {
                        "registration_number": "12345678",
                        "country_registered": "United Kingdom",
                        "place_registered": "Companies House",
                    },
                    "natures_of_control": [
                        "ownership-of-shares-75-to-100-percent"
                    ],
                },
            ]
        },
    }


def test_map_companies_house_produces_entity_person_relationship() -> None:
    bundle = map_companies_house(_sample_bundle())
    statements = list(bundle)

    types = [s["recordType"] for s in statements]
    assert types.count("entity") == 2  # subject + corporate PSC
    assert types.count("person") == 1  # individual PSC
    assert types.count("relationship") == 2  # one per PSC


def test_individual_psc_shareholding_interest() -> None:
    bundle = map_companies_house(_sample_bundle())
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    # Find the relationship pointing at the individual Jane Smith.
    people = {
        s["statementId"]
        for s in bundle
        if s["recordType"] == "person"
    }
    jane_rel = next(
        r
        for r in rels
        if r["recordDetails"]["interestedParty"].get("describedByPersonStatement")
        in people
    )

    interest_types = [i["type"] for i in jane_rel["recordDetails"]["interests"]]
    assert "shareholding" in interest_types
    assert "votingRights" in interest_types

    sh = next(
        i for i in jane_rel["recordDetails"]["interests"] if i["type"] == "shareholding"
    )
    assert sh["share"] == {"minimum": 50, "maximum": 75, "exclusiveMinimum": True}


def test_mapper_output_passes_shape_validator() -> None:
    bundle = map_companies_house(_sample_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_ceased_pscs_are_skipped() -> None:
    payload = _sample_bundle()
    payload["pscs"]["items"][0]["ceased_on"] = "2024-01-01"
    bundle = map_companies_house(payload)
    # One active PSC left (the corporate one) + the subject entity + one relationship.
    types = [s["recordType"] for s in bundle]
    assert types.count("relationship") == 1
