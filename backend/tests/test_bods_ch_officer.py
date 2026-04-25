"""Tests for the Companies House officer-appointments BODS mapper (Phase 3)."""

from __future__ import annotations

from opencheck.bods import map_companies_house, validate_shape


def _officer_bundle() -> dict:
    return {
        "source_id": "companies_house",
        "officer_id": "zS_RY9pRYlJ9XwGJEOFtkJgrf8s",
        "appointments": {
            "name": "Jane SMITH",
            "date_of_birth": {"year": 1975, "month": 8},
            "nationality": "British",
            "items": [
                {
                    "appointed_to": {
                        "company_name": "ACME LTD",
                        "company_number": "00102498",
                    },
                    "officer_role": "director",
                    "appointed_on": "2020-01-15",
                },
                {
                    "appointed_to": {
                        "company_name": "WIDGETS PLC",
                        "company_number": "OC403762",
                    },
                    "officer_role": "secretary",
                    "appointed_on": "2018-03-01",
                    "resigned_on": "2022-06-30",
                },
            ],
        },
    }


def test_map_ch_officer_emits_person_plus_companies_plus_relationships() -> None:
    bundle = map_companies_house(_officer_bundle())
    types = [s["recordType"] for s in bundle]
    # 1 person + 2 entities + 2 relationships
    assert types.count("person") == 1
    assert types.count("entity") == 2
    assert types.count("relationship") == 2


def test_map_ch_officer_uses_directors_boardmember_interest() -> None:
    bundle = map_companies_house(_officer_bundle())
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    director_rel = next(
        r for r in rels
        if "from 2020-01-15" in r["recordDetails"]["interests"][0]["details"]
    )
    interest = director_rel["recordDetails"]["interests"][0]
    assert interest["type"] == "boardMember"
    assert interest["startDate"] == "2020-01-15"
    assert "endDate" not in interest


def test_map_ch_officer_secretary_records_end_date_when_resigned() -> None:
    bundle = map_companies_house(_officer_bundle())
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    secretary_rel = next(
        r for r in rels
        if "from 2018-03-01" in r["recordDetails"]["interests"][0]["details"]
    )
    interest = secretary_rel["recordDetails"]["interests"][0]
    assert interest["endDate"] == "2022-06-30"
    # Secretary is not a director — ours falls through to otherInfluence.
    assert interest["type"] == "otherInfluenceOrControl"


def test_map_ch_officer_sets_birth_date_and_nationality_on_person() -> None:
    bundle = map_companies_house(_officer_bundle())
    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["birthDate"] == "1975-08"
    assert person["recordDetails"]["nationalities"] == [{"name": "British"}]


def test_map_ch_officer_carries_officer_id_as_identifier() -> None:
    bundle = map_companies_house(_officer_bundle())
    person = next(s for s in bundle if s["recordType"] == "person")
    schemes = {i["scheme"] for i in person["recordDetails"]["identifiers"]}
    assert "GB-COH-OFFICER" in schemes


def test_map_ch_officer_passes_validator() -> None:
    bundle = map_companies_house(_officer_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_map_ch_officer_handles_no_appointments() -> None:
    """A still-active officer with no historic appointments must not crash."""
    bundle = map_companies_house(
        {
            "officer_id": "abc",
            "appointments": {"name": "John DOE", "items": []},
        }
    )
    statements = list(bundle)
    assert len(statements) == 1
    assert statements[0]["recordType"] == "person"
    assert validate_shape(bundle) == []
