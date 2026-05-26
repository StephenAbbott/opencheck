"""Tests for Companies House director officer → BODS mapping.

Covers the company-bundle path (_emit_company_statements) — distinct from
test_bods_ch_officer.py which covers the officer-appointments bundle path
(_map_companies_house_officer).
"""

from __future__ import annotations

from opencheck.bods import map_companies_house, validate_shape


def _company_bundle_with_directors(
    *,
    include_psc: bool = False,
    resigned_director: bool = False,
) -> dict:
    """Build a minimal company bundle that includes director officer items."""
    officers_items = [
        {
            "name": "SMITH, Jane",
            "officer_role": "director",
            "appointed_on": "2018-06-01",
            "date_of_birth": {"year": 1975, "month": 3},
            "nationality": "British",
            "address": {
                "premises": "1",
                "address_line_1": "High Street",
                "locality": "London",
                "postal_code": "EC1A 1AA",
                "country": "England",
            },
            "links": {
                "officer": {"appointments": "/officers/abc123/appointments"}
            },
        },
        {
            "name": "DOE, John",
            "officer_role": "secretary",
            "appointed_on": "2019-01-10",
        },
    ]
    if resigned_director:
        officers_items.append(
            {
                "name": "FORMER, Director",
                "officer_role": "director",
                "appointed_on": "2010-01-01",
                "resigned_on": "2015-12-31",
            }
        )

    pscs_items = []
    if include_psc:
        pscs_items.append(
            {
                "name": "JONES, Alice",
                "kind": "individual-person-with-significant-control",
                "natures_of_control": ["ownership-of-shares-25-to-50-percent"],
                "notified_on": "2016-04-06",
                "date_of_birth": {"year": 1980, "month": 7},
                "nationality": "British",
            }
        )

    return {
        "source_id": "companies_house",
        "company_number": "00102498",
        "profile": {"company_name": "DEMO HOLDINGS PLC"},
        "officers": {"items": officers_items},
        "pscs": {"items": pscs_items},
        "related_companies": {},
    }


# ---------------------------------------------------------------------------
# Director statement counts
# ---------------------------------------------------------------------------


def test_active_director_emits_person_and_relationship() -> None:
    """One active director produces one person + one relationship."""
    bundle = map_companies_house(_company_bundle_with_directors())
    types = [s["recordType"] for s in bundle]
    # entity (company) + person (director) + relationship (director→company)
    assert types.count("entity") == 1
    assert types.count("person") == 1
    assert types.count("relationship") == 1


def test_secretary_not_mapped_as_director() -> None:
    """Secretary role is excluded from director mapping."""
    bundle = map_companies_house(_company_bundle_with_directors())
    persons = [s for s in bundle if s["recordType"] == "person"]
    assert len(persons) == 1
    assert "SMITH" in persons[0]["recordDetails"]["names"][0]["fullName"]


def test_resigned_director_is_excluded() -> None:
    """A director who has resigned_on should not appear."""
    bundle = map_companies_house(
        _company_bundle_with_directors(resigned_director=True)
    )
    persons = [s for s in bundle if s["recordType"] == "person"]
    # Only the active director (SMITH), not the former one (FORMER)
    assert len(persons) == 1
    assert "FORMER" not in persons[0]["recordDetails"]["names"][0]["fullName"]


def test_director_and_psc_both_emitted() -> None:
    """Director + PSC can coexist; both person types are emitted."""
    bundle = map_companies_house(_company_bundle_with_directors(include_psc=True))
    types = [s["recordType"] for s in bundle]
    # entity + director person + psc person + director rel + psc rel
    assert types.count("entity") == 1
    assert types.count("person") == 2
    assert types.count("relationship") == 2


# ---------------------------------------------------------------------------
# Interest shape
# ---------------------------------------------------------------------------


def test_director_interest_type_is_senior_managing_official() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    assert len(rels) == 1
    interest = rels[0]["recordDetails"]["interests"][0]
    assert interest["type"] == "seniorManagingOfficial"


def test_director_interest_beneficial_ownership_is_false() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    interest = rels[0]["recordDetails"]["interests"][0]
    assert interest["beneficialOwnershipOrControl"] is False


def test_director_interest_records_start_date() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    interest = rels[0]["recordDetails"]["interests"][0]
    assert interest["startDate"] == "2018-06-01"


def test_director_interest_has_no_end_date_when_active() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    interest = rels[0]["recordDetails"]["interests"][0]
    assert "endDate" not in interest


# ---------------------------------------------------------------------------
# Person statement fields
# ---------------------------------------------------------------------------


def test_director_person_has_birth_date_and_nationality() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["birthDate"] == "1975-03"
    assert person["recordDetails"]["nationalities"] == [{"name": "British"}]


def test_director_person_name_matches_officer() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["names"][0]["fullName"] == "SMITH, Jane"


def test_director_person_type_is_known_person() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["personType"] == "knownPerson"


# ---------------------------------------------------------------------------
# Relationship direction
# ---------------------------------------------------------------------------


def test_director_relationship_subject_is_company() -> None:
    """The entity (company) is the *subject*; the person is the *interestedParty*."""
    bundle = map_companies_house(_company_bundle_with_directors())
    entity = next(s for s in bundle if s["recordType"] == "entity")
    person = next(s for s in bundle if s["recordType"] == "person")
    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert rel["recordDetails"]["subject"] == entity["statementId"]
    assert rel["recordDetails"]["interestedParty"] == person["statementId"]


# ---------------------------------------------------------------------------
# Local id stability (officer id from links)
# ---------------------------------------------------------------------------


def test_director_local_id_uses_officer_id_from_links() -> None:
    """When links.officer.appointments is present the officer id is embedded."""
    from opencheck.bods.mapper import _ch_officer_local_id

    officer = {
        "name": "SMITH, Jane",
        "appointed_on": "2018-06-01",
        "links": {"officer": {"appointments": "/officers/abc123/appointments"}},
    }
    local_id = _ch_officer_local_id("00102498", officer)
    assert local_id == "00102498:director:abc123"


def test_director_local_id_falls_back_to_hash_without_links() -> None:
    """Without links, the local id is a deterministic hash."""
    from opencheck.bods.mapper import _ch_officer_local_id

    officer = {"name": "SMITH, Jane", "appointed_on": "2018-06-01"}
    local_id = _ch_officer_local_id("00102498", officer)
    assert local_id.startswith("00102498:director:")
    # Should be stable across calls
    assert local_id == _ch_officer_local_id("00102498", officer)


# ---------------------------------------------------------------------------
# BODS schema validation
# ---------------------------------------------------------------------------


def test_director_bundle_passes_bods_validator() -> None:
    bundle = map_companies_house(_company_bundle_with_directors())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_director_and_psc_bundle_passes_bods_validator() -> None:
    bundle = map_companies_house(_company_bundle_with_directors(include_psc=True))
    issues = validate_shape(bundle)
    assert issues == [], issues


# ---------------------------------------------------------------------------
# No-officers edge case
# ---------------------------------------------------------------------------


def test_empty_officers_produces_no_extra_statements() -> None:
    """A company with no officers should just emit the entity statement."""
    bundle_data = {
        "source_id": "companies_house",
        "company_number": "00102498",
        "profile": {"company_name": "EMPTY CORP LTD"},
        "officers": {"items": []},
        "pscs": {"items": []},
        "related_companies": {},
    }
    bundle = map_companies_house(bundle_data)
    types = [s["recordType"] for s in bundle]
    assert types == ["entity"]


def test_missing_officers_key_produces_no_extra_statements() -> None:
    """A bundle without an 'officers' key should not crash."""
    bundle_data = {
        "source_id": "companies_house",
        "company_number": "00102498",
        "profile": {"company_name": "NO OFFICERS LTD"},
        "pscs": {"items": []},
        "related_companies": {},
    }
    bundle = map_companies_house(bundle_data)
    types = [s["recordType"] for s in bundle]
    assert types == ["entity"]
