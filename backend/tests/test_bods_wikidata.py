"""Tests for the Wikidata BODS v0.4 mapper (Phase 3)."""

from __future__ import annotations

from opencheck.bods import map_wikidata, validate_shape


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


def _person_bundle() -> dict:
    """A summarised Wikidata bundle for a notional politician."""
    return {
        "source_id": "wikidata",
        "qid": "Q7747",
        "summary": {
            "qid": "Q7747",
            "label": "Vladimir Putin",
            "description": "President of Russia",
            "is_person": True,
            "is_entity": False,
            "instance_of": [{"qid": "Q5", "label": "human"}],
            "citizenships": [
                {"qid": "Q15180", "label": "Soviet Union"},
                {"qid": "Q159", "label": "Russia"},
            ],
            "positions": [
                {
                    "qid": "Q123028",
                    "label": "President of Russia",
                    "start": "2012-05-07T00:00:00Z",
                    "end": None,
                }
            ],
            "identifiers": {},
            "country": None,
            "dob": "1952-10-07T00:00:00Z",
            "dod": None,
            "inception": None,
        },
    }


def _entity_bundle() -> dict:
    return {
        "source_id": "wikidata",
        "qid": "Q152057",
        "summary": {
            "qid": "Q152057",
            "label": "BP p.l.c.",
            "description": "British multinational oil and gas company",
            "is_person": False,
            "is_entity": True,
            "instance_of": [{"qid": "Q891723", "label": "public company"}],
            "citizenships": [],
            "positions": [],
            "identifiers": {
                "lei": "213800LBDB8WB3QGVN21",
                "opencorporates": "gb/00102498",
            },
            "country": {"qid": "Q145", "label": "United Kingdom"},
            "dob": None,
            "dod": None,
            "inception": "1909-04-14T00:00:00Z",
            "parent_orgs": [],
        },
    }


def _entity_with_parent_bundle() -> dict:
    """An entity that declares a parent organisation via P749/P127."""
    return {
        "source_id": "wikidata",
        "qid": "Q61788",
        "summary": {
            "qid": "Q61788",
            "label": "Ericsson AB",
            "description": "Swedish telecommunications company, subsidiary of Telefonaktiebolaget LM Ericsson",
            "is_person": False,
            "is_entity": True,
            "instance_of": [{"qid": "Q891723", "label": "public company"}],
            "citizenships": [],
            "positions": [],
            "identifiers": {
                "lei": "549300MLH00Y3BN4HD49",
            },
            "country": {"qid": "Q34", "label": "Sweden"},
            "dob": None,
            "dod": None,
            "inception": "1876-01-01T00:00:00Z",
            "parent_orgs": [
                {"qid": "Q204119", "label": "Telefonaktiebolaget LM Ericsson"},
            ],
        },
    }


# ---------------------------------------------------------------------
# Person path
# ---------------------------------------------------------------------


def test_map_wikidata_person_emits_person_statement() -> None:
    bundle = map_wikidata(_person_bundle())
    statements = list(bundle)
    assert len(statements) == 1
    person = statements[0]
    assert person["recordType"] == "person"
    assert person["recordDetails"]["names"][0]["fullName"] == "Vladimir Putin"


def test_map_wikidata_person_carries_qid_identifier() -> None:
    bundle = map_wikidata(_person_bundle())
    person = next(iter(bundle))
    schemes = {i["scheme"] for i in person["recordDetails"]["identifiers"]}
    assert "WIKIDATA" in schemes
    qid_id = next(
        i for i in person["recordDetails"]["identifiers"] if i["scheme"] == "WIKIDATA"
    )
    assert qid_id["id"] == "Q7747"
    assert qid_id["uri"] == "https://www.wikidata.org/wiki/Q7747"


def test_map_wikidata_person_normalises_dob() -> None:
    bundle = map_wikidata(_person_bundle())
    person = next(iter(bundle))
    assert person["recordDetails"]["birthDate"] == "1952-10-07"


def test_map_wikidata_person_lists_nationalities() -> None:
    bundle = map_wikidata(_person_bundle())
    person = next(iter(bundle))
    nationality_qids = {n["code"] for n in person["recordDetails"]["nationalities"]}
    assert nationality_qids == {"Q15180", "Q159"}


def test_map_wikidata_person_passes_validator() -> None:
    bundle = map_wikidata(_person_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


# ---------------------------------------------------------------------
# Entity path
# ---------------------------------------------------------------------


def test_map_wikidata_entity_emits_entity_statement() -> None:
    bundle = map_wikidata(_entity_bundle())
    statements = list(bundle)
    assert len(statements) == 1
    entity = statements[0]
    assert entity["recordType"] == "entity"
    assert entity["recordDetails"]["entityType"]["type"] == "registeredEntity"
    assert entity["recordDetails"]["name"] == "BP p.l.c."


def test_map_wikidata_entity_carries_lei_and_opencorporates_bridges() -> None:
    bundle = map_wikidata(_entity_bundle())
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert {"WIKIDATA", "XI-LEI", "OpenCorporates"}.issubset(schemes)


def test_map_wikidata_entity_resolves_jurisdiction_to_iso_code() -> None:
    """Wikidata says 'United Kingdom' — pycountry should yield 'GB'."""
    bundle = map_wikidata(_entity_bundle())
    entity = next(iter(bundle))
    assert entity["recordDetails"]["jurisdiction"] == {
        "name": "United Kingdom",
        "code": "GB",
    }


def test_map_wikidata_entity_normalises_inception_date() -> None:
    bundle = map_wikidata(_entity_bundle())
    entity = next(iter(bundle))
    assert entity["recordDetails"]["foundingDate"] == "1909-04-14"


def test_map_wikidata_entity_passes_validator() -> None:
    bundle = map_wikidata(_entity_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


# ---------------------------------------------------------------------
# P749 / P127 parent organisation relationships
# ---------------------------------------------------------------------


def test_map_wikidata_entity_with_parent_emits_three_statements() -> None:
    """Subject entity + parent stub entity + relationship = 3 statements."""
    bundle = map_wikidata(_entity_with_parent_bundle())
    statements = list(bundle)
    assert len(statements) == 3


def test_map_wikidata_entity_with_parent_has_correct_record_types() -> None:
    bundle = map_wikidata(_entity_with_parent_bundle())
    statements = list(bundle)
    record_types = [s["recordType"] for s in statements]
    assert record_types == ["entity", "entity", "relationship"]


def test_map_wikidata_entity_with_parent_relationship_links_subject_to_parent() -> None:
    bundle = map_wikidata(_entity_with_parent_bundle())
    statements = list(bundle)
    subject_entity = statements[0]
    parent_entity = statements[1]
    rel = statements[2]

    assert rel["recordDetails"]["subject"] == subject_entity["statementId"]
    assert rel["recordDetails"]["interestedParty"] == parent_entity["statementId"]


def test_map_wikidata_entity_with_parent_interest_type() -> None:
    bundle = map_wikidata(_entity_with_parent_bundle())
    rel = list(bundle)[2]
    interests = rel["recordDetails"]["interests"]
    assert len(interests) == 1
    assert interests[0]["type"] == "otherInfluenceOrControl"
    assert interests[0]["beneficialOwnershipOrControl"] is False


def test_map_wikidata_entity_with_parent_stub_carries_wikidata_identifier() -> None:
    bundle = map_wikidata(_entity_with_parent_bundle())
    parent_entity = list(bundle)[1]
    schemes = {i["scheme"] for i in parent_entity["recordDetails"]["identifiers"]}
    assert "WIKIDATA" in schemes
    wd_id = next(
        i for i in parent_entity["recordDetails"]["identifiers"]
        if i["scheme"] == "WIKIDATA"
    )
    assert wd_id["id"] == "Q204119"


def test_map_wikidata_entity_with_parent_passes_validator() -> None:
    bundle = map_wikidata(_entity_with_parent_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_map_wikidata_entity_no_parents_still_emits_one_statement() -> None:
    """Entities without parent_orgs continue to emit exactly one statement."""
    bundle = map_wikidata(_entity_bundle())
    statements = list(bundle)
    assert len(statements) == 1
    assert statements[0]["recordType"] == "entity"


# ---------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------


def test_map_wikidata_unknown_kind_falls_back_to_unknown_entity() -> None:
    """A QID with no P31 still maps — defaults to entityType=unknownEntity."""
    payload = {
        "source_id": "wikidata",
        "qid": "Q99999999",
        "summary": {
            "qid": "Q99999999",
            "label": "Mystery Item",
            "description": None,
            "is_person": False,
            "is_entity": False,
            "instance_of": [],
            "citizenships": [],
            "positions": [],
            "identifiers": {},
            "country": None,
            "dob": None,
            "dod": None,
            "inception": None,
        },
    }
    bundle = map_wikidata(payload)
    entity = next(iter(bundle))
    assert entity["recordDetails"]["entityType"]["type"] == "unknownEntity"
    assert validate_shape(bundle) == []


def test_map_wikidata_empty_bundle_still_emits_a_statement() -> None:
    """Defensive: a bare ``{}`` should not crash, just emit a stub Q0 entity."""
    bundle = map_wikidata({})
    statements = list(bundle)
    assert len(statements) == 1
    assert statements[0]["recordType"] == "entity"


# ---------------------------------------------------------------------
# Roleholders (P169/P488/P3320 … → seniorManagingOfficial)
# ---------------------------------------------------------------------


def _entity_with_roleholders_bundle() -> dict:
    """An entity with two current roleholders — a CEO and a chairperson."""
    return {
        "source_id": "wikidata",
        "qid": "Q157062",
        "summary": {
            "qid": "Q157062",
            "label": "Unilever",
            "description": "British-Dutch multinational consumer goods company",
            "is_person": False,
            "is_entity": True,
            "instance_of": [{"qid": "Q891723", "label": "public company"}],
            "citizenships": [],
            "positions": [],
            "identifiers": {"lei": "549300MKFYEKVRWML317"},
            "country": {"qid": "Q145", "label": "United Kingdom"},
            "dob": None,
            "dod": None,
            "inception": "1929-09-02T00:00:00Z",
            "parent_orgs": [],
            "roleholders": [
                {
                    "qid": "Q111111",
                    "name": "Hein Schumacher",
                    "roles": [
                        {"label": "chief executive officer", "start": "+2023-07-01T00:00:00Z"},
                    ],
                },
                {
                    "qid": "Q222222",
                    "name": "Ian Meakins",
                    "roles": [
                        {"label": "chairperson", "start": "+2023-05-03T00:00:00Z"},
                    ],
                },
            ],
        },
    }


def _entity_with_dual_role_holder_bundle() -> dict:
    """An entity where one person holds two concurrent roles."""
    return {
        "source_id": "wikidata",
        "qid": "Q157062",
        "summary": {
            "qid": "Q157062",
            "label": "Unilever",
            "description": "British-Dutch multinational consumer goods company",
            "is_person": False,
            "is_entity": True,
            "instance_of": [{"qid": "Q891723", "label": "public company"}],
            "citizenships": [],
            "positions": [],
            "identifiers": {},
            "country": None,
            "dob": None,
            "dod": None,
            "inception": None,
            "parent_orgs": [],
            "roleholders": [
                {
                    "qid": "Q333333",
                    "name": "Jane Smith",
                    "roles": [
                        {"label": "chief executive officer", "start": None},
                        {"label": "board member", "start": None},
                    ],
                },
            ],
        },
    }


def test_map_wikidata_roleholders_emit_correct_statement_count() -> None:
    """entity + 2×(person + relationship) = 5 statements."""
    bundle = map_wikidata(_entity_with_roleholders_bundle())
    statements = list(bundle)
    assert len(statements) == 5


def test_map_wikidata_roleholders_record_types() -> None:
    bundle = map_wikidata(_entity_with_roleholders_bundle())
    types = [s["recordType"] for s in bundle]
    assert types == ["entity", "person", "relationship", "person", "relationship"]


def test_map_wikidata_roleholder_interest_type_is_senior_managing_official() -> None:
    bundle = map_wikidata(_entity_with_roleholders_bundle())
    relationships = [s for s in bundle if s["recordType"] == "relationship"]
    for rel in relationships:
        for interest in rel["recordDetails"]["interests"]:
            assert interest["type"] == "seniorManagingOfficial"
            assert interest["beneficialOwnershipOrControl"] is False


def test_map_wikidata_roleholder_details_carries_role_title() -> None:
    bundle = map_wikidata(_entity_with_roleholders_bundle())
    relationships = [s for s in bundle if s["recordType"] == "relationship"]
    details = {i["details"] for rel in relationships for i in rel["recordDetails"]["interests"]}
    assert "chief executive officer" in details
    assert "chairperson" in details


def test_map_wikidata_roleholder_start_date_normalised() -> None:
    bundle = map_wikidata(_entity_with_roleholders_bundle())
    relationships = [s for s in bundle if s["recordType"] == "relationship"]
    ceo_rel = next(
        rel for rel in relationships
        if any(i["details"] == "chief executive officer" for i in rel["recordDetails"]["interests"])
    )
    ceo_interest = next(
        i for i in ceo_rel["recordDetails"]["interests"]
        if i["details"] == "chief executive officer"
    )
    assert ceo_interest.get("startDate") == "2023-07-01"


def test_map_wikidata_dual_role_holder_emits_two_interests_on_one_relationship() -> None:
    """One person holding CEO + board member → one relationship with two interests."""
    bundle = map_wikidata(_entity_with_dual_role_holder_bundle())
    relationships = [s for s in bundle if s["recordType"] == "relationship"]
    assert len(relationships) == 1
    interests = relationships[0]["recordDetails"]["interests"]
    assert len(interests) == 2
    labels = {i["details"] for i in interests}
    assert labels == {"chief executive officer", "board member"}


def test_map_wikidata_roleholders_relationship_links_to_correct_statements() -> None:
    """Each relationship's subject is the entity; interestedParty is the person."""
    bundle = map_wikidata(_entity_with_roleholders_bundle())
    statements = list(bundle)
    entity_id = statements[0]["statementId"]
    relationships = [s for s in statements if s["recordType"] == "relationship"]
    persons = {s["statementId"] for s in statements if s["recordType"] == "person"}
    for rel in relationships:
        assert rel["recordDetails"]["subject"] == entity_id
        assert rel["recordDetails"]["interestedParty"] in persons


def test_map_wikidata_roleholders_passes_validator() -> None:
    bundle = map_wikidata(_entity_with_roleholders_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_map_wikidata_entity_without_roleholders_key_still_emits_one_statement() -> None:
    """Existing bundles lacking the roleholders key are unaffected (backward compat)."""
    bundle = map_wikidata(_entity_bundle())  # _entity_bundle has no roleholders key
    statements = list(bundle)
    assert len(statements) == 1
    assert statements[0]["recordType"] == "entity"
