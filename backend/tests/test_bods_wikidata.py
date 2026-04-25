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
    assert {"WIKIDATA", "XI-LEI", "OPENCORPORATES"}.issubset(schemes)


def test_map_wikidata_entity_resolves_jurisdiction_to_iso_code() -> None:
    """Wikidata says 'United Kingdom' — pycountry should yield 'GB'."""
    bundle = map_wikidata(_entity_bundle())
    entity = next(iter(bundle))
    assert entity["recordDetails"]["incorporatedInJurisdiction"] == {
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
