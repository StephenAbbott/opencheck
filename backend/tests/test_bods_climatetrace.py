"""Tests for the Climate TRACE / GEM BODS v0.4 mapper."""

from __future__ import annotations

from opencheck.bods import map_climatetrace, validate_shape


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _entity_bundle() -> dict:
    """A minimal GEM/Climate TRACE bundle for an energy company."""
    return {
        "source_id": "climatetrace",
        "entity_id": "E100000001096",
        "entity_name": "BP p.l.c.",
        "lei": "213800LH1BZH3DI6G760",
        "gem_row": {
            "Entity ID": "E100000001096",
            "Entity Name": "BP p.l.c.",
            "Global Legal Entity Identifier Index": "213800LH1BZH3DI6G760",
            "Country": "United Kingdom",
            "Gem parents IDs": "",
            "Gem parents": "",
        },
        "emissions": {
            "total_co2e_tonnes": 200_800_000.0,
            "unit": "tonnes CO2e (GWP100)",
            "year": 2024,
            "by_sector": {"oil-and-gas": 200_800_000.0},
        },
        "assets": [],
        "parents": [],
        "is_stub": False,
    }


def _entity_with_parent_bundle() -> dict:
    """A GEM bundle for a subsidiary that declares a parent."""
    return {
        "source_id": "climatetrace",
        "entity_id": "E100000002000",
        "entity_name": "BP Exploration (Alaska) Inc.",
        "lei": "AAAAAAAAAAAAAAAAAA01",
        "gem_row": {
            "Entity ID": "E100000002000",
            "Entity Name": "BP Exploration (Alaska) Inc.",
            "Global Legal Entity Identifier Index": "AAAAAAAAAAAAAAAAAA01",
            "Country": "United States",
            "Gem parents IDs": "E100000001096",
            "Gem parents": "BP p.l.c.",
        },
        "emissions": {
            "total_co2e_tonnes": 5_000_000.0,
            "unit": "tonnes CO2e (GWP100)",
            "year": 2024,
            "by_sector": {"oil-and-gas": 5_000_000.0},
        },
        "assets": [],
        "parents": [{"entity_id": "E100000001096", "name": "BP p.l.c."}],
        "is_stub": False,
    }


def _stub_bundle() -> dict:
    return {
        "source_id": "climatetrace",
        "entity_id": "E100000001096",
        "entity_name": "BP p.l.c.",
        "lei": "213800LH1BZH3DI6G760",
        "gem_row": {},
        "emissions": {},
        "assets": [],
        "parents": [],
        "is_stub": True,
    }


# ---------------------------------------------------------------------------
# Basic entity path
# ---------------------------------------------------------------------------


def test_map_climatetrace_entity_emits_one_statement() -> None:
    bundle = map_climatetrace(_entity_bundle())
    statements = list(bundle)
    assert len(statements) == 1
    assert statements[0]["recordType"] == "entity"


def test_map_climatetrace_entity_name() -> None:
    bundle = map_climatetrace(_entity_bundle())
    entity = next(iter(bundle))
    assert entity["recordDetails"]["name"] == "BP p.l.c."


def test_map_climatetrace_entity_carries_gem_identifier() -> None:
    bundle = map_climatetrace(_entity_bundle())
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "GEM-ENTITY" in schemes
    gem_id = next(
        i for i in entity["recordDetails"]["identifiers"] if i["scheme"] == "GEM-ENTITY"
    )
    assert gem_id["id"] == "E100000001096"


def test_map_climatetrace_entity_carries_lei_identifier() -> None:
    bundle = map_climatetrace(_entity_bundle())
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "XI-LEI" in schemes
    lei_id = next(
        i for i in entity["recordDetails"]["identifiers"] if i["scheme"] == "XI-LEI"
    )
    assert lei_id["id"] == "213800LH1BZH3DI6G760"


def test_map_climatetrace_entity_resolves_jurisdiction() -> None:
    bundle = map_climatetrace(_entity_bundle())
    entity = next(iter(bundle))
    assert entity["recordDetails"]["incorporatedInJurisdiction"]["code"] == "GB"


def test_map_climatetrace_entity_passes_validator() -> None:
    bundle = map_climatetrace(_entity_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


# ---------------------------------------------------------------------------
# Parent organisation path
# ---------------------------------------------------------------------------


def test_map_climatetrace_with_parent_emits_three_statements() -> None:
    """Subject entity + parent stub + relationship = 3 statements."""
    bundle = map_climatetrace(_entity_with_parent_bundle())
    statements = list(bundle)
    assert len(statements) == 3


def test_map_climatetrace_with_parent_record_types() -> None:
    bundle = map_climatetrace(_entity_with_parent_bundle())
    statements = list(bundle)
    record_types = [s["recordType"] for s in statements]
    assert record_types == ["entity", "entity", "relationship"]


def test_map_climatetrace_with_parent_relationship_links_correctly() -> None:
    bundle = map_climatetrace(_entity_with_parent_bundle())
    statements = list(bundle)
    subject_entity = statements[0]
    parent_entity = statements[1]
    rel = statements[2]

    assert rel["recordDetails"]["subject"] == subject_entity["statementId"]
    assert rel["recordDetails"]["interestedParty"] == parent_entity["statementId"]


def test_map_climatetrace_with_parent_interest_type() -> None:
    bundle = map_climatetrace(_entity_with_parent_bundle())
    rel = list(bundle)[2]
    interests = rel["recordDetails"]["interests"]
    assert len(interests) == 1
    assert interests[0]["type"] == "otherInfluenceOrControl"
    assert interests[0]["beneficialOwnershipOrControl"] is False


def test_map_climatetrace_parent_stub_carries_gem_identifier() -> None:
    bundle = map_climatetrace(_entity_with_parent_bundle())
    parent_entity = list(bundle)[1]
    schemes = {i["scheme"] for i in parent_entity["recordDetails"]["identifiers"]}
    assert "GEM-ENTITY" in schemes
    gem_id = next(
        i for i in parent_entity["recordDetails"]["identifiers"]
        if i["scheme"] == "GEM-ENTITY"
    )
    assert gem_id["id"] == "E100000001096"


def test_map_climatetrace_with_parent_passes_validator() -> None:
    bundle = map_climatetrace(_entity_with_parent_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


# ---------------------------------------------------------------------------
# Stub / edge cases
# ---------------------------------------------------------------------------


def test_map_climatetrace_stub_bundle_returns_empty() -> None:
    """A stub bundle should yield no statements."""
    bundle = map_climatetrace(_stub_bundle())
    assert list(bundle) == []


def test_map_climatetrace_empty_bundle_returns_empty() -> None:
    bundle = map_climatetrace({})
    assert list(bundle) == []


def test_map_climatetrace_no_lei_omits_lei_identifier() -> None:
    b = _entity_bundle()
    b["lei"] = ""
    b["gem_row"]["Global Legal Entity Identifier Index"] = ""
    bundle = map_climatetrace(b)
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "XI-LEI" not in schemes
    assert "GEM-ENTITY" in schemes


def test_map_climatetrace_no_country_omits_jurisdiction() -> None:
    b = _entity_bundle()
    b["gem_row"]["Country"] = ""
    bundle = map_climatetrace(b)
    entity = next(iter(bundle))
    assert "incorporatedInJurisdiction" not in entity["recordDetails"]


def test_map_climatetrace_source_is_third_party() -> None:
    """GEM/Climate TRACE is not an official register."""
    bundle = map_climatetrace(_entity_bundle())
    entity = next(iter(bundle))
    assert "thirdParty" in entity["source"]["type"]
