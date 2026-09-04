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
            "Full Name": "BP p.l.c.",
            "Global Legal Entity Identifier Index": "213800LH1BZH3DI6G760",
            "Headquarters Country": "GBR",
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
            "Full Name": "BP Exploration (Alaska) Inc.",
            "Global Legal Entity Identifier Index": "AAAAAAAAAAAAAAAAAA01",
            "Headquarters Country": "USA",
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
    assert entity["recordDetails"]["jurisdiction"]["code"] == "GB"


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
    b["gem_row"]["Headquarters Country"] = ""
    bundle = map_climatetrace(b)
    entity = next(iter(bundle))
    assert "jurisdiction" not in entity["recordDetails"]


def test_map_climatetrace_source_is_third_party() -> None:
    """GEM/Climate TRACE is not an official register."""
    bundle = map_climatetrace(_entity_bundle())
    entity = next(iter(bundle))
    assert "thirdParty" in entity["source"]["type"]


# ---------------------------------------------------------------------------
# Entity types from GEM's Entity Type column (Phase 142)
# ---------------------------------------------------------------------------


def _typed_bundle(entity_type: str, *, lei: str = "213800LH1BZH3DI6G760") -> dict:
    b = _entity_bundle()
    b["lei"] = lei
    b["gem_row"]["Entity Type"] = entity_type
    if not lei:
        b["gem_row"]["Global Legal Entity Identifier Index"] = ""
    return b


def test_map_climatetrace_state_entity_type() -> None:
    entity = next(iter(map_climatetrace(_typed_bundle("state"))))
    assert entity["recordDetails"]["entityType"]["type"] == "state"


def test_map_climatetrace_state_body_entity_type() -> None:
    entity = next(iter(map_climatetrace(_typed_bundle("state body"))))
    assert entity["recordDetails"]["entityType"]["type"] == "stateBody"


def test_map_climatetrace_arrangement_entity_type() -> None:
    """GEM 'arrangement' maps honestly — accepted 2026-08-28 as a legitimate
    TRUST_OR_ARRANGEMENT trigger even though climatetrace is ESG-category."""
    entity = next(iter(map_climatetrace(_typed_bundle("arrangement"))))
    assert entity["recordDetails"]["entityType"]["type"] == "arrangement"


def test_map_climatetrace_arrangement_fires_trust_signal() -> None:
    """Pin the accepted decision: an arrangement-typed GEM entity trips the
    risk engine's TRUST_OR_ARRANGEMENT detection."""
    from opencheck.risk import _trust_or_arrangement_signal

    statements = list(map_climatetrace(_typed_bundle("arrangement")))
    signal = _trust_or_arrangement_signal("climatetrace", "E100000001096", statements)
    assert signal is not None
    assert signal.evidence["matches"][0]["match"] == "entityType=arrangement"


def test_map_climatetrace_unknown_entity_type() -> None:
    entity = next(iter(map_climatetrace(_typed_bundle("unknown entity"))))
    assert entity["recordDetails"]["entityType"]["type"] == "unknownEntity"


def test_map_climatetrace_legal_entity_with_lei_is_registered() -> None:
    entity = next(iter(map_climatetrace(_typed_bundle("legal entity"))))
    assert entity["recordDetails"]["entityType"]["type"] == "registeredEntity"


def test_map_climatetrace_legal_entity_without_lei_is_legal_entity() -> None:
    entity = next(iter(map_climatetrace(_typed_bundle("legal entity", lei=""))))
    assert entity["recordDetails"]["entityType"]["type"] == "legalEntity"


def test_map_climatetrace_missing_entity_type_keeps_old_behaviour() -> None:
    """July-2026-and-earlier CSVs: no Entity Type column, LEI present →
    registeredEntity, exactly as before Phase 142."""
    entity = next(iter(map_climatetrace(_entity_bundle())))
    assert entity["recordDetails"]["entityType"]["type"] == "registeredEntity"


def test_map_climatetrace_person_row_emits_nothing() -> None:
    """GEM types 2 records as natural persons — an entityStatement would
    misdescribe them, so the mapper emits no statements."""
    assert list(map_climatetrace(_typed_bundle("person"))) == []


# ---------------------------------------------------------------------------
# Entity status: joint venture, dissolved, amalgamated (Phase 142)
# ---------------------------------------------------------------------------


def test_map_climatetrace_jv_in_entity_type_details() -> None:
    b = _entity_bundle()
    b["entity_status"] = {"jv": True}
    entity = next(iter(map_climatetrace(b)))
    assert entity["recordDetails"]["entityType"]["details"] == (
        "Joint venture (per Global Energy Monitor)"
    )


def test_map_climatetrace_dissolved_annotation_no_dissolution_date() -> None:
    b = _entity_bundle()
    b["entity_status"] = {
        "status": "dissolved",
        "urls": ["https://example.org/strike-off"],
    }
    entity = next(iter(map_climatetrace(b)))
    # No date is published, so no dissolutionDate may be asserted.
    assert "dissolutionDate" not in entity["recordDetails"]
    notes = entity.get("annotations") or []
    assert len(notes) == 1
    assert notes[0]["motivation"] == "commenting"
    assert notes[0]["statementPointerTarget"] == "/recordDetails"
    assert "dissolved" in notes[0]["description"]
    assert "https://example.org/strike-off" in notes[0]["description"]


def _amalgamated_bundle() -> dict:
    b = _entity_bundle()
    b["entity_id"] = "E100001013982"
    b["entity_name"] = "3Bear Energy LLC"
    b["gem_row"]["Entity ID"] = "E100001013982"
    b["entity_status"] = {
        "status": "amalgamated",
        "merged_into": "E100001014363",
        "merged_into_name": "Delek Logistics Partners LP",
        "merged_into_lei": "549300UVYITDIU51P724",
        "urls": ["https://example.org/acquisition"],
    }
    return b


def test_map_climatetrace_amalgamated_annotation_names_successor() -> None:
    statements = list(map_climatetrace(_amalgamated_bundle()))
    subject = statements[0]
    notes = subject.get("annotations") or []
    assert len(notes) == 1
    assert "amalgamated into Delek Logistics Partners LP" in notes[0]["description"]
    assert "E100001014363" in notes[0]["description"]


def test_map_climatetrace_amalgamated_emits_successor_stub() -> None:
    statements = list(map_climatetrace(_amalgamated_bundle()))
    assert len(statements) == 2
    successor = statements[1]
    assert successor["recordType"] == "entity"
    assert successor["recordDetails"]["name"] == "Delek Logistics Partners LP"
    ids = {i["scheme"]: i["id"] for i in successor["recordDetails"]["identifiers"]}
    assert ids["GEM-ENTITY"] == "E100001014363"
    assert ids["XI-LEI"] == "549300UVYITDIU51P724"
    assert successor["recordDetails"]["entityType"]["type"] == "registeredEntity"


def test_map_climatetrace_amalgamated_no_relationship_statement() -> None:
    """A merger is not an ownership or control interest — no relationship
    statement may link the dissolved entity to its successor."""
    statements = list(map_climatetrace(_amalgamated_bundle()))
    assert all(s["recordType"] == "entity" for s in statements)


def test_map_climatetrace_successor_without_lei_is_unknown_entity() -> None:
    b = _amalgamated_bundle()
    del b["entity_status"]["merged_into_lei"]
    statements = list(map_climatetrace(b))
    successor = statements[1]
    assert successor["recordDetails"]["entityType"]["type"] == "unknownEntity"
    schemes = {i["scheme"] for i in successor["recordDetails"]["identifiers"]}
    assert "XI-LEI" not in schemes


def test_map_climatetrace_amalgamated_passes_validator() -> None:
    statements = map_climatetrace(_amalgamated_bundle())
    issues = validate_shape(statements)
    assert issues == [], issues


# ---------------------------------------------------------------------------
# State-owned enterprises — BODS "representing state-owned enterprises"
# ---------------------------------------------------------------------------


def _soe_bundle(parent_type: str | None, *, country: str | None = "IDN") -> dict:
    """PT Pertamina (Persero) with the Republic of Indonesia declared above it."""
    return {
        "source_id": "climatetrace",
        "entity_id": "E100000000538",
        "entity_name": "PT Pertamina (Persero)",
        "lei": "254900NDAKGNZ2IBBL45",
        "gem_row": {
            "Entity ID": "E100000000538",
            "Full Name": "PT Pertamina (Persero)",
            "Global Legal Entity Identifier Index": "254900NDAKGNZ2IBBL45",
            "Headquarters Country": "IDN",
            "Entity Type": "legal entity",
            "Gem parents IDs": "E100001000084 [100.0%]",
            "Gem parents": "Government of Indonesia [100%]",
        },
        "emissions": {},
        "assets": [],
        "parents": [
            {
                "entity_id": "E100001000084",
                "name": "Government of Indonesia",
                "share": 100.0,
                "entity_type": parent_type,
                "country": country,
            }
        ],
        "is_stub": False,
    }


def _parent_statement(statements: list[dict]) -> dict:
    return next(
        s
        for s in statements
        if s.get("recordType") == "entity"
        and s["recordDetails"]["name"] == "Government of Indonesia"
    )


def test_state_parent_is_typed_state_with_jurisdiction() -> None:
    """A GEM `state` parent becomes a BODS `state` entity carrying its state.

    BODS: an SOE's entity statement MUST be the subject of relationship
    statements connecting it to an entity statement typed `state`/`stateBody`,
    and jurisdiction represents the particular state.
    """
    statements = list(map_climatetrace(_soe_bundle("state")))
    parent = _parent_statement(statements)
    assert parent["recordDetails"]["entityType"]["type"] == "state"
    assert parent["recordDetails"]["jurisdiction"]["code"] == "ID"
    assert "Global Energy Monitor" in parent["recordDetails"]["entityType"]["details"]
    assert validate_shape(statements) == []


def test_state_body_parent_is_typed_state_body() -> None:
    parent = _parent_statement(list(map_climatetrace(_soe_bundle("state body"))))
    assert parent["recordDetails"]["entityType"]["type"] == "stateBody"


def test_state_parent_raises_state_controlled_naming_its_source() -> None:
    """The SOE structure alone drives the risk signal — no bespoke GEM rule.

    The summary must name GEM as the source of the structure; hard-coding
    "Wikidata-sourced" would misattribute the provenance of the caveat.
    """
    from opencheck.risk import STATE_CONTROLLED, _state_controlled_signals

    statements = list(map_climatetrace(_soe_bundle("state")))
    signals = _state_controlled_signals("climatetrace", "E100000000538", statements)
    assert [s.code for s in signals] == [STATE_CONTROLLED]
    assert "Government of Indonesia" in signals[0].evidence["state_owners"]
    assert "Global Energy Monitor" in signals[0].summary
    assert "Wikidata" not in signals[0].summary


def test_parent_without_its_own_row_stays_unknown_entity() -> None:
    """No row for the parent means "unknown", not a guessed type."""
    parent = _parent_statement(list(map_climatetrace(_soe_bundle(None, country=None))))
    assert parent["recordDetails"]["entityType"]["type"] == "unknownEntity"
    assert "jurisdiction" not in parent["recordDetails"]


def test_person_parent_emits_neither_node_nor_edge() -> None:
    """GEM types a couple of parents as natural persons; an entity statement
    would misdescribe them, so the whole edge is dropped rather than mistyped."""
    statements = list(map_climatetrace(_soe_bundle("person")))
    assert not any(
        s.get("recordType") == "entity"
        and s["recordDetails"]["name"] == "Government of Indonesia"
        for s in statements
    )
    assert not any(s.get("recordType") == "relationship" for s in statements)


def test_legal_entity_parent_is_not_typed_state() -> None:
    """Ordinary corporate parents keep their own type; nothing state-ish leaks."""
    parent = _parent_statement(list(map_climatetrace(_soe_bundle("legal entity"))))
    assert parent["recordDetails"]["entityType"]["type"] == "legalEntity"
    assert "details" not in parent["recordDetails"]["entityType"]


def test_self_declared_parent_emits_no_relationship() -> None:
    """GEM lists a group's top entity as its own parent — never emit that.

    5,132 rows in the September 2026 GEOT snapshot declare themselves as their
    own parent (PT Pertamina (Persero) included). A relationship whose subject
    and interestedParty are the same statement says an entity owns itself.
    """
    bundle = _soe_bundle("state")
    bundle["parents"] = [
        {
            "entity_id": bundle["entity_id"],
            "name": bundle["entity_name"],
            "share": 100.0,
            "entity_type": "legal entity",
            "country": "IDN",
        }
    ]
    statements = list(map_climatetrace(bundle))
    assert not any(s.get("recordType") == "relationship" for s in statements)
    assert validate_shape(statements) == []
