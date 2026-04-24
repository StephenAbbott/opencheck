"""Tests for the GLEIF + FtM BODS v0.4 mappers (Phase 2)."""

from __future__ import annotations

from opencheck.bods import (
    map_gleif,
    map_openaleph,
    map_opensanctions,
    validate_shape,
)


# ---------------------------------------------------------------------
# GLEIF
# ---------------------------------------------------------------------


def _gleif_bundle_with_direct_parent() -> dict:
    return {
        "lei": "213800LBDB8WB3QGVN21",
        "record": {
            "id": "213800LBDB8WB3QGVN21",
            "attributes": {
                "lei": "213800LBDB8WB3QGVN21",
                "entity": {
                    "legalName": {"name": "BP P.L.C."},
                    "jurisdiction": "GB",
                    "registeredAs": "00102498",
                    # GLEIF RA code for UK Companies House.
                    "registeredAt": {"id": "RA000585", "other": None},
                    "legalAddress": {
                        "addressLines": ["1 St James's Square"],
                        "city": "London",
                        "postalCode": "SW1Y 4PD",
                        "country": "GB",
                    },
                },
            },
        },
        "direct_parent": {
            "id": "PARENTXXXXXXXXXXXXXX",
            "attributes": {
                "lei": "PARENTXXXXXXXXXXXXXX",
                "entity": {
                    "legalName": {"name": "BP Group Holdings"},
                    "jurisdiction": "GB",
                },
            },
        },
        "ultimate_parent": None,
    }


def test_map_gleif_emits_subject_and_parent() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    statements = list(bundle)
    types = [s["recordType"] for s in statements]
    assert types.count("entity") == 2
    assert types.count("relationship") == 1


def test_map_gleif_subject_has_lei_identifier() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    schemes = [i["scheme"] for i in subject["recordDetails"]["identifiers"]]
    assert "XI-LEI" in schemes


def test_map_gleif_uses_registration_authority_scheme() -> None:
    """Subject picks up the GLEIF RA code (e.g. RA000585) as identifier scheme."""
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    ra = next(
        i for i in subject["recordDetails"]["identifiers"] if i["scheme"] == "RA000585"
    )
    assert ra["id"] == "00102498"
    assert "Registration Authority" in ra["schemeName"]


def test_map_gleif_resolves_jurisdiction_name() -> None:
    """'GB' should resolve to 'United Kingdom' via pycountry."""
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    assert subject["recordDetails"]["incorporatedInJurisdiction"] == {
        "name": "United Kingdom",
        "code": "GB",
    }


def test_map_gleif_resolves_subdivision_jurisdiction() -> None:
    payload = _gleif_bundle_with_direct_parent()
    payload["record"]["attributes"]["entity"]["jurisdiction"] = "GB-ENG"
    bundle = map_gleif(payload)
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    jur = subject["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "GB-ENG"
    assert "United Kingdom" in jur["name"]


def test_map_gleif_relationship_has_control_interest() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    rel = next(s for s in bundle if s["recordType"] == "relationship")
    interests = rel["recordDetails"]["interests"]
    assert interests[0]["type"] == "otherInfluenceOrControl"
    assert "direct-parent" in interests[0]["details"]


def test_map_gleif_output_passes_validator() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_map_gleif_handles_missing_parents() -> None:
    payload = _gleif_bundle_with_direct_parent()
    payload["direct_parent"] = None
    bundle = map_gleif(payload)
    types = [s["recordType"] for s in bundle]
    assert types == ["entity"]  # just the subject


def test_map_gleif_emits_bridge_for_natural_persons_exception() -> None:
    """A NATURAL_PERSONS exception becomes an unknownPerson + relationship."""
    payload = {
        "lei": "LEI00000000000000001",
        "record": {
            "id": "LEI00000000000000001",
            "attributes": {
                "lei": "LEI00000000000000001",
                "entity": {
                    "legalName": {"name": "Family Trust Holdings Ltd"},
                    "jurisdiction": "GB",
                },
            },
        },
        "direct_parent": None,
        "direct_parent_exception": {
            "attributes": {
                "exceptionCategory": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                "exceptionReason": "NATURAL_PERSONS",
            }
        },
        "ultimate_parent": None,
        "ultimate_parent_exception": None,
    }
    bundle = map_gleif(payload)
    types = [s["recordType"] for s in bundle]
    # subject + bridge person + relationship = 3 statements
    assert types == ["entity", "person", "relationship"]

    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["personType"] == "unknownPerson"

    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert "NATURAL_PERSONS" in rel["recordDetails"]["interests"][0]["details"].upper() \
        or "natural persons" in rel["recordDetails"]["interests"][0]["details"].lower()

    # Validator still clean.
    assert validate_shape(bundle) == []


def test_map_gleif_emits_anonymous_entity_for_no_lei_exception() -> None:
    payload = {
        "lei": "LEI00000000000000002",
        "record": {
            "id": "LEI00000000000000002",
            "attributes": {
                "lei": "LEI00000000000000002",
                "entity": {
                    "legalName": {"name": "Quiet Holdings SA"},
                    "jurisdiction": "CH",
                },
            },
        },
        "direct_parent": None,
        "direct_parent_exception": None,
        "ultimate_parent": None,
        "ultimate_parent_exception": {
            "attributes": {
                "exceptionCategory": "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
                "exceptionReason": "NO_LEI",
            }
        },
    }
    bundle = map_gleif(payload)
    entities = [s for s in bundle if s["recordType"] == "entity"]
    # subject + anonymousEntity bridge
    assert len(entities) == 2
    bridge = next(
        e for e in entities
        if e["recordDetails"]["entityType"]["type"] == "anonymousEntity"
    )
    assert "reporting exception" in bridge["recordDetails"]["name"].lower()

    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["directOrIndirect"] == "indirect"
    assert validate_shape(bundle) == []


# ---------------------------------------------------------------------
# FtM — OpenSanctions
# ---------------------------------------------------------------------


def _opensanctions_company_bundle() -> dict:
    return {
        "source_id": "opensanctions",
        "entity_id": "NK-rosneft",
        "entity": {
            "id": "NK-rosneft",
            "schema": "Company",
            "caption": "Rosneft Oil Company",
            "properties": {
                "name": ["Rosneft Oil Company"],
                "leiCode": ["253400VC22A0KFSOPB29"],
                "wikidataId": ["Q219617"],
                "jurisdiction": ["RU"],
                "incorporationDate": ["1993-09-30"],
            },
        },
    }


def test_map_opensanctions_emits_entity_with_identifiers() -> None:
    bundle = map_opensanctions(_opensanctions_company_bundle())
    statements = list(bundle)
    assert len(statements) == 1
    entity = statements[0]
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert {"OPENSANCTIONS", "XI-LEI", "WIKIDATA"}.issubset(schemes)
    assert entity["recordDetails"]["foundingDate"] == "1993-09-30"


def test_map_opensanctions_person() -> None:
    payload = {
        "entity": {
            "id": "NK-putin",
            "schema": "Person",
            "caption": "Vladimir Putin",
            "properties": {
                "name": ["Vladimir Putin"],
                "nationality": ["ru"],
                "birthDate": ["1952-10-07"],
                "wikidataId": ["Q7747"],
            },
        }
    }
    bundle = map_opensanctions(payload)
    statements = list(bundle)
    assert len(statements) == 1
    s = statements[0]
    assert s["recordType"] == "person"
    assert s["recordDetails"]["birthDate"] == "1952-10-07"
    schemes = {i["scheme"] for i in s["recordDetails"]["identifiers"]}
    assert "WIKIDATA" in schemes


def test_map_opensanctions_passes_validator() -> None:
    bundle = map_opensanctions(_opensanctions_company_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_ftm_owners_of_property_emits_relationship() -> None:
    """A Person with `ownersOf` → [Company] should emit a shareholding link."""
    payload = {
        "entity": {
            "id": "NK-owner",
            "schema": "Person",
            "caption": "Jane Owner",
            "properties": {
                "name": ["Jane Owner"],
                "ownersOf": [
                    {
                        "id": "NK-target",
                        "schema": "Company",
                        "caption": "Target Holdings",
                        "properties": {"name": ["Target Holdings"]},
                    }
                ],
            },
        }
    }
    bundle = map_opensanctions(payload)
    types = [s["recordType"] for s in bundle]
    assert types.count("person") == 1  # Jane
    assert types.count("entity") == 1  # Target
    assert types.count("relationship") == 1
    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["type"] == "shareholding"


# ---------------------------------------------------------------------
# FtM — OpenAleph
# ---------------------------------------------------------------------


def test_map_openaleph_entity() -> None:
    payload = {
        "entity": {
            "id": "aleph-123",
            "schema": "Company",
            "properties": {
                "name": ["Acme Holdings"],
                "leiCode": ["LEI0000000000000ACME"],
                "jurisdiction": ["BVI"],
            },
        },
        "collection": {"label": "ICIJ leaks", "license": "CC BY-NC 4.0"},
    }
    bundle = map_openaleph(payload)
    issues = validate_shape(bundle)
    assert issues == [], issues
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert {"OPENALEPH", "XI-LEI"}.issubset(schemes)
