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
    # statementId == recordId by design: bods-dagre v0.4 resolves graph edges
    # by looking up the relationship's referenced id against each node's recordId.
    # Since BODS relationships reference by statementId, the two must be equal
    # for the lookup to succeed.  Opencheck never versions records so the
    # semantic distinction doesn't apply.
    assert s["statementId"] == s["recordId"]


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
    assert rd["subject"] == entity["statementId"]
    assert rd["interestedParty"] == person["statementId"]
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
        if r["recordDetails"]["interestedParty"] in people
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


# ---------------------------------------------------------------------
# Multi-hop related_companies depth
# ---------------------------------------------------------------------


def _make_related_bundle(number: str, name: str, psc_items: list) -> dict:
    """Helper: minimal company bundle suitable for use as a related_companies entry."""
    return {
        "source_id": "companies_house",
        "company_number": number,
        "profile": {"company_number": number, "company_name": name},
        "officers": {"items": []},
        "pscs": {"items": psc_items},
    }


def test_related_companies_connected_statementids() -> None:
    """The corporate PSC entity statementId in the root bundle must match the
    entity statementId emitted for the same company in related_companies."""
    root = _sample_bundle()
    # The corporate PSC in _sample_bundle has registration_number "12345678".
    # Add it as a related company with its own PSC.
    root["related_companies"] = {
        "12345678": _make_related_bundle(
            "12345678",
            "Acme Holdings Ltd",
            [
                {
                    "kind": "individual-person-with-significant-control",
                    "name": "Bob JONES",
                    "etag": "bj001",
                    "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                }
            ],
        )
    }

    bundle = map_companies_house(root)
    statements = list(bundle)

    # Collect entity statementIds and the interestedParty refs in relationships.
    # In BODS v0.4, subject/interestedParty are bare strings; resolve entity refs
    # by cross-referencing against the known entity statement IDs.
    entity_sids = {s["statementId"] for s in statements if s["recordType"] == "entity"}
    person_sids = {s["statementId"] for s in statements if s["recordType"] == "person"}
    entity_ip_refs = {
        s["recordDetails"]["interestedParty"]
        for s in statements
        if s["recordType"] == "relationship"
        and isinstance(s["recordDetails"].get("interestedParty"), str)
        # Exclude person IPs — only check entity→entity links are connected.
        and s["recordDetails"]["interestedParty"] not in person_sids
    }

    # Every entity interestedParty ref must point to a real entity statement.
    assert entity_ip_refs <= entity_sids, (
        f"Dangling entity refs (graph disconnected): {entity_ip_refs - entity_sids}"
    )

    # Specifically: the relationship from "00102498" → "12345678" must resolve.
    # The corporate PSC entity statement (local_id="12345678") should appear
    # exactly once (no duplicate from root vs. related pass).
    from opencheck.bods.mapper import _stable_id
    acme_sid = _stable_id("companies_house", "entity", "12345678")
    assert acme_sid in entity_sids, "Acme Holdings entity statement missing"
    assert sum(1 for s in statements if s["statementId"] == acme_sid) == 1, (
        "Duplicate entity statement for Acme Holdings"
    )


def test_related_companies_no_duplicates() -> None:
    """No statement should appear more than once when related_companies is present."""
    root = _sample_bundle()
    root["related_companies"] = {
        "12345678": _make_related_bundle("12345678", "Acme Holdings Ltd", [])
    }
    bundle = map_companies_house(root)
    sids = [s["statementId"] for s in bundle]
    assert len(sids) == len(set(sids)), f"Duplicate statementIds: {[s for s in sids if sids.count(s) > 1]}"


def test_related_companies_empty_is_backward_compatible() -> None:
    """Bundles without related_companies (old shape) still map correctly."""
    root = _sample_bundle()
    # No related_companies key at all.
    bundle = map_companies_house(root)
    types = [s["recordType"] for s in bundle]
    assert types.count("entity") == 2
    assert types.count("person") == 1
    assert types.count("relationship") == 2
