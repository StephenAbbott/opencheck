"""Tests for the Companies House → BODS v0.4 mapper."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from bods_validation_helpers import check_graph_connectivity, check_interest_types  # noqa: E402

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
    assert s["recordDetails"]["jurisdiction"] == {
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
                "share": {"exclusiveMinimum": 50, "maximum": 75},
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
    assert sh["share"] == {"exclusiveMinimum": 50, "maximum": 75}


def test_mapper_output_passes_shape_validator() -> None:
    bundle = map_companies_house(_sample_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_ceased_pscs_emit_closed_record() -> None:
    """A ceased PSC is represented by a 'closed' relationship (BODS Information
    updates), not dropped."""
    payload = _sample_bundle()
    payload["pscs"]["items"][0]["ceased_on"] = "2024-01-01"  # Jane (individual PSC)
    bundle = list(map_companies_house(payload))
    rels = [s for s in bundle if s["recordType"] == "relationship"]
    # Both PSCs still yield a relationship; the ceased one is 'closed'.
    assert len(rels) == 2
    closed = [r for r in rels if r["recordStatus"] == "closed"]
    assert len(closed) == 1
    c = closed[0]
    # Stable recordId vs distinct statementId; supersedes the original 'new'.
    assert c["recordId"] != c["statementId"]
    assert c["replacesStatements"], "closed record must record what it replaces"
    # Cessation date stamped on every interest and on the publication date.
    assert all(
        i.get("endDate") == "2024-01-01" for i in c["recordDetails"]["interests"]
    )
    assert c["publicationDetails"]["publicationDate"] == "2024-01-01"


def test_relationship_recordid_stable_across_lifecycle() -> None:
    """recordId is stable across new->closed; statementId is distinct;
    replacesStatements links the closed statement to the original 'new'."""
    common = dict(
        source_id="companies_house",
        local_id="00102498:psc:abc",
        subject_statement_id="subj-record-id",
        interested_party_statement_id="ip-record-id",
        interests=[
            {
                "type": "shareholding",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": True,
            }
        ],
    )
    new = make_relationship_statement(**common)
    closed = make_relationship_statement(
        **common, record_status="closed", publication_date="2024-01-01"
    )
    assert new["recordId"] == closed["recordId"]  # stable over lifecycle
    assert new["statementId"] != closed["statementId"]  # each statement unique
    assert new["recordStatus"] == "new"
    assert closed["recordStatus"] == "closed"
    assert closed["replacesStatements"] == [new["statementId"]]
    assert "replacesStatements" not in new


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


# ---------------------------------------------------------------------------
# Graph connectivity (Phase 2)
# ---------------------------------------------------------------------------


def test_ch_mapper_graph_connectivity() -> None:
    """All CH PSC relationship refs resolve to in-bundle statementIds."""
    stmts = list(map_companies_house(_sample_bundle()))
    issues = check_graph_connectivity(stmts)
    assert issues == [], issues


def test_ch_mapper_interest_types_valid() -> None:
    """All CH mapper interest types are valid BODS v0.4 codelist members."""
    stmts = list(map_companies_house(_sample_bundle()))
    invalid = check_interest_types(stmts)
    assert invalid == [], invalid


# ---------------------------------------------------------------------------
# natures_of_control coverage (every official CH code) — see the coverage
# audit recorded on the "UK PSC > BODS livestream demo" Notion ticket.
# ---------------------------------------------------------------------------

import pytest  # noqa: E402

from opencheck.bods.mapper import _parse_nature  # noqa: E402
from opencheck.bods.psc_natures import PSC_NATURE_DESCRIPTIONS  # noqa: E402
from opencheck.bods.validator import _VALID_INTEREST_TYPES  # noqa: E402


def test_all_official_psc_codes_map_to_valid_interest_type() -> None:
    """Every one of the 86 official CH natures_of_control codes maps to a valid
    BODS v0.4 interestType (never an invalid code, never a crash)."""
    assert len(PSC_NATURE_DESCRIPTIONS) == 86
    for code in PSC_NATURE_DESCRIPTIONS:
        entry = _parse_nature(code)
        assert entry["type"] in _VALID_INTEREST_TYPES, (code, entry["type"])


def test_every_code_carries_its_official_descriptor_in_details() -> None:
    """interest.details is the official CH descriptor for the code."""
    for code, descriptor in PSC_NATURE_DESCRIPTIONS.items():
        assert _parse_nature(code)["details"] == descriptor


def test_unknown_code_falls_back_to_raw_string() -> None:
    """A code not in the enumeration keeps the raw string in details and still
    yields a valid (generic) interest type."""
    entry = _parse_nature("some-future-unmapped-code")
    assert entry["details"] == "some-future-unmapped-code"
    assert entry["type"] == "otherInfluenceOrControl"


@pytest.mark.parametrize(
    "code, expected_type",
    [
        # shareholding
        ("ownership-of-shares-25-to-50-percent", "shareholding"),
        ("ownership-of-shares-more-than-25-percent-registered-overseas-entity", "shareholding"),
        # votingRights
        ("voting-rights-75-to-100-percent", "votingRights"),
        ("voting-rights-25-to-50-percent-limited-liability-partnership", "votingRights"),
        # appointmentOfBoard — incl. the previously-broken singular partnership form
        ("right-to-appoint-and-remove-directors", "appointmentOfBoard"),
        ("right-to-appoint-and-remove-members-limited-liability-partnership", "appointmentOfBoard"),
        ("right-to-appoint-and-remove-person", "appointmentOfBoard"),
        ("right-to-appoint-and-remove-person-as-firm", "appointmentOfBoard"),
        # rightsToSurplusAssetsOnDissolution — LLP + Scottish partnership
        ("right-to-share-surplus-assets-75-to-100-percent-limited-liability-partnership", "rightsToSurplusAssetsOnDissolution"),
        ("part-right-to-share-surplus-assets-25-to-50-percent", "rightsToSurplusAssetsOnDissolution"),
        # otherInfluenceOrControl — significant influence + (deliberately) nominee
        ("significant-influence-or-control", "otherInfluenceOrControl"),
        ("significant-influence-or-control-as-trust", "otherInfluenceOrControl"),
        ("registered-owner-as-nominee-person-england-wales-registered-overseas-entity", "otherInfluenceOrControl"),
    ],
)
def test_representative_code_families_map_as_expected(code: str, expected_type: str) -> None:
    assert _parse_nature(code)["type"] == expected_type


def test_country_obj_never_emits_overlong_code() -> None:
    """BODS Country.code is maxLength/minLength 2. Unresolvable country names
    (real PSC addresses use 'Great Britain', 'Turkey', etc.) must yield a
    name-only object, never an over-long `code`. Regression for the 'TURKEY'/
    'GREAT BRITAIN' is too long schema errors seen on live stream data."""
    from opencheck.bods.mapper import _country_obj

    assert _country_obj("GB") == {"name": "United Kingdom", "code": "GB"}
    assert _country_obj("Great Britain")["code"] == "GB"
    assert _country_obj("Turkey")["code"] == "TR"
    # Truly unresolvable -> name only, no code.
    out = _country_obj("Some Made Up Place")
    assert out == {"name": "Some Made Up Place"}
    assert "code" not in out


def test_share_band_extracted_for_banded_codes() -> None:
    assert _parse_nature("ownership-of-shares-50-to-75-percent")["share"] == {
        "exclusiveMinimum": 50,
        "maximum": 75,
    }
    assert _parse_nature("ownership-of-shares-75-to-100-percent")["share"] == {
        "exclusiveMinimum": 75,
        "maximum": 100,
    }
