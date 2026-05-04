"""Tests for the Bolagsverket → BODS v0.4 mapper."""

from __future__ import annotations

import pytest

from opencheck.bods import map_bolagsverket, validate_shape
from opencheck.sources.bolagsverket import BV_RA_CODE, format_org_number, normalise_org_number

# ---------------------------------------------------------------------------
# Sample fixtures (based on expected Bolagsverket API response shape)
# ---------------------------------------------------------------------------

_COMPANY_ERICSSON = {
    "organisationsnummer": "556016-0680",
    "namn": "Telefonaktiebolaget LM Ericsson",
    "status": "Aktiv",
    "juridiskForm": {"kod": "AB", "klartext": "Aktiebolag"},
    "registreringsdatum": "1918-08-18",
    "adress": {
        "gatuadress": "Torshamnsgatan 21",
        "postnummer": "164 83",
        "postort": "STOCKHOLM",
    },
    "foretradare": [
        {
            "roll": "Styrelseledamot",
            "namn": "BORJE EKHOLM",
            "fodelsedat": "1963",
        },
        {
            "roll": "Styrelseordförande",
            "namn": "KRISTIN SKOGEN LUND",
            "fodelsedat": "1966",
        },
    ],
}

_COMPANY_NO_DATE = {
    "organisationsnummer": "556007-8970",
    "namn": "Volvo AB",
    "status": "Aktiv",
    "adress": {
        "gatuadress": "Gropegårdsgatan 2",
        "postnummer": "405 31",
        "postort": "GÖTEBORG",
    },
    "foretradare": [],
}

_COMPANY_NO_ADDRESS = {
    "organisationsnummer": "999999-9999",
    "namn": "ACME Sverige AB",
}

_COMPANY_WITH_FULL_BIRTH_DATE = {
    "organisationsnummer": "556123-4567",
    "namn": "Test AB",
    "foretradare": [
        {
            "roll": "Verkställande direktör",
            "namn": "ANNA SVENSSON",
            "fodelsedat": "1975-03-15",
        }
    ],
}


def _bundle(
    company: dict | None = None,
    org_number: str = "5560160680",
    legal_name: str = "",
) -> dict:
    return {
        "source_id": "bolagsverket",
        "org_number": org_number,
        "company": company if company is not None else _COMPANY_ERICSSON,
        "legal_name": legal_name,
        "is_stub": False,
    }


# ---------------------------------------------------------------------------
# Organisation number normalisation / formatting utilities
# ---------------------------------------------------------------------------


def test_normalise_org_number_10_digit() -> None:
    assert normalise_org_number("5560160680") == "5560160680"


def test_normalise_org_number_hyphenated() -> None:
    assert normalise_org_number("556016-0680") == "5560160680"


def test_normalise_org_number_strips_whitespace() -> None:
    assert normalise_org_number("  556016-0680  ") == "5560160680"


def test_normalise_org_number_invalid_raises() -> None:
    with pytest.raises(ValueError):
        normalise_org_number("12345")


def test_format_org_number() -> None:
    assert format_org_number("5560160680") == "556016-0680"


def test_format_org_number_from_hyphenated() -> None:
    assert format_org_number("556016-0680") == "556016-0680"


def test_bv_ra_code() -> None:
    assert BV_RA_CODE == "RA000544"


# ---------------------------------------------------------------------------
# map_bolagsverket — entity statement shape
# ---------------------------------------------------------------------------


def test_map_bolagsverket_produces_statements() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    assert len(stmts) > 0


def test_map_bolagsverket_first_is_entity() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    assert stmts[0]["recordType"] == "entity"


def test_map_bolagsverket_entity_name() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    assert stmts[0]["recordDetails"]["name"] == "Telefonaktiebolaget LM Ericsson"


def test_map_bolagsverket_entity_jurisdiction() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    jur = stmts[0]["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "SE"
    assert "Sweden" in jur["name"]


def test_map_bolagsverket_identifier_scheme() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "SE-BLV" in schemes


def test_map_bolagsverket_identifier_value() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    blv_id = next(
        i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "SE-BLV"
    )
    assert blv_id == "556016-0680"


# ---------------------------------------------------------------------------
# map_bolagsverket — founding date
# ---------------------------------------------------------------------------


def test_map_bolagsverket_founding_date_parsed() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    assert stmts[0]["recordDetails"]["foundingDate"] == "1918-08-18"


def test_map_bolagsverket_founding_date_absent_when_missing() -> None:
    stmts = list(map_bolagsverket(_bundle(_COMPANY_NO_DATE, org_number="5560078970")))
    assert "foundingDate" not in stmts[0]["recordDetails"]


# ---------------------------------------------------------------------------
# map_bolagsverket — address
# ---------------------------------------------------------------------------


def test_map_bolagsverket_address_present() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert len(addrs) == 1
    assert addrs[0]["country"] == "SE"
    assert "STOCKHOLM" in addrs[0]["address"]
    assert "Torshamnsgatan" in addrs[0]["address"]


def test_map_bolagsverket_address_absent_when_missing() -> None:
    stmts = list(map_bolagsverket(_bundle(_COMPANY_NO_ADDRESS, org_number="9999999999")))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert addrs == []


# ---------------------------------------------------------------------------
# map_bolagsverket — person + relationship statements (officers)
# ---------------------------------------------------------------------------


def test_map_bolagsverket_emits_person_statements() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    person_stmts = [s for s in stmts if s.get("recordType") == "person"]
    assert len(person_stmts) == 2


def test_map_bolagsverket_emits_relationship_statements() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    rel_stmts = [s for s in stmts if s.get("recordType") == "relationship"]
    assert len(rel_stmts) == 2


def test_map_bolagsverket_person_name() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    person_stmts = [s for s in stmts if s.get("recordType") == "person"]
    names = {
        s["recordDetails"]["names"][0]["fullName"]
        for s in person_stmts
    }
    assert "BORJE EKHOLM" in names
    assert "KRISTIN SKOGEN LUND" in names


def test_map_bolagsverket_board_member_interest_type() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    rel_stmts = [s for s in stmts if s.get("recordType") == "relationship"]
    interest_types = {
        rel["recordDetails"]["interests"][0]["type"]
        for rel in rel_stmts
    }
    # Styrelseledamot → boardMember; Styrelseordförande → boardChair
    assert "boardMember" in interest_types
    assert "boardChair" in interest_types


def test_map_bolagsverket_officer_not_beneficial_owner() -> None:
    """All officer relationships must have beneficialOwnershipOrControl=False."""
    stmts = list(map_bolagsverket(_bundle()))
    rel_stmts = [s for s in stmts if s.get("recordType") == "relationship"]
    for rel in rel_stmts:
        for interest in rel["recordDetails"]["interests"]:
            assert interest.get("beneficialOwnershipOrControl") is False


def test_map_bolagsverket_no_officers_returns_entity_only() -> None:
    stmts = list(map_bolagsverket(_bundle(_COMPANY_NO_DATE, org_number="5560078970")))
    assert len(stmts) == 1
    assert stmts[0]["recordType"] == "entity"


def test_map_bolagsverket_full_birth_date_included() -> None:
    """When fodelsedat is a full ISO date, it is mapped to birthDate."""
    stmts = list(map_bolagsverket(_bundle(_COMPANY_WITH_FULL_BIRTH_DATE, org_number="5561234567")))
    person_stmts = [s for s in stmts if s.get("recordType") == "person"]
    assert len(person_stmts) == 1
    assert person_stmts[0]["recordDetails"].get("birthDate") == "1975-03-15"


def test_map_bolagsverket_birth_year_only_not_mapped() -> None:
    """Year-only fodelsedat must not produce a birthDate (BODS requires full date)."""
    stmts = list(map_bolagsverket(_bundle()))
    person_stmts = [s for s in stmts if s.get("recordType") == "person"]
    for p in person_stmts:
        assert "birthDate" not in p["recordDetails"]


# ---------------------------------------------------------------------------
# map_bolagsverket — early exits
# ---------------------------------------------------------------------------


def test_map_bolagsverket_stub_returns_empty() -> None:
    bundle = {
        "source_id": "bolagsverket",
        "org_number": "5560160680",
        "company": None,
        "legal_name": "",
        "is_stub": True,
    }
    assert list(map_bolagsverket(bundle)) == []


def test_map_bolagsverket_empty_company_returns_empty() -> None:
    bundle = {
        "source_id": "bolagsverket",
        "org_number": "5560160680",
        "company": {},
        "legal_name": "",
        "is_stub": False,
    }
    assert list(map_bolagsverket(bundle)) == []


def test_map_bolagsverket_missing_org_number_returns_empty() -> None:
    bundle = {
        "source_id": "bolagsverket",
        "org_number": "",
        "company": _COMPANY_ERICSSON,
        "legal_name": "",
        "is_stub": False,
    }
    assert list(map_bolagsverket(bundle)) == []


def test_map_bolagsverket_legal_name_fallback() -> None:
    """When company has no namn, fall back to legal_name from GLEIF."""
    company_no_name = {
        "organisationsnummer": "556016-0680",
        "status": "Aktiv",
        "adress": {"postort": "STOCKHOLM"},
    }
    stmts = list(
        map_bolagsverket(
            _bundle(company_no_name, org_number="5560160680", legal_name="Ericsson (GLEIF)")
        )
    )
    assert stmts[0]["recordDetails"]["name"] == "Ericsson (GLEIF)"


def test_map_bolagsverket_no_name_at_all_returns_empty() -> None:
    company_no_name = {"status": "Aktiv"}
    stmts = list(map_bolagsverket(_bundle(company_no_name, org_number="5560160680", legal_name="")))
    assert stmts == []


# ---------------------------------------------------------------------------
# map_bolagsverket — source block
# ---------------------------------------------------------------------------


def test_map_bolagsverket_source_type_official_register() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    source = stmts[0].get("source") or {}
    assert source.get("type") == "officialRegister"


# ---------------------------------------------------------------------------
# BODS validator compliance
# ---------------------------------------------------------------------------


def test_map_bolagsverket_passes_validator() -> None:
    issues = validate_shape(map_bolagsverket(_bundle()))
    assert issues == [], issues


def test_map_bolagsverket_no_date_passes_validator() -> None:
    issues = validate_shape(map_bolagsverket(_bundle(_COMPANY_NO_DATE, org_number="5560078970")))
    assert issues == [], issues


def test_map_bolagsverket_full_birth_date_passes_validator() -> None:
    issues = validate_shape(map_bolagsverket(_bundle(_COMPANY_WITH_FULL_BIRTH_DATE, org_number="5561234567")))
    assert issues == [], issues
