"""Tests for the Bolagsverket → BODS v0.4 mapper.

Fixtures use the confirmed field names from the Bolagsverket
POST /organisationer API response (värdefulla datamängder v1).
Officer data is NOT returned by this endpoint, so no person or
relationship statements are emitted by map_bolagsverket.
"""

from __future__ import annotations

import pytest

from opencheck.bods import map_bolagsverket, validate_shape
from opencheck.sources.bolagsverket import BV_RA_CODE, format_org_number, normalise_org_number

# ---------------------------------------------------------------------------
# Sample fixtures — real field names from confirmed API response schema
# ---------------------------------------------------------------------------

_COMPANY_ERICSSON = {
    "organisationsidentitet": {"identitetsbeteckning": "5560160680"},
    "organisationsnamn": {
        "organisationsnamnLista": [
            {
                "namn": "Telefonaktiebolaget LM Ericsson",
                "registreringsdatum": "1918-08-18",
            }
        ]
    },
    "organisationsdatum": {"registreringsdatum": "1918-08-18"},
    "organisationsform": {"kod": "AB", "klartext": "Aktiebolag"},
    "postadressOrganisation": {
        "postadress": {
            "utdelningsadress": "Torshamnsgatan 21",
            "postnummer": "164 83",
            "postort": "STOCKHOLM",
            "land": "Sverige",
        }
    },
    "verksamOrganisation": {"kod": "JA"},
}

_COMPANY_NO_DATE = {
    "organisationsidentitet": {"identitetsbeteckning": "5560078970"},
    "organisationsnamn": {
        "organisationsnamnLista": [
            {"namn": "Volvo AB", "registreringsdatum": "1915-08-07"}
        ]
    },
    # organisationsdatum intentionally absent
    "postadressOrganisation": {
        "postadress": {
            "utdelningsadress": "Gropegårdsgatan 2",
            "postnummer": "405 31",
            "postort": "GÖTEBORG",
            "land": "Sverige",
        }
    },
}

_COMPANY_NO_ADDRESS = {
    "organisationsidentitet": {"identitetsbeteckning": "9999999999"},
    "organisationsnamn": {
        "organisationsnamnLista": [{"namn": "ACME Sverige AB"}]
    },
    "organisationsdatum": {"registreringsdatum": "2010-06-01"},
}

_COMPANY_WITH_CO_ADDRESS = {
    "organisationsidentitet": {"identitetsbeteckning": "5591234567"},
    "organisationsnamn": {
        "organisationsnamnLista": [{"namn": "Test C/O AB"}]
    },
    "organisationsdatum": {"registreringsdatum": "2005-01-01"},
    "postadressOrganisation": {
        "postadress": {
            "coAdress": "C/o Holding Group",
            "utdelningsadress": "Kungsgatan 1",
            "postnummer": "111 43",
            "postort": "Stockholm",
            "land": "Sverige",
        }
    },
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


def test_map_bolagsverket_emits_only_entity_statement() -> None:
    """/organisationer returns no officer data — only one entity statement emitted."""
    stmts = list(map_bolagsverket(_bundle()))
    assert len(stmts) == 1
    assert stmts[0]["recordType"] == "entity"


def test_map_bolagsverket_entity_name() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    assert stmts[0]["recordDetails"]["name"] == "Telefonaktiebolaget LM Ericsson"


def test_map_bolagsverket_entity_name_from_first_namn_lista_entry() -> None:
    """Takes namn from organisationsnamn.organisationsnamnLista[0].namn."""
    company = {
        "organisationsidentitet": {"identitetsbeteckning": "5560160680"},
        "organisationsnamn": {
            "organisationsnamnLista": [
                {"namn": "First Name AB"},
                {"namn": "Second Name AB"},
            ]
        },
        "organisationsdatum": {"registreringsdatum": "2000-01-01"},
    }
    stmts = list(map_bolagsverket(_bundle(company)))
    assert stmts[0]["recordDetails"]["name"] == "First Name AB"


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
    """organisationsdatum.registreringsdatum → foundingDate."""
    stmts = list(map_bolagsverket(_bundle()))
    assert stmts[0]["recordDetails"]["foundingDate"] == "1918-08-18"


def test_map_bolagsverket_founding_date_absent_when_missing() -> None:
    stmts = list(map_bolagsverket(_bundle(_COMPANY_NO_DATE, org_number="5560078970")))
    assert "foundingDate" not in stmts[0]["recordDetails"]


def test_map_bolagsverket_founding_date_timestamp_ignored() -> None:
    """ISO timestamps (length != 10) must not be mapped."""
    company = {
        "organisationsidentitet": {"identitetsbeteckning": "5560160680"},
        "organisationsnamn": {
            "organisationsnamnLista": [{"namn": "Timestamp Co AB"}]
        },
        "organisationsdatum": {
            "registreringsdatum": "2000-01-23T00:00:00.000+00:00"
        },
    }
    stmts = list(map_bolagsverket(_bundle(company)))
    assert "foundingDate" not in stmts[0]["recordDetails"]


# ---------------------------------------------------------------------------
# map_bolagsverket — address
# ---------------------------------------------------------------------------


def test_map_bolagsverket_address_present() -> None:
    """postadressOrganisation.postadress fields mapped to address string."""
    stmts = list(map_bolagsverket(_bundle()))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert len(addrs) == 1
    assert addrs[0]["type"] == "registered"
    assert "STOCKHOLM" in addrs[0]["address"]
    assert "Torshamnsgatan" in addrs[0]["address"]


def test_map_bolagsverket_address_country() -> None:
    stmts = list(map_bolagsverket(_bundle()))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert addrs[0]["country"] == "Sverige"


def test_map_bolagsverket_address_includes_co_address() -> None:
    stmts = list(map_bolagsverket(_bundle(_COMPANY_WITH_CO_ADDRESS, org_number="5591234567")))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert len(addrs) == 1
    assert "C/o Holding Group" in addrs[0]["address"]
    assert "Kungsgatan" in addrs[0]["address"]


def test_map_bolagsverket_address_absent_when_missing() -> None:
    stmts = list(map_bolagsverket(_bundle(_COMPANY_NO_ADDRESS, org_number="9999999999")))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert addrs == []


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
    """When organisationsnamnLista is absent, fall back to GLEIF legal_name."""
    company_no_name = {
        "organisationsidentitet": {"identitetsbeteckning": "5560160680"},
        "organisationsdatum": {"registreringsdatum": "1918-08-18"},
        "postadressOrganisation": {
            "postadress": {"postort": "STOCKHOLM", "land": "Sverige"}
        },
    }
    stmts = list(
        map_bolagsverket(
            _bundle(company_no_name, org_number="5560160680", legal_name="Ericsson (GLEIF)")
        )
    )
    assert stmts[0]["recordDetails"]["name"] == "Ericsson (GLEIF)"


def test_map_bolagsverket_no_name_at_all_returns_empty() -> None:
    company_no_name = {
        "organisationsidentitet": {"identitetsbeteckning": "5560160680"},
    }
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


def test_map_bolagsverket_no_address_passes_validator() -> None:
    issues = validate_shape(map_bolagsverket(_bundle(_COMPANY_NO_ADDRESS, org_number="9999999999")))
    assert issues == [], issues


def test_map_bolagsverket_co_address_passes_validator() -> None:
    issues = validate_shape(map_bolagsverket(_bundle(_COMPANY_WITH_CO_ADDRESS, org_number="5591234567")))
    assert issues == [], issues
