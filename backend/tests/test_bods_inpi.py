"""Tests for the INPI → BODS v0.4 mapper."""

from __future__ import annotations

import pytest

from opencheck.bods import map_inpi, validate_shape
from opencheck.sources.inpi import INPI_RA_CODE, normalise_siren

# ---------------------------------------------------------------------------
# Sample fixtures (based on RNE API schema / Annexe 3 JSON example)
# ---------------------------------------------------------------------------

_COMPANY_BOLLORE = {
    # Actual RNE API structure: denomination is at top-level identite,
    # company data is nested under formality.content.
    "diffusionINSEE": "O",
    "siren": "055804124",
    "identite": {
        "entreprise": {
            "siren": "055804124",
            "denomination": "BOLLORE SE",
            "formeJuridique": "5800",
        }
    },
    "formality": {
        "siren": "055804124",
        "content": {
            "personneMorale": {
                "adresseEntreprise": {
                    "adresse": {
                        "numVoie": "31",
                        "typeVoie": "QUAI",
                        "voie": "DE DION BOUTON",
                        "codePostal": "92800",
                        "commune": "PUTEAUX",
                    }
                },
                "composition": {
                    "pouvoirs": [
                        {
                            "typeDePersonne": "INDIVIDU",
                            "beneficiaireEffectif": False,
                            "individu": {
                                "descriptionPersonne": {
                                    "nom": "DOE",
                                    "prenoms": ["JANE"],
                                }
                            },
                        }
                    ]
                },
            },
            "natureCreation": {
                "dateCreation": "1906-07-07",
            },
        },
    },
}

_COMPANY_NO_DATE = {
    "diffusionINSEE": "O",
    "siren": "123456789",
    "identite": {
        "entreprise": {"denomination": "ACME FRANCE SAS"},
    },
    "formality": {
        "content": {
            "personneMorale": {
                "adresseEntreprise": {},
                "composition": {"pouvoirs": []},
            },
            "natureCreation": {},
        }
    },
}

_COMPANY_NON_DIFFUSABLE = {
    "diffusionINSEE": "N",
    "siren": "999999999",
}

_COMPANY_SOLE_TRADER = {
    "diffusionINSEE": "O",
    "siren": "111111111",
    "formality": {
        "content": {
            "personnePhysique": {
                "identite": {"nom": "DUPONT", "prenoms": ["JEAN"]},
            }
        }
    },
}


def _bundle(
    company: dict | None = None,
    siren: str = "055804124",
) -> dict:
    return {
        "source_id": "inpi",
        "siren": siren,
        "company": company if company is not None else _COMPANY_BOLLORE,
        "is_stub": False,
    }


# ---------------------------------------------------------------------------
# SIREN normalisation utility
# ---------------------------------------------------------------------------


def test_normalise_siren_9_digit() -> None:
    assert normalise_siren("055804124") == "055804124"


def test_normalise_siren_zero_pad() -> None:
    assert normalise_siren("12345678") == "012345678"


def test_normalise_siren_strips_whitespace() -> None:
    assert normalise_siren("  055804124  ") == "055804124"


def test_inpi_ra_code() -> None:
    assert INPI_RA_CODE == "RA000189"


# ---------------------------------------------------------------------------
# map_inpi — basic shape
# ---------------------------------------------------------------------------


def test_map_inpi_produces_one_entity() -> None:
    stmts = list(map_inpi(_bundle()))
    assert len(stmts) == 1
    assert stmts[0]["recordType"] == "entity"


def test_map_inpi_entity_name() -> None:
    stmts = list(map_inpi(_bundle()))
    assert stmts[0]["recordDetails"]["name"] == "BOLLORE SE"


def test_map_inpi_entity_jurisdiction() -> None:
    stmts = list(map_inpi(_bundle()))
    jur = stmts[0]["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "FR"
    assert "France" in jur["name"]


def test_map_inpi_identifier_scheme() -> None:
    stmts = list(map_inpi(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "FR-SIREN" in schemes


def test_map_inpi_identifier_value() -> None:
    stmts = list(map_inpi(_bundle()))
    siren_id = next(
        i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "FR-SIREN"
    )
    assert siren_id == "055804124"


# ---------------------------------------------------------------------------
# map_inpi — founding date
# ---------------------------------------------------------------------------


def test_map_inpi_founding_date_parsed() -> None:
    stmts = list(map_inpi(_bundle()))
    assert stmts[0]["recordDetails"]["foundingDate"] == "1906-07-07"


def test_map_inpi_founding_date_absent_when_missing() -> None:
    stmts = list(map_inpi(_bundle(_COMPANY_NO_DATE, siren="123456789")))
    assert "foundingDate" not in stmts[0]["recordDetails"]


# ---------------------------------------------------------------------------
# map_inpi — address
# ---------------------------------------------------------------------------


def test_map_inpi_address_present() -> None:
    stmts = list(map_inpi(_bundle()))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert len(addrs) == 1
    assert addrs[0]["country"] == {"name": "France", "code": "FR"}
    assert "DION BOUTON" in addrs[0]["address"]
    assert "92800" in addrs[0]["address"]


def test_map_inpi_address_absent_when_empty() -> None:
    stmts = list(map_inpi(_bundle(_COMPANY_NO_DATE, siren="123456789")))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert addrs == []


# ---------------------------------------------------------------------------
# map_inpi — no person statements (BO restriction)
# ---------------------------------------------------------------------------


def test_map_inpi_no_person_statements() -> None:
    """map_inpi must never emit person statements regardless of pouvoirs content."""
    stmts = list(map_inpi(_bundle()))
    person_stmts = [s for s in stmts if s.get("recordType") == "person"]
    assert person_stmts == []


def test_map_inpi_no_relationship_statements() -> None:
    stmts = list(map_inpi(_bundle()))
    rel_stmts = [s for s in stmts if s.get("recordType") == "relationship"]
    assert rel_stmts == []


# ---------------------------------------------------------------------------
# map_inpi — early exits
# ---------------------------------------------------------------------------


def test_map_inpi_stub_returns_empty() -> None:
    bundle = {
        "source_id": "inpi",
        "siren": "055804124",
        "company": None,
        "is_stub": True,
    }
    assert list(map_inpi(bundle)) == []


def test_map_inpi_empty_company_returns_empty() -> None:
    bundle = {
        "source_id": "inpi",
        "siren": "055804124",
        "company": {},
        "is_stub": False,
    }
    assert list(map_inpi(bundle)) == []


def test_map_inpi_non_diffusable_stub_returns_empty() -> None:
    """Non-diffusable companies arrive as stubs; map_inpi must emit nothing."""
    bundle = {
        "source_id": "inpi",
        "siren": "999999999",
        "company": None,
        "is_stub": True,
        "non_diffusable": True,
    }
    assert list(map_inpi(bundle)) == []


def test_map_inpi_sole_trader_returns_empty() -> None:
    """personnePhysique (sole trader) companies are out of scope — empty."""
    stmts = list(map_inpi(_bundle(_COMPANY_SOLE_TRADER, siren="111111111")))
    assert stmts == []


def test_map_inpi_missing_siren_returns_empty() -> None:
    bundle = {
        "source_id": "inpi",
        "siren": "",
        "company": _COMPANY_BOLLORE,
        "is_stub": False,
    }
    assert list(map_inpi(bundle)) == []


# ---------------------------------------------------------------------------
# map_inpi — source block
# ---------------------------------------------------------------------------


def test_map_inpi_source_url_contains_siren() -> None:
    stmts = list(map_inpi(_bundle()))
    source = stmts[0].get("source") or {}
    url = source.get("url", "")
    assert "055804124" in url


def test_map_inpi_source_type_official_register() -> None:
    stmts = list(map_inpi(_bundle()))
    source = stmts[0].get("source") or {}
    assert source.get("type") == ["officialRegister"]


# ---------------------------------------------------------------------------
# BODS validator compliance
# ---------------------------------------------------------------------------


def test_map_inpi_passes_validator() -> None:
    issues = validate_shape(map_inpi(_bundle()))
    assert issues == [], issues


def test_map_inpi_no_date_passes_validator() -> None:
    issues = validate_shape(map_inpi(_bundle(_COMPANY_NO_DATE, siren="123456789")))
    assert issues == [], issues
