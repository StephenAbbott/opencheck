"""Tests for the INPI → BODS v0.4 mapper."""

from __future__ import annotations

import pytest

from opencheck.bods import map_inpi, validate_shape
from opencheck.bods.mapper import _inpi_role_interest_type, _INPI_ROLE_LABELS
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
                            # Non-BO director — WILL be emitted (roleEntreprise 53 = Directeur Général)
                            "typeDePersonne": "INDIVIDU",
                            "beneficiaireEffectif": False,
                            "individu": {
                                "descriptionPersonne": {
                                    "nom": "DOE",
                                    "prenoms": ["JANE"],
                                    "nationalite": "Française",
                                    "roleEntreprise": 53,
                                    "dateEffetRoleDeclarant": "2020-03-01",
                                    "dateEffetRoleDeclarantPresent": True,
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

# Company with no pouvoirs — used for basic entity-only shape tests.
_COMPANY_ENTITY_ONLY = {
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
                "composition": {"pouvoirs": []},
            },
            "natureCreation": {"dateCreation": "1906-07-07"},
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
    stmts = list(map_inpi(_bundle(_COMPANY_ENTITY_ONLY)))
    assert len(stmts) == 1
    assert stmts[0]["recordType"] == "entity"


def test_map_inpi_entity_name() -> None:
    stmts = list(map_inpi(_bundle()))
    assert stmts[0]["recordDetails"]["name"] == "BOLLORE SE"


def test_map_inpi_entity_jurisdiction() -> None:
    stmts = list(map_inpi(_bundle()))
    jur = stmts[0]["recordDetails"]["jurisdiction"]
    assert jur["code"] == "FR"
    assert "France" in jur["name"]


def test_map_inpi_identifier_scheme() -> None:
    stmts = list(map_inpi(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "FR-INSEE" in schemes


def test_map_inpi_identifier_value() -> None:
    stmts = list(map_inpi(_bundle()))
    siren_id = next(
        i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "FR-INSEE"
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
# map_inpi — non-BO INDIVIDU → person + relationship statements
# ---------------------------------------------------------------------------


def test_map_inpi_non_bo_individu_emits_person() -> None:
    """A non-BO INDIVIDU pouvoir produces a person statement."""
    stmts = list(map_inpi(_bundle()))
    persons = [s for s in stmts if s["recordType"] == "person"]
    assert len(persons) == 1


def test_map_inpi_non_bo_individu_emits_relationship() -> None:
    stmts = list(map_inpi(_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    assert len(rels) == 1


def test_map_inpi_person_full_name() -> None:
    stmts = list(map_inpi(_bundle()))
    person = next(s for s in stmts if s["recordType"] == "person")
    assert person["recordDetails"]["names"][0]["fullName"] == "JANE DOE"


def test_map_inpi_person_nationality() -> None:
    stmts = list(map_inpi(_bundle()))
    person = next(s for s in stmts if s["recordType"] == "person")
    assert person["recordDetails"]["nationalities"] == [{"name": "Française"}]


def test_map_inpi_relationship_interest_type_senior_managing() -> None:
    """roleEntreprise 53 (Directeur Général) → seniorManagingOfficial."""
    stmts = list(map_inpi(_bundle()))
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["type"] == "seniorManagingOfficial"


def test_map_inpi_relationship_details_contains_french_label() -> None:
    stmts = list(map_inpi(_bundle()))
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    details = rel["recordDetails"]["interests"][0]["details"]
    assert "Directeur Général" in details


def test_map_inpi_relationship_start_date_from_date_effet() -> None:
    stmts = list(map_inpi(_bundle()))
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["startDate"] == "2020-03-01"


def test_map_inpi_relationship_beneficial_ownership_false() -> None:
    stmts = list(map_inpi(_bundle()))
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["beneficialOwnershipOrControl"] is False


def test_map_inpi_relationship_subject_is_entity() -> None:
    stmts = list(map_inpi(_bundle()))
    entity = next(s for s in stmts if s["recordType"] == "entity")
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["subject"] == entity["statementId"]


def test_map_inpi_relationship_interested_party_is_person() -> None:
    stmts = list(map_inpi(_bundle()))
    person = next(s for s in stmts if s["recordType"] == "person")
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interestedParty"] == person["statementId"]


# ---------------------------------------------------------------------------
# map_inpi — BO restriction: beneficiaireEffectif=True must be skipped
# ---------------------------------------------------------------------------


def test_map_inpi_skips_bo_individu() -> None:
    """Pouvoir with beneficiaireEffectif=True must never produce a person statement."""
    bo_company = {
        "diffusionINSEE": "O",
        "siren": "055804124",
        "identite": {"entreprise": {"denomination": "TEST SA"}},
        "formality": {
            "content": {
                "personneMorale": {
                    "adresseEntreprise": {},
                    "composition": {
                        "pouvoirs": [
                            {
                                "typeDePersonne": "INDIVIDU",
                                "beneficiaireEffectif": True,   # BO record — must be skipped
                                "individu": {
                                    "descriptionPersonne": {
                                        "nom": "SECRET",
                                        "prenoms": ["OWNER"],
                                        "roleEntreprise": 30,
                                    }
                                },
                            }
                        ]
                    },
                },
                "natureCreation": {},
            }
        },
    }
    stmts = list(map_inpi(_bundle(bo_company)))
    assert all(s["recordType"] == "entity" for s in stmts), (
        "BO individual must not be emitted as a person statement"
    )


def test_map_inpi_skips_non_individu_pouvoirs() -> None:
    """typeDePersonne != INDIVIDU entries are not emitted as persons."""
    company = {
        "diffusionINSEE": "O",
        "siren": "055804124",
        "identite": {"entreprise": {"denomination": "TEST SA"}},
        "formality": {
            "content": {
                "personneMorale": {
                    "adresseEntreprise": {},
                    "composition": {
                        "pouvoirs": [
                            {
                                "typeDePersonne": "ENTREPRISE",
                                "beneficiaireEffectif": False,
                                "entreprise": {"denomination": "PARENT SA"},
                            }
                        ]
                    },
                },
                "natureCreation": {},
            }
        },
    }
    stmts = list(map_inpi(_bundle(company)))
    assert all(s["recordType"] == "entity" for s in stmts)


# ---------------------------------------------------------------------------
# map_inpi — role type splitting
# ---------------------------------------------------------------------------


def test_map_inpi_auditor_role_maps_to_other_influence() -> None:
    """roleEntreprise 71 (Commissaire aux comptes titulaire) → otherInfluenceOrControl."""
    auditor_company = {
        "diffusionINSEE": "O",
        "siren": "055804124",
        "identite": {"entreprise": {"denomination": "TEST SA"}},
        "formality": {
            "content": {
                "personneMorale": {
                    "adresseEntreprise": {},
                    "composition": {
                        "pouvoirs": [
                            {
                                "typeDePersonne": "INDIVIDU",
                                "beneficiaireEffectif": False,
                                "individu": {
                                    "descriptionPersonne": {
                                        "nom": "MARTIN",
                                        "prenoms": ["LUC"],
                                        "roleEntreprise": 71,
                                    }
                                },
                            }
                        ]
                    },
                },
                "natureCreation": {},
            }
        },
    }
    stmts = list(map_inpi(_bundle(auditor_company)))
    rel = next(s for s in stmts if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["type"] == "otherInfluenceOrControl"
    assert "Commissaire aux comptes titulaire" in rel["recordDetails"]["interests"][0]["details"]


def test_map_inpi_avocat_maps_to_other_influence() -> None:
    assert _inpi_role_interest_type(150) == "otherInfluenceOrControl"


def test_map_inpi_mandataire_fiscal_maps_to_other_influence() -> None:
    assert _inpi_role_interest_type(109) == "otherInfluenceOrControl"


def test_map_inpi_auditeur_durabilite_maps_to_other_influence() -> None:
    assert _inpi_role_interest_type(220) == "otherInfluenceOrControl"


def test_map_inpi_gerant_maps_to_senior_managing() -> None:
    assert _inpi_role_interest_type(30) == "seniorManagingOfficial"


def test_map_inpi_president_sas_maps_to_senior_managing() -> None:
    assert _inpi_role_interest_type(73) == "seniorManagingOfficial"


def test_map_inpi_unknown_code_defaults_to_senior_managing() -> None:
    assert _inpi_role_interest_type(9999) == "seniorManagingOfficial"


def test_map_inpi_none_code_defaults_to_senior_managing() -> None:
    assert _inpi_role_interest_type(None) == "seniorManagingOfficial"


def test_map_inpi_string_code_is_accepted() -> None:
    """roleEntreprise may arrive as a string from the API."""
    assert _inpi_role_interest_type("71") == "otherInfluenceOrControl"
    assert _inpi_role_interest_type("30") == "seniorManagingOfficial"


# ---------------------------------------------------------------------------
# map_inpi — role label completeness
# ---------------------------------------------------------------------------


def test_inpi_role_labels_covers_all_other_influence_codes() -> None:
    """Every code in _INPI_OTHER_INFLUENCE_CODES has a label in _INPI_ROLE_LABELS."""
    from opencheck.bods.mapper import _INPI_OTHER_INFLUENCE_CODES
    missing = _INPI_OTHER_INFLUENCE_CODES - set(_INPI_ROLE_LABELS.keys())
    assert not missing, f"Missing labels for codes: {missing}"


# ---------------------------------------------------------------------------
# map_inpi — same person, multiple roles
# ---------------------------------------------------------------------------


def test_map_inpi_same_person_two_roles_one_person_two_rels() -> None:
    """The same individual with two roles gets one person statement, two relationships."""
    company = {
        "diffusionINSEE": "O",
        "siren": "123456789",
        "identite": {"entreprise": {"denomination": "DUAL ROLE SA"}},
        "formality": {
            "content": {
                "personneMorale": {
                    "adresseEntreprise": {},
                    "composition": {
                        "pouvoirs": [
                            {
                                "typeDePersonne": "INDIVIDU",
                                "beneficiaireEffectif": False,
                                "individu": {
                                    "descriptionPersonne": {
                                        "nom": "DUPONT",
                                        "prenoms": ["JEAN"],
                                        "roleEntreprise": 53,  # Directeur Général
                                    }
                                },
                            },
                            {
                                "typeDePersonne": "INDIVIDU",
                                "beneficiaireEffectif": False,
                                "individu": {
                                    "descriptionPersonne": {
                                        "nom": "DUPONT",
                                        "prenoms": ["JEAN"],
                                        "roleEntreprise": 65,  # Administrateur
                                    }
                                },
                            },
                        ]
                    },
                },
                "natureCreation": {},
            }
        },
    }
    stmts = list(map_inpi(_bundle(company, siren="123456789")))
    types = [s["recordType"] for s in stmts]
    assert types.count("person") == 1
    assert types.count("relationship") == 2


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
    """Full bundle (entity + person + relationship) passes BODS schema validation."""
    issues = validate_shape(map_inpi(_bundle()))
    assert issues == [], issues


def test_map_inpi_no_date_passes_validator() -> None:
    issues = validate_shape(map_inpi(_bundle(_COMPANY_NO_DATE, siren="123456789")))
    assert issues == [], issues


def test_map_inpi_with_person_passes_validator() -> None:
    """Validate the full three-statement bundle against the BODS schema."""
    issues = validate_shape(map_inpi(_bundle()))
    assert issues == [], issues
