"""Phase 1 — BODS graph connectivity baseline audit.

For every Tier 2 mapper (those that produce person and/or relationship
statements), this file verifies that:

1. **No dangling references** — every relationship's ``subject`` and
   ``interestedParty`` references a ``statementId`` present in the same bundle.
   This is the direct cause of floating nodes in bods-dagre.

2. **Interest type validity** — every interest ``type`` is a valid BODS v0.4
   codelist member.

3. **isComponent bookkeeping** — documents the current isComponent=True/False
   distribution across adapters so regressions are caught.

Note: ``isComponent`` and ``componentRecords`` fixes for indirect chains are
tracked separately in Phase 2 fixes; this test currently only asserts
connectivity (no dangling refs).

Run with::

    pytest tests/test_bods_graph_integrity.py -v
"""

from __future__ import annotations

import sys
import os

import pytest

# Add tests/ to path so we can import fixture factories from sibling test files.
sys.path.insert(0, os.path.dirname(__file__))

from bods_validation_helpers import (  # type: ignore[import]
    check_graph_connectivity as check_connectivity,
    check_interest_types,
    to_stmts,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# --- Companies House: individual PSC ---

_CH_INDIVIDUAL_PSC = {
    "company_number": "00000006",
    "profile": {
        "company_name": "TEST CO LTD",
        "company_number": "00000006",
        "type": "private-limited-company",
        "company_status": "active",
        "jurisdiction": "england-wales",
        "date_of_creation": "2000-01-01",
        "registered_office_address": {
            "address_line_1": "1 Test Road",
            "locality": "London",
            "postal_code": "SW1A 1AA",
            "country": "England",
        },
    },
    "officers": {
        "items": [
            {
                "name": "Smith, John",
                "officer_role": "director",
                "appointed_on": "2010-01-01",
                "links": {"officer": {"appointments": "/officers/ABCDEF/appointments"}},
                "address": {"address_line_1": "1 Lane", "locality": "London", "country": "England"},
            }
        ],
        "total_results": 1,
    },
    "pscs": {
        "items": [
            {
                "name": "JOHN SMITH",
                "kind": "individual-person-with-significant-control",
                "natures_of_control": ["ownership-of-shares-25-to-50-percent"],
                "notified_on": "2016-04-06",
                "nationality": "British",
                "date_of_birth": {"year": 1960, "month": 3},
                "address": {"address_line_1": "1 High St", "locality": "London", "country": "England"},
            }
        ],
        "total_results": 1,
    },
    "related_companies": {},
}

# --- Companies House: corporate PSC (2-hop chain: C → B → A) ---

_CH_CORPORATE_PSC = {
    "company_number": "00102498",
    "profile": {
        "company_name": "SUBSIDIARY LTD",
        "company_number": "00102498",
        "type": "private-limited-company",
        "company_status": "active",
        "jurisdiction": "england-wales",
        "date_of_creation": "2005-01-01",
        "registered_office_address": {"address_line_1": "1 Corp Rd", "locality": "London", "country": "England"},
    },
    "officers": {"items": [], "total_results": 0},
    "pscs": {
        "items": [
            {
                "name": "HOLDING COMPANY LTD",
                "kind": "corporate-entity-person-with-significant-control",
                "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                "notified_on": "2016-04-06",
                "identification": {
                    "registration_number": "12345678",
                    "country_registered": "England",
                    "place_registered": "Companies House",
                },
                "address": {"address_line_1": "2 Corp Rd", "locality": "London", "country": "England"},
            }
        ],
        "total_results": 1,
    },
    "related_companies": {
        "12345678": {
            "company_number": "12345678",
            "profile": {
                "company_name": "HOLDING COMPANY LTD",
                "company_number": "12345678",
                "type": "private-limited-company",
                "company_status": "active",
                "jurisdiction": "england-wales",
                "date_of_creation": "2000-01-01",
                "registered_office_address": {"address_line_1": "2 Corp Rd", "locality": "London", "country": "England"},
            },
            "officers": {"items": [], "total_results": 0},
            "pscs": {
                "items": [
                    {
                        "name": "ULTIMATE OWNER",
                        "kind": "individual-person-with-significant-control",
                        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                        "notified_on": "2016-04-06",
                        "nationality": "British",
                        "date_of_birth": {"year": 1970, "month": 1},
                        "address": {"address_line_1": "3 Owner Lane", "locality": "London", "country": "England"},
                    }
                ],
                "total_results": 1,
            },
            "related_companies": {},
        }
    },
}

# --- GLEIF: entity with direct parent ---

_GLEIF_WITH_PARENT = {
    "lei": "213800LBDB8WB3QGVN21",
    "record": {
        "id": "213800LBDB8WB3QGVN21",
        "attributes": {
            "lei": "213800LBDB8WB3QGVN21",
            "entity": {
                "legalName": {"name": "TEST GMBH"},
                "jurisdiction": "DE",
                "registeredAs": "HRB 12345",
                "registeredAt": {"id": "RA000561", "other": None},
                "legalAddress": {
                    "addressLines": ["Musterstrasse 1"],
                    "city": "Berlin",
                    "postalCode": "10115",
                    "country": "DE",
                },
            },
        },
    },
    "direct_parent": {
        "id": "PARENTXXXXXXXXXXXXXX",
        "attributes": {
            "lei": "PARENTXXXXXXXXXXXXXX",
            "entity": {
                "legalName": {"name": "PARENT HOLDING AG"},
                "jurisdiction": "DE",
            },
        },
    },
    "ultimate_parent": None,
    "direct_parent_exception": None,
    "ultimate_parent_exception": None,
    "direct_children": [],
}

# --- INPI: dirigeant with beneficiaireEffectif=False ---

_INPI_WITH_DIRIGEANT = {
    "source_id": "inpi",
    "siren": "055804124",
    "is_stub": False,
    "company": {
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
                "natureCreation": {"dateCreation": "1906-07-07"},
            },
        },
    },
}

# --- Brreg: company with board member ---

_BRREG_WITH_ROLE = {
    "source_id": "brreg",
    "orgnr": "923609016",
    "entity": {
        "organisasjonsnummer": "923609016",
        "navn": "TEST AS",
        "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
        "stiftelsesdato": "2000-01-01",
        "forretningsadresse": {
            "adresse": ["Testveien 1"],
            "postnummer": "0100",
            "poststed": "OSLO",
            "landkode": "NO",
            "land": "Norge",
        },
    },
    "roles": [
        {
            "type": {"kode": "LEDE", "beskrivelse": "Styreleder"},
            "person": {
                "fornavn": "Ola",
                "mellomnavn": "",
                "etternavn": "Nordmann",
                "fodselsdato": "1970-01-01",
                "erDød": False,
            },
            "fratreden": None,
            "_group_type": {"kode": "STYR", "beskrivelse": "Styre"},
        }
    ],
    "legal_name": "TEST AS",
    "is_stub": False,
}

# --- UR Latvia: entity with beneficial owner and officer ---

_UR_LATVIA_WITH_PERSONS = {
    "source_id": "ur_latvia",
    "hit_id": "40003521407",
    "lv_regcode": "40003521407",
    "legal_name": "TEST SIA",
    "entity": {
        "name": "TEST SIA",
        "regNumber": "40003521407",
        "registered": "2000-01-15",
        "type": "SIA",
        "status": "Reģistrēts",
        "address": "Brīvības iela 1, Rīga, LV-1001",
        "sepa": "",
    },
    "historical_names": [],
    "beneficial_owners": [
        {
            "name": "Jānis Bērziņš",
            "share_percent": "51",
            "country": "LV",
            "from_date": "2000-01-15",
            "type": "person",
        }
    ],
    "officers": [
        {
            "name": "Anna Kalniņa",
            "role": "valdes priekšsēdētājs",
            "from_date": "2010-03-01",
            "type": "person",
        }
    ],
    "members": [],
    "is_stub": False,
}

# --- Ariregister: company with shareholder and board member ---

_ARIREGISTER_WITH_PERSONS = {
    "source_id": "ariregister",
    "registry_code": "14064835",
    "name": "TEST OÜ",
    "legal_form": "Osaühing",
    "vat_number": None,
    "status": "R",
    "registration_date": "2000-01-01",
    "address": "Testitänaval 1, 10001 Tallinn, Harjumaa",
    "link": "https://ariregister.rik.ee/est/company/14064835",
    "shareholders": [
        {
            "name": "Jaan Tamm",
            "share_percent": "60",
            "shareholder_type": "person",
            "from_date": "2010-01-01",
            "country": "EE",
        }
    ],
    "officers": [
        {
            "name": "Mari Mägi",
            "role": "juhatuse liige",
            "from_date": "2015-06-01",
        }
    ],
    "beneficial_owners": [],
    "is_stub": False,
}

# --- Corporations Canada: company with directors ---

_CORPS_CANADA_WITH_DIRECTOR = {
    "source_id": "corporations_canada",
    "corp_id": "1007",
    "corporation": {
        "corporationId": "1007",
        "legalName": "Abbotsford Chamber of Commerce",
        "corporationNames": [
            {
                "legalName": "Abbotsford Chamber of Commerce",
                "nameTypeCd": "LN",
                "endEventId": None,
            }
        ],
        "status": "Active",
        "businessNumber": "106679285",
        "corporationType": {"desc": "Business Corporation", "cd": "A"},
        "incorporationDate": "1947-01-10",
        "offices": [
            {
                "officeType": "registeredOffice",
                "deliveryAddress": {
                    "streetAddress": "123 Test St",
                    "addressCity": "Abbotsford",
                    "addressRegion": "BC",
                    "postalCode": "V2S 6H1",
                    "addressCountry": "CA",
                },
            }
        ],
    },
    "directors": [
        {
            "firstName": "Jane",
            "lastName": "Smith",
            "roles": [{"roleType": "Director", "appointmentDate": "2015-01-01"}],
            "deliveryAddress": {
                "streetAddress": "456 Oak Ave",
                "addressCity": "Abbotsford",
                "addressRegion": "BC",
                "postalCode": "V2S 1A1",
                "addressCountry": "CA",
            },
        },
        {
            "firstName": "Bob",
            "lastName": "Jones",
            "roles": [{"roleType": "Director", "appointmentDate": "2018-06-01"}],
            "deliveryAddress": None,
        },
    ],
    "legal_name": "Abbotsford Chamber of Commerce",
    "is_stub": False,
}

# --- Firmenbuch: uses XML-parsed extract via test helper ---

def _build_firmenbuch_bundle() -> dict:
    """Build a Firmenbuch bundle using the real XML parser from the test file."""
    from test_bods_firmenbuch import _GMBH_EXTRACT  # type: ignore
    return {
        "source_id": "firmenbuch",
        "fn": "473888w",
        "extract": _GMBH_EXTRACT,
        "legal_name": "",
        "is_stub": False,
    }


# ---------------------------------------------------------------------------
# Parametrized connectivity tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("label,mapper_path,bundle_factory", [
    (
        "companies_house/individual_psc",
        "opencheck.bods.mapper.map_companies_house",
        lambda: _CH_INDIVIDUAL_PSC,
    ),
    (
        "companies_house/corporate_psc_chain",
        "opencheck.bods.mapper.map_companies_house",
        lambda: _CH_CORPORATE_PSC,
    ),
    (
        "gleif/with_direct_parent",
        "opencheck.bods.mapper.map_gleif",
        lambda: _GLEIF_WITH_PARENT,
    ),
    (
        "inpi/with_dirigeant",
        "opencheck.bods.mapper.map_inpi",
        lambda: _INPI_WITH_DIRIGEANT,
    ),
    (
        "brreg/with_board_member",
        "opencheck.bods.mapper.map_brreg",
        lambda: _BRREG_WITH_ROLE,
    ),
    (
        "ur_latvia/with_bo_and_officer",
        "opencheck.bods.mapper.map_ur_latvia",
        lambda: _UR_LATVIA_WITH_PERSONS,
    ),
    (
        "ariregister/with_shareholder_and_officer",
        "opencheck.bods.mapper.map_ariregister",
        lambda: _ARIREGISTER_WITH_PERSONS,
    ),
    (
        "corporations_canada/with_directors",
        "opencheck.bods.mapper.map_corporations_canada",
        lambda: _CORPS_CANADA_WITH_DIRECTOR,
    ),
    (
        "firmenbuch/gmbh_with_officers_and_shareholders",
        "opencheck.bods.mapper.map_firmenbuch",
        _build_firmenbuch_bundle,
    ),
])
def test_no_dangling_references(label, mapper_path, bundle_factory):
    """No relationship statement has a subject/interestedParty that is absent from the bundle."""
    import importlib
    module_path, func_name = mapper_path.rsplit(".", 1)
    mapper = getattr(importlib.import_module(module_path), func_name)
    stmts = to_stmts(mapper(bundle_factory()))
    issues = check_connectivity(stmts)
    assert issues == [], f"[{label}] Connectivity issues:\n" + "\n".join(issues)


@pytest.mark.parametrize("label,mapper_path,bundle_factory", [
    (
        "companies_house/individual_psc",
        "opencheck.bods.mapper.map_companies_house",
        lambda: _CH_INDIVIDUAL_PSC,
    ),
    (
        "inpi/with_dirigeant",
        "opencheck.bods.mapper.map_inpi",
        lambda: _INPI_WITH_DIRIGEANT,
    ),
    (
        "brreg/with_board_member",
        "opencheck.bods.mapper.map_brreg",
        lambda: _BRREG_WITH_ROLE,
    ),
    (
        "ur_latvia/with_bo_and_officer",
        "opencheck.bods.mapper.map_ur_latvia",
        lambda: _UR_LATVIA_WITH_PERSONS,
    ),
    (
        "ariregister/with_shareholder_and_officer",
        "opencheck.bods.mapper.map_ariregister",
        lambda: _ARIREGISTER_WITH_PERSONS,
    ),
    (
        "corporations_canada/with_directors",
        "opencheck.bods.mapper.map_corporations_canada",
        lambda: _CORPS_CANADA_WITH_DIRECTOR,
    ),
    (
        "firmenbuch/gmbh_with_officers_and_shareholders",
        "opencheck.bods.mapper.map_firmenbuch",
        _build_firmenbuch_bundle,
    ),
])
def test_interest_types_are_valid(label, mapper_path, bundle_factory):
    """All interest type codes are valid BODS v0.4 codelist members."""
    import importlib
    module_path, func_name = mapper_path.rsplit(".", 1)
    mapper = getattr(importlib.import_module(module_path), func_name)
    stmts = to_stmts(mapper(bundle_factory()))
    invalid = check_interest_types(stmts)
    assert invalid == [], f"[{label}] Invalid interest types:\n" + "\n".join(invalid)


# ---------------------------------------------------------------------------
# isComponent documentation tests
# (These document the current state; they will need updating when
#  isComponent/componentRecords fixes are applied in Phase 2.)
# ---------------------------------------------------------------------------


def test_ch_corporate_psc_intermediary_iscomponent_currently_false():
    """Document: intermediate entity in a CH corporate PSC chain currently has
    isComponent=False. This is a known BODS compliance gap — it should be True.
    This test PASSES now (documenting the bug) and should be FLIPPED to assert
    True once the fix is applied."""
    from opencheck.bods.mapper import map_companies_house
    stmts = to_stmts(map_companies_house(_CH_CORPORATE_PSC))

    # The holding company (12345678) is an intermediary; currently isComponent=False.
    entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
    assert len(entity_stmts) == 2, f"Expected 2 entity stmts, got {len(entity_stmts)}"

    is_component_values = {
        s["recordDetails"].get("isComponent") for s in entity_stmts
    }
    # Currently all False — document this:
    assert is_component_values == {False}, (
        "isComponent values changed — update this test and the BODS compliance plan"
    )


def test_ch_corporate_psc_no_component_records_currently():
    """Document: primary relationship in a CH corporate PSC chain currently has
    no componentRecords. This is a known BODS compliance gap."""
    from opencheck.bods.mapper import map_companies_house
    stmts = to_stmts(map_companies_house(_CH_CORPORATE_PSC))

    rel_stmts = [s for s in stmts if s["recordType"] == "relationship"]
    # None should have componentRecords currently:
    stmts_with_component_records = [
        s for s in rel_stmts
        if "componentRecords" in (s.get("recordDetails") or {})
    ]
    assert stmts_with_component_records == [], (
        "componentRecords appeared — update this test and remove the compliance gap note"
    )


# ---------------------------------------------------------------------------
# INPI security invariant: beneficiaireEffectif=True must never be emitted
# ---------------------------------------------------------------------------

def test_inpi_bo_record_never_emitted():
    """INPI: pouvoirs with beneficiaireEffectif=True must produce zero statements.
    Required by Loi Sapin II / décret 2017-1094."""
    from opencheck.bods.mapper import map_inpi

    bundle = {
        "source_id": "inpi",
        "siren": "055804124",
        "is_stub": False,
        "company": {
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
                                    # This IS a BO record — MUST be silently skipped
                                    "typeDePersonne": "INDIVIDU",
                                    "beneficiaireEffectif": True,
                                    "individu": {
                                        "descriptionPersonne": {
                                            "nom": "SECRET",
                                            "prenoms": ["OWNER"],
                                            "nationalite": "Française",
                                            "roleEntreprise": 53,
                                        }
                                    },
                                }
                            ]
                        },
                    },
                    "natureCreation": {"dateCreation": "1906-07-07"},
                },
            },
        },
    }
    stmts = to_stmts(map_inpi(bundle))
    person_stmts = [s for s in stmts if s["recordType"] == "person"]
    rel_stmts = [s for s in stmts if s["recordType"] == "relationship"]

    assert person_stmts == [], (
        f"SECURITY VIOLATION: INPI BO person statement was emitted: {person_stmts}"
    )
    assert rel_stmts == [], (
        f"SECURITY VIOLATION: INPI BO relationship statement was emitted: {rel_stmts}"
    )
