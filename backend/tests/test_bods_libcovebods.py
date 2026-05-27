"""Phase 41 — lib-cove-bods validation for all OpenCheck BODS mappers.

Validates that the BODS v0.4 statements produced by every mapper pass:
  1. JSON Schema validation (hard errors — data is non-conformant if any fail).
  2. BODS additional checks (soft quality checks) *excluding* the
     ``entity_identifiers_not_known_scheme`` advisory, which we track but do
     not fail on (some OpenCheck-internal schemes like ``EE-KMKR-HASH`` are
     intentionally non-standard).

Run with::

    pytest tests/test_bods_libcovebods.py -v

References:
  - https://github.com/openownership/lib-cove-bods
  - https://standard.openownership.org/en/0.4.0/
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any

import pytest

from libcovebods.config import LibCoveBODSConfig
from libcovebods.data_reader import DataReader
from libcovebods.jsonschemavalidate import JSONSchemaValidator
from libcovebods.schema import SchemaBODS
import libcovebods.run_tasks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_list(result: Any) -> list[dict[str, Any]]:
    """Accept both BODSBundle (has .statements) and raw generators."""
    if hasattr(result, "statements"):
        return result.statements
    return list(result)


def validate_bods_statements(statements: list[dict[str, Any]]) -> dict[str, Any]:
    """Run lib-cove-bods JSON schema + additional checks on *statements*.

    Returns a dict with keys:
      ``json_errors``        — list of hard JSON schema error dicts
      ``additional_errors``  — list of non-advisory additional check results
      ``unknown_schemes``    — scheme codes flagged by entity_identifiers_not_known_scheme
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as fh:
        json.dump(statements, fh)
        tmppath = fh.name

    try:
        config = LibCoveBODSConfig()
        schema = SchemaBODS(
            data_reader=DataReader(tmppath), lib_cove_bods_config=config
        )
        js_errors = JSONSchemaValidator(schema).validate(DataReader(tmppath))
        additional = libcovebods.run_tasks.process_additional_checks(
            DataReader(tmppath), config, schema
        )
    finally:
        os.unlink(tmppath)

    all_additional = additional["additional_checks"]
    unknown_schemes = [
        e["scheme"]
        for e in all_additional
        if e.get("type") == "entity_identifiers_not_known_scheme"
    ]
    # Non-advisory = everything except the unknown-scheme informational check
    additional_errors = [
        e
        for e in all_additional
        if e.get("type") != "entity_identifiers_not_known_scheme"
    ]

    return {
        "json_errors": [e.json() for e in js_errors],
        "additional_errors": additional_errors,
        "unknown_schemes": unknown_schemes,
    }


def assert_valid(result: Any, label: str = "") -> dict[str, Any]:
    """Map result → statements → validate → assert clean."""
    stmts = _to_list(result)
    report = validate_bods_statements(stmts)
    assert report["json_errors"] == [], (
        f"{label}: JSON schema errors: {report['json_errors']}"
    )
    assert report["additional_errors"] == [], (
        f"{label}: BODS additional check errors: {report['additional_errors']}"
    )
    return report


# ---------------------------------------------------------------------------
# Fixtures — minimal but representative bundles for each mapper
# ---------------------------------------------------------------------------

_CH_BUNDLE: dict[str, Any] = {
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
                "links": {"officer": {"appointments": "/officers/x/appointments"}},
                "address": {
                    "address_line_1": "1 Lane",
                    "locality": "London",
                    "country": "England",
                },
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
                "address": {
                    "address_line_1": "1 High Street",
                    "locality": "London",
                    "country": "England",
                },
            }
        ],
        "total_results": 1,
    },
    "related_companies": {},
}

_GLEIF_BUNDLE: dict[str, Any] = {
    "lei": "529900T8BM49AURSDO55",
    "entity": {
        "legalName": {"name": "TEST GMBH"},
        "legalAddress": {
            "addressLines": ["Musterstrasse 1"],
            "city": "Berlin",
            "country": "DE",
        },
        "headquartersAddress": {
            "addressLines": ["Musterstrasse 1"],
            "city": "Berlin",
            "country": "DE",
        },
        "registeredAt": {"id": "RA000561"},
        "registeredAs": "HRB 12345",
        "jurisdiction": "DE",
        "entityStatus": "ACTIVE",
        "legalForm": {"id": "8Z6G"},
    },
    "registration": {
        "initialRegistrationDate": "2014-01-14T00:00:00.000Z",
        "lastUpdateDate": "2023-06-15T00:00:00.000Z",
        "status": "ISSUED",
        "managingLou": "EVK05KS7XY1DEII3R011",
        "validationSources": "FULLY_CORROBORATED",
    },
    "relationships": {"directParent": None, "ultimateParent": None},
}

_OC_BUNDLE: dict[str, Any] = {
    "company": {
        "jurisdiction_code": "gb",
        "company_number": "00000006",
        "name": "Test Ltd",
        "company_type": "private-limited-company",
        "incorporation_date": "2000-01-01",
        "registered_address": {
            "street_address": "1 Test Road",
            "locality": "London",
            "country": "United Kingdom",
        },
        "officers": [
            {
                "officer": {
                    "id": 1234,
                    "name": "John Smith",
                    "role": "director",
                    "start_date": "2010-01-01",
                    "address": "1 Lane, London",
                }
            }
        ],
        "corporate_groupings": [],
    }
}


# ---------------------------------------------------------------------------
# Tests — one per mapper
# ---------------------------------------------------------------------------


def test_libcovebods_companies_house():
    from opencheck.bods.mapper import map_companies_house
    assert_valid(map_companies_house(_CH_BUNDLE), "Companies House")


def test_libcovebods_gleif():
    from opencheck.bods.mapper import map_gleif
    assert_valid(map_gleif(_GLEIF_BUNDLE), "GLEIF")


def test_libcovebods_opencorporates():
    from opencheck.bods.mapper import map_opencorporates
    assert_valid(map_opencorporates(_OC_BUNDLE), "OpenCorporates")


def test_libcovebods_kvk():
    from opencheck.bods.mapper import map_kvk
    bundle = {
        "kvkNummer": "12345678",
        "naam": "Test BV",
        "rechtsvorm": "BV",
        "datumOprichting": "2000-01-01",
        "adressen": [
            {
                "type": "bezoekadres",
                "straatnaam": "Teststraat",
                "huisnummer": "1",
                "plaats": "Amsterdam",
                "landCode": "NL",
            }
        ],
        "gemachtigden": [
            {
                "naam": "Jan de Vries",
                "functie": "Bestuurder",
                "geboortedatum": "1970-01-01",
                "handlichting": "Alleen/zelfstandig bevoegd",
            }
        ],
        "eigenaar": None,
        "aandeelhouders": [],
        "ubo_register": [],
    }
    assert_valid(map_kvk(bundle), "KvK")


def test_libcovebods_bolagsverket():
    from opencheck.bods.mapper import map_bolagsverket
    bundle = {
        "registrationNumber": "556000-0000",
        "name": "Test AB",
        "legalForm": {"code": "AB", "label": "Aktiebolag"},
        "registrationDate": "1990-01-01",
        "address": {
            "street": "Testgatan 1",
            "postalCode": "11122",
            "city": "Stockholm",
            "country": "Sverige",
        },
        "board": [
            {
                "personId": "19500101-1234",
                "name": "Lars Svensson",
                "role": "Styrelseledamot",
                "fromDate": "2015-01-01",
            }
        ],
        "shareholders": [],
        "beneficialOwners": [],
    }
    assert_valid(map_bolagsverket(bundle), "Bolagsverket")


def test_libcovebods_brreg():
    from opencheck.bods.mapper import map_brreg
    bundle = {
        "organisasjonsnummer": "923609016",
        "navn": "TEST AS",
        "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
        "stiftelsesdato": "2000-01-01",
        "forretningsadresse": {
            "adresse": ["Testveien 1"],
            "postnummer": "0100",
            "poststed": "OSLO",
            "landkode": "NO",
        },
        "rollegrupper": [
            {
                "type": {"kode": "STYR", "beskrivelse": "Styre"},
                "roller": [
                    {
                        "type": {"kode": "LEDE", "beskrivelse": "Styreleder"},
                        "person": {"fornavn": "Ola", "etternavn": "Nordmann"},
                    }
                ],
            }
        ],
        "relasjoner": [],
    }
    assert_valid(map_brreg(bundle), "Brreg")


def test_libcovebods_krs_poland():
    from opencheck.bods.mapper import map_krs_poland
    bundle = {
        "unit": {
            "krs": "0000000019",
            "name": "TEST SPOLKA Z O.O.",
            "form": {"name": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ"},
            "address": {
                "country": "POLSKA",
                "street": "ul. Testowa 1",
                "city": "Warszawa",
                "postalCode": "00-001",
            },
            "registrationDate": "2000-01-01",
            "nip": "1234567890",
            "representatives": [
                {
                    "firstName": "Jan",
                    "lastName": "Kowalski",
                    "function": "PREZES ZARZĄDU",
                }
            ],
            "shareholders": [],
        }
    }
    assert_valid(map_krs_poland(bundle), "KRS Poland")


def test_libcovebods_firmenbuch():
    from opencheck.bods.mapper import map_firmenbuch
    bundle = {
        "company_number": "FN123456a",
        "name": "Test GmbH",
        "legal_form": "GmbH",
        "date_of_foundation": "2000-01-01",
        "status": "active",
        "address": "Testgasse 1, 1010 Wien, Austria",
        "representatives": [
            {
                "function": "Geschäftsführer",
                "name": "Hans Müller",
                "since": "2010-01-01",
            }
        ],
        "shareholders": [],
    }
    assert_valid(map_firmenbuch(bundle), "Firmenbuch Austria")


def test_libcovebods_ares():
    from opencheck.bods.mapper import map_ares
    bundle = {
        "ico": "12345678",
        "obchodniJmeno": "Test s.r.o.",
        "pravniForma": {"kod": "112", "nazev": "Společnost s ručením omezeným"},
        "datumVznikuAZaniku": "2000-01-01",
        "sidlo": {
            "textovaAdresa": "Testová 1, 110 00 Praha 1",
            "stat": {"nazevStatu": "Česká republika", "kodStatu": "CZ"},
        },
        "zastupci": [
            {
                "jmeno": "Jan",
                "prijmeni": "Novák",
                "funkce": "jednatel",
                "datumVzniku": "2010-01-01",
            }
        ],
        "spolecnici": [],
    }
    assert_valid(map_ares(bundle), "ARES Czech")


def test_libcovebods_inpi():
    from opencheck.bods.mapper import map_inpi
    bundle = {
        "siren": "123456789",
        "uniteLegale": {
            "denominationUniteLegale": "Test SAS",
            "categorieJuridiqueUniteLegale": "5710",
            "activitePrincipaleUniteLegale": "6201Z",
        },
        "formeSociale": {"libelle": "Société par actions simplifiée"},
        "content": {
            "natureCreation": {"dateCreation": "2000-01-01"},
            "adresseEntreprise": {
                "adresse": {
                    "pays": "France",
                    "codePostal": "75001",
                    "commune": "Paris",
                    "voie": "1 Rue de Test",
                }
            },
            "dirigeants": [],
            "beneficiairesEffectifs": [],
        },
    }
    assert_valid(map_inpi(bundle), "INPI France")


def test_libcovebods_rpo_slovakia():
    from opencheck.bods.mapper import map_rpo_slovakia
    bundle = {
        "ico": "12345678",
        "nazovSubjektu": "Test s.r.o.",
        "formaSubjektu": {"id": "112", "value": "Spoločnosť s ručením obmedzeným"},
        "adresaSidla": {
            "ulica": "Testová",
            "cisloUlice": "1",
            "obec": "Bratislava",
            "stat": "SVK",
        },
        "datumVpisu": "2000-01-01",
        "osoby": [
            {
                "typ": "Konateľ",
                "meno": "Ján",
                "priezvisko": "Novák",
                "od": "2010-01-01",
            }
        ],
        "spolocnici": [],
    }
    assert_valid(map_rpo_slovakia(bundle), "RPO Slovakia")


def test_libcovebods_rpvs_slovakia():
    from opencheck.bods.mapper import map_rpvs_slovakia
    bundle = {
        "ico": "12345678",
        "nazov": "Test s.r.o.",
        "adresaSidla": {
            "ulica": "Testová",
            "cislo": "1",
            "obec": "Bratislava",
            "stat": "Slovenská republika",
        },
        "konecniUzivateliaVyhod": [
            {
                "meno": "Ján",
                "priezvisko": "Novák",
                "datumNarodenia": "1970-01-01",
                "statObcanstva": "SVK",
                "adresa": "Testová 1, Bratislava",
                "podiel": "100%",
            }
        ],
    }
    assert_valid(map_rpvs_slovakia(bundle), "RPVS Slovakia")


def test_libcovebods_bce_belgium():
    from opencheck.bods.mapper import map_bce_belgium
    bundle = {
        "enterpriseNumber": "0123456789",
        "name": "Test BVBA",
        "legalForm": {
            "code": "014",
            "label": "Besloten vennootschap met beperkte aansprakelijkheid",
        },
        "startDate": "2000-01-01",
        "status": "Active",
        "address": {
            "streetAndNumber": "Testlaan 1",
            "zipCode": "1000",
            "municipality": "Brussel",
            "country": "BE",
        },
        "contacts": [],
        "persons": [
            {
                "name": "Jan Janssen",
                "quality": "Zaakvoerder",
                "startDate": "2010-01-01",
            }
        ],
    }
    assert_valid(map_bce_belgium(bundle), "BCE Belgium")


def test_libcovebods_acra_singapore():
    from opencheck.bods.mapper import map_acra_singapore
    bundle = {
        "uen": "200000000N",
        "entity_name": "TEST PTE. LTD.",
        "entity_type": "BN",
        "uen_status": "Live",
        "reg_date": "2000-01-01",
        "address": {
            "block_house_number": "1",
            "street_name": "TEST ROAD",
            "postal_code": "123456",
            "country_desc": "SINGAPORE",
        },
        "officers": [
            {
                "person_name": "JOHN TAN",
                "position": "Director",
                "appointment_date": "2010-01-01",
            }
        ],
        "shareholders": [],
    }
    assert_valid(map_acra_singapore(bundle), "ACRA Singapore")


def test_libcovebods_ariregister():
    from opencheck.bods.mapper import map_ariregister
    bundle = {
        "registry_code": "12345678",
        "name": "Test OÜ",
        "status": "R",
        "address": "Testitänaval 1, 10001 Tallinn",
        "registration_date": "2000-01-01",
        "members": [
            {
                "name": "Jaan Tamm",
                "role": "JUHATUSE LIIGE",
                "start": "2010-01-01",
                "type": "natural_person",
            }
        ],
        "shareholders": [],
    }
    assert_valid(map_ariregister(bundle), "Ariregister Estonia")


def test_libcovebods_ur_latvia():
    from opencheck.bods.mapper import map_ur_latvia
    bundle = {
        "regCode": "12345678901",
        "name": "TEST SIA",
        "type": "SIA",
        "status": "Reģistrēts",
        "address": "Testela iela 1, Rīga, LV-1001",
        "regDate": "2000-01-01",
        "board": [
            {
                "name": "Jānis Bērziņš",
                "role": "Valdes loceklis",
                "from": "2010-01-01",
            }
        ],
        "shareholders": [],
    }
    assert_valid(map_ur_latvia(bundle), "UR Latvia")


def test_libcovebods_corporations_canada():
    from opencheck.bods.mapper import map_corporations_canada
    bundle = {
        "corporationNumber": "1234567",
        "legalName": "TEST CORP",
        "corporationType": "Business Corporation",
        "status": "Active",
        "incorporationDate": "2000-01-01",
        "offices": [
            {
                "officeType": "registeredOffice",
                "deliveryAddress": {
                    "streetAddress": "123 Test St",
                    "city": "Ottawa",
                    "province": "ON",
                    "postalCode": "K1A 0A1",
                    "country": "CA",
                },
            }
        ],
        "directors": [
            {"firstName": "John", "lastName": "Smith", "startDate": "2010-01-01"}
        ],
    }
    assert_valid(map_corporations_canada(bundle), "Corporations Canada")


def test_libcovebods_zefix():
    from opencheck.bods.mapper import map_zefix
    bundle = {
        "source_id": "zefix",
        "uid": "CHE313550547",
        "company": {
            "name": "Test AG",
            "uid": "CHE-313.550.547",
            "canton": "ZH",
            "ehraid": 99999,
            "status": {"shortNameDe": "ACTIVE"},
            "legalForm": {"shortNameDe": "AG"},
            "address": {
                "street": "Teststrasse",
                "houseNumber": "1",
                "postalCode": "8000",
                "city": "Zurich",
            },
            "zefixDetailWeb": {
                "en": "https://www.zefix.ch/en/search/entity/list/firm/99999"
            },
        },
        "is_stub": False,
    }
    assert_valid(map_zefix(bundle), "Zefix Switzerland")


def test_libcovebods_jar_lithuania():
    from opencheck.bods.mapper import map_jar_lithuania
    bundle = {
        "jarCode": "123456789",
        "name": "TEST UAB",
        "legalForm": {"name": "Uždaroji akcinė bendrovė"},
        "registrationDate": "2000-01-01",
        "status": "Įregistruotas",
        "address": {
            "country": "LT",
            "municipality": "Vilnius",
            "street": "Testinė g.",
            "house": "1",
        },
        "managers": [
            {
                "firstName": "Jonas",
                "lastName": "Jonaitis",
                "position": "Direktorius",
                "from": "2010-01-01",
            }
        ],
        "shareholders": [],
    }
    assert_valid(map_jar_lithuania(bundle), "JAR Lithuania")


def test_libcovebods_cro_ireland():
    from opencheck.bods.mapper import map_cro
    bundle = {
        "company_number": "123456",
        "company_name": "TEST LIMITED",
        "company_type": "Private Company Limited by Shares",
        "company_status": "Normal",
        "incorporation_date": "2000-01-01",
        "registered_address": "1 Test Road, Dublin 2, D02 T123, Ireland",
        "officers": [
            {
                "officer_name": "John Murphy",
                "role": "Director",
                "date_of_appointment": "2010-01-01",
            }
        ],
        "shareholders": [],
    }
    assert_valid(map_cro(bundle), "CRO Ireland")


def test_libcovebods_sec_edgar():
    from opencheck.bods.mapper import map_sec_edgar
    bundle = {
        "cik": "0001318605",
        "name": "Tesla, Inc.",
        "sic": "3711",
        "sic_description": "Motor Vehicles & Passenger Car Bodies",
        "ein": "912197729",
        "state_of_inc": "DE",
        "fiscal_year_end": "1231",
        "address": {
            "mailing": {
                "street1": "1 Tesla Rd",
                "city": "Austin",
                "stateOrCountry": "TX",
                "zipCode": "78725",
                "stateOrCountryDescription": "TX",
            }
        },
        "tickers": [{"ticker": "TSLA", "exchange": "Nasdaq"}],
        "officers": [
            {
                "name": "Elon Musk",
                "title": "Chief Executive Officer",
                "since": "2018-01-01",
            }
        ],
        "filings": [],
    }
    assert_valid(map_sec_edgar(bundle), "SEC EDGAR")


def test_libcovebods_gleif_with_parent_relationship():
    """GLEIF with parent → ownership-or-control statements must also validate."""
    from opencheck.bods.mapper import map_gleif
    bundle = {
        **_GLEIF_BUNDLE,
        "relationships": {
            "directParent": {
                "startNode": {"id": "529900T8BM49AURSDO55", "type": "LEI"},
                "endNode": {"id": "7ZW8QJWVPR4P1J1KQY45", "type": "LEI"},
                "relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                "relationshipStatus": "PUBLISHED",
                "relationshipPeriods": [
                    {
                        "startDate": "2020-01-01T00:00:00.000Z",
                        "periodType": "ACCOUNTING_PERIOD",
                    }
                ],
                "qualificationDocuments": [],
            },
            "ultimateParent": None,
        },
        "parent_entity": {
            "lei": "7ZW8QJWVPR4P1J1KQY45",
            "entity": {
                "legalName": {"name": "PARENT HOLDING AG"},
                "legalAddress": {
                    "addressLines": ["Holdingstrasse 1"],
                    "city": "Frankfurt",
                    "country": "DE",
                },
                "headquartersAddress": {
                    "addressLines": ["Holdingstrasse 1"],
                    "city": "Frankfurt",
                    "country": "DE",
                },
                "registeredAt": {"id": "RA000561"},
                "registeredAs": "HRB 99999",
                "jurisdiction": "DE",
                "entityStatus": "ACTIVE",
                "legalForm": {"id": "8Z6G"},
            },
            "registration": {
                "initialRegistrationDate": "2010-01-01T00:00:00.000Z",
                "lastUpdateDate": "2023-01-01T00:00:00.000Z",
                "status": "ISSUED",
                "managingLou": "EVK05KS7XY1DEII3R011",
                "validationSources": "FULLY_CORROBORATED",
            },
        },
    }
    assert_valid(map_gleif(bundle), "GLEIF with parent")


def test_libcovebods_companies_house_corporate_psc():
    """CH bundle with a corporate PSC (entity→entity OCS) must validate."""
    from opencheck.bods.mapper import map_companies_house
    bundle = {
        **_CH_BUNDLE,
        "pscs": {
            "items": [
                {
                    "name": "HOLDING COMPANY LTD",
                    "kind": "corporate-entity-person-with-significant-control",
                    "natures_of_control": [
                        "ownership-of-shares-75-to-100-percent"
                    ],
                    "notified_on": "2016-04-06",
                    "identification": {
                        "registration_number": "07654321",
                        "country_registered": "England",
                        "place_registered": "Companies House",
                    },
                    "address": {
                        "address_line_1": "2 Corp Road",
                        "locality": "London",
                        "country": "England",
                    },
                }
            ],
            "total_results": 1,
        },
    }
    assert_valid(map_companies_house(bundle), "Companies House (corporate PSC)")
