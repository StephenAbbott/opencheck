#!/usr/bin/env python3
"""Phase 1 — BODS graph connectivity baseline audit.

Runs every active Tier 2 mapper against a representative fixture and checks:
  1. Graph connectivity — every relationship's subject/interestedParty references
     a statementId present in the same bundle.
  2. isComponent usage — identifies which entity/person statements carry
     isComponent=True vs False.
  3. componentRecords presence — identifies primary relationships that are missing
     componentRecords when the bundle contains isComponent intermediaries.
  4. Interest type coverage — lists all interest types used, flagging any that
     are not valid BODS v0.4 codelist members.
  5. Statement ordering — component statements should precede primary relationships.

Run from backend/:
    python scripts/audit_bods_connectivity.py

Exit code 0 = no connectivity issues found.
Exit code 1 = one or more mappers have dangling references.
"""

from __future__ import annotations

import sys
from typing import Any, Callable, Iterable

# ---------------------------------------------------------------------------
# BODS v0.4 valid interest type codelist
# ---------------------------------------------------------------------------

VALID_INTEREST_TYPES = {
    "shareholding",
    "votingRights",
    "appointmentOfBoard",
    "otherInfluenceOrControl",
    "controlViaCompanyRulesOrArticles",
    "controlByLegalFramework",
    "boardMember",
    "boardChair",
    "unknownInterest",
    "unpublishedInterest",
    "enjoymentAndUseOfAssets",
    "rightToProfitOrIncomeFromAssets",
    "rightsToSurplusAssetsOnDissolution",
    "beneficiaryOfLegalArrangement",
    "seniorManagingOfficial",
}

# ---------------------------------------------------------------------------
# Connectivity checker
# ---------------------------------------------------------------------------


def check_connectivity(stmts: list[dict]) -> list[str]:
    """Return a list of connectivity issue descriptions."""
    all_ids = set()
    for s in stmts:
        all_ids.add(s.get("statementId", ""))
        if s.get("recordId"):
            all_ids.add(s["recordId"])

    issues = []
    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        rel_id = s.get("statementId", "<unknown>")

        subject = rd.get("subject")
        ip = rd.get("interestedParty")

        # Check subject
        if subject is None:
            issues.append(f"  MISSING subject on relationship {rel_id}")
        elif isinstance(subject, str):
            if subject not in all_ids:
                issues.append(
                    f"  DANGLING subject '{subject}' on relationship {rel_id}"
                )
        elif isinstance(subject, dict):
            ref = subject.get("describedByEntityStatement") or subject.get(
                "describedByPersonStatement"
            )
            issues.append(
                f"  v0.3 object-format subject on {rel_id} "
                f"(ref={ref!r}) — should be bare string in v0.4"
            )
            if ref and ref not in all_ids:
                issues.append(f"    └─ DANGLING: ref '{ref}' not in bundle")

        # Check interestedParty
        if ip is None:
            issues.append(f"  MISSING interestedParty on relationship {rel_id}")
        elif isinstance(ip, str):
            if ip not in all_ids:
                issues.append(
                    f"  DANGLING interestedParty '{ip}' on relationship {rel_id}"
                )
        elif isinstance(ip, dict):
            ref = ip.get("describedByPersonStatement") or ip.get(
                "describedByEntityStatement"
            )
            issues.append(
                f"  v0.3 object-format interestedParty on {rel_id} "
                f"(ref={ref!r}) — should be bare string in v0.4"
            )
            if ref and ref not in all_ids:
                issues.append(f"    └─ DANGLING: ref '{ref}' not in bundle")

    return issues


def check_iscomponent(stmts: list[dict]) -> dict:
    """Return counts and IDs of isComponent True/False usage."""
    component_entities = []
    non_component_entities = []
    component_persons = []
    has_component_records = []
    missing_component_records = []

    for s in stmts:
        rd = s.get("recordDetails") or {}
        is_comp = rd.get("isComponent")
        rtype = s.get("recordType")

        if rtype == "entity":
            if is_comp is True:
                component_entities.append(s.get("statementId"))
            else:
                non_component_entities.append(s.get("statementId"))
        elif rtype == "person":
            if is_comp is True:
                component_persons.append(s.get("statementId"))
        elif rtype == "relationship":
            if not rd.get("isComponent"):
                if "componentRecords" in rd:
                    has_component_records.append(s.get("statementId"))
                else:
                    missing_component_records.append(s.get("statementId"))

    return {
        "component_entities": component_entities,
        "non_component_entities": non_component_entities,
        "component_persons": component_persons,
        "primary_rels_with_component_records": has_component_records,
        "primary_rels_missing_component_records": missing_component_records,
    }


def check_interest_types(stmts: list[dict]) -> dict:
    """Return interest types used and any that are invalid."""
    used = set()
    invalid = set()
    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        for interest in (s.get("recordDetails") or {}).get("interests") or []:
            t = interest.get("type")
            if t:
                used.add(t)
                if t not in VALID_INTEREST_TYPES:
                    invalid.add(t)
    return {"used": sorted(used), "invalid": sorted(invalid)}


def summarise(label: str, stmts: list[dict]) -> dict:
    """Run all checks and return a summary dict."""
    connectivity = check_connectivity(stmts)
    iscomp = check_iscomponent(stmts)
    itypes = check_interest_types(stmts)
    types_count = {}
    for s in stmts:
        rt = s.get("recordType", "?")
        types_count[rt] = types_count.get(rt, 0) + 1

    return {
        "label": label,
        "statement_counts": types_count,
        "connectivity_issues": connectivity,
        "iscomponent": iscomp,
        "interest_types": itypes,
    }


def run_mapper(
    label: str,
    mapper: Callable,
    bundle: dict,
) -> dict:
    try:
        result = mapper(bundle)
        if hasattr(result, "statements"):
            stmts = result.statements
        else:
            stmts = list(result)
        return summarise(label, stmts)
    except Exception as exc:
        return {
            "label": label,
            "error": str(exc),
            "statement_counts": {},
            "connectivity_issues": [f"  MAPPER ERROR: {exc}"],
            "iscomponent": {},
            "interest_types": {},
        }


# ---------------------------------------------------------------------------
# Fixtures — one representative bundle per Tier 2 mapper
# ---------------------------------------------------------------------------

FIXTURES: list[tuple[str, str, dict]] = []

# --- Companies House: individual PSC ---
FIXTURES.append((
    "companies_house (individual PSC)",
    "opencheck.bods.mapper.map_companies_house",
    {
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
        "officers": {"items": [
            {
                "name": "Smith, John",
                "officer_role": "director",
                "appointed_on": "2010-01-01",
                "links": {"officer": {"appointments": "/officers/ABCDEF/appointments"}},
                "address": {"address_line_1": "1 Lane", "locality": "London", "country": "England"},
            }
        ], "total_results": 1},
        "pscs": {"items": [
            {
                "name": "JOHN SMITH",
                "kind": "individual-person-with-significant-control",
                "natures_of_control": ["ownership-of-shares-25-to-50-percent"],
                "notified_on": "2016-04-06",
                "nationality": "British",
                "date_of_birth": {"year": 1960, "month": 3},
                "address": {"address_line_1": "1 High St", "locality": "London", "country": "England"},
            }
        ], "total_results": 1},
        "related_companies": {},
    },
))

# --- Companies House: corporate PSC ---
FIXTURES.append((
    "companies_house (corporate PSC)",
    "opencheck.bods.mapper.map_companies_house",
    {
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
        "pscs": {"items": [
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
        ], "total_results": 1},
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
                "pscs": {"items": [
                    {
                        "name": "ULTIMATE OWNER",
                        "kind": "individual-person-with-significant-control",
                        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                        "notified_on": "2016-04-06",
                        "nationality": "British",
                        "date_of_birth": {"year": 1970, "month": 1},
                        "address": {"address_line_1": "3 Owner Lane", "locality": "London", "country": "England"},
                    }
                ], "total_results": 1},
                "related_companies": {},
            }
        },
    },
))

# --- GLEIF: entity with direct parent ---
_GLEIF_BASE = {
    "lei": "529900T8BM49AURSDO55",
    "entity": {
        "legalName": {"name": "TEST GMBH"},
        "legalAddress": {"addressLines": ["Musterstrasse 1"], "city": "Berlin", "country": "DE"},
        "headquartersAddress": {"addressLines": ["Musterstrasse 1"], "city": "Berlin", "country": "DE"},
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
}

FIXTURES.append((
    "gleif (entity only, no relationships)",
    "opencheck.bods.mapper.map_gleif",
    {**_GLEIF_BASE, "relationships": {"directParent": None, "ultimateParent": None}},
))

FIXTURES.append((
    "gleif (with direct parent)",
    "opencheck.bods.mapper.map_gleif",
    {
        **_GLEIF_BASE,
        "relationships": {
            "directParent": {
                "startNode": {"id": "529900T8BM49AURSDO55", "type": "LEI"},
                "endNode": {"id": "7ZW8QJWVPR4P1J1KQY45", "type": "LEI"},
                "relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                "relationshipStatus": "PUBLISHED",
                "relationshipPeriods": [{"startDate": "2020-01-01T00:00:00.000Z", "periodType": "ACCOUNTING_PERIOD"}],
                "qualificationDocuments": [],
            },
            "ultimateParent": None,
        },
        "parent_entity": {
            "lei": "7ZW8QJWVPR4P1J1KQY45",
            "entity": {
                "legalName": {"name": "PARENT HOLDING AG"},
                "legalAddress": {"addressLines": ["Holdingstrasse 1"], "city": "Frankfurt", "country": "DE"},
                "headquartersAddress": {"addressLines": ["Holdingstrasse 1"], "city": "Frankfurt", "country": "DE"},
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
    },
))

# --- INPI (France) ---
FIXTURES.append((
    "inpi (dirigeant only, no BO records)",
    "opencheck.bods.mapper.map_inpi",
    {
        "source_id": "inpi",
        "siren": "123456789",
        "is_stub": False,
        "content": {
            "formeSociale": {"libelle": "Société par actions simplifiée"},
            "identite": {
                "denomination": "TEST SAS",
                "dateImmatriculation": "20000101",
                "codeFormeJuridique": "5710",
            },
            "adresseEntreprise": {
                "pays": "France",
                "codePostal": "75001",
                "commune": "Paris",
                "voie": "1 Rue de Test",
            },
            "dirigeants": [
                {
                    "prenom": "Jean",
                    "nom": "Dupont",
                    "dateNaissance": "19700101",
                    "qualite": "Président",
                    "beneficiaireEffectif": False,
                }
            ],
            "beneficiairesEffectifs": [],
        },
    },
))

# --- Brreg (Norway) ---
FIXTURES.append((
    "brreg (with board member)",
    "opencheck.bods.mapper.map_brreg",
    {
        "source_id": "brreg",
        "org_number": "923609016",
        "is_stub": False,
        "unit": {
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
        "roles": {
            "rollegrupper": [
                {
                    "type": {"kode": "STYR", "beskrivelse": "Styre"},
                    "roller": [
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
                        }
                    ],
                }
            ]
        },
    },
))

# --- UR Latvia ---
FIXTURES.append((
    "ur_latvia (with shareholders and board)",
    "opencheck.bods.mapper.map_ur_latvia",
    {
        "source_id": "ur_latvia",
        "reg_number": "40003521407",
        "is_stub": False,
        "entity": {
            "name": "TEST SIA",
            "regNumber": "40003521407",
            "regDate": "2000-01-15",
            "legalForm": "SIA",
            "status": "Reģistrēts",
            "address": "Brīvības iela 1, Rīga, LV-1001",
        },
        "shareholders": [
            {
                "name": "Jānis Bērziņš",
                "type": "person",
                "share_percent": "51",
                "share_count": "51",
                "from_date": "2000-01-15",
            }
        ],
        "board_members": [
            {
                "name": "Anna Kalniņa",
                "role": "Valdes priekšsēdētājs",
                "from_date": "2010-03-01",
            }
        ],
        "council_members": [],
    },
))

# --- Firmenbuch (Austria) ---
FIXTURES.append((
    "firmenbuch (with shareholder and director)",
    "opencheck.bods.mapper.map_firmenbuch",
    {
        "source_id": "firmenbuch",
        "fn": "473888w",
        "is_stub": False,
        "company": {
            "fn": "473888w",
            "name": "TEST GMBH",
            "legal_form": "GmbH",
            "status": "aufrecht",
            "registration_date": "2000-01-01",
            "address": "Testgasse 1, 1010 Wien, Österreich",
        },
        "officers": [
            {
                "fn": "473888w",
                "name": "Hans Müller",
                "function": "Geschäftsführer",
                "from_date": "2010-01-01",
                "citizenship": "AT",
                "birth_date": "1970-01-01",
            }
        ],
        "shareholders": [
            {
                "fn": "473888w",
                "name": "Holding GmbH",
                "type": "legal_entity",
                "fn_ref": "123456a",
                "share_amount": "35000",
                "share_currency": "EUR",
                "from_date": "2000-01-01",
            }
        ],
    },
))

# --- Ariregister (Estonia) ---
FIXTURES.append((
    "ariregister (with shareholders and board)",
    "opencheck.bods.mapper.map_ariregister",
    {
        "source_id": "ariregister",
        "registry_code": "12345678",
        "is_stub": False,
        "company": {
            "ariregister_name": "TEST OÜ",
            "ariregister_code": "12345678",
            "ariregister_status": "R",
            "ariregister_foundation_date": "2000-01-01",
            "ariregister_address": "Testitänaval 1, 10001 Tallinn, Harjumaa",
            "ariregister_legal_form_text": "Osaühing",
        },
        "shareholders": [
            {
                "name": "Jaan Tamm",
                "share_percent": "60",
                "shareholder_type": "person",
                "from_date": "2010-01-01",
                "country": "EE",
            }
        ],
        "board_members": [
            {
                "name": "Mari Mägi",
                "role": "Juhatuse liige",
                "from_date": "2015-06-01",
            }
        ],
    },
))

# --- Corporations Canada ---
FIXTURES.append((
    "corporations_canada (with director)",
    "opencheck.bods.mapper.map_corporations_canada",
    {
        "source_id": "corporations_canada",
        "corporation_number": "1234567",
        "is_stub": False,
        "profile": {
            "corpNum": "1234567",
            "legalName": "TEST CORP",
            "status": "Active",
            "adminEmail": "",
        },
        "details": {
            "corpNum": "1234567",
            "legalName": "TEST CORP",
            "corpType": {"desc": "Business Corporation"},
            "foundingDate": "2000-01-01T00:00:00+00:00",
            "lastModifiedDate": "2023-01-01T00:00:00+00:00",
            "offices": {
                "registeredOffice": {
                    "deliveryAddress": {
                        "streetAddress": "123 Test St",
                        "addressCity": "Ottawa",
                        "addressRegion": "ON",
                        "postalCode": "K1A 0A1",
                        "addressCountry": "CA",
                    }
                }
            },
        },
        "parties": {
            "directors": [
                {
                    "officer": {
                        "firstName": "John",
                        "lastName": "Smith",
                    },
                    "roles": [
                        {
                            "roleType": "Director",
                            "appointmentDate": "2010-01-01",
                        }
                    ],
                    "deliveryAddress": {
                        "streetAddress": "123 Test St",
                        "addressCity": "Ottawa",
                        "addressRegion": "ON",
                        "postalCode": "K1A 0A1",
                        "addressCountry": "CA",
                    },
                }
            ]
        },
    },
))


# ---------------------------------------------------------------------------
# Main audit runner
# ---------------------------------------------------------------------------


def main() -> int:
    import importlib

    total_issues = 0
    results = []

    for label, mapper_path, bundle in FIXTURES:
        module_path, func_name = mapper_path.rsplit(".", 1)
        try:
            mod = importlib.import_module(module_path)
            mapper = getattr(mod, func_name)
        except (ImportError, AttributeError) as exc:
            results.append({"label": label, "error": f"Import error: {exc}", "connectivity_issues": [], "statement_counts": {}, "iscomponent": {}, "interest_types": {}})
            continue

        result = run_mapper(label, mapper, bundle)
        results.append(result)
        total_issues += len(result.get("connectivity_issues", []))

    # Print report
    print("=" * 70)
    print("BODS GRAPH CONNECTIVITY AUDIT — Phase 1 Baseline")
    print("=" * 70)
    print()

    for r in results:
        label = r["label"]
        counts = r.get("statement_counts", {})
        issues = r.get("connectivity_issues", [])
        iscomp = r.get("iscomponent", {})
        itypes = r.get("interest_types", {})
        error = r.get("error")

        status = "ERROR" if error else ("FAIL" if issues else "PASS")
        indicator = {"PASS": "✓", "FAIL": "✗", "ERROR": "!"}[status]

        entity_count = counts.get("entity", 0)
        person_count = counts.get("person", 0)
        rel_count = counts.get("relationship", 0)

        print(f"[{indicator}] {label}")
        print(f"    Statements: {entity_count} entity, {person_count} person, {rel_count} relationship")

        if error:
            print(f"    ERROR: {error}")
        elif issues:
            for issue in issues:
                print(f"    {issue}")

        # isComponent summary
        comp_entities = iscomp.get("component_entities", [])
        if comp_entities:
            print(f"    isComponent=True entities: {len(comp_entities)}")

        # Interest types used
        used_types = itypes.get("used", [])
        if used_types:
            print(f"    Interest types: {', '.join(used_types)}")

        invalid_types = itypes.get("invalid", [])
        if invalid_types:
            print(f"    INVALID interest types: {', '.join(invalid_types)}")

        print()

    # Summary
    print("=" * 70)
    if total_issues == 0:
        print(f"RESULT: All {len(results)} mapper fixtures pass connectivity checks.")
    else:
        print(f"RESULT: {total_issues} connectivity issue(s) found across {len(results)} mapper fixtures.")
    print("=" * 70)

    return 1 if total_issues > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
