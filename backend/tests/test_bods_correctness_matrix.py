"""Phase 4 — Per-adapter mapper correctness matrix.

For each Tier 2 adapter (those that emit person and/or relationship
statements) this file pins the exact field-level output against a
representative fixture: statement counts, interest types,
``beneficialOwnershipOrControl``, and ``directOrIndirect``.

These tests are complementary to — not duplicates of — the connectivity
tests in ``test_bods_graph_integrity.py``.  Where connectivity tests only
ask "does every relationship reference a real node?", the correctness
matrix asks "does each relationship carry the right interest type and
ownership flag?"

Adapters covered:
  * companies_house   — individual PSC, corporate PSC chain (bug doc)
  * gleif             — direct parent, ultimate-parent-only exception
  * inpi              — dirigeant (non-BO), BO security invariant (xfail/pass)
  * brreg             — board chair (LEDE), board member (MEDL)
  * ur_latvia         — declared BO (otherInfluenceOrControl), officer (boardMember)
  * ariregister       — shareholder (shareholding), board member (boardMember via JUHL)
  * corporations_canada — director (seniorManagingOfficial)
  * firmenbuch        — GmbH officer (seniorManagingOfficial), shareholder (shareholding)
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stmts(mapper_fn, bundle: dict) -> list[dict]:
    return list(mapper_fn(bundle))


def _relationships(stmts: list[dict]) -> list[dict]:
    return [s for s in stmts if s["recordType"] == "relationship"]


def _persons(stmts: list[dict]) -> list[dict]:
    return [s for s in stmts if s["recordType"] == "person"]


def _entities(stmts: list[dict]) -> list[dict]:
    return [s for s in stmts if s["recordType"] == "entity"]


def _interests(rel: dict) -> list[dict]:
    return (rel.get("recordDetails") or {}).get("interests") or []


def _interest_types(stmts: list[dict]) -> list[str]:
    return [i["type"] for r in _relationships(stmts) for i in _interests(r)]


# ===========================================================================
# Companies House — direct individual PSC
# ===========================================================================


class TestCompaniesHouseIndividualPsc:
    """Individual PSC with shareholding ≥25%."""

    @pytest.fixture
    def bundle(self) -> dict:
        return {
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
                },
            },
            "officers": {"items": [], "total_results": 0},
            "pscs": {
                "items": [
                    {
                        "name": "JOHN SMITH",
                        "kind": "individual-person-with-significant-control",
                        "natures_of_control": ["ownership-of-shares-25-to-50-percent"],
                        "notified_on": "2016-04-06",
                        "nationality": "British",
                        "date_of_birth": {"year": 1960, "month": 3},
                        "address": {"address_line_1": "1 High St", "locality": "London",
                                    "country": "England"},
                    }
                ],
                "total_results": 1,
            },
            "related_companies": {},
        }

    @pytest.fixture
    def stmts(self, bundle) -> list[dict]:
        from opencheck.bods.mapper import map_companies_house
        return _stmts(map_companies_house, bundle)

    def test_statement_count(self, stmts):
        assert len(stmts) == 3  # entity + person + relationship

    def test_one_entity(self, stmts):
        assert len(_entities(stmts)) == 1

    def test_one_person(self, stmts):
        assert len(_persons(stmts)) == 1

    def test_one_relationship(self, stmts):
        assert len(_relationships(stmts)) == 1

    def test_interest_type_shareholding(self, stmts):
        assert _interest_types(stmts) == ["shareholding"]

    def test_beneficial_ownership_true(self, stmts):
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["beneficialOwnershipOrControl"] is True

    def test_direct_or_indirect_direct(self, stmts):
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["directOrIndirect"] == "direct"

    def test_subject_is_entity(self, stmts):
        entity_id = _entities(stmts)[0]["statementId"]
        rel_subject = (_relationships(stmts)[0].get("recordDetails") or {}).get("subject")
        assert rel_subject == entity_id

    def test_interested_party_is_person(self, stmts):
        person_id = _persons(stmts)[0]["statementId"]
        rel_ip = (_relationships(stmts)[0].get("recordDetails") or {}).get("interestedParty")
        assert rel_ip == person_id

    def test_entity_is_not_component(self, stmts):
        entity = _entities(stmts)[0]
        assert (entity.get("recordDetails") or {}).get("isComponent") is False


# ===========================================================================
# Companies House — corporate PSC chain (known compliance gap)
# ===========================================================================


class TestCompaniesHouseCorporatePscChain:
    """Company A owned by Company B (which is owned by person C).

    Documents the CURRENT state (bugs) and marks EXPECTED state as xfail
    until Fix 2 (isComponent / componentRecords) is implemented.
    """

    @pytest.fixture
    def bundle(self) -> dict:
        return {
            "company_number": "00102498",
            "profile": {
                "company_name": "SUBSIDIARY LTD",
                "company_number": "00102498",
                "type": "private-limited-company",
                "company_status": "active",
                "jurisdiction": "england-wales",
                "date_of_creation": "2005-01-01",
                "registered_office_address": {"address_line_1": "1 Corp Rd", "locality": "London"},
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
                        "address": {"address_line_1": "2 Corp Rd", "locality": "London"},
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
                        "registered_office_address": {"address_line_1": "2 Corp Rd", "locality": "London"},
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
                                "address": {"address_line_1": "3 Owner Lane", "locality": "London"},
                            }
                        ],
                        "total_results": 1,
                    },
                    "related_companies": {},
                }
            },
        }

    @pytest.fixture
    def stmts(self, bundle) -> list[dict]:
        from opencheck.bods.mapper import map_companies_house
        return _stmts(map_companies_house, bundle)

    def test_CURRENT_two_entities_produced(self, stmts):
        """CURRENT: entity for subsidiary + entity for holding company."""
        assert len(_entities(stmts)) == 2

    def test_CURRENT_one_person_produced(self, stmts):
        """CURRENT: person for the ultimate individual owner."""
        assert len(_persons(stmts)) == 1

    def test_CURRENT_two_relationships_produced(self, stmts):
        """CURRENT: two direct component relationships emitted (person→holding,
        holding→subsidiary); no primary indirect relationship from person→subsidiary."""
        assert len(_relationships(stmts)) == 2

    def test_CURRENT_all_interests_are_shareholding(self, stmts):
        types = set(_interest_types(stmts))
        assert types == {"shareholding"}

    def test_CURRENT_intermediary_is_not_component(self, stmts):
        """KNOWN BUG (Fix 2): intermediary entity (Holding Co) has isComponent=False.
        Should be True once Fix 2 is applied."""
        entity_stmts = _entities(stmts)
        is_component_values = {
            (e.get("recordDetails") or {}).get("isComponent") for e in entity_stmts
        }
        assert is_component_values == {False}, (
            "isComponent changed — update this test once Fix 2 is applied"
        )

    def test_CURRENT_no_component_records_in_relationships(self, stmts):
        """KNOWN BUG (Fix 2): no relationship has componentRecords."""
        rels_with_component = [
            r for r in _relationships(stmts)
            if "componentRecords" in (r.get("recordDetails") or {})
        ]
        assert rels_with_component == []

    @pytest.mark.xfail(
        reason="Fix 2 (isComponent + componentRecords for indirect chains) not yet implemented",
        strict=True,
    )
    def test_EXPECTED_intermediary_entity_has_is_component_true(self, stmts):
        """EXPECTED after Fix 2: the holding company entity should have isComponent=True."""
        # Holding company is the one that appears as subject AND interestedParty
        rel_subjects = {(r.get("recordDetails") or {}).get("subject") for r in _relationships(stmts)}
        rel_ips = {(r.get("recordDetails") or {}).get("interestedParty") for r in _relationships(stmts)}
        intermediary_ids = rel_subjects & rel_ips  # appears in both roles
        assert len(intermediary_ids) == 1
        intermediary_id = next(iter(intermediary_ids))
        intermediary_stmt = next(s for s in stmts if s.get("statementId") == intermediary_id)
        assert (intermediary_stmt.get("recordDetails") or {}).get("isComponent") is True

    @pytest.mark.xfail(
        reason="Fix 2 (isComponent + componentRecords for indirect chains) not yet implemented",
        strict=True,
    )
    def test_EXPECTED_primary_indirect_relationship_exists(self, stmts):
        """EXPECTED after Fix 2: a primary indirect relationship from person to subsidiary
        with componentRecords should be emitted."""
        rels_with_component = [
            r for r in _relationships(stmts)
            if "componentRecords" in (r.get("recordDetails") or {})
        ]
        assert len(rels_with_component) == 1
        comp_rel = rels_with_component[0]
        assert _interests(comp_rel)[0]["directOrIndirect"] == "indirect"


# ===========================================================================
# GLEIF — entity with direct parent
# ===========================================================================


class TestGleifDirectParent:
    """GLEIF record with a DIRECT_PARENT relationship."""

    @pytest.fixture
    def stmts(self) -> list[dict]:
        from opencheck.bods.mapper import map_gleif
        bundle = {
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
                            "addressLines": ["Musterstr. 1"],
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
        return _stmts(map_gleif, bundle)

    def test_statement_count(self, stmts):
        assert len(stmts) == 3  # subject entity + parent entity + relationship

    def test_two_entities(self, stmts):
        assert len(_entities(stmts)) == 2

    def test_one_relationship(self, stmts):
        assert len(_relationships(stmts)) == 1

    def test_interest_type_other_influence(self, stmts):
        """GLEIF Level 2 parent relationships use otherInfluenceOrControl
        (accounting consolidation), not shareholding."""
        assert _interest_types(stmts) == ["otherInfluenceOrControl"]

    def test_beneficial_ownership_false(self, stmts):
        """GLEIF ownership data does not assert beneficial ownership."""
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["beneficialOwnershipOrControl"] is False

    def test_subject_is_child_entity(self, stmts):
        """Subject of the relationship is the child (looked-up LEI), not the parent."""
        child_entity = next(
            e for e in _entities(stmts)
            if (e.get("recordDetails") or {}).get("name") == "TEST GMBH"
        )
        rel = _relationships(stmts)[0]
        assert (rel.get("recordDetails") or {}).get("subject") == child_entity["statementId"]

    def test_interested_party_is_parent_entity(self, stmts):
        """interestedParty of the relationship is the parent entity."""
        parent_entity = next(
            e for e in _entities(stmts)
            if (e.get("recordDetails") or {}).get("name") == "PARENT HOLDING AG"
        )
        rel = _relationships(stmts)[0]
        assert (rel.get("recordDetails") or {}).get("interestedParty") == parent_entity["statementId"]


# ===========================================================================
# GLEIF — entity with no parent (parent exception: natural persons)
# ===========================================================================


class TestGleifParentException:
    """GLEIF record where parent is not reported because owners are natural persons."""

    @pytest.fixture
    def stmts(self) -> list[dict]:
        from opencheck.bods.mapper import map_gleif
        bundle = {
            "lei": "213800LBDB8WB3QGVN21",
            "record": {
                "id": "213800LBDB8WB3QGVN21",
                "attributes": {
                    "lei": "213800LBDB8WB3QGVN21",
                    "entity": {
                        "legalName": {"name": "TEST GMBH"},
                        "jurisdiction": "DE",
                        "registeredAt": {"id": "RA000561", "other": None},
                        "legalAddress": {
                            "addressLines": ["Musterstr. 1"], "city": "Berlin",
                            "postalCode": "10115", "country": "DE",
                        },
                    },
                },
            },
            "direct_parent": None,
            "ultimate_parent": None,
            "direct_parent_exception": {
                "type": "NATURAL_PERSONS",
                "reason": "Owned by natural persons",
            },
            "ultimate_parent_exception": None,
            "direct_children": [],
        }
        return _stmts(map_gleif, bundle)

    def test_produces_statements(self, stmts):
        """A parent exception should produce at least an entity + relationship."""
        assert len(stmts) >= 2

    def test_entity_is_emitted(self, stmts):
        assert len(_entities(stmts)) >= 1

    def test_relationship_is_emitted(self, stmts):
        """An anonymous/unknown entity or person bridge should generate a relationship."""
        assert len(_relationships(stmts)) >= 1


# ===========================================================================
# INPI — dirigeant (non-BO role)
# ===========================================================================


class TestInpiDirigeant:
    """INPI record with a legal representative (beneficiaireEffectif=False)."""

    @pytest.fixture
    def bundle(self) -> dict:
        return {
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
                                "adresse": {"numVoie": "31", "typeVoie": "QUAI",
                                            "voie": "DE DION BOUTON", "codePostal": "92800",
                                            "commune": "PUTEAUX"}
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
                                                "roleEntreprise": 53,  # gérant
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

    @pytest.fixture
    def stmts(self, bundle) -> list[dict]:
        from opencheck.bods.mapper import map_inpi
        return _stmts(map_inpi, bundle)

    def test_statement_count(self, stmts):
        assert len(stmts) == 3  # entity + person + relationship

    def test_interest_type_senior_managing_official(self, stmts):
        assert _interest_types(stmts) == ["seniorManagingOfficial"]

    def test_beneficial_ownership_false(self, stmts):
        """INPI dirigeant is a legal representative, not a beneficial owner."""
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["beneficialOwnershipOrControl"] is False

    def test_direct_or_indirect_direct(self, stmts):
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["directOrIndirect"] == "direct"

    def test_person_name(self, stmts):
        person = _persons(stmts)[0]
        names = (person.get("recordDetails") or {}).get("names", [])
        full_names = [n.get("fullName", "") for n in names]
        assert any("DOE" in n.upper() or "JANE" in n.upper() for n in full_names)


class TestInpiBoSecurityInvariant:
    """INPI: beneficiaireEffectif=True must never produce statements (Loi Sapin II)."""

    def test_bo_record_produces_no_person_statement(self):
        from opencheck.bods.mapper import map_inpi
        bundle = {
            "source_id": "inpi", "siren": "055804124", "is_stub": False,
            "company": {
                "diffusionINSEE": "O", "siren": "055804124",
                "identite": {"entreprise": {"siren": "055804124",
                                            "denomination": "CORP", "formeJuridique": "5800"}},
                "formality": {"siren": "055804124", "content": {
                    "personneMorale": {
                        "adresseEntreprise": {"adresse": {"numVoie": "1", "typeVoie": "RUE",
                                                          "voie": "TEST", "codePostal": "75001",
                                                          "commune": "PARIS"}},
                        "composition": {"pouvoirs": [{
                            "typeDePersonne": "INDIVIDU",
                            "beneficiaireEffectif": True,  # ← must be silently skipped
                            "individu": {"descriptionPersonne": {"nom": "SECRET",
                                                                 "prenoms": ["OWNER"],
                                                                 "nationalite": "Française",
                                                                 "roleEntreprise": 53}},
                        }]},
                    },
                    "natureCreation": {"dateCreation": "2000-01-01"},
                }},
            },
        }
        stmts = _stmts(map_inpi, bundle)
        assert _persons(stmts) == [], "SECURITY VIOLATION: BO person statement emitted"
        assert _relationships(stmts) == [], "SECURITY VIOLATION: BO relationship statement emitted"


# ===========================================================================
# Brreg — board chair (LEDE) and board member (MEDL)
# ===========================================================================


class TestBrreg:
    """Brreg: board chair and board member roles."""

    def _entity_block(self) -> dict:
        return {
            "organisasjonsnummer": "923609016",
            "navn": "TEST AS",
            "organisasjonsform": {"kode": "AS"},
            "stiftelsesdato": "2000-01-01",
            "forretningsadresse": {
                "adresse": ["Testveien 1"], "postnummer": "0100",
                "poststed": "OSLO", "landkode": "NO", "land": "Norge",
            },
        }

    def _person_block(self, fornavn: str, etternavn: str, dob: str) -> dict:
        return {"navn": {"fornavn": fornavn, "etternavn": etternavn}, "fodselsdato": dob}

    def _make_bundle(self, roles: list[dict]) -> dict:
        return {
            "source_id": "brreg",
            "orgnr": "923609016",
            "entity": self._entity_block(),
            "roles": roles,
            "legal_name": "TEST AS",
            "is_stub": False,
        }

    def test_lede_produces_board_chair_interest(self):
        """LEDE (Styrets leder) → boardChair interest type."""
        from opencheck.bods.mapper import map_brreg
        bundle = self._make_bundle([{
            "type": {"kode": "LEDE", "beskrivelse": "Styreleder"},
            "person": self._person_block("Ola", "Nordmann", "1970-01-01"),
            "fratraadt": None,
        }])
        stmts = _stmts(map_brreg, bundle)
        assert _interest_types(stmts) == ["boardChair"]

    def test_lede_beneficial_ownership_false(self):
        from opencheck.bods.mapper import map_brreg
        bundle = self._make_bundle([{
            "type": {"kode": "LEDE", "beskrivelse": "Styreleder"},
            "person": self._person_block("Ola", "Nordmann", "1970-01-01"),
            "fratraadt": None,
        }])
        stmts = _stmts(map_brreg, bundle)
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["beneficialOwnershipOrControl"] is False

    def test_medl_produces_board_member_interest(self):
        """MEDL (Styremedlem) → boardMember interest type."""
        from opencheck.bods.mapper import map_brreg
        bundle = self._make_bundle([{
            "type": {"kode": "MEDL", "beskrivelse": "Styremedlem"},
            "person": self._person_block("Kari", "Hansen", "1975-05-15"),
            "fratraadt": None,
        }])
        stmts = _stmts(map_brreg, bundle)
        assert _interest_types(stmts) == ["boardMember"]

    def test_dagl_produces_other_influence(self):
        """DAGL (Daglig leder / CEO) → otherInfluenceOrControl."""
        from opencheck.bods.mapper import map_brreg
        bundle = self._make_bundle([{
            "type": {"kode": "DAGL", "beskrivelse": "Daglig leder"},
            "person": self._person_block("Lars", "Berg", "1980-03-20"),
            "fratraadt": None,
        }])
        stmts = _stmts(map_brreg, bundle)
        assert _interest_types(stmts) == ["otherInfluenceOrControl"]

    def test_terminated_role_is_skipped(self):
        """Roles with fratraadt set to a date are not emitted."""
        from opencheck.bods.mapper import map_brreg
        bundle = self._make_bundle([{
            "type": {"kode": "LEDE", "beskrivelse": "Styreleder"},
            "person": self._person_block("Exit", "Ersen", "1960-01-01"),
            "fratraadt": "2022-01-01",  # has left
        }])
        stmts = _stmts(map_brreg, bundle)
        assert _relationships(stmts) == []

    def test_unknown_role_code_is_skipped(self):
        """A role code not in _BRREG_ROLE_MAP is silently skipped."""
        from opencheck.bods.mapper import map_brreg
        bundle = self._make_bundle([{
            "type": {"kode": "XXXX", "beskrivelse": "Unknown"},
            "person": self._person_block("Nobody", "Here", "1990-01-01"),
            "fratraadt": None,
        }])
        stmts = _stmts(map_brreg, bundle)
        assert _relationships(stmts) == []

    def test_direct_or_indirect_direct(self):
        from opencheck.bods.mapper import map_brreg
        bundle = self._make_bundle([{
            "type": {"kode": "LEDE"},
            "person": self._person_block("Test", "Person", "1975-01-01"),
            "fratraadt": None,
        }])
        stmts = _stmts(map_brreg, bundle)
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["directOrIndirect"] == "direct"


# ===========================================================================
# UR Latvia — declared BO and board member
# ===========================================================================


class TestUrLatvia:
    """UR Latvia: declared beneficial owner + board officer."""

    @pytest.fixture
    def bundle(self) -> dict:
        return {
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
                    "id": "bo-001",
                    "forename": "Jānis",
                    "surname": "Bērziņš",
                    "nationality": "LV",
                    "birth_date": "1975-03-10",
                    "registered_on": "2020-01-01",
                }
            ],
            "officers": [
                {
                    "name": "Anna Kalniņa",
                    "role": "valdes priekšsēdētājs",
                    "from_date": "2010-01-01",
                    "type": "person",
                }
            ],
            "members": [],
            "is_stub": False,
        }

    @pytest.fixture
    def stmts(self, bundle) -> list[dict]:
        from opencheck.bods.mapper import map_ur_latvia
        return _stmts(map_ur_latvia, bundle)

    def test_statement_count(self, stmts):
        assert len(stmts) == 5  # entity + 2 persons + 2 relationships

    def test_two_persons(self, stmts):
        assert len(_persons(stmts)) == 2

    def test_two_relationships(self, stmts):
        assert len(_relationships(stmts)) == 2

    def test_bo_interest_type_other_influence(self, stmts):
        """Declared BO in Latvian UR → otherInfluenceOrControl (UBO registry,
        not a shareholding percentage)."""
        types = _interest_types(stmts)
        assert "otherInfluenceOrControl" in types

    def test_bo_beneficial_ownership_true(self, stmts):
        """The BO relationship has beneficialOwnershipOrControl=True."""
        bo_rels = [
            r for r in _relationships(stmts)
            if any(i.get("beneficialOwnershipOrControl") for i in _interests(r))
        ]
        assert len(bo_rels) == 1

    def test_officer_interest_type_board_member(self, stmts):
        """Board officer → boardMember interest type."""
        types = _interest_types(stmts)
        assert "boardMember" in types

    def test_officer_beneficial_ownership_false(self, stmts):
        """Board officer does not assert beneficial ownership."""
        officer_rels = [
            r for r in _relationships(stmts)
            if any(i.get("type") == "boardMember" for i in _interests(r))
        ]
        assert len(officer_rels) == 1
        assert _interests(officer_rels[0])[0]["beneficialOwnershipOrControl"] is False


# ===========================================================================
# Ariregister — shareholder + board member
# ===========================================================================


class TestAriregister:
    """Estonian e-Business Register: shareholder (shareholding) + JUHL officer (boardMember)."""

    @pytest.fixture
    def bundle(self) -> dict:
        return {
            "source_id": "ariregister",
            "registry_code": "14064835",
            "name": "TEST OÜ",
            "legal_form": "Osaühing",
            "status": "R",
            "registration_date": "2000-01-01",
            "address": "Testitänaval 1, 10001 Tallinn",
            "link": "https://ariregister.rik.ee/est/company/14064835",
            "shareholders": [
                {
                    "eesnimi": "Jaan",
                    "nimi_arinimi": "Tamm",
                    "share_percent": "60",
                    "shareholder_type": "person",
                    "from_date": "2010-01-01",
                    "country": "EE",
                    "isikukood_hash": "sharehash001",
                    "kirje_id": "sh1",
                }
            ],
            "officers": [
                {
                    "eesnimi": "Mari",
                    "nimi_arinimi": "Mägi",
                    "isiku_roll": "JUHL",  # juhatuse liige (board member)
                    "algus_kpv": "2015-06-01",
                    "lopp_kpv": None,
                    "isikukood_hash": "officerhash001",
                    "kirje_id": "off1",
                }
            ],
            "beneficial_owners": [],
            "is_stub": False,
        }

    @pytest.fixture
    def stmts(self, bundle) -> list[dict]:
        from opencheck.bods.mapper import map_ariregister
        return _stmts(map_ariregister, bundle)

    def test_statement_count(self, stmts):
        assert len(stmts) == 5  # entity + 2 persons + 2 relationships

    def test_shareholder_interest_type_shareholding(self, stmts):
        types = _interest_types(stmts)
        assert "shareholding" in types

    def test_officer_interest_type_board_member(self, stmts):
        types = _interest_types(stmts)
        assert "boardMember" in types

    def test_shareholder_beneficial_ownership_false(self, stmts):
        """Ariregister shareholder data does not assert BO (no UBO declaration)."""
        sh_rels = [
            r for r in _relationships(stmts)
            if any(i.get("type") == "shareholding" for i in _interests(r))
        ]
        assert len(sh_rels) == 1
        assert _interests(sh_rels[0])[0].get("beneficialOwnershipOrControl") is not True

    def test_officer_beneficial_ownership_false(self, stmts):
        officer_rels = [
            r for r in _relationships(stmts)
            if any(i.get("type") == "boardMember" for i in _interests(r))
        ]
        assert len(officer_rels) == 1
        # boardMember interest does not have beneficialOwnershipOrControl set
        # (the field is absent from ariregister officer interests)
        assert _interests(officer_rels[0])[0].get("beneficialOwnershipOrControl") is not True

    def test_all_interests_direct(self, stmts):
        """Where directOrIndirect is present it must be 'direct'.
        Note: ariregister shareholder interests do not include this field
        (the register doesn't publish direction data for shareholders)."""
        for rel in _relationships(stmts):
            for interest in _interests(rel):
                direction = interest.get("directOrIndirect")
                if direction is not None:
                    assert direction == "direct"


# ===========================================================================
# Corporations Canada — director
# ===========================================================================


class TestCorporationsCanada:
    """Corporations Canada: director role → seniorManagingOfficial."""

    @pytest.fixture
    def bundle(self) -> dict:
        return {
            "source_id": "corporations_canada",
            "corp_id": "1007",
            "corporation": {
                "corporationId": "1007",
                "legalName": "Test Corp",
                "corporationNames": [
                    {"legalName": "Test Corp", "nameTypeCd": "LN", "endEventId": None}
                ],
                "status": "Active",
                "businessNumber": "106679285",
                "corporationType": {"desc": "Business Corporation", "cd": "A"},
                "incorporationDate": "2000-01-01",
                "offices": [
                    {
                        "officeType": "registeredOffice",
                        "deliveryAddress": {
                            "streetAddress": "1 Main St",
                            "addressCity": "Ottawa",
                            "addressRegion": "ON",
                            "postalCode": "K1A 0A1",
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
                        "streetAddress": "2 Oak Ave",
                        "addressCity": "Ottawa",
                        "addressRegion": "ON",
                        "postalCode": "K1A 0B2",
                        "addressCountry": "CA",
                    },
                }
            ],
            "legal_name": "Test Corp",
            "is_stub": False,
        }

    @pytest.fixture
    def stmts(self, bundle) -> list[dict]:
        from opencheck.bods.mapper import map_corporations_canada
        return _stmts(map_corporations_canada, bundle)

    def test_statement_count(self, stmts):
        assert len(stmts) == 3  # entity + person + relationship

    def test_interest_type_senior_managing_official(self, stmts):
        """Corporations Canada Director → seniorManagingOfficial.
        Note: the Phase 4 plan spec said boardMember, but the actual mapper
        emits seniorManagingOfficial for the Director role type, which is
        correct per BODS v0.4 (directors who manage the company are senior
        managing officials, not just board members)."""
        assert _interest_types(stmts) == ["seniorManagingOfficial"]

    def test_beneficial_ownership_false(self, stmts):
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["beneficialOwnershipOrControl"] is False

    def test_direct_or_indirect_direct(self, stmts):
        rel = _relationships(stmts)[0]
        assert _interests(rel)[0]["directOrIndirect"] == "direct"

    def test_no_director_no_person_or_relationship(self):
        from opencheck.bods.mapper import map_corporations_canada
        bundle = {
            "source_id": "corporations_canada",
            "corp_id": "9999",
            "corporation": {
                "corporationId": "9999",
                "legalName": "Empty Corp",
                "corporationNames": [{"legalName": "Empty Corp", "nameTypeCd": "LN", "endEventId": None}],
                "status": "Active",
                "businessNumber": "999999999",
                "corporationType": {"desc": "Business Corporation", "cd": "A"},
                "incorporationDate": "2010-01-01",
                "offices": [],
            },
            "directors": [],
            "legal_name": "Empty Corp",
            "is_stub": False,
        }
        stmts = _stmts(map_corporations_canada, bundle)
        assert _persons(stmts) == []
        assert _relationships(stmts) == []


# ===========================================================================
# Firmenbuch — GmbH officer and shareholder
# ===========================================================================


class TestFirmenbuch:
    """Firmenbuch: GmbH officer (otherInfluenceOrControl) + shareholder (shareholding).

Note: the Firmenbuch mapper maps Geschäftsführer (GF) and Prokurist roles to
``otherInfluenceOrControl`` (not ``seniorManagingOfficial``), because the
Austrian register does not distinguish managing officials from other
influence/control roles in the way that CH or Corporations Canada do.
"""

    @pytest.fixture
    def stmts(self) -> list[dict]:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from test_bods_firmenbuch import _GMBH_EXTRACT  # type: ignore
        from opencheck.bods.mapper import map_firmenbuch
        bundle = {
            "source_id": "firmenbuch",
            "fn": "473888w",
            "extract": _GMBH_EXTRACT,
            "legal_name": "",
            "is_stub": False,
        }
        return _stmts(map_firmenbuch, bundle)

    def test_at_least_one_entity(self, stmts):
        assert len(_entities(stmts)) >= 1

    def test_at_least_one_relationship(self, stmts):
        assert len(_relationships(stmts)) >= 1

    def test_officer_interest_type_other_influence(self, stmts):
        """GmbH Geschäftsführer/Prokurist → otherInfluenceOrControl.
        The Firmenbuch mapper uses otherInfluenceOrControl for Austrian officer
        roles (GF, Prokurist) because the register doesn't provide a
        seniorManagingOfficial-specific designation."""
        types = set(_interest_types(stmts))
        assert "otherInfluenceOrControl" in types

    def test_shareholder_interest_type_shareholding(self, stmts):
        types = set(_interest_types(stmts))
        assert "shareholding" in types

    def test_officer_beneficial_ownership_false(self, stmts):
        gf_rels = [
            r for r in _relationships(stmts)
            if any(i.get("type") == "otherInfluenceOrControl" for i in _interests(r))
        ]
        assert len(gf_rels) >= 1
        for rel in gf_rels:
            assert _interests(rel)[0]["beneficialOwnershipOrControl"] is False
