"""Tests for the GLEIF + FtM BODS v0.4 mappers (Phase 2)."""

from __future__ import annotations

from opencheck.bods import (
    map_gleif,
    map_openaleph,
    map_opensanctions,
    validate_shape,
)


# ---------------------------------------------------------------------
# GLEIF
# ---------------------------------------------------------------------


def _gleif_bundle_with_direct_parent() -> dict:
    return {
        "lei": "213800LBDB8WB3QGVN21",
        "record": {
            "id": "213800LBDB8WB3QGVN21",
            "attributes": {
                "lei": "213800LBDB8WB3QGVN21",
                "entity": {
                    "legalName": {"name": "BP P.L.C."},
                    "jurisdiction": "GB",
                    "registeredAs": "00102498",
                    # GLEIF RA code for UK Companies House.
                    "registeredAt": {"id": "RA000585", "other": None},
                    "legalAddress": {
                        "addressLines": ["1 St James's Square"],
                        "city": "London",
                        "postalCode": "SW1Y 4PD",
                        "country": "GB",
                    },
                },
            },
        },
        "direct_parent": {
            "id": "PARENTXXXXXXXXXXXXXX",
            "attributes": {
                "lei": "PARENTXXXXXXXXXXXXXX",
                "entity": {
                    "legalName": {"name": "BP Group Holdings"},
                    "jurisdiction": "GB",
                },
            },
        },
        "ultimate_parent": None,
    }


def test_map_gleif_emits_subject_and_parent() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    statements = list(bundle)
    types = [s["recordType"] for s in statements]
    assert types.count("entity") == 2
    assert types.count("relationship") == 1


def test_map_gleif_subject_has_lei_identifier() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    schemes = [i["scheme"] for i in subject["recordDetails"]["identifiers"]]
    assert "XI-LEI" in schemes


def test_map_gleif_uses_registration_authority_scheme() -> None:
    """Subject includes a registration-authority-resolved identifier.

    RA000585 is UK Companies House — mapped to GB-COH in _GLEIF_RA_TO_ORG_ID,
    so the identifier should carry scheme="GB-COH" and schemeName="Companies House".
    """
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    ra = next(
        i for i in subject["recordDetails"]["identifiers"]
        if i.get("scheme") == "GB-COH"
    )
    assert ra["id"] == "00102498"
    assert ra["schemeName"] == "Companies House"


def test_map_gleif_resolves_jurisdiction_name() -> None:
    """'GB' should resolve to 'United Kingdom' via pycountry."""
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    assert subject["recordDetails"]["incorporatedInJurisdiction"] == {
        "name": "United Kingdom",
        "code": "GB",
    }


def test_map_gleif_resolves_subdivision_jurisdiction() -> None:
    payload = _gleif_bundle_with_direct_parent()
    payload["record"]["attributes"]["entity"]["jurisdiction"] = "GB-ENG"
    bundle = map_gleif(payload)
    subject = next(
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "BP P.L.C."
    )
    jur = subject["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "GB-ENG"
    assert "United Kingdom" in jur["name"]


def test_map_gleif_relationship_has_control_interest() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    rel = next(s for s in bundle if s["recordType"] == "relationship")
    interests = rel["recordDetails"]["interests"]
    assert interests[0]["type"] == "otherInfluenceOrControl"
    assert "direct-parent" in interests[0]["details"]


def test_map_gleif_output_passes_validator() -> None:
    bundle = map_gleif(_gleif_bundle_with_direct_parent())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_map_gleif_handles_missing_parents() -> None:
    payload = _gleif_bundle_with_direct_parent()
    payload["direct_parent"] = None
    bundle = map_gleif(payload)
    types = [s["recordType"] for s in bundle]
    assert types == ["entity"]  # just the subject


def test_map_gleif_emits_bridge_for_natural_persons_exception() -> None:
    """A NATURAL_PERSONS exception becomes an unknownPerson + relationship."""
    payload = {
        "lei": "LEI00000000000000001",
        "record": {
            "id": "LEI00000000000000001",
            "attributes": {
                "lei": "LEI00000000000000001",
                "entity": {
                    "legalName": {"name": "Family Trust Holdings Ltd"},
                    "jurisdiction": "GB",
                },
            },
        },
        "direct_parent": None,
        "direct_parent_exception": {
            "attributes": {
                "exceptionCategory": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                "exceptionReason": "NATURAL_PERSONS",
            }
        },
        "ultimate_parent": None,
        "ultimate_parent_exception": None,
    }
    bundle = map_gleif(payload)
    types = [s["recordType"] for s in bundle]
    # subject + bridge person + relationship = 3 statements
    assert types == ["entity", "person", "relationship"]

    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["personType"] == "unknownPerson"

    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert "NATURAL_PERSONS" in rel["recordDetails"]["interests"][0]["details"].upper() \
        or "natural persons" in rel["recordDetails"]["interests"][0]["details"].lower()

    # Validator still clean.
    assert validate_shape(bundle) == []


def test_map_gleif_emits_anonymous_entity_for_no_lei_exception() -> None:
    payload = {
        "lei": "LEI00000000000000002",
        "record": {
            "id": "LEI00000000000000002",
            "attributes": {
                "lei": "LEI00000000000000002",
                "entity": {
                    "legalName": {"name": "Quiet Holdings SA"},
                    "jurisdiction": "CH",
                },
            },
        },
        "direct_parent": None,
        "direct_parent_exception": None,
        "ultimate_parent": None,
        "ultimate_parent_exception": {
            "attributes": {
                "exceptionCategory": "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
                "exceptionReason": "NO_LEI",
            }
        },
    }
    bundle = map_gleif(payload)
    entities = [s for s in bundle if s["recordType"] == "entity"]
    # subject + anonymousEntity bridge
    assert len(entities) == 2
    bridge = next(
        e for e in entities
        if e["recordDetails"]["entityType"]["type"] == "anonymousEntity"
    )
    assert "reporting exception" in bridge["recordDetails"]["name"].lower()

    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["directOrIndirect"] == "indirect"
    assert validate_shape(bundle) == []


# ---------------------------------------------------------------------
# GLEIF Reporting Exceptions — live API field names
# ---------------------------------------------------------------------


def test_map_gleif_exception_live_api_field_names_natural_persons() -> None:
    """Live GLEIF API uses 'reason'/'category' not 'exceptionReason'/'exceptionCategory'.
    Mapper must read the live field names correctly."""
    payload = {
        "lei": "LEI00000000000000010",
        "record": {
            "id": "LEI00000000000000010",
            "attributes": {
                "lei": "LEI00000000000000010",
                "entity": {
                    "legalName": {"name": "Live Exception Co Ltd"},
                    "jurisdiction": "GB",
                },
            },
        },
        "direct_parent": None,
        "direct_parent_exception": {
            "attributes": {
                # Live API field names (not the OO SQLite dump names).
                "category": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                "reason": "NATURAL_PERSONS",
            }
        },
        "ultimate_parent": None,
        "ultimate_parent_exception": None,
    }
    bundle = map_gleif(payload)
    types = [s["recordType"] for s in bundle]
    assert types == ["entity", "person", "relationship"]
    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["personType"] == "unknownPerson"
    assert validate_shape(bundle) == []


def test_map_gleif_exception_live_api_no_lei() -> None:
    """Live 'reason': 'NO_LEI' (ultimate) should emit an anonymousEntity bridge."""
    payload = {
        "lei": "LEI00000000000000011",
        "record": {
            "id": "LEI00000000000000011",
            "attributes": {
                "lei": "LEI00000000000000011",
                "entity": {
                    "legalName": {"name": "No-LEI Parent Co"},
                    "jurisdiction": "DE",
                },
            },
        },
        "direct_parent": None,
        "direct_parent_exception": None,
        "ultimate_parent": None,
        "ultimate_parent_exception": {
            "attributes": {
                "category": "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
                "reason": "NO_LEI",
            }
        },
    }
    bundle = map_gleif(payload)
    entities = [s for s in bundle if s["recordType"] == "entity"]
    assert len(entities) == 2
    bridge = next(
        e for e in entities
        if e["recordDetails"]["entityType"]["type"] == "anonymousEntity"
    )
    assert "reporting exception" in bridge["recordDetails"]["name"].lower()
    assert validate_shape(bundle) == []


def test_map_gleif_exception_live_api_no_known_person() -> None:
    """Live 'reason': 'NO_KNOWN_PERSON' should emit unknownPerson bridge."""
    payload = {
        "lei": "LEI00000000000000012",
        "record": {
            "id": "LEI00000000000000012",
            "attributes": {
                "lei": "LEI00000000000000012",
                "entity": {
                    "legalName": {"name": "Mystery Holdings"},
                    "jurisdiction": "KY",
                },
            },
        },
        "direct_parent": None,
        "direct_parent_exception": {
            "attributes": {
                "category": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                "reason": "NO_KNOWN_PERSON",
            }
        },
        "ultimate_parent": None,
        "ultimate_parent_exception": None,
    }
    bundle = map_gleif(payload)
    person = next(s for s in bundle if s["recordType"] == "person")
    assert person["recordDetails"]["personType"] == "unknownPerson"
    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert "no known person" in rel["recordDetails"]["interests"][0]["details"].lower()
    assert validate_shape(bundle) == []


def test_map_gleif_exception_both_old_and_new_field_names_work() -> None:
    """Backward-compatibility: OO SQLite dump uses 'exceptionReason', both must work."""
    def _make_payload(field_name: str) -> dict:
        return {
            "lei": "LEI00000000000000013",
            "record": {
                "id": "LEI00000000000000013",
                "attributes": {
                    "lei": "LEI00000000000000013",
                    "entity": {
                        "legalName": {"name": "Compat Test Co"},
                        "jurisdiction": "US",
                    },
                },
            },
            "direct_parent": None,
            "direct_parent_exception": {
                "attributes": {field_name: "NON_CONSOLIDATING"}
            },
            "ultimate_parent": None,
            "ultimate_parent_exception": None,
        }

    for field in ("reason", "exceptionReason"):
        bundle = map_gleif(_make_payload(field))
        rel = next(s for s in bundle if s["recordType"] == "relationship")
        details = rel["recordDetails"]["interests"][0]["details"]
        assert "non_consolidating" in details.lower() or "consolidat" in details.lower(), (
            f"field '{field}' did not resolve to NON_CONSOLIDATING: {details}"
        )
        assert validate_shape(bundle) == []


# ---------------------------------------------------------------------
# GLEIF LEI Mapping cross-reference identifiers (ocid, qcc, mic, bic)
# ---------------------------------------------------------------------


def _gleif_bundle_with_lei_mappings() -> dict:
    """Bundle that includes all four GLEIF LEI Mapping identifiers."""
    return {
        "lei": "549300MLH00Y3BN4HD49",
        "record": {
            "id": "549300MLH00Y3BN4HD49",
            "attributes": {
                "lei": "549300MLH00Y3BN4HD49",
                "entity": {
                    "legalName": {"name": "Ericsson AB"},
                    "jurisdiction": "SE",
                    "registeredAs": "556056-6258",
                    "registeredAt": {"id": "RA000544", "other": None},
                },
                "ocid": "se/556056-6258",
                "qcc": "QSEVC89DTN",
                "mic": "XSTO",
                "bic": "SWEDSESS",
            },
        },
        "direct_parent": None,
        "ultimate_parent": None,
    }


def _gleif_bundle_with_null_lei_mappings() -> dict:
    """Bundle where all four GLEIF LEI Mapping fields are null."""
    return {
        "lei": "213800NULLNULLNULLXX",
        "record": {
            "id": "213800NULLNULLNULLXX",
            "attributes": {
                "lei": "213800NULLNULLNULLXX",
                "entity": {
                    "legalName": {"name": "No Mappings Ltd"},
                    "jurisdiction": "GB",
                },
                "ocid": None,
                "qcc": None,
                "mic": None,
                "bic": None,
            },
        },
        "direct_parent": None,
        "ultimate_parent": None,
    }


def _subject_entity(bundle: dict) -> dict:
    """Return the subject entity statement from a mapped GLEIF bundle."""
    stmts = list(map_gleif(bundle))
    return next(s for s in stmts if s["recordType"] == "entity")


def test_gleif_lei_mapping_ocid_included() -> None:
    """ocid present in attributes → OpenCorporates identifier in entity statement."""
    subj = _subject_entity(_gleif_bundle_with_lei_mappings())
    oc = next(
        (i for i in subj["recordDetails"]["identifiers"] if i["scheme"] == "OpenCorporates"),
        None,
    )
    assert oc is not None
    assert oc["id"] == "se/556056-6258"
    assert "OpenCorporates" in oc["schemeName"]
    assert oc["uri"] == "https://opencorporates.com/companies/se/556056-6258"


def test_gleif_lei_mapping_qcc_included() -> None:
    """qcc present in attributes → QCC Code identifier in entity statement."""
    subj = _subject_entity(_gleif_bundle_with_lei_mappings())
    qcc = next(
        (i for i in subj["recordDetails"]["identifiers"] if i["scheme"] == "QCC Code"),
        None,
    )
    assert qcc is not None
    assert qcc["id"] == "QSEVC89DTN"


def test_gleif_lei_mapping_mic_included() -> None:
    """mic present in attributes → ISO-10383 identifier in entity statement."""
    subj = _subject_entity(_gleif_bundle_with_lei_mappings())
    mic = next(
        (i for i in subj["recordDetails"]["identifiers"] if i["scheme"] == "ISO-10383"),
        None,
    )
    assert mic is not None
    assert mic["id"] == "XSTO"
    assert "Market Identifier" in mic["schemeName"]


def test_gleif_lei_mapping_bic_included() -> None:
    """bic present in attributes → ISO-9362 identifier in entity statement."""
    subj = _subject_entity(_gleif_bundle_with_lei_mappings())
    bic = next(
        (i for i in subj["recordDetails"]["identifiers"] if i["scheme"] == "ISO-9362"),
        None,
    )
    assert bic is not None
    assert bic["id"] == "SWEDSESS"
    assert "Bank Identifier" in bic["schemeName"]


def test_gleif_lei_mapping_null_values_excluded() -> None:
    """Null ocid/qcc/mic/bic must not produce empty identifier entries."""
    subj = _subject_entity(_gleif_bundle_with_null_lei_mappings())
    schemes = {i["scheme"] for i in subj["recordDetails"]["identifiers"]}
    assert "OPENCORPORATES" not in schemes
    assert "QCC Code" not in schemes
    assert "ISO-10383" not in schemes
    assert "ISO-9362" not in schemes


def test_gleif_lei_mapping_absent_attrs_safe() -> None:
    """Bundle with no attrs key at all (e.g. older cached bundles) must not raise."""
    bundle = {
        "lei": "213800NOATTRSXXXXXXX",
        "record": {
            "id": "213800NOATTRSXXXXXXX",
            "attributes": {
                "lei": "213800NOATTRSXXXXXXX",
                "entity": {"legalName": {"name": "No Attrs Co"}, "jurisdiction": "DE"},
                # no ocid/qcc/mic/bic keys at all
            },
        },
        "direct_parent": None,
        "ultimate_parent": None,
    }
    stmts = list(map_gleif(bundle))
    assert any(s["recordType"] == "entity" for s in stmts)


def test_gleif_lei_mapping_passes_validator() -> None:
    """Full bundle with all four mappings must pass the BODS shape validator."""
    issues = validate_shape(map_gleif(_gleif_bundle_with_lei_mappings()))
    assert issues == [], issues


# ---------------------------------------------------------------------
# FtM — OpenSanctions
# ---------------------------------------------------------------------


def _opensanctions_company_bundle() -> dict:
    return {
        "source_id": "opensanctions",
        "entity_id": "NK-rosneft",
        "entity": {
            "id": "NK-rosneft",
            "schema": "Company",
            "caption": "Rosneft Oil Company",
            "properties": {
                "name": ["Rosneft Oil Company"],
                "leiCode": ["253400VC22A0KFSOPB29"],
                "wikidataId": ["Q219617"],
                "jurisdiction": ["RU"],
                "incorporationDate": ["1993-09-30"],
            },
        },
    }


def test_map_opensanctions_emits_entity_with_identifiers() -> None:
    bundle = map_opensanctions(_opensanctions_company_bundle())
    statements = list(bundle)
    assert len(statements) == 1
    entity = statements[0]
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert {"OPENSANCTIONS", "XI-LEI", "WIKIDATA"}.issubset(schemes)
    assert entity["recordDetails"]["foundingDate"] == "1993-09-30"


def test_map_opensanctions_person() -> None:
    payload = {
        "entity": {
            "id": "NK-putin",
            "schema": "Person",
            "caption": "Vladimir Putin",
            "properties": {
                "name": ["Vladimir Putin"],
                "nationality": ["ru"],
                "birthDate": ["1952-10-07"],
                "wikidataId": ["Q7747"],
            },
        }
    }
    bundle = map_opensanctions(payload)
    statements = list(bundle)
    assert len(statements) == 1
    s = statements[0]
    assert s["recordType"] == "person"
    assert s["recordDetails"]["birthDate"] == "1952-10-07"
    schemes = {i["scheme"] for i in s["recordDetails"]["identifiers"]}
    assert "WIKIDATA" in schemes


def test_map_opensanctions_passes_validator() -> None:
    bundle = map_opensanctions(_opensanctions_company_bundle())
    issues = validate_shape(bundle)
    assert issues == [], issues


def test_ftm_owners_of_property_emits_relationship() -> None:
    """A Person with `ownersOf` → [Company] should emit a shareholding link."""
    payload = {
        "entity": {
            "id": "NK-owner",
            "schema": "Person",
            "caption": "Jane Owner",
            "properties": {
                "name": ["Jane Owner"],
                "ownersOf": [
                    {
                        "id": "NK-target",
                        "schema": "Company",
                        "caption": "Target Holdings",
                        "properties": {"name": ["Target Holdings"]},
                    }
                ],
            },
        }
    }
    bundle = map_opensanctions(payload)
    types = [s["recordType"] for s in bundle]
    assert types.count("person") == 1  # Jane
    assert types.count("entity") == 1  # Target
    assert types.count("relationship") == 1
    rel = next(s for s in bundle if s["recordType"] == "relationship")
    assert rel["recordDetails"]["interests"][0]["type"] == "shareholding"


# ---------------------------------------------------------------------
# FtM — OpenAleph
# ---------------------------------------------------------------------


def test_map_openaleph_entity() -> None:
    payload = {
        "entity": {
            "id": "aleph-123",
            "schema": "Company",
            "properties": {
                "name": ["Acme Holdings"],
                "leiCode": ["LEI0000000000000ACME"],
                "jurisdiction": ["BVI"],
            },
        },
        "collection": {"label": "ICIJ leaks", "license": "CC BY-NC 4.0"},
    }
    bundle = map_openaleph(payload)
    issues = validate_shape(bundle)
    assert issues == [], issues
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert {"OPENALEPH", "XI-LEI"}.issubset(schemes)


# ---------------------------------------------------------------------
# FtM field-quality fixes
# ---------------------------------------------------------------------


def test_ftm_nationality_resolved_to_full_name_and_code() -> None:
    """ISO alpha-2 nationality codes (e.g. 'ru') should resolve to full name + code."""
    payload = {
        "entity": {
            "id": "NK-putin",
            "schema": "Person",
            "properties": {"name": ["Vladimir Putin"], "nationality": ["ru"]},
        }
    }
    bundle = map_opensanctions(payload)
    person = next(iter(bundle))
    nats = person["recordDetails"]["nationalities"]
    assert nats == [{"name": "Russian Federation", "code": "RU"}]


def test_ftm_jurisdiction_uppercase_code_resolves_to_full_name() -> None:
    """Jurisdiction 'RU' should produce name='Russian Federation', code='RU'."""
    payload = {
        "entity": {
            "id": "NK-co",
            "schema": "Company",
            "properties": {"name": ["Rosneft"], "jurisdiction": ["RU"]},
        }
    }
    bundle = map_opensanctions(payload)
    entity = next(iter(bundle))
    jur = entity["recordDetails"]["incorporatedInJurisdiction"]
    assert jur == {"name": "Russian Federation", "code": "RU"}


def test_ftm_jurisdiction_lowercase_code_resolves() -> None:
    """Lowercase 'ru' should resolve identically to uppercase 'RU'."""
    payload = {
        "entity": {
            "id": "NK-co2",
            "schema": "Company",
            "properties": {"name": ["Test Co"], "jurisdiction": ["ru"]},
        }
    }
    bundle = map_opensanctions(payload)
    entity = next(iter(bundle))
    jur = entity["recordDetails"]["incorporatedInJurisdiction"]
    assert jur == {"name": "Russian Federation", "code": "RU"}


def test_ftm_registration_number_scheme_qualified_when_jurisdiction_known() -> None:
    """registrationNumber should use 'REG-{alpha2}' when jurisdiction is present."""
    payload = {
        "entity": {
            "id": "NK-co3",
            "schema": "Company",
            "properties": {
                "name": ["Test Co"],
                "jurisdiction": ["RU"],
                "registrationNumber": ["1027700139019"],
            },
        }
    }
    bundle = map_opensanctions(payload)
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "REG-RU" in schemes
    assert "REG" not in schemes  # bare REG should not appear when jurisdiction known


def test_ftm_registration_number_falls_back_to_reg_without_jurisdiction() -> None:
    """registrationNumber without a jurisdiction should stay as generic 'REG'."""
    payload = {
        "entity": {
            "id": "NK-co4",
            "schema": "Company",
            "properties": {"name": ["Unknown Co"], "registrationNumber": ["ABC123"]},
        }
    }
    bundle = map_opensanctions(payload)
    entity = next(iter(bundle))
    schemes = {i["scheme"] for i in entity["recordDetails"]["identifiers"]}
    assert "REG" in schemes
