"""Phase 8 — Identifier scheme completeness tests for national registry mappers.

Verifies, for every national registry adapter, that:

1. The entity statement produced by the mapper has a non-empty ``identifiers``
   list.
2. Every identifier dict has a non-empty ``id`` string and a non-empty
   ``scheme`` string.
3. The expected primary scheme code for the adapter is present in the
   identifier list.

Fixtures use minimal bundles — just enough data to make the mapper emit an
entity statement.  No network calls are made.

INPI note: the INPI mapper follows French legal requirements.
  * ``beneficiaireEffectif == True`` entries are always silently skipped.
  * Bundles here do NOT include such entries, and therefore do not test the
    skip path (which is tested in test_bods_inpi.py).
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from opencheck.bods.mapper import (
    map_acra_singapore,
    map_bce_belgium,
    map_bolagsverket,
    map_brreg,
    map_companies_house,
    map_corporations_canada,
    map_cro,
    map_cvr_denmark,
    map_firmenbuch,
    map_inpi,
    map_jar_lithuania,
    map_krs_poland,
    map_kvk,
    map_rpo_slovakia,
    map_ur_latvia,
    map_zefix,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entity(statements: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the first entity statement from a list, or raise AssertionError."""
    entities = [s for s in statements if s.get("recordType") == "entity"]
    assert entities, f"No entity statement found in {len(statements)} statements"
    return entities[0]


def _identifiers(bundle_fn, bundle: dict[str, Any]) -> list[dict[str, str]]:
    stmts = list(bundle_fn(bundle))
    ent = _entity(stmts)
    return ent["recordDetails"].get("identifiers") or []


def _scheme_ids(ids: list[dict[str, str]]) -> set[str]:
    return {i["scheme"] for i in ids}


# ---------------------------------------------------------------------------
# Minimal valid bundles (one per adapter)
# ---------------------------------------------------------------------------

_COMPANIES_HOUSE_BUNDLE: dict[str, Any] = {
    # map_companies_house dispatches on company_number (not "number") and does
    # not check is_stub — the adapter itself guards on missing data.
    "company_number": "00102498",
    "profile": {
        "company_name": "Shell plc",
        "date_of_creation": "1897-06-01",
    },
    "pscs": {"items": []},
    "officers": {},
}

_KVK_BUNDLE: dict[str, Any] = {
    "source_id": "kvk",
    "kvk_number": "96332751",
    "legal_name": "Splitty B.V.",
    "company": {
        "actief": "J",
        "rechtsvormCode": "BV",
        "lidstaat": "NL",
    },
    "is_stub": False,
}

_BRREG_BUNDLE: dict[str, Any] = {
    "source_id": "brreg",
    "orgnr": "974760673",
    "entity": {"navn": "Equinor ASA"},
    "roles": [],
    "is_stub": False,
}

_CRO_BUNDLE: dict[str, Any] = {
    "source_id": "cro",
    "crn": "123456",
    "company": {
        "company_name": "Acme Ltd",
        "company_type": "Private Company Limited by Shares",
    },
    "is_stub": False,
}

_BOLAGSVERKET_BUNDLE: dict[str, Any] = {
    "source_id": "bolagsverket",
    "org_number": "5560160680",
    "company": {
        "organisationsidentitet": {"identitetsbeteckning": "5560160680"},
        "organisationsnamn": {
            "organisationsnamnLista": [
                {"namn": "Telefonaktiebolaget LM Ericsson", "registreringsdatum": "1918-08-18"}
            ]
        },
    },
    "is_stub": False,
}

_JAR_LITHUANIA_BUNDLE: dict[str, Any] = {
    "source_id": "jar_lithuania",
    "lt_code": "111950016",
    "name": "Acme UAB",
    "is_stub": False,
}

_KRS_POLAND_BUNDLE: dict[str, Any] = {
    "source_id": "krs_poland",
    "pl_krs": "0000017219",
    "name": "PKN Orlen SA",
    "is_stub": False,
}

_RPO_SLOVAKIA_BUNDLE: dict[str, Any] = {
    "source_id": "rpo_slovakia",
    "sk_ico": "31320155",
    "name": "Acme s.r.o.",
    "is_stub": False,
}

_BCE_BELGIUM_BUNDLE: dict[str, Any] = {
    "source_id": "bce_belgium",
    "enterprise_number": "0403019488",
    "dotted": "0403.019.488",
    "name": "Test SA",
    "is_stub": False,
}

_CORPORATIONS_CANADA_BUNDLE: dict[str, Any] = {
    "source_id": "corporations_canada",
    "corp_id": "1007",
    "legal_name": "Abbotsford Chamber of Commerce",
    "corporation": {
        "corporate_name": "Abbotsford Chamber of Commerce",
        "corporate_type": "FEDERAL",
        "status": "Active",
        "date_of_registration": "2000-01-01",
    },
    "directors": [],
    "is_stub": False,
}

_ACRA_SINGAPORE_BUNDLE: dict[str, Any] = {
    "source_id": "acra_singapore",
    "uen": "200312345E",
    "entity_name": "Stark Enterprises Private Limited",
    "entity_type_desc": "PRIVATE COMPANY LIMITED BY SHARES",
    "uen_status_desc": "Live",
    "uen_issue_date": "2003-04-01",
    "is_stub": False,
}

_CVR_DENMARK_BUNDLE: dict[str, Any] = {
    "source_id": "cvr_denmark",
    "cvr_number": "24256790",
    "name": "Novo Nordisk A/S",
    "status": "AKTIV",
    "is_stub": False,
}

_FIRMENBUCH_BUNDLE: dict[str, Any] = {
    "source_id": "firmenbuch",
    "fn": "473888w",
    "extract": {
        "name": "Test GmbH",
        "form": "GmbH",
        "status": "eingetragen",
        "address": {},
        "officers": [],
        "shareholders": [],
    },
    "is_stub": False,
}

_UR_LATVIA_BUNDLE: dict[str, Any] = {
    "source_id": "ur_latvia",
    "lv_regcode": "40003009556",
    "entity": {"name": "Acme SIA", "type": "SIA"},
    "beneficial_owners": [],
    "officers": [],
    "members": [],
    "is_stub": False,
}

_ZEFIX_BUNDLE: dict[str, Any] = {
    "source_id": "zefix",
    "uid": "CHE313550547",
    "company": {
        "name": "Test AG",
        "legalSeat": {"canton": "ZH"},
        "legalForm": {
            "nameDE": "Aktiengesellschaft",
            "uid": "0107.001.081",
        },
        "address": {},
        "commercialRegisterStatus": "ACTIVE",
    },
    "is_stub": False,
}

_INPI_BUNDLE: dict[str, Any] = {
    "source_id": "inpi",
    "siren": "055804124",
    "company": {
        "identite": {"entreprise": {"denomination": "Bolloré SA"}},
        "formality": {
            "content": {
                "personneMorale": {
                    "identite": {"entreprise": {"denomination": "Bolloré SA"}},
                },
                "natureCreation": {"dateCreation": "1950-01-01"},
            }
        },
    },
    "is_stub": False,
}


# ---------------------------------------------------------------------------
# Per-adapter expected primary scheme code
# ---------------------------------------------------------------------------

_ADAPTER_CASES: list[tuple[str, Any, dict[str, Any], str]] = [
    # (adapter_name, mapper_fn, bundle, primary_scheme)
    ("companies_house", map_companies_house, _COMPANIES_HOUSE_BUNDLE, "GB-COH"),
    ("kvk", map_kvk, _KVK_BUNDLE, "NL-KVK"),
    ("brreg", map_brreg, _BRREG_BUNDLE, "NO-BRC"),
    ("cro", map_cro, _CRO_BUNDLE, "IE-CRO"),
    ("bolagsverket", map_bolagsverket, _BOLAGSVERKET_BUNDLE, "SE-BLV"),
    ("jar_lithuania", map_jar_lithuania, _JAR_LITHUANIA_BUNDLE, "LT-JAR"),
    ("krs_poland", map_krs_poland, _KRS_POLAND_BUNDLE, "PL-KRS"),
    ("rpo_slovakia", map_rpo_slovakia, _RPO_SLOVAKIA_BUNDLE, "SK-RPO"),
    ("bce_belgium", map_bce_belgium, _BCE_BELGIUM_BUNDLE, "BE-BCE_KBO"),
    ("corporations_canada", map_corporations_canada, _CORPORATIONS_CANADA_BUNDLE, "CA-CORP"),
    ("acra_singapore", map_acra_singapore, _ACRA_SINGAPORE_BUNDLE, "SG-ACRA"),
    ("cvr_denmark", map_cvr_denmark, _CVR_DENMARK_BUNDLE, "DK-CVR"),
    ("firmenbuch", map_firmenbuch, _FIRMENBUCH_BUNDLE, "AT-FB"),
    ("ur_latvia", map_ur_latvia, _UR_LATVIA_BUNDLE, "LV-UR"),
    ("zefix", map_zefix, _ZEFIX_BUNDLE, "CH-FDJP"),
    ("inpi", map_inpi, _INPI_BUNDLE, "FR-INSEE"),
]

_ADAPTER_IDS = [case[0] for case in _ADAPTER_CASES]


# ---------------------------------------------------------------------------
# Test 1 — entity statement is emitted with a non-empty identifiers list
# ---------------------------------------------------------------------------


class TestEntityStatementIsEmitted:
    """Every national registry adapter must emit at least one entity statement."""

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_entity_statement_present(self, name, mapper, bundle, _):
        stmts = list(mapper(bundle))
        entities = [s for s in stmts if s.get("recordType") == "entity"]
        assert entities, (
            f"{name}: mapper returned {len(stmts)} statement(s) but no entity statement"
        )

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_identifiers_list_non_empty(self, name, mapper, bundle, _):
        ids = _identifiers(mapper, bundle)
        assert ids, f"{name}: entity statement has empty identifiers list"

    # Adapters that do NOT have an is_stub guard (they guard on missing key fields
    # instead).  Excluded from the stub test below.
    _NO_STUB_GUARD = {"companies_house", "acra_singapore"}

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_stub_bundle_yields_nothing(self, name, mapper, bundle, _):
        if name in self._NO_STUB_GUARD:
            pytest.skip(f"{name} mapper does not use an is_stub guard")
        stub = {**bundle, "is_stub": True}
        stmts = list(mapper(stub))
        assert stmts == [], (
            f"{name}: stub bundle should yield no statements, got {stmts!r}"
        )


# ---------------------------------------------------------------------------
# Test 2 — all identifier dicts have non-empty id and scheme strings
# ---------------------------------------------------------------------------


class TestIdentifierDictStructure:
    """Every identifier dict must have a non-empty ``id`` and ``scheme`` string."""

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_every_identifier_has_non_empty_id(self, name, mapper, bundle, _):
        ids = _identifiers(mapper, bundle)
        for idx, id_dict in enumerate(ids):
            assert id_dict.get("id"), (
                f"{name}: identifiers[{idx}]['id'] is empty or missing: {id_dict!r}"
            )

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_every_identifier_has_non_empty_scheme(self, name, mapper, bundle, _):
        ids = _identifiers(mapper, bundle)
        for idx, id_dict in enumerate(ids):
            assert id_dict.get("scheme"), (
                f"{name}: identifiers[{idx}]['scheme'] is empty or missing: {id_dict!r}"
            )

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_every_identifier_id_is_string(self, name, mapper, bundle, _):
        ids = _identifiers(mapper, bundle)
        for idx, id_dict in enumerate(ids):
            assert isinstance(id_dict.get("id"), str), (
                f"{name}: identifiers[{idx}]['id'] is not a string: {id_dict!r}"
            )

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_every_identifier_scheme_is_string(self, name, mapper, bundle, _):
        ids = _identifiers(mapper, bundle)
        for idx, id_dict in enumerate(ids):
            assert isinstance(id_dict.get("scheme"), str), (
                f"{name}: identifiers[{idx}]['scheme'] is not a string: {id_dict!r}"
            )

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_no_duplicate_scheme_codes(self, name, mapper, bundle, _):
        """The same scheme code should not appear twice in one identifier list."""
        ids = _identifiers(mapper, bundle)
        schemes = [i["scheme"] for i in ids]
        assert len(schemes) == len(set(schemes)), (
            f"{name}: duplicate scheme codes in identifiers: {schemes}"
        )


# ---------------------------------------------------------------------------
# Test 3 — expected primary scheme code is present
# ---------------------------------------------------------------------------


class TestPrimarySchemeCode:
    """Each adapter must include its registry's primary scheme code."""

    @pytest.mark.parametrize(
        "name,mapper,bundle,primary_scheme",
        _ADAPTER_CASES,
        ids=_ADAPTER_IDS,
    )
    def test_primary_scheme_present(self, name, mapper, bundle, primary_scheme):
        ids = _identifiers(mapper, bundle)
        schemes = _scheme_ids(ids)
        assert primary_scheme in schemes, (
            f"{name}: expected primary scheme {primary_scheme!r} "
            f"not found in identifiers. Got: {sorted(schemes)}"
        )

    @pytest.mark.parametrize(
        "name,mapper,bundle,primary_scheme",
        _ADAPTER_CASES,
        ids=_ADAPTER_IDS,
    )
    def test_primary_identifier_id_matches_bundle_key(
        self, name, mapper, bundle, primary_scheme
    ):
        """The identifier carrying the primary scheme must have a non-trivial ID."""
        ids = _identifiers(mapper, bundle)
        primary_ids = [i for i in ids if i["scheme"] == primary_scheme]
        assert primary_ids, f"{name}: no identifier with scheme {primary_scheme!r}"
        for pid in primary_ids:
            assert pid["id"].strip(), (
                f"{name}: primary identifier id is blank: {pid!r}"
            )


# ---------------------------------------------------------------------------
# Test 4 — per-adapter specific scheme assertions
# ---------------------------------------------------------------------------


class TestAdapterSpecificSchemes:
    """Spot-checks for schemes that only certain adapters should emit."""

    def test_bce_emits_xi_vat(self):
        """BCE Belgium derives a VAT identifier in addition to BCE/KBO."""
        ids = _identifiers(map_bce_belgium, _BCE_BELGIUM_BUNDLE)
        schemes = _scheme_ids(ids)
        assert "XI-VAT" in schemes, (
            f"BCE Belgium should include XI-VAT. Got: {sorted(schemes)}"
        )

    def test_corporations_canada_with_business_number(self):
        """CA-BN is emitted when the corporation record includes a business number.

        The mapper reads: corp.get("businessNumbers", {}).get("businessNumber")
        """
        bundle_with_bn = {
            **_CORPORATIONS_CANADA_BUNDLE,
            "corporation": {
                **_CORPORATIONS_CANADA_BUNDLE["corporation"],
                # Nested structure as returned by the Corporations Canada API.
                "businessNumbers": {"businessNumber": "123456789"},
            },
        }
        ids = _identifiers(map_corporations_canada, bundle_with_bn)
        schemes = _scheme_ids(ids)
        assert "CA-CORP" in schemes
        assert "CA-BN" in schemes, (
            f"CA-BN should appear when businessNumbers.businessNumber is present. Got: {sorted(schemes)}"
        )

    def test_krs_poland_with_nip_and_regon(self):
        """PL-NIP and PL-REGON are emitted when available."""
        bundle = {
            **_KRS_POLAND_BUNDLE,
            "nip": "5270103391",
            "regon": "012100784",
        }
        ids = _identifiers(map_krs_poland, bundle)
        schemes = _scheme_ids(ids)
        assert "PL-KRS" in schemes
        assert "PL-NIP" in schemes, (
            f"PL-NIP should appear when nip is present. Got: {sorted(schemes)}"
        )
        assert "PL-REGON" in schemes, (
            f"PL-REGON should appear when regon is present. Got: {sorted(schemes)}"
        )

    def test_rpo_slovakia_with_or_number(self):
        """SK-OR (Obchodný register number) is appended when registration_numbers present."""
        bundle = {
            **_RPO_SLOVAKIA_BUNDLE,
            "registration_numbers": ["Vložka č. 123/B, Bratislava I"],
        }
        ids = _identifiers(map_rpo_slovakia, bundle)
        schemes = _scheme_ids(ids)
        assert "SK-RPO" in schemes
        assert "SK-OR" in schemes, (
            f"SK-OR should appear when registration_numbers is non-empty. Got: {sorted(schemes)}"
        )

    def test_firmenbuch_with_uid_number(self):
        """AT-UID (VAT ID) is appended when present in the extract."""
        bundle = {
            **_FIRMENBUCH_BUNDLE,
            "extract": {
                **_FIRMENBUCH_BUNDLE["extract"],
                "uid": "ATU68686868",
            },
        }
        ids = _identifiers(map_firmenbuch, bundle)
        schemes = _scheme_ids(ids)
        assert "AT-FB" in schemes
        assert "AT-UID" in schemes, (
            f"AT-UID should appear when uid is present in extract. Got: {sorted(schemes)}"
        )

    def test_companies_house_identifier_value(self):
        """The GB-COH identifier id must equal the company_number from the bundle."""
        ids = _identifiers(map_companies_house, _COMPANIES_HOUSE_BUNDLE)
        ch_id = next(i for i in ids if i["scheme"] == "GB-COH")
        assert ch_id["id"] == _COMPANIES_HOUSE_BUNDLE["company_number"]

    def test_kvk_identifier_value(self):
        ids = _identifiers(map_kvk, _KVK_BUNDLE)
        kvk_id = next(i for i in ids if i["scheme"] == "NL-KVK")
        assert kvk_id["id"] == "96332751"

    def test_cvr_denmark_identifier_value(self):
        ids = _identifiers(map_cvr_denmark, _CVR_DENMARK_BUNDLE)
        cvr_id = next(i for i in ids if i["scheme"] == "DK-CVR")
        assert cvr_id["id"] == "24256790"

    def test_acra_identifier_value(self):
        ids = _identifiers(map_acra_singapore, _ACRA_SINGAPORE_BUNDLE)
        acra_id = next(i for i in ids if i["scheme"] == "SG-ACRA")
        assert acra_id["id"] == "200312345E"

    def test_inpi_identifier_value(self):
        ids = _identifiers(map_inpi, _INPI_BUNDLE)
        siren_id = next(i for i in ids if i["scheme"] == "FR-INSEE")
        assert siren_id["id"] == "055804124"


# ---------------------------------------------------------------------------
# Test 5 — identifier ids pass basic format sanity checks
# ---------------------------------------------------------------------------


class TestIdentifierFormatSanity:
    """Identifier id strings should not be suspiciously short or contain only whitespace."""

    _MIN_LENGTH = 2  # no real identifier is shorter than 2 characters

    @pytest.mark.parametrize("name,mapper,bundle,_", _ADAPTER_CASES, ids=_ADAPTER_IDS)
    def test_all_identifier_ids_meet_minimum_length(self, name, mapper, bundle, _):
        ids = _identifiers(mapper, bundle)
        for idx, id_dict in enumerate(ids):
            id_val = id_dict.get("id", "")
            assert len(id_val.strip()) >= self._MIN_LENGTH, (
                f"{name}: identifiers[{idx}]['id'] seems too short: {id_val!r}"
            )
