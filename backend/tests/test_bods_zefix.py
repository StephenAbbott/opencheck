"""Tests for the Zefix → BODS v0.4 mapper and UID utilities."""

from __future__ import annotations

import pytest

from opencheck.bods import map_zefix, validate_shape
from opencheck.sources.zefix import format_uid, normalise_uid

# ---------------------------------------------------------------------------
# Sample fixtures (based on Zefix CompanyFull API schema)
# ---------------------------------------------------------------------------

_COMPANY_GSSB = {
    "name": "GSSB GmbH",
    "ehraid": 348639,
    "uid": "CHE313550547",
    "chid": "CH12345678901234",
    "legalSeatId": 1509,
    "legalSeat": "Stans",
    "registryOfCommerceId": 16,
    "legalForm": {
        "id": 23,
        "uid": "0106",
        "name": {"de": "Gesellschaft mit beschränkter Haftung", "en": "Limited liability company", "fr": "Société à responsabilité limitée", "it": "Società a responsabilità limitata"},
        "shortName": {"de": "GmbH", "en": "LLC", "fr": "Sàrl", "it": "Sagl"},
    },
    "status": "ACTIVE",
    "canton": "NW",
    "capitalNominal": "20000",
    "capitalCurrency": "CHF",
    "purpose": "Consulting services in the field of software development.",
    "address": {
        "organisation": None,
        "careOf": None,
        "street": "Hans-von-Matt Weg",
        "houseNumber": "1",
        "addon": None,
        "poBox": None,
        "city": "Stans",
        "swissZipCode": "6370",
    },
    "zefixDetailWeb": {
        "de": "https://www.zefix.ch/de/search/entity/list/firm/348639",
        "en": "https://www.zefix.ch/en/search/entity/list/firm/348639",
        "fr": "https://www.zefix.ch/fr/search/entity/list/firm/348639",
        "it": "https://www.zefix.ch/it/search/entity/list/firm/348639",
    },
    "cantonalExcerptWeb": "https://hr.nw.ch/uid/CHE-313.550.547",
    "oldNames": [],
    "translation": ["GSSB LLC", "GSSB Sàrl"],
    "headOffices": [],
    "branchOffices": [],
}

_COMPANY_DOTTED_UID = {
    "name": "DeWit Biosciences GmbH",
    "ehraid": 999001,
    "uid": "CHE-346.487.424",  # dotted format as returned by some validators
    "legalSeatId": 2401,
    "legalSeat": "Walchwil",
    "legalForm": {
        "id": 23,
        "uid": "0106",
        "name": {"de": "GmbH", "en": "LLC"},
        "shortName": {"de": "GmbH", "en": "LLC"},
    },
    "status": "ACTIVE",
    "canton": "ZG",
    "address": {
        "street": "Zugerstrasse",
        "houseNumber": "44D",
        "city": "Walchwil",
        "swissZipCode": "6318",
    },
}

_COMPANY_NO_CANTON = {
    "name": "No Canton Corp AG",
    "ehraid": 999002,
    "uid": "CHE111222333",
    "legalForm": {"id": 3, "uid": "0101", "name": {"en": "Corporation"}, "shortName": {"en": "AG"}},
    "status": "ACTIVE",
    "address": {},
}


def _bundle(company: dict | None = None) -> dict:
    return {
        "source_id": "zefix",
        "uid": (company or _COMPANY_GSSB).get("uid", "CHE313550547"),
        "company": company or _COMPANY_GSSB,
        "is_stub": False,
    }


# ---------------------------------------------------------------------------
# UID normalisation / formatting utilities
# ---------------------------------------------------------------------------


def test_normalise_uid_no_separators() -> None:
    assert normalise_uid("CHE313550547") == "CHE313550547"


def test_normalise_uid_dotted() -> None:
    assert normalise_uid("CHE-313.550.547") == "CHE313550547"


def test_normalise_uid_mixed() -> None:
    assert normalise_uid("CHE313.550.547") == "CHE313550547"


def test_format_uid_round_trip() -> None:
    assert format_uid("CHE313550547") == "CHE-313.550.547"


def test_format_uid_already_formatted() -> None:
    assert format_uid("CHE-313.550.547") == "CHE-313.550.547"


# ---------------------------------------------------------------------------
# map_zefix — entity statement
# ---------------------------------------------------------------------------


def test_map_zefix_produces_entity() -> None:
    stmts = list(map_zefix(_bundle()))
    assert len(stmts) == 1
    assert stmts[0]["recordType"] == "entity"


def test_map_zefix_entity_name() -> None:
    stmts = list(map_zefix(_bundle()))
    assert stmts[0]["recordDetails"]["name"] == "GSSB GmbH"


def test_map_zefix_entity_jurisdiction_canton() -> None:
    stmts = list(map_zefix(_bundle()))
    jur = stmts[0]["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "CH-NW"
    assert "Nidwalden" in jur["name"]
    assert "Switzerland" in jur["name"]


def test_map_zefix_entity_jurisdiction_no_canton() -> None:
    """When canton is absent, jurisdiction falls back to country-level CH."""
    stmts = list(map_zefix(_bundle(_COMPANY_NO_CANTON)))
    jur = stmts[0]["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "CH"


def test_map_zefix_identifier_ch_uid() -> None:
    stmts = list(map_zefix(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "CH-UID" in schemes


def test_map_zefix_identifier_uid_formatted() -> None:
    """The CH-UID identifier should use the CHE-NNN.NNN.NNN display format."""
    stmts = list(map_zefix(_bundle()))
    uid_id = next(
        i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "CH-UID"
    )
    assert uid_id == "CHE-313.550.547"


def test_map_zefix_identifier_ehraid() -> None:
    stmts = list(map_zefix(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "CH-ZEFIX" in schemes
    ehraid_id = next(
        i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "CH-ZEFIX"
    )
    assert ehraid_id == "348639"


def test_map_zefix_address() -> None:
    stmts = list(map_zefix(_bundle()))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert len(addrs) == 1
    assert "Hans-von-Matt Weg" in addrs[0]["address"]
    assert "Stans" in addrs[0]["address"]
    assert addrs[0]["country"] == {"name": "Switzerland", "code": "CH"}


def test_map_zefix_empty_address_omitted() -> None:
    stmts = list(map_zefix(_bundle(_COMPANY_NO_CANTON)))
    addrs = stmts[0]["recordDetails"].get("addresses", [])
    assert addrs == []


def test_map_zefix_dotted_uid_normalised() -> None:
    """A company whose uid is stored in dotted format should still map cleanly."""
    stmts = list(map_zefix(_bundle(_COMPANY_DOTTED_UID)))
    assert len(stmts) == 1
    uid_id = next(
        i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "CH-UID"
    )
    assert uid_id == "CHE-346.487.424"


def test_map_zefix_stub_returns_empty() -> None:
    bundle = {"source_id": "zefix", "uid": "CHE123456789", "company": None, "is_stub": True}
    assert list(map_zefix(bundle)) == []


def test_map_zefix_empty_company_returns_empty() -> None:
    bundle = {"source_id": "zefix", "uid": "", "company": {}, "is_stub": False}
    assert list(map_zefix(bundle)) == []


# ---------------------------------------------------------------------------
# BODS validator compliance
# ---------------------------------------------------------------------------


def test_map_zefix_passes_validator() -> None:
    issues = validate_shape(map_zefix(_bundle()))
    assert issues == [], issues


def test_map_zefix_dotted_uid_passes_validator() -> None:
    issues = validate_shape(map_zefix(_bundle(_COMPANY_DOTTED_UID)))
    assert issues == [], issues


def test_map_zefix_no_canton_passes_validator() -> None:
    issues = validate_shape(map_zefix(_bundle(_COMPANY_NO_CANTON)))
    assert issues == [], issues
