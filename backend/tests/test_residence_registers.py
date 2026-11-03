"""Country of residence from four more registers (the residence sweep).

Follow-up to the Companies House country-of-residence ticket: the mapper sweep
found Latvia, New Zealand, EITI Armenia and Czechia publish a person's
residence country that OpenCheck dropped. Each is a country only — never a
street — through ``bods/residence.py``.
"""

from __future__ import annotations

import copy
import os
import sys
from typing import Any

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from opencheck.bods import map_ares, map_nz_companies, map_ur_latvia, validate_shape  # noqa: E402
from opencheck.bods.residence import residence_note  # noqa: E402
from opencheck.sources.ares import _extract_person  # noqa: E402
from opencheck.sources.nz_companies import (  # noqa: E402
    NzCompaniesAdapter,
    _residence_country,
)

_RESIDENCE = "residence"


def _residences(stmt: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        a for a in stmt["recordDetails"].get("addresses") or [] if a.get("type") == _RESIDENCE
    ]


def _people(stmts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    assert validate_shape(stmts) == []
    return [s for s in stmts if s["recordType"] == "person"]


def _country_notes(stmt: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        a for a in stmt.get("annotations") or []
        if a["statementPointerTarget"].endswith("/country")
    ]


def test_a_filed_iso_code_needs_no_note() -> None:
    """Latvia and New Zealand file the code itself; saying "files 'LV'; given
    as Latvia (LV)" on every person would be noise."""
    assert residence_note("LV", 0, register="X") is None
    assert residence_note("nz", 0, register="X") is None
    assert residence_note("GBR", 0, register="X") is None
    # A non-ISO abbreviation still says what was filed.
    assert residence_note("UK", 0, register="X") is not None


# ---------------------------------------------------------------------------
# Latvia — the BO resource's ``residence`` column
# ---------------------------------------------------------------------------


def _lv_bundle(**bo: Any) -> dict[str, Any]:
    return {
        "source_id": "ur_latvia",
        "lv_regcode": "40003000001",
        "entity": {"regcode": "40003000001", "name": "TESTS SIA", "registered": "2000-01-01"},
        "beneficial_owners": [
            {
                "id": "1", "forename": "Frederic", "surname": "Martin",
                "nationality": "FR", "birth_date": "1970-05-01",
                "registered_on": "2018-01-01", **bo,
            }
        ],
        "officers": [],
        "members": [],
    }


def test_latvia_beneficial_owner_carries_residence() -> None:
    (person,) = _people(list(map_ur_latvia(_lv_bundle(residence="LV"))))
    assert _residences(person) == [
        {"type": "residence", "country": {"name": "Latvia", "code": "LV"}}
    ]
    # Nationality and residence are different facts and both are kept.
    assert person["recordDetails"]["nationalities"][0]["code"] == "FR"
    assert _country_notes(person) == []


def test_latvia_owner_without_residence_has_no_address() -> None:
    (person,) = _people(list(map_ur_latvia(_lv_bundle())))
    assert _residences(person) == []


# ---------------------------------------------------------------------------
# New Zealand — residential blocks only, never an alternative address
# ---------------------------------------------------------------------------

_PHYS = {"address1": "19 Manakohi Street", "countryCode": "NZ", "addressType": "PHYSICAL"}
_ALT = {"address1": "Level 2, 100 Devon Street", "countryCode": "AU", "addressType": "ALTERNATIVE"}
_UNTYPED = {"address1": "1 Queen Street", "countryCode": "NZ", "addressType": None}


def test_nz_residence_country_reads_residential_blocks_only() -> None:
    assert _residence_country([_PHYS]) == "NZ"
    assert _residence_country([_UNTYPED]) == "NZ"
    # An alternative address is typically an agent's office: never residence.
    assert _residence_country([_ALT]) is None
    assert _residence_country([_ALT, _PHYS]) == "NZ"
    assert _residence_country([{**_PHYS, "endDate": "2020-01-01"}]) is None
    assert _residence_country(_UNTYPED) == "NZ"  # a shareholder's single block
    assert _residence_country(None) is None


def _nz_entity() -> dict[str, Any]:
    from test_nz_companies import FULL_ENTITY

    entity = copy.deepcopy(FULL_ENTITY)
    entity["roles"][0]["roleAddress"] = [dict(_ALT), {**_PHYS, "countryCode": "AU"}]
    holder = entity["company-details"]["shareholding"]["shareAllocation"][0]["shareholder"][0]
    holder["shareholderAddress"] = dict(_ALT)
    return entity


def test_nz_director_takes_the_residential_country_and_a_shareholder_on_alternative_gets_none() -> None:
    from test_nz_companies import _NUMBER, _NZBN

    bundle = NzCompaniesAdapter()._normalise(_NUMBER, _NZBN, _nz_entity(), legal_name="")
    people = {p["recordDetails"]["names"][0]["fullName"]: p for p in _people(list(map_nz_companies(bundle)))}
    john = next(p for name, p in people.items() if "John" in name)
    assert _residences(john) == [
        {"type": "residence", "country": {"name": "Australia", "code": "AU"}}
    ]
    jane = next(p for name, p in people.items() if "Jane" in name)
    assert _residences(jane) == []
    # No street from any block reaches a person statement.
    for person in people.values():
        for address in person["recordDetails"].get("addresses") or []:
            assert "address" not in address


# ---------------------------------------------------------------------------
# EITI Armenia — the v0.2 address's country only
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_armenia_person_carries_country_of_residence_and_no_street() -> None:
    from opencheck.bods import map_eiti_bo
    from opencheck.sources import eiti_bo

    eiti_bo._reset_index_for_tests()
    index, _meta = eiti_bo._load()
    lei = next(
        lei for lei, rec in index.items()
        if any(
            s.get("statementType") == "personStatement" and s.get("addresses")
            for s in ((rec.get("armenia") or {}).get("bods_v02") or [])
        )
    )
    stmts = list(map_eiti_bo(await eiti_bo.EitiBoAdapter().fetch_by_lei(lei)))
    with_residence = [p for p in _people(stmts) if _residences(p)]
    assert with_residence, "a person with a filed address should carry its country"
    for person in with_residence:
        for address in person["recordDetails"]["addresses"]:
            assert set(address) == {"type", "country"}
            assert len(address["country"]["code"]) == 2


# ---------------------------------------------------------------------------
# Czechia — the residence (bydliště) country of a natural person
# ---------------------------------------------------------------------------


def test_ares_person_extract_keeps_the_residence_country() -> None:
    person = _extract_person(
        {
            "fyzickaOsoba": {
                "jmeno": "Hans", "prijmeni": "Müller", "statniObcanstvi": "DE",
                "adresa": {"kodStatu": "AT", "textovaAdresa": "Hauptstraße 1, Wien"},
            }
        }
    )
    assert person is not None and person["residence_country"] == "AT"


def test_ares_owner_and_director_carry_residence() -> None:
    bundle = {
        "source_id": "ares",
        "cz_ico": "27082440",
        "is_stub": False,
        "entity": {"ico": "27082440", "name": "TEST a.s.", "address": "Praha"},
        "owners": [
            {"type": "person", "name": "Hans Müller", "given_name": "Hans",
             "family_name": "Müller", "nationality": "DE", "residence_country": "AT",
             "role": "shareholder", "start_date": "2020-01-01"},
        ],
        "former_owners": [],
        "directors": [
            {"type": "person", "name": "Jana Nováková", "nationality": "CZ",
             "residence_country": "CZ", "role": "director",
             "role_label": "Člen představenstva", "start_date": "2021-01-01"},
            {"type": "person", "name": "Petr Novák", "nationality": "CZ",
             "role": "director", "role_label": "Člen představenstva"},
        ],
    }
    people = {p["recordDetails"]["names"][0]["fullName"]: p for p in _people(list(map_ares(bundle)))}
    assert _residences(people["Hans Müller"]) == [
        {"type": "residence", "country": {"name": "Austria", "code": "AT"}}
    ]
    assert _residences(people["Jana Nováková"])[0]["country"]["code"] == "CZ"
    assert _residences(people["Petr Novák"]) == []


def test_lib_cove_bods_accepts_the_new_residences() -> None:
    pytest.importorskip("libcovebods")
    from test_bods_libcovebods import assert_valid

    assert_valid(list(map_ur_latvia(_lv_bundle(residence="LV"))), "UR Latvia residence")
