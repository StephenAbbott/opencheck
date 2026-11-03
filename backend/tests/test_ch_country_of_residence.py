"""Companies House country of residence → a BODS ``residence`` address.

Notion "Map PSC country of residence into BODS person statements", from the
check against Open Ownership's usable beneficial ownership data guidance
(feature 14, relevant person information). Companies House files
``country_of_residence`` on every individual PSC and officer; until this
ticket the mapper dropped it, so OpenCheck published no residence for a UK
person at all.

The resolver's cases come from the strings in OpenCheck's local Companies
House cache on 3 Nov 2026 (5,500 values; "England" 2,769, "United Kingdom"
1,693, then a long tail including "Uk", "U.K.", "Untied Kingdom", "Gb-Eng",
"Hong Kong Sar", "Channel Islands", "New York" and "Dollar").
"""

from __future__ import annotations

import os
import sys
from typing import Any

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from opencheck.bods import map_companies_house, validate_shape  # noqa: E402
from opencheck.bods.residence import (  # noqa: E402
    ALIASES,
    UNRESOLVABLE_EXAMPLES,
    resolve_country,
    residence_address,
    residence_note,
)
from opencheck.icij_check import _KIND_PERSON, _party_facts  # noqa: E402

_NUMBER = "00102498"


# ---------------------------------------------------------------------------
# The resolver
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("filed", "code"),
    [
        ("England", "GB"),
        ("Scotland", "GB"),
        ("Wales", "GB"),
        ("Northern Ireland", "GB"),
        ("United Kingdom", "GB"),
        ("Uk", "GB"),
        ("U.K.", "GB"),
        ("Untied Kingdom", "GB"),
        ("Great Britain", "GB"),
        ("Gb-Eng", "GB"),
        ("GBR", "GB"),
        ("  usa ", "US"),
        ("United States Of America", "US"),
        ("United States America", "US"),
        ("Russia", "RU"),
        ("Turkey", "TR"),
        ("Iran", "IR"),
        ("Hong Kong Sar", "HK"),
        ("Isle Of Man", "IM"),
        ("Jersey", "JE"),
        ("Virgin Islands, British", "VG"),
        ("Republic Of Ireland", "IE"),
        ("Phillipines", "PH"),
        ("South Korea", "KR"),
        ("Georgia", "GE"),
    ],
)
def test_a_filed_country_resolves_to_its_iso_code(filed: str, code: str) -> None:
    country = resolve_country(filed)
    assert country is not None and country.get("code") == code, (filed, country)
    assert country["name"]


@pytest.mark.parametrize("filed", [*UNRESOLVABLE_EXAMPLES, "Dollar"])
def test_an_unresolvable_string_keeps_its_name_and_gets_no_code(filed: str) -> None:
    """A stand-in code would reach the ISO-keyed risk lists (the Phase 239
    input contract); BODS requires a name, so the filer's words are it."""
    assert resolve_country(filed) == {"name": filed}


def test_nothing_filed_is_no_country() -> None:
    for empty in (None, "", "   "):
        assert resolve_country(empty) is None
        assert residence_address(empty) is None


def test_every_alias_names_a_real_iso_country() -> None:
    import pycountry

    for spelling, code in ALIASES.items():
        assert pycountry.countries.get(alpha_2=code) is not None, spelling
        assert spelling == spelling.lower().strip(), spelling


def test_a_residence_address_is_a_country_and_nothing_else() -> None:
    """Never a street: the register publishes none, and the Estonia rule."""
    address = residence_address("England")
    assert address == {"type": "residence", "country": {"name": "United Kingdom", "code": "GB"}}
    assert set(residence_address("Channel Islands") or {}) == {"type", "country"}


def test_the_note_says_what_the_register_filed() -> None:
    renamed = residence_note("England", 1, register="Companies House")
    assert renamed is not None
    assert renamed["motivation"] == "transformation"
    assert renamed["statementPointerTarget"] == "/recordDetails/addresses/1/country"
    assert "'England'" in renamed["description"] and "(GB)" in renamed["description"]

    unresolved = residence_note("Channel Islands", 0, register="Companies House")
    assert unresolved is not None and unresolved["motivation"] == "commenting"
    assert "'Channel Islands'" in unresolved["description"]
    assert "no country code" in unresolved["description"]

    # The filed words are the published name (case aside): nothing to say.
    assert residence_note("Isle Of Man", 0, register="Companies House") is None
    assert residence_note("Ireland", 0, register="Companies House") is None
    assert residence_note(None, 0, register="Companies House") is None


# ---------------------------------------------------------------------------
# The three Companies House person paths
# ---------------------------------------------------------------------------


def _psc(**extra: Any) -> dict[str, Any]:
    return {
        "name": "JONES, Alice",
        "kind": "individual-person-with-significant-control",
        "natures_of_control": ["ownership-of-shares-25-to-50-percent"],
        "notified_on": "2016-04-06",
        "date_of_birth": {"year": 1980, "month": 7},
        "nationality": "British",
        "address": {
            "premises": "1",
            "address_line_1": "High Street",
            "locality": "London",
            "postal_code": "EC1A 1AA",
            "country": "England",
        },
        **extra,
    }


def _director(**extra: Any) -> dict[str, Any]:
    return {
        "name": "SMITH, Jane",
        "officer_role": "director",
        "appointed_on": "2018-06-01",
        "date_of_birth": {"year": 1975, "month": 3},
        "nationality": "British",
        "links": {"officer": {"appointments": "/officers/abc123/appointments"}},
        **extra,
    }


def _company(*, pscs: list[dict[str, Any]] = (), officers: list[dict[str, Any]] = ()) -> dict:
    return {
        "source_id": "companies_house",
        "company_number": _NUMBER,
        "profile": {"company_name": "DEMO HOLDINGS PLC"},
        "officers": {"items": list(officers)},
        "pscs": {"items": list(pscs)},
        "related_companies": {},
    }


def _people(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    statements = list(map_companies_house(bundle))
    assert validate_shape(statements) == []
    return [s for s in statements if s["recordType"] == "person"]


def _residences(person: dict[str, Any]) -> list[dict[str, Any]]:
    return [a for a in person["recordDetails"].get("addresses") or [] if a.get("type") == "residence"]


def test_an_individual_psc_carries_its_country_of_residence() -> None:
    (person,) = _people(_company(pscs=[_psc(country_of_residence="Scotland")]))
    addresses = person["recordDetails"]["addresses"]
    # The service address is unchanged and comes first.
    assert addresses[0]["type"] == "service"
    assert addresses[0]["address"].startswith("1, High Street")
    assert addresses[1] == {
        "type": "residence",
        "country": {"name": "United Kingdom", "code": "GB"},
    }
    notes = [
        a for a in person.get("annotations") or []
        if a["statementPointerTarget"] == "/recordDetails/addresses/1/country"
    ]
    assert len(notes) == 1 and "'Scotland'" in notes[0]["description"]


def test_an_unresolvable_residence_is_published_by_name_with_a_comment() -> None:
    (person,) = _people(_company(pscs=[_psc(country_of_residence="Channel Islands")]))
    assert _residences(person) == [{"type": "residence", "country": {"name": "Channel Islands"}}]
    (note,) = [
        a for a in person.get("annotations") or []
        if a["statementPointerTarget"].endswith("/country")
    ]
    assert note["motivation"] == "commenting"


def test_a_psc_without_a_country_of_residence_carries_no_residence() -> None:
    (person,) = _people(_company(pscs=[_psc()]))
    assert _residences(person) == []
    assert not any(
        a["statementPointerTarget"].endswith("/country") for a in person.get("annotations") or []
    )


def test_a_director_carries_its_country_of_residence() -> None:
    (person,) = _people(_company(officers=[_director(country_of_residence="United States")]))
    assert _residences(person) == [
        {"type": "residence", "country": {"name": "United States", "code": "US"}}
    ]
    # Filed as the ISO name: no annotation.
    assert not any(
        a["statementPointerTarget"].endswith("/country") for a in person.get("annotations") or []
    )


def _appointments(items: list[dict[str, Any]], **envelope: Any) -> dict[str, Any]:
    return {
        "source_id": "companies_house",
        "officer_id": "zS_RY9pRYlJ9XwGJEOFtkJgrf8s",
        "appointments": {
            "name": "Jane SMITH",
            "date_of_birth": {"year": 1975, "month": 8},
            "is_corporate_officer": False,
            "items": items,
            **envelope,
        },
    }


def _appointment(number: str, *, appointed: str, resigned: str | None = None, cor: str | None) -> dict:
    item: dict[str, Any] = {
        "appointed_to": {"company_name": f"CO {number}", "company_number": number},
        "officer_role": "director",
        "appointed_on": appointed,
    }
    if resigned:
        item["resigned_on"] = resigned
    if cor is not None:
        item["country_of_residence"] = cor
    return item


def test_an_officer_looked_up_by_id_takes_the_residence_of_its_latest_current_appointment() -> None:
    """The envelope carries no country of residence; the appointments do, as
    filed for each company. A current appointment beats a resigned one, and
    the later of two current ones wins."""
    (person,) = _people(
        _appointments([
            _appointment("00000001", appointed="2010-01-01", resigned="2025-01-01", cor="Spain"),
            _appointment("00000002", appointed="2015-01-01", cor="France"),
            _appointment("00000003", appointed="2020-01-01", cor="Ireland"),
            _appointment("00000004", appointed="2022-01-01", cor=None),
        ])
    )
    assert _residences(person) == [
        {"type": "residence", "country": {"name": "Ireland", "code": "IE"}}
    ]


def test_an_officer_with_only_resigned_appointments_takes_the_latest_resignation() -> None:
    (person,) = _people(
        _appointments([
            _appointment("00000001", appointed="2001-01-01", resigned="2005-01-01", cor="Spain"),
            _appointment("00000002", appointed="2003-01-01", resigned="2012-01-01", cor="Wales"),
        ])
    )
    assert _residences(person) == [
        {"type": "residence", "country": {"name": "United Kingdom", "code": "GB"}}
    ]
    assert person["recordDetails"]["addresses"] == _residences(person)


def test_an_officer_with_no_residence_filed_anywhere_carries_none() -> None:
    (person,) = _people(_appointments([_appointment("00000001", appointed="2020-01-01", cor=None)]))
    assert "addresses" not in person["recordDetails"] or _residences(person) == []


# ---------------------------------------------------------------------------
# What the risk engine reads
# ---------------------------------------------------------------------------


def test_the_icij_gate_reads_the_residence_and_not_the_service_address() -> None:
    """Phase 282 excluded a person's service address from the ICIJ
    jurisdiction gate (it is usually the company's office). The residence is
    the country the gate was waiting for."""
    (person,) = _people(_company(pscs=[_psc(country_of_residence="Spain")]))
    _born, countries = _party_facts(person["recordDetails"], _KIND_PERSON)
    assert countries == {"ES"}

    (no_residence,) = _people(_company(pscs=[_psc()]))
    _born, countries = _party_facts(no_residence["recordDetails"], _KIND_PERSON)
    assert countries == set()


# ---------------------------------------------------------------------------
# The standard
# ---------------------------------------------------------------------------


def test_lib_cove_bods_accepts_a_residence_with_and_without_a_code() -> None:
    pytest.importorskip("libcovebods")
    from test_bods_libcovebods import assert_valid

    statements = list(
        map_companies_house(
            _company(
                pscs=[_psc(country_of_residence="England")],
                officers=[_director(country_of_residence="Channel Islands")],
            )
        )
    )
    assert_valid(statements, "Companies House residence")
    assert_valid(
        list(
            map_companies_house(
                _appointments([_appointment("00000001", appointed="2020-01-01", cor="Hong Kong Sar")])
            )
        ),
        "Companies House officer residence",
    )
