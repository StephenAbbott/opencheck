"""Companies House nationalities: read from the appointments, resolved to ISO.

The officer nationality ticket (4 Nov 2026): the officer-by-id path read
``nationality`` from the appointments envelope, which never carries one, and
no Companies House path wrote a ``code``, which is all ``cross_check`` compares.
"""

from __future__ import annotations

import pycountry
import pytest

from opencheck import cross_check
from opencheck.bods import map_companies_house
from opencheck.bods.nationality import DEMONYMS, ch_nationalities, nationality_code


@pytest.mark.parametrize(
    ("filed", "expected"),
    [
        ("British", [{"name": "British", "code": "GB"}]),
        ("English", [{"name": "English", "code": "GB"}]),
        ("Scottish", [{"name": "Scottish", "code": "GB"}]),
        ("American", [{"name": "American", "code": "US"}]),
        ("Chinese", [{"name": "Chinese", "code": "CN"}]),
        ("New Zealander", [{"name": "New Zealander", "code": "NZ"}]),
        ("Saudi Arabian", [{"name": "Saudi Arabian", "code": "SA"}]),
        ("Citizen Of Usa", [{"name": "Citizen Of Usa", "code": "US"}]),
        ("United States Citizen", [{"name": "United States Citizen", "code": "US"}]),
        ("Us", [{"name": "Us", "code": "US"}]),
        ("United Kingdom", [{"name": "United Kingdom", "code": "GB"}]),
        ("Hksar", [{"name": "Hksar", "code": "HK"}]),
        (
            "British,Australian",
            [{"name": "British", "code": "GB"}, {"name": "Australian", "code": "AU"}],
        ),
        ("Dutch And Usa", [{"name": "Dutch", "code": "NL"}, {"name": "Usa", "code": "US"}]),
        ("Irish Canadian", [{"name": "Irish", "code": "IE"}, {"name": "Canadian", "code": "CA"}]),
        (
            "Hong Konger,Chinese",
            [{"name": "Hong Konger", "code": "HK"}, {"name": "Chinese", "code": "CN"}],
        ),
        # No code rather than a stand-in.
        ("Other", [{"name": "Other"}]),
        ("Kosovan", [{"name": "Kosovan"}]),
        ("Congolese", [{"name": "Congolese"}]),
        ("", []),
        (None, []),
    ],
)
def test_filed_values_resolve(filed, expected) -> None:
    assert ch_nationalities(filed) == expected


def test_distinct_values_across_filings_dedupe_on_the_code() -> None:
    assert ch_nationalities(["British", "English", "Irish,British", "Other", "other"]) == [
        {"name": "British", "code": "GB"},
        {"name": "Irish", "code": "IE"},
        {"name": "Other"},
    ]


def test_every_demonym_names_an_iso_country() -> None:
    for demonym, code in DEMONYMS.items():
        assert demonym == demonym.casefold(), demonym
        assert pycountry.countries.get(alpha_2=code), (demonym, code)


def test_nationality_code_is_empty_for_nothing() -> None:
    assert nationality_code("  ") == ""
    assert nationality_code("Citizen Of") == ""


# --- officer by id: the live envelope ---------------------------------------


def _officer_bundle() -> dict:
    """The keys of a live ``/officers/{id}/appointments`` answer (3 Nov 2026):
    no ``nationality`` and no ``country_of_residence`` on the envelope."""
    return {
        "officer_id": "abc123",
        "appointments": {
            "active_count": 2,
            "date_of_birth": {"year": 1970, "month": 4},
            "etag": "x",
            "inactive_count": 0,
            "is_corporate_officer": False,
            "items_per_page": 35,
            "kind": "personal-appointment",
            "links": {"self": "/officers/abc123/appointments"},
            "name": "Jane EXAMPLE",
            "resigned_count": 1,
            "start_index": 0,
            "total_results": 3,
            "items": [
                {
                    "appointed_to": {"company_name": "OLD CO LTD", "company_number": "00000003"},
                    "officer_role": "director",
                    "appointed_on": "2001-01-01",
                    "resigned_on": "2010-01-01",
                    "nationality": "Irish",
                },
                {
                    "appointed_to": {"company_name": "ACME LTD", "company_number": "00000001"},
                    "officer_role": "director",
                    "appointed_on": "2015-01-01",
                    "nationality": "British",
                },
                {
                    "appointed_to": {"company_name": "NEW CO LTD", "company_number": "00000002"},
                    "officer_role": "director",
                    "appointed_on": "2020-01-01",
                    "nationality": "British,Australian",
                },
            ],
        },
    }


def _person(bundle) -> dict:
    return next(s for s in bundle if s["recordType"] == "person")


def test_officer_by_id_reads_every_appointments_nationality() -> None:
    person = _person(map_companies_house(_officer_bundle()))
    # Current appointments latest first, then the resigned one.
    assert person["recordDetails"]["nationalities"] == [
        {"name": "British", "code": "GB"},
        {"name": "Australian", "code": "AU"},
        {"name": "Irish", "code": "IE"},
    ]


def test_an_envelope_nationality_comes_first_if_the_register_adds_one() -> None:
    raw = _officer_bundle()
    raw["appointments"]["nationality"] = "Canadian"
    nats = _person(map_companies_house(raw))["recordDetails"]["nationalities"]
    assert [n["code"] for n in nats] == ["CA", "GB", "AU", "IE"]


def test_officer_with_no_nationality_filed_has_none() -> None:
    raw = _officer_bundle()
    for item in raw["appointments"]["items"]:
        item.pop("nationality")
    rd = _person(map_companies_house(raw))["recordDetails"]
    assert not rd.get("nationalities")


# --- the screens can now read it --------------------------------------------


def test_cross_check_reads_the_code_a_ch_person_now_carries() -> None:
    rd = _person(map_companies_house(_officer_bundle()))["recordDetails"]
    codes = cross_check._person_nationalities(rd)
    assert codes == ("GB", "AU", "IE")
    target = {"birth_year": None, "nationalities": codes}
    assert cross_check.corroborating_attributes(
        target, {"properties": {"nationality": ["gb"]}}
    ) == ("nationality",)
    assert cross_check.corroborating_attributes(
        target, {"properties": {"nationality": ["fr"]}}
    ) == ()
