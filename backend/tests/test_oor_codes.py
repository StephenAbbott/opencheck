"""ISO 5009 Official Organizational Role codes on the interest (the ISO 5009
roles ticket, Nov 2026).

A code is a role *in one legal form*: it goes on the relationship interest as
``officialOrganizationalRole``, never on the person statement (Stephen,
13 Nov 2026), and only from a source's reviewed ``(ELF, role) → code`` table.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck.bods import oor
from opencheck.bods.mappers.companies_house import CH_OOR, CH_TYPE_ELF, map_companies_house
from opencheck.bods.mappers.estonia import EE_FORM_ELF, EE_OOR, _ee_company_parties
from opencheck.bods.mappers.new_zealand import NZ_OOR, NZ_TYPE_ELF, map_nz_companies
from opencheck.bods.oor import OOR_KEY, mark_role, oor_entry, role_object

#: Every source table, with the country its codes must be filed under.
TABLES: list[tuple[str, str, dict[tuple[str, str], str], dict[str, str]]] = [
    ("companies_house", "GB", CH_OOR, CH_TYPE_ELF),
    ("ariregister", "EE", EE_OOR, EE_FORM_ELF),
    ("nz_companies", "NZ", NZ_OOR, NZ_TYPE_ELF),
]


def test_the_vendored_list_is_gleifs_1_0_0() -> None:
    assert oor.oor_version() == "1.0.0"
    assert len(oor._load()["roles"]) == 2127
    assert oor_entry("UDU176")["elfs"] == ["H0PO"]
    assert oor.oor_name(oor_entry("UDU176")) == "Director"
    # No English row: the list's local name.
    assert oor.oor_name(oor_entry("5RJVJB")) == "Juhatuse liige"


@pytest.mark.parametrize("source, country, table, elfs", TABLES, ids=[t[0] for t in TABLES])
def test_every_table_entry_is_a_code_the_list_files_under_that_form(
    source: str, country: str, table: dict, elfs: dict
) -> None:
    for (elf, role), code in table.items():
        entry = oor_entry(code)
        assert entry is not None, f"{source}: {code} is not in the list"
        assert entry["country"] == country, f"{source}: {code} is {entry['country']}'s"
        assert entry["subdivision"] is None
        assert elf in entry["elfs"], f"{source}: {code} is not filed under {elf}"
        assert elf in elfs.values(), f"{source}: no legal form maps to {elf}"


def test_a_wrong_code_fails_loudly_rather_than_reach_a_statement() -> None:
    with pytest.raises(KeyError):
        role_object("ZZZZZZ", "H0PO")
    with pytest.raises(ValueError):
        role_object("UDU176", "B6ES")


def test_no_table_entry_no_code() -> None:
    interest = {"type": "seniorManagingOfficial"}
    assert mark_role(interest, CH_OOR, "H0PO", "secretary") == {"type": "seniorManagingOfficial"}
    assert mark_role(interest, CH_OOR, None, "director") == {"type": "seniorManagingOfficial"}


# ---------------------------------------------------------------------------
# Companies House
# ---------------------------------------------------------------------------


def _ch_bundle(company_type: str) -> dict[str, Any]:
    return {
        "company_number": "01234567",
        "profile": {"company_name": "EXAMPLE LIMITED", "type": company_type},
        "officers": {"items": [
            {"name": "SMITH, Jane", "officer_role": "director", "appointed_on": "2015-01-01",
             "links": {"officer": {"appointments": "/officers/abc123/appointments"}}},
            {"name": "BROWN, Sam", "officer_role": "llp-designated-member",
             "links": {"officer": {"appointments": "/officers/def456/appointments"}}},
        ]},
        "pscs": {"items": [
            {"kind": "individual-person-with-significant-control", "name": "Ms Jane Smith",
             "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
             "notified_on": "2016-04-06", "links": {"self": "/company/01234567/persons-with-significant-control/individual/p1"}},
            {"kind": "corporate-entity-person-with-significant-control", "name": "HOLDCO LIMITED",
             "natures_of_control": ["ownership-of-shares-25-to-50-percent"],
             "identification": {"registration_number": "07654321", "country_registered": "England",
                                "place_registered": "Companies House", "legal_authority": "Companies Act 2006"},
             "notified_on": "2016-04-06", "links": {"self": "/company/01234567/persons-with-significant-control/corporate-entity/c1"}},
        ]},
    }


def _roles(statements: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
    out = []
    for s in statements:
        if s["recordType"] != "relationship":
            continue
        for i in s["recordDetails"].get("interests") or []:
            out.append(oor.official_role(i))
    return out


def _codes(statements: list[dict[str, Any]]) -> set[str | None]:
    return {r["code"] if r else None for r in _roles(statements)}


def test_a_private_limited_companys_director_and_psc_are_coded() -> None:
    statements = map_companies_house(_ch_bundle("ltd")).statements
    codes = _codes(statements)
    assert "UDU176" in codes  # Director, Private Limited Company
    assert "DIUHJM" in codes  # Person with significant control
    # The corporate PSC (a relevant legal entity) is not a person with
    # significant control; it gets no code.
    assert None in codes
    director = next(r for r in _roles(statements) if r and r["code"] == "UDU176")
    assert director == {"scheme": "ISO 5009", "code": "UDU176", "name": "Director", "legalForm": "H0PO"}


def test_a_plc_director_takes_the_plc_code() -> None:
    assert "VXSNEB" in _codes(map_companies_house(_ch_bundle("plc")).statements)


def test_a_type_iso_5009_does_not_list_gets_no_code() -> None:
    assert _codes(map_companies_house(_ch_bundle("private-unlimited")).statements) == {None}


def test_an_llp_member_is_coded_only_in_an_llp() -> None:
    assert "FKY9GU" in _codes(map_companies_house(_ch_bundle("llp")).statements)
    assert "FKY9GU" not in _codes(map_companies_house(_ch_bundle("ltd")).statements)


def test_the_code_is_never_on_a_person_statement() -> None:
    for s in map_companies_house(_ch_bundle("ltd")).statements:
        if s["recordType"] != "person":
            continue
        rd = s["recordDetails"]
        assert not any("5009" in str(i.get("scheme")) for i in rd.get("identifiers") or [])
        assert OOR_KEY not in str(s)


# ---------------------------------------------------------------------------
# Estonia
# ---------------------------------------------------------------------------


def _ee_bundle(legal_form: str) -> dict[str, Any]:
    person = {"isiku_tyyp": "F", "eesnimi": "Mari", "nimi_arinimi": "Maasikas",
              "isikukood_hash": "11111111-1111-1111-1111-111111111111"}
    return {
        "legal_form": legal_form,
        "shareholders": [{**person, "kirje_id": "1", "osaluse_protsent": "100"}],
        "officers": [
            {**person, "kirje_id": "2", "isiku_roll": "JUHL", "algus_kpv": "2020-01-01"},
            {**person, "kirje_id": "3", "isiku_roll": "JUHE", "algus_kpv": "2020-01-01"},
        ],
        "off_card_persons": [{**person, "kirje_id": "4", "isiku_roll": "N"}],
        "beneficial_owners": [],
    }


def _ee_codes(legal_form: str) -> list[str | None]:
    statements = list(_ee_company_parties(_ee_bundle(legal_form), "opencheck-subject", "https://x", set(), set(), include_founders=True))
    return [r["code"] if r else None for r in _roles(statements)]


def test_an_ou_s_shareholder_board_member_chair_and_council_member_are_coded() -> None:
    assert _ee_codes("Osaühing") == ["PMWIUQ", "5RJVJB", "HK0BV9", "K710NY"]
    # The print page's English name for the same form.
    assert _ee_codes("Private limited company") == ["PMWIUQ", "5RJVJB", "HK0BV9", "K710NY"]


def test_an_as_chair_is_left_uncoded_because_the_list_does_not_say_which_board() -> None:
    assert _ee_codes("Aktsiaselts") == ["XFIHZL", "3TZ9PM", None, "TV1LWM"]


def test_a_form_iso_5009_does_not_list_gets_no_code() -> None:
    assert set(_ee_codes("Täisühing")) == {None}


# ---------------------------------------------------------------------------
# New Zealand
# ---------------------------------------------------------------------------


def _nz_bundle(entity_type: str) -> dict[str, Any]:
    return {
        "nz_company_number": "1234567",
        "nzbn": "9429000000000",
        "company": {"name": "EXAMPLE LIMITED", "entity_type": entity_type},
        "roles": [{"kind": "person", "name": "Jane Smith", "role_type": "Director"}],
        "shareholders": [{"kind": "person", "name": "Jane Smith", "percent": 60.0}],
    }


def test_a_nz_limited_companys_director_and_shareholder_are_coded() -> None:
    assert _codes(list(map_nz_companies(_nz_bundle("NZ Limited Company")))) == {"OQDTGX", "XSLEA6"}


def test_a_nz_co_operative_gets_no_code() -> None:
    assert _codes(list(map_nz_companies(_nz_bundle("NZ Co-operative Company")))) == {None}
