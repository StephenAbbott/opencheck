"""Phase 293 — Companies House corporate officers are entities, not persons.

METASTAR INVEST LLP (OC346224), an Azerbaijani Laundromat vehicle, has two
designated members, ADVANCE DEVELOPMENTS LIMITED and CORPORATE SOLUTIONS
LIMITED — Belize IBCs. Until Phase 293 both were mapped as ``knownPerson``.
The fixtures below are the officers register's own shapes (6 Oct 2026),
including the free-text registration fields other laundromat LLPs file.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from opencheck.bods import map_companies_house, validate_shape  # noqa: E402
from opencheck.bods.mappers.companies_house import _country_in_text  # noqa: E402

BELIZE_ADDRESS = {
    "premises": "35",
    "address_line_1": "Barrack Road",
    "address_line_2": "3rd Floor",
    "locality": "Belize City",
    "region": "C A",
    "country": "Belize",
}


def _corporate_member(name: str, number: str, officer_id: str, **ident) -> dict:
    identification = {
        "identification_type": "non-eea",
        "legal_authority": "INTERNATIONAL BUSINESS COMPANIES ACT 1990, BELIZE",
        "legal_form": "LIMITED",
        "place_registered": "REGISTRAR OF INTERNATIONAL BUSINESS COMPANIES",
        "registration_number": number,
    }
    identification.update(ident)
    return {
        "name": name,
        "officer_role": "corporate-llp-designated-member",
        "appointed_on": "2009-06-08",
        "address": dict(BELIZE_ADDRESS),
        "identification": identification,
        "links": {"officer": {"appointments": f"/officers/{officer_id}/appointments"}},
    }


def _bundle(items: list[dict], number: str = "OC346224", name: str = "METASTAR INVEST LLP") -> dict:
    return {
        "source_id": "companies_house",
        "company_number": number,
        "profile": {"company_name": name, "type": "llp"},
        "officers": {"items": items},
        "pscs": {"items": []},
        "related_companies": {},
    }


METASTAR = _bundle([
    _corporate_member("ADVANCE DEVELOPMENTS LIMITED", "36,265", "ADVDEV1"),
    _corporate_member("CORPORATE SOLUTIONS LIMITED", "36,269", "CORPSOL1"),
])


def _statements(bundle: dict) -> list[dict]:
    return list(map_companies_house(bundle))


def _by_type(stmts: list[dict], kind: str) -> list[dict]:
    return [s for s in stmts if s["recordType"] == kind]


def test_metastar_members_are_entities_with_jurisdiction_bz() -> None:
    stmts = _statements(METASTAR)
    assert validate_shape(stmts) == []
    assert _by_type(stmts, "person") == []
    entities = _by_type(stmts, "entity")
    assert len(entities) == 3  # the LLP and its two members
    members = {e["recordDetails"]["name"]: e["recordDetails"] for e in entities[1:]}
    assert set(members) == {"ADVANCE DEVELOPMENTS LIMITED", "CORPORATE SOLUTIONS LIMITED"}
    adv = members["ADVANCE DEVELOPMENTS LIMITED"]
    assert adv["entityType"]["type"] == "registeredEntity"
    assert adv["jurisdiction"] == {"name": "Belize", "code": "BZ"}
    # Display grouping removed, the number keyed under the country's register.
    assert adv["identifiers"] == [{
        "id": "36265", "scheme": "REG-BZ",
        "schemeName": "REGISTRAR OF INTERNATIONAL BUSINESS COMPANIES",
    }]
    assert adv["addresses"][0]["type"] == "alternative"
    assert adv["addresses"][0]["country"]["code"] == "BZ"
    assert "Legal form: LIMITED" in adv["entityType"]["details"]


def test_inferred_jurisdiction_carries_a_note_quoting_the_filing() -> None:
    stmts = _statements(METASTAR)
    member = _by_type(stmts, "entity")[1]
    notes = [a for a in member.get("annotations") or [] if a["motivation"] == "commenting"]
    assert len(notes) == 1
    assert notes[0]["statementPointerTarget"] == "/recordDetails/jurisdiction"
    assert "INTERNATIONAL BUSINESS COMPANIES ACT 1990, BELIZE" in notes[0]["description"]
    assert "legal authority" in notes[0]["description"]


def test_relationship_is_senior_managing_official_with_an_entity_party() -> None:
    stmts = _statements(METASTAR)
    llp, *members = _by_type(stmts, "entity")
    rels = _by_type(stmts, "relationship")
    assert len(rels) == 2
    member_ids = {m["statementId"] for m in members}
    for rel in rels:
        rd = rel["recordDetails"]
        assert rd["subject"] == llp["statementId"]
        assert rd["interestedParty"] in member_ids
        (interest,) = rd["interests"]
        assert interest["type"] == "seniorManagingOfficial"
        assert interest["details"] == "LLP Designated Member, from 2009-06-08"
        assert interest["startDate"] == "2009-06-08"
        assert "beneficialOwnershipOrControl" not in interest


def test_one_corporate_member_of_two_llps_is_one_entity() -> None:
    """Keyed on the register's officer id: the Seychelles pair that sits on
    ten laundromat LLPs is one node with ten appointments, not ten nodes."""
    member = _corporate_member(
        "SHARED NOMINEE LTD", "096479", "SHARED1",
        legal_authority="INTERNATIONAL BUSINESS COMPANIES ACT",
        place_registered="SEYCHELLES",
    )
    a = _statements(_bundle([member], "OC371790", "BONDWEST LLP"))
    b = _statements(_bundle([member], "OC369315", "BENTCARD IMPORT LLP"))
    ent_a = next(e for e in _by_type(a, "entity") if e["recordDetails"]["name"] == "SHARED NOMINEE LTD")
    ent_b = next(e for e in _by_type(b, "entity") if e["recordDetails"]["name"] == "SHARED NOMINEE LTD")
    assert ent_a["statementId"] == ent_b["statementId"]
    # The appointment stays company-scoped.
    assert _by_type(a, "relationship")[0]["statementId"] != _by_type(b, "relationship")[0]["statementId"]
    # Leading zeros are part of the number; the place registered names the country.
    assert ent_a["recordDetails"]["identifiers"][0] == {
        "id": "096479", "scheme": "REG-SC", "schemeName": "SEYCHELLES",
    }


def test_place_registered_names_the_country_when_the_authority_does_not() -> None:
    member = _corporate_member(
        "DOMINICA MEMBER LTD", "16575", "DM1",
        legal_authority="INTERNATIONAL BUSINESS COMPANIES ACT",
        place_registered="COMMONWEALTH OF DOMINICA",
    )
    ent = _by_type(_statements(_bundle([member])), "entity")[1]["recordDetails"]
    assert ent["jurisdiction"]["code"] == "DM"
    assert ent["identifiers"][0]["scheme"] == "REG-DM"


def test_address_country_gives_the_jurisdiction_but_never_a_reg_country_scheme() -> None:
    member = _corporate_member(
        "ADDRESS ONLY LTD", "777", "ADDR1",
        legal_authority="BUSINESS CORPORATIONS ACT",
        place_registered="OFFICE OF THE REGISTRAR OF CORPORATIONS, MARSHALL",
    )
    member["address"]["country"] = "Marshall Islands"
    ent = _by_type(_statements(_bundle([member])), "entity")[1]
    rd = ent["recordDetails"]
    assert rd["jurisdiction"]["code"] == "MH"
    assert rd["identifiers"] == [{
        "id": "777", "scheme": "REG",
        "schemeName": "OFFICE OF THE REGISTRAR OF CORPORATIONS, MARSHALL",
    }]
    note = ent["annotations"][0]["description"]
    assert "service address" in note and "Marshall Islands" in note


def test_no_country_anywhere_leaves_the_jurisdiction_unset() -> None:
    member = _corporate_member(
        "NOWHERE LTD", "1", "NOWHERE1",
        legal_authority="COMPANIES LAW", place_registered="REGISTRAR",
    )
    member["address"] = {"address_line_1": "PO Box 1"}
    rd = _by_type(_statements(_bundle([member])), "entity")[1]["recordDetails"]
    assert "jurisdiction" not in rd
    assert rd["identifiers"][0]["scheme"] == "REG"


def test_uk_corporate_director_is_keyed_on_its_company_number() -> None:
    """A UK company as director gets GB-COH with the canonical number, keyed
    on it — the statementId the related-company pass gives the same company,
    and an identifier the FullCheck frontier can hop on (Phase 182)."""
    director = {
        "name": "UK CORPORATE DIRECTOR LIMITED",
        "officer_role": "corporate-director",
        "appointed_on": "2015-01-01",
        "address": {"address_line_1": "1 Street", "locality": "London", "country": "England"},
        "identification": {
            "identification_type": "uk-limited-company",
            "legal_authority": "COMPANIES ACT 2006",
            "legal_form": "LIMITED BY SHARES",
            "place_registered": "COMPANIES HOUSE",
            "registration_number": "2999029",
        },
        "links": {"officer": {"appointments": "/officers/UKDIR1/appointments"}},
    }
    stmts = _statements(_bundle([director], "01234567", "SUBJECT LIMITED"))
    assert _by_type(stmts, "person") == []
    ent = _by_type(stmts, "entity")[1]
    rd = ent["recordDetails"]
    assert rd["identifiers"] == [
        {"id": "02999029", "scheme": "GB-COH", "schemeName": "UK Companies House"}
    ]
    assert rd["jurisdiction"] == {"name": "United Kingdom", "code": "GB"}
    assert "annotations" not in ent  # the register said UK; nothing was inferred
    root = map_companies_house(_bundle([], "02999029", "UK CORPORATE DIRECTOR LIMITED"))
    assert ent["statementId"] == next(iter(root))["statementId"]


def test_natural_people_are_unchanged_beside_a_corporate_member() -> None:
    person = {
        "name": "SMITH, Jane",
        "officer_role": "llp-designated-member",
        "appointed_on": "2018-06-01",
        "date_of_birth": {"year": 1975, "month": 3},
        "links": {"officer": {"appointments": "/officers/abc123/appointments"}},
    }
    stmts = _statements(_bundle([person, _corporate_member("CORP LTD", "1", "CORP1")]))
    people = _by_type(stmts, "person")
    assert len(people) == 1
    assert people[0]["recordDetails"]["names"][0]["fullName"] == "SMITH, Jane"
    # Phase 193 grouping note is still on the person, never on the company.
    assert any(a["motivation"] == "identifying" for a in people[0]["annotations"])
    corp = next(e for e in _by_type(stmts, "entity") if e["recordDetails"]["name"] == "CORP LTD")
    assert all(a["motivation"] != "identifying" for a in corp.get("annotations") or [])


def test_resigned_corporate_member_is_skipped_like_a_resigned_person() -> None:
    member = _corporate_member("GONE LTD", "1", "GONE1")
    member["resigned_on"] = "2012-01-01"
    assert len(_by_type(_statements(_bundle([member])), "entity")) == 1


def test_metastar_bundle_passes_libcovebods() -> None:
    pytest.importorskip("libcovebods")
    from test_bods_libcovebods import validate_bods_statements

    result = validate_bods_statements(_statements(METASTAR))
    assert result["json_errors"] == []
    assert result["additional_errors"] == []


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("INTERNATIONAL BUSINESS COMPANIES ACT 1990, BELIZE", "BZ"),
        ("REG INTL BUS COMP BELIZE", "BZ"),
        ("REGISTRAR OF INTERNATIONAL BUSINESS COMPANIES", ""),
        ("COMMONWEALTH OF DOMINICA", "DM"),
        ("DOMINICAN REPUBLIC", "DO"),
        ("NEVIS", "KN"),
        ("SEYCHELLES INTERNATIONAL BUSINESS AUTHORITY", "SC"),
        ("OFFICE OF THE REGISTRAR OF CORPORATIONS, MARSHALL", ""),
        ("MARSHALL ISLANDS BUSINESS CORPORATIONS ACT", "MH"),
        ("BRITISH VIRGIN ISLANDS BUSINESS COMPANIES ACT 2004", "VG"),
        ("COMPANIES ACT, NEW SOUTH WALES", "AU"),
        ("NEW JERSEY BUSINESS CORPORATION ACT", "US"),
        ("PAPUA NEW GUINEA", "PG"),
        ("ENGLAND AND WALES", "GB"),
        ("BELIZE AND SEYCHELLES", ""),  # two countries: ambiguous, none
        ("", ""),
    ],
)
def test_country_in_text(text: str, code: str) -> None:
    assert _country_in_text(text) == code
