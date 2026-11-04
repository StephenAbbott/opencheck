"""FtM ``citizenship`` reaches BODS ``nationalities`` (the FtM citizenship ticket).

The two fixtures are real OpenSanctions records, read on 4 Nov 2026 and cut
to the properties that matter here: Arkadij Abramovič (Q4791679) has a
citizenship and no nationality; Alisher Usmanov (Q503147) has both, with the
Soviet Union (``suhh``) among his citizenships.
"""

from __future__ import annotations

import json
from pathlib import Path

from opencheck.bods import map_openaleph, map_opensanctions
from opencheck.bods.mappers.ftm import (
    _ftm_resolve_nationality,
    ftm_person_nationalities,
)
from opencheck.bods.statements import stale_statement_id
from opencheck.cross_check import _person_nationalities, corroborating_attributes

FIXTURES = Path(__file__).parent / "fixtures" / "opensanctions"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _person(statements) -> dict:
    people = [s for s in statements if s["recordType"] == "person"]
    assert len(people) == 1
    return people[0]


def test_citizenship_without_nationality_becomes_a_nationality():
    person = _person(map_opensanctions(_load("person_citizenship_only_Q4791679.json")))
    assert person["recordDetails"]["nationalities"] == [
        {"name": "Russian Federation", "code": "RU"}
    ]
    notes = [
        a
        for a in person.get("annotations", [])
        if a["statementPointerTarget"] == "/recordDetails/nationalities"
    ]
    assert len(notes) == 1
    assert notes[0]["motivation"] == "commenting"
    assert "Russian Federation (RU) is read from" in notes[0]["description"]
    assert "citizenship property" in notes[0]["description"]
    assert not stale_statement_id(person)


def test_both_lists_are_read_and_deduplicated_on_code():
    person = _person(
        map_opensanctions(_load("person_citizenship_and_nationality_Q503147.json"))
    )
    assert person["recordDetails"]["nationalities"] == [
        {"name": "Russian Federation", "code": "RU"},
        {"name": "Uzbekistan", "code": "UZ"},
        {"name": "USSR, Union of Soviet Socialist Republics"},
    ]
    (note,) = [
        a
        for a in person["annotations"]
        if a["statementPointerTarget"] == "/recordDetails/nationalities"
    ]
    # Only what nationality did not already say is attributed to citizenship.
    assert note["description"].startswith(
        "Uzbekistan (UZ), USSR, Union of Soviet Socialist Republics are read from"
    )
    assert "Russian Federation" not in note["description"]


def test_country_on_a_person_is_never_a_nationality():
    # Usmanov's FtM ``country`` includes Monaco; it is not a nationality.
    payload = _load("person_citizenship_and_nationality_Q503147.json")
    person = _person(map_opensanctions(payload))
    codes = {n.get("code") for n in person["recordDetails"]["nationalities"]}
    assert "MC" not in codes


def test_nationality_alone_carries_no_annotation():
    payload = _load("person_citizenship_only_Q4791679.json")
    payload["properties"] = {
        **payload["properties"],
        "nationality": ["ru"],
        "citizenship": ["RU", "ru"],
    }
    person = _person(map_opensanctions(payload))
    assert person["recordDetails"]["nationalities"] == [
        {"name": "Russian Federation", "code": "RU"}
    ]
    assert not any(
        a["statementPointerTarget"] == "/recordDetails/nationalities"
        for a in person.get("annotations", [])
    )


def test_openaleph_reads_the_same_two_properties():
    payload = _load("person_citizenship_only_Q4791679.json")
    person = _person(map_openaleph(payload))
    assert person["recordDetails"]["nationalities"] == [
        {"name": "Russian Federation", "code": "RU"}
    ]


def test_former_states_and_unknown_values_are_name_only():
    assert _ftm_resolve_nationality("suhh") == {
        "name": "USSR, Union of Soviet Socialist Republics"
    }
    assert _ftm_resolve_nationality("xk") == {"name": "Kosovo"}
    assert _ftm_resolve_nationality("Narnia") == {"name": "Narnia"}
    assert _ftm_resolve_nationality("gb") == {"name": "United Kingdom", "code": "GB"}


def test_name_only_entries_dedupe_on_name():
    out, from_cit = ftm_person_nationalities(
        {"nationality": ["suhh"], "citizenship": ["SUHH", "su"]}
    )
    assert out == [{"name": "USSR, Union of Soviet Socialist Republics"}]
    assert from_cit == []


def test_a_citizenship_only_screening_target_now_corroborates_on_nationality():
    """The cross-check reads codes on our side and nationality / citizenship /
    country on the hit's. A FtM person whose only country was citizenship had
    no code to compare before; now it has one."""
    person = _person(map_opensanctions(_load("person_citizenship_only_Q4791679.json")))
    target = {"nationalities": _person_nationalities(person["recordDetails"])}
    assert target["nationalities"] == ("RU",)
    hit = {"properties": {"citizenship": ["ru"]}}
    assert corroborating_attributes(target, hit) == ("nationality",)
    # A name-only entry (the Soviet Union) is never compared.
    usmanov = _person(
        map_opensanctions(_load("person_citizenship_and_nationality_Q503147.json"))
    )
    assert _person_nationalities(usmanov["recordDetails"]) == ("RU", "UZ")
