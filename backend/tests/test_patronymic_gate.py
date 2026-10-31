"""The patronymic gate on related-party person matches (the ЄДР name-gate ticket).

The ЄДР publishes no birth date, so its 674,413 beneficial owners and its
founders reach the related-party screens with a name and nothing else. A
Ukrainian name carries the father's given name, and two people who share a
surname and a given name but not a father are different people.

Stephen's decision (31 Oct 2026): reject only a *clear* conflict. OpenSanctions
often spells a Ukrainian person's patronymic in its Russian form —
"Vladimirovich" for ВОЛОДИМИРОВИЧ, 0.59 similar — so a rule that demanded the
strings agree would drop true sanctions matches. These tests pin both halves.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck import names
from opencheck.cross_check import (
    _patronymic_compatible,
    _signal_from_ep,
    _signals_from_os,
)
from opencheck.sources import SearchKind, SourceHit

UA = "ШЕВЧЕНКО ОЛЕКСАНДР ВОЛОДИМИРОВИЧ"


# ── names.patronymics_conflict ─────────────────────────────────────────


@pytest.mark.parametrize(
    ("ours", "theirs"),
    [
        # Ukrainian and Russian spellings of one father.
        (UA, "Shevchenko Aleksandr Vladimirovich"),
        (UA, "Oleksandr Volodymyrovych Shevchenko"),
        (UA, "Шевченко Александр Владимирович"),
        ("КОВАЛЕНКО СЕРГІЙ ІГОРОВИЧ", "Sergei Igorevich Kovalenko"),
        ("ПЕТРЕНКО ОЛЕКСІЙ ЮРІЙОВИЧ", "Petrenko Aleksei Yurevich"),
        ("ПЕТРЕНКО ОЛЕКСІЙ МИКОЛАЙОВИЧ", "Petrenko Aleksei Nikolaevich"),
        ("ТКАЧ ІГОР СЕРГІЙОВИЧ", "Igor Sergeevich Tkach"),
        ("ТКАЧ КИРИЛО ДАНИЛОВИЧ", "Kirill Daniilovich Tkach"),
        ("БОНДАР ЄВГЕН ЄВГЕНОВИЧ", "Evgenii Evgenevich Bondar"),
        ("БОНДАР ГРИГОРІЙ ГРИГОРОВИЧ", "Grigorii Grigorevich Bondar"),
        # Women's forms, both spellings.
        ("БОНДАР ОЛЕНА ВАСИЛІВНА", "Elena Vasilevna Bondar"),
        ("ШЕВЧЕНКО ОЛЕНА ВОЛОДИМИРІВНА", "Elena Vladimirovna Shevchenko"),
    ],
)
def test_one_father_spelt_two_ways_is_no_conflict(ours: str, theirs: str) -> None:
    assert not names.patronymics_conflict(ours, [theirs])


@pytest.mark.parametrize(
    ("ours", "theirs"),
    [
        (UA, "Oleksandr Ivanovych Shevchenko"),
        (UA, "Шевченко Александр Иванович"),
        ("КОВАЛЕНКО СЕРГІЙ ГРИГОРОВИЧ", "Sergei Igorevich Kovalenko"),
        # Oleksandr and Oleksii are two names, not two spellings of one.
        ("ПЕТРЕНКО ОЛЕКСІЙ ОЛЕКСАНДРОВИЧ", "Petrenko Aleksei Alekseevich"),
        ("БОНДАР ОЛЕНА ВАСИЛІВНА", "Elena Petrovna Bondar"),
    ],
)
def test_two_fathers_conflict(ours: str, theirs: str) -> None:
    assert names.patronymics_conflict(ours, [theirs])


def test_a_record_with_no_patronymic_refutes_nothing() -> None:
    assert not names.patronymics_conflict(UA, ["Oleksandr Shevchenko"])
    assert not names.patronymics_conflict("Oleksandr Shevchenko", [UA])


def test_any_agreeing_form_on_the_record_is_enough() -> None:
    assert not names.patronymics_conflict(
        UA, ["Oleksandr Ivanovych Shevchenko", "Aleksandr Vladimirovich Shevchenko"]
    )


def test_the_record_s_father_name_field_counts() -> None:
    # FtM ``fatherName`` holds either the patronymic or the father's name.
    assert not names.patronymics_conflict(UA, ["Oleksandr Shevchenko"], ["Volodymyr"])
    assert not names.patronymics_conflict(UA, ["Oleksandr Shevchenko"], ["Vladimirovich"])
    assert names.patronymics_conflict(UA, ["Oleksandr Shevchenko"], ["Ivanovych"])


def test_a_patronymic_shaped_surname_is_never_a_conflict_by_itself() -> None:
    # "Abramovich" reads as a patronymic; it is on both sides of a match.
    assert not names.patronymics_conflict(
        "АБРАМОВИЧ РОМАН АРКАДІЙОВИЧ", ["Roman Arkadyevich Abramovich"]
    )
    assert not names.patronymics_conflict(
        "Roman Abramovich", ["Roman Arkadyevich Abramovich"]
    )


# ── the person token rule (ICIJ, Phase 282) folds the same spellings ──


def test_person_token_agreement_folds_a_russian_spelt_patronymic() -> None:
    assert names.person_token_agreement(UA, "Shevchenko Aleksandr Vladimirovich")
    assert not names.person_token_agreement(UA, "Shevchenko Aleksandr Ivanovich")


# ── wired into the OpenSanctions and EveryPolitician paths ────────────


def _target(name: str = UA) -> dict[str, Any]:
    return {
        "kind": "person",
        "statement_id": "opencheck-person",
        "name": name,
        "birth_year": None,  # the ЄДР publishes none
        "nationalities": ("UA",),
        "former": False,
    }


def _hit(name: str, source_id: str = "opensanctions", **props: Any) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id="Q1",
        kind=SearchKind.PERSON,
        name=name,
        summary="",
        identifiers={},
        raw={"id": "Q1", "caption": name, "properties": {"topics": ["sanction"], **props}},
        is_stub=False,
    )


def test_a_sanctioned_namesake_with_another_father_raises_no_signal() -> None:
    hit = _hit("Oleksandr Ivanovych Shevchenko")
    assert not _patronymic_compatible(_target(), hit)
    assert _signals_from_os(hit, _target(), min_score=0.88) == []


def test_the_same_person_in_russian_spelling_still_raises_the_signal() -> None:
    hit = _hit("Aleksandr Vladimirovich Shevchenko")
    signals = _signals_from_os(hit, _target(), min_score=0.88)
    assert [s.code for s in signals] == ["RELATED_SANCTIONED"]
    # Still a name match only: agreeing patronymics are part of the name.
    assert signals[0].summary.startswith("Possible name match only")


def test_an_alias_naming_another_father_is_read() -> None:
    hit = _hit("Oleksandr Shevchenko", alias=["Шевченко Олександр Іванович"])
    assert _signals_from_os(hit, _target(), min_score=0.88) == []


def test_an_alias_naming_the_same_father_keeps_the_match() -> None:
    hit = _hit(
        "Oleksandr Ivanovych Shevchenko",
        alias=["Oleksandr Volodymyrovych Shevchenko"],
    )
    assert _patronymic_compatible(_target(), hit)


def test_everypolitician_is_gated_the_same_way() -> None:
    hit = _hit("Oleksandr Ivanovych Shevchenko", source_id="everypolitician")
    assert _signal_from_ep(hit, _target(), min_score=0.88) is None
    hit = _hit("Aleksandr Vladimirovich Shevchenko", source_id="everypolitician")
    assert _signal_from_ep(hit, _target(), min_score=0.88) is not None


def test_entities_are_never_patronymic_gated() -> None:
    target = {**_target("ТОВ ПЕТРОВИЧ"), "kind": "entity"}
    assert _patronymic_compatible(target, _hit("Ivanovich LLC"))
