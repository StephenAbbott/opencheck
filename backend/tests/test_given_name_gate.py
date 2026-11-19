"""The given-name gate on related-party person matches (the Bulgarian PEP ticket).

Found on 19 Nov 2026 on АКВАХИМ (`BG-EIK` 200984964): the director
"Елица Петрова Рускова" raised RELATED_PEP (medium) on OpenSanctions'
"НЕЛИ РУСКОВА ПЕТРОВА" — 0.905 on a token-sort score, because both names hold
Петрова and Рускова. In Bulgarian order (given, patronymic, family) these are
two people: Елица, daughter of Петър, and Нели, daughter of Руско. A Bulgarian
patronymic is shaped like a surname (-ова), so the patronymic gate cannot see
it, and Bulgarian register persons carry no birth year.

The gate is order-free: two names conflict when each holds a name token with
no near partner in the other. A subset is never a conflict, and spellings of
one name (Mohammed / Muhammad, Serhii / Sergei) are not two names.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck import names
from opencheck.cross_check import (
    _given_names_compatible,
    _signal_from_ep,
    _signals_from_os,
)
from opencheck.sources import SearchKind, SourceHit

OURS = "Елица Петрова Рускова"
THEIRS = "НЕЛИ РУСКОВА ПЕТРОВА"


# ── names.person_names_conflict ───────────────────────────────────────


def test_the_akvahim_pair_conflicts() -> None:
    assert names.name_similarity(OURS, THEIRS) >= names.NAME_MATCH_THRESHOLD
    assert names.person_names_conflict(OURS, [THEIRS])


@pytest.mark.parametrize(
    ("ours", "theirs"),
    [
        # Order alone is not a conflict.
        (OURS, "Рускова Елица Петрова"),
        (OURS, "Elitsa Petrova Ruskova"),
        ("FIORE, Norman Benito", "NORMAN BENITO FIORE"),
        # A subset is not a conflict: a middle name or patronymic left out.
        ("Елица Рускова", OURS),
        ("JONES, Christopher Paul", "CHRISTOPHER JONES"),
        ("Michael R. Gordon", "Michael Gordon"),
        # Spellings of one given name.
        ("Mohammed Ali Hassan", "Muhammad Ali Hassan"),
        ("Alex Ivanov Petrov", "Alexander Ivanov Petrov"),
        # The patronymic gate's agreeing pairs must stay matches here too.
        ("ШЕВЧЕНКО ОЛЕКСАНДР ВОЛОДИМИРОВИЧ", "Shevchenko Aleksandr Vladimirovich"),
        ("КОВАЛЕНКО СЕРГІЙ ІГОРОВИЧ", "Sergei Igorevich Kovalenko"),
        ("ПЕТРЕНКО ОЛЕКСІЙ ЮРІЙОВИЧ", "Petrenko Aleksei Yurevich"),
        ("ПЕТРЕНКО ОЛЕКСІЙ МИКОЛАЙОВИЧ", "Petrenko Aleksei Nikolaevich"),
        ("ТКАЧ КИРИЛО ДАНИЛОВИЧ", "Kirill Daniilovich Tkach"),
        ("БОНДАР ЄВГЕН ЄВГЕНОВИЧ", "Evgenii Evgenevich Bondar"),
        ("БОНДАР ГРИГОРІЙ ГРИГОРОВИЧ", "Grigorii Grigorevich Bondar"),
        ("БОНДАР ОЛЕНА ВАСИЛІВНА", "Elena Vasilevna Bondar"),
        # A single-token name cannot conflict.
        (OURS, "Рускова"),
    ],
)
def test_no_conflict(ours: str, theirs: str) -> None:
    assert not names.person_names_conflict(ours, [theirs])


@pytest.mark.parametrize(
    ("ours", "theirs"),
    [
        ("Robert Frederick Smith", "Robert Frederick White"),
        ("Иван Петров Георгиев", "Димитър Петров Георгиев"),
    ],
)
def test_conflict(ours: str, theirs: str) -> None:
    assert names.person_names_conflict(ours, [theirs])


def test_one_record_name_that_agrees_keeps_the_match() -> None:
    assert not names.person_names_conflict(OURS, [THEIRS, "Elitsa Petrova Ruskova"])


# ── wired into the screens ────────────────────────────────────────────


def _target(name: str = OURS, birth_year: int | None = None) -> dict[str, Any]:
    return {
        "kind": "person",
        "statement_id": "opencheck-person",
        "name": name,
        "birth_year": birth_year,
        "nationalities": ("BG",),
        "former": False,
    }


def _hit(
    name: str, source_id: str = "opensanctions", **props: Any
) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id="Q1",
        kind=SearchKind.PERSON,
        name=name,
        summary="",
        identifiers={},
        raw={
            "id": "Q1",
            "caption": name,
            "properties": {"topics": ["role.pep"], "country": ["bg"], **props},
        },
        is_stub=False,
    )


def test_the_akvahim_pep_match_raises_no_signal() -> None:
    hit = _hit(THEIRS)
    assert not _given_names_compatible(_target(), hit)
    assert _signals_from_os(hit, _target(), min_score=0.88) == []


def test_the_same_person_still_raises_the_signal() -> None:
    hit = _hit("Elitsa Petrova Ruskova")
    assert [s.code for s in _signals_from_os(hit, _target(), min_score=0.88)] == [
        "RELATED_PEP"
    ]


def test_everypolitician_is_gated_the_same_way() -> None:
    assert _signal_from_ep(_hit(THEIRS, "everypolitician"), _target(), min_score=0.88) is None


def test_an_agreeing_birth_year_keeps_the_match() -> None:
    hit = _hit(THEIRS, birthDate=["1970-05-01"])
    assert _given_names_compatible(_target(birth_year=1970), hit)
    assert not _given_names_compatible(_target(birth_year=1950), hit)


def test_entities_are_never_given_name_gated() -> None:
    target = {**_target("Petrova Ruskova Holding"), "kind": "entity"}
    assert _given_names_compatible(target, _hit("Ruskova Petrova Trading"))
