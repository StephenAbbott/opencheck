"""``/person-check`` scores a hit against every name its record files.

The person-check caption ticket (20 Nov 2026): the company lookup of АКВАХИМ
(``BG-EIK 200984964``) raised ``RELATED_PEP`` for Борислав Любенов Великов,
while ``/person-check`` on the same name raised nothing — OpenSanctions'
record is captioned "Borislav Velikov" (0.78 against the full name) and files
the full Cyrillic name as an alias. The records below are in OpenSanctions'
shape, with the names the ticket quotes.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.routers.person_check import _score_hit
from opencheck.sources import SearchKind, SourceHit

VELIKOV = {
    "id": "Q9177631",
    "schema": "Person",
    "caption": "Borislav Velikov",
    "properties": {
        "name": ["Borislav Velikov"],
        "alias": ["БОРИСЛАВ ЛЮБЕНОВ ВЕЛИКОВ", "BORISLAV LIUBENOV VELIKOV"],
        "birthDate": ["1946"],
        "nationality": ["bg"],
        "topics": ["role.pep"],
    },
}

#: The PR #657 case: two family tokens in swapped places, two given names.
PETROVA = {
    "id": "bg-pep-petrova",
    "schema": "Person",
    "caption": "Neli Petrova",
    "properties": {
        "name": ["Neli Petrova"],
        "alias": ["НЕЛИ РУСКОВА ПЕТРОВА"],
        "topics": ["role.pep"],
    },
}


def _hit(source_id: str, record: dict, *, hit_id: str | None = None) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id or record["id"],
        kind=SearchKind.PERSON,
        name=record.get("caption") or record.get("label") or "",
        summary="",
        identifiers={},
        raw=record,
        is_stub=False,
    )


@pytest.mark.parametrize("source_id", ["opensanctions", "everypolitician"])
@pytest.mark.parametrize(
    "query", ["Борислав Любенов Великов", "Borislav Lyubenov Velikov"]
)
def test_a_full_name_matches_the_alias_the_record_files(source_id: str, query: str) -> None:
    match = _score_hit(_hit(source_id, VELIKOV), query, None)
    assert match.strong
    assert match.name_score >= 0.88
    assert match.matched_name == "БОРИСЛАВ ЛЮБЕНОВ ВЕЛИКОВ"
    # The caption is still what the hit is called.
    assert match.hit.name == "Borislav Velikov"


def test_a_caption_that_scores_best_is_reported_as_before() -> None:
    match = _score_hit(_hit("opensanctions", VELIKOV), "Borislav Velikov", None)
    assert match.strong and match.name_score == 1.0
    assert match.matched_name == "Borislav Velikov"


def test_a_different_given_name_is_still_refused() -> None:
    """Scoring aliases lifts Рускова to 0.905 against НЕЛИ РУСКОВА ПЕТРОВА;
    the PR #657 given-name refusal keeps her off the record."""
    match = _score_hit(_hit("opensanctions", PETROVA), "Елица Петрова Рускова", None)
    assert match.name_score >= 0.88
    assert not match.strong


def test_a_birth_year_conflict_is_still_refused() -> None:
    match = _score_hit(_hit("opensanctions", VELIKOV), "Борислав Любенов Великов", 1980)
    assert not match.birth_year_compatible
    assert not match.strong


def test_a_patronymic_conflict_is_still_refused() -> None:
    record = {
        "id": "ua-pep",
        "caption": "Oleksandr Kovalenko",
        "properties": {
            "name": ["Oleksandr Kovalenko"],
            "alias": ["Олександр Павлович Коваленко"],
            "topics": ["role.pep"],
        },
    }
    match = _score_hit(_hit("opensanctions", record), "Олександр Петрович Коваленко", None)
    assert match.name_score >= 0.88
    assert not match.strong


def test_weak_alias_is_not_scored() -> None:
    record = {
        "id": "x",
        "caption": "Borislav Velikov",
        "properties": {"weakAlias": ["Борислав Любенов Великов"], "topics": ["role.pep"]},
    }
    match = _score_hit(_hit("opensanctions", record), "Борислав Любенов Великов", None)
    assert match.matched_name != "Борислав Любенов Великов"


def test_a_wikidata_hit_is_scored_against_the_name_the_search_matched() -> None:
    item = {
        "id": "Q9177631",
        "label": "Borislav Velikov",
        "match": {"type": "alias", "language": "bg", "text": "Борислав Любенов Великов"},
    }
    match = _score_hit(_hit("wikidata", item), "Борислав Любенов Великов", None)
    assert match.strong
    assert match.matched_name == "Борислав Любенов Великов"


def test_the_endpoint_raises_the_pep_signal_and_says_which_name_matched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_adapters(q, kind):
        return {
            "opensanctions": [_hit("opensanctions", VELIKOV)],
            "everypolitician": [_hit("everypolitician", PETROVA)],
        }, {}

    monkeypatch.setattr("opencheck.routers.person_check._run_adapters", fake_run_adapters)
    body = TestClient(app).get(
        "/person-check", params={"name": "Борислав Любенов Великов"}
    ).json()
    codes = [(s["code"], s["hit_id"]) for s in body["risk_signals"]]
    assert ("PEP", "Q9177631") in codes
    sig = next(s for s in body["risk_signals"] if s["hit_id"] == "Q9177631")
    match = sig["evidence"]["match"]
    assert match["matched_name"] == "БОРИСЛАВ ЛЮБЕНОВ ВЕЛИКОВ"
    # A name match is never an identity: no birth year was given, so this
    # stays a possible name match.
    assert sig["summary"].startswith("Possible name match only")


def test_rouskova_gets_no_signal_from_the_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_run_adapters(q, kind):
        return {"opensanctions": [_hit("opensanctions", PETROVA)]}, {}

    monkeypatch.setattr("opencheck.routers.person_check._run_adapters", fake_run_adapters)
    body = TestClient(app).get(
        "/person-check", params={"name": "Елица Петрова Рускова"}
    ).json()
    assert body["risk_signals"] == []
