"""Tests for the related-party name cross-check.

Covers the deterministic helpers (target extraction, name scoring,
DOB compatibility, dedupe) directly, plus an integration test that
exercises ``assess_cross_source_names`` against mocked OpenSanctions
and EveryPolitician adapters.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck import cross_check
from opencheck.config import get_settings
from opencheck.cross_check import (
    RELATED_PEP,
    RELATED_SANCTIONED,
    _birth_year_compatible,
    _collect_targets,
    _name_score,
    _normalise,
    assess_cross_source_names,
)
from opencheck.sources import REGISTRY, SearchKind, SourceHit


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------


def test_normalise_strips_diacritics_and_punctuation() -> None:
    assert _normalise("Władysław Sikorski") == "wladyslaw sikorski"
    assert _normalise("María-José d'Almeida") == "maria jose d almeida"
    assert _normalise("  Vladimir   Putin!  ") == "vladimir putin"


def test_name_score_exact_after_normalisation_is_one() -> None:
    assert _name_score("Vladimir Putin", "VLADIMIR PUTIN") == 1.0
    assert _name_score("María-José", "Maria Jose") == 1.0


def test_name_score_returns_high_for_minor_differences() -> None:
    # Single-character typos still cross the 0.88 threshold the
    # cross-check uses.
    assert _name_score("Vladimir Putin", "Vladmir Putin") > 0.88


def test_name_score_is_low_for_unrelated_names() -> None:
    assert _name_score("Vladimir Putin", "John Smith") < 0.5


# ---------------------------------------------------------------------
# Target extraction
# ---------------------------------------------------------------------


def _person(sid: str, full_name: str | None = None, *, birth: str | None = None,
            person_type: str = "knownPerson") -> dict[str, Any]:
    rd: dict[str, Any] = {"personType": person_type}
    if full_name:
        rd["names"] = [{"type": "individual", "fullName": full_name}]
    if birth:
        rd["birthDate"] = birth
    return {"statementId": sid, "recordType": "person", "recordDetails": rd}


def _entity(sid: str, name: str | None = None, *, entity_type: str = "registeredEntity") -> dict[str, Any]:
    rd: dict[str, Any] = {"entityType": {"type": entity_type}}
    if name:
        rd["name"] = name
    return {"statementId": sid, "recordType": "entity", "recordDetails": rd}


def test_collect_targets_extracts_known_persons_and_entities() -> None:
    bods = [
        _person("p1", "Vladimir Putin", birth="1952-10-07"),
        _entity("e1", "Acme Holdings"),
    ]
    targets = _collect_targets(bods)
    assert {(t["kind"], t["name"]) for t in targets} == {
        ("person", "Vladimir Putin"),
        ("entity", "Acme Holdings"),
    }
    p = next(t for t in targets if t["kind"] == "person")
    assert p["birth_year"] == 1952


def test_collect_targets_skips_unknown_persons_and_anonymous_entities() -> None:
    bods = [
        _person("p1", person_type="unknownPerson"),
        _entity("e1", "Anon Co", entity_type="anonymousEntity"),
        _person("p2", "Real Person"),
    ]
    targets = _collect_targets(bods)
    assert [t["statement_id"] for t in targets] == ["p2"]


def test_collect_targets_skips_records_without_a_name() -> None:
    bods = [
        _person("p1", None),
        _entity("e1", None),
    ]
    assert _collect_targets(bods) == []


# ---------------------------------------------------------------------
# DOB compatibility
# ---------------------------------------------------------------------


def _hit_with_birth(years: list[str]) -> SourceHit:
    return SourceHit(
        source_id="opensanctions",
        hit_id="X",
        kind=SearchKind.PERSON,
        name="X",
        summary="",
        identifiers={},
        raw={"properties": {"birthDate": years}},
        is_stub=False,
    )


def test_birth_year_compatible_passes_when_target_has_no_year() -> None:
    assert _birth_year_compatible(None, _hit_with_birth(["1980"])) is True


def test_birth_year_compatible_passes_when_hit_has_no_year() -> None:
    hit = SourceHit(
        source_id="opensanctions",
        hit_id="X",
        kind=SearchKind.PERSON,
        name="X",
        summary="",
        identifiers={},
        raw={"properties": {}},
        is_stub=False,
    )
    assert _birth_year_compatible(1952, hit) is True


def test_birth_year_compatible_within_one_year() -> None:
    assert _birth_year_compatible(1952, _hit_with_birth(["1952-10-07"])) is True
    assert _birth_year_compatible(1952, _hit_with_birth(["1953"])) is True
    assert _birth_year_compatible(1952, _hit_with_birth(["1990"])) is False


# ---------------------------------------------------------------------
# Integration — assess_cross_source_names
# ---------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _live_with_key(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _StubAdapter:
    """Tiny stand-in for an adapter — returns a canned list of hits
    regardless of query, used to inject results into the cross-check."""

    def __init__(self, hits: list[SourceHit]) -> None:
        self._hits = hits

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return list(self._hits)


def _stub(monkeypatch, source_id: str, hits: list[SourceHit]) -> None:
    monkeypatch.setitem(REGISTRY, source_id, _StubAdapter(hits))


async def test_no_op_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    bundle = [_person("p1", "Vladimir Putin")]
    assert await assess_cross_source_names(bundle) == []


async def test_no_op_without_opensanctions_key(monkeypatch) -> None:
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    get_settings.cache_clear()
    bundle = [_person("p1", "Vladimir Putin")]
    assert await assess_cross_source_names(bundle) == []


async def test_emits_related_sanctioned_for_matching_person(monkeypatch) -> None:
    """OS returns a sanctioned record matching the related person's name."""
    sanctioned_hit = SourceHit(
        source_id="opensanctions",
        hit_id="NK-bp",
        kind=SearchKind.PERSON,
        name="Vladimir Putin",
        summary="",
        identifiers={"opensanctions_id": "NK-bp"},
        raw={
            "id": "NK-bp",
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Putin"],
                "topics": ["sanction"],
                "birthDate": ["1952-10-07"],
            },
            "topics": ["sanction"],
        },
        is_stub=False,
    )
    _stub(monkeypatch, "opensanctions", [sanctioned_hit])
    _stub(monkeypatch, "everypolitician", [])

    bundle = [_person("p1", "Vladimir Putin", birth="1952-10-07")]
    signals = await assess_cross_source_names(bundle)
    assert [s.code for s in signals] == [RELATED_SANCTIONED]
    s = signals[0]
    assert s.confidence == "high"
    assert s.evidence["subject_statement_id"] == "p1"
    assert s.evidence["matched_name"] == "Vladimir Putin"
    assert s.source_id == "opensanctions"
    assert s.hit_id == "NK-bp"


async def test_emits_related_pep_for_pep_topic_or_everypolitician(
    monkeypatch,
) -> None:
    """OS returns ``role.pep`` topic; EP returns the same PEP — dedupe
    keeps the higher-confidence signal."""
    os_hit = SourceHit(
        source_id="opensanctions",
        hit_id="OS-1",
        kind=SearchKind.PERSON,
        name="Vladimir Putin",
        summary="",
        identifiers={},
        raw={"properties": {"topics": ["role.pep"]}, "topics": ["role.pep"]},
        is_stub=False,
    )
    ep_hit = SourceHit(
        source_id="everypolitician",
        hit_id="Q7747-pep",
        kind=SearchKind.PERSON,
        name="Vladimir Putin",
        summary="",
        identifiers={},
        raw={"properties": {}},
        is_stub=False,
    )
    _stub(monkeypatch, "opensanctions", [os_hit])
    _stub(monkeypatch, "everypolitician", [ep_hit])

    bundle = [_person("p1", "Vladimir Putin")]
    signals = await assess_cross_source_names(bundle)
    codes_by_source = {(s.code, s.source_id) for s in signals}
    assert (RELATED_PEP, "opensanctions") in codes_by_source
    assert (RELATED_PEP, "everypolitician") in codes_by_source


async def test_no_match_below_threshold(monkeypatch) -> None:
    """Names that don't match at >=0.88 don't produce signals."""
    hit = SourceHit(
        source_id="opensanctions",
        hit_id="OS-noise",
        kind=SearchKind.PERSON,
        name="Volodymyr Zelenskyy",
        summary="",
        identifiers={},
        raw={"topics": ["sanction"]},
        is_stub=False,
    )
    _stub(monkeypatch, "opensanctions", [hit])
    _stub(monkeypatch, "everypolitician", [])
    bundle = [_person("p1", "Vladimir Putin")]
    assert await assess_cross_source_names(bundle) == []


async def test_birth_year_filter_drops_wrong_dob(monkeypatch) -> None:
    """Same name + wildly different birth year → not a match."""
    hit = SourceHit(
        source_id="opensanctions",
        hit_id="OS-other-putin",
        kind=SearchKind.PERSON,
        name="Vladimir Putin",
        summary="",
        identifiers={},
        raw={
            "topics": ["sanction"],
            "properties": {"birthDate": ["1990-01-01"]},
        },
        is_stub=False,
    )
    _stub(monkeypatch, "opensanctions", [hit])
    _stub(monkeypatch, "everypolitician", [])
    bundle = [_person("p1", "Vladimir Putin", birth="1952-10-07")]
    assert await assess_cross_source_names(bundle) == []


async def test_entity_targets_only_emit_sanctioned_signal(monkeypatch) -> None:
    """Entities can be SANCTIONED but never PEP."""
    pep_hit = SourceHit(
        source_id="opensanctions",
        hit_id="OS-pep",
        kind=SearchKind.ENTITY,
        name="Acme Holdings",
        summary="",
        identifiers={},
        raw={"topics": ["role.pep"]},
        is_stub=False,
    )
    sanction_hit = SourceHit(
        source_id="opensanctions",
        hit_id="OS-sanction",
        kind=SearchKind.ENTITY,
        name="Acme Holdings",
        summary="",
        identifiers={},
        raw={"topics": ["sanction"]},
        is_stub=False,
    )
    # OS returns both for the same entity name; only the sanctioned
    # record should emit a signal.
    _stub(monkeypatch, "opensanctions", [pep_hit, sanction_hit])

    bundle = [_entity("e1", "Acme Holdings")]
    signals = await assess_cross_source_names(bundle)
    assert [s.code for s in signals] == [RELATED_SANCTIONED]


async def test_max_targets_caps_request_volume(monkeypatch) -> None:
    """``max_targets`` bounds how many statements get cross-checked."""
    calls: list[str] = []

    class CountingAdapter:
        async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
            calls.append(query)
            return []

    monkeypatch.setitem(REGISTRY, "opensanctions", CountingAdapter())
    monkeypatch.setitem(REGISTRY, "everypolitician", CountingAdapter())

    bundle = [_person(f"p{i}", f"Person {i}") for i in range(10)]
    await assess_cross_source_names(bundle, max_targets=3)
    # 3 targets × 2 adapters (OS + EP) = 6 calls.
    assert len(calls) == 6


async def test_dedupe_collapses_duplicate_signals(monkeypatch) -> None:
    """Two probes hitting the same upstream record id should collapse
    to one signal."""

    # Stub both OS and EP to return the same hit — dedupe should
    # collapse them, since they share (code, source_id, hit_id, subject).
    same_hit = SourceHit(
        source_id="opensanctions",
        hit_id="OS-same",
        kind=SearchKind.PERSON,
        name="Vladimir Putin",
        summary="",
        identifiers={},
        raw={"topics": ["role.pep"]},
        is_stub=False,
    )
    _stub(monkeypatch, "opensanctions", [same_hit, same_hit])
    _stub(monkeypatch, "everypolitician", [])

    bundle = [_person("p1", "Vladimir Putin")]
    signals = await assess_cross_source_names(bundle)
    assert len(signals) == 1
