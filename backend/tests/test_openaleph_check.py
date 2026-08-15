"""Tests for openaleph_check.py — related-party screening via percolation.

Covers target→text construction, the two-call split (persons broad /
entities topic-scoped), surface-form attribution back to statement ids,
the shared cross_check gates (similarity, single-token person guard,
birth-year), the topic → RELATED_* ladder, the informational screening
out-collector, degradation records (issue #50 contract: an empty result
with a failed screen is never a clean screen), and the pipeline wiring.
"""

from __future__ import annotations

import pytest

from opencheck.config import get_settings
from opencheck.openaleph_check import (
    _WATCHLIST_TOPICS,
    CHECK_NAME,
    assess_openaleph_names,
)
from opencheck.risk import DEGRADED_NOT_CONFIGURED, DEGRADED_UPSTREAM_ERROR, DegradedSource
from opencheck.sources import REGISTRY


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENALEPH_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Fixtures — a minimal BODS bundle and percolation result items
# ---------------------------------------------------------------------------


def _person_stmt(sid: str, name: str, birth: str | None = None) -> dict:
    rd: dict = {"names": [{"type": "individual", "fullName": name}]}
    if birth:
        rd["birthDate"] = birth
    return {"statementId": sid, "recordType": "person", "recordDetails": rd}


def _entity_stmt(sid: str, name: str) -> dict:
    return {
        "statementId": sid,
        "recordType": "entity",
        "recordDetails": {"name": name},
    }


_BUNDLE = [
    _entity_stmt("stmt-subject", "Acme Holdings PLC"),
    _person_stmt("stmt-sechin", "Igor Sechin", "1960-09-07"),
    _person_stmt("stmt-clean", "Jane Ordinary"),
    _entity_stmt("stmt-vehicle", "Offshore Vehicle Ltd"),
]


def _percolate_item(
    name: str,
    *,
    surface_form: str,
    topics: list[str] | None = None,
    schema: str = "Person",
    entity_id: str | None = None,
    collection: str = "Test Watchlist",
    birth_date: str | None = None,
    match: list[str] | None = None,
) -> dict:
    props: dict = {"name": [name]}
    if topics:
        props["topics"] = topics
    if birth_date:
        props["birthDate"] = [birth_date]
    return {
        "id": entity_id or f"oa-{name.lower().replace(' ', '-')}",
        "schema": schema,
        "caption": name,
        "properties": props,
        "collection": {"id": 1, "foreign_id": "test", "label": collection},
        "links": {"ui": f"https://search.openaleph.org/entities/{entity_id or 'x'}"},
        "percolator_match": match or ["name"],
        "surface_forms": [surface_form],
        "score": 1.5,
    }


class _FakePercolate:
    """Stands in for OpenAlephAdapter.percolate_text on the registry adapter."""

    def __init__(self, person_results, entity_results):
        self.person_results = person_results
        self.entity_results = entity_results
        self.calls: list[dict] = []

    async def __call__(self, text, *, schema=None, topics=(), limit=25):
        self.calls.append(
            {"text": text, "schema": schema, "topics": topics, "limit": limit}
        )
        if schema == "Person":
            return self.person_results
        return self.entity_results


@pytest.fixture()
def fake_percolate(monkeypatch):
    def _install(person_results, entity_results):
        fake = _FakePercolate(person_results, entity_results)
        monkeypatch.setattr(REGISTRY["openaleph"], "percolate_text", fake)
        return fake

    return _install


# ---------------------------------------------------------------------------
# Call construction
# ---------------------------------------------------------------------------


async def test_two_calls_persons_broad_entities_topic_scoped(fake_percolate) -> None:
    fake = fake_percolate([], [])
    await assess_openaleph_names(_BUNDLE)
    assert len(fake.calls) == 2
    person_call = next(c for c in fake.calls if c["schema"] == "Person")
    entity_call = next(c for c in fake.calls if c["schema"] == "LegalEntity")
    # Persons: broad (no topic filter), one name per line, all persons in.
    assert person_call["topics"] == ()
    assert set(person_call["text"].split("\n")) == {"Igor Sechin", "Jane Ordinary"}
    # Entities: topic-scoped to exactly what the signal ladder can act on.
    assert entity_call["topics"] == _WATCHLIST_TOPICS
    assert set(entity_call["text"].split("\n")) == {
        "Acme Holdings PLC",
        "Offshore Vehicle Ltd",
    }


async def test_duplicate_names_deduplicated_in_text(fake_percolate) -> None:
    fake = fake_percolate([], [])
    bundle = _BUNDLE + [_person_stmt("stmt-sechin-2", "IGOR SECHIN")]
    await assess_openaleph_names(bundle)
    person_call = next(c for c in fake.calls if c["schema"] == "Person")
    # Case-variant duplicate collapses via the shared normaliser.
    assert person_call["text"].count("Sechin") + person_call["text"].count("SECHIN") == 1


# ---------------------------------------------------------------------------
# Signals — topic ladder + attribution
# ---------------------------------------------------------------------------


async def test_sanctioned_person_yields_related_sanctioned(fake_percolate) -> None:
    fake_percolate(
        [
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=["sanction"],
                collection="UK FCDO Sanctions List",
            )
        ],
        [],
    )
    signals = await assess_openaleph_names(_BUNDLE)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.code == "RELATED_SANCTIONED"
    assert sig.source_id == "openaleph"
    assert sig.confidence == "high"  # exact name match
    assert sig.evidence["subject_statement_id"] == "stmt-sechin"
    assert sig.evidence["surface_form"] == "Igor Sechin"
    assert sig.evidence["collection"] == "UK FCDO Sanctions List"
    assert "UK FCDO Sanctions List" in sig.summary


async def test_topic_ladder_priority_and_pep(fake_percolate) -> None:
    fake_percolate(
        [
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=["role.pep"],
                entity_id="oa-pep",
                collection="Wikidata PEPs",
            ),
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=["debarment"],
                entity_id="oa-debar",
                collection="US SAM Exclusions",
            ),
        ],
        [],
    )
    signals = await assess_openaleph_names(_BUNDLE)
    codes = {s.code for s in signals}
    assert codes == {"RELATED_PEP", "RELATED_DEBARMENT"}


async def test_control_suppresses_adjacency_in_the_shared_ladder(fake_percolate) -> None:
    """The OpenAleph screen shares cross_check's reporting rule, so the same
    multi-signal behaviour applies here — one classifier, one rule."""
    fake_percolate(
        [
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=["sanction.control", "sanction.linked", "debarment"],
                entity_id="oa-control",
                collection="OpenSanctions Default",
            ),
        ],
        [],
    )
    signals = await assess_openaleph_names(_BUNDLE)
    codes = [s.code for s in signals]
    assert codes == ["RELATED_SANCTIONS_CONTROLLED", "RELATED_DEBARMENT"]
    assert "RELATED_SANCTIONS_LINKED" not in codes
    assert "ownership chain" in signals[0].summary


async def test_counter_sanction_reports_separately_and_last(fake_percolate) -> None:
    """One classifier, one rule — the percolation screen must split
    ``sanction.counter`` out of RELATED_SANCTIONED exactly as cross_check
    does, and rank it last so it is never taken as the headline finding."""
    fake_percolate(
        [
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=["sanction", "sanction.counter"],
                entity_id="oa-counter",
                collection="OpenSanctions Default",
            ),
        ],
        [],
    )
    signals = await assess_openaleph_names(_BUNDLE)
    assert [s.code for s in signals] == [
        "RELATED_SANCTIONED",
        "RELATED_COUNTER_SANCTIONED",
    ]
    assert "weak democratic institutions" in signals[-1].summary


async def test_entity_pep_topic_never_fires_related_pep(fake_percolate) -> None:
    """Entities can't be PEPs — a role.pep-tagged entity hit maps to no
    signal and lands in the informational block instead (cross_check rule)."""
    fake_percolate(
        [],
        [
            _percolate_item(
                "Offshore Vehicle Ltd",
                surface_form="Offshore Vehicle Ltd",
                topics=["role.pep"],
                schema="Company",
            )
        ],
    )
    screening: list[dict] = []
    signals = await assess_openaleph_names(_BUNDLE, screening=screening)
    assert signals == []
    assert len(screening) == 1
    assert screening[0]["statement_id"] == "stmt-vehicle"


async def test_partial_name_match_is_gated_out(fake_percolate) -> None:
    """An entity named just 'Offshore' fires on 'Offshore Vehicle Ltd'
    (percolation matches sub-phrases) but fails the similarity gate."""
    fake_percolate(
        [],
        [
            _percolate_item(
                "Offshore",
                surface_form="Offshore",
                topics=["sanction"],
                schema="Company",
            )
        ],
    )
    screening: list[dict] = []
    signals = await assess_openaleph_names(_BUNDLE, screening=screening)
    assert signals == []
    assert screening == []  # below the gate → not even informational


async def test_alias_match_scored_against_best_hit_name(fake_percolate) -> None:
    """percolator_match=other_name: the hit's display name differs but an
    alias equals the target — the best-name scorer must find it."""
    item = _percolate_item(
        "Igor Ivanovich SECHIN",
        surface_form="Igor Sechin",
        topics=["sanction"],
        match=["other_name"],
    )
    item["properties"]["alias"] = ["Igor Sechin"]
    fake_percolate([item], [])
    signals = await assess_openaleph_names(_BUNDLE)
    assert len(signals) == 1
    assert signals[0].evidence["matched_name"] == "Igor Sechin"


async def test_birth_year_mismatch_rejected(fake_percolate) -> None:
    fake_percolate(
        [
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=["sanction"],
                birth_date="1932-01-01",  # bundle says 1960
            )
        ],
        [],
    )
    assert await assess_openaleph_names(_BUNDLE) == []


async def test_single_token_person_name_guarded(fake_percolate) -> None:
    """A bare-surname person target can't base a match (ftmg guard)."""
    bundle = [_person_stmt("stmt-surname", "Fernández")]
    fake_percolate(
        [
            _percolate_item(
                "Fernández",
                surface_form="Fernández",
                topics=["sanction"],
            )
        ],
        [],
    )
    assert await assess_openaleph_names(bundle) == []


# ---------------------------------------------------------------------------
# Informational screening block
# ---------------------------------------------------------------------------


async def test_sub_signal_match_goes_to_screening(fake_percolate) -> None:
    fake_percolate(
        [
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=["poi"],
                collection="Russian Oligarch Database",
            ),
            _percolate_item(
                "Igor Sechin",
                surface_form="Igor Sechin",
                topics=None,  # e.g. a court-records collection
                entity_id="oa-court",
                collection="Court Filings",
            ),
        ],
        [],
    )
    screening: list[dict] = []
    signals = await assess_openaleph_names(_BUNDLE, screening=screening)
    assert signals == []
    assert len(screening) == 2
    first = screening[0]
    assert first["statement_id"] == "stmt-sechin"
    assert first["collection"] == "Russian Oligarch Database"
    assert first["surface_form"] == "Igor Sechin"
    assert first["url"].startswith("https://search.openaleph.org/entities/")


async def test_screening_entries_deduplicated_per_statement_and_entity(
    fake_percolate,
) -> None:
    item = _percolate_item(
        "Igor Sechin",
        surface_form="Igor Sechin",
        topics=["poi"],
        entity_id="oa-same",
    )
    fake_percolate([item, dict(item)], [])
    screening: list[dict] = []
    await assess_openaleph_names(_BUNDLE, screening=screening)
    assert len(screening) == 1


# ---------------------------------------------------------------------------
# Degradation (issue #50 contract)
# ---------------------------------------------------------------------------


async def test_no_api_key_records_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("OPENALEPH_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        degraded: list[DegradedSource] = []
        signals = await assess_openaleph_names(_BUNDLE, degraded=degraded)
        assert signals == []
        assert len(degraded) == 1
        assert degraded[0].source_id == "openaleph"
        assert degraded[0].check == CHECK_NAME
        assert degraded[0].reason == DEGRADED_NOT_CONFIGURED
        assert "RELATED_SANCTIONED" in degraded[0].affected_signals
        # Never the names themselves — counts only.
        assert "Sechin" not in degraded[0].detail
    finally:
        get_settings.cache_clear()


async def test_failed_call_records_upstream_error(fake_percolate) -> None:
    """percolate_text returning None (HTTP failure / pre-5.3.1 404) is
    'screen did not run' — degradation, not a clean screen."""
    fake_percolate(None, [])
    degraded: list[DegradedSource] = []
    signals = await assess_openaleph_names(_BUNDLE, degraded=degraded)
    assert signals == []
    assert len(degraded) == 1
    assert degraded[0].reason == DEGRADED_UPSTREAM_ERROR
    assert "person" in degraded[0].detail
    assert "Sechin" not in degraded[0].detail


async def test_live_mode_off_is_a_noop(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    try:
        degraded: list[DegradedSource] = []
        assert await assess_openaleph_names(_BUNDLE, degraded=degraded) == []
        assert degraded == []  # offline is expected, not a degradation
    finally:
        get_settings.cache_clear()


async def test_empty_bundle_is_a_noop() -> None:
    assert await assess_openaleph_names([]) == []


# ---------------------------------------------------------------------------
# Pipeline wiring
# ---------------------------------------------------------------------------


async def test_pipeline_gathers_openaleph_screen(monkeypatch) -> None:
    """The lookup pipeline's risk stage must call assess_openaleph_names
    alongside cross_check and icij, and put its screening block on the
    risk_signals event."""
    from opencheck.routers import lookup as lookup_mod

    called: dict = {}

    async def fake_assess(bods, *, degraded=None, screening=None, **kw):
        called["bods_len"] = len(bods)
        if screening is not None:
            screening.append({"statement_id": "stmt-x", "collection": "C"})
        return []

    monkeypatch.setattr(lookup_mod, "assess_openaleph_names", fake_assess)
    # The import-time reference in the /report path is separate; this test
    # pins the streaming pipeline (which both endpoints share).
    src = None
    async for event, payload in lookup_mod._lookup_pipeline(
        "5493001KJTIIGC8Y1R12", deepen_top=0
    ):
        if event == "risk_signals":
            src = payload
    assert called  # the screen ran
    assert src is not None
    assert src.get("openaleph_screening") == [
        {"statement_id": "stmt-x", "collection": "C"}
    ]
