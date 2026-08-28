"""Tests for the ICIJ Offshore Leaks name cross-check.

Covers the deterministic helpers (target extraction, name similarity,
dataset/jurisdiction parsing, dedup) directly, plus integration tests
that exercise ``assess_icij_names`` against a mocked reconciliation API.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.icij_check import (
    _MIN_NAME_SIM,
    _RECONCILE_URL,
    _RESULTS_PER_TYPE,
    _SCREENED_TYPES,
    _collect_targets,
    _dedupe,
    _name_sim,
    _node_type,
    _normalise,
    _screenable,
    _parse_collection,
    _parse_dataset,
    _parse_jurisdiction,
    _signal_from_match,
    assess_icij_names,
)
from opencheck.risk import OFFSHORE_LEAKS, RiskSignal


# ---------------------------------------------------------------------
# Helpers — person / entity statement builders
# ---------------------------------------------------------------------


def _person(sid: str, full_name: str | None = None, *, person_type: str = "knownPerson") -> dict[str, Any]:
    rd: dict[str, Any] = {"personType": person_type}
    if full_name:
        rd["names"] = [{"type": "individual", "fullName": full_name}]
    return {"statementId": sid, "recordType": "person", "recordDetails": rd}


def _entity(sid: str, name: str | None = None, *, entity_type: str = "registeredEntity") -> dict[str, Any]:
    rd: dict[str, Any] = {"entityType": {"type": entity_type}}
    if name:
        rd["name"] = name
    return {"statementId": sid, "recordType": "entity", "recordDetails": rd}


# ---------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------


def test_normalise_strips_diacritics() -> None:
    assert _normalise("BJÖRK") == "bjork"
    assert _normalise("Ángel") == "angel"


def test_normalise_lower_and_collapses_spaces() -> None:
    assert _normalise("  ACME  CORP  ") == "acme corp"


# ---------------------------------------------------------------------
# Name similarity
# ---------------------------------------------------------------------


def test_name_sim_exact_match() -> None:
    assert _name_sim("Acme Holdings", "ACME HOLDINGS") == 1.0


def test_name_sim_partial_overlap() -> None:
    sim = _name_sim("Acme Holdings Ltd", "ACME HOLDINGS")
    assert 0.6 < sim < 1.0


def test_name_sim_no_overlap() -> None:
    # Two unrelated names must land well clear of the screening gate. The
    # shared scorer is character-based, so unrelated strings still share some
    # letters — what matters is the margin below _MIN_NAME_SIM, not zero.
    assert _name_sim("Vladimir Putin", "John Smith") < 0.5
    assert _name_sim("Vladimir Putin", "John Smith") < _MIN_NAME_SIM


def test_name_sim_is_the_shared_phase_d_scorer() -> None:
    """The bespoke token-Jaccard scorer is gone: this module must use the
    same scorer as every other matching surface, so improvements there land
    here too."""
    from opencheck import names

    for a, b in (
        ("CHAUMET INTERNATIONAL SA.", "BRONTE INTERNATIONAL SA"),
        ("MOET HENNESSY INTERNATIONAL", "HENNESSY INTERNATIONAL LIMITED"),
        ("GLENCORE PLC", "Glencore plc"),
    ):
        assert _name_sim(a, b) == names.name_similarity(a, b)


def test_name_sim_separates_boilerplate_collisions_from_real_matches() -> None:
    """The old unweighted token-overlap scored both of these 0.500 — a false
    positive and a true match, indistinguishable — because INTERNATIONAL / SA
    / LIMITED counted as evidence."""
    collision = _name_sim("CHAUMET INTERNATIONAL SA.", "BRONTE INTERNATIONAL SA")
    real = _name_sim("MOET HENNESSY INTERNATIONAL", "HENNESSY INTERNATIONAL LIMITED")
    assert collision < real
    # Both still sit under the calibrated gate for THIS surface: company
    # names against 800k offshore records need 0.93, not the 0.88 used for
    # person screening. See _MIN_NAME_SIM.
    assert collision < _MIN_NAME_SIM


def test_name_sim_empty_returns_zero() -> None:
    assert _name_sim("", "ACME") == 0.0
    assert _name_sim("ACME", "") == 0.0


# ---------------------------------------------------------------------
# Dataset / jurisdiction parsing
# ---------------------------------------------------------------------


def test_parse_dataset_panama() -> None:
    assert _parse_dataset("Panama Papers · British Virgin Islands") == "Panama Papers"


def test_parse_dataset_pandora() -> None:
    assert _parse_dataset("Pandora Papers · Luxembourg") == "Pandora Papers"


def test_parse_dataset_paradise() -> None:
    assert _parse_dataset("Paradise Papers · Bermuda") == "Paradise Papers"


def test_parse_dataset_unknown_returns_first_part() -> None:
    assert _parse_dataset("Some New Leak · Cayman Islands") == "Some New Leak"


def test_parse_dataset_empty() -> None:
    assert _parse_dataset("") == ""


def test_parse_jurisdiction_returns_second_part() -> None:
    assert _parse_jurisdiction("Panama Papers · British Virgin Islands") == "British Virgin Islands"


def test_parse_jurisdiction_no_separator() -> None:
    assert _parse_jurisdiction("Panama Papers") == ""


def test_parse_jurisdiction_empty() -> None:
    assert _parse_jurisdiction("") == ""


# ---------------------------------------------------------------------
# Reconciliation v0.2 description sentences. ICIJ replaced the
# bullet-separated "Panama Papers · British Virgin Islands" with a
# free-text sentence, "<NodeType> node extracted from the <Dataset>
# data." — confirmed live 2026-07-30 across all four node types. The
# bullet parser matched none of it, so the whole sentence landed in the
# user-facing summary ("matches a record in the Entity node extracted
# from the Panama Papers data.") and jurisdiction was always empty.
# ---------------------------------------------------------------------

_V02_DESCRIPTIONS = [
    ("Entity node extracted from the Panama Papers data.", "Panama Papers", ""),
    (
        "Address node extracted from the Paradise Papers - Appleby data.",
        "Paradise Papers",
        "Appleby",
    ),
    (
        "Officer node extracted from the Paradise Papers - Aruba corporate registry data.",
        "Paradise Papers",
        "Aruba corporate registry",
    ),
    ("Entity node extracted from the Offshore Leaks data.", "Offshore Leaks", ""),
    ("Entity node extracted from the Bahamas Leaks data.", "Bahamas Leaks", ""),
]


@pytest.mark.parametrize("description,dataset,collection", _V02_DESCRIPTIONS)
def test_parse_v02_sentence_descriptions(
    description: str, dataset: str, collection: str
) -> None:
    assert _parse_dataset(description) == dataset
    assert _parse_collection(description) == collection
    # A leak sub-collection is not a jurisdiction — "Appleby" is a law firm.
    assert _parse_jurisdiction(description) == ""


def test_parse_dataset_never_returns_prose() -> None:
    """An unparsed sentence yields "" so the caller falls back to the
    generic label, rather than pasting prose into the summary."""
    assert _parse_dataset("Something else entirely extracted from nowhere") == ""


def test_parse_dataset_unknown_sentence_leak_passes_through() -> None:
    # A leak ICIJ adds later should still be named, once isolated from prose.
    assert (
        _parse_dataset("Entity node extracted from the Some New Leak data.")
        == "Some New Leak"
    )


def test_parse_collection_legacy_and_empty() -> None:
    assert _parse_collection("Panama Papers · British Virgin Islands") == ""
    assert _parse_collection("") == ""


def test_node_type_reads_types_and_legacy_type() -> None:
    # v0.2 renamed the field from "type" to "types"; both are read.
    assert _node_type({"types": [{"id": ".../oldb/address", "name": "Address"}]}) == "Address"
    assert _node_type({"type": [{"id": "/type/entity", "name": "Entity"}]}) == "Entity"
    assert _node_type({"types": ["Officer"]}) == "Officer"
    assert _node_type({}) == ""
    assert _node_type({"types": "Entity"}) == ""


def test_signal_summary_reads_cleanly_on_v02_description() -> None:
    """Regression on the garbled production copy once seen on LVMH MOET
    HENNESSY LOUIS VUITTON (LEI IOG4E947OATN0KJYSD45) — the whole ICIJ
    description sentence was being pasted into the summary."""
    sig = _signal_from_match(
        {
            "id": "10171805",
            "name": "GLENCORE INTERNATIONAL AG",
            "score": 100,
            "match": False,
            "types": [{"id": ".../oldb/entity", "name": "Entity"}],
            "description": "Entity node extracted from the Panama Papers data.",
        },
        {
            "kind": "entity",
            "statement_id": "stmt-1",
            "name": "GLENCORE INTERNATIONAL AG",
        },
        min_score=70,
    )
    assert sig is not None
    assert sig.summary == (
        "Related entity 'GLENCORE INTERNATIONAL AG' matches a record in "
        "the Panama Papers."
    )
    # The score is on the record, not in the sentence (Phase 136).
    assert sig.evidence["icij_score"] == 100
    assert sig.evidence["dataset"] == "Panama Papers"
    assert sig.evidence["node_type"] == "Entity"
    assert sig.evidence["collection"] == ""


def test_intermediary_match_is_worded_as_a_different_finding() -> None:
    """An Intermediary node is the law firm / formation agent that ARRANGED
    the structure, not a party named in it — so it must not be folded into
    the generic "matches a record" wording."""
    sig = _signal_from_match(
        {
            "id": "77",
            "name": "GLENCORE INTERNATIONAL AG",
            "score": 100,
            "match": True,
            "types": [{"id": ".../oldb/intermediary", "name": "Intermediary"}],
            "description": "Intermediary node extracted from the Panama Papers data.",
        },
        {
            "kind": "entity",
            "statement_id": "stmt-2",
            "name": "GLENCORE INTERNATIONAL AG",
        },
        min_score=70,
    )
    assert sig is not None
    assert sig.summary == (
        "Related entity 'GLENCORE INTERNATIONAL AG' appears as an "
        "offshore-services intermediary in the Panama Papers."
    )
    assert sig.evidence["node_type"] == "Intermediary"


def test_boilerplate_collision_no_longer_fires() -> None:
    """Measured false positive: two unrelated Turkish banks sharing only
    legal-form tokens. Fired under the old 0.45 token-overlap gate."""
    sig = _signal_from_match(
        {
            "id": "1",
            "name": "TURKIYE GARANTI BANKASI ANONIM SIRKETI",
            "score": 77,
            "match": False,
            "types": [{"id": ".../oldb/entity", "name": "Entity"}],
            "description": "Entity node extracted from the Panama Papers data.",
        },
        {
            "kind": "entity",
            "statement_id": "stmt-3",
            "name": "TÜRKİYE FİNANS KATILIM BANKASI",
        },
        min_score=70,
    )
    assert sig is None


def test_signal_summary_names_the_sub_collection() -> None:
    sig = _signal_from_match(
        {
            "id": "1",
            "name": "ACME HOLDINGS LIMITED",
            "score": 91,
            "match": True,
            "types": [{"id": ".../oldb/entity", "name": "Entity"}],
            "description": "Entity node extracted from the Paradise Papers - Appleby data.",
        },
        {"kind": "entity", "statement_id": "stmt-2", "name": "ACME HOLDINGS LIMITED"},
        min_score=70,
    )
    assert sig is not None
    assert "the Paradise Papers (Appleby)" in sig.summary
    assert sig.evidence["collection"] == "Appleby"


# ---------------------------------------------------------------------
# Target extraction
# ---------------------------------------------------------------------


def test_collect_targets_extracts_persons_and_entities() -> None:
    bods = [
        _person("p1", "Mossack Fonseca"),
        _entity("e1", "Acme BVI Ltd"),
    ]
    targets = _collect_targets(bods)
    assert {(t["kind"], t["name"]) for t in targets} == {
        ("person", "Mossack Fonseca"),
        ("entity", "Acme BVI Ltd"),
    }


def test_collect_targets_skips_unknown_and_anonymous() -> None:
    bods = [
        _person("p1", person_type="unknownPerson"),
        _entity("e1", "Anon", entity_type="anonymousEntity"),
        _person("p2", "Real Person"),
    ]
    targets = _collect_targets(bods)
    assert [t["statement_id"] for t in targets] == ["p2"]


def test_collect_targets_skips_nameless_records() -> None:
    bods = [_person("p1", None), _entity("e1", None)]
    assert _collect_targets(bods) == []


# ---------------------------------------------------------------------
# Signal construction
# ---------------------------------------------------------------------


def _icij_match(
    name: str = "ACME BVI LTD",
    score: int = 90,
    match: bool = True,
    description: str = "Panama Papers · British Virgin Islands",
    node_id: str = "https://offshoreleaks.icij.org/nodes/12345",
) -> dict[str, Any]:
    return {
        "id": node_id,
        "name": name,
        "score": score,
        "match": match,
        "description": description,
    }


def _target(name: str = "Acme BVI Ltd", kind: str = "entity", sid: str = "e1") -> dict[str, Any]:
    return {"kind": kind, "statement_id": sid, "name": name}


# ---------------------------------------------------------------------
# Reconciliation API v0.2 (ICIJ moved to /api/v1/ — the bare path 404s)
# ---------------------------------------------------------------------


async def test_http_error_is_logged_as_a_warning(
    httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 404 (the reconciliation service moving again) must be visible in the
    logs — the failure is still swallowed, but no longer silent."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    httpx_mock.add_response(url=_RECONCILE_URL, method="POST", status_code=404)

    bods = [
        {
            "statementId": "e1",
            "recordType": "entity",
            "recordDetails": {"name": "ACME BVI LTD"},
        }
    ]
    with caplog.at_level(logging.WARNING):
        signals = await assess_icij_names(bods)

    assert signals == []  # still degrades gracefully
    assert "HTTP 404" in caplog.text
    assert "degraded" in caplog.text
    get_settings.cache_clear()


async def test_network_error_is_logged_as_a_warning(
    httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    httpx_mock.add_exception(httpx.ConnectError("connection refused"))

    bods = [
        {
            "statementId": "e1",
            "recordType": "entity",
            "recordDetails": {"name": "ACME BVI LTD"},
        }
    ]
    with caplog.at_level(logging.WARNING):
        signals = await assess_icij_names(bods)

    assert signals == []
    assert "ConnectError" in caplog.text
    get_settings.cache_clear()


def test_reconcile_url_is_the_versioned_api_path() -> None:
    """Regression: ``https://offshoreleaks.icij.org/reconcile`` now 404s.
    ICIJ serves the reconciliation service under ``/api/v1/``."""
    from opencheck.icij_check import _RECONCILE_URL

    assert _RECONCILE_URL == "https://offshoreleaks.icij.org/api/v1/reconcile"


def test_bare_node_id_is_expanded_to_a_public_node_url() -> None:
    """Spec v0.2 returns a bare node id, not a URL — the link is rebuilt."""
    sig = _signal_from_match(_icij_match(node_id="12345"), _target(), min_score=70)
    assert sig is not None
    assert sig.evidence["node_url"] == "https://offshoreleaks.icij.org/nodes/12345"
    assert sig.hit_id == "https://offshoreleaks.icij.org/nodes/12345"


def test_absolute_node_url_passes_through_unchanged() -> None:
    """A full URL (the pre-v0.2 shape) must not be double-prefixed."""
    sig = _signal_from_match(
        _icij_match(node_id="https://offshoreleaks.icij.org/nodes/777"),
        _target(),
        min_score=70,
    )
    assert sig is not None
    assert sig.evidence["node_url"] == "https://offshoreleaks.icij.org/nodes/777"


def test_missing_node_id_yields_empty_node_url() -> None:
    sig = _signal_from_match(_icij_match(node_id=""), _target(), min_score=70)
    assert sig is not None
    assert sig.evidence["node_url"] == ""
    assert sig.hit_id.startswith("icij:")  # falls back to the slug


def test_signal_from_match_high_confidence_when_match_true() -> None:
    sig = _signal_from_match(_icij_match(match=True, score=85), _target(), min_score=70)
    assert sig is not None
    assert sig.code == OFFSHORE_LEAKS
    assert sig.confidence == "high"
    assert sig.source_id == "icij"


def test_signal_from_match_medium_confidence_when_match_false_above_threshold() -> None:
    sig = _signal_from_match(_icij_match(match=False, score=75), _target(), min_score=70)
    assert sig is not None
    assert sig.confidence == "medium"


def test_signal_from_match_none_below_threshold() -> None:
    sig = _signal_from_match(_icij_match(match=False, score=50), _target(), min_score=70)
    assert sig is None


def test_signal_from_match_match_true_overrides_threshold() -> None:
    """match: true should produce a signal even below the score threshold."""
    sig = _signal_from_match(_icij_match(match=True, score=30), _target(), min_score=70)
    assert sig is not None
    assert sig.confidence == "high"


def test_signal_from_match_name_too_dissimilar_returns_none() -> None:
    """Returned name wildly different from search name → sanity-check rejects."""
    sig = _signal_from_match(
        _icij_match(name="TOTALLY UNRELATED COMPANY LTD", match=True, score=95),
        _target(name="Acme BVI Ltd"),
        min_score=70,
    )
    assert sig is None


def test_distinctive_token_gate_kills_boilerplate_collision() -> None:
    """The Phase 120 gate: generic tokens matching while the distinctive
    token differs must never produce a signal — even with ICIJ's own
    match flag set, since ICIJ rated the ENERGEN/BIOGAS collision 90/100."""
    sig = _signal_from_match(
        _icij_match(name="COSCO INTERNATIONAL HOLDINGS LTD", match=True, score=95),
        _target(name="CASTROL Holdings International Ltd"),
        min_score=70,
    )
    assert sig is None


def test_distinctive_token_gate_kills_numbered_spv_collision() -> None:
    """'HORNSEA 1 LIMITED' vs 'HORNSEA LIMITED' scores 0.9375 — above even
    the old 0.93 cut — and the corpus re-measurement found the same shape
    twice more (WIGMORE 1, PRACTICE PLUS/PLAN). The numeric discriminator
    rule is what kills it."""
    sig = _signal_from_match(
        _icij_match(name="HORNSEA LIMITED", match=False, score=88),
        _target(name="HORNSEA 1 LIMITED"),
        min_score=70,
    )
    assert sig is None


def test_lowered_threshold_recovers_named_true_matches() -> None:
    """The two matches PR #86 lost at 0.93 come back at 0.87: the person
    pair via the threshold alone (persons bypass the token gate), the org
    pair via subset agreement on its distinctive residue. The second one
    is LVMH's only offshore-leaks signal."""
    person = _signal_from_match(
        _icij_match(name="NICHOLAS RATCLIFFE", match=False, score=78),
        _target(name="NICHOLAS PAUL RATCLIFFE", kind="person"),
        min_score=70,
    )
    assert person is not None
    org = _signal_from_match(
        _icij_match(name="HENNESSY INTERNATIONAL LIMITED", match=False, score=83),
        _target(name="MOET HENNESSY INTERNATIONAL"),
        min_score=70,
    )
    assert org is not None


def test_corporate_officer_person_target_is_gated() -> None:
    """BODS person statements sometimes hold corporate officers. A
    'person' whose name carries a legal form is an organisation for
    matching purposes — the eval corpus caught 'CSC CORPORATE SERVICES
    (UK) LIMITED' (person-kind) matching 'HMSA CORPORATE SERVICES (UK)
    LIMITED' at 0.925, which a kind-only bypass would admit."""
    sig = _signal_from_match(
        _icij_match(name="HMSA CORPORATE SERVICES (UK) LIMITED", match=False, score=91),
        _target(name="CSC CORPORATE SERVICES (UK) LIMITED", kind="person"),
        min_score=70,
    )
    assert sig is None


def test_min_name_sim_is_injectable() -> None:
    """The harness sweeps thresholds through the kwarg instead of
    monkeypatching the module constant."""
    match = _icij_match(name="HENNESSY INTERNATIONAL LIMITED", match=False, score=83)
    target = _target(name="MOET HENNESSY INTERNATIONAL")
    assert _signal_from_match(match, target, min_score=70, min_name_sim=0.93) is None
    assert _signal_from_match(match, target, min_score=70, min_name_sim=0.87) is not None


def test_signal_evidence_contains_expected_fields() -> None:
    sig = _signal_from_match(_icij_match(), _target(), min_score=70)
    assert sig is not None
    assert sig.evidence["subject_statement_id"] == "e1"
    assert sig.evidence["dataset"] == "Panama Papers"
    assert sig.evidence["jurisdiction"] == "British Virgin Islands"
    assert sig.evidence["node_url"] == "https://offshoreleaks.icij.org/nodes/12345"
    assert sig.evidence["icij_score"] == 90


def test_signal_summary_mentions_dataset_and_jurisdiction() -> None:
    sig = _signal_from_match(_icij_match(), _target(), min_score=70)
    assert sig is not None
    assert "Panama Papers" in sig.summary
    assert "British Virgin Islands" in sig.summary


def test_signal_hit_id_is_icij_node_url() -> None:
    sig = _signal_from_match(_icij_match(node_id="https://offshoreleaks.icij.org/nodes/99"), _target(), min_score=70)
    assert sig is not None
    assert sig.hit_id == "https://offshoreleaks.icij.org/nodes/99"


# ---------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------


def _make_sig(hit_id: str = "node1", sid: str = "e1", confidence: str = "medium") -> RiskSignal:
    return RiskSignal(
        code=OFFSHORE_LEAKS,
        confidence=confidence,
        summary="test",
        source_id="icij",
        hit_id=hit_id,
        evidence={"subject_statement_id": sid},
    )


def test_dedupe_collapses_same_node_same_statement() -> None:
    sigs = [_make_sig("node1", "e1", "medium"), _make_sig("node1", "e1", "high")]
    deduped = _dedupe(sigs)
    assert len(deduped) == 1
    assert deduped[0].confidence == "high"


def test_dedupe_keeps_same_node_different_statements() -> None:
    """One ICIJ node matching two different BODS statements → two chips."""
    sigs = [_make_sig("node1", "e1"), _make_sig("node1", "e2")]
    assert len(_dedupe(sigs)) == 2


def test_dedupe_keeps_different_nodes_same_statement() -> None:
    sigs = [_make_sig("node1", "e1"), _make_sig("node2", "e1")]
    assert len(_dedupe(sigs)) == 2


# ---------------------------------------------------------------------
# Integration — assess_icij_names
# ---------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _live_mode(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_no_op_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    assert await assess_icij_names([_entity("e1", "Acme")]) == []


async def test_no_op_on_empty_bundle() -> None:
    assert await assess_icij_names([]) == []


async def test_no_op_when_only_anonymous_entities() -> None:
    bods = [_entity("e1", "Anon", entity_type="anonymousEntity")]
    assert await assess_icij_names(bods) == []


async def test_emits_signal_on_reconciliation_match(monkeypatch) -> None:
    """Mock the ICIJ API to return a high-confidence match and verify the signal."""
    # Query keys are per-name AND per-node-type now; the Entity slot carries
    # the match, the other scoped slots come back empty as they would live.
    api_response = {
        "q0-entity": {
            "result": [
                {
                    "id": "https://offshoreleaks.icij.org/nodes/12345",
                    "name": "MOSSACK FONSECA",
                    "score": 95,
                    "match": True,
                    "description": "Panama Papers · Panama",
                }
            ]
        },
        "q0-officer": {"result": []},
        "q0-intermediary": {"result": []},
    }

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = MagicMock(return_value=api_response)

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("opencheck.icij_check.build_client", return_value=mock_client):
        bods = [_entity("e1", "Mossack Fonseca")]
        signals = await assess_icij_names(bods)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.code == OFFSHORE_LEAKS
    assert sig.source_id == "icij"
    assert sig.confidence == "high"
    assert sig.evidence["subject_statement_id"] == "e1"
    assert sig.evidence["dataset"] == "Panama Papers"
    assert sig.hit_id == "https://offshoreleaks.icij.org/nodes/12345"


async def test_no_signal_when_score_below_threshold(monkeypatch) -> None:
    api_response = {
        "q0": {
            "result": [
                {
                    "id": "https://offshoreleaks.icij.org/nodes/99999",
                    "name": "MOSSACK FONSECA",
                    "score": 40,
                    "match": False,
                    "description": "Panama Papers · Panama",
                }
            ]
        }
    }

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = MagicMock(return_value=api_response)
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("opencheck.icij_check.build_client", return_value=mock_client):
        bods = [_entity("e1", "Mossack Fonseca")]
        signals = await assess_icij_names(bods)

    assert signals == []


async def test_api_error_returns_empty_not_exception(monkeypatch) -> None:
    """Network errors should be swallowed — risk pipeline continues."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(side_effect=Exception("Connection refused"))

    with patch("opencheck.icij_check.build_client", return_value=mock_client):
        bods = [_entity("e1", "Acme BVI")]
        signals = await assess_icij_names(bods)

    assert signals == []


async def test_max_targets_limits_batch_size(monkeypatch) -> None:
    """Only the first N targets should be sent to the API."""
    posted_queries: list[dict] = []

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = MagicMock(return_value={})

    async def capture_post(url, **kwargs):
        data = kwargs.get("data") or {}
        queries_raw = data.get("queries", "{}")
        posted_queries.append(json.loads(queries_raw))
        return mock_response

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = capture_post

    with patch("opencheck.icij_check.build_client", return_value=mock_client):
        bods = [_entity(f"e{i}", f"Company {i}") for i in range(20)]
        await assess_icij_names(bods, max_targets=5)

    # 5 targets, batched 8 names per request → 1 request, and each name is
    # asked once per screened node type.
    assert len(posted_queries) == 1
    total_queries = sum(len(q) for q in posted_queries)
    assert total_queries == 5 * len(_SCREENED_TYPES)


async def test_batch_is_type_scoped_and_excludes_address() -> None:
    """Every outgoing query carries exactly one node type, Address is never
    asked for, and a batch stays inside the service's declared batchSize."""
    posted_queries: list[dict[str, Any]] = []

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = MagicMock(return_value={})

    async def capture_post(url, **kwargs):
        posted_queries.append(json.loads((kwargs.get("data") or {})["queries"]))
        return mock_response

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = capture_post

    with patch("opencheck.icij_check.build_client", return_value=mock_client):
        bods = [_entity(f"e{i}", f"Company Number {i}") for i in range(20)]
        await assess_icij_names(bods, max_targets=20)

    asked_types: set[str] = set()
    for batch in posted_queries:
        # The manifest declares batchSize 25 — never exceed it.
        assert len(batch) <= 25
        for spec in batch.values():
            # A LIST of types is silently ignored by the service, so each
            # query must carry exactly one scalar type.
            assert isinstance(spec["type"], str)
            asked_types.add(spec["type"])
            assert spec["limit"] == _RESULTS_PER_TYPE

    assert asked_types == set(_SCREENED_TYPES.values())
    assert not any("address" in t for t in asked_types)


def test_screenable_guard_blocks_names_that_erode_to_nothing() -> None:
    """The "S +" trap: an LVMH subsidiary whose sanitised name is one
    character matches an ICIJ officer node named "s" at score 100 / sim 1.00.
    Real one-word company names must survive the guard."""
    from opencheck.http import sanitize_name_query

    assert not _screenable(sanitize_name_query("S +"))
    assert not _screenable("")
    assert not _screenable("AB")
    for keep in ("KENZO", "CELINE", "BERLUTI", "PRIMAE", "UFIPAR", "LVMH"):
        assert _screenable(sanitize_name_query(keep)), keep


# ----------------------------------------------------------------------
# No retrieval score reaches the sentence (Phase 136)
# ----------------------------------------------------------------------


@pytest.mark.parametrize("score", [70, 77, 90, 99, 100])
@pytest.mark.parametrize(
    "node", ["Entity", "Officer", "Intermediary"]
)
def test_no_offshore_leaks_summary_prints_the_icij_score(
    score: int, node: str
) -> None:
    """Every summary this module can produce, read rather than reasoned about.

    The string came off the page because it overstated what it measured: a
    denominator reads as certainty, and ICIJ's own scorer rated the
    ENERGEN/BIOGAS collision 90/100 — which is why `min_name_sim` and the
    distinctive-token gate exist. Both wordings carried it (the Intermediary
    branch is a separate sentence, and a fix to one would not have touched the
    other), so this drives both node families across the whole accepted range.

    The subject name is deliberately digit-free, so asserting the score's
    digits are absent cannot be satisfied by a name that happens to contain
    them.
    """
    sig = _signal_from_match(
        {
            "id": "1",
            "name": "GLENCORE INTERNATIONAL AG",
            "score": score,
            "match": True,
            "types": [{"id": f".../oldb/{node.lower()}", "name": node}],
            "description": f"{node} node extracted from the Panama Papers data.",
        },
        {
            "kind": "entity",
            "statement_id": "stmt-1",
            "name": "GLENCORE INTERNATIONAL AG",
        },
        min_score=70,
    )
    assert sig is not None
    assert "score" not in sig.summary.lower()
    assert "/100" not in sig.summary
    assert str(score) not in sig.summary
    # Kept where the gate and any later analysis can read it.
    assert sig.evidence["icij_score"] == score
