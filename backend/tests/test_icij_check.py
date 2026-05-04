"""Tests for the ICIJ Offshore Leaks name cross-check.

Covers the deterministic helpers (target extraction, name similarity,
dataset/jurisdiction parsing, dedup) directly, plus integration tests
that exercise ``assess_icij_names`` against a mocked reconciliation API.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.config import get_settings
from opencheck.icij_check import (
    _collect_targets,
    _dedupe,
    _name_sim,
    _normalise,
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
    # "acme holdings ltd" vs "acme holdings" shares 2/3 tokens → 2/3
    sim = _name_sim("Acme Holdings Ltd", "ACME HOLDINGS")
    assert 0.6 < sim < 1.0


def test_name_sim_no_overlap() -> None:
    assert _name_sim("Vladimir Putin", "John Smith") < 0.2


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
    api_response = {
        "q0": {
            "result": [
                {
                    "id": "https://offshoreleaks.icij.org/nodes/12345",
                    "name": "MOSSACK FONSECA",
                    "score": 95,
                    "match": True,
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

    # 5 targets with batch_size=10 → 1 batch with 5 queries
    total_queries = sum(len(q) for q in posted_queries)
    assert total_queries == 5
