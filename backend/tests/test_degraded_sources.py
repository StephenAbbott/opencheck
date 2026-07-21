"""Degraded upstream screens (issue #50).

When a derived risk check — cross-source name screening, ICIJ
offshore-leaks reconciliation — cannot fully run, the lookup must say so
via ``degraded_sources`` records instead of letting the empty result pass
for a clean screen. These tests cover:

* reason classification (closed vocabulary),
* record emission from both derived checks (missing key, upstream
  failures, timeouts, rate limiting),
* the privacy constraint: degradation records NEVER contain the
  related-party names being screened,
* the narrative packet (degradations become citable gaps; the "no risks
  found" fact is qualified),
* the report builders (Screening limitations block; the no-signals text
  no longer claims "returned clear" when degraded).
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from opencheck.config import get_settings
from opencheck.cross_check import assess_cross_source_names
from opencheck.icij_check import _RECONCILE_URL, assess_icij_names
from opencheck.narrative.packet import build_evidence_packet
from opencheck.reporting.html_report import _risk as _html_risk
from opencheck.reporting.markdown_report import _risk as _md_risk
from opencheck.risk import (
    DEGRADED_NOT_CONFIGURED,
    DEGRADED_RATE_LIMITED,
    DEGRADED_TIMEOUT,
    DEGRADED_UPSTREAM_ERROR,
    DegradedSource,
    classify_degradation_reason,
    pick_degradation_reason,
)
from opencheck.sources import REGISTRY, SearchKind, SourceHit

# Distinctive names that must never surface in a degradation record.
_SECRET_PERSON = "Zaltan Quirrelmort"
_SECRET_ENTITY = "Obsidian Falcon Holdings"


def _person(sid: str, full_name: str) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordType": "person",
        "recordDetails": {
            "personType": "knownPerson",
            "names": [{"type": "individual", "fullName": full_name}],
        },
    }


def _entity(sid: str, name: str) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordType": "entity",
        "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": name},
    }


def _bundle() -> list[dict[str, Any]]:
    return [_person("p1", _SECRET_PERSON), _entity("e1", _SECRET_ENTITY)]


def _assert_no_names(degraded: list[DegradedSource]) -> None:
    """The privacy constraint from issue #50: counts only, never names."""
    payload = json.dumps([d.to_dict() for d in degraded])
    for fragment in (
        _SECRET_PERSON,
        _SECRET_ENTITY,
        "Zaltan",
        "Quirrelmort",
        "Obsidian",
        "Falcon",
    ):
        assert fragment not in payload, f"degradation record leaked a name: {fragment}"


# ---------------------------------------------------------------------
# Reason classification
# ---------------------------------------------------------------------


def _status_error(code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.org")
    response = httpx.Response(code, request=request)
    return httpx.HTTPStatusError("boom", request=request, response=response)


def test_classify_timeout() -> None:
    assert classify_degradation_reason(httpx.ConnectTimeout("slow")) == DEGRADED_TIMEOUT


def test_classify_rate_limited() -> None:
    assert classify_degradation_reason(_status_error(429)) == DEGRADED_RATE_LIMITED


def test_classify_http_error() -> None:
    assert classify_degradation_reason(_status_error(500)) == DEGRADED_UPSTREAM_ERROR


def test_classify_fallback() -> None:
    assert classify_degradation_reason(ValueError("weird")) == DEGRADED_UPSTREAM_ERROR


def test_pick_reason_prefers_most_frequent_then_most_systemic() -> None:
    assert (
        pick_degradation_reason({DEGRADED_TIMEOUT: 3, DEGRADED_UPSTREAM_ERROR: 1})
        == DEGRADED_TIMEOUT
    )
    # Tie → the more systemic reason wins.
    assert (
        pick_degradation_reason({DEGRADED_UPSTREAM_ERROR: 1, DEGRADED_RATE_LIMITED: 1})
        == DEGRADED_RATE_LIMITED
    )
    assert pick_degradation_reason({}) == DEGRADED_UPSTREAM_ERROR


# ---------------------------------------------------------------------
# cross_check — degradation emission
# ---------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _live_with_key(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _FailingAdapter:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        raise self._exc


class _EmptyAdapter:
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []


async def test_cross_check_missing_key_emits_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    get_settings.cache_clear()
    degraded: list[DegradedSource] = []
    signals = await assess_cross_source_names(_bundle(), degraded=degraded)
    assert signals == []
    assert {d.source_id for d in degraded} == {"opensanctions", "everypolitician"}
    assert all(d.reason == DEGRADED_NOT_CONFIGURED for d in degraded)
    assert all(d.check == "cross_source_names" for d in degraded)
    os_rec = next(d for d in degraded if d.source_id == "opensanctions")
    assert "RELATED_SANCTIONED" in os_rec.affected_signals
    assert "RELATED_PEP" in os_rec.affected_signals
    ep_rec = next(d for d in degraded if d.source_id == "everypolitician")
    assert ep_rec.affected_signals == ["RELATED_PEP"]
    _assert_no_names(degraded)


async def test_cross_check_missing_key_without_targets_is_not_degraded(
    monkeypatch,
) -> None:
    """No screenable names → nothing was skipped → no degradation records."""
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    get_settings.cache_clear()
    degraded: list[DegradedSource] = []
    bods = [{"statementId": "r1", "recordType": "relationship", "recordDetails": {}}]
    assert await assess_cross_source_names(bods, degraded=degraded) == []
    assert degraded == []


async def test_cross_check_live_off_is_not_degraded(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    degraded: list[DegradedSource] = []
    assert await assess_cross_source_names(_bundle(), degraded=degraded) == []
    assert degraded == []


async def test_cross_check_upstream_failures_emit_per_source_records(
    monkeypatch,
) -> None:
    monkeypatch.setitem(
        REGISTRY, "opensanctions", _FailingAdapter(_status_error(429))
    )
    monkeypatch.setitem(
        REGISTRY, "everypolitician", _FailingAdapter(httpx.ConnectTimeout("slow"))
    )
    degraded: list[DegradedSource] = []
    signals = await assess_cross_source_names(_bundle(), degraded=degraded)
    assert signals == []
    by_source = {d.source_id: d for d in degraded}
    assert by_source["opensanctions"].reason == DEGRADED_RATE_LIMITED
    # EveryPolitician is only probed for persons (1 of the 2 targets).
    assert by_source["everypolitician"].reason == DEGRADED_TIMEOUT
    assert "2 of 2" in by_source["opensanctions"].detail
    assert "1 of 2" in by_source["everypolitician"].detail
    _assert_no_names(degraded)


async def test_cross_check_clean_run_emits_nothing(monkeypatch) -> None:
    monkeypatch.setitem(REGISTRY, "opensanctions", _EmptyAdapter())
    monkeypatch.setitem(REGISTRY, "everypolitician", _EmptyAdapter())
    degraded: list[DegradedSource] = []
    assert await assess_cross_source_names(_bundle(), degraded=degraded) == []
    assert degraded == []


async def test_cross_check_without_collector_still_works(monkeypatch) -> None:
    """The collector is optional — existing callers keep their behaviour."""
    monkeypatch.setitem(REGISTRY, "opensanctions", _FailingAdapter(_status_error(500)))
    monkeypatch.setitem(REGISTRY, "everypolitician", _EmptyAdapter())
    assert await assess_cross_source_names(_bundle()) == []


# ---------------------------------------------------------------------
# icij_check — degradation emission
# ---------------------------------------------------------------------


async def test_icij_failure_emits_degraded_record(httpx_mock) -> None:
    httpx_mock.add_exception(httpx.ConnectTimeout("slow"), url=_RECONCILE_URL)
    degraded: list[DegradedSource] = []
    signals = await assess_icij_names(_bundle(), degraded=degraded)
    assert signals == []
    assert len(degraded) == 1
    rec = degraded[0]
    assert rec.source_id == "icij"
    assert rec.check == "icij_offshore_leaks"
    assert rec.affected_signals == ["OFFSHORE_LEAKS"]
    assert rec.reason == DEGRADED_TIMEOUT
    assert "1 of 1" in rec.detail and "2 of 2" in rec.detail
    _assert_no_names(degraded)


async def test_icij_rate_limit_classified(httpx_mock) -> None:
    httpx_mock.add_response(url=_RECONCILE_URL, method="POST", status_code=429)
    degraded: list[DegradedSource] = []
    await assess_icij_names(_bundle(), degraded=degraded)
    assert degraded[0].reason == DEGRADED_RATE_LIMITED


async def test_icij_success_emits_nothing(httpx_mock) -> None:
    httpx_mock.add_response(
        url=_RECONCILE_URL, method="POST", json={"q0": {"result": []}, "q1": {"result": []}}
    )
    degraded: list[DegradedSource] = []
    assert await assess_icij_names(_bundle(), degraded=degraded) == []
    assert degraded == []


# ---------------------------------------------------------------------
# Narrative packet
# ---------------------------------------------------------------------


def _degraded_dicts() -> list[dict[str, Any]]:
    return [
        DegradedSource(
            source_id="opensanctions",
            check="cross_source_names",
            affected_signals=["RELATED_SANCTIONED", "RELATED_PEP"],
            detail="Search failed for 2 of 2 related-party name(s).",
            reason=DEGRADED_TIMEOUT,
        ).to_dict()
    ]


def test_packet_degraded_screens_become_gaps() -> None:
    report = {
        "lei": "5493001KJTIIGC8Y1R12",
        "legal_name": "Example AG",
        "bods": [],
        "risk_signals": [],
        "degraded_sources": _degraded_dicts(),
    }
    packet = build_evidence_packet(report)
    gap_text = " ".join(g.statement for g in packet.gaps)
    assert "did not fully run" in gap_text
    assert "not a clean screen" in gap_text
    assert "RELATED_SANCTIONED" in gap_text


def test_packet_no_risk_fact_is_qualified_when_degraded() -> None:
    report = {
        "lei": "5493001KJTIIGC8Y1R12",
        "legal_name": "Example AG",
        "bods": [],
        "risk_signals": [],
        "degraded_sources": _degraded_dicts(),
    }
    packet = build_evidence_packet(report)
    absence = next(
        f for f in packet.facts if "no structural or jurisdictional" in f.statement
    )
    assert "did not fully run" in absence.statement
    assert "not conclusive" in absence.statement
    assert absence.confidence == "medium"


def test_packet_no_risk_fact_is_unqualified_when_clean() -> None:
    report = {
        "lei": "5493001KJTIIGC8Y1R12",
        "legal_name": "Example AG",
        "bods": [],
        "risk_signals": [],
        "degraded_sources": [],
    }
    packet = build_evidence_packet(report)
    absence = next(
        f for f in packet.facts if "no structural or jurisdictional" in f.statement
    )
    assert "did not fully run" not in absence.statement
    assert absence.confidence == "high"


# ---------------------------------------------------------------------
# Report builders (PDF is rendered from the HTML)
# ---------------------------------------------------------------------


def test_html_risk_section_renders_screening_limitations() -> None:
    html = _html_risk({"risk_signals": [], "degraded_sources": _degraded_dicts()})
    assert "Screening limitations" in html
    assert "not a complete clear" in html
    assert "returned clear" not in html
    assert "the upstream service timed out" in html


def test_html_risk_section_clean_run_keeps_original_wording() -> None:
    html = _html_risk({"risk_signals": [], "degraded_sources": []})
    assert "Screening limitations" not in html
    assert "returned clear" in html


def test_markdown_risk_section_renders_screening_limitations() -> None:
    lines = _md_risk({"risk_signals": [], "degraded_sources": _degraded_dicts()})
    text = "\n".join(lines)
    assert "Screening limitations" in text
    assert "not a complete clear" in text
    assert "returned clear" not in text


def test_html_limitations_render_alongside_signals() -> None:
    sig = {
        "code": "NON_EU_JURISDICTION",
        "confidence": "medium",
        "summary": "Registered outside the EU/EEA.",
        "source_id": "gleif",
        "hit_id": "x",
        "evidence": {},
    }
    html = _html_risk({"risk_signals": [sig], "degraded_sources": _degraded_dicts()})
    assert "Screening limitations" in html
    assert "Non Eu Jurisdiction" in html
