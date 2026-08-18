"""Risk-signal instrumentation (``opencheck.signalstats``).

Covers:

* counting happens **after** dedup, so numbers match what a user sees
  rather than what was generated internally,
* the opt-in ``record_as`` gate, so ``/report`` traffic cannot inflate the
  per-lookup denominator,
* the ``RELATED_*`` split — screening reach and subject risk are different
  questions and the raw matrix should not have to be re-partitioned by
  whoever reads it,
* degradation counting on the closed-vocabulary fields only,
* the privacy constraint: aggregate counters NEVER carry entity names,
  LEIs, related-party names, hit ids or evidence,
* the cardinality cap, and fail-soft behaviour on malformed input,
* the ``/signalstats`` endpoint's response contract.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck import signalstats
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers.lookup import _merge_signals

# Distinctive values that must never surface in an aggregate counter.
_SECRET_PERSON = "Zaltan Quirrelmort"
_SECRET_ENTITY = "Obsidian Falcon Holdings"
_SECRET_LEI = "9999000000000000ZZ99"


@pytest.fixture(autouse=True)
def _clean_counters():
    signalstats.reset()
    yield
    signalstats.reset()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _signal(
    code: str,
    source_id: str = "opensanctions",
    hit_id: str = "hit-1",
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "confidence": "high",
        "summary": f"{code} on a match",
        "source_id": source_id,
        "hit_id": hit_id,
        "evidence": evidence or {},
    }


# ---------------------------------------------------------------------------
# Counting at the choke point
# ---------------------------------------------------------------------------


def test_merge_signals_counts_only_when_asked() -> None:
    """`/report` shares this helper. Counting it would inflate the lookup
    denominator with hand-run debugging traffic."""
    _merge_signals([_signal("SANCTIONED")])
    assert signalstats.stats()["signals_total"] == 0

    _merge_signals([_signal("SANCTIONED")], record_as="lookup")
    assert signalstats.stats()["signals_total"] == 1


def test_non_eu_jurisdiction_dedups_per_source_like_fatf() -> None:
    """Two sources finding DIFFERENT non-EU nodes must both survive.

    NON_EU_JURISDICTION used to sit in _STRUCTURAL_SIGNAL_CODES, which
    collapses on ``(code,)`` and overwrites — so the last source
    processed won outright and every jurisdiction node found by earlier
    sources was silently dropped from ``evidence.jurisdictions[]``,
    un-badging those nodes in the graph. It is a jurisdiction rule and
    must behave like the FATF jurisdiction rules.
    """
    gleif = _signal(
        "NON_EU_JURISDICTION",
        source_id="gleif",
        evidence={"jurisdictions": [{"statement_id": "E-GB", "code": "GB"}]},
    )
    ch = _signal(
        "NON_EU_JURISDICTION",
        source_id="companies_house",
        evidence={"jurisdictions": [{"statement_id": "E-US", "code": "US"}]},
    )
    merged = _merge_signals([gleif], [ch])

    survived = {
        j["statement_id"]
        for s in merged
        if s["code"] == "NON_EU_JURISDICTION"
        for j in s["evidence"]["jurisdictions"]
    }
    assert survived == {"E-GB", "E-US"}

    # Identical (code, source, hit) must still collapse to one.
    assert len(_merge_signals([gleif], [dict(gleif)])) == 1


@pytest.mark.parametrize("code", ["TRUST_OR_ARRANGEMENT", "NOMINEE"])
def test_node_scoped_structural_signals_dedup_per_source(code: str) -> None:
    """Q5: both carry per-node statement_ids and must not collapse globally.

    Structural codes key on ``(code,)`` and the merge assigns rather than
    combines, so GLEIF finding a Stiftung at E1 and Companies House finding
    a nominee at E7 collapsed to E7 alone — E1 lost its graph badge. Same
    defect and same fix as NON_EU_JURISDICTION.
    """
    gleif = _signal(
        code, source_id="gleif",
        evidence={"matches": [{"statement_id": "E1", "match": "legalForm contains 'stiftung'"}]},
    )
    ch = _signal(
        code, source_id="companies_house",
        evidence={"matches": [{"statement_id": "E7", "match": "nominee"}]},
    )
    merged = _merge_signals([gleif], [ch])
    kept = {
        m["statement_id"]
        for s in merged if s["code"] == code
        for m in s["evidence"]["matches"]
    }
    assert kept == {"E1", "E7"}
    # Identical (code, source, hit) must still collapse to one.
    assert len(_merge_signals([gleif], [dict(gleif)])) == 1


def test_whole_structure_signals_still_collapse_globally() -> None:
    """The counterpart guard — do NOT "fix" these the same way.

    COMPLEX_OWNERSHIP_LAYERS per-source would fire once per source with
    different layer counts, and the chip strip picks its winner by
    confidence, not depth, so the number shown would become arbitrary.
    """
    from opencheck.routers.lookup import _STRUCTURAL_SIGNAL_CODES

    assert {
        "COMPLEX_OWNERSHIP_LAYERS",
        "COMPLEX_CORPORATE_STRUCTURE",
        "POSSIBLE_OBFUSCATION",
    } <= _STRUCTURAL_SIGNAL_CODES
    for code in ("TRUST_OR_ARRANGEMENT", "NOMINEE", "NON_EU_JURISDICTION"):
        assert code not in _STRUCTURAL_SIGNAL_CODES


def test_counts_are_post_dedup() -> None:
    """The number reported must be what a user sees, not what was
    generated internally — related-party paths emit several signals per hit
    since PR #115, so pre-dedup counting would overstate."""
    dupe = _signal("RELATED_PEP", evidence={"subject_statement_id": "s1"})
    merged = _merge_signals([dupe], [dict(dupe)], [dict(dupe)], record_as="lookup")

    assert len(merged) == 1
    assert signalstats.stats()["signals"] == {"RELATED_PEP|opensanctions": 1}


def test_statement_scoped_signals_on_different_statements_count_separately() -> None:
    """Dedup keys RELATED_* on the subject statement, so two related parties
    flagged by one hit are two signals — and two counts."""
    _merge_signals(
        [
            _signal("RELATED_SANCTIONED", evidence={"subject_statement_id": "s1"}),
            _signal("RELATED_SANCTIONED", evidence={"subject_statement_id": "s2"}),
        ],
        record_as="lookup",
    )
    assert signalstats.stats()["signals"]["RELATED_SANCTIONED|opensanctions"] == 2


def test_same_code_from_different_sources_counts_separately() -> None:
    """The whole point of the (code, source_id) key: which source produced it."""
    _merge_signals(
        [
            _signal("PEP", source_id="opensanctions", hit_id="a"),
            _signal("PEP", source_id="openaleph", hit_id="b"),
            _signal("PEP", source_id="wikidata", hit_id="c"),
        ],
        record_as="lookup",
    )
    signals = signalstats.stats()["signals"]
    assert signals["PEP|opensanctions"] == 1
    assert signals["PEP|openaleph"] == 1
    assert signals["PEP|wikidata"] == 1


def test_related_and_subject_codes_are_reported_separately() -> None:
    """Screening reach vs subject risk — different questions, so the split
    is computed here rather than left to whoever reads the endpoint."""
    _merge_signals(
        [
            _signal("SANCTIONED"),
            _signal("RELATED_SANCTIONED", evidence={"subject_statement_id": "s1"}),
            _signal("RELATED_PEP", source_id="openaleph", evidence={"subject_statement_id": "s2"}),
        ],
        record_as="lookup",
    )
    stats = signalstats.stats()
    assert stats["signals_subject"] == {"SANCTIONED|opensanctions": 1}
    assert stats["signals_related"] == {
        "RELATED_SANCTIONED|opensanctions": 1,
        "RELATED_PEP|openaleph": 1,
    }
    # …and both halves are still visible in the combined matrix.
    assert stats["signals_total"] == 3
    assert len(stats["signals"]) == 3


# ---------------------------------------------------------------------------
# Degradation counting — the denominator that makes signal counts readable
# ---------------------------------------------------------------------------


def test_degraded_counts_key_on_source_check_reason() -> None:
    signalstats.record_degraded(
        [
            {
                "source_id": "opensanctions",
                "check": "cross_source_names",
                "reason": "rate_limited",
                "affected_signals": ["RELATED_SANCTIONED"],
                "detail": "3 of 12 names could not be screened",
            },
            {
                "source_id": "opensanctions",
                "check": "cross_source_names",
                "reason": "rate_limited",
                "affected_signals": ["RELATED_SANCTIONED"],
                "detail": "1 of 12 names could not be screened",
            },
            {
                "source_id": "icij",
                "check": "icij_offshore_leaks",
                "reason": "timeout",
                "affected_signals": ["OFFSHORE_LEAKS"],
                "detail": "the upstream service timed out",
            },
        ]
    )
    stats = signalstats.stats()
    # The two rate-limited records differ only in `detail`, which is
    # deliberately not part of the key — it is free text with unbounded
    # cardinality.
    assert stats["degraded"] == {
        "opensanctions|cross_source_names|rate_limited": 2,
        "icij|icij_offshore_leaks|timeout": 1,
    }
    assert stats["degraded_total"] == 3


def test_lookup_counter_is_the_denominator() -> None:
    assert signalstats.stats()["lookups"] == 0
    signalstats.record_lookup()
    signalstats.record_lookup()
    assert signalstats.stats()["lookups"] == 2


# ---------------------------------------------------------------------------
# Privacy — aggregate only, enforced rather than promised
# ---------------------------------------------------------------------------


def test_counters_never_carry_names_leis_or_evidence() -> None:
    """Names are stuffed into every free-text field a signal has. None of
    them may reach the counters, which read only `code` and `source_id`."""
    signalstats.record_signals(
        [
            {
                "code": "RELATED_SANCTIONED",
                "confidence": "high",
                "summary": f"{_SECRET_PERSON} is a beneficial owner of {_SECRET_ENTITY}",
                "source_id": "opensanctions",
                "hit_id": f"NK-{_SECRET_PERSON.replace(' ', '')}",
                "evidence": {
                    "subject_statement_id": "s1",
                    "matched_name": _SECRET_PERSON,
                    "lei": _SECRET_LEI,
                    "related_parties": [_SECRET_PERSON, _SECRET_ENTITY],
                },
            }
        ]
    )
    signalstats.record_degraded(
        [
            {
                "source_id": "opensanctions",
                "check": "cross_source_names",
                "reason": "timeout",
                "affected_signals": ["RELATED_SANCTIONED"],
                "detail": f"could not screen {_SECRET_PERSON}",
            }
        ]
    )

    blob = json.dumps(signalstats.stats())
    for secret in (_SECRET_PERSON, _SECRET_ENTITY, _SECRET_LEI):
        assert secret not in blob
    # The name-free parts still landed, so this is not passing by counting
    # nothing at all.
    assert "RELATED_SANCTIONED|opensanctions" in blob
    assert "opensanctions|cross_source_names|timeout" in blob


def test_privacy_holds_through_a_real_lookup(
    client: TestClient, monkeypatch, tmp_path: Path
) -> None:
    """End-to-end: run the pipeline over a seeded bundle whose entity name
    and LEI are distinctive, then check the public endpoint's payload."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    try:
        lei = "2138000000000000A001"
        target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(
                {
                    "statementId": "e-subject",
                    "recordType": "entity",
                    "recordDetails": {
                        "name": _SECRET_ENTITY,
                        "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                        "identifiers": [{"id": lei, "scheme": "XI-LEI"}],
                    },
                }
            )
            + "\n"
        )

        r = client.get("/lookup", params={"lei": lei})
        assert r.status_code == 200

        stats = client.get("/signalstats")
        assert stats.status_code == 200
        blob = json.dumps(stats.json())
        assert _SECRET_ENTITY not in blob
        assert lei not in blob
        # The pipeline ran, so the denominator moved — otherwise this test
        # would pass on an endpoint that returns nothing.
        assert stats.json()["lookups"] >= 1
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Fail-soft and bounds — instrumentation must never break a lookup
# ---------------------------------------------------------------------------


def test_malformed_records_are_skipped_not_raised() -> None:
    signalstats.record_signals(
        [
            {"code": "SANCTIONED"},  # no source_id
            {"source_id": "icij"},  # no code
            {"code": 7, "source_id": "icij"},  # wrong type
            _signal("PEP"),  # the one good record
        ]
    )
    signalstats.record_degraded([{"source_id": "icij"}, {}])
    stats = signalstats.stats()
    assert stats["signals"] == {"PEP|opensanctions": 1}
    assert stats["degraded"] == {}


def test_cardinality_cap_flags_rather_than_grows() -> None:
    """A defect that widens a key should become visible, not eat memory."""
    signalstats.record_signals(
        [_signal("CODE", source_id=f"src-{i}") for i in range(signalstats._MAX_KEYS + 50)]
    )
    stats = signalstats.stats()
    assert len(stats["signals"]) == signalstats._MAX_KEYS
    assert stats["truncated"] is True


def test_existing_keys_still_count_after_the_cap_is_hit() -> None:
    signalstats.record_signals(
        [_signal("CODE", source_id=f"src-{i}") for i in range(signalstats._MAX_KEYS)]
    )
    signalstats.record_signals([_signal("CODE", source_id="src-0")])
    assert signalstats.stats()["signals"]["CODE|src-0"] == 2


# ---------------------------------------------------------------------------
# The endpoint
# ---------------------------------------------------------------------------


def test_signalstats_endpoint_shape_and_headers(client: TestClient) -> None:
    r = client.get("/signalstats")
    assert r.status_code == 200
    assert r.headers["cache-control"] == "no-store"
    body = r.json()
    for key in (
        "started_at",
        "uptime_s",
        "lookups",
        "signals_total",
        "signals",
        "signals_subject",
        "signals_related",
        "degraded_total",
        "degraded",
        "truncated",
    ):
        assert key in body, f"missing {key}"
    # Serialises cleanly from cold, with nothing recorded yet.
    assert body["signals"] == {}
    assert body["truncated"] is False


def test_signalstats_endpoint_reports_recorded_counts(client: TestClient) -> None:
    _merge_signals([_signal("OFFSHORE_LEAKS", source_id="icij")], record_as="lookup")
    signalstats.record_lookup()
    body = client.get("/signalstats").json()
    assert body["signals"]["OFFSHORE_LEAKS|icij"] == 1
    assert body["lookups"] == 1
