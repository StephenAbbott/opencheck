"""The verdict sentence must state findings without implying a conclusion,
and must never read as a clean screen when a screen did not run.
"""

from __future__ import annotations

from typing import Any

from opencheck.verdict import build_verdict


def _sig(code: str, kind: str = "risk", **evidence: Any) -> dict[str, Any]:
    return {
        "code": code,
        "confidence": "high",
        "summary": f"{code} fired.",
        "source_id": "opensanctions",
        "hit_id": "hit-1",
        "evidence": evidence,
        "kind": kind,
    }


def _degraded(source_id: str = "opentender") -> dict[str, Any]:
    return {
        "source_id": source_id,
        "check": "debarment",
        "affected_signals": ["DEBARMENT"],
        "detail": "the upstream service timed out",
        "reason": "timeout",
    }


def test_a_genuinely_empty_run_says_so_plainly() -> None:
    # Every source answered and none of them found anything. That is a
    # result, and the sentence names the qualifier that makes it one.
    assert build_verdict([], []) == (
        "No risk signals surfaced across the sources that answered."
    )


def test_subject_sanctions_lead_the_sentence() -> None:
    v = build_verdict([_sig("SANCTIONED")], [])
    assert v == "Sanctions findings on the company itself."


def test_two_findings_are_joined_but_a_third_is_not() -> None:
    v = build_verdict(
        [
            _sig("SANCTIONED"),
            _sig("EXPORT_CONTROLLED"),
            _sig("OFFSHORE_LEAKS"),
        ],
        [],
    )
    assert v is not None
    # Two clauses read as a sentence; three read as a list, and the chips
    # beside the sentence already carry the full set.
    assert v.count(", and ") == 1
    assert "offshore-leaks" not in v


def test_subject_findings_outrank_related_ones() -> None:
    v = build_verdict([_sig("RELATED_SANCTIONED"), _sig("SANCTIONED")], [])
    assert v is not None
    assert v.startswith("Sanctions findings on the company itself")


def test_structure_is_appended_not_asserted_as_a_finding() -> None:
    v = build_verdict(
        [_sig("SANCTIONED"), _sig("COMPLEX_OWNERSHIP_LAYERS", layers=4)],
        [],
    )
    assert v == "Sanctions findings on the company itself, over an ownership chain 4 layers deep."


def test_layer_count_is_read_from_the_signal_not_recomputed() -> None:
    v = build_verdict([_sig("COMPLEX_OWNERSHIP_LAYERS", layers=7)], [])
    assert v is not None
    assert "7 layers deep" in v


def test_deepest_layer_count_wins_when_several_are_present() -> None:
    v = build_verdict(
        [
            _sig("COMPLEX_OWNERSHIP_LAYERS", layers=3),
            _sig("COMPLEX_OWNERSHIP_LAYERS", layers=5),
        ],
        [],
    )
    assert v is not None
    assert "5 layers deep" in v


def test_context_signals_never_produce_a_risk_clause() -> None:
    # NON_EU_JURISDICTION is kind="context" — a structural observation, not
    # an adverse finding, and the sentence must not present it as one.
    v = build_verdict([_sig("NON_EU_JURISDICTION", kind="context")], [])
    assert v is not None
    assert "finding" not in v


def test_a_clean_run_with_a_failed_check_never_reads_as_clean() -> None:
    v = build_verdict([], [_degraded()])
    assert v is not None
    assert "not a clean screen" in v
    assert "one check did not run" in v


def test_failed_checks_are_counted_by_distinct_source() -> None:
    v = build_verdict([], [_degraded("opentender"), _degraded("icij"), _degraded("icij")])
    assert v is not None
    assert "2 checks did not run" in v


def test_structure_only_run_with_a_failed_check_keeps_the_caveat() -> None:
    v = build_verdict(
        [_sig("COMPLEX_OWNERSHIP_LAYERS", layers=3)],
        [_degraded()],
    )
    assert v is not None
    assert "3 layers deep" in v
    assert "not a clean screen" in v


def test_the_sentence_never_grades_the_company() -> None:
    # The verdict states what the records contain. Grading is the analyst's.
    banned = ("high risk", "low risk", "safe", "clean", "recommend", "should not")
    for signals in (
        [_sig("SANCTIONED"), _sig("PEP")],
        [_sig("RELATED_DEBARMENT")],
        [_sig("STATE_CONTROLLED"), _sig("COMPLEX_OWNERSHIP_LAYERS", layers=6)],
        [],
    ):
        v = build_verdict(signals, []) or ""
        lowered = v.lower()
        for word in banned:
            # "not a clean screen" is the one permitted use of "clean".
            if word == "clean" and "not a clean screen" in lowered:
                continue
            assert word not in lowered, f"{word!r} in {v!r}"


def test_every_sentence_is_one_sentence() -> None:
    for signals in (
        [_sig("SANCTIONED")],
        [_sig("SANCTIONED"), _sig("EXPORT_CONTROLLED"), _sig("COMPLEX_OWNERSHIP_LAYERS", layers=4)],
        [_sig("STATE_CONTROLLED")],
    ):
        v = build_verdict(signals, [])
        assert v is not None
        assert v.endswith(".")
        assert v.count(".") == 1
        assert v[0].isupper()


def test_unknown_codes_are_dropped_rather_than_half_rendered() -> None:
    # A code added to risk.py without a clause here must simply not appear,
    # never produce "Findings on the company itself" with an empty subject.
    v = build_verdict([_sig("SOME_FUTURE_CODE")], [])
    assert v is None
