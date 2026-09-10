"""The week-over-week statement diff: a retyped edge is not a lost edge.

Phase 199 retyped GEM's direct owners from ``otherInfluenceOrControl`` to
``shareholding``. The Fingrid probe's ``interest:otherInfluenceOrControl`` went
4 → 0 with the same four edges still there, and because the diff compares
against the last *successful* run, that would have redded the sweep every
Monday. These tests pin both halves: the reclassification is excused, and a
real loss of one interest type is still the alarm it was built to be.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

_had_allow_live = "OPENCHECK_ALLOW_LIVE" in os.environ
import source_health as sweep  # noqa: E402

if not _had_allow_live:
    os.environ.pop("OPENCHECK_ALLOW_LIVE", None)


def _run(counts: dict[str, int], source_id: str = "climatetrace") -> dict:
    return {source_id: {"statement_counts": counts}}


def _previous(counts: dict[str, int], source_id: str = "climatetrace") -> dict:
    return {"sources": _run(counts, source_id)}


FINGRID_BEFORE = {
    "entity": 5, "person": 0, "relationship": 4, "interest:otherInfluenceOrControl": 4,
}
FINGRID_AFTER = {
    "entity": 5, "person": 0, "relationship": 4, "interest:shareholding": 4,
}


def test_retyped_edges_are_not_a_collapse() -> None:
    assert sweep.diff_statement_counts(_run(FINGRID_AFTER), _previous(FINGRID_BEFORE)) == {}


def test_retyped_edges_are_still_reported_as_a_reclassification() -> None:
    assert sweep.reclassified_statement_counts(
        _run(FINGRID_AFTER), _previous(FINGRID_BEFORE)
    ) == {
        "climatetrace": {
            "interest:otherInfluenceOrControl": {"was": 4, "now": 0},
            "interest:shareholding": {"was": 0, "now": 4},
        }
    }


def test_losing_an_interest_type_with_nothing_in_its_place_is_still_a_collapse() -> None:
    """Estonia's beneficial owners arrive as otherInfluenceOrControl beside the
    officers' seniorManagingOfficial. If the BO half disappears behind an access
    wall the relationship total need not halve — this is the alarm."""
    before = {
        "entity": 1, "person": 6, "relationship": 6,
        "interest:seniorManagingOfficial": 4, "interest:otherInfluenceOrControl": 2,
    }
    after = {
        "entity": 1, "person": 4, "relationship": 4,
        "interest:seniorManagingOfficial": 4,
    }
    collapses = sweep.diff_statement_counts(
        _run(after, "ariregister"), _previous(before, "ariregister")
    )
    assert collapses == {
        "ariregister": {"interest:otherInfluenceOrControl": {"was": 2, "now": 0}}
    }
    assert sweep.reclassified_statement_counts(
        _run(after, "ariregister"), _previous(before, "ariregister")
    ) == {}


def test_a_retyping_that_also_loses_edges_is_still_a_collapse() -> None:
    """Gains elsewhere must not paper over a shrinking total: 50 interests
    becoming 31 is a loss whatever the per-type arithmetic says."""
    before = {"relationship": 50, "interest:a": 10, "interest:b": 40}
    after = {"relationship": 31, "interest:b": 21, "interest:c": 10}
    assert sweep.diff_statement_counts(_run(after), _previous(before)) == {
        "climatetrace": {"interest:a": {"was": 10, "now": 0}}
    }
    assert sweep.reclassified_statement_counts(_run(after), _previous(before)) == {}


def test_a_record_type_collapse_is_never_excused_by_interest_gains() -> None:
    before = {"entity": 10, "relationship": 8, "interest:otherInfluenceOrControl": 8}
    after = {"entity": 2, "relationship": 8, "interest:shareholding": 8}
    assert sweep.diff_statement_counts(_run(after), _previous(before)) == {
        "climatetrace": {"entity": {"was": 10, "now": 2}}
    }


def test_markdown_names_the_reclassification_and_still_says_no_collapse() -> None:
    report = sweep.build_report([], [], {"generated_at": "2026-09-07T06:00:00Z", "sources": {}})
    report["statement_reclassifications"] = sweep.reclassified_statement_counts(
        _run(FINGRID_AFTER), _previous(FINGRID_BEFORE)
    )
    md = sweep.render_markdown(report)
    assert "- ✅ no collapse against the run of 2026-09-07T06:00:00Z." in md
    assert (
        "- ↔️ `climatetrace` — interest types reclassified, not a collapse: "
        "interest:otherInfluenceOrControl 4 → 0, interest:shareholding 0 → 4"
    ) in md
