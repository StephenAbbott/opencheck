"""The sweep's GLEIF dispatch-drift check (Phase 162).

The first published sweep reported twelve of twenty-four anchors as drifted.
None had drifted: the check fetched each anchor through ``GleifAdapter.fetch``
— four to six GLEIF requests per LEI — and the shared 50/min budget ran out.
Three things are pinned here: the check costs one request per anchor; an
anchor that could not be fetched is *unchecked*, never drift; and the report
says how many were verified, how many could not be checked and how many
drifted, as three numbers.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

# The script is a CLI and sets OPENCHECK_ALLOW_LIVE at import so that a sweep
# reaches real sources; a test process must not inherit that, or every stub
# test collected after this file sees a live-enabled registry.
_had_allow_live = "OPENCHECK_ALLOW_LIVE" in os.environ
import source_health as sweep  # noqa: E402

if not _had_allow_live:
    os.environ.pop("OPENCHECK_ALLOW_LIVE", None)
from opencheck.sources import REGISTRY  # noqa: E402
from opencheck.sources.gleif import GleifAdapter, GleifRateLimitedError  # noqa: E402
from opencheck.sources.probes import PROBES  # noqa: E402


def _entity(registered_at: str, registered_as: str) -> dict[str, Any]:
    return {"registeredAt": {"id": registered_at}, "registeredAs": registered_as}


@pytest.fixture
def gleif_calls(monkeypatch):
    """Replace the Level 1 fetch with a scripted one and count the calls."""
    calls: list[str] = []
    answers: dict[str, Any] = {}

    async def fetch_entity(self, lei: str) -> dict[str, Any]:
        calls.append(lei)
        answer = answers[lei]
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(GleifAdapter, "fetch_entity", fetch_entity)

    async def fetch(self, hit_id: str):  # the expensive path must not be used
        raise AssertionError("dispatch-drift check must not call GleifAdapter.fetch")

    monkeypatch.setattr(GleifAdapter, "fetch", fetch)
    return calls, answers


def _anchored(source_id: str) -> tuple[str, str]:
    probe = PROBES[source_id]
    assert probe.anchor_lei, f"{source_id} needs an anchor LEI for this test"
    return probe.anchor_lei, str(probe.args[0])


def test_verified_anchor_costs_one_level_1_request(gleif_calls) -> None:
    calls, answers = gleif_calls
    lei, expected = _anchored("brreg")
    ra = next(iter(getattr(REGISTRY["brreg"], "lookup_derivers")[0].ra_codes))
    # GLEIF returns Norway's number with spaces — the normaliser is exercised for real.
    answers[lei] = _entity(ra, f"{expected[:3]} {expected[3:6]} {expected[6:]}")
    import asyncio

    results = asyncio.run(sweep.check_dispatch_drift(["brreg"]))
    assert [(r.source_id, r.status, r.derived) for r in results] == [("brreg", sweep.OK, expected)]
    assert calls == [lei], "one Level 1 request per anchor, nothing else"


def test_unfetchable_anchor_is_unchecked_not_drift(gleif_calls) -> None:
    calls, answers = gleif_calls
    lei, _ = _anchored("brreg")
    answers[lei] = GleifRateLimitedError(
        "GLEIF request budget exhausted (50/min shared across this process) and no slot frees within 15s"
    )
    import asyncio

    (result,) = asyncio.run(sweep.check_dispatch_drift(["brreg"]))
    assert result.status == sweep.UNCHECKED
    assert result.status != sweep.FAIL
    assert result.reason.startswith("anchor could not be fetched from GLEIF: ")
    assert "budget exhausted" in result.reason


def test_real_drift_is_still_a_failure(gleif_calls) -> None:
    calls, answers = gleif_calls
    lei, expected = _anchored("brreg")
    answers[lei] = _entity("RA999999", expected)  # registered somewhere the adapter never dispatches on
    import asyncio

    (result,) = asyncio.run(sweep.check_dispatch_drift(["brreg"]))
    assert result.status == sweep.FAIL
    assert "dispatch drift" in result.reason


def test_failures_sort_before_unchecked_before_verified(gleif_calls) -> None:
    calls, answers = gleif_calls
    ok_lei, ok_expected = _anchored("brreg")
    ok_ra = next(iter(getattr(REGISTRY["brreg"], "lookup_derivers")[0].ra_codes))
    answers[ok_lei] = _entity(ok_ra, ok_expected)
    bad_lei, bad_expected = _anchored("ares")
    answers[bad_lei] = _entity("RA999999", bad_expected)
    un_lei, _ = _anchored("cvr_denmark")
    answers[un_lei] = GleifRateLimitedError("budget")
    import asyncio

    results = asyncio.run(sweep.check_dispatch_drift(["brreg", "cvr_denmark", "ares"]))
    assert [r.status for r in results] == [sweep.FAIL, sweep.UNCHECKED, sweep.OK]


def _report(drift: list[sweep.DriftResult]) -> dict[str, Any]:
    return sweep.build_report([], drift, None)


def test_markdown_says_three_numbers_and_never_counts_unchecked_as_drift() -> None:
    drift = [
        sweep.DriftResult("ares", sweep.OK, registered_at="RA000163", derived="29700949"),
        sweep.DriftResult("brreg", sweep.OK, registered_at="RA000472", derived="923609016"),
        sweep.DriftResult("kvk", sweep.UNCHECKED, reason="anchor could not be fetched from GLEIF: budget"),
        sweep.DriftResult("zefix", sweep.UNCHECKED, reason="anchor could not be fetched from GLEIF: budget"),
        sweep.DriftResult("prh", sweep.FAIL, reason="dispatch drift: GLEIF anchor is registered at RA000001, adapter dispatches on RA000488 — this source would never be reached"),
        sweep.DriftResult("malta_mbr", sweep.SKIPPED, reason="no anchor LEI on the probe — dispatch is not covered"),
    ]
    md = sweep.render_markdown(_report(drift))
    assert (
        "6 identifier-dispatched sources: **2 verified** from their GLEIF anchor · "
        "**2 could not be checked** · **1 drifted** · 1 not covered."
    ) in md
    assert "still resolve from their GLEIF anchor" not in md
    assert "- ❌ `prh` — dispatch drift" in md
    assert "- ⚠️ `kvk` — anchor could not be fetched from GLEIF: budget" in md
    assert "- ⏭️ not covered (no anchor LEI): `malta_mbr`" in md


def test_markdown_all_clear() -> None:
    md = sweep.render_markdown(_report([sweep.DriftResult("ares", sweep.OK, registered_at="RA000163", derived="1")]))
    assert "**1 verified**" in md and "- ✅ no drift" in md


def test_only_real_drift_reds_the_run() -> None:
    """The exit-code rule reads FAIL alone; UNCHECKED is reported, not fatal."""
    unchecked = _report([sweep.DriftResult("kvk", sweep.UNCHECKED, reason="x")])
    assert not any(row["status"] == sweep.FAIL for row in unchecked["dispatch_drift"].values())
    drifted = _report([sweep.DriftResult("kvk", sweep.FAIL, reason="x")])
    assert any(row["status"] == sweep.FAIL for row in drifted["dispatch_drift"].values())


def test_fetch_entity_reads_the_level_1_record_only(monkeypatch) -> None:
    """One request, to /lei-records/{lei}, with the record's own cache key."""
    seen: list[tuple[str, str]] = []

    async def _get(self, path: str, *, cache_key: str, max_age_days=None):
        seen.append((path, cache_key))
        return {"data": {"attributes": {"entity": {"registeredAs": "923 609 016", "registeredAt": {"id": "RA000472"}}}}}

    monkeypatch.setattr(GleifAdapter, "_get", _get)
    import asyncio

    entity = asyncio.run(REGISTRY["gleif"].fetch_entity(" 5967007lieexzx4lpr43 "))
    assert entity == {"registeredAs": "923 609 016", "registeredAt": {"id": "RA000472"}}
    assert seen == [("/lei-records/5967007LIEEXZX4LPR43", "gleif/lei/5967007LIEEXZX4LPR43")]


@pytest.mark.parametrize(
    ("source_id", "registered_at", "registered_as"),
    [
        # Values as GLEIF returned them on 2026-09-03, when each anchor was chosen (Phase 163).
        ("abr_australia", "RA000014", "123 123 124"),
        ("malta_mbr", "RA000443", "C 2833"),
        ("mca_india", "RA000394", "L85110KA1981PLC013115"),
    ],
)
def test_phase_163_anchors_derive_their_probe_subject(gleif_calls, source_id, registered_at, registered_as) -> None:
    """The three probes that had no anchor LEI now have one whose GLEIF record
    derives the probe subject through the adapter's own normaliser — the
    whole point of an anchor. Pinned against the values GLEIF actually
    returned, so a future edit to a probe cannot silently break the join."""
    calls, answers = gleif_calls
    lei, expected = _anchored(source_id)
    answers[lei] = _entity(registered_at, registered_as)
    import asyncio

    (result,) = asyncio.run(sweep.check_dispatch_drift([source_id]))
    assert (result.status, result.derived) == (sweep.OK, expected), result.reason


def test_every_identifier_dispatched_probe_now_has_an_anchor() -> None:
    """Phase 163 closed the last three gaps; the drift check covers the whole set."""
    uncovered = [
        sid
        for sid, probe in PROBES.items()
        if getattr(REGISTRY[sid], "lookup_derivers", ()) and not probe.anchor_lei
    ]
    assert uncovered == []
