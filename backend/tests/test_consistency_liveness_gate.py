"""The liveness gate's second way in (18 Nov 2026).

A liveness pair with no disagreement may enter the allowlist when it has
enough decided comparisons and every register side has read a company as
ended at least once — the proof the comparison can disagree. The counters
carry that count as ``terminal_readings``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from opencheck import consistencystats
from opencheck.bods import liveness
from opencheck.consistency import assess_consistency
from opencheck.consistency_allowlist import ZERO_DISAGREE_MIN_DECIDED, passes_gate

from tests.test_consistency import _stmt


def _row(agree: int, disagree: int, readings: dict[str, int] | None = None) -> dict:
    row = {"agree": agree, "disagree": disagree}
    if readings is not None:
        row["terminal_readings"] = readings
    return row


# ---------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------


def test_the_phase_337_gate_is_unchanged() -> None:
    assert passes_gate("liveness", "brreg", "gleif", _row(2426, 35))
    assert not passes_gate("liveness", "firmenbuch", "gleif", _row(5246, 745))
    assert not passes_gate("liveness", "dlcp_dc", "gleif", _row(1, 3))
    assert not passes_gate("liveness", "a", "gleif", _row(0, 0))


def test_volume_alone_does_not_admit_a_pair_that_never_disagreed() -> None:
    assert not passes_gate("liveness", "ariregister", "gleif", _row(520, 0))
    assert not passes_gate("liveness", "ariregister", "gleif", _row(520, 0, {"ariregister": 0, "gleif": 3}))


def test_an_ended_reading_by_the_register_and_volume_admit_it() -> None:
    assert passes_gate("liveness", "ariregister", "gleif", _row(520, 0, {"ariregister": 1, "gleif": 0}))


def test_too_few_decided_comparisons_are_not_enough() -> None:
    n = ZERO_DISAGREE_MIN_DECIDED - 1
    assert not passes_gate("liveness", "ariregister", "gleif", _row(n, 0, {"ariregister": 5, "gleif": 0}))


def test_two_registers_must_each_show_an_ended_reading() -> None:
    pair = ("anaf_romania", "onrc_romania")
    assert not passes_gate("liveness", *pair, _row(900, 0, {"anaf_romania": 4, "onrc_romania": 0}))
    assert passes_gate("liveness", *pair, _row(900, 0, {"anaf_romania": 4, "onrc_romania": 1}))


def test_the_second_way_in_is_for_liveness_only() -> None:
    assert not passes_gate("jurisdiction", "ariregister", "gleif", _row(900, 0, {"ariregister": 3}))


# ---------------------------------------------------------------------
# The counter
# ---------------------------------------------------------------------


def test_each_side_s_ended_readings_are_counted_whatever_the_relation(tmp_path: Path) -> None:
    path = tmp_path / "consistencystats.sqlite"
    consistencystats.reset()
    try:
        consistencystats.configure(path)
        # disagree: the register reads ended, GLEIF live
        consistencystats.record(
            assess_consistency(
                [_stmt("ariregister", status=liveness.TERMINAL), _stmt("gleif", status=liveness.LIVE)]
            )
        )
        # one_missing: the register reads ended, GLEIF has no status
        consistencystats.record(
            assess_consistency([_stmt("ariregister", status=liveness.TERMINAL), _stmt("gleif")])
        )
        # agree on live: no ended reading
        consistencystats.record(
            assess_consistency([_stmt("ariregister", status=liveness.LIVE), _stmt("gleif", status=liveness.LIVE)])
        )
        row = consistencystats.stats()["pairs"]["liveness|ariregister|gleif"]
        assert row["terminal_readings"] == {"ariregister": 2, "gleif": 0}
        assert row["agree"] == 1 and row["disagree"] == 1 and row["one_missing"] == 1
        assert not any(k.startswith("_liveness_ended") for k in consistencystats.stats()["pairs"])
        # Survives a restart, and the file holds only which side.
        consistencystats.reset()
        consistencystats.configure(path)
        again = consistencystats.stats()["pairs"]["liveness|ariregister|gleif"]
        assert again["terminal_readings"] == {"ariregister": 2, "gleif": 0}
    finally:
        consistencystats.reset()
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            "SELECT relation, n FROM pairs WHERE field = '_liveness_ended'"
        ).fetchall()
        assert rows == [("a", 2)]
    finally:
        conn.close()


def test_a_non_liveness_row_carries_no_terminal_readings() -> None:
    consistencystats.reset()
    try:
        consistencystats.record(
            assess_consistency(
                [_stmt("ariregister", status=liveness.TERMINAL), _stmt("gleif", status=liveness.LIVE)]
            )
        )
        for key, row in consistencystats.stats()["pairs"].items():
            assert ("terminal_readings" in row) == key.startswith("liveness|")
    finally:
        consistencystats.reset()
