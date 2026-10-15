"""Phase 338 — integrated ownership of a group (GROW), on T-Rank's own
worked examples (docs.trank.no white papers, read 15 Oct 2026)."""

from __future__ import annotations

import pytest

from opencheck.ownership_math import (
    REASON_NO_PATH,
    REASON_NO_SHARES,
    REASON_NOT_CONVERGED,
    REASON_OVER_100,
    Edge,
    figure_text,
    integrated_ownership,
    share_bounds,
)


def e(owner: str, owned: str, pct: float | None, hi: float | None = ...) -> Edge:  # type: ignore[assignment]
    if pct is None:
        return Edge(owner, owned, 0.0, None)
    if hi is ...:
        return Edge(owner, owned, pct / 100, pct / 100)
    return Edge(owner, owned, pct / 100, None if hi is None else hi / 100)


def test_example_3_1_one_path_multiplies():
    r = integrated_ownership([e("X", "A", 40), e("A", "T", 80)], {"X"}, "T")
    assert r.minimum == pytest.approx(32) and r.maximum == pytest.approx(32)
    assert r.as_dict()["exact"] == pytest.approx(32)
    assert r.via == ["A"]


def test_example_3_2_paths_add():
    edges = [e("X", "A", 50), e("A", "T", 5), e("X", "B", 100), e("B", "T", 30), e("X", "T", 65)]
    r = integrated_ownership(edges, {"X"}, "T")
    assert r.minimum == pytest.approx(97.5)


def test_example_3_4_indirect_self_ownership_is_given_back():
    # 50% × 80% × 80% × 50% / (100% − 4%) = 16.67%; the 4% is T's own share
    # round the cycle T → B → C → T (10% × 80% × 50%).
    edges = [e("X", "A", 50), e("A", "B", 80), e("B", "C", 80), e("C", "T", 50), e("T", "B", 10)]
    r = integrated_ownership(edges, {"X"}, "T")
    assert r.minimum == pytest.approx(16.6667, abs=1e-3)
    assert figure_text(r.as_dict()) == "16.67%"


def test_direct_treasury_shares_are_given_back():
    # T holds 20% of itself; X's 40% is half of the 80% held outside.
    r = integrated_ownership([e("X", "T", 40), e("T", "T", 20)], {"X"}, "T")
    assert r.minimum == pytest.approx(50)


def test_grow_counts_each_share_once():
    # GROW paper: naive sum of the four integrated ownerships is 61.5%; the
    # group {S1..S4} holds 48%. S3 and S4 hold each other (a two-node SCC).
    edges = [
        e("S1", "C", 20), e("S2", "C", 20), e("S2", "S1", 60),
        e("S2", "N1", 50), e("N1", "C", 10),
        e("S3", "N2", 20), e("S4", "N2", 10), e("S3", "S4", 70), e("S4", "S3", 40),
        e("N2", "C", 10),
    ]
    r = integrated_ownership(edges, {"S1", "S2", "S3", "S4"}, "C")
    assert r.minimum == pytest.approx(48)
    assert r.maximum == pytest.approx(48)
    assert set(r.via) == {"N1", "N2"}


def test_a_band_carries_both_ends():
    r = integrated_ownership([Edge("S", "T", 0.75, 1.0)], {"S"}, "T")
    assert (r.minimum, r.maximum) == (pytest.approx(75), pytest.approx(100))
    assert figure_text(r.as_dict()) == "75–100%"


def test_a_band_through_an_intermediate():
    r = integrated_ownership([Edge("S", "A", 0.5, 0.75), e("A", "T", 40)], {"S"}, "T")
    assert figure_text(r.as_dict()) == "20–30%"


def test_a_missing_share_reads_at_least():
    edges = [e("S", "T", 30), e("S", "A", None), e("A", "T", 20)]
    r = integrated_ownership(edges, {"S"}, "T")
    assert r.minimum == pytest.approx(30)
    assert r.maximum is None and r.incomplete
    assert figure_text(r.as_dict()) == "at least 30%"


def test_no_share_anywhere_gives_no_figure():
    r = integrated_ownership([e("S", "T", None)], {"S"}, "T")
    assert r.reason == REASON_NO_SHARES
    assert figure_text(r.as_dict()) is None


def test_no_path_gives_no_figure():
    r = integrated_ownership([e("S", "A", 50)], {"S"}, "T")
    assert r.reason == REASON_NO_PATH


def test_one_stake_named_twice_by_one_source_is_refused_not_doubled():
    r = integrated_ownership([e("Gov", "T", 67), e("Ministry", "T", 67)], {"Gov", "Ministry"}, "T")
    assert r.reason == REASON_OVER_100
    assert r.minimum is None


def test_a_repeated_pair_is_one_holding():
    r = integrated_ownership([e("S", "T", 67), e("S", "T", 67)], {"S"}, "T")
    assert r.minimum == pytest.approx(67)


def test_holdings_inside_the_group_are_not_a_second_stake():
    # The ministry owns the state holding company; both hold the target.
    edges = [e("Ministry", "Holdco", 100), e("Holdco", "T", 30), e("Ministry", "T", 20)]
    r = integrated_ownership(edges, {"Ministry", "Holdco"}, "T")
    assert r.minimum == pytest.approx(50)


def test_a_cycle_that_does_not_settle_gives_no_figure():
    # A and B each hold all of the other: no holder outside the cycle.
    edges = [e("S", "A", 50), e("A", "B", 100), e("B", "A", 100), e("B", "T", 50)]
    r = integrated_ownership(edges, {"S"}, "T")
    assert r.reason == REASON_NOT_CONVERGED
    assert r.minimum is None


def test_share_bounds_reads_every_v04_shape():
    assert share_bounds({"exact": 67}) == (0.67, 0.67)
    assert share_bounds({"exclusiveMinimum": 75, "maximum": 100}) == (0.75, 1.0)
    assert share_bounds({"minimum": 25, "exclusiveMaximum": 50}) == (0.25, 0.5)
    assert share_bounds({"minimum": 10}) == (0.1, None)
    assert share_bounds({}) is None
    assert share_bounds(None) is None


@pytest.mark.parametrize("fig,text", [
    ({"minimum": 67, "maximum": 67}, "67%"),
    ({"minimum": 50.1, "maximum": 67}, "50.1–67%"),
    ({"minimum": 24}, "at least 24%"),
    ({"minimum": 0, "maximum": 10}, "up to 10%"),
    ({"minimum": 0}, None),
    (None, None),
])
def test_figure_text(fig, text):
    assert figure_text(fig) == text
