"""Phase 338 — STATE_CONTROLLED states one accumulated share per state.

The three defects of the Phase 240 sentence, pinned first (15 Oct 2026):

1. a state party that holds an *intermediate* company printed that company's
   share as if it were the subject's ("Norway, 60%" for 60% × 40% = 24%);
2. two different stakes of one state, in one source, were joined with "or"
   ("30% or 20%") when together they are 50%;
3. a banded share (UK PSC ``exclusiveMinimum``) was dropped.

Then the rules Stephen set on the ticket: the figure only (no "majority"),
"at least" when a share is missing on a path, and each source's figure
listed when sources disagree — never added together.
"""

from __future__ import annotations

from opencheck.risk import (
    INCLUDING_ENDED,
    STATE_CONTROLLED,
    _state_controlled_signals,
    merge_state_controlled,
)

TODAY_PAST = "2020-01-01"


def _entity(sid, name, *, etype="registeredEntity", country=None):
    rd = {"entityType": {"type": etype}, "name": name}
    if country:
        rd["jurisdiction"] = {"name": country, "code": country}
    return {"statementId": sid, "recordId": sid, "recordType": "entity", "recordDetails": rd}


def _rel(sid, subject, ip, share=None, *, end=None, itype="shareholding"):
    interest = {"type": itype}
    if share is not None:
        interest["share"] = share
    if end:
        interest["endDate"] = end
    stmt = {
        "statementId": sid, "recordId": sid, "recordType": "relationship",
        "recordDetails": {
            "isComponent": False, "subject": subject, "interestedParty": ip,
            "interests": [interest],
        },
    }
    if end:
        stmt["recordStatus"] = "closed"
    return stmt


def _one(bods, source="opensanctions"):
    (sig,) = _state_controlled_signals(source, "", bods)
    assert sig.code == STATE_CONTROLLED
    return sig


# --- the three defects --------------------------------------------------------


def test_an_indirect_state_party_shows_the_states_share_in_the_subject():
    bods = [
        _entity("T", "Target Co"),
        _entity("A", "Holding A"),
        _entity("S", "Ministry of Finance", etype="stateBody", country="NO"),
        _rel("r1", "A", "S", {"exact": 60}),
        _rel("r2", "T", "A", {"exact": 40}),
    ]
    sig = _one(bods)
    assert "Norway, 24% — " in sig.summary
    assert "60%" not in sig.summary
    assert "through Holding A" in sig.summary
    (holding,) = sig.evidence["state_holdings"]
    assert holding["shares"] == ["24%"]
    (party,) = sig.evidence["matches"]
    assert "share" not in party          # its own edge is not into the subject
    assert party["via"] == ["A"]


def test_two_stakes_of_one_state_in_one_source_add_up():
    bods = [
        _entity("T", "Target Co"),
        _entity("S1", "Ministry of Finance", etype="stateBody", country="NO"),
        _entity("S2", "State Holding Company", etype="stateBody", country="NO"),
        _rel("r1", "T", "S1", {"exact": 30}),
        _rel("r2", "T", "S2", {"exact": 20}),
    ]
    sig = _one(bods)
    assert "Norway, 50% — " in sig.summary
    assert "30% or 20%" not in sig.summary


def test_a_banded_state_holding_is_read():
    bods = [
        _entity("T", "Target Co"),
        _entity("S", "HM Treasury", etype="stateBody", country="GB"),
        _rel("r1", "T", "S", {"exclusiveMinimum": 75, "maximum": 100}),
    ]
    sig = _one(bods, "companies_house")
    assert "United Kingdom, 75–100% — HM Treasury" in sig.summary


# --- the rules on the ticket --------------------------------------------------


def test_a_missing_share_on_a_path_reads_at_least():
    bods = [
        _entity("T", "Target Co"),
        _entity("A", "Holding A"),
        _entity("S", "Ministry of Finance", etype="stateBody", country="NO"),
        _rel("r1", "T", "S", {"exact": 30}),
        _rel("r2", "A", "S"),                     # no share stated
        _rel("r3", "T", "A", {"exact": 20}),
    ]
    sig = _one(bods)
    assert "Norway, at least 30% — " in sig.summary
    (fig,) = sig.evidence["state_holdings"][0]["accumulated"]
    assert fig["incomplete"] is True and "maximum" not in fig


def test_a_majority_is_stated_as_the_figure_only():
    bods = [
        _entity("T", "Target Co"),
        _entity("S", "Ministry of Finance", etype="stateBody", country="NO"),
        _rel("r1", "T", "S", {"exact": 67}),
    ]
    assert "majority" not in _one(bods).summary.lower()


def test_no_shares_at_all_keeps_the_phase_240_sentence():
    bods = [
        _entity("T", "Rosneft"),
        _entity("S", "Government of Russia", etype="stateBody", country="RU"),
        _rel("r1", "T", "S", itype="otherInfluenceOrControl"),
    ]
    sig = _one(bods, "wikidata")
    assert "Russian Federation — Government of Russia (Wikidata)" in sig.summary
    assert sig.evidence["state_holdings"][0]["shares"] == []


def test_one_stake_named_twice_in_one_source_gives_no_figure_not_134():
    bods = [
        _entity("T", "Equinor"),
        _entity("S1", "Government of Norway", etype="state", country="NO"),
        _entity("S2", "Ministry of Trade", etype="stateBody", country="NO"),
        _rel("r1", "T", "S1", {"exact": 67}),
        _rel("r2", "T", "S2", {"exact": 67}),
    ]
    sig = _one(bods, "wikidata")
    assert "134" not in sig.summary
    (fig,) = sig.evidence["state_holdings"][0]["accumulated"]
    assert fig["reason"] == "shares_exceed_100"


def test_an_ended_holding_is_left_out_of_the_figure():
    bods = [
        _entity("T", "Target Co"),
        _entity("S1", "Ministry of Trade", etype="stateBody", country="NO"),
        _entity("S2", "Ministry of Energy", etype="stateBody", country="NO"),
        _rel("r1", "T", "S1", {"exact": 67}),
        _rel("r2", "T", "S2", {"exact": 67}, end=TODAY_PAST),
    ]
    sig = _one(bods, "wikidata")
    assert "Norway, 67% — " in sig.summary
    assert "formerly Ministry of Energy" in sig.summary


def test_a_state_with_only_ended_holdings_has_no_figure_and_is_qualified():
    bods = [
        _entity("T", "Target Co"),
        _entity("S", "Ministry of Energy", etype="stateBody", country="NO"),
        _rel("r1", "T", "S", {"exact": 67}, end=TODAY_PAST),
    ]
    sig = _one(bods, "wikidata")
    assert INCLUDING_ENDED in sig.summary
    assert sig.evidence["state_holdings"][0]["shares"] == []


# --- across sources -----------------------------------------------------------


def _norway(source, name, sid, pct):
    return [
        _entity("T", "Equinor"),
        _entity(sid, name, etype="stateBody", country="NO"),
        _rel(f"r-{sid}", "T", sid, {"exact": pct}),
    ]


def test_sources_that_agree_read_one_figure_never_a_sum():
    a = _one(_norway("opensanctions", "FINANSDEPARTEMENTET", "os-1", 67)).to_dict()
    b = _one(_norway("wikidata", "Ministry of Trade", "wd-1", 67), "wikidata").to_dict()
    merged = merge_state_controlled(a, b)
    assert "Norway, 67% — " in merged["summary"]
    assert "134" not in merged["summary"]
    (h,) = merged["evidence"]["state_holdings"]
    assert h["shares"] == ["67%"]
    assert {f["source_id"] for f in h["accumulated"]} == {"opensanctions", "wikidata"}


def test_sources_that_disagree_are_each_listed():
    a = _one(_norway("opensanctions", "FINANSDEPARTEMENTET", "os-1", 67)).to_dict()
    b = _one(
        _norway("climatetrace", "Government of Norway", "gem-1", 50.1), "climatetrace"
    ).to_dict()
    merged = merge_state_controlled(a, b)
    assert (
        "Norway, 67% (OpenSanctions) or 50.1% (Global Energy Monitor / Climate TRACE) — "
        in merged["summary"]
    )


def test_merge_is_order_independent():
    a = _one(_norway("opensanctions", "FINANSDEPARTEMENTET", "os-1", 67)).to_dict()
    b = _one(
        _norway("climatetrace", "Government of Norway", "gem-1", 50.1), "climatetrace"
    ).to_dict()
    assert (
        merge_state_controlled(a, b)["evidence"]["state_holdings"]
        == merge_state_controlled(b, a)["evidence"]["state_holdings"]
    )
    assert merge_state_controlled(a, b)["summary"] == merge_state_controlled(b, a)["summary"]
