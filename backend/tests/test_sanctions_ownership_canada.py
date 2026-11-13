"""Canada as the fourth regime of the sanctions 50% rule.

The Canadian sanctions 50% rule ticket (12 Nov 2026). SEMA s.2.1 and JVCFOA
s.2.01: a listed person controls an entity it holds 50% or more of the shares
**or** 50% or more of the voting rights in, directly or indirectly, or whose
board it can change; Global Affairs Canada's 2026 guidance adds listed
persons' positions together. Stephen's decisions: the SEMA/JVCFOA lists only;
votes as a second limb for Canada (never added to shares); board power as
context only; the EU and UK unchanged; one signal with Canada as a fourth
row; ``SIGNAL_RULES`` bumped.

GAC's worked examples 1–3 are run as written; example 4 (a sale to an
associate after listing) is named as not computed.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck import sanctions_ownership as so
from opencheck import watchlist
from opencheck.risk import SANCTIONS_OWNERSHIP_TEST, SANCTIONS_OWNERSHIP_TEST_POSSIBLE

AS_OF = "2026-11-12"
CA = ["ca_dfatd_sema_sanctions"]


def _entity(sid: str) -> dict[str, Any]:
    return {"statementId": sid, "recordType": "entity", "recordDetails": {"name": sid}}


def _rel(owner: str, owned: str, *interests: dict[str, Any],
         source: str = "companies_house") -> dict[str, Any]:
    return {
        "statementId": f"{owner}-{owned}-{source}", "recordType": "relationship",
        "source": {"opencheckSourceId": source, "description": source.title()},
        "recordDetails": {"subject": owned, "interestedParty": owner, "interests": list(interests)},
    }


def _shares(pct: float | None = None, **share: float) -> dict[str, Any]:
    i: dict[str, Any] = {"type": "shareholding"}
    if pct is not None:
        i["share"] = {"exact": pct}
    elif share:
        i["share"] = share
    return i


def _votes(pct: float | None = None, **share: float) -> dict[str, Any]:
    i: dict[str, Any] = {"type": "votingRights"}
    if pct is not None:
        i["share"] = {"exact": pct}
    elif share:
        i["share"] = share
    return i


BOARD = {"type": "appointmentOfBoard"}


def _listed(sid: str, datasets: list[str] = CA, *, ident: bool = True) -> dict[str, Any]:
    ev: dict[str, Any] = {
        "subject_statement_id": sid, "matched_name": sid, "search_name": sid,
        "corroboration": [], "name_match_only": not ident, "datasets": datasets,
    }
    if ident:
        ev["identifier_match"] = True
    return {"code": "RELATED_SANCTIONED", "source_id": "opensanctions", "hit_id": f"NK-{sid}",
            "confidence": "high", "kind": "risk", "evidence": ev}


def _run(rels: list[dict[str, Any]], listed: list[dict[str, Any]], subject: str = "T"):
    nodes = {subject}
    for r in rels:
        nodes |= {r["recordDetails"]["subject"], r["recordDetails"]["interestedParty"]}
    bods = [_entity(n) for n in sorted(nodes)] + rels
    return so.build_signal(listed, bods, [subject], subject="LEI", as_of=AS_OF)


def _results(sig: dict[str, Any] | None) -> dict[str, str]:
    assert sig is not None
    return {r["regime"]: r["result"] for r in sig["evidence"]["regimes"]}


def _ca(sig: dict[str, Any]) -> dict[str, Any]:
    return next(r for r in sig["evidence"]["regimes"] if r["regime"] == "CA")


# --- the regime itself ----------------------------------------------------------


def test_canada_is_the_fourth_regime_fifty_or_more_aggregated():
    ca = next(r for r in so.REGIMES if r.key == "CA")
    assert [r.key for r in so.REGIMES] == ["OFAC", "EU", "UK", "CA"]
    assert (ca.threshold, ca.inclusive, ca.aggregate, ca.votes, ca.board) == (0.5, True, True, True, True)
    assert ca.datasets == {"ca_dfatd_sema_sanctions", "ca_sema_sanctions_news"}


def test_the_eu_and_uk_read_shares_only():
    for r in so.REGIMES:
        if r.key != "CA":
            assert not r.votes and not r.board


@pytest.mark.parametrize("dataset", ["ca_dfatd_sema_sanctions", "ca_sema_sanctions_news"])
def test_both_canadian_lists_designate_for_canada(dataset):
    assert so.regimes_for([dataset]) == ["CA"]


@pytest.mark.parametrize("dataset", ["ca_facfoa", "ca_listed_terrorists", "un_sc_sanctions"])
def test_other_canadian_and_un_instruments_do_not_count(dataset):
    assert so.regimes_for([dataset]) == []
    assert _run([_rel("P", "T", _shares(60))], [_listed("P", [dataset])]) is None


# --- aggregation and threshold --------------------------------------------------


def test_two_canada_only_parties_add_up_and_no_other_regime_has_a_holder():
    sig = _run([_rel("P", "T", _shares(30)), _rel("Q", "T", _shares(20))],
               [_listed("P"), _listed("Q")])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST
    assert _results(sig) == {
        "OFAC": so.RESULT_NONE, "EU": so.RESULT_NONE, "UK": so.RESULT_NONE, "CA": so.RESULT_MEETS,
    }
    assert _ca(sig)["sources"][0]["figure"]["text"] == "50%"
    assert "Canada" in sig["summary"]


def test_exactly_fifty_meets_canada():
    sig = _run([_rel("P", "T", _shares(50))], [_listed("P")])
    assert _results(sig)["CA"] == so.RESULT_MEETS


def test_below_fifty_does_not_fire():
    assert _run([_rel("P", "T", _shares(49))], [_listed("P")]) is None


# --- the votes limb -------------------------------------------------------------


def test_fifty_percent_of_the_votes_meets_canada_on_the_votes_limb_only():
    both = ["ca_dfatd_sema_sanctions", "us_ofac_sdn", "eu_fsf", "gb_hmt_sanctions"]
    sig = _run([_rel("P", "T", _shares(10), _votes(50))], [_listed("P", both)])
    res = _results(sig)
    assert res["CA"] == so.RESULT_MEETS
    assert {res["OFAC"], res["EU"], res["UK"]} == {so.RESULT_NOT_MET}
    row = _ca(sig)["sources"][0]
    assert row["limb"] == so.LIMB_VOTES
    assert row["figure"]["text"] == "50% of the voting rights"
    assert row["holders"][0]["stake"] == "50% of the voting rights"
    assert "50% of the voting rights held by designated parties" in sig["summary"]


def test_shares_and_votes_are_never_added_together():
    sig = _run([_rel("P", "T", _shares(30)), _rel("Q", "T", _votes(30))],
               [_listed("P"), _listed("Q")])
    assert sig is None


def test_votes_add_up_across_listed_persons():
    sig = _run([_rel("P", "T", _votes(30)), _rel("Q", "T", _votes(25))],
               [_listed("P"), _listed("Q")])
    assert _results(sig)["CA"] == so.RESULT_MEETS


def test_a_company_caught_on_votes_passes_its_shares_down_in_full():
    """P holds 60% of A's votes (10% of its shares); A holds 50% of T's
    shares. A is deemed P's, so all of A's 50% counts."""
    sig = _run([_rel("P", "A", _shares(10), _votes(60)), _rel("A", "T", _shares(50))],
               [_listed("P")])
    row = _ca(sig)["sources"][0]
    assert row["result"] == so.RESULT_MEETS and "limb" not in row
    holder = row["holders"][0]
    assert holder["designated"] is False and holder["name"] == "A"
    assert holder["held_by"] == [{"name": "P", "stake": "10%"},
                                 {"name": "P", "stake": "60% of the voting rights"}]


def test_a_voting_band_that_reaches_fifty_may_meet():
    sig = _run([_rel("P", "T", _votes(exclusiveMinimum=25, maximum=50))], [_listed("P")])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE
    assert _results(sig)["CA"] == so.RESULT_MAY_MEET


def test_an_ended_voting_interest_does_not_count():
    sig = _run([_rel("P", "T", {**_votes(60), "endDate": "2020-01-01"})], [_listed("P")])
    assert sig is None


# --- the board limb (context only) ----------------------------------------------


def test_board_power_alone_may_meet_and_never_meets():
    sig = _run([_rel("P", "T", BOARD)], [_listed("P")])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE and sig["kind"] == "context"
    row = _ca(sig)["sources"][0]
    assert row["result"] == so.RESULT_MAY_MEET and row["limb"] == so.LIMB_BOARD
    assert row["figure"]["text"] == so.BOARD_TEXT
    assert "board power is read as context only" in sig["summary"]


def test_shares_that_meet_outrank_board_power():
    sig = _run([_rel("P", "T", _shares(60), BOARD)], [_listed("P")])
    row = _ca(sig)["sources"][0]
    assert row["result"] == so.RESULT_MEETS and "limb" not in row


def test_board_power_is_not_read_for_ofac():
    assert _run([_rel("P", "T", BOARD)], [_listed("P", ["us_ofac_sdn"])]) is None


# --- Global Affairs Canada's worked examples ------------------------------------


def test_gac_example_1_fifty_percent_owned_by_one_listed_entity():
    sig = _run([_rel("E", "D", _shares(50))], [_listed("E")], subject="D")
    assert _results(sig)["CA"] == so.RESULT_MEETS


def test_gac_example_2_fifty_percent_owned_by_two_listed_persons_together():
    sig = _run([_rel("G", "F", _shares(25)), _rel("H", "F", _shares(25))],
               [_listed("G"), _listed("H")], subject="F")
    assert _results(sig)["CA"] == so.RESULT_MEETS


def test_gac_example_3_board_majority_by_agreement_is_context():
    """J (listed) holds 40% and can appoint the board; L (not listed) 60%.
    Read from records, board power makes it "may meet", never "meets"."""
    sig = _run([_rel("J", "K", _shares(40), BOARD), _rel("L", "K", _shares(60))],
               [_listed("J")], subject="K")
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE
    assert _results(sig)["CA"] == so.RESULT_MAY_MEET


@pytest.mark.parametrize("phrase", [
    "direction limb", "family members or associates after listing",
    "Global Affairs Canada", "FACFOA", "foreign state",
])
def test_the_not_computed_note_names_canadas_gaps(phrase):
    """GAC example 4 (a sale to an associate after listing) and limb (c)."""
    assert phrase in so.NOT_COMPUTED


def test_the_canadian_sources_are_the_pages_read():
    assert so.CA_SOURCES[0].startswith("https://www.international.gc.ca/")
    assert all(u.startswith("https://laws-lois.justice.gc.ca/") for u in so.CA_SOURCES[1:])


# --- the matches that count, and the rules that are unchanged --------------------


def test_a_name_only_canadian_match_is_never_counted():
    sig = _run([_rel("P", "T", _shares(60))], [_listed("P", ident=False)])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE
    assert _ca(sig)["if_confirmed"] is True


def test_two_sources_are_never_added_for_canada():
    rels = [_rel("P", "T", _shares(30), source="companies_house"),
            _rel("Q", "T", _shares(30), source="opensanctions")]
    assert _run(rels, [_listed("P"), _listed("Q")]) is None


def test_a_party_on_every_list_meets_all_four():
    every = ["us_ofac_sdn", "eu_fsf", "gb_hmt_sanctions", "ca_dfatd_sema_sanctions"]
    sig = _run([_rel("P", "T", _shares(60))], [_listed("P", every)])
    assert set(_results(sig).values()) == {so.RESULT_MEETS}
    assert "OFAC (US), EU, UK and Canada" in sig["summary"]


def test_report_rows_carry_a_canadian_row():
    sig = _run([_rel("P", "T", _votes(55))], [_listed("P")])
    rows = so.report_rows(sig)
    assert [r[0] for r in rows] == ["OFAC (US)", "EU", "UK", "Canada"]
    assert rows[-1][3].startswith("55% of the voting rights")


def test_signal_rules_bumped_for_canada():
    assert watchlist.SIGNAL_RULES >= 7
    assert watchlist.SIGNAL_RULES_CHANGED[7] == {
        "SANCTIONS_OWNERSHIP_TEST", "SANCTIONS_OWNERSHIP_TEST_POSSIBLE",
    }
