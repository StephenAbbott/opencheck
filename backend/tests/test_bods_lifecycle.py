"""Phase 219 — the rule for "this relationship has ended".

Parallel to ``frontend/src/lib/relationshipStatus.test.ts``: the same cases
against ``opencheck/bods/lifecycle.py``. Change one, change the other.
"""

from __future__ import annotations

from opencheck.bods.lifecycle import (
    Lifecycle,
    ended_phrase,
    interest_ended,
    relationship_lifecycle,
    statement_lifecycle,
)

AS_OF = "2026-09-16"


def test_past_or_same_day_end_date_is_ended():
    assert interest_ended({"endDate": "2024-11-30"}, False, AS_OF)
    assert interest_ended({"endDate": AS_OF}, False, AS_OF)


def test_future_end_date_is_current():
    assert not interest_ended({"endDate": "2027-01-01"}, False, AS_OF)


def test_closed_record_ends_every_interest_dated_or_not():
    assert interest_ended({}, True, AS_OF)
    assert interest_ended({"endDate": "2027-01-01"}, True, AS_OF)


def test_date_time_compares_on_its_date_and_junk_is_ignored():
    assert interest_ended({"endDate": "2024-11-30T00:00:00Z"}, False, AS_OF)
    assert not interest_ended({"endDate": "sometime"}, False, AS_OF)


def test_ended_when_every_interest_ended_dated_with_latest():
    assert relationship_lifecycle(
        [{"endDate": "2019-06-18"}, {"endDate": "2024-11-30"}], False, AS_OF
    ) == Lifecycle(True, "2024-11-30")


def test_current_while_any_interest_is_current():
    assert relationship_lifecycle([{"endDate": "2024-11-30"}, {}], False, AS_OF) == Lifecycle(False)


def test_closed_record_never_gets_an_invented_date():
    assert relationship_lifecycle([{}], True, AS_OF) == Lifecycle(True, None)
    assert relationship_lifecycle([{"endDate": "2027-01-01"}], True, AS_OF) == Lifecycle(True, None)


def test_no_interests_is_ended_only_when_closed():
    assert relationship_lifecycle([], False, AS_OF) == Lifecycle(False)
    assert relationship_lifecycle([], True, AS_OF) == Lifecycle(True, None)


def test_statement_reads_record_status_and_interests():
    stmt = {
        "recordStatus": "closed",
        "recordDetails": {"interests": [{"type": "shareholding", "endDate": "2024-11-30"}]},
    }
    assert statement_lifecycle(stmt, AS_OF) == Lifecycle(True, "2024-11-30")
    assert statement_lifecycle({"recordStatus": "new", "recordDetails": {}}, AS_OF) == Lifecycle(False)


def test_phrase_spells_the_month_and_never_guesses():
    assert ended_phrase("2024-11-30") == "ended 30 November 2024"
    assert ended_phrase("2018-01-05") == "ended 5 January 2018"
    assert ended_phrase(None) == "ended"
    assert ended_phrase("2024-13-01") == "ended"
    assert ended_phrase("garbage") == "ended"
