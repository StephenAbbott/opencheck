"""Phase 289 — GLEIF sometimes serialises a relationship record's ``periods``
as a JSON object keyed by index instead of an array. One such record under
HSBC Holdings (``213800XGI5VTUFBD4932``, 5 Oct 2026) crashed
``assemble_subsidiaries`` for the whole 465-child network."""

from __future__ import annotations

from opencheck.bods.mapper import _gleif_rr_period, _gleif_rr_periods


def _rr(periods):
    return {"attributes": {"relationship": {"periods": periods}}}


# Verbatim shape from the live record, keys deliberately out of order.
OBJECT_PERIODS = {
    "0": {"startDate": "2022-01-01T00:00:00Z", "endDate": "2022-12-31T00:00:00Z",
          "type": "ACCOUNTING_PERIOD"},
    "2": {"startDate": "2014-07-04T00:00:00Z", "type": "RELATIONSHIP_PERIOD"},
    "1": {"type": "DOCUMENT_FILING_PERIOD"},
}


def test_periods_serialised_as_an_object_are_read_as_a_list() -> None:
    periods = _gleif_rr_periods(_rr(OBJECT_PERIODS))
    assert [p["type"] for p in periods] == [
        "ACCOUNTING_PERIOD", "DOCUMENT_FILING_PERIOD", "RELATIONSHIP_PERIOD",
    ]
    assert _gleif_rr_period(_rr(OBJECT_PERIODS)) == ("2014-07-04", None)


def test_array_periods_unchanged_and_junk_dropped() -> None:
    arr = [{"type": "RELATIONSHIP_PERIOD", "startDate": "2020-02-03", "endDate": "2021-01-01"}]
    assert _gleif_rr_period(_rr(arr)) == ("2020-02-03", "2021-01-01")
    junk = ["not-a-period", None, {"type": "ACCOUNTING_PERIOD"}]
    assert _gleif_rr_period(_rr(junk)) == (None, None)
    assert _gleif_rr_period(_rr("garbage")) == (None, None)
    assert _gleif_rr_period(None) == (None, None)
    assert _gleif_rr_period({}) == (None, None)
