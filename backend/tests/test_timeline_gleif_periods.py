"""The History tab's GLEIF rows, read from real field-modification logs
(the History-tab GLEIF ticket, 23 Nov 2026).

The weekly production check found three things on EIDE SEAFOOD AS and PEPSICO
DE ARGENTINA SRL: a current consolidation dated as ended (the start of the
RELATIONSHIP_PERIOD paired with the end of an ACCOUNTING_PERIOD), the raw
codelist value IS_ULTIMATELY_CONSOLIDATED_BY under "Owner / parent added",
and a transliterated address's ``@type`` shown as an address change. The
fixtures are GLEIF's logs for both LEIs as served on 23 Nov 2026.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from opencheck.timeline.assemble import assemble_timeline
from opencheck.timeline.gleif import (
    classify_gleif_modification,
    relationship_interest_dates,
    relationship_periods,
)
from opencheck.timeline.model import ChangeType, Tier

_FIXTURES = Path(__file__).parent / "fixtures" / "gleif_history"
_EIDE = "1595T7RFSMR9TFCTJX53"
_PEPSICO_AR = "984500D899C841ADC669"
_PERIOD = (
    "/rr:RelationshipData/rr:RelationshipRecords/rr:RelationshipRecord/"
    "rr:Relationship/rr:RelationshipPeriods/rr:RelationshipPeriod/"
)


def _log(name: str) -> tuple[list[dict], list[dict]]:
    data = json.loads((_FIXTURES / name).read_text())["data"]
    attrs = [n["attributes"] for n in data]
    return (
        [a for a in attrs if a.get("recordType") != "RR"],
        [a for a in attrs if a.get("recordType") == "RR"],
    )


def _timeline(name: str, lei: str):
    lei_mods, rr_mods = _log(name)
    return assemble_timeline(lei=lei, gleif_lei_mods=lei_mods, gleif_rr_mods=rr_mods)


def _rr(field: str, mtype: str, old, new, rel="IS_ULTIMATELY_CONSOLIDATED_BY"):
    return {
        "lei": _EIDE, "recordType": "RR", "modificationType": mtype,
        "field": _PERIOD + field, "date": "2025-08-13T00:00:00Z",
        "valueOld": old, "valueNew": new,
        "context": {"relationshipType": rel, "endNode": "636700DMOPCUTPDHD616"},
    }


# --------------------------------------------------------------------------
# Periods — start and end from the RELATIONSHIP_PERIOD only
# --------------------------------------------------------------------------

def test_eide_seafood_current_consolidation_has_no_end():
    tl = _timeline("eide_seafood_1595T7RFSMR9TFCTJX53.json", _EIDE)
    rows = [e for e in tl.notable if e.change_type is ChangeType.OWNER_ADDED]
    assert len(rows) == 1
    row = rows[0]
    assert row.interest_start_date == "2022-12-30"
    # The ACCOUNTING_PERIOD's end (2023-12-31) is not the relationship's.
    assert row.interest_end_date is None
    assert row.counterparty == "636700DMOPCUTPDHD616"


def test_eide_seafood_periods_are_paired_by_type():
    _, rr = _log("eide_seafood_1595T7RFSMR9TFCTJX53.json")
    assert relationship_periods(rr) == [
        {"start": "2022-12-30", "type": "RELATIONSHIP_PERIOD"},
        {"start": "2023-01-01", "end": "2023-12-31", "type": "ACCOUNTING_PERIOD"},
    ]


def test_pepsico_argentina_direct_and_ultimate_are_separate_and_current():
    tl = _timeline("pepsico_argentina_984500D899C841ADC669.json", _PEPSICO_AR)
    rows = [e for e in tl.notable if e.change_type is ChangeType.OWNER_ADDED]
    assert sorted(r.primary.value_new for r in rows) == [
        "Consolidated by its direct parent",
        "Consolidated by its ultimate parent",
    ]
    for r in rows:
        assert r.interest_start_date == "1959-04-07"
        assert r.interest_end_date is None  # not the accounts' 2025-12-31


def test_a_relationship_period_end_is_kept():
    mods = [
        _rr("rr:PeriodType", "INITIAL", None, "ACCOUNTING_PERIOD"),
        _rr("rr:EndDate", "INITIAL", None, "2023-12-31T00:00:00+01:00"),
        _rr("rr:StartDate", "INITIAL", None, "2023-01-01T00:00:00+01:00"),
        _rr("rr:PeriodType", "INITIAL", None, "RELATIONSHIP_PERIOD"),
        _rr("rr:EndDate", "INITIAL", None, "2024-06-30T00:00:00+01:00"),
        _rr("rr:StartDate", "INITIAL", None, "2020-01-01T00:00:00+01:00"),
    ]
    assert relationship_interest_dates(mods) == ("2020-01-01", "2024-06-30")


def test_a_later_update_finds_its_period_by_the_value_it_replaces():
    mods = [
        # newest first: the relationship period gained an end date
        _rr("rr:EndDate", "INSERT", None, "2026-03-31T00:00:00Z"),
        _rr("rr:EndDate", "UPDATE", "2023-12-31T00:00:00Z", "2024-12-31T00:00:00Z"),
        _rr("rr:PeriodType", "INITIAL", None, "ACCOUNTING_PERIOD"),
        _rr("rr:EndDate", "INITIAL", None, "2023-12-31T00:00:00Z"),
        _rr("rr:StartDate", "INITIAL", None, "2023-01-01T00:00:00Z"),
        _rr("rr:PeriodType", "INITIAL", None, "RELATIONSHIP_PERIOD"),
        _rr("rr:StartDate", "INITIAL", None, "2022-12-30T00:00:00Z"),
    ]
    # An end inserted with no PeriodType beside it names no period: no dates
    # rather than a guess.
    assert relationship_interest_dates(mods) == (None, None)
    # Without the unpaired insert, the accounting update lands on its period
    # and the relationship stays open.
    assert relationship_interest_dates(mods[1:]) == ("2022-12-30", None)


@pytest.mark.parametrize(
    "mods",
    [
        # dates with no PeriodType at all (older fixtures' shape)
        [_rr("rr:StartDate", "INITIAL", None, "2021-11-01T00:00:00Z")],
        # an update whose old value two periods hold
        [
            _rr("rr:StartDate", "UPDATE", "2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z"),
            _rr("rr:PeriodType", "INITIAL", None, "RELATIONSHIP_PERIOD"),
            _rr("rr:StartDate", "INITIAL", None, "2025-01-01T00:00:00Z"),
            _rr("rr:PeriodType", "INITIAL", None, "ACCOUNTING_PERIOD"),
            _rr("rr:StartDate", "INITIAL", None, "2025-01-01T00:00:00Z"),
        ],
    ],
)
def test_a_log_that_cannot_be_paired_gives_no_dates(mods):
    assert relationship_interest_dates(mods) == (None, None)


# --------------------------------------------------------------------------
# Words — consolidation, never ownership; no codelist value on screen
# --------------------------------------------------------------------------

def test_relationship_rows_say_consolidated_by_never_owner():
    for name, lei in (
        ("eide_seafood_1595T7RFSMR9TFCTJX53.json", _EIDE),
        ("pepsico_argentina_984500D899C841ADC669.json", _PEPSICO_AR),
    ):
        tl = _timeline(name, lei)
        for e in tl.notable:
            if e.change_type is not ChangeType.OWNER_ADDED:
                continue
            assert e.label == "Consolidating parent recorded"
            assert "owner" not in e.label.lower()
            assert "_" not in (e.primary.value_new or "")
            assert e.primary.value_old is None


def test_relationship_removal_is_worded():
    ev = classify_gleif_modification({
        "lei": _EIDE, "recordType": "RR", "modificationType": "UPDATE",
        "field": "/rr:RelationshipData/rr:RelationshipRecords/rr:RelationshipRecord/"
        "rr:Relationship/rr:RelationshipStatus",
        "date": "2026-01-01T00:00:00Z", "valueOld": "ACTIVE", "valueNew": "INACTIVE",
        "context": {"relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY", "endNode": "X"},
    })
    assert ev.change_type is ChangeType.OWNER_REMOVED
    assert ev.label == "Consolidating parent removed"
    assert ev.value_new == "No longer consolidated by its direct parent"


# --------------------------------------------------------------------------
# Attributes and transliterations are not address changes
# --------------------------------------------------------------------------

def test_eide_seafood_has_no_address_rows():
    tl = _timeline("eide_seafood_1595T7RFSMR9TFCTJX53.json", _EIDE)
    assert not [e for e in tl.notable if e.change_type is ChangeType.ADDRESS_CHANGE]
    shown = " ".join(str(e.primary.value_new) for e in tl.notable)
    assert "PREFERRED_ASCII" not in shown


def test_an_attribute_on_the_legal_name_is_not_a_name_change():
    ev = classify_gleif_modification({
        "lei": _EIDE, "recordType": "LEI", "modificationType": "UPDATE",
        "field": "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/lei:Entity/lei:LegalName/@xml:lang",
        "date": "2026-01-01T00:00:00Z", "valueOld": "no", "valueNew": "nb",
    })
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE


def test_a_real_headquarters_move_is_still_an_address_change():
    ev = classify_gleif_modification({
        "lei": _EIDE, "recordType": "LEI", "modificationType": "UPDATE",
        "field": "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/lei:Entity/"
        "lei:HeadquartersAddress/lei:City",
        "date": "2026-01-01T00:00:00Z", "valueOld": "EIKELANDSOSEN", "valueNew": "BERGEN",
    })
    assert ev.change_type is ChangeType.ADDRESS_CHANGE


# --------------------------------------------------------------------------
# The parent is named from the Golden Copy mirror, never by a GLEIF call
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parent_is_named_from_the_mirror(monkeypatch):
    from types import SimpleNamespace

    from opencheck import entity_pages
    from opencheck.timeline import service

    asked: list[list[str]] = []

    class _Store:
        def get_many(self, leis):
            asked.append(list(leis))
            return {"636700DMOPCUTPDHD616": SimpleNamespace(name="A PARENT AS (STUB NAME)")}

    monkeypatch.setattr(entity_pages, "get_store", lambda: _Store())
    tl = _timeline("eide_seafood_1595T7RFSMR9TFCTJX53.json", _EIDE)
    await service._name_gleif_parents(tl)
    row = next(e for e in tl.notable if e.change_type is ChangeType.OWNER_ADDED)
    assert row.counterparty_name == "A PARENT AS (STUB NAME)"
    assert row.counterparty == "636700DMOPCUTPDHD616"
    assert asked == [["636700DMOPCUTPDHD616"]]


@pytest.mark.asyncio
async def test_no_mirror_leaves_the_lei_alone(monkeypatch):
    from opencheck import entity_pages
    from opencheck.timeline import service

    monkeypatch.setattr(entity_pages, "get_store", lambda: None)
    tl = _timeline("eide_seafood_1595T7RFSMR9TFCTJX53.json", _EIDE)
    await service._name_gleif_parents(tl)
    assert all(e.counterparty_name is None for e in tl.notable)
