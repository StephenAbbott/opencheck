"""Phase 305 — GLEIF Legal Entity Events in the GLEIF entity statement.

Record shapes are the live API's as read on 8 Oct 2026: Lambson Limited
(ACTIVE, liquidation in progress), Westlake Pharmacy Services Ltd (INACTIVE,
dissolved ``2024-10-24T22:00:00Z``, which Companies House dates 25 October 2024),
East Capital Holding AB (ACTIVE, absorption completed).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from opencheck.bods import gleif_events as ge
from opencheck.bods import liveness
from opencheck.bods.annotations import resolve_pointer, validate_annotations
from opencheck.bods.mapper import map_gleif
from opencheck.entity_pages import EntityRow, _event_groups, gleif_record_from_row

REPO = Path(__file__).resolve().parents[2]

#: The same table as the ``eventDay`` cases in frontend/src/lib/watchlist.test.ts
#: (``test_frontend_event_day_cases_are_this_table`` keeps them together).
EVENT_DAY_CASES: list[tuple[str | None, str | None]] = [
    ("2024-10-24T22:00:00Z", "2024-10-25"),
    ("2026-10-05T23:00:00Z", "2026-10-06"),
    ("2026-10-06T00:00:00Z", "2026-10-06"),
    ("2023-06-30T18:30:00Z", "2023-07-01"),
    ("2023-06-30T16:00:00Z", "2023-07-01"),
    ("2024-01-31T10:59:19Z", "2024-01-31"),
    ("2024-01-31T11:59:59Z", "2024-01-31"),
    ("2024-01-31T12:00:00Z", "2024-02-01"),
    ("2024-12-31T23:00:00Z", "2025-01-01"),
    ("2024-02-28T22:00:00Z", "2024-02-29"),
    ("2026-10-05T23:00:00+00:00", "2026-10-06"),
    ("2026-10-05T23:00:00+02:00", "2026-10-05"),
    ("2026-10-06T01:00:00-05:00", "2026-10-06"),
    ("2026-10-06", "2026-10-06"),
    ("2026-13-01T00:00:00Z", None),
    ("not a date", None),
    ("", None),
    (None, None),
]


@pytest.mark.parametrize("value,expected", EVENT_DAY_CASES)
def test_event_day(value, expected):
    assert ge.event_day(value) == expected


def test_frontend_event_day_cases_are_this_table():
    text = (REPO / "frontend/src/lib/watchlist.test.ts").read_text()
    block = text[text.index('describe("eventDay"') :]
    block = block[: block.index("];")]
    rows = re.findall(r'\[(null|"[^"]*"),\s*"([^"]*)"\]', block)
    parsed = [(None if a == "null" else a.strip('"'), b or None) for a, b in rows]
    assert parsed == EVENT_DAY_CASES


def test_frontend_event_words_are_the_backend_tables():
    text = (REPO / "frontend/src/lib/watchlist.ts").read_text()

    def table(name: str) -> dict[str, str]:
        body = text[text.index(f"const {name}") :]
        body = body[body.index("{") + 1 : body.index("};")]
        return dict(re.findall(r'(\w+):\s*"([^"]*)"', body))

    assert table("EVENT_TYPE_WORDS") == ge.EVENT_TYPE_WORDS
    assert table("EVENT_STATUS_WORDS") == ge.EVENT_STATUS_WORDS


def test_watch_and_watchlist_read_the_one_module():
    from opencheck import watchlist
    from opencheck.routers import watch

    assert watch.EVENT_TYPE_WORDS is ge.EVENT_TYPE_WORDS
    assert watch.EVENT_STATUS_WORDS is ge.EVENT_STATUS_WORDS
    assert watchlist.CORPORATE_EVENT_EXCLUDED is ge.EXCLUDED_TYPES


# ---------------------------------------------------------------------------
# Records


def _event(type_, status, effective, recorded=None, docs="SUPPORTING_DOCUMENTS"):
    e = {"validationDocuments": docs, "effectiveDate": effective, "type": type_, "status": status}
    if recorded:
        e["recordedDate"] = recorded
    return e


def _bundle(lei, name, status, events, *, expiration=None, reg_status="ISSUED"):
    entity = {
        "legalName": {"name": name, "language": "en"},
        "jurisdiction": "GB",
        "status": status,
        "expiration": expiration or {"date": None, "reason": None},
        "creationDate": "2004-12-21T00:00:00Z",
        "eventGroups": [{"groupType": "STANDALONE", "events": [e]} for e in events],
    }
    if not events:
        entity.pop("eventGroups")
    return {
        "lei": lei,
        "record": {
            "id": lei,
            "attributes": {
                "lei": lei,
                "entity": entity,
                "registration": {
                    "initialRegistrationDate": "2014-02-17T00:00:00Z",
                    "lastUpdateDate": "2026-10-06T08:12:39Z",
                    "status": reg_status,
                    "nextRenewalDate": "2027-02-18T00:00:00Z",
                },
            },
        },
    }


def _subject(bundle):
    stmts = map_gleif(bundle).statements
    subj = [s for s in stmts if s.get("recordType") == "entity"][0]
    assert validate_annotations(subj) == []
    return subj


def _event_annotations(stmt):
    return [a for a in stmt.get("annotations") or [] if ge.EVENT_PROPERTY in a]


LAMBSON = _bundle(
    "2138002I8THYYYMIJS32",
    "LAMBSON LIMITED",
    "ACTIVE",
    [
        _event(
            "LIQUIDATION", "IN_PROGRESS", "2026-10-06T00:00:00Z", "2026-10-06T08:12:38Z",
            "ACCOUNTS_FILING",
        )
    ],
    reg_status="LAPSED",
)

WESTLAKE = _bundle(
    "15950KH3699OGEG3XT45",
    "WESTLAKE PHARMACY SERVICES LTD",
    "INACTIVE",
    [
        _event("CHANGE_LEGAL_ADDRESS", "COMPLETED", "2019-05-01T22:00:00Z"),
        _event("DISSOLUTION", "COMPLETED", "2024-10-24T22:00:00Z", "2024-11-11T23:00:00Z"),
    ],
)


def test_in_progress_liquidation_is_annotated_never_dated():
    subj = _subject(LAMBSON)
    rd = subj["recordDetails"]
    assert "dissolutionDate" not in rd
    assert liveness.read_register_status(subj)["liveness"] == liveness.LIVE
    [ann] = _event_annotations(subj)
    assert ann["motivation"] == "commenting"
    assert ann["statementPointerTarget"] == "/recordDetails"
    assert ann["description"] == (
        "GLEIF records a legal entity event: liquidation, in progress, effective 6 October 2026 "
        "(GLEIF timestamp 2026-10-06T00:00:00Z; recorded 6 October 2026)."
    )
    assert ann[ge.EVENT_PROPERTY] == {
        "type": "LIQUIDATION",
        "status": "IN_PROGRESS",
        "effectiveDate": "2026-10-06T00:00:00Z",
        "recordedDate": "2026-10-06T08:12:38Z",
        "validationDocuments": "ACCOUNTS_FILING",
        "effectiveDay": "2026-10-06",
        "groupType": "STANDALONE",
    }


def test_completed_dissolution_dates_an_inactive_entity_on_the_local_day():
    subj = _subject(WESTLAKE)
    assert subj["recordDetails"]["dissolutionDate"] == "2024-10-25"  # Companies House's day
    status = liveness.read_register_status(subj)
    assert status["liveness"] == liveness.TERMINAL and status["since"] == "2024-10-25"
    [ann] = _event_annotations(subj)  # the address change is not material
    assert ann["statementPointerTarget"] == "/recordDetails/dissolutionDate"
    assert resolve_pointer(subj, ann["statementPointerTarget"]) == "2024-10-25"
    assert "effective 25 October 2024 (GLEIF timestamp 2024-10-24T22:00:00Z" in ann["description"]


def test_expiration_date_given_by_gleif_wins_over_an_event():
    b = _bundle(
        "529900TESTEXPIRATION01",
        "OLD CO",
        "INACTIVE",
        [_event("DISSOLUTION", "COMPLETED", "2020-03-01T00:00:00Z")],
        expiration={"date": "2019-12-31T00:00:00Z", "reason": "DISSOLVED"},
    )
    subj = _subject(b)
    assert subj["recordDetails"]["dissolutionDate"] == "2019-12-31"
    [ann] = _event_annotations(subj)
    assert ann["statementPointerTarget"] == "/recordDetails"


@pytest.mark.parametrize(
    "type_", ["ABSORPTION", "MERGERS_AND_ACQUISITIONS", "DISSOLUTION"]
)
def test_completed_terminal_event_on_an_active_entity_sets_no_date(type_):
    # East Capital Holding AB: ACTIVE, ISSUED, absorption completed 23 Jun 2026.
    b = _bundle(
        "2138001LFTPUWJ6XDE72",
        "East Capital Holding Aktiebolag",
        "ACTIVE",
        [_event(type_, "COMPLETED", "2026-06-23T00:00:00Z")],
    )
    subj = _subject(b)
    assert "dissolutionDate" not in subj["recordDetails"]
    assert liveness.read_register_status(subj)["liveness"] == liveness.LIVE
    assert len(_event_annotations(subj)) == 1


def test_latest_completed_terminal_event_dates_the_end():
    b = _bundle(
        "2138006CM62KNBKX2I11",
        "TWO EVENTS LTD",
        "INACTIVE",
        [
            _event("DISSOLUTION", "COMPLETED", "2025-09-15T00:00:00Z"),
            _event("LIQUIDATION", "COMPLETED", "2025-08-18T00:00:00Z"),
            _event("BANKRUPTCY", "IN_PROGRESS", "2026-01-01T00:00:00Z"),
            _event("CHANGE_LEGAL_NAME", "COMPLETED", "2027-01-01T00:00:00Z"),
        ],
    )
    subj = _subject(b)
    assert subj["recordDetails"]["dissolutionDate"] == "2025-09-15"
    targets = [a["statementPointerTarget"] for a in _event_annotations(subj)]
    assert targets == [
        "/recordDetails/dissolutionDate",
        "/recordDetails",
        "/recordDetails",
        "/recordDetails/name",
    ]


def test_inactive_with_only_in_progress_or_withdrawn_events_is_undated():
    b = _bundle(
        "529900TESTWITHDRAWN001",
        "PENDING CO",
        "INACTIVE",
        [
            _event("DISSOLUTION", "WITHDRAWN_CANCELLED", "2024-01-01T00:00:00Z"),
            _event("LIQUIDATION", "IN_PROGRESS", "2025-01-01T00:00:00Z"),
        ],
    )
    subj = _subject(b)
    assert "dissolutionDate" not in subj["recordDetails"]
    assert liveness.read_register_status(subj)["liveness"] == liveness.TERMINAL
    descs = [a["description"] for a in _event_annotations(subj)]
    assert descs[0].startswith(
        "GLEIF records a legal entity event: dissolution, withdrawn or cancelled, "
        "effective 1 January 2024"
    )


def test_event_without_effective_date_is_annotated_not_dated():
    b = _bundle(
        "529900TESTNOEFFECTIVE1",
        "UNDATED CO",
        "INACTIVE",
        [{"type": "DISSOLUTION", "status": "COMPLETED", "recordedDate": "2024-02-01T00:00:00Z"}],
    )
    subj = _subject(b)
    assert "dissolutionDate" not in subj["recordDetails"]
    [ann] = _event_annotations(subj)
    assert ann["description"] == (
        "GLEIF records a legal entity event: dissolution, completed (recorded 1 February 2024)."
    )


def test_record_without_events_is_unchanged():
    with_none = _bundle("529900TESTNOEVENTS0001", "PLAIN CO", "ACTIVE", [])
    subj = _subject(with_none)
    assert _event_annotations(subj) == []
    assert "dissolutionDate" not in subj["recordDetails"]
    # The same record with an empty eventGroups list maps identically.
    empty = json.loads(json.dumps(with_none))
    empty["record"]["attributes"]["entity"]["eventGroups"] = []
    assert map_gleif(empty).statements == map_gleif(with_none).statements


def test_mirror_and_live_shapes_map_alike():
    """A mirror row's flat events, rendered by ``gleif_record_from_row``,
    give the same statement as the live API's ``eventGroups``."""
    flat = [
        {
            "type": "DISSOLUTION",
            "status": "COMPLETED",
            "effectiveDate": "2024-10-24T22:00:00Z",
            "recordedDate": "2024-11-11T23:00:00Z",
            "validationDocuments": "SUPPORTING_DOCUMENTS",
            "groupType": "STANDALONE",
        }
    ]
    row = EntityRow(
        lei="15950KH3699OGEG3XT45",
        name="WESTLAKE PHARMACY SERVICES LTD",
        slug="westlake",
        entity_status="INACTIVE",
        registration_status="ISSUED",
        jurisdiction="GB",
        legal_form=None,
        city=None,
        region=None,
        country="GB",
        first_registered=None,
        last_updated=None,
        successor_lei=None,
        direct_parent_lei=None,
        ultimate_parent_lei=None,
        detail={"events": flat},
    )
    record = gleif_record_from_row(row)
    assert record["attributes"]["entity"]["eventGroups"] == _event_groups(flat)
    subj = _subject({"lei": row.lei, "record": record})
    assert subj["recordDetails"]["dissolutionDate"] == "2024-10-25"
    [ann] = _event_annotations(subj)
    assert ann[ge.EVENT_PROPERTY]["effectiveDay"] == "2024-10-25"


def test_exported_statements_validate_under_libcovebods():
    pytest.importorskip("libcovebods")
    from tests.test_bods_libcovebods import validate_bods_statements

    stmts = []
    for b in (LAMBSON, WESTLAKE):
        stmts.extend(map_gleif(b).statements)
    report = validate_bods_statements(stmts)
    assert report["json_errors"] == [], report["json_errors"]
    assert report["additional_errors"] == [], report["additional_errors"]
