"""The Israeli change log (the ica-changes ticket, 11 Nov 2026) — the index's
``change`` table, the carry-forward past the register's one-year window, the
reader, the History emitter and its wiring.

Every index is built by the **real builder** from records in the datastore's
exact published shape (Hebrew field names, ``~`` for the Hebrew quotation
mark, day-first dates, numbers as numbers, a charge id only on charge rows)
and read back through the **real adapter**.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck.config import get_settings
from opencheck.sources import ica_israel
from opencheck.sources.ica_israel_changes import (
    GROUPS,
    MATERIAL_CODES,
    MATERIAL_GROUPS,
    REQUEST_LABELS,
    group_of,
    is_material,
    label_for,
)
from opencheck.timeline import service
from opencheck.timeline.ica_israel import SHARES_NOTE, ica_israel_change_events
from opencheck.timeline.model import CHANGE_TYPES, ChangeType, RecordType, Tier

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_ica_israel_index.py"
_FIXTURES = Path(__file__).parent / "fixtures" / "ica_israel"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_ica_israel_index_changes", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

#: The 97 request codes and their Hebrew words, as the file carried them on
#: 11 Nov 2026.
REQUEST_WORDS: dict[int, str] = {
    int(k): v for k, v in json.loads((_FIXTURES / "request_types.json").read_text("utf-8")).items()
}

CUT = "2026-11-11"
NUMBER = "516478872"
OTHER = "513000001"


def _change(number: str, code: int, day: str, charge: int | None = None, **over: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "_id": 1,
        "מספר תאגיד": int(number),
        "שם תאגיד": "פייפאל ישראל שירותי תשלום  בע~מ",
        "סוג בקשה": REQUEST_WORDS.get(code, ""),
        "תאריך עדכון סטטוס": day,
        "מזהה השיעבוד": charge,
        "קוד סוג בקשה": code,
    }
    rec.update(over)
    return rec


def _changes() -> list[dict[str, Any]]:
    return [
        _change(NUMBER, 22, "18/08/2026"),            # annual report — routine
        _change(NUMBER, 22032, "18/08/2026"),         # AR: director added
        _change(NUMBER, 4031, "02/03/2026"),          # director removed
        _change(NUMBER, 3, "09/11/2026"),             # share transfer
        _change(NUMBER, 24, "20/01/2026"),            # change of name
        _change(NUMBER, 146, "01/10/2026"),           # fast-track voluntary winding-up
        _change(NUMBER, 55, "15/12/2025"),            # voluntary merger, absorbing
        _change(NUMBER, 68, "21/07/2026", charge=58),  # charge registered
        _change(NUMBER, 70, "29/10/2026", charge=58),  # charge removed
        _change(OTHER, 1000, "12/11/2025"),           # violation update, the window's first day
    ]


def _build(
    tmp_path: Path,
    changes: list[dict[str, Any]] | None = None,
    *,
    name: str = "ica_israel.sqlite",
    cut: str = CUT,
    previous: Path | None = None,
) -> tuple[Path, dict[str, int]]:
    out = tmp_path / "data" / name
    out.parent.mkdir(exist_ok=True)
    stats = builder.build(
        {"company": [], "partnership": [], "change": _changes() if changes is None else changes},
        out, cut, min_rows={}, changes_cut=cut, previous=previous,
    )
    return out, stats


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("ICA_ISRAEL_DB_FILE", None)
    else:
        os.environ["ICA_ISRAEL_DB_FILE"] = str(path)
    get_settings.cache_clear()
    ica_israel.reset_connection()


@pytest.fixture()
def index(tmp_path: Path):
    out, stats = _build(tmp_path)
    _point_at(out)
    yield out, stats
    _point_at(None)


# --- the request-code table --------------------------------------------------


def test_every_code_the_file_carries_has_an_english_label_and_one_group() -> None:
    assert set(REQUEST_WORDS) <= set(REQUEST_LABELS)
    seen: list[int] = [code for codes in GROUPS.values() for code in codes]
    assert len(seen) == len(set(seen)), "a code sits in two groups"
    assert set(seen) == set(REQUEST_LABELS)


def test_the_material_set_is_stephens() -> None:
    # 11 Nov 2026: the recommended set; charges are not material.
    assert set(MATERIAL_GROUPS) == {
        "shares", "capital", "directors", "name", "status", "merger", "attachment", "classification",
    }
    for code in (68, 69, 70, 71, 72, 22, 1000, 1001, 275, 25, 43, 22171, 64, 120):
        assert not is_material(code), code
    for code in (3, 2, 22124, 21, 4031, 4032, 24, 146, 152, 45, 55, 74, 39):
        assert is_material(code), code
    assert MATERIAL_CODES == frozenset().union(*(GROUPS[g] for g in MATERIAL_GROUPS))


def test_a_label_carries_the_register_words_and_an_unknown_code_reads_as_them() -> None:
    assert label_for(3, "העברת מניות") == "Share transfer (העברת מניות)"
    assert label_for(99999, "בקשה חדשה") == "בקשה חדשה"
    assert label_for(99999, None) == "Request type 99999"
    assert group_of(99999) == "other"
    assert not is_material(99999)


# --- the builder ----------------------------------------------------------------


def test_the_change_table_holds_every_row_keyed_on_number_code_day_charge(index) -> None:
    out, stats = index
    assert stats["change_rows"] == 10
    con = sqlite3.connect(out)
    assert con.execute("SELECT COUNT(*) FROM change").fetchone()[0] == 10
    # The Hebrew words are a vocabulary, with ``~`` back to ״'s ASCII stand-in.
    assert con.execute("SELECT words FROM request_type WHERE code = 22").fetchone()[0] == 'דו"ח שנתי'
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert meta["changes_cut"] == CUT
    assert meta["changes_window_start"] == "2025-11-12"
    assert meta["changes_kept_since"] == "2025-11-12"
    assert meta["changes_first_cut"] == CUT
    assert "CC BY" in meta["changes_licence"]
    assert "ica-changes" in meta["source"]


def test_an_index_without_a_change_log_still_builds(tmp_path: Path) -> None:
    out = tmp_path / "plain.sqlite"
    builder.build({"company": [], "partnership": []}, out, CUT, min_rows={})
    meta = dict(sqlite3.connect(out).execute("SELECT key, value FROM meta"))
    assert "changes_cut" not in meta


@pytest.mark.parametrize(
    "records,needle",
    [
        ([{**_change(NUMBER, 3, "09/11/2026"), "עמודה חדשה": 1}], "field set changed"),
        ([{k: v for k, v in _change(NUMBER, 3, "09/11/2026").items() if k != "מזהה השיעבוד"}], "field set changed"),
        ([_change(NUMBER, 3, "09/11/2026"), _change(NUMBER, 3, "01/01/2025")], "more than 370 days"),
        ([_change(NUMBER, 3, "20/11/2026")], "after the cut"),
    ],
)
def test_the_builder_refuses_a_change_file_it_does_not_know(tmp_path: Path, records, needle) -> None:
    with pytest.raises(builder.RefusedInput, match=needle):
        _build(tmp_path, records)
    assert not (tmp_path / "data" / "ica_israel.sqlite").exists()


def test_too_few_change_rows_is_not_the_change_log(tmp_path: Path) -> None:
    out = tmp_path / "x.sqlite"
    with pytest.raises(builder.RefusedInput, match="not the change log"):
        builder.build(
            {"company": [], "partnership": [], "change": _changes()}, out, CUT,
            min_rows={"change": 400_000},
        )


def test_malformed_rows_are_counted_and_a_flood_refused(tmp_path: Path) -> None:
    bad = _change(NUMBER, 3, "not a date")
    _, stats = _build(tmp_path, [*_changes(), bad])
    assert stats["change_malformed"] == 1
    with pytest.raises(builder.RefusedInput, match="malformed"):
        _build(tmp_path, [*_changes(), *[bad] * (builder.MAX_MALFORMED + 1)], name="b.sqlite")


def test_rows_that_fall_out_of_the_window_are_carried_forward(tmp_path: Path) -> None:
    first, _ = _build(tmp_path, name="first.sqlite", cut=CUT)
    # The next night the window moves on: the oldest day drops off, a row is
    # added, and a row inside the window is withdrawn by the register.
    later = [
        r for r in _changes()
        if r["תאריך עדכון סטטוס"] != "12/11/2025" and r["קוד סוג בקשה"] != 4031
    ] + [_change(NUMBER, 2, "12/11/2026")]
    second, stats = _build(tmp_path, later, name="second.sqlite", cut="2026-11-12", previous=first)
    assert stats["change_retained"] == 1
    assert stats["change_withdrawn"] == 1
    con = sqlite3.connect(second)
    rows = {
        (n, c, d): (seen, kept)
        for n, c, d, seen, kept in con.execute(
            "SELECT number, code, day, first_seen, retained FROM change"
        )
    }
    assert rows[(OTHER, 1000, "2025-11-12")] == (CUT, 1)          # kept past the window
    assert rows[(NUMBER, 3, "2026-11-09")] == (CUT, 0)            # read again: first seen kept
    assert rows[(NUMBER, 2, "2026-11-12")] == ("2026-11-12", 0)   # new tonight
    assert (NUMBER, 4031, "2026-03-02") not in rows               # withdrawn, not kept
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert meta["changes_window_start"] == "2025-12-15"
    assert meta["changes_kept_since"] == "2025-11-12"
    assert meta["changes_first_cut"] == CUT
    # And the next build carries the retained row again.
    third, stats3 = _build(tmp_path, later, name="third.sqlite", cut="2026-11-13", previous=second)
    assert stats3["change_retained"] == 1
    assert dict(sqlite3.connect(third).execute("SELECT key, value FROM meta"))["changes_first_cut"] == CUT


def test_a_previous_build_without_a_change_log_carries_nothing(tmp_path: Path) -> None:
    old = tmp_path / "old.sqlite"
    builder.build({"company": [], "partnership": []}, old, "2026-11-10", min_rows={})
    _, stats = _build(tmp_path, previous=old)
    assert stats["change_retained"] == 0


# --- the reader ------------------------------------------------------------------


def test_change_rows_reads_one_number_oldest_first(index) -> None:
    rows = ica_israel.change_rows(NUMBER)
    assert rows is not None and len(rows) == 9
    assert [r["day"] for r in rows] == sorted(r["day"] for r in rows)
    charge = [r for r in rows if r["code"] == 68][0]
    assert charge["charge_id"] == 58
    assert charge["words"] == "רישום שעבוד"
    assert [r for r in rows if r["code"] == 3][0]["charge_id"] is None
    assert ica_israel.change_rows("520000002") == []


def test_an_index_built_before_the_change_table_answers_none(tmp_path: Path) -> None:
    out = tmp_path / "data" / "plain.sqlite"
    out.parent.mkdir()
    builder.build({"company": [], "partnership": []}, out, CUT, min_rows={})
    _point_at(out)
    try:
        assert ica_israel.change_rows(NUMBER) is None
        assert ica_israel.change_coverage() is None
    finally:
        _point_at(None)


def test_change_coverage_names_the_window_and_what_opencheck_kept(index) -> None:
    assert ica_israel.change_coverage() == {
        "cut": CUT, "window_start": "2025-11-12", "kept_since": "2025-11-12", "first_cut": CUT,
    }


# --- the History emitter -----------------------------------------------------------


def _events() -> list:
    rows = [
        {"code": r["קוד סוג בקשה"], "words": r["סוג בקשה"].replace("~", '"'),
         "day": builder._iso_day(r["תאריך עדכון סטטוס"]), "charge_id": r["מזהה השיעבוד"]}
        for r in _changes() if r["מספר תאגיד"] == int(NUMBER)
    ]
    return ica_israel_change_events(NUMBER, rows)


def _by_code(events, code: int):
    return [e for e in events if e.raw_field.startswith(f"request code {code}")][0]


def test_status_merger_and_name_filings_are_notable_and_say_filed() -> None:
    events = _events()
    wind = _by_code(events, 146)
    assert wind.change_type is ChangeType.STATUS_CHANGED and wind.tier is Tier.IDENTITY_STATUS
    assert wind.value_new == "Fast-track voluntary winding-up (פירוק מרצון מזורז) filed"
    assert wind.value_old is None
    merger = _by_code(events, 55)
    assert merger.change_type is ChangeType.SUCCESSION
    name = _by_code(events, 24)
    assert name.change_type is ChangeType.LEGAL_NAME_CHANGE
    assert name.value_new == "Change of name (שינוי שם) filed"
    assert all(e.event_date and e.date_basis.value == "effective" for e in events)


def test_a_share_transfer_ranks_with_ownership_and_names_no_party() -> None:
    share = _by_code(_events(), 3)
    assert share.change_type is ChangeType.SHARES_FILED
    assert share.tier is Tier.OWNERSHIP_CONTROL
    assert share.record_type is RecordType.RELATIONSHIP
    assert share.counterparty is None
    assert share.value_new == f"Share transfer (העברת מניות) filed; {SHARES_NOTE}"
    assert share.is_notable
    spec = CHANGE_TYPES[ChangeType.SHARES_FILED]
    assert spec.label == "Shares filed (parties not published)"
    # Never an owner added or removed: the file names nobody.
    assert "owner" not in spec.label.lower()


def test_directors_are_the_board_stream_with_no_person() -> None:
    events = _events()
    added, removed = _by_code(events, 22032), _by_code(events, 4031)
    assert added.change_type is ChangeType.OFFICER_APPOINTED and added.tier is Tier.BOARD_CHANGE
    assert removed.change_type is ChangeType.OFFICER_RESIGNED
    assert added.counterparty is None and removed.counterparty is None


def test_charges_and_the_annual_report_are_administrative() -> None:
    events = _events()
    for code in (22, 68, 70):
        ev = _by_code(events, code)
        assert ev.tier is Tier.ADMIN_NOISE and ev.change_type is None and not ev.is_notable
    assert _by_code(events, 68).raw_field == "request code 68 · charge 58"
    assert _by_code(events, 68).raw_change_type == "Charge registered (רישום שעבוד)"


def test_no_rows_no_events() -> None:
    assert ica_israel_change_events(NUMBER, None) == []
    assert ica_israel_change_events(NUMBER, []) == []


# --- the History service -------------------------------------------------------


@pytest.fixture()
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _israeli_registration(monkeypatch, number: str) -> None:
    async def registration(_client, _lei):
        return (None, None, None, None, None, None, number, "PAYPAL ISRAEL PAYMENT SERVICES LTD")

    async def modifications(_client, _lei):
        return [], []

    monkeypatch.setattr(service, "_gleif_registration", registration)
    monkeypatch.setattr(service, "_gleif_modifications", modifications)


async def test_the_history_service_reads_the_israeli_change_log(index, live, monkeypatch) -> None:
    _israeli_registration(monkeypatch, "51-647887-2")
    timeline = await service.fetch_timeline("5299000ISRAEL0000000")
    il = [e for e in timeline.events if e.source_id == "ica_israel"]
    assert len(il) == 9
    assert timeline.registry_numbers["ica_israel"] == NUMBER
    assert timeline.registers_kept_since == {"ica_israel": "2025-11-12"}
    notable = {e.change_type for e in timeline.notable}
    assert {ChangeType.SHARES_FILED, ChangeType.STATUS_CHANGED, ChangeType.SUCCESSION,
            ChangeType.LEGAL_NAME_CHANGE} <= notable


async def test_registration_numbers_reads_the_israeli_authority() -> None:
    payload = {"data": {"attributes": {"entity": {
        "registeredAs": "516478872", "registeredAt": {"id": "RA000406"},
        "jurisdiction": "IL", "legalName": {"name": "PAYPAL"},
    }}}}
    assert service._registration_numbers(payload)[6] == "516478872"
    assert service._registration_numbers(payload)[7] == "PAYPAL"
