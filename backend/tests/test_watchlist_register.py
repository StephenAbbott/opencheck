"""The watchlist's register tier — the Estonian index diff.

The daily refresh diffs each build of the Estonian index against the one it
replaces (``scripts/diff_ariregister_index.py``) and publishes the last
fourteen days as ``ariregister-delta.json``; the watchlist worker reads it
once the server holds the new index and re-runs a watched Estonian company
whose people lists, name, status or legal form changed (Stephen, 16 Oct
2026). Pinned here: what the diff counts and what it does not (an address, a
founder, a re-numbered row), the rolling file, the worker's watermark, gaps
and waiting, the entry that is written even when the re-run finds nothing
else, and the words on both sides.
"""

from __future__ import annotations

import importlib.util
import json
import re
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck import watchlist as wl
from opencheck.routers.watch import _entry_content, _entry_title
from tests.test_ariregister_index import CUT, EESTI, HOLD, _build, _point_at
from tests.test_watchlist import EASY, _watch, client, env  # noqa: F401 — fixtures

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "backend" / "scripts" / "diff_ariregister_index.py"


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location("diff_ariregister_index", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["diff_ariregister_index"] = module
    spec.loader.exec_module(module)
    return module


differ = _load_script()

NEXT_CUT = "2026-10-13"


def _pair(tmp_path: Path, edit: str | list[str]) -> tuple[Path, Path]:
    """Yesterday's build and today's: a copy with ``edit`` applied and the
    cut moved on a day."""
    old, _meta = _build(tmp_path / "ee")
    new = tmp_path / "ee" / "today.sqlite"
    shutil.copy(old, new)
    conn = sqlite3.connect(new)
    for sql in [edit] if isinstance(edit, str) else edit:
        conn.execute(sql)
    conn.execute("UPDATE meta SET value = ? WHERE key = 'cut'", (NEXT_CUT,))
    conn.commit()
    conn.close()
    return old, new


# ---- the diff ----------------------------------------------------------------


def test_an_identical_build_is_an_empty_day(tmp_path: Path) -> None:
    old, new = _pair(tmp_path, [])
    day = differ.diff_indexes(old, new)
    assert day["cut"] == NEXT_CUT and day["previous_cut"] == CUT
    assert day["companies_changed"] == 0 and day["changes"] == {} and day["summary"] == {}


def test_a_shareholder_leaving_is_counted_not_named(tmp_path: Path) -> None:
    old, new = _pair(tmp_path, f"DELETE FROM shareholder WHERE rowid = (SELECT MIN(rowid) FROM shareholder WHERE code = '{HOLD}')")
    before = sqlite3.connect(old).execute("SELECT COUNT(*) FROM shareholder WHERE code = ?", (HOLD,)).fetchone()[0]
    day = differ.diff_indexes(old, new)
    assert day["changes"] == {HOLD: {"shareholders": [before, before - 1]}}
    assert "eesnimi" not in json.dumps(day) and "nimi_arinimi" not in json.dumps(day)


@pytest.mark.parametrize(
    "edit, expected",
    [
        (f"UPDATE entity SET status_code = 'L' WHERE code = '{EESTI}'", {EESTI: {"status": ["R", "L"]}}),
        (f"UPDATE entity SET name = 'Eesti Energia AS (renamed)' WHERE code = '{EESTI}'",
         {EESTI: {"name": ["Eesti Energia AS", "Eesti Energia AS (renamed)"]}}),
        (f"DELETE FROM entity WHERE code = '{EESTI}'", {EESTI: {"removed": True}}),
    ],
)
def test_name_status_and_removal_are_changes(tmp_path: Path, edit: str, expected: dict) -> None:
    old, new = _pair(tmp_path, edit)
    assert differ.diff_indexes(old, new)["changes"] == expected


@pytest.mark.parametrize(
    "edit",
    [
        f"UPDATE entity SET address = 'Somewhere else 1' WHERE code = '{EESTI}'",
        f"UPDATE entity SET vat_number = 'EE000' WHERE code = '{EESTI}'",
        # A row re-registered with the same content: a new kirje_id only.
        f"UPDATE officer SET kirje_id = kirje_id + 1000 WHERE code = '{EESTI}'",
        # Founders are not read.
        "UPDATE off_card_person SET nimi_arinimi = 'Someone else' WHERE isiku_roll IN ('A', 'B')",
        f"DELETE FROM card_entry WHERE code = '{EESTI}'",
        f"UPDATE general SET capital = '1.00' WHERE code = '{EESTI}'",
    ],
)
def test_what_is_not_a_change(tmp_path: Path, edit: str) -> None:
    old, new = _pair(tmp_path, edit)
    assert differ.diff_indexes(old, new)["changes"] == {}


def test_supervisory_board_and_as_shareholders_come_from_off_the_card(tmp_path: Path) -> None:
    conn = sqlite3.connect(_build(tmp_path / "probe")[0])
    roles = dict(conn.execute("SELECT isiku_roll, MIN(code) FROM off_card_person GROUP BY isiku_roll"))
    conn.close()
    assert "N" in roles or "E" in roles, "the fixture holds a supervisory board member"
    role = "N" if "N" in roles else "E"
    old, new = _pair(tmp_path, f"DELETE FROM off_card_person WHERE isiku_roll = '{role}' AND code = '{roles[role]}'")
    change = differ.diff_indexes(old, new)["changes"][roles[role]]
    assert set(change) == {"supervisory_board"} and change["supervisory_board"][1] < change["supervisory_board"][0]


def test_a_day_touching_too_many_companies_is_kept_without_codes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(differ, "SUSPECT_SHARE", 0.0)
    monkeypatch.setattr(differ, "SUSPECT_MIN", 0)
    old, new = _pair(tmp_path, f"UPDATE entity SET status_code = 'L' WHERE code = '{EESTI}'")
    day = differ.diff_indexes(old, new)
    assert day["suspect"] is True and "changes" not in day and day["summary"] == {"status": 1}


def test_the_rolling_file_keeps_fourteen_days_and_survives_a_same_day_rerun() -> None:
    history = None
    for n in range(1, 17):
        history = differ.merge_history(history, {"cut": f"2026-10-{n:02d}", "previous_cut": f"2026-09-{n:02d}", "changes": {}})
    assert [d["cut"] for d in history["days"]] == [f"2026-10-{n:02d}" for n in range(3, 17)]
    # The workflow re-run on the same cut diffs a build against itself: the
    # day it already holds is kept, not replaced by an empty one.
    held = {"cut": "2026-10-16", "previous_cut": "2026-10-15", "changes": {EESTI: {"status": ["R", "L"]}}}
    history = differ.merge_history(history, held)
    history = differ.merge_history(history, {"cut": "2026-10-16", "previous_cut": "2026-10-16", "changes": {}})
    assert history["days"][-1] == held
    assert history["schema"] == differ.DELTA_SCHEMA == 1


def test_the_command_line_writes_the_file(tmp_path: Path) -> None:
    old, new = _pair(tmp_path, f"UPDATE entity SET status_code = 'L' WHERE code = '{EESTI}'")
    out = tmp_path / "delta.json"
    assert differ.main(["--old", str(old), "--new", str(new), "--history", str(tmp_path / "none.json"), "--out", str(out)]) == 0
    doc = json.loads(out.read_text())
    assert doc["days"][0]["changes"] == {EESTI: {"status": ["R", "L"]}}
    # No previous build: the file is written, with no day for this cut.
    assert differ.main(["--old", str(tmp_path / "absent.sqlite"), "--new", str(new), "--history", str(out), "--out", str(out)]) == 0
    assert len(json.loads(out.read_text())["days"]) == 1


def test_the_script_and_the_worker_name_the_same_lists() -> None:
    assert differ.PEOPLE_LISTS == wl.ARIREGISTER_LISTS
    assert tuple(differ.ENTITY_FIELDS) == wl.ARIREGISTER_FIELDS
    assert set(differ.CHANGE_KEYS) == {*wl.ARIREGISTER_LISTS, *wl.ARIREGISTER_FIELDS, *wl.ARIREGISTER_FLAGS}


# ---- the worker --------------------------------------------------------------


def test_a_watchs_code_comes_from_gleif_then_the_baseline() -> None:
    assert wl.watch_register_code({"gleif_facts": {"registered_at": "RA000181", "registered_as": "10421629"}}) == EESTI
    assert wl.watch_register_code({"gleif_facts": {"registered_at": "RA000585", "registered_as": "10421629"}}) is None
    assert wl.watch_register_code({"snapshot": {"identifiers": {"EE-ARIREGISTER": "10421629"}}}) == EESTI
    assert wl.watch_register_code({"gleif_facts": {"registered_at": "RA000181", "registered_as": "x"}}) is None


def test_several_days_merge_first_to_last() -> None:
    merged = wl.merge_register_changes([
        ("2026-10-13", {"shareholders": [3, 4], "status": ["R", "L"]}),
        ("2026-10-14", {"shareholders": [4, 5], "officers": [2, 2]}),
        ("2026-10-15", {"status": ["L", "R"], "future_key": [1, 2]}),
    ])
    # The status went round and came back; an unknown key is never guessed at.
    assert merged == {"shareholders": [3, 5], "officers": [2, 2]}


@pytest.fixture
def estonia(env: Path, tmp_path: Path, client: TestClient) -> dict:  # noqa: F811
    """An index on disk (cut CUT), EASY watched as the Estonian company
    EESTI, and the tier's watermark one day back."""
    out, _meta = _build(tmp_path / "ee")
    _point_at(out)
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    with store._conn() as conn:
        conn.execute(
            "UPDATE watches SET gleif_facts_json = ? WHERE lei = ?",
            (json.dumps({"registered_at": "RA000181", "registered_as": EESTI}), EASY),
        )
    store.set_meta(wl.ARIREGISTER_CUT_KEY, "2026-10-11")
    yield {"store": store, "token": token, "index": out}
    _point_at(None)


def _delta(*days: dict, updated_at: str = "2026-10-12T11:00:00Z") -> dict:
    return {"schema": 1, "updated_at": updated_at, "days": list(days)}


URL = "https://github.com/StephenAbbott/opencheck/releases/download/ariregister-index/ariregister-delta.json"


def test_a_changed_watched_company_is_queued_with_its_counts(estonia: dict, httpx_mock) -> None:  # noqa: ANN001
    httpx_mock.add_response(url=URL, json=_delta(
        {"cut": CUT, "previous_cut": "2026-10-11", "changes": {EESTI: {"beneficial_owners": [11, 12]}, "11111111": {"status": ["R", "L"]}}},
    ))
    assert wl.ariregister_tick() == {"days": 1, "queued": 1}
    store = estonia["store"]
    assert store.get_meta(wl.ARIREGISTER_CUT_KEY) == CUT
    (item,) = store.take_pending(10)
    assert item["lei"] == EASY and item["tier"] == wl.TIER_REGISTER
    assert item["trigger"] == {
        "tier": "register", "register": "ariregister", "code": EESTI, "cuts": [CUT],
        "changes": {"beneficial_owners": [11, 12]},
    }
    # Read once: the next tick downloads nothing.
    assert wl.ariregister_tick() == {"days": 0, "queued": 0}
    assert len(httpx_mock.get_requests()) == 1


def test_a_day_with_no_change_runs_nothing(estonia: dict, httpx_mock) -> None:  # noqa: ANN001
    httpx_mock.add_response(url=URL, json=_delta({"cut": CUT, "previous_cut": "2026-10-11", "changes": {}}))
    assert wl.ariregister_tick() == {"days": 1, "queued": 0}
    assert estonia["store"].pending_count() == 0
    assert estonia["store"].get_meta(wl.ARIREGISTER_CUT_KEY) == CUT


def test_the_first_run_marks_the_day_without_reading(estonia: dict, httpx_mock) -> None:  # noqa: ANN001
    with estonia["store"]._conn() as conn:
        conn.execute("DELETE FROM meta WHERE key = ?", (wl.ARIREGISTER_CUT_KEY,))
    assert wl.ariregister_tick() == {"days": 0, "queued": 0, "started": CUT}
    assert httpx_mock.get_requests() == []


def test_nothing_estonian_watched_downloads_nothing(env: Path, tmp_path: Path, httpx_mock) -> None:  # noqa: ANN001
    out, _meta = _build(tmp_path / "ee")
    _point_at(out)
    try:
        store = wl.get_store()
        assert store is not None
        store.set_meta(wl.ARIREGISTER_CUT_KEY, "2026-10-11")
        assert wl.ariregister_tick() == {"days": 0, "queued": 0}
        assert httpx_mock.get_requests() == []
        assert store.get_meta(wl.ARIREGISTER_CUT_KEY) == CUT
    finally:
        _point_at(None)


def test_a_delta_older_than_the_index_is_waited_for(estonia: dict, httpx_mock) -> None:  # noqa: ANN001
    httpx_mock.add_response(url=URL, json=_delta(updated_at="2000-01-01T00:00:00Z"))
    assert wl.ariregister_tick() == {"days": 0, "queued": 0, "waiting": CUT}
    assert estonia["store"].get_meta(wl.ARIREGISTER_CUT_KEY) == "2026-10-11"
    assert wl.state()["ee_waiting"] is True


def test_a_day_the_delta_will_never_name_is_a_gap(estonia: dict, httpx_mock) -> None:  # noqa: ANN001
    # Written after the index was built, and still without the day: the
    # build could not be diffed. Every watched Estonian company is re-checked.
    httpx_mock.add_response(url=URL, json=_delta(updated_at="2999-01-01T00:00:00Z"))
    out = wl.ariregister_tick()
    assert out["gaps"] == 1 and out["catch_up_queued"] == 1
    (item,) = estonia["store"].take_pending(10)
    assert item["tier"] == wl.TIER_CATCHUP and item["trigger"]["reason"] == "register_gap"
    assert estonia["store"].get_meta(wl.ARIREGISTER_CUT_KEY) == CUT
    assert wl.ariregister_gaps(estonia["store"])[-1]["detail"] == "not diffed"


@pytest.mark.parametrize(
    "day, detail",
    [
        ({"cut": CUT, "previous_cut": "2026-10-10", "changes": {}}, "the build before it was not diffed"),
        ({"cut": CUT, "previous_cut": "2026-10-11", "suspect": True, "summary": {"officers": 99999}}, "too many changes to list"),
    ],
)
def test_a_broken_chain_or_a_suspect_day_is_a_gap(estonia: dict, httpx_mock, day: dict, detail: str) -> None:  # noqa: ANN001
    httpx_mock.add_response(url=URL, json=_delta(day))
    out = wl.ariregister_tick()
    assert out["gaps"] == 1
    assert wl.ariregister_gaps(estonia["store"])[-1]["detail"] == detail


def test_a_failed_download_leaves_the_watermark(estonia: dict, httpx_mock) -> None:  # noqa: ANN001
    httpx_mock.add_response(url=URL, status_code=503)
    assert wl.ariregister_tick() == {"days": 0, "queued": 0}
    assert estonia["store"].get_meta(wl.ARIREGISTER_CUT_KEY) == "2026-10-11"
    assert "503" in (wl.state()["ee_last_error"] or "")


async def test_a_register_rerun_writes_an_entry_even_when_nothing_else_moved(estonia: dict) -> None:
    trigger = {"tier": "register", "register": "ariregister", "code": EESTI, "cuts": [CUT],
               "changes": {"shareholders": [1, 2], "status": ["R", "L"]}}
    # Back to the mirror's own facts, so the re-run finds nothing of GLEIF's.
    facts, watermark = wl._mirror_facts(EASY)
    with estonia["store"]._conn() as conn:
        conn.execute("UPDATE watches SET gleif_facts_json = ? WHERE lei = ?", (json.dumps(facts), EASY))
    result = await wl.rerun(EASY, wl.TIER_REGISTER, trigger)
    assert result["entries"] == 1 and result["changes"] == []
    th = wl.token_hash(estonia["token"])
    (entry,) = estonia["store"].entries(th)
    assert entry["tier"] == "register" and entry["trigger"] == trigger
    # A shareholder-list change on the register's own trigger grades the
    # entry "Recheck the source" even though the re-run's snapshot moved
    # nothing (pKYC stage A: the trigger is graded too).
    assert entry["materiality"] == "recheck" and entry["materiality_rules"] == 1
    assert _entry_title(entry) == "Recheck the source — EASY POWER: Estonian register record changed"
    content = _entry_content(entry)
    assert content.splitlines()[:3] == [
        f"The Estonian e-Business Register recorded a change to this company in its open data of {CUT} (registry code {EESTI}).",
        " - Shareholders: 1 → 2 entries.",
        " - Status: Entered in the register → In liquidation.",
    ]
    assert "A full re-run found nothing else that differs from the last check." in content


def test_the_feed_words_every_register_change() -> None:
    lines = wl.register_change_lines({
        "removed": True, "officers": [2, 2], "beneficial_owners": [0, 1], "supervisory_board": [3, 0],
        "name": ["A OÜ", "B OÜ"], "legal_form": ["OÜ", "AS"], "status": ["R", "Q"],
    })
    assert lines == [
        "The company is no longer in the register's open data — deleted companies are not published.",
        "Persons on the registry card: changed (2 entries before and after).",
        "Beneficial owners: none → 1 entry.",
        "Supervisory board: 3 → none entries.",
        "Name: A OÜ → B OÜ.",
        "Status: Entered in the register → Q.",
        "Legal form: OÜ → AS.",
    ]


def test_the_page_carries_the_same_words() -> None:
    from opencheck.bods.mappers.estonia import EE_STATUS_LABELS

    text = (REPO / "frontend/src/lib/watchlist.ts").read_text()

    def table(name: str) -> dict[str, str]:
        body = text[text.index(f"const {name}") :]
        body = body[body.index("{") + 1 : body.index("};")]
        return dict(re.findall(r'(\w+):\s*"([^"]*)"', body))

    assert table("REGISTER_CHANGE_WORDS") == wl.REGISTER_CHANGE_WORDS
    assert table("EE_STATUS_WORDS") == EE_STATUS_LABELS


def test_the_tier_line_says_what_estonia_holds(estonia: dict, client: TestClient) -> None:  # noqa: F811
    r = client.get(f"/watch/{estonia['token']}")
    assert r.status_code == 200
    reg = r.json()["tiers"]["register"]
    assert reg["available"] is True and reg["index_cut"] == CUT and reg["gaps"] == []


async def test_the_index_refresh_loop_calls_the_asset_check(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    from opencheck.sources import ariregister

    calls: list[int] = []

    def _warm() -> dict:
        calls.append(1)
        if len(calls) >= 2:
            raise asyncio.CancelledError
        return {"ariregister": "already present: x"}

    monkeypatch.setattr(ariregister, "warm_index", _warm)
    with pytest.raises(asyncio.CancelledError):
        await ariregister.refresh_loop(0.0)
    assert len(calls) == 2
