"""The watchlist's register tier — the Israel Corporations Authority's index
diff (the ica-changes ticket, 11 Nov 2026).

The daily refresh diffs each build of the Israeli index against the one it
replaces (``scripts/diff_ica_israel_index.py``) and publishes the last
fourteen days as ``ica-israel-delta.json``; the watchlist worker reads it once
the server holds the new index and re-runs a watched Israeli company with a
new filing in a material group of the change log, or whose name, status,
sub-status or violating mark changed (Stephen, 11 Nov 2026: the recommended
material set; charges are not material). Pinned here: what the diff counts
and what it does not, the first build with a change log, the worker, the
entry, its grade and its words on both sides. The tick itself is the
Estonian one (``_register_delta_tick``) — its watermark, waiting and gap
rules are pinned in ``test_watchlist_register.py``.
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
from opencheck import watchlist_materiality as m
from opencheck import watchlist_psc as psc
from opencheck.routers.watch import _entry_content, _entry_title
from tests.test_ica_israel import _company
from tests.test_ica_israel_changes import CUT, NUMBER, _change, _point_at, builder
from tests.test_watchlist import EASY, _watch, client, env  # noqa: F401 — fixtures

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "backend" / "scripts" / "diff_ica_israel_index.py"


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location("diff_ica_israel_index", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["diff_ica_israel_index"] = module
    spec.loader.exec_module(module)
    return module


differ = _load_script()

NEXT_CUT = "2026-11-12"
OTHER = "513000001"


def _build(tmp_path: Path, *, changes: bool = True) -> Path:
    out = tmp_path / "il" / "ica_israel.sqlite"
    out.parent.mkdir(parents=True, exist_ok=True)
    builder.build(
        {
            "company": [_company(NUMBER, "פייפאל ישראל בע~מ"), _company(OTHER, "אחרת בע~מ")],
            "partnership": [],
            **({"change": [_change(NUMBER, 3, "02/11/2026"), _change(NUMBER, 22, "18/08/2026")]} if changes else {}),
        },
        out, CUT, min_rows={}, changes_cut=CUT if changes else None,
    )
    return out


def _pair(tmp_path: Path, edits: list[str], *, old_changes: bool = True) -> tuple[Path, Path]:
    old = _build(tmp_path, changes=old_changes)
    new = old.parent / "today.sqlite"
    shutil.copy(old, new)
    conn = sqlite3.connect(new)
    for sql in edits:
        conn.execute(sql)
    conn.execute("UPDATE meta SET value = ? WHERE key = 'cut'", (NEXT_CUT,))
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('changes_cut', ?)", (NEXT_CUT,))
    conn.commit()
    conn.close()
    return old, new


def _row(code: int, day: str, charge: int = 0, number: str = NUMBER) -> str:
    return f"INSERT INTO change VALUES ('{number}', {code}, '{day}', {charge}, '{NEXT_CUT}', 0)"


# ---- the diff ----------------------------------------------------------------


def test_an_identical_build_is_an_empty_day(tmp_path: Path) -> None:
    old, new = _pair(tmp_path, [])
    day = differ.diff_indexes(old, new)
    assert day["cut"] == NEXT_CUT and day["previous_cut"] == CUT
    assert day["changes"] == {} and day["filings_diffed"] is True and day["new_filings"] == 0


def test_a_new_material_filing_is_listed_as_its_code_and_day(tmp_path: Path) -> None:
    old, new = _pair(tmp_path, [
        _row(3, "2026-11-11"),             # share transfer — material
        _row(146, "2026-11-10"),           # fast-track voluntary winding-up — material
        _row(68, "2026-11-11", charge=9),  # charge registered — not material
        _row(22, "2026-11-11"),            # annual report — routine
        _row(25, "2026-11-11", number=OTHER),  # officers updated — not material
    ])
    day = differ.diff_indexes(old, new)
    assert day["changes"] == {NUMBER: {"filings": [[146, "2026-11-10"], [3, "2026-11-11"]]}}
    assert day["new_filings"] == 2 and day["summary"] == {"filings": 1}


def test_a_row_carried_from_yesterday_is_not_new(tmp_path: Path) -> None:
    # The same share transfer as yesterday's build; only another day is new.
    old, new = _pair(tmp_path, [_row(3, "2026-11-03")])
    assert differ.diff_indexes(old, new)["changes"] == {NUMBER: {"filings": [[3, "2026-11-03"]]}}


@pytest.mark.parametrize(
    "edit, expected",
    [
        (f"UPDATE entity SET name_he = 'שם חדש' WHERE number = '{NUMBER}'", {"name": ["פייפאל ישראל בע\"מ", "שם חדש"]}),
        (f"UPDATE entity SET status_words = 'בפירוק מרצון' WHERE number = '{NUMBER}'", {"status": ["פעילה", "בפירוק מרצון"]}),
        (f"UPDATE entity SET violating = 'מפרה' WHERE number = '{NUMBER}'", {"violating": [None, "מפרה"]}),
        (f"DELETE FROM entity WHERE number = '{NUMBER}'", {"removed": True}),
    ],
)
def test_the_companies_file_fields_and_removal_are_changes(tmp_path: Path, edit: str, expected: dict) -> None:
    old, new = _pair(tmp_path, [edit])
    assert differ.diff_indexes(old, new)["changes"] == {NUMBER: expected}


def test_an_address_is_not_a_change(tmp_path: Path) -> None:
    old, new = _pair(tmp_path, [f"UPDATE entity SET city = 'חיפה', street = 'הרצל' WHERE number = '{NUMBER}'"])
    assert differ.diff_indexes(old, new)["changes"] == {}


def test_the_first_build_with_a_change_log_diffs_no_filings(tmp_path: Path) -> None:
    # Yesterday's build held no change log: every row of the year would read
    # as new, so the filings are not diffed — the fields still are.
    old, new = _pair(tmp_path, [_row(3, "2026-11-11")], old_changes=False)
    day = differ.diff_indexes(old, new)
    assert day["filings_diffed"] is False and day["changes"] == {}


def test_a_day_with_too_many_filings_is_kept_without_numbers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(differ, "SUSPECT_ROWS", 1)
    old, new = _pair(tmp_path, [_row(3, "2026-11-11"), _row(2, "2026-11-11")])
    day = differ.diff_indexes(old, new)
    assert day["suspect"] is True and "changes" not in day and day["new_filings"] == 2


def test_the_command_line_writes_the_rolling_file(tmp_path: Path) -> None:
    old, new = _pair(tmp_path, [_row(3, "2026-11-11")])
    out = tmp_path / "delta.json"
    assert differ.main(["--old", str(old), "--new", str(new), "--out", str(out)]) == 0
    doc = json.loads(out.read_text())
    assert doc["schema"] == 1 and [d["cut"] for d in doc["days"]] == [NEXT_CUT]
    # A same-day re-run diffs the build against itself: the day is kept.
    assert differ.main(["--old", str(new), "--new", str(new), "--history", str(out), "--out", str(out)]) == 0
    assert json.loads(out.read_text())["days"][0]["changes"] == {NUMBER: {"filings": [[3, "2026-11-11"]]}}


def test_the_script_and_the_worker_name_the_same_fields() -> None:
    assert tuple(differ.ENTITY_FIELDS) == wl.ICA_ISRAEL_FIELDS


# ---- the worker --------------------------------------------------------------


def test_a_watchs_number_comes_from_gleif_then_the_baseline() -> None:
    assert wl.watch_il_number({"gleif_facts": {"registered_at": "RA000406", "registered_as": "51-647-8872"}}) == NUMBER
    assert wl.watch_il_number({"gleif_facts": {"registered_at": "RA000181", "registered_as": NUMBER}}) is None
    assert wl.watch_il_number({"snapshot": {"identifiers": {"IL-ROC": NUMBER}}}) == NUMBER
    assert wl.watch_il_number({}) is None


def test_several_days_merge_into_one_list_of_filings() -> None:
    merged = wl.merge_il_changes([
        ("2026-11-12", {"filings": [[3, "2026-11-11"]], "status": ["פעילה", "בפירוק"]}),
        ("2026-11-13", {"filings": [[3, "2026-11-11"], [24, "2026-11-12"]], "status": ["בפירוק", "פעילה"]}),
    ])
    # The status went round and came back; the filings are each said once.
    assert merged == {"filings": [
        {"code": 3, "day": "2026-11-11", "group": "shares", "label": "Share transfer"},
        {"code": 24, "day": "2026-11-12", "group": "name", "label": "Change of name"},
    ]}


@pytest.fixture
def israel(env: Path, tmp_path: Path, client: TestClient):  # noqa: F811
    """An index on disk (cut CUT), EASY watched as the Israeli company
    NUMBER, and the tier's watermark one day back."""
    out = _build(tmp_path)
    _point_at(out)
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    with store._conn() as conn:
        conn.execute(
            "UPDATE watches SET gleif_facts_json = ? WHERE lei = ?",
            (json.dumps({"registered_at": "RA000406", "registered_as": NUMBER}), EASY),
        )
    store.set_meta(wl.ICA_ISRAEL_CUT_KEY, "2026-11-10")
    yield {"store": store, "token": token}
    _point_at(None)


URL = "https://github.com/StephenAbbott/opencheck/releases/download/ica-israel-index/ica-israel-delta.json"


def test_a_watched_company_with_a_new_filing_is_queued(israel: dict, httpx_mock) -> None:  # noqa: ANN001
    httpx_mock.add_response(url=URL, json={"schema": 1, "updated_at": "2026-11-11T06:00:00Z", "days": [
        {"cut": CUT, "previous_cut": "2026-11-10",
         "changes": {NUMBER: {"filings": [[3, "2026-11-10"]]}, OTHER: {"status": ["a", "b"]}}},
    ]})
    assert wl.ica_israel_tick() == {"days": 1, "queued": 1}
    store = israel["store"]
    assert store.get_meta(wl.ICA_ISRAEL_CUT_KEY) == CUT
    (item,) = store.take_pending(10)
    assert item["lei"] == EASY and item["tier"] == wl.TIER_REGISTER
    assert item["trigger"] == {
        "tier": "register", "register": "ica_israel", "code": NUMBER, "cuts": [CUT],
        "changes": {"filings": [{"code": 3, "day": "2026-11-10", "group": "shares", "label": "Share transfer"}]},
    }
    assert wl.state()["il_last_cut"] == CUT
    # The Estonian watermark is the Estonian tier's: untouched.
    assert store.get_meta(wl.ARIREGISTER_CUT_KEY) is None


def test_a_suspect_day_is_a_catch_up_of_every_watched_israeli_company(israel: dict, httpx_mock) -> None:  # noqa: ANN001
    httpx_mock.add_response(url=URL, json={"schema": 1, "updated_at": "2026-11-11T06:00:00Z", "days": [
        {"cut": CUT, "previous_cut": "2026-11-10", "suspect": True},
    ]})
    out = wl.ica_israel_tick()
    assert out["gaps"] == 1 and out["catch_up_queued"] == 1
    (item,) = israel["store"].take_pending(10)
    assert item["trigger"]["reason"] == "register_gap" and item["trigger"]["register"] == "ica_israel"
    assert wl.ica_israel_gaps(israel["store"])[-1]["detail"] == "too many changes to list"


async def test_a_register_rerun_writes_an_israeli_entry(israel: dict) -> None:
    trigger = {"tier": "register", "register": "ica_israel", "code": NUMBER, "cuts": [CUT], "changes": wl.merge_il_changes(
        [(CUT, {"filings": [[3, "2026-11-10"], [4032, "2026-11-10"]], "status": ["פעילה", "בפירוק מרצון"]})]
    )}
    facts, _watermark = wl._mirror_facts(EASY)
    with israel["store"]._conn() as conn:
        conn.execute("UPDATE watches SET gleif_facts_json = ? WHERE lei = ?", (json.dumps(facts), EASY))
    result = await wl.rerun(EASY, wl.TIER_REGISTER, trigger)
    assert result["entries"] == 1 and result["changes"] == []
    (entry,) = israel["store"].entries(wl.token_hash(israel["token"]))
    assert entry["materiality"] == "recheck"
    assert _entry_title(entry) == "Recheck the source — EASY POWER: Israeli register record changed"
    assert _entry_content(entry).splitlines()[:4] == [
        f"The Israel Corporations Authority recorded a change to this company in its open data of {CUT} (corporate number {NUMBER}).",
        " - Share transfer filed with the Corporations Authority on 2026-11-10; parties not published.",
        " - Director added filed with the Corporations Authority on 2026-11-10.",
        " - Status: פעילה → בפירוק מרצון.",
    ]


def test_the_tier_line_says_what_israel_holds(israel: dict, client: TestClient) -> None:  # noqa: F811
    r = client.get(f"/watch/{israel['token']}")
    assert r.status_code == 200
    body = r.json()
    reg = body["tiers"]["register_il"]
    assert reg["available"] is True and reg["index_cut"] == CUT and reg["gaps"] == []
    (watch,) = [w for w in body["watches"] if w["lei"] == EASY]
    assert watch["register_tier"]["register"] == "ica_israel"
    assert watch["register_tier"]["words"] == "Corporations Authority open data, daily"


# ---- grading and words ------------------------------------------------------


def _filing(code: int) -> dict[str, Any]:
    return wl.merge_il_changes([(CUT, {"filings": [[code, "2026-11-10"]]})])


@pytest.mark.parametrize(
    "changes, tier",
    [
        (_filing(3), "recheck"),      # share transfer
        (_filing(146), "recheck"),    # winding-up
        (_filing(55), "recheck"),     # merger
        (_filing(74), "recheck"),     # attachment order
        (_filing(21), "note"),        # capital increased
        (_filing(4032), "note"),      # director added
        (_filing(39), "note"),        # reclassified
        ({"removed": True}, "escalate"),
        ({"violating": [None, "מפרה"]}, "note"),
        ({"status": ["a", "b"]}, "recheck"),
        ({}, "note"),
    ],
)
def test_an_israeli_trigger_is_graded_by_its_filings_and_fields(changes: dict, tier: str) -> None:
    assert m.register_trigger_tier({"register": "ica_israel", "changes": changes}) == tier


def test_every_material_group_is_graded() -> None:
    from opencheck.sources.ica_israel_changes import MATERIAL_GROUPS

    assert set(m.IL_FILING_GROUP_TIER) == set(MATERIAL_GROUPS)
    assert set(m.IL_FIELD_TIER) == set(wl.ICA_ISRAEL_FIELDS) | set(wl.ICA_ISRAEL_FLAGS)
    assert set(m.IL_FILING_GROUP_TIER.values()) | set(m.IL_FIELD_TIER.values()) <= set(m.TIERS)


def test_the_page_carries_the_same_words() -> None:
    text = (REPO / "frontend/src/lib/watchlist.ts").read_text()

    def table(name: str) -> dict[str, str]:
        body = text[text.index(f"const {name}") :]
        body = body[body.index("{") + 1 : body.index("};")]
        return dict(re.findall(r'(\w+):\s*"([^"]*)"', body))

    assert table("IL_CHANGE_WORDS") == wl.IL_CHANGE_WORDS
    assert table("REGISTER_FOLLOWED_WORDS") == psc.REGISTER_FOLLOWED_WORDS


async def test_the_index_refresh_loop_calls_the_asset_check(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    from opencheck.sources import ica_israel

    calls: list[int] = []

    def _warm() -> dict:
        calls.append(1)
        if len(calls) >= 2:
            raise asyncio.CancelledError
        return {"ica_israel": "already present: x"}

    monkeypatch.setattr(ica_israel, "warm_index", _warm)
    with pytest.raises(asyncio.CancelledError):
        await ica_israel.refresh_loop(0.0)
    assert len(calls) == 2
