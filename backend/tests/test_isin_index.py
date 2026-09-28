"""Phase 258 — GLEIF's ISIN-to-LEI file as a local table (opencheck/isin_index.py).

Small hand-made files, in GLEIF's exact shape (a zip holding one ``LEI,ISIN``
CSV). The real file was checked against live GLEIF on 28 Sept 2026 — Shell
1,813, ``529900W18LQJJN6SJ336`` 636,388, ``549300TS3U4JKMR1B479`` 531,250, all
exact, every Shell page equal to the sorted list — which is what these pin in
miniature: counts, ISIN order, pages across chunk boundaries, "none" for an
LEI the file does not list, and a table that is never used once it is old.
"""

from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from opencheck import isin_index as ix
from opencheck.config import get_settings

SHELL = "21380068P1DRHMJ8KU70"
BIG = "529900W18LQJJN6SJ336"
NONE = "213800WMPZ7LH3F92517"  # Quantexa: GLEIF lists no ISINs


def _now_iso(days_ago: float = 0.0) -> str:
    when = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def _zip(tmp_path: Path, rows: list[tuple[str, str]], *, header=("LEI", "ISIN")) -> Path:
    buf = io.StringIO()
    buf.write(",".join(header) + "\r\n")
    for lei, isin in rows:
        buf.write(f"{lei},{isin}\r\n")
    path = tmp_path / "isin-lei-test.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lei-isin-test.csv", buf.getvalue())
    return path


def _isins(prefix: str, n: int) -> list[str]:
    return [f"{prefix}{i:010d}"[:12] for i in range(n)]


@pytest.fixture
def table(tmp_path, monkeypatch):
    """A built table at the configured path, file uploaded an hour ago."""
    db = tmp_path / "isin.sqlite"
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(db))
    get_settings.cache_clear()
    ix.reset_for_tests()
    shell = _isins("US", 7)
    rows = [(SHELL, i) for i in reversed(shell)]  # unsorted on purpose
    rows += [(BIG, i) for i in _isins("DE", 23)]
    rows += [(SHELL, shell[0])]  # a duplicated row
    rows += [("not-an-lei", "US0000000001"), (SHELL, "bad")]  # skipped
    meta = ix.build_index(
        _zip(tmp_path, rows), db, file_name="isin-lei-test.zip", uploaded_at=_now_iso(1 / 24)
    )
    yield {"db": db, "meta": meta, "shell": sorted(shell), "big": _isins("DE", 23)}
    get_settings.cache_clear()
    ix.reset_for_tests()


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def test_build_counts_dedupes_and_skips(table):
    meta = table["meta"]
    assert meta["leis"] == "2"
    assert meta["isins"] == str(7 + 23)
    assert meta["skipped_rows"] == "2"
    assert meta["schema_version"] == ix.SCHEMA_VERSION
    assert not Path(str(table["db"]) + ".building").exists()


def test_lists_come_back_in_isin_order(table):
    page = ix.lookup(SHELL, 1, 20)
    assert page.total == 7
    assert page.isins == table["shell"]
    assert page.file_name == "isin-lei-test.zip"


def test_pages_slice_across_chunk_boundaries(tmp_path, monkeypatch):
    monkeypatch.setattr(ix, "CHUNK", 4)
    db = tmp_path / "chunked.sqlite"
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(db))
    get_settings.cache_clear()
    ix.reset_for_tests()
    isins = _isins("DE", 23)
    ix.build_index(_zip(tmp_path, [(BIG, i) for i in isins]), db,
                   file_name="f.zip", uploaded_at=_now_iso())
    got: list[str] = []
    for page in range(1, 6):
        p = ix.lookup(BIG, page, 5)
        assert p.total == 23
        got.extend(p.isins)
    assert got == isins
    assert ix.lookup(BIG, 3, 7).isins == isins[14:21]  # spans chunks 3–5
    assert ix.lookup(BIG, 9, 5).isins == []  # past the end
    get_settings.cache_clear()
    ix.reset_for_tests()


def test_an_lei_the_file_does_not_list_is_answered_none(table):
    page = ix.lookup(NONE, 1, 20)
    assert page is not None
    assert page.total == 0 and page.isins == []
    assert page.as_of  # the answer is still dated


def test_the_answer_is_dated_by_the_file(table):
    page = ix.lookup(SHELL)
    assert page.as_of == table["meta"]["uploaded_at"]


def test_a_wrong_header_is_refused_and_leaves_the_old_table(table, tmp_path):
    bad = _zip(tmp_path, [(SHELL, "US0000000001")], header=("ISIN", "LEI"))
    with pytest.raises(ix.IndexBuildError):
        ix.build_index(bad, table["db"], file_name="bad.zip", uploaded_at=_now_iso())
    ix.reset_connection()
    assert ix.lookup(SHELL).total == 7  # the old table still answers
    assert not Path(str(table["db"]) + ".building").exists()


def test_an_empty_file_is_refused(tmp_path):
    with pytest.raises(ix.IndexBuildError):
        ix.build_index(_zip(tmp_path, []), tmp_path / "e.sqlite",
                       file_name="e.zip", uploaded_at=_now_iso())


def test_a_bare_csv_builds_too(tmp_path):
    csv_path = tmp_path / "x.csv"
    csv_path.write_text(f"LEI,ISIN\n{SHELL},US0000000001\n", encoding="utf-8")
    meta = ix.build_index(csv_path, tmp_path / "c.sqlite", file_name="x.csv",
                          uploaded_at=_now_iso())
    assert meta["isins"] == "1"


# ---------------------------------------------------------------------------
# Read: when there is no usable table
# ---------------------------------------------------------------------------


def test_no_file_means_no_table(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(tmp_path / "missing.sqlite"))
    get_settings.cache_clear()
    ix.reset_for_tests()
    assert ix.lookup(SHELL) is None
    assert ix.usable() is False
    get_settings.cache_clear()


def test_an_old_file_is_not_used(tmp_path, monkeypatch):
    db = tmp_path / "old.sqlite"
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_MAX_AGE_DAYS", "3")
    get_settings.cache_clear()
    ix.reset_for_tests()
    ix.build_index(_zip(tmp_path, [(SHELL, "US0000000001")]), db,
                   file_name="old.zip", uploaded_at=_now_iso(4))
    assert ix.usable() is False
    assert ix.lookup(SHELL) is None
    get_settings.cache_clear()
    ix.reset_for_tests()


def test_a_wrong_schema_is_not_used(table):
    conn = sqlite3.connect(table["db"])
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    ix.reset_connection()
    assert ix.lookup(SHELL) is None


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


@pytest.fixture
def syncing(tmp_path, monkeypatch):
    db = tmp_path / "sync.sqlite"
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_SYNC", "true")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    ix.reset_for_tests()
    state = {"file": "isin-lei-A.zip", "rows": [(SHELL, "US0000000001")], "downloads": 0}

    def latest():
        return {"file_name": state["file"], "uploaded_at": _now_iso(), "url": "https://x/dl"}

    def download(url, target):
        state["downloads"] += 1
        src = _zip(tmp_path, state["rows"])
        target.write_bytes(src.read_bytes())
        return target.stat().st_size

    monkeypatch.setattr(ix, "latest_file", latest)
    monkeypatch.setattr(ix, "_download", download)
    yield state
    get_settings.cache_clear()
    ix.reset_for_tests()


def test_sync_builds_then_keeps_then_rebuilds(syncing):
    first = ix.sync_index()
    assert first["isin_index"] == "built" and first["isins"] == 1
    assert ix.lookup(SHELL).total == 1

    again = ix.sync_index()
    assert again["isin_index"] == "kept"
    assert syncing["downloads"] == 1  # same file name: nothing downloaded

    syncing["file"] = "isin-lei-B.zip"
    syncing["rows"] = [(SHELL, "US0000000001"), (SHELL, "US0000000002")]
    third = ix.sync_index()
    assert third["isin_index"] == "built"
    assert ix.lookup(SHELL).total == 2  # the new table is read, not the old


def test_a_failed_sync_keeps_the_table_and_retries_sooner(syncing, monkeypatch):
    ix.sync_index()
    monkeypatch.setattr(ix.time, "sleep", lambda s: None)

    def broken():
        raise httpx.ConnectError("Connection reset by peer")

    monkeypatch.setattr(ix, "latest_file", broken)
    out = ix.sync_index()
    assert out["isin_index"] == "failed" and "ConnectError" in out["error"]
    assert ix.lookup(SHELL).total == 1
    assert ix._next_wait(21600.0) == ix.RETRY_AFTER_FAILURE_S


def test_retrying_recovers_from_one_reset(monkeypatch):
    monkeypatch.setattr(ix.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("Connection reset by peer")
        return "ok"

    assert ix._retrying(flaky) == "ok" and calls["n"] == 2


def test_sync_disabled_never_downloads(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(tmp_path / "x.sqlite"))
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_SYNC", "false")
    get_settings.cache_clear()
    ix.reset_for_tests()

    def boom():  # pragma: no cover - must not run
        raise AssertionError("downloaded with sync off")

    monkeypatch.setattr(ix, "latest_file", boom)
    assert ix.warm_index()["isin_index"] == "not_configured"
    get_settings.cache_clear()


def test_status_names_no_lei(table):
    status = ix.status()
    assert status["usable"] is True and status["leis"] == 2 and status["isins"] == 30
    dumped = json.dumps(status)
    assert SHELL not in dumped and BIG not in dumped
