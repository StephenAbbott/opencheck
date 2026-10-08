"""Phase 260 — schema versions for watchlist.sqlite and saved_reports.sqlite."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import consistencystats as cs
from opencheck import saved_reports as sr
from opencheck import sqlite_schema as ss
from opencheck import watchlist as wl
from opencheck.app import app
from opencheck.config import get_settings


def _ver(path: Path) -> int:
    conn = sqlite3.connect(path)
    try:
        return ss.user_version(conn)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "migrations",
    [wl.MIGRATIONS, sr.MIGRATIONS, cs.MIGRATIONS],
    ids=["watchlist", "saved_reports", "consistencystats"],
)
def test_each_store_numbers_its_steps_from_one(migrations: tuple[ss.Migration, ...]) -> None:
    ss.check_contiguous(migrations)
    assert migrations[0].version == 1


def test_a_watchlist_file_from_before_260_is_stamped_and_keeps_its_rows(tmp_path: Path) -> None:
    path = tmp_path / "watchlist.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript(wl.SCHEMA)  # exactly what Phase 215 wrote, user_version 0
    conn.execute("INSERT INTO lists VALUES ('h', '2026-09-16T00:00:00Z', '2026-09-16T00:00:00Z')")
    conn.execute("INSERT INTO meta VALUES ('opensanctions_version', 'v1')")
    conn.commit()
    conn.close()
    assert _ver(path) == 0

    store = wl.WatchlistStore(path, wl.Caps(10, 10))
    # Stamped 1, then every later step applied (Phase 303 added step 2).
    latest = len(wl.MIGRATIONS)
    assert store.schema_version == latest and _ver(path) == latest
    assert store.list_exists("h") and store.get_meta("opensanctions_version") == "v1"
    # Opening again is a no-op.
    assert wl.WatchlistStore(path, wl.Caps(10, 10)).schema_version == latest


def test_a_saved_reports_file_from_before_260_is_stamped(tmp_path: Path) -> None:
    path = tmp_path / "saved_reports.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript(sr.DDL)
    conn.execute(
        "INSERT INTO dispositions VALUES ('L', 'r', '{}', '2026-09-16T00:00:00Z')"
    )
    conn.commit()
    conn.close()
    store = sr.SavedReportsStore(path, retention_days=90, max_total=10)
    assert store.schema_version == 1 and _ver(path) == 1
    assert store.get_disposition("L", "r") == "{}"


def test_a_new_file_starts_at_the_latest_version(tmp_path: Path) -> None:
    assert wl.WatchlistStore(tmp_path / "w.sqlite", wl.Caps(1, 1)).schema_version == ss.latest(wl.MIGRATIONS)
    assert sr.SavedReportsStore(tmp_path / "s.sqlite", retention_days=1, max_total=1).schema_version == ss.latest(
        sr.MIGRATIONS
    )


def test_a_file_from_a_newer_build_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "w.sqlite"
    wl.WatchlistStore(path, wl.Caps(1, 1))
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = 99")
    conn.close()
    with pytest.raises(ss.SchemaTooNewError, match="version 99"):
        wl.WatchlistStore(path, wl.Caps(1, 1))


def test_the_watch_routes_say_so_rather_than_500(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "w.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = 99")
    conn.close()
    monkeypatch.setenv("OPENCHECK_WATCHLIST_DB_FILE", str(path))
    monkeypatch.setenv("OPENCHECK_WATCHLIST_INTERVAL_S", "0")
    get_settings.cache_clear()
    wl.reset_for_tests()
    try:
        r = TestClient(app).get("/watch/some-token")
        assert r.status_code == 503 and "newer version" in r.json()["detail"]
    finally:
        get_settings.cache_clear()
        wl.reset_for_tests()


def test_a_later_step_runs_once_and_a_failing_step_rolls_back(tmp_path: Path) -> None:
    path = tmp_path / "x.sqlite"
    base = (ss.Migration(1, "base", ("CREATE TABLE t (a TEXT)",)),)
    add = base + (ss.Migration(2, "add b", ("ALTER TABLE t ADD COLUMN b TEXT",)),)
    broken = add + (ss.Migration(3, "broken", ("ALTER TABLE t ADD COLUMN c TEXT", "NOT SQL")),)

    def _open(migs: tuple[ss.Migration, ...]) -> int:
        conn = sqlite3.connect(path, isolation_level=None)
        try:
            return ss.migrate(conn, path, migs)
        finally:
            conn.close()

    assert _open(base) == 1
    assert _open(add) == 2
    assert _open(add) == 2  # not applied twice (ALTER would fail)
    with pytest.raises(sqlite3.OperationalError):
        _open(broken)
    assert _ver(path) == 2
    conn = sqlite3.connect(path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(t)")]
    conn.close()
    assert cols == ["a", "b"]  # step 3's first statement was rolled back with it


def test_steps_must_be_numbered_in_order() -> None:
    with pytest.raises(ValueError):
        ss.check_contiguous((ss.Migration(2, "x", ()),))
