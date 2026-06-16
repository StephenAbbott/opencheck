"""Tests for the OpenTender build-script finalisation (layer A).

`finalise_db()` must turn a freshly-built (WAL-mode) DB into a clean,
self-contained, integrity-verified single-file artifact and return its SHA-256,
so the runtime download can pin and verify it.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

# The build script isn't an importable package — load it by path.
_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "extract_opentender.py"
_spec = importlib.util.spec_from_file_location("extract_opentender", _SCRIPT)
extract_opentender = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(extract_opentender)


def _wal_db(path: Path) -> None:
    """Build a small WAL-mode DB, as the extract script does."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE tenders (persistent_id TEXT PRIMARY KEY, data TEXT)")
    conn.executemany(
        "INSERT INTO tenders VALUES (?, ?)",
        [(f"p{i}", "{}") for i in range(50)],
    )
    conn.commit()
    conn.close()


def test_finalise_db_produces_clean_verified_artifact(tmp_path: Path) -> None:
    db = tmp_path / "opentender.db"
    _wal_db(db)

    digest = extract_opentender.finalise_db(db)

    # A real, stable SHA-256 of the finalised file.
    assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest.lower())
    assert digest == extract_opentender._sha256_file(db)

    # Self-contained: no WAL/SHM dependency, no leftover temp.
    assert not (tmp_path / "opentender.db-wal").exists()
    assert not (tmp_path / "opentender.db-shm").exists()
    assert not (tmp_path / "opentender.db.clean").exists()

    # Opens exactly as the adapter does at runtime (read-only/immutable) and is intact.
    ro = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    try:
        assert ro.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert ro.execute("SELECT count(*) FROM tenders").fetchone()[0] == 50
    finally:
        ro.close()


def test_finalise_db_raises_on_corrupt_input(tmp_path: Path) -> None:
    good = tmp_path / "good.db"
    _wal_db(good)
    extract_opentender.finalise_db(good)  # finalise to a clean single file
    blob = good.read_bytes()

    bad = tmp_path / "bad.db"
    bad.write_bytes(blob[: len(blob) // 2])  # valid header, truncated body

    with pytest.raises(Exception):
        extract_opentender.finalise_db(bad)
