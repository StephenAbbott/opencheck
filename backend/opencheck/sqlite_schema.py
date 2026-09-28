"""Schema versions for OpenCheck's own SQLite files (Phase 260).

``watchlist.sqlite`` and ``saved_reports.sqlite`` are the two files OpenCheck
writes and promises to keep: a watchlist's baseline and log, and a saved
report whose SHA-256 a reader can re-check. Until Phase 260 each opened with
``CREATE TABLE IF NOT EXISTS`` and nothing else, so a column added later had
no way in and a file written by a newer build was read by an older one as if
nothing had changed.

Each file now carries ``PRAGMA user_version``. A store declares an ordered
list of :class:`Migration` s — version 1 is the schema as it shipped, so an
existing file (``user_version`` 0) is stamped 1 without anything changing —
and :func:`migrate` applies the missing ones in one ``BEGIN IMMEDIATE``
transaction, so two processes opening the file at once cannot both apply a
step. A file whose version is *newer* than this build knows is refused with
:class:`SchemaTooNewError` rather than written to: that is a rollback, and an
older build writing rows a newer schema reads differently is how a durable
record stops being one.

Adding a step: append ``Migration(n + 1, "what it does", (...statements...))``
to the store's list. Never edit or reorder a shipped step — a deployed file
has already run it. ``tests/test_sqlite_schema_phase260.py`` pins that each
list is contiguous from 1 and that a version-0 file of the shipped shape
opens unchanged.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

Step = str | Callable[[sqlite3.Connection], None]


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    steps: tuple[Step, ...]


class SchemaTooNewError(RuntimeError):
    """The file was written by a newer OpenCheck than this one."""

    def __init__(self, path: Path, found: int, known: int):
        self.path = path
        self.found = found
        self.known = known
        super().__init__(
            f"{Path(path).name} is at schema version {found}; this build knows up to {known}. "
            "It was written by a newer OpenCheck — refusing to open it rather than write "
            "rows the newer schema would read differently."
        )


def statements(ddl: str) -> tuple[str, ...]:
    """Split a DDL script into statements. For OpenCheck's own schemas only —
    they hold no ``;`` inside a string or a trigger body."""
    return tuple(s.strip() for s in ddl.split(";") if s.strip())


def latest(migrations: Sequence[Migration]) -> int:
    return migrations[-1].version if migrations else 0


def check_contiguous(migrations: Sequence[Migration]) -> None:
    for i, m in enumerate(migrations, start=1):
        if m.version != i:
            raise ValueError(f"migration {i} is numbered {m.version}; steps must run 1, 2, 3…")


def user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def migrate(conn: sqlite3.Connection, path: Path, migrations: Sequence[Migration]) -> int:
    """Bring the file ``conn`` is open on up to the last migration. Returns
    the version the file is at afterwards. ``conn`` must be in autocommit
    mode (``isolation_level=None``), as both stores open it."""
    check_contiguous(migrations)
    known = latest(migrations)
    found = user_version(conn)
    if found > known:
        raise SchemaTooNewError(path, found, known)
    if found == known:
        return found
    conn.execute("BEGIN IMMEDIATE")
    try:
        # Re-read under the write lock: another process may have got here first.
        found = user_version(conn)
        if found > known:
            raise SchemaTooNewError(path, found, known)
        for m in migrations:
            if m.version <= found:
                continue
            for step in m.steps:
                if callable(step):
                    step(conn)
                else:
                    conn.execute(step)
            # PRAGMA takes no bound parameters; the value is an int we own.
            conn.execute(f"PRAGMA user_version = {int(m.version)}")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return known
