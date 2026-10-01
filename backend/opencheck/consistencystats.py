"""consistencystats — how often independent sources agree, per field and pair.

Phase 152, shadow mode. The record-consistency check (``consistency.py``)
computes, for every entity that several sources described, whether they agree
on liveness, jurisdiction, founding date and one-per-entity identifiers.
Before any of that reaches the results page, the question is: *for which
(field, source A, source B) is disagreement rare enough to be informative?*
A pair-field where the sources disagree a third of the time is two sources
answering different questions and must be re-aligned or dropped; one where
they disagree once in a hundred lookups is a finding when it fires.

This is the counter that answers it, in the same shape as ``signalstats``:
aggregate only, bounded, failing soft, served at ``/consistencystats``. Keys
are closed vocabularies — field names, adapter ids, relation names — so no
entity name, LEI, identifier value or date can appear. The ``values`` an
``Item`` carries are never recorded here.

Phase 268: the counters survive a deploy. The first reading (17 Sept 2026)
found an eleven-hour window where the gate asks for two weeks, because the
counts were in-process and OpenCheck deploys most days. With
``OPENCHECK_CONSISTENCYSTATS_DB_FILE`` set (``render.yaml`` puts it on the
persistent disk beside the watchlist), every ``record()`` writes the changed
rows through to a small SQLite file in one transaction and a boot loads them
back, so ``since`` is the first persisted count and ``boots`` says how many
processes have added to it. The file holds exactly what the endpoint shows —
integer counts under closed-vocabulary keys — and nothing else. Unset, or an
unwritable path, leaves the counters in-process as before and never touches
the lookup.

Exit criterion for Phase D (written on the Notion plan): a pair-field enters
the UI only if its measured ``disagree / (agree + disagree)`` is under 10 %
**and** it has fired at least once, after at least two weeks of traffic.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

from . import sqlite_schema
from .consistency import RELATIONS, ConsistencyResult

log = logging.getLogger("opencheck.consistencystats")

_MAX_KEYS = 5_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pairs (
    field     TEXT NOT NULL,
    source_a  TEXT NOT NULL,
    source_b  TEXT NOT NULL,
    relation  TEXT NOT NULL,
    n         INTEGER NOT NULL,
    PRIMARY KEY (field, source_a, source_b, relation)
);
"""

MIGRATIONS: tuple[sqlite_schema.Migration, ...] = (
    sqlite_schema.Migration(1, "Phase 268 schema", sqlite_schema.statements(SCHEMA)),
)


class _Counters:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started = time.time()
        self.lookups = 0
        self.lookups_with_groups = 0
        #: (field, source_a, source_b, relation) → n; the pair is sorted so
        #: (gleif, companies_house) and (companies_house, gleif) are one key.
        self.pairs: Counter[tuple[str, str, str, str]] = Counter()
        self.truncated = False
        #: Phase 268 — the file the counters are written through to, or
        #: ``None`` for in-process only. ``boots`` counts the processes that
        #: have added to the file (1 for an in-process run).
        self.path: Path | None = None
        self.boots = 1


totals = _Counters()


def reset() -> None:
    """Clear all counters and detach any file. Tests only."""
    global totals
    totals = _Counters()


# ---------------------------------------------------------------------
# Persistence (Phase 268)
# ---------------------------------------------------------------------


def _conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def configure(path: str | Path | None) -> bool:
    """Attach the counters to ``path`` (create or load it). Returns whether
    the file is in use.

    Called once at boot from the app lifespan with the configured setting.
    A path that cannot be opened or written leaves the counters in-process
    and logs why — instrumentation never breaks a lookup, and never breaks a
    boot either.
    """
    if not path:
        with totals.lock:
            totals.path = None
        return False
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        conn = _conn(p)
        try:
            sqlite_schema.migrate(conn, p, MIGRATIONS)
            meta = {k: v for k, v in conn.execute("SELECT key, value FROM meta")}
            rows = conn.execute("SELECT field, source_a, source_b, relation, n FROM pairs").fetchall()
            boots = int(meta.get("boots", "0")) + 1
            started = float(meta.get("started", "0") or 0) or time.time()
            with totals.lock:
                totals.path = p
                totals.started = started
                totals.lookups = int(meta.get("lookups", "0"))
                totals.lookups_with_groups = int(meta.get("lookups_with_groups", "0"))
                totals.truncated = meta.get("truncated", "0") == "1"
                totals.boots = boots
                totals.pairs = Counter({(f, a, b, r): int(n) for f, a, b, r, n in rows})
                conn.execute("BEGIN IMMEDIATE")
                _write_meta(conn)
                conn.execute("COMMIT")
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("consistencystats: %s is not usable (%s); counters stay in-process", p, exc)
        with totals.lock:
            totals.path = None
        return False
    log.info(
        "consistencystats: persisted at %s (boot %d, %d lookups since %s)",
        p,
        totals.boots,
        totals.lookups,
        _iso(totals.started),
    )
    return True


def _write_meta(conn: sqlite3.Connection) -> None:
    """Caller holds the lock and an open transaction."""
    conn.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [
            ("started", repr(totals.started)),
            ("lookups", str(totals.lookups)),
            ("lookups_with_groups", str(totals.lookups_with_groups)),
            ("truncated", "1" if totals.truncated else "0"),
            ("boots", str(totals.boots)),
        ],
    )


def _write_through(changed: set[tuple[str, str, str, str]]) -> None:
    """Caller holds the lock. One short transaction per lookup; a failure
    detaches the file (and says so once) rather than failing the lookup."""
    path = totals.path
    if path is None:
        return
    try:
        conn = _conn(path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT INTO pairs (field, source_a, source_b, relation, n) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(field, source_a, source_b, relation) DO UPDATE SET n = excluded.n",
                [(f, a, b, r, totals.pairs[(f, a, b, r)]) for (f, a, b, r) in changed],
            )
            _write_meta(conn)
            conn.execute("COMMIT")
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("consistencystats: write to %s failed (%s); counters continue in-process", path, exc)
        totals.path = None


# ---------------------------------------------------------------------
# Counting
# ---------------------------------------------------------------------


def record(result: ConsistencyResult) -> None:
    """Count one lookup's comparison outcomes."""
    try:
        with totals.lock:
            totals.lookups += 1
            if result.groups:
                totals.lookups_with_groups += 1
            changed: set[tuple[str, str, str, str]] = set()
            for item in result.items:
                if item.relation not in RELATIONS:
                    continue
                a, b = sorted(item.sources)
                key = (item.field, a, b, item.relation)
                if key not in totals.pairs and len(totals.pairs) >= _MAX_KEYS:
                    totals.truncated = True
                    continue
                totals.pairs[key] += 1
                changed.add(key)
            _write_through(changed)
    except Exception as exc:  # noqa: BLE001
        log.debug("consistencystats.record failed, ignoring: %s", exc)


def _iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def stats() -> dict[str, Any]:
    """Aggregate-only snapshot for ``/consistencystats``.

    ``pairs`` is a flat ``"field|source_a|source_b"`` → ``{relation: n}``
    map, plus a ``disagree_rate`` (``disagree / (agree + disagree)``,
    ``None`` when neither happened) so the base-rate gate can be read
    straight off the endpoint. ``since`` is the first persisted count when a
    file is in use (``persisted``), else this process's start; ``boots`` is
    how many processes have added to the file.
    """
    with totals.lock:
        rows: dict[str, dict[str, Any]] = {}
        for (field, a, b, relation), n in totals.pairs.items():
            row = rows.setdefault(f"{field}|{a}|{b}", {r: 0 for r in RELATIONS})
            row[relation] = n
        for row in rows.values():
            decided = row["agree"] + row["disagree"]
            row["disagree_rate"] = (row["disagree"] / decided) if decided else None
        persisted = totals.path is not None
        return {
            "since": _iso(totals.started),
            "uptime_s": int(time.time() - totals.started),
            "persisted": persisted,
            "boots": totals.boots,
            "lookups": totals.lookups,
            "lookups_with_groups": totals.lookups_with_groups,
            "pairs": dict(sorted(rows.items())),
            "truncated": totals.truncated,
            "note": (
                "Shadow-mode counters for the record-consistency check (Phase 152). "
                "Aggregate only; keys are field names, adapter ids and relation "
                "names. 'stale'/'mirror' are pairs where one source republishes the "
                "other (see /sources derived_from). "
                + (
                    "Persisted across deploys since 'since' (Phase 268)."
                    if persisted
                    else "Not persisted: resets on deploy (set OPENCHECK_CONSISTENCYSTATS_DB_FILE)."
                )
            ),
        }
