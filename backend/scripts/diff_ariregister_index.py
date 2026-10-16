#!/usr/bin/env python3
"""Diff two builds of the Estonian e-Business Register index — the
register's day-to-day changes, for the watchlist's register tier.

The daily refresh (``refresh-ariregister-index.yml``) downloads the index it
is about to replace, builds today's, and runs this script over the pair.
Each build is one cut of RIK's open-data files, so the difference between two
consecutive builds is what the register published as changed that day. The
result is appended to ``ariregister-delta.json``, a rolling file of the last
:data:`KEEP_DAYS` days published beside the index; the backend's watchlist
worker reads it and re-runs a watched Estonian company whose register record
changed (``opencheck/watchlist.py``, the register tier).

What counts as a change (Stephen, 16 Oct 2026):

* **the people lists** — the persons on the registry card (``officers``),
  the shareholders (the shareholders file and the AS shareholders recorded
  off the card, role ``S``), the beneficial owners, and the supervisory
  board (off the card, roles ``N`` and ``E``). Founders and auditors are not
  read: a founder does not change, and an auditor is no interest.
* **the company's name, status and legal form** — the entity row without its
  address or VAT number.
* a company **added to** or **removed from** the files (deleted companies are
  not published, so removal is how a deletion shows).

Nothing else: an address, contact, capital or a new card entry is not a
change here (the ``GLEIF_MATERIAL_FIELDS`` lesson — churn must not re-run).

A list's change is recorded as its row count before and after, never the
people in it: the watchlist store holds LEIs, legal names and digests, not
persons (the Phase 215 line). The row ``kirje_id`` is left out of the digest,
so a row re-registered with the same content is not a change.

A day on which more than :data:`SUSPECT_SHARE` of the companies changed is
kept as a summary with ``suspect: true`` and no codes — a regenerated
person key or a format change would otherwise re-run every watched Estonian
company with nothing to say about any of them.

Standard library only: it runs in the refresh workflow with plain
``python3``.

    python3 scripts/diff_ariregister_index.py --old yesterday.sqlite \\
        --new today.sqlite --history ariregister-delta.json \\
        --out ariregister-delta.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

#: The delta file's shape. Bump when a reader would misread the new one.
DELTA_SCHEMA = 1

#: Days kept in the rolling file. Two weeks covers a worker that was down
#: for a long weekend and keeps the file small enough to read hourly.
KEEP_DAYS = 14

#: A day touching more than this share of the register's companies is kept
#: as a summary without codes (see the module docstring).
SUSPECT_SHARE = 0.10
#: …and at least this many: a small register (or a test fixture) is listed.
SUSPECT_MIN = 1000

#: The people lists, in the order the feed names them. The backend reads the
#: same names (``opencheck.watchlist.ARIREGISTER_LISTS``); a test pins it.
PEOPLE_LISTS = ("officers", "shareholders", "beneficial_owners", "supervisory_board")

#: The entity row's fields that count, by the name the delta gives them.
ENTITY_FIELDS = {"name": "name", "status": "status_code", "legal_form": "legal_form"}

#: Every key a code's change can carry (``added`` / ``removed`` are flags).
CHANGE_KEYS = (*PEOPLE_LISTS, *ENTITY_FIELDS, "added", "removed")

#: Roles off the registry card that feed a list (founders, auditors and the
#: rest are not read).
_OFF_CARD_LISTS = {"S": "shareholders", "N": "supervisory_board", "E": "supervisory_board"}

Grouped = Iterator[tuple[str, str, int]]


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def _row_text(columns: list[str], row: tuple[Any, ...]) -> str:
    parts = []
    for name, value in zip(columns, row, strict=True):
        if name in ("code", "kirje_id"):
            continue
        if isinstance(value, bytes):
            value = value.hex()
        parts.append(f"{name}={'' if value is None else value}")
    return "\x1f".join(parts)


def _grouped(
    conn: sqlite3.Connection, table: str, keep: Callable[[dict[str, Any]], bool] | None = None
) -> Grouped:
    """``(code, digest, rows)`` for every code with rows in ``table``, in
    code order: the digest of the code's rows sorted, ``kirje_id`` aside."""
    try:
        columns = _columns(conn, table)
    except sqlite3.Error:
        return
    if not columns:
        return
    cur = conn.execute(f"SELECT {', '.join(columns)} FROM {table} ORDER BY code")
    code_at = columns.index("code")
    current: str | None = None
    texts: list[str] = []
    for row in cur:
        if keep is not None and not keep(dict(zip(columns, row, strict=True))):
            continue
        code = str(row[code_at])
        if code != current:
            if current is not None:
                yield current, _digest(texts), len(texts)
            current, texts = code, []
        texts.append(_row_text(columns, row))
    if current is not None:
        yield current, _digest(texts), len(texts)


def _digest(texts: list[str]) -> str:
    h = hashlib.sha256()
    for text in sorted(texts):
        h.update(text.encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()[:16]


def _merge_streams(*streams: Grouped) -> Grouped:
    """Several code-ordered streams of one list as one: the digests of a
    code's parts combined, its rows summed."""
    import heapq

    merged = heapq.merge(*streams, key=lambda item: item[0])
    current: str | None = None
    parts: list[str] = []
    rows = 0
    for code, digest, n in merged:
        if code != current:
            if current is not None:
                yield current, _digest(parts), rows
            current, parts, rows = code, [], 0
        parts.append(digest)
        rows += n
    if current is not None:
        yield current, _digest(parts), rows


def list_streams(conn: sqlite3.Connection) -> dict[str, Callable[[], Grouped]]:
    """Each people list as a code-ordered stream of ``(code, digest, rows)``."""

    def off_card(name: str) -> Callable[[dict[str, Any]], bool]:
        return lambda row: _OFF_CARD_LISTS.get(str(row.get("isiku_roll") or "")) == name

    return {
        "officers": lambda: _grouped(conn, "officer"),
        "shareholders": lambda: _merge_streams(
            _grouped(conn, "shareholder"),
            _grouped(conn, "off_card_person", off_card("shareholders")),
        ),
        "beneficial_owners": lambda: _grouped(conn, "beneficial_owner"),
        "supervisory_board": lambda: _grouped(conn, "off_card_person", off_card("supervisory_board")),
    }


def _diff_streams(old: Grouped, new: Grouped) -> Iterator[tuple[str, int, int]]:
    """``(code, rows before, rows after)`` for every code whose digest moved."""
    sentinel = ("￿", "", 0)
    a, b = next(old, sentinel), next(new, sentinel)
    while a is not sentinel or b is not sentinel:
        if a[0] == b[0]:
            if a[1] != b[1]:
                yield a[0], a[2], b[2]
            a, b = next(old, sentinel), next(new, sentinel)
        elif a[0] < b[0]:
            yield a[0], a[2], 0
            a = next(old, sentinel)
        else:
            yield b[0], 0, b[2]
            b = next(new, sentinel)


def _entities(conn: sqlite3.Connection) -> Iterator[tuple[str, dict[str, Any]]]:
    columns = ", ".join(ENTITY_FIELDS.values())
    for row in conn.execute(f"SELECT code, {columns} FROM entity ORDER BY code"):
        yield str(row[0]), dict(zip(ENTITY_FIELDS, row[1:], strict=True))


def _meta(conn: sqlite3.Connection) -> dict[str, str]:
    return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}


def diff_indexes(old_path: Path, new_path: Path) -> dict[str, Any]:
    """One day of the delta: what changed between the two builds."""
    old = sqlite3.connect(f"file:{old_path}?mode=ro", uri=True)
    new = sqlite3.connect(f"file:{new_path}?mode=ro", uri=True)
    try:
        old_meta, new_meta = _meta(old), _meta(new)
        changed: dict[str, dict[str, Any]] = {}

        # The entity rows: added, removed, and the three fields.
        sentinel: tuple[str, dict[str, Any]] = ("￿", {})
        it_a, it_b = _entities(old), _entities(new)
        a, b = next(it_a, sentinel), next(it_b, sentinel)
        while a is not sentinel or b is not sentinel:
            if a[0] == b[0]:
                for field in ENTITY_FIELDS:
                    if a[1].get(field) != b[1].get(field):
                        changed.setdefault(a[0], {})[field] = [a[1].get(field), b[1].get(field)]
                a, b = next(it_a, sentinel), next(it_b, sentinel)
            elif a[0] < b[0]:
                changed.setdefault(a[0], {})["removed"] = True
                a = next(it_a, sentinel)
            else:
                changed.setdefault(b[0], {})["added"] = True
                b = next(it_b, sentinel)

        # The people lists. A company added or removed is said once, as
        # such; its lists appearing or vanishing with it are not repeated.
        old_lists, new_lists = list_streams(old), list_streams(new)
        for name in PEOPLE_LISTS:
            for code, before, after in _diff_streams(old_lists[name](), new_lists[name]()):
                entry = changed.setdefault(code, {})
                if entry.get("added") or entry.get("removed"):
                    continue
                entry[name] = [before, after]
        changed = {code: c for code, c in changed.items() if c}

        entities = int(new_meta.get("entities") or 0) or 1
        summary = {key: sum(1 for c in changed.values() if key in c) for key in CHANGE_KEYS}
        day: dict[str, Any] = {
            "cut": new_meta.get("cut"),
            "previous_cut": old_meta.get("cut"),
            "built_at": new_meta.get("built_at"),
            "companies_changed": len(changed),
            "summary": {k: v for k, v in summary.items() if v},
        }
        if len(changed) > max(SUSPECT_SHARE * entities, SUSPECT_MIN):
            day["suspect"] = True
        else:
            day["changes"] = dict(sorted(changed.items()))
        return day
    finally:
        old.close()
        new.close()


def merge_history(history: dict[str, Any] | None, day: dict[str, Any], *, keep: int = KEEP_DAYS) -> dict[str, Any]:
    """Add ``day`` to the rolling file. A day already held (a re-run of the
    workflow on the same cut) is replaced only by a day diffed against the
    same previous cut — a same-day re-run diffs a build against itself and
    would otherwise erase what that day changed."""
    days = [d for d in (history or {}).get("days") or [] if isinstance(d, dict) and d.get("cut")]
    if day.get("cut") == day.get("previous_cut"):
        pass  # the same cut against itself: nothing that day, keep what is held
    else:
        days = [d for d in days if d.get("cut") != day.get("cut")]
        days.append(day)
    days.sort(key=lambda d: str(d.get("cut")))
    days = days[-keep:]
    return {
        "schema": DELTA_SCHEMA,
        "source": "Estonian e-Business Register open data (RIK), CC BY 4.0",
        "source_page": "https://avaandmed.ariregister.rik.ee/en/downloading-open-data",
        "generated_by": "backend/scripts/diff_ariregister_index.py",
        "updated_at": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "keep_days": keep,
        "lists": list(PEOPLE_LISTS),
        "entity_fields": list(ENTITY_FIELDS),
        "days": days,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--old", type=Path, help="the build being replaced (absent on a first run)")
    parser.add_argument("--new", type=Path, required=True, help="today's build")
    parser.add_argument("--history", type=Path, help="the delta file published so far")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    history: dict[str, Any] | None = None
    if args.history and args.history.exists():
        try:
            history = json.loads(args.history.read_text())
        except (OSError, ValueError) as exc:
            print(f"Ignoring an unreadable delta file ({exc}); starting a new one.", file=sys.stderr)
    if args.old is None or not args.old.exists():
        # A first run, or the previous build could not be fetched: nothing to
        # diff. The file still moves on, so the reader can see the gap.
        print("No previous build to diff against; the delta records none for this cut.")
        out = merge_history(history, {})
    else:
        day = diff_indexes(args.old, args.new)
        out = merge_history(history, day)
        print(
            f"cut {day['cut']} against {day['previous_cut']}: {day['companies_changed']} companies changed"
            + (" — SUSPECT, kept as a summary" if day.get("suspect") else "")
        )
        for key, n in day["summary"].items():
            print(f"  {key}: {n}")
    args.out.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
