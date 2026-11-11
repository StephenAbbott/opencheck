#!/usr/bin/env python3
"""Diff two builds of the Israel Corporations Authority index — the
register's day-to-day changes, for the watchlist's register tier.

The daily refresh (``refresh-ica-israel-index.yml``) downloads the index it is
about to replace, builds today's (carrying the change log forward with
``--previous``), and runs this script over the pair. The result is appended to
``ica-israel-delta.json``, a rolling file of the last :data:`KEEP_DAYS` days
published beside the index; the backend's watchlist worker reads it and
re-runs a watched Israeli company whose register record changed
(``opencheck/watchlist.py``, the register tier — the Estonian arrangement,
``scripts/diff_ariregister_index.py``).

What counts as a change (Stephen, 11 Nov 2026 — the recommended set):

* **a new filing in a material group** of the Authority's change log
  (``ica-changes``): shares, capital, directors, name, status, merger,
  attachment and classification (``ica_israel_changes.MATERIAL_GROUPS``).
  A filing is new when its row — number, request code, day, charge — was not
  in the build being replaced. Charges, officers, the annual report and the
  rest are not material. The change log says **that** something was filed,
  never what it changed to, so the delta carries the request code and day
  and nothing else.
* **the company's name, status, sub-status and violating mark** in the
  companies and partnerships files — not its address, type or report year.
* a corporation **added to** or **removed from** the files.

When the build being replaced holds no change log (the first run after the
change log was added), the filings are not diffed (``changes_diffed:
false``): every row would read as new. A day on which more than
:data:`SUSPECT_SHARE` of the corporations changed, or more than
:data:`SUSPECT_ROWS` filings appeared, is kept as a summary with ``suspect:
true`` and no numbers — a reloaded change log would otherwise re-run every
watched Israeli company with nothing true to say about any of them.

    uv run python scripts/diff_ica_israel_index.py --old yesterday.sqlite \\
        --new today.sqlite --history ica-israel-delta.json \\
        --out ica-israel-delta.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencheck.sources.ica_israel_changes import MATERIAL_CODES  # noqa: E402

#: The delta file's shape. Bump when a reader would misread the new one.
DELTA_SCHEMA = 1

#: Days kept in the rolling file (the Estonian file's fourteen).
KEEP_DAYS = 14

#: A day touching more than this share of the corporations is kept as a
#: summary without numbers…
SUSPECT_SHARE = 0.10
#: …and at least this many: a small register (or a test fixture) is listed.
SUSPECT_MIN = 1000
#: A day with more new material filings than this is suspect too. The log
#: carries about 1,500 rows a working day; a reloaded file carries 500,000.
SUSPECT_ROWS = 20_000

#: The entity row's fields that count, by the name the delta gives them. The
#: backend reads the same names (``opencheck.watchlist.ICA_ISRAEL_FIELDS``);
#: a test pins it.
ENTITY_FIELDS = {
    "name": "name_he",
    "status": "status_words",
    "sub_status": "sub_status",
    "violating": "violating",
}

#: Every key a number's change can carry.
CHANGE_KEYS = ("filings", *ENTITY_FIELDS, "added", "removed")


def _meta(conn: sqlite3.Connection, schema: str = "main") -> dict[str, str]:
    try:
        return {str(k): str(v) for k, v in conn.execute(f"SELECT key, value FROM {schema}.meta")}
    except sqlite3.Error:
        return {}


def _entities(conn: sqlite3.Connection, schema: str) -> Iterator[tuple[str, dict[str, Any]]]:
    columns = ", ".join(ENTITY_FIELDS.values())
    for row in conn.execute(f"SELECT number, {columns} FROM {schema}.entity ORDER BY number"):
        yield str(row[0]), dict(zip(ENTITY_FIELDS, row[1:], strict=True))


def new_filings(conn: sqlite3.Connection) -> Iterator[tuple[str, int, str]]:
    """``(number, code, day)`` for every material change-log row in ``main``
    that ``old`` did not hold, in number order. Both builds are attached."""
    rows = conn.execute(
        """
        SELECT n.number, n.code, n.day FROM main.change n
        WHERE NOT EXISTS (
          SELECT 1 FROM old.change o
          WHERE o.number = n.number AND o.code = n.code AND o.day = n.day AND o.charge_id = n.charge_id
        )
        ORDER BY n.number, n.day, n.code
        """
    )
    for number, code, day in rows:
        if int(code) in MATERIAL_CODES:
            yield str(number), int(code), str(day)


def diff_indexes(old_path: Path, new_path: Path) -> dict[str, Any]:
    """One day of the delta: what changed between the two builds."""
    conn = sqlite3.connect(f"file:{new_path}?mode=ro", uri=True)
    try:
        conn.execute("ATTACH DATABASE ? AS old", (f"file:{old_path}?mode=ro",))
        old_meta, new_meta = _meta(conn, "old"), _meta(conn, "main")
        changed: dict[str, dict[str, Any]] = {}

        sentinel: tuple[str, dict[str, Any]] = ("￿", {})
        it_a, it_b = _entities(conn, "old"), _entities(conn, "main")
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

        # The change log: only when both builds hold one. The first build
        # with a log would otherwise read every row of the year as new.
        filings_diffed = bool(old_meta.get("changes_cut") and new_meta.get("changes_cut"))
        new_rows = 0
        if filings_diffed:
            for number, code, day in new_filings(conn):
                changed.setdefault(number, {}).setdefault("filings", []).append([code, day])
                new_rows += 1
        changed = {number: c for number, c in changed.items() if c}

        entities = int(conn.execute("SELECT COUNT(*) FROM main.entity").fetchone()[0]) or 1
        summary = {key: sum(1 for c in changed.values() if key in c) for key in CHANGE_KEYS}
        day: dict[str, Any] = {
            "cut": new_meta.get("cut"),
            "previous_cut": old_meta.get("cut"),
            "changes_cut": new_meta.get("changes_cut"),
            "built_at": new_meta.get("built_at"),
            "filings_diffed": filings_diffed,
            "new_filings": new_rows,
            "companies_changed": len(changed),
            "summary": {k: v for k, v in summary.items() if v},
        }
        if len(changed) > max(SUSPECT_SHARE * entities, SUSPECT_MIN) or new_rows > SUSPECT_ROWS:
            day["suspect"] = True
        else:
            day["changes"] = dict(sorted(changed.items()))
        return day
    finally:
        conn.close()


def merge_history(history: dict[str, Any] | None, day: dict[str, Any], *, keep: int = KEEP_DAYS) -> dict[str, Any]:
    """Add ``day`` to the rolling file — ``diff_ariregister_index``'s rule: a
    day already held is replaced only by a day diffed against another cut."""
    days = [d for d in (history or {}).get("days") or [] if isinstance(d, dict) and d.get("cut")]
    if day.get("cut") and day.get("cut") != day.get("previous_cut"):
        days = [d for d in days if d.get("cut") != day.get("cut")]
        days.append(day)
    days.sort(key=lambda d: str(d.get("cut")))
    days = days[-keep:]
    return {
        "schema": DELTA_SCHEMA,
        "source": "Israel Corporations Authority open data on data.gov.il (ica_companies, ica_partnerships, ica-changes)",
        "source_page": "https://data.gov.il/dataset/ica-changes",
        "generated_by": "backend/scripts/diff_ica_israel_index.py",
        "updated_at": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "keep_days": keep,
        "entity_fields": list(ENTITY_FIELDS),
        "material_codes": sorted(MATERIAL_CODES),
        "days": days,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
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
        print("No previous build to diff against; the delta records none for this cut.")
        out = merge_history(history, {})
    else:
        day = diff_indexes(args.old, args.new)
        out = merge_history(history, day)
        print(
            f"cut {day['cut']} against {day['previous_cut']}: {day['companies_changed']} corporations changed, "
            f"{day['new_filings']} new material filings"
            + ("" if day["filings_diffed"] else " (change log not diffed: the previous build held none)")
            + (" — SUSPECT, kept as a summary" if day.get("suspect") else "")
        )
        for key, n in day["summary"].items():
            print(f"  {key}: {n}")
    args.out.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
