"""Build the Slovenian Business Register (PRS) SQLite index from AJPES's open CSV.

AJPES publishes the Slovenian Business Register's basic data as one CSV on
podatki.gov.si (``opsiprs.csv``, CC BY 4.0, fortnightly). The portal answers
HTTP 407 to every client that is not a browser — datacentres and ``curl`` on
a home connection alike (12 Nov 2026) — so the file is saved from a browser
and this builder reads it from disk. ``scripts/refresh_si_prs_index.sh``
wraps it and uploads the ``si-prs-index`` release asset. Standard library
only: no venv needed.

Usage
-----
    python3 scripts/build_si_prs_index.py --csv ~/Downloads/opsiprs.csv \\
        --cut 2026-11-01 --out si_prs.sqlite --gzip

``--cut`` is the day the portal last changed the file: the "last modified"
date on the dataset page. It is the register's clock (Phase 314); the build
time is recorded separately as ``built_at``.

The format (measured 12 Nov 2026)
---------------------------------
128,455,276 bytes, **UTF-16LE with a BOM**, CRLF, every field quoted, twelve
columns (``HEADER`` — the published names carry stray spaces, compared here
with whitespace collapsed). No status, no dates, no people.

What is kept
------------
* Every row whose number is ten digits (others are counted and skipped; the
  builder refuses past ``MAX_MALFORMED``). A repeated number keeps its first
  row and is counted (refused past ``MAX_REPEATED``).
* ``HSEID`` (a house identifier) is never stored.
* A **natural person's** street, house number and suffix are never stored:
  the settlement, postcode and post office are. Which rows are natural
  persons is decided by the adapter's ``form_is_legal_person`` — a form with
  no legal-person marker counts as a natural person's. Every form is logged
  with its count and reading, so an unforeseen one is seen on the first run.

Refusals (exit 2, nothing written): a file that is not UTF-16 (the portal's
407 page is HTML), a changed header, fewer than ``MIN_ROWS`` rows, too many
malformed or repeated numbers, a malformed ``--cut``.
"""

from __future__ import annotations

import argparse
import codecs
import csv
import gzip
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path

# Standard library only, so the refresh runs under a bare ``python3`` on a
# Mac. These three are copies of ``opencheck.sources.si_prs``'s, pinned equal
# by ``tests/test_si_prs.py`` — the index and its reader cannot disagree on
# which rows are natural persons.
INDEX_SCHEMA_VERSION = "1"
CSV_URL = (
    "https://podatki.gov.si/dataset/9ee1a9aa-c224-4995-b2ad-3760d7af0748/"
    "resource/beb70929-3d0d-41c6-9af2-25d525d906d3/download/opsiprs.csv"
)
_NATURAL_PERSON_MARKERS = re.compile(
    r"(\bs\.\s?p\.|posameznik|sobodajal|\bkmet\b|zasebnik|fizičn|dopolnilne? dejavnost|na kmetiji)",
    re.IGNORECASE,
)
_LEGAL_PERSON_MARKERS = re.compile(
    r"(d\.\s?o\.\s?o\.|\bd\.\s?d\.|\bk\.\s?d\.|d\.\s?n\.\s?o\.|z\.\s?o\.\s?o\.|z\.\s?b\.\s?o\."
    r"|družba|zadrug|zavod|društv|ustanov|podružnic|združenj|zvez|zbornic|sindikat"
    r"|strank|skupnost|agencij|sklad|občin|ministrstv|uprav|organ|\bjavn|evropsk|gospodarsk)",
    re.IGNORECASE,
)


def form_is_legal_person(form: str | None) -> bool:
    """A form with no legal-person marker is read as a natural person's."""
    text = form or ""
    if _NATURAL_PERSON_MARKERS.search(text):
        return False
    return bool(_LEGAL_PERSON_MARKERS.search(text))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_si_prs_index")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

#: The published header, whitespace collapsed.
HEADER = [
    "Matična številka", "Popolno ime", "HSEID", "Pravnoorganizacijska oblika",
    "Registrski organ", "Ulica", "Hišna št", "Hišna št dodatek", "Naselje",
    "Poštna št", "Pošta", "Država",
]

#: Below this the file is not the register (~295,000 rows in Nov 2026).
MIN_ROWS = 200_000
MAX_MALFORMED = 500
MAX_REPEATED = 500

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE entity (
  number TEXT PRIMARY KEY, name TEXT, form TEXT, organ TEXT,
  street TEXT, house TEXT, house_suffix TEXT,
  settlement TEXT, postcode TEXT, post_office TEXT, country TEXT,
  legal_person INTEGER NOT NULL
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
INDEXES = "CREATE INDEX ix_entity_name ON entity(legal_person, name);"


class RefusedInput(Exception):
    """The input is not the register as this builder knows it."""


def _clean(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def read_rows(path: Path) -> Iterator[list[str]]:
    """Yield the CSV's rows after the header; refuse anything that is not the
    UTF-16 file with the expected header."""
    with open(path, "rb") as fh:
        head = fh.read(2)
    if head != codecs.BOM_UTF16_LE:
        raise RefusedInput(
            f"{path.name}: not UTF-16LE with a BOM — a portal error page? "
            "(save the file from a browser)"
        )
    with open(path, encoding="utf-16", newline="") as fh:
        reader = csv.reader(fh)
        header = [_clean(cell) for cell in next(reader, [])]
        if header != HEADER:
            raise RefusedInput(f"{path.name}: header {header!r} is not {HEADER!r}")
        yield from reader


def build(
    rows: Iterable[list[str]], out: Path, cut: str, *, min_rows: int | None = None
) -> dict[str, int]:
    """Build the index from data rows (header removed). ``min_rows``
    overrides ``MIN_ROWS`` (tests pass 0)."""
    floor = MIN_ROWS if min_rows is None else min_rows
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cut or ""):
        raise RefusedInput(f"cut {cut!r} is not YYYY-MM-DD")
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    try:
        con.executescript(SCHEMA)
        stats = Counter(rows=0, malformed=0, repeated=0, legal_persons=0, natural_persons=0)
        forms: Counter[str] = Counter()
        seen: set[str] = set()
        batch: list[tuple] = []
        for row in rows:
            if len(row) != len(HEADER):
                stats["malformed"] += 1
                continue
            (number, name, _hseid, form, organ, street, house, suffix,
             settlement, postcode, post_office, country) = (_clean(c) for c in row)
            if not re.fullmatch(r"\d{10}", number):
                stats["malformed"] += 1
                continue
            if number in seen:
                stats["repeated"] += 1
                continue
            seen.add(number)
            legal = form_is_legal_person(form)
            forms[form] += 1
            stats["rows"] += 1
            stats["legal_persons" if legal else "natural_persons"] += 1
            batch.append((
                number, name or None, form or None, organ or None,
                (street or None) if legal else None,
                (house or None) if legal else None,
                (suffix or None) if legal else None,
                settlement or None, postcode or None, post_office or None,
                country or None, 1 if legal else 0,
            ))
            if len(batch) >= 50_000:
                con.executemany("INSERT INTO entity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                batch.clear()
        con.executemany("INSERT INTO entity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)

        if stats["rows"] < floor:
            raise RefusedInput(f"only {stats['rows']:,} rows — not the register")
        if stats["malformed"] > MAX_MALFORMED:
            raise RefusedInput(f"{stats['malformed']:,} malformed rows — the format has changed")
        if stats["repeated"] > MAX_REPEATED:
            raise RefusedInput(f"{stats['repeated']:,} repeated numbers — the format has changed")

        for form, count in forms.most_common():
            log.info("  form %-70s %8s  %s", form or "(blank)", f"{count:,}",
                     "legal person" if form_is_legal_person(form) else "natural person / unrecognised")

        con.commit()
        con.executescript(INDEXES)
        con.executemany(
            "INSERT OR REPLACE INTO meta VALUES (?,?)",
            [
                ("schema_version", INDEX_SCHEMA_VERSION),
                ("source", "AJPES Slovenian Business Register open file (opsiprs.csv)"),
                ("source_url", CSV_URL),
                ("cut", cut),
                ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
                ("licence", "CC-BY-4.0 — AJPES"),
                *[(f"count_{k}", str(v)) for k, v in stats.items()],
            ],
        )
        con.commit()
        con.execute("VACUUM")
        con.close()
    except BaseException:
        con.close()
        if tmp.exists():
            tmp.unlink()
        raise
    os.replace(tmp, out)
    result = dict(stats)
    result["seconds"] = int(time.time() - t0)
    result["megabytes"] = int(os.path.getsize(out) / 1e6)
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True, type=Path, help="opsiprs.csv, saved from a browser")
    ap.add_argument("--cut", required=True, help="the portal's last-modified day, YYYY-MM-DD")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)

    try:
        stats = build(read_rows(args.csv), args.out, args.cut)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], args.cut)
    for key, value in stats.items():
        log.info("  %-18s %s", key, f"{value:,}")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
