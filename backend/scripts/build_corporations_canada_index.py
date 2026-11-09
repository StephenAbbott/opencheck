"""Build the Corporations Canada SQLite index from the Federal Corporations CSVs.

ISED publishes the federal register as open data on open.canada.ca
(`Federal Corporations`, OGL-Canada, CKAN ``frequency: P1D``): eight CSVs on
CloudFront, English and French × active / inactive-or-dissolved × CBCA /
other Acts. This builder reads the four **English** files — the French ones
hold the same rows with French headers — and writes one SQLite index keyed on
the corporation number, with an FTS5 table over both name forms and a
partial index on the Business Number. ``.github/workflows/refresh-corporations-canada-index.yml``
runs it daily and ships the result as the ``corporations-canada-index``
release asset, which the backend downloads at boot (``warm_index``).

Usage
-----
    python3 scripts/build_corporations_canada_index.py --download --out corporations_canada.sqlite --gzip
    python3 scripts/build_corporations_canada_index.py --dir ./csv --cut 2026-11-08 --out corporations_canada.sqlite

``--download`` fetches the four files (no key, no login; CloudFront answers a
datacentre address) and takes the cut from their ``Last-Modified`` headers;
given files need ``--cut``.

What the files hold (8 Nov 2026, measured)
-----------------------------------------
* 1,578,206 rows: active CBCA 645,677, active other 51,348, inactive-or-
  dissolved CBCA 838,289, inactive-or-dissolved other 42,892. **Every
  corporation number is unique across the four files**, 4–8 digits, check
  digit included, never a leading zero.
* UTF-8 with a BOM; no field spans lines; every date ISO; Country ``CA`` on
  every row.
* Business Number: nine digits on 1,487,279 rows, empty on 90,926, ``0`` on
  one. 16,351 BNs are shared by several corporations (amalgamations and
  continuances keep it); in every such group at most one is active.
* No dissolution date, no amalgamation successor, no name history, no
  directors — the ISED API carries those (see the adapter).

The builder refuses — and writes nothing — when a header is not the one
below, a file is short (``MIN_ROWS``), a corporation number is repeated or
malformed past ``MAX_MALFORMED``, or the files' ``Last-Modified`` days are
more than a day apart (a download that straddled a regeneration).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import urllib.request
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.corporations_canada import (  # noqa: E402
    CSV_BASE,
    CSV_FILES,
    INDEX_COLUMNS,
    INDEX_SCHEMA_VERSION,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_corporations_canada_index")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"

#: The header every English file carries — a changed header is a changed
#: format, so refuse.
HEADER: list[str] = [
    "Corporation number", "Business number (BN)", "Corporate name - form 1",
    "Corporate name - form 2", "Governing legislation", "Status", "Status Detail",
    "Anniversary date", "Year of last annual filing", "Date of last annual meeting",
    "Street", "Street 2", "City/town", "Province/territory", "Country",
    "Postal code", "Minimum number of directors", "Maximum number of directors",
]

#: Below these a file is not the register (an error page, a cut file). About
#: 80% of the 8 Nov 2026 counts.
MIN_ROWS: dict[str, int] = {
    "active_cbca": 500_000,
    "active_other": 40_000,
    "inactive_cbca": 650_000,
    "inactive_other": 33_000,
}
#: Malformed or repeated corporation numbers above which the format changed.
MAX_MALFORMED = 200

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE corp (
  id INTEGER PRIMARY KEY, bn TEXT, name TEXT, name_other TEXT, act TEXT,
  status TEXT, detail TEXT, anniversary TEXT, last_filing_year TEXT,
  last_agm TEXT, street TEXT, street2 TEXT, city TEXT, province TEXT,
  country TEXT, postal_code TEXT, directors_min INTEGER, directors_max INTEGER
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
INDEXES = """
CREATE INDEX ix_corp_bn ON corp(bn) WHERE bn IS NOT NULL;
CREATE VIRTUAL TABLE corp_fts USING fts5(
  name, name_other, content='corp', content_rowid='id',
  tokenize='unicode61 remove_diacritics 2'
);
INSERT INTO corp_fts(rowid, name, name_other) SELECT id, name, name_other FROM corp;
"""

assert len(INDEX_COLUMNS) == 18


class RefusedInput(Exception):
    """The input is not the register as this builder knows it."""


def _clean(value: str | None) -> str | None:
    text = re.sub(r"\s+", " ", (value or "")).strip()
    return text or None


def _int(value: str | None) -> int | None:
    text = (value or "").strip()
    return int(text) if text.isdigit() else None


def _day(value: str | None) -> str | None:
    text = (value or "").strip()
    return text if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) else None


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download(work: Path) -> tuple[dict[str, Path], str, dict[str, str]]:
    """Fetch the four English files; return their paths, the cut (the newest
    ``Last-Modified`` day) and each file's ``Last-Modified``."""
    paths: dict[str, Path] = {}
    modified: dict[str, datetime] = {}
    for short, name in CSV_FILES.items():
        url = f"{CSV_BASE}/{name}"
        dest = work / name
        log.info("downloading %s", url)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=300) as resp, open(dest, "wb") as fh:
            shutil.copyfileobj(resp, fh, 1 << 20)
            lm = resp.headers.get("Last-Modified")
        if lm:
            try:
                modified[short] = parsedate_to_datetime(lm).astimezone(UTC)
            except (TypeError, ValueError):
                pass
        paths[short] = dest
    if len(modified) != len(CSV_FILES):
        raise RefusedInput("a download carried no Last-Modified header — pass --cut")
    return paths, cut_of(modified), {k: v.isoformat() for k, v in modified.items()}


def cut_of(modified: dict[str, datetime]) -> str:
    """The newest day among the files; refuse files more than a day apart."""
    newest, oldest = max(modified.values()), min(modified.values())
    if (newest - oldest).total_seconds() > 86_400:
        raise RefusedInput(
            f"the files were modified {oldest:%Y-%m-%d %H:%M} to {newest:%Y-%m-%d %H:%M} "
            "— more than a day apart, so they are not one cut"
        )
    return newest.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _rows(path: Path, stats: dict[str, int], seen: set[int]):
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header != HEADER:
            raise RefusedInput(f"{path.name}: header {header!r} is not {HEADER!r}")
        for row in reader:
            if len(row) != len(HEADER):
                stats["malformed"] += 1
                continue
            number = row[0].strip()
            if not number.isdigit() or number.startswith("0"):
                stats["malformed"] += 1
                continue
            corp_id = int(number)
            if corp_id in seen:
                stats["repeated"] += 1
                continue
            seen.add(corp_id)
            bn = row[1].strip()
            yield (
                corp_id,
                bn if re.fullmatch(r"\d{9}", bn) else None,
                _clean(row[2]), _clean(row[3]), _clean(row[4]), _clean(row[5]),
                _clean(row[6]), _day(row[7]), _clean(row[8]), _day(row[9]),
                _clean(row[10]), _clean(row[11]), _clean(row[12]), _clean(row[13]),
                _clean(row[14]), _clean(row[15]), _int(row[16]), _int(row[17]),
            )


def build(
    paths: dict[str, Path],
    out: Path,
    cut: str,
    *,
    last_modified: dict[str, str] | None = None,
    min_rows: dict[str, int] | None = None,
) -> dict[str, int]:
    """Build the index; ``min_rows`` overrides ``MIN_ROWS`` (tests pass ``{}``)."""
    floors = MIN_ROWS if min_rows is None else min_rows
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cut):
        raise RefusedInput(f"cut {cut!r} is not YYYY-MM-DD")
    missing = [s for s in CSV_FILES if s not in paths]
    if missing:
        raise RefusedInput(f"no file for {missing}")
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    con.executescript(SCHEMA)
    stats: dict[str, int] = {"malformed": 0, "repeated": 0}
    seen: set[int] = set()
    insert = f"INSERT INTO corp VALUES ({','.join('?' * len(INDEX_COLUMNS))})"
    try:
        for short in CSV_FILES:
            before = len(seen)
            batch: list[tuple] = []
            for record in _rows(paths[short], stats, seen):
                batch.append(record)
                if len(batch) >= 50_000:
                    con.executemany(insert, batch)
                    batch.clear()
            con.executemany(insert, batch)
            stats[short] = len(seen) - before
            log.info("%s: %s rows, %.0fs", short, f"{stats[short]:,}", time.time() - t0)
            if stats[short] < floors.get(short, 0):
                raise RefusedInput(f"{short}: only {stats[short]:,} rows — not the register")
        if stats["malformed"] + stats["repeated"] > MAX_MALFORMED:
            raise RefusedInput(
                f"{stats['malformed']:,} malformed and {stats['repeated']:,} repeated "
                "rows — the format has changed"
            )
    except RefusedInput:
        con.close()
        tmp.unlink()
        raise

    con.commit()
    con.executescript(INDEXES)
    stats["corporations"] = len(seen)
    stats["with_bn"] = con.execute("SELECT COUNT(*) FROM corp WHERE bn IS NOT NULL").fetchone()[0]
    meta = [
        ("schema_version", INDEX_SCHEMA_VERSION),
        ("source", "Corporations Canada — Federal Corporations open data (four English CSVs)"),
        ("source_files", ", ".join(paths[s].name for s in CSV_FILES)),
        ("cut", cut),
        ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        ("licence", "OGL-Canada-2.0 — Corporations Canada, ISED"),
        *[(f"count_{k}", str(v)) for k, v in stats.items()],
        *[(f"last_modified_{k}", v) for k, v in (last_modified or {}).items()],
    ]
    con.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)", meta)
    con.commit()
    con.execute("VACUUM")
    con.close()
    os.replace(tmp, out)
    stats["seconds"] = int(time.time() - t0)
    stats["megabytes"] = int(os.path.getsize(out) / 1e6)
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="fetch the four English CSVs")
    ap.add_argument("--work", type=Path, help="where --download puts the files (default: beside --out)")
    ap.add_argument("--dir", type=Path, help="a folder holding the four English CSVs, as published")
    ap.add_argument("--cut", help="the files' Last-Modified day, YYYY-MM-DD (required with --dir)")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)

    try:
        last_modified: dict[str, str] | None = None
        if args.download:
            work = args.work or args.out.parent
            work.mkdir(parents=True, exist_ok=True)
            paths, cut, last_modified = download(work)
        else:
            if not (args.dir and args.cut):
                ap.error("--dir and --cut, or --download")
            paths = {short: args.dir / name for short, name in CSV_FILES.items()}
            cut = args.cut
        stats = build(paths, args.out, cut, last_modified=last_modified)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], cut)
    for key, value in stats.items():
        log.info("  %-16s %s", key, f"{value:,}")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
