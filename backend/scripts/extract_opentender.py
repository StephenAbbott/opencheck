#!/usr/bin/env python3
"""Build the opentender.db SQLite index from OpenTender (DIGIWHIST) NDJSON data.

OpenTender publishes procurement data from 35 jurisdictions as per-country
NDJSON.gz archives at:
  https://opentender.eu/all/download

Each file is named ``data-<country_code>-ndjson.zip`` (e.g. ``data-uk-ndjson.zip``)
and contains one or more ``.ndjson.gz`` files — one JSON object (tender) per line.

This script ingests one or more such archives (or extracted ``.ndjson`` / ``.ndjson.gz``
files) and builds a SQLite database with:

  tenders            — full tender JSON blobs, keyed by persistentId
  body_names_fts     — FTS5 virtual table for buyer/bidder name search
  body_ids           — flat identifier index (VAT, CH number, etc.)

Usage
-----
  # From per-country zip archives (can mix multiple countries):
  python scripts/extract_opentender.py \\
      --input data-uk-ndjson.zip data-de-ndjson.zip \\
      --output /path/to/opentender.db

  # From already-extracted NDJSON or NDJSON.gz files:
  python scripts/extract_opentender.py \\
      --input /path/to/data-uk-0.ndjson.gz /path/to/data-de-0.ndjson.gz \\
      --output /path/to/opentender.db

  # Stream a single country from a zip to stdout-count only (dry-run):
  python scripts/extract_opentender.py --input data-uk-ndjson.zip --output :memory: --dry-run

Notes
-----
  - No extra dependencies beyond the standard library; json, gzip, zipfile are used.
  - Processing the full UK NDJSON (~150 k records) takes ~2 min and produces
    a ~400 MB SQLite file; processing all 35 countries will be several GB.
  - Run with OPENTENDER_DB_FILE=/path/to/opentender.db in your .env to activate
    the adapter, or set OPENTENDER_S3_URL to have the adapter download it at startup.

License note
------------
OpenTender data is published under CC BY-NC-SA 4.0. Your use of any database
built by this script (and any exports derived from it) must comply with that
licence — non-commercial, share-alike.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import logging
import re
import sqlite3
import sys
import zipfile
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# SQLite schema
# ------------------------------------------------------------------

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=OFF;

CREATE TABLE IF NOT EXISTS tenders (
    persistent_id       TEXT PRIMARY KEY,
    source_id           TEXT,
    country             TEXT NOT NULL,
    title               TEXT,
    is_awarded          INTEGER DEFAULT 0,
    award_date          TEXT,
    integrity_score     REAL,
    transparency_score  REAL,
    data                TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS body_names_fts
USING fts5(
    persistent_id UNINDEXED,
    name,
    role,
    tokenize = "unicode61 remove_diacritics 1"
);

CREATE TABLE IF NOT EXISTS body_ids (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    persistent_id TEXT NOT NULL,
    id_type       TEXT,
    id_scope      TEXT,
    id_value      TEXT
);

CREATE INDEX IF NOT EXISTS idx_body_ids_lookup
    ON body_ids (id_type, id_value);

CREATE INDEX IF NOT EXISTS idx_tenders_country
    ON tenders (country);
"""

# ------------------------------------------------------------------
# Country-code normalisation
# ------------------------------------------------------------------

# DIGIWHIST uses "UK" internally; ISO 3166-1 (and BODS) use "GB".
_COUNTRY_NORM: dict[str, str] = {"UK": "GB"}


def _norm_country(code: str) -> str:
    return _COUNTRY_NORM.get((code or "").upper(), (code or "").upper())


# ------------------------------------------------------------------
# Body walking
# ------------------------------------------------------------------

def _walk_bodies(tender: dict) -> Iterator[tuple[str, dict]]:
    """Yield (role, body) pairs for every body referenced in a tender."""
    for body in tender.get("buyers") or []:
        if isinstance(body, dict):
            yield "buyer", body
    for body in tender.get("onBehalfOf") or []:
        if isinstance(body, dict):
            yield "onBehalfOf", body
    for lot in tender.get("lots") or []:
        for bid in lot.get("bids") or []:
            role = "winner" if bid.get("isWinning") else "bidder"
            for body in bid.get("bidders") or []:
                if isinstance(body, dict):
                    yield role, body
            for body in bid.get("subcontractors") or []:
                if isinstance(body, dict):
                    yield "subcontractor", body


# ------------------------------------------------------------------
# NDJSON / archive streaming
# ------------------------------------------------------------------

def _iter_ndjson_gz(fh: io.IOBase) -> Iterator[dict]:
    """Yield parsed JSON objects from an open NDJSON.gz or NDJSON binary stream."""
    # Peek at the first two bytes to detect gzip magic.
    first = fh.read(2)
    if first == b"\x1f\x8b":
        # Gzip-compressed — wrap the rest.
        remaining = fh.read()
        data = gzip.decompress(first + remaining)
        lines = data.splitlines()
    else:
        remaining = fh.read()
        lines = (first + remaining).splitlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as exc:
            logger.debug("JSON decode error (skipping line): %s", exc)


def _iter_source(path: Path) -> Iterator[dict]:
    """Yield tender dicts from a path.

    Supports:
    - .zip containing .ndjson.gz or .ndjson files
    - .ndjson.gz
    - .ndjson
    """
    suffix = path.suffix.lower()

    if suffix == ".zip":
        with zipfile.ZipFile(path) as zf:
            for name in sorted(zf.namelist()):
                name_lower = name.lower()
                if name_lower.endswith(".ndjson.gz") or name_lower.endswith(".ndjson"):
                    logger.info("  → reading %s from %s", name, path.name)
                    with zf.open(name) as member:
                        yield from _iter_ndjson_gz(member)
    elif suffix == ".gz" or path.name.lower().endswith(".ndjson.gz"):
        with open(path, "rb") as fh:
            yield from _iter_ndjson_gz(fh)
    else:
        # Assume plain NDJSON.
        with open(path, "rb") as fh:
            yield from _iter_ndjson_gz(fh)


# ------------------------------------------------------------------
# Date filtering
# ------------------------------------------------------------------

_YEAR_RE = re.compile(r"^(\d{4})")


def _tender_year(tender: dict) -> int | None:
    """Return the most recent year found across all date fields, or None if undated.

    We take the LATEST year across every date field so that a tender is only
    excluded when ALL of its dates pre-date from_year. Using the latest year
    also means a re-published or re-awarded tender isn't accidentally dropped.

    Fields checked (DIGIWHIST PPDS):
      - awardDecisionDate          (tender level)
      - contractSignatureDate      (tender level)
      - publicationDate            (tender level — not always populated)
      - publications[].publicationDate  (per-notice date, most reliably present)
      - lots[].awardDecisionDate
      - lots[].contractSignatureDate

    Returns None when no date can be found — callers treat None as
    "include" so undated records are never silently dropped.
    """
    candidate_years: list[int] = []

    def _extract(date_str: object) -> None:
        if date_str:
            m = _YEAR_RE.match(str(date_str))
            if m:
                candidate_years.append(int(m.group(1)))

    _extract(tender.get("awardDecisionDate"))
    _extract(tender.get("contractSignatureDate"))
    _extract(tender.get("publicationDate"))

    for pub in tender.get("publications") or []:
        if isinstance(pub, dict):
            _extract(pub.get("publicationDate"))

    for lot in tender.get("lots") or []:
        if isinstance(lot, dict):
            _extract(lot.get("awardDecisionDate"))
            _extract(lot.get("contractSignatureDate"))

    return max(candidate_years) if candidate_years else None


# ------------------------------------------------------------------
# Insertion helpers
# ------------------------------------------------------------------

_LEI_RE = re.compile(r"^[A-Z0-9]{20}$")


def _classify_id(id_type: str, id_scope: str, id_value: str) -> tuple[str, str] | None:
    """Return (normalised_type, normalised_value) for strong-bridge identifiers, or None."""
    t = id_type.upper()
    s = id_scope.upper()
    v = id_value.strip()

    # LEI shape detection.
    if _LEI_RE.match(v.upper()):
        return "lei", v.upper()

    if t == "VAT":
        return "vat", v

    if t == "ORGANIZATION_ID" and s in {"GB", "UNKNOWN"}:
        # DIGIWHIST may strip leading zeros from CH numbers.
        clean = v.zfill(8) if v.isdigit() and len(v) < 8 else v
        return "gb_coh", clean

    if t in {"HEADER_ICO", "STATISTICAL", "TAX_ID", "TRADE_REGISTER"}:
        return "registration_number", v

    if t == "BVD_ID":
        return "bvd_id", v

    return None


def _insert_tender(
    cur: sqlite3.Cursor,
    tender: dict,
    *,
    from_year: int | None = None,
    dry_run: bool = False,
) -> bool:
    """Insert one tender into all tables. Returns True if inserted/updated."""
    persistent_id = (tender.get("persistentId") or tender.get("id") or "").strip()
    if not persistent_id:
        return False

    # Date filter: skip tenders whose year is known and before from_year.
    # Undated tenders (year=None) are always included.
    if from_year is not None:
        year = _tender_year(tender)
        if year is not None and year < from_year:
            return False

    country_raw = (tender.get("country") or "").upper()
    country = _norm_country(country_raw)

    title = tender.get("title") or tender.get("titleEnglish") or ""
    is_awarded = 1 if tender.get("isAwarded") else 0
    award_date = tender.get("awardDecisionDate") or None

    # DIGIWHIST integrity/transparency composite scores.
    scores = tender.get("ot") or {}
    integrity_score = scores.get("integrity")
    transparency_score = scores.get("transparency")

    data_json = json.dumps(tender, ensure_ascii=False)

    if dry_run:
        return True

    cur.execute(
        """
        INSERT OR REPLACE INTO tenders
          (persistent_id, source_id, country, title, is_awarded, award_date,
           integrity_score, transparency_score, data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            persistent_id, tender.get("id"), country, title, is_awarded,
            award_date, integrity_score, transparency_score, data_json,
        ),
    )

    # Remove stale FTS and body_ids rows (idempotent re-ingestion).
    cur.execute("DELETE FROM body_names_fts WHERE persistent_id = ?", (persistent_id,))
    cur.execute("DELETE FROM body_ids WHERE persistent_id = ?", (persistent_id,))

    seen_names: set[str] = set()
    for role, body in _walk_bodies(tender):
        name = (body.get("name") or "").strip()
        if name and name not in seen_names:
            cur.execute(
                "INSERT INTO body_names_fts (persistent_id, name, role) VALUES (?, ?, ?)",
                (persistent_id, name, role),
            )
            seen_names.add(name)

        for ident in body.get("bodyIds") or []:
            id_type = str(ident.get("type") or "")
            id_scope = str(ident.get("scope") or "")
            id_value = str(ident.get("id") or "").strip()
            if not id_value:
                continue
            cur.execute(
                "INSERT INTO body_ids (persistent_id, id_type, id_scope, id_value) "
                "VALUES (?, ?, ?, ?)",
                (persistent_id, id_type, id_scope, id_value),
            )

    return True


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def build(
    inputs: list[Path],
    output: Path | str,
    *,
    from_year: int | None = None,
    dry_run: bool = False,
    batch_size: int = 500,
) -> int:
    """Build (or update) the SQLite database. Returns the number of tenders processed."""
    if dry_run:
        logger.info("Dry-run mode — no database writes.")
        conn = sqlite3.connect(":memory:")
    elif str(output) == ":memory:":
        conn = sqlite3.connect(":memory:")
    else:
        conn = sqlite3.connect(str(output))

    conn.executescript(_DDL)
    cur = conn.cursor()

    total = 0
    errors = 0

    for path in inputs:
        logger.info("Processing %s …", path)
        batch_count = 0

        for tender in _iter_source(path):
            try:
                inserted = _insert_tender(cur, tender, from_year=from_year, dry_run=dry_run)
                if inserted:
                    total += 1
                    batch_count += 1
                    if batch_count % batch_size == 0 and not dry_run:
                        conn.commit()
                        logger.info("  … %d tenders committed", total)
            except Exception as exc:
                errors += 1
                logger.warning("Error inserting tender (skipping): %s", exc)
                if errors > 1000:
                    logger.error("Too many errors (%d), aborting.", errors)
                    break

        if not dry_run:
            conn.commit()
        logger.info("  done — %d tenders from %s", batch_count, path.name)

    if not dry_run and str(output) != ":memory:":
        logger.info("Running ANALYZE …")
        conn.execute("ANALYZE")
        conn.commit()
        conn.close()

    logger.info("Total: %d tenders processed, %d errors.", total, errors)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build opentender.db from OpenTender NDJSON archives.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input", "-i",
        nargs="+",
        required=True,
        metavar="FILE",
        help=(
            "One or more input files: .zip (per-country NDJSON archive), "
            ".ndjson.gz, or .ndjson."
        ),
    )
    parser.add_argument(
        "--output", "-o",
        default="opentender.db",
        metavar="DB",
        help="Output SQLite path (default: opentender.db).",
    )
    parser.add_argument(
        "--from-year",
        type=int,
        default=2024,
        metavar="YEAR",
        help=(
            "Only include tenders with an award or publication date >= YEAR "
            "(default: 2024). Undated tenders are always included. "
            "Pass 0 to disable the filter and include all years."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse inputs and count records without writing to the database.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Commit to SQLite after every N tenders (default: 500).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    inputs = [Path(p) for p in args.input]
    missing = [p for p in inputs if not p.exists()]
    if missing:
        for p in missing:
            logger.error("Input file not found: %s", p)
        sys.exit(1)

    from_year = args.from_year if args.from_year > 0 else None
    if from_year:
        logger.info("Date filter: including tenders from %d onwards.", from_year)
    else:
        logger.info("Date filter: disabled — including all years.")

    total = build(
        inputs,
        output=args.output,
        from_year=from_year,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )

    if not args.dry_run:
        size_mb = Path(args.output).stat().st_size / 1_000_000 if Path(args.output).exists() else 0
        logger.info(
            "Database written to %s (%.1f MB, %d tenders)",
            args.output, size_mb, total,
        )
        logger.info(
            "Set OPENTENDER_DB_FILE=%s in your .env to activate the adapter.",
            args.output,
        )


if __name__ == "__main__":
    main()
