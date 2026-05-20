#!/usr/bin/env python3
"""Build the bce.db SQLite index from Belgian BCE/KBO open data.

Downloads and extracts (or accepts a pre-downloaded) the KBO public data ZIP
published at:
  https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/

and builds a SQLite database indexed by ``enterprise_number`` (10 raw digits,
no dots — e.g. "0433795975" for enterprise "0433.795.975").

Files consumed from the ZIP:
  enterprise.csv      — status, juridical form, start date          (~1.5 M rows)
  denomination.csv    — NL / FR / DE names, official + commercial   (~3.8 M rows)
  address.csv         — registered-office and branch addresses       (~2.3 M rows)
  meta.csv            — version / last-update metadata (informational only)

Files that are NOT consumed (not needed for entity-level data):
  activity.csv, branch.csv, code.csv, contact.csv, establishment.csv

Output schema
─────────────
  entities (
      enterprise_number TEXT PRIMARY KEY,  -- 10-digit, no dots
      status            TEXT,              -- "Active" | "Stopped" | …
      juridical_form    TEXT,              -- e.g. "SA/NV", "BVBA/SPRL", …
      start_date        TEXT,              -- ISO YYYY-MM-DD or raw BCE string
      name_nl           TEXT,              -- official/commercial NL name
      name_fr           TEXT,              -- official/commercial FR name
      name_de           TEXT,              -- official/commercial DE name
      address           TEXT,              -- formatted registered-office address
      link              TEXT               -- KBO portal URL
  )

  entities_fts (
      VIRTUAL TABLE using fts5
      content='entities', content_rowid='rowid'
      columns: enterprise_number (UNINDEXED), name_nl, name_fr, name_de
  )

Usage
─────
  # From a pre-downloaded ZIP (recommended):
  python scripts/extract_bce.py \\
      --zip-file  /path/to/KboOpenData_YYYYMMDD_YYYYMMDD_FullData.zip \\
      --output    /path/to/bce.db

  # From a directory already containing the CSV files:
  python scripts/extract_bce.py \\
      --data-dir  /path/to/csv_dir \\
      --output    /path/to/bce.db

After the build completes, add to your .env:
  BCE_BELGIUM_DB_FILE=/path/to/bce.db

Performance notes
─────────────────
  Processing all three CSV files takes roughly 5-10 minutes on a laptop
  and produces a ~600 MB SQLite database.  The FTS5 index roughly doubles
  that (total ~1.1 GB).  Memory usage stays low — rows are streamed and
  batch-inserted in chunks of 5 000.

BCE denomination type codes
───────────────────────────
  001 = official denomination
  002 = commercial name ("enseigne")
  003 = abbreviated official name
  The adapter picks type 001 preferentially, falling back to 002.

BCE language codes
──────────────────
  1 = French (FR), 2 = Dutch (NL), 3 = German (DE), 4 = Unknown
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sqlite3
import sys
import zipfile
from io import TextIOWrapper
from pathlib import Path
from typing import Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_BATCH_SIZE = 5_000

# KBO portal deep-link for a single enterprise.
_ENTITY_URL = (
    "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
    "?ondernemingsnummer={enterprise_number}"
)

# BCE enterprise-number format: NNNN.NNN.NNN (10 digits with two dots).
_DOT_RE = re.compile(r"\D")


def _normalise(raw: str) -> str:
    """Strip non-digits and zero-pad to 10 characters."""
    digits = _DOT_RE.sub("", (raw or "").strip())
    return digits.zfill(10) if digits else ""


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA cache_size   = -65536;

CREATE TABLE IF NOT EXISTS entities (
    enterprise_number TEXT PRIMARY KEY,
    status            TEXT,
    juridical_form    TEXT,
    start_date        TEXT,
    name_nl           TEXT,
    name_fr           TEXT,
    name_de           TEXT,
    address           TEXT,
    link              TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5 (
    enterprise_number UNINDEXED,
    name_nl,
    name_fr,
    name_de,
    content='entities',
    content_rowid='rowid'
);
"""


# ---------------------------------------------------------------------------
# CSV streaming helpers
# ---------------------------------------------------------------------------

def _csv_rows(path: Path) -> Iterator[dict[str, str]]:
    """Yield rows from a BCE CSV file as dicts, handling BOM and mixed encodings."""
    encodings = ("utf-8-sig", "utf-8", "latin-1")
    for enc in encodings:
        try:
            with open(path, newline="", encoding=enc, errors="replace") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    yield row
            return
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode {path} with any known encoding")


def _csv_rows_from_zip(zf: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    """Yield rows from a named CSV inside a ZipFile object."""
    with zf.open(name) as raw_fh:
        # Try UTF-8 with BOM first (many BCE ZIPs use UTF-8-BOM).
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            raw_fh.seek(0)  # type: ignore[attr-defined]
            try:
                text_fh = TextIOWrapper(raw_fh, encoding=enc, errors="replace", newline="")
                reader = csv.DictReader(text_fh)
                for row in reader:
                    yield row
                return
            except (UnicodeDecodeError, AttributeError):
                continue


# ---------------------------------------------------------------------------
# Phase 1 — Load enterprise basics
# ---------------------------------------------------------------------------

def _load_enterprise(
    conn: sqlite3.Connection,
    rows: Iterator[dict[str, str]],
) -> int:
    """Insert rows from enterprise.csv."""
    cur = conn.cursor()
    batch: list[tuple] = []
    total = 0

    for row in rows:
        num = _normalise(row.get("EnterpriseNumber") or row.get("enterprise_number") or "")
        if not num:
            continue
        status = (row.get("Status") or "").strip()
        jform = (row.get("JuridicalForm") or row.get("JuridicalFormCAC") or "").strip()
        start = (row.get("StartDate") or "").strip()
        # Normalise date YYYYMMDD → YYYY-MM-DD if needed.
        if len(start) == 8 and start.isdigit():
            start = f"{start[:4]}-{start[4:6]}-{start[6:]}"
        link = _ENTITY_URL.format(enterprise_number=num)

        batch.append((num, status, jform, start, "", "", "", "", link))
        if len(batch) >= _BATCH_SIZE:
            cur.executemany(
                """
                INSERT OR IGNORE INTO entities
                    (enterprise_number, status, juridical_form, start_date,
                     name_nl, name_fr, name_de, address, link)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                batch,
            )
            total += len(batch)
            batch.clear()
            if total % 100_000 == 0:
                logger.info("  enterprise: %d rows", total)

    if batch:
        cur.executemany(
            """
            INSERT OR IGNORE INTO entities
                (enterprise_number, status, juridical_form, start_date,
                 name_nl, name_fr, name_de, address, link)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            batch,
        )
        total += len(batch)

    conn.commit()
    logger.info("enterprise: %d rows loaded", total)
    return total


# ---------------------------------------------------------------------------
# Phase 2 — Load denominations
# ---------------------------------------------------------------------------

# TypeOfDenomination priority for official name selection.
# Lower number = higher priority.
_DENOM_PRIORITY = {"001": 0, "003": 1, "002": 2}


def _load_denominations(
    conn: sqlite3.Connection,
    rows: Iterator[dict[str, str]],
) -> int:
    """
    Read denomination.csv and update entities.name_nl / name_fr / name_de.

    Only rows whose EntityNumber matches an existing enterprise are processed.
    Denominations with TypeOfDenomination 001 (official) take priority over
    002 (commercial) and 003 (abbreviated).
    """
    # Build in-memory accumulator: enterprise_number → {lang: (priority, name)}
    acc: dict[str, dict[str, tuple[int, str]]] = {}
    total = 0

    _lang_col = {
        "1": "fr",   # French
        "2": "nl",   # Dutch (Flemish)
        "3": "de",   # German
    }

    for row in rows:
        num = _normalise(row.get("EntityNumber") or row.get("entity_number") or "")
        if not num:
            continue
        lang_code = str(row.get("Language") or "").strip()
        lang = _lang_col.get(lang_code)
        if lang is None:
            continue
        denom_type = str(row.get("TypeOfDenomination") or "").strip().zfill(3)
        priority = _DENOM_PRIORITY.get(denom_type, 99)
        name = (row.get("Denomination") or "").strip()
        if not name:
            continue

        if num not in acc:
            acc[num] = {}
        existing = acc[num].get(lang)
        if existing is None or priority < existing[0]:
            acc[num][lang] = (priority, name)
        total += 1
        if total % 500_000 == 0:
            logger.info("  denomination: %d rows scanned", total)

    logger.info("denomination: %d rows scanned; writing %d entities", total, len(acc))

    # Flush updates in batches.
    cur = conn.cursor()
    items = list(acc.items())
    updated = 0
    for i in range(0, len(items), _BATCH_SIZE):
        chunk = items[i : i + _BATCH_SIZE]
        for num, lang_map in chunk:
            name_nl = (lang_map.get("nl") or (None, ""))[1]
            name_fr = (lang_map.get("fr") or (None, ""))[1]
            name_de = (lang_map.get("de") or (None, ""))[1]
            cur.execute(
                """
                UPDATE entities
                   SET name_nl = CASE WHEN ? != '' THEN ? ELSE name_nl END,
                       name_fr = CASE WHEN ? != '' THEN ? ELSE name_fr END,
                       name_de = CASE WHEN ? != '' THEN ? ELSE name_de END
                 WHERE enterprise_number = ?
                """,
                (name_nl, name_nl, name_fr, name_fr, name_de, name_de, num),
            )
        conn.commit()
        updated += len(chunk)
        logger.info("  denomination: %d entities updated", updated)

    return total


# ---------------------------------------------------------------------------
# Phase 3 — Load addresses
# ---------------------------------------------------------------------------

def _load_addresses(
    conn: sqlite3.Connection,
    rows: Iterator[dict[str, str]],
) -> int:
    """
    Read address.csv and update entities.address with the registered-office
    address (TypeOfAddress = "REGO" / "1").
    """
    acc: dict[str, str] = {}
    total = 0

    for row in rows:
        num = _normalise(row.get("EntityNumber") or row.get("entity_number") or "")
        if not num:
            continue
        addr_type = str(row.get("TypeOfAddress") or "").strip().upper()
        # We only want the registered office address.
        if addr_type not in ("REGO", "1", "REGISTERED_OFFICE"):
            continue
        if num in acc:
            continue  # Keep first (most recent) address only.

        zipcode = (row.get("Zipcode") or "").strip()
        municipality = (row.get("MunicipalityNL") or row.get("MunicipalityFR") or "").strip()
        street = (row.get("StreetNL") or row.get("StreetFR") or "").strip()
        number = (row.get("HouseNumber") or "").strip()
        box = (row.get("Box") or "").strip()
        country = (row.get("CountryNL") or row.get("CountryFR") or "").strip()

        parts: list[str] = []
        if street:
            parts.append(f"{street} {number}".strip())
        if box:
            parts.append(f"box {box}")
        if zipcode or municipality:
            parts.append(f"{zipcode} {municipality}".strip())
        if country and country.lower() not in ("", "belgie", "belgique", "belgien", "belgium"):
            parts.append(country)
        else:
            parts.append("Belgium")

        acc[num] = ", ".join(p for p in parts if p)
        total += 1
        if total % 200_000 == 0:
            logger.info("  address: %d rows scanned", total)

    logger.info("address: %d registered-office addresses; writing", len(acc))

    cur = conn.cursor()
    items = list(acc.items())
    written = 0
    for i in range(0, len(items), _BATCH_SIZE):
        chunk = items[i : i + _BATCH_SIZE]
        cur.executemany(
            "UPDATE entities SET address = ? WHERE enterprise_number = ?",
            [(addr, num) for num, addr in chunk],
        )
        conn.commit()
        written += len(chunk)
    logger.info("  address: %d rows written", written)

    return total


# ---------------------------------------------------------------------------
# Phase 4 — Build FTS5 index
# ---------------------------------------------------------------------------

def _build_fts(conn: sqlite3.Connection) -> None:
    """Populate the FTS5 index from the entities table."""
    logger.info("Building FTS5 index …")
    conn.execute(
        "INSERT INTO entities_fts(entities_fts) VALUES('rebuild')"
    )
    conn.commit()
    logger.info("FTS5 index built.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _resolve_csv(zip_path: Path | None, data_dir: Path | None, name: str) -> Path | None:
    """Return the path of a CSV file, checking the data dir."""
    if data_dir:
        p = data_dir / name
        if p.exists():
            return p
        # BCE sometimes includes the date in the filename — fuzzy find.
        matches = sorted(data_dir.glob(f"*{name}*"))
        if matches:
            return matches[0]
    return None


def _iter_csv(
    zip_path: Path | None,
    data_dir: Path | None,
    name: str,
) -> Iterator[dict[str, str]]:
    """Yield rows from a CSV file, trying zip_path first then data_dir."""
    if zip_path:
        with zipfile.ZipFile(zip_path) as zf:
            # The ZIP may contain the CSV at the root or inside a subdirectory.
            candidates = [n for n in zf.namelist() if n.lower().endswith(name.lower())]
            if candidates:
                yield from _csv_rows_from_zip(zf, candidates[0])
                return
            logger.warning("'%s' not found in zip (entries: %s)", name, zf.namelist()[:10])
    if data_dir:
        p = _resolve_csv(None, data_dir, name)
        if p:
            yield from _csv_rows(p)
            return
    logger.error("Cannot find '%s' in zip or data-dir — skipping.", name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a SQLite index from BCE/KBO open data."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--zip-file",
        metavar="PATH",
        help="Path to the KBO open data ZIP (e.g. KboOpenData_*.zip).",
    )
    source.add_argument(
        "--data-dir",
        metavar="DIR",
        help="Directory containing the unzipped BCE CSV files.",
    )
    parser.add_argument(
        "--output",
        metavar="PATH",
        default="bce.db",
        help="Output SQLite file (default: bce.db).",
    )
    parser.add_argument(
        "--no-fts",
        action="store_true",
        help="Skip building the FTS5 name index (smaller DB, no name search).",
    )
    args = parser.parse_args()

    zip_path = Path(args.zip_file) if args.zip_file else None
    data_dir = Path(args.data_dir) if args.data_dir else None
    output = Path(args.output)

    if zip_path and not zip_path.exists():
        logger.error("ZIP file not found: %s", zip_path)
        sys.exit(1)
    if data_dir and not data_dir.is_dir():
        logger.error("data-dir is not a directory: %s", data_dir)
        sys.exit(1)

    logger.info("Opening database: %s", output)
    conn = sqlite3.connect(str(output))
    for stmt in _DDL.strip().split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()

    logger.info("Phase 1: Loading enterprise.csv …")
    _load_enterprise(conn, _iter_csv(zip_path, data_dir, "enterprise.csv"))

    logger.info("Phase 2: Loading denomination.csv …")
    _load_denominations(conn, _iter_csv(zip_path, data_dir, "denomination.csv"))

    logger.info("Phase 3: Loading address.csv …")
    _load_addresses(conn, _iter_csv(zip_path, data_dir, "address.csv"))

    if not args.no_fts:
        _build_fts(conn)

    conn.close()
    logger.info("Done. Database written to: %s", output)
    size_mb = output.stat().st_size / 1_048_576
    logger.info("Database size: %.1f MB", size_mb)


if __name__ == "__main__":
    main()
