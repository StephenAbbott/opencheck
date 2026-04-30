#!/usr/bin/env python3
"""Extract BrightQuery company records that have an LEI and their associated
people from local files, writing to a SQLite database for OpenCheck.

BrightQuery's open data is distributed as many individual files in Senzing
JSON format.  This script does two passes:

  1. Scan every file under --org-dir.  Records that carry an LEI (detected
     via multiple field-name patterns) are written to the ``companies`` table.

  2. Scan every file under --people-dir.  People whose REL_POINTER_KEY (or
     GROUP_ASSN_ID_NUMBER) matches a BQ_ID extracted in pass 1 are written
     to the ``people`` table.

Supported file formats
----------------------
  * Single JSON object per file  (.json)
  * JSON array per file          (.json)
  * JSONL (one record per line)  (.jsonl, .json, or no extension)
  * Gzip-compressed variants     (.json.gz, .jsonl.gz)

LEI field detection (tried in order)
-------------------------------------
  1. FEATURES entry with OTHER_ID_TYPE == "LEI"   (original Senzing pattern)
  2. FEATURES entry with OTHER_ID_TYPE == "bq_lei" (lowercase variant)
  3. FEATURES entry with direct key "bq_lei"
  4. FEATURES entry with direct key "LEI"
  5. Top-level "bq_lei" field on the record

Usage
-----
    python backend/scripts/extract_brightquery.py

    # Or with explicit paths:
    python backend/scripts/extract_brightquery.py \\
        --org-dir ~/Downloads/brightquery/organisation \\
        --people-dir ~/Downloads/brightquery/people \\
        --output ~/Downloads/brightquery/brightquery.db

Run diagnose_brightquery.py first if you're unsure of the file format.

Activate in OpenCheck by adding to your .env:

    BRIGHTQUERY_DB_FILE=/path/to/brightquery.db
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQLite schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    bq_id    TEXT PRIMARY KEY,
    lei      TEXT NOT NULL,
    name     TEXT,
    raw_json TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_lei ON companies (lei);

CREATE TABLE IF NOT EXISTS people (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    org_bq_id TEXT NOT NULL,
    raw_json  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_people_org ON people (org_bq_id);
"""

_BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# File reading — handles JSON, JSONL, and gzip variants
# ---------------------------------------------------------------------------


def _open_file(path: Path):
    """Open a file for reading, transparently handling gzip compression."""
    if path.suffix == ".gz" or str(path).endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open(encoding="utf-8", errors="replace")


def _iter_records(path: Path):
    """Stream records one at a time from a JSONL file (or JSON object/array).

    Reads line-by-line so multi-GB JSONL files never load fully into RAM.
    Each line is expected to be a complete JSON object (JSONL format).
    Lines that fail to parse as JSON are silently skipped.
    """
    try:
        with _open_file(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        yield obj
                    elif isinstance(obj, list):
                        # Handle rare case where a line contains a JSON array
                        for item in obj:
                            if isinstance(item, dict):
                                yield item
                except json.JSONDecodeError:
                    pass
    except Exception as exc:
        logger.debug("Skipping %s (%s)", path.name, exc)


def _iter_files(directory: Path):
    """Yield every file under *directory* (recursive, any extension)."""
    for root, _dirs, files in os.walk(directory):
        for fname in sorted(files):
            yield Path(root) / fname


# ---------------------------------------------------------------------------
# Feature-array helpers (Senzing format)
# ---------------------------------------------------------------------------


def _features(record: dict) -> list[dict]:
    return record.get("FEATURES") or []


def _get_feature(feats: list[dict], key: str) -> dict | None:
    for f in feats:
        if isinstance(f, dict) and key in f:
            return f
    return None


def _find_lei(record: dict) -> str | None:
    """Return the LEI from a record using multiple detection patterns."""
    # Pattern 5: top-level bq_lei field
    top_lei = record.get("bq_lei")
    if top_lei:
        return str(top_lei).strip()

    feats = _features(record)
    for f in feats:
        if not isinstance(f, dict):
            continue

        # Pattern 1: OTHER_ID_TYPE == "LEI"  (most likely for Senzing bulk data)
        id_type = (f.get("OTHER_ID_TYPE") or "").strip()
        if id_type.upper() == "LEI":
            val = (f.get("OTHER_ID_NUMBER") or "").strip()
            if val:
                return val

        # Pattern 2: OTHER_ID_TYPE == "bq_lei"
        if id_type.lower() == "bq_lei":
            val = (f.get("OTHER_ID_NUMBER") or "").strip()
            if val:
                return val

        # Pattern 3: direct bq_lei key in a feature dict
        if "bq_lei" in f:
            val = str(f["bq_lei"]).strip()
            if val:
                return val

        # Pattern 4: direct LEI key
        if "LEI" in f:
            val = str(f["LEI"]).strip()
            if val:
                return val

    return None


def _find_name(feats: list[dict]) -> str:
    f = _get_feature(feats, "NAME_ORG")
    return str(f["NAME_ORG"]).strip() if f else ""


def _find_org_bq_id(feats: list[dict]) -> str:
    """Return the parent company's BQ_ID from a people-business record."""
    for f in feats:
        if isinstance(f, dict) and "REL_POINTER_KEY" in f:
            return str(f["REL_POINTER_KEY"]).strip()
    for f in feats:
        if isinstance(f, dict) and f.get("GROUP_ASSN_ID_TYPE") == "BQ_ID":
            val = (f.get("GROUP_ASSN_ID_NUMBER") or "").strip()
            if val:
                return val
    return ""


# ---------------------------------------------------------------------------
# Pass 1: companies
# ---------------------------------------------------------------------------


def extract_companies(org_dir: Path, conn: sqlite3.Connection) -> set[str]:
    """Scan org files; insert companies that have a LEI. Returns set of bq_ids."""
    extracted: set[str] = set()
    total = skipped = files_scanned = files_empty = 0
    batch: list[tuple[str, str, str, str]] = []

    def _flush() -> None:
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO companies (bq_id, lei, name, raw_json) VALUES (?,?,?,?)",
                batch,
            )
            conn.commit()
            batch.clear()

    for path in _iter_files(org_dir):
        files_scanned += 1
        file_had_records = False

        for record in _iter_records(path):
            total += 1
            file_had_records = True
            lei = _find_lei(record)
            if not lei:
                skipped += 1
                continue
            bq_id = str(record.get("RECORD_ID") or "").strip()
            if not bq_id:
                skipped += 1
                continue
            feats = _features(record)
            name = _find_name(feats)
            batch.append((bq_id, lei, name, json.dumps(record, ensure_ascii=False)))
            extracted.add(bq_id)
            if len(batch) >= _BATCH_SIZE:
                _flush()

        if not file_had_records:
            files_empty += 1
            if files_empty <= 3:
                logger.debug("Empty or unparseable: %s", path.name)

        if files_scanned % 50 == 0:
            logger.info(
                "  Orgs  : files %-7d  records %-7d  with LEI %-6d  no-LEI %d",
                files_scanned, total, len(extracted), skipped,
            )

    _flush()

    if files_empty > 0 and total == 0:
        logger.warning(
            "%d files scanned, all empty or unparseable — "
            "run diagnose_brightquery.py to inspect the actual file format",
            files_scanned,
        )
    else:
        logger.info(
            "Companies done : %d files · %d records · %d with LEI · %d no-LEI",
            files_scanned, total, len(extracted), skipped,
        )

    return extracted


# ---------------------------------------------------------------------------
# Pass 2: people
# ---------------------------------------------------------------------------


def extract_people(
    people_dir: Path,
    conn: sqlite3.Connection,
    company_ids: set[str],
) -> None:
    """Scan people files; insert rows whose org_bq_id is in *company_ids*."""
    total = matched = files_scanned = 0
    batch: list[tuple[str, str, str]] = []

    def _flush() -> None:
        if batch:
            conn.executemany(
                "INSERT INTO people (person_id, org_bq_id, raw_json) VALUES (?,?,?)",
                batch,
            )
            conn.commit()
            batch.clear()

    for path in _iter_files(people_dir):
        files_scanned += 1
        for record in _iter_records(path):
            total += 1
            feats = _features(record)
            org_bq_id = _find_org_bq_id(feats)
            if not org_bq_id or org_bq_id not in company_ids:
                continue
            person_id = str(record.get("RECORD_ID") or "").strip()
            batch.append((person_id, org_bq_id, json.dumps(record, ensure_ascii=False)))
            matched += 1
            if len(batch) >= _BATCH_SIZE:
                _flush()

        if files_scanned % 50 == 0:
            logger.info(
                "  People: files %-7d  records %-7d  matched %d",
                files_scanned, total, matched,
            )

    _flush()
    logger.info(
        "People done    : %d files · %d records · %d matched to LEI companies",
        files_scanned, total, matched,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    home = Path.home()
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--org-dir",
        type=Path,
        default=home / "Downloads/brightquery/organisation",
        metavar="DIR",
        help="Directory containing BrightQuery organisation files "
             "(default: ~/Downloads/brightquery/organisation)",
    )
    parser.add_argument(
        "--people-dir",
        type=Path,
        default=home / "Downloads/brightquery/people",
        metavar="DIR",
        help="Directory containing BrightQuery people_business files "
             "(default: ~/Downloads/brightquery/people)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=home / "Downloads/brightquery/brightquery.db",
        metavar="FILE",
        help="Output SQLite database path "
             "(default: ~/Downloads/brightquery/brightquery.db)",
    )
    args = parser.parse_args()

    for label, path in (("org-dir", args.org_dir), ("people-dir", args.people_dir)):
        if not path.exists():
            logger.error("--%s not found: %s", label, path)
            sys.exit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Output database : %s", args.output)
    conn = sqlite3.connect(str(args.output))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    conn.commit()

    logger.info("Pass 1 — companies with LEI from %s", args.org_dir)
    company_ids = extract_companies(args.org_dir, conn)

    if not company_ids:
        logger.warning(
            "No companies with LEI found.\n"
            "Run this first to inspect your files:\n\n"
            "    python backend/scripts/diagnose_brightquery.py "
            "--org-dir %s",
            args.org_dir,
        )
        conn.close()
        sys.exit(0)

    logger.info(
        "Pass 2 — people for %d companies from %s",
        len(company_ids), args.people_dir,
    )
    extract_people(args.people_dir, conn, company_ids)

    c_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    p_count = conn.execute("SELECT COUNT(*) FROM people").fetchone()[0]
    conn.close()

    logger.info(
        "Done. Database contains %d companies and %d people.", c_count, p_count
    )
    logger.info(
        "Activate in OpenCheck:\n\n"
        "    BRIGHTQUERY_DB_FILE=%s\n\n"
        "Add that line to your backend/.env (or repo-root .env).",
        args.output,
    )


if __name__ == "__main__":
    main()
