"""Build the ACRA Singapore SQLite database from data.gov.sg CSV exports.

The Accounting and Corporate Regulatory Authority (ACRA) publishes two
monthly CSV datasets on data.gov.sg:

  Dataset A — Entities Registered with ACRA
    URL: https://data.gov.sg/datasets/d_3f960c10fed6145404ca7b821f263b87/view
    Size: ~230 MB

  Dataset B — Entities Registered with Other UEN Issuance Agencies
    URL: https://data.gov.sg/datasets/d_b1d2b840ab9e993570c037b706b39bb8/view
    Size: ~3 MB

Both files share the same CSV schema:
  uen, issuance_agency_desc, uen_status_desc, entity_name,
  entity_type_desc, uen_issue_date, reg_street_name, reg_postal_code

Usage
-----
1. Download both CSV files from the dataset pages linked above (click
   "Download" on each dataset — you may need to be logged in to data.gov.sg).

2. Run this script:

   python scripts/extract_acra.py \\
     --acra-csv entities_with_acra.csv \\
     --other-csv entities_with_other_agencies.csv \\
     --output /path/to/acra.db

   Only ``--acra-csv`` is required.  ``--other-csv`` is optional but
   recommended for completeness.

3. Set ``ACRA_SINGAPORE_DB_FILE=/path/to/acra.db`` in the backend ``.env``.

The resulting SQLite database contains:
  - An ``entities`` table keyed on ``uen``
  - An FTS5 virtual table ``entities_fts`` indexing ``entity_name``

License
-------
Singapore Open Data Licence 1.0 — https://data.gov.sg/open-data-licence
Attribution: Accounting and Corporate Regulatory Authority (ACRA),
Government of Singapore.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Expected CSV columns (order may vary — we use header names).
_EXPECTED_COLS = {
    "uen",
    "issuance_agency_desc",
    "uen_status_desc",
    "entity_name",
    "entity_type_desc",
    "uen_issue_date",
    "reg_street_name",
    "reg_postal_code",
}

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS entities (
    uen                 TEXT PRIMARY KEY,
    issuance_agency_desc TEXT,
    uen_status_desc     TEXT,
    entity_name         TEXT,
    entity_type_desc    TEXT,
    uen_issue_date      TEXT,
    reg_street_name     TEXT,
    reg_postal_code     TEXT
);
"""

_CREATE_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
    entity_name,
    uen UNINDEXED,
    content='entities',
    content_rowid='rowid'
);
"""

_INSERT = """
INSERT OR REPLACE INTO entities
    (uen, issuance_agency_desc, uen_status_desc, entity_name,
     entity_type_desc, uen_issue_date, reg_street_name, reg_postal_code)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""

_FTS_REBUILD = "INSERT INTO entities_fts(entities_fts) VALUES('rebuild');"


def _validate_headers(headers: list[str], path: Path) -> None:
    cols = {h.strip().lower() for h in headers}
    missing = _EXPECTED_COLS - cols
    if missing:
        raise ValueError(
            f"{path}: missing expected columns: {', '.join(sorted(missing))}"
        )


def _load_csv(conn: sqlite3.Connection, path: Path, label: str) -> int:
    """Load a single CSV file into the entities table.  Returns row count."""
    logger.info("Loading %s from %s …", label, path)
    count = 0
    errors = 0

    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"{path}: empty or unreadable CSV")
        _validate_headers(list(reader.fieldnames), path)

        # Normalise column names once.
        def _get(row: dict[str, str], col: str) -> str | None:
            v = row.get(col) or row.get(col.upper()) or None
            return v.strip() or None if v else None

        batch: list[tuple[str | None, ...]] = []
        for row in reader:
            uen = (_get(row, "uen") or "").strip().upper()
            if not uen:
                errors += 1
                continue
            batch.append((
                uen,
                _get(row, "issuance_agency_desc"),
                _get(row, "uen_status_desc"),
                _get(row, "entity_name"),
                _get(row, "entity_type_desc"),
                _get(row, "uen_issue_date"),
                _get(row, "reg_street_name"),
                _get(row, "reg_postal_code"),
            ))
            if len(batch) >= 10_000:
                conn.executemany(_INSERT, batch)
                count += len(batch)
                batch.clear()
                if count % 100_000 == 0:
                    logger.info("  … %d rows loaded", count)
                    conn.commit()

        if batch:
            conn.executemany(_INSERT, batch)
            count += len(batch)

    conn.commit()
    if errors:
        logger.warning("  %d rows skipped (missing UEN)", errors)
    logger.info("  → %d rows loaded from %s", count, label)
    return count


def build_db(
    acra_csv: Path,
    other_csv: Path | None,
    output: Path,
) -> None:
    """Create (or recreate) the ACRA SQLite database."""
    if output.exists():
        logger.info("Removing existing database at %s", output)
        output.unlink()

    logger.info("Creating database at %s", output)
    conn = sqlite3.connect(str(output))

    # Tune for bulk insert performance.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=-64000;")  # 64 MB page cache

    conn.execute(_CREATE_TABLE)
    conn.commit()

    total = 0
    t0 = time.monotonic()

    total += _load_csv(conn, acra_csv, "ACRA entities")

    if other_csv is not None:
        total += _load_csv(conn, other_csv, "other UEN agencies")

    # Build FTS5 index.
    logger.info("Building FTS5 index on entity_name …")
    conn.execute(_CREATE_FTS)
    conn.execute(_FTS_REBUILD)
    conn.commit()

    elapsed = time.monotonic() - t0
    size_mb = output.stat().st_size / 1_048_576
    logger.info(
        "Done. %d entities in %.1fs. DB size: %.1f MB.", total, elapsed, size_mb
    )

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the ACRA Singapore SQLite DB from data.gov.sg CSV files."
    )
    parser.add_argument(
        "--acra-csv",
        required=True,
        type=Path,
        metavar="PATH",
        help="Path to the 'Entities Registered with ACRA' CSV file.",
    )
    parser.add_argument(
        "--other-csv",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Optional path to the 'Entities Registered with Other UEN Issuance "
            "Agencies' CSV file."
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        help="Output SQLite database path (e.g. /data/acra.db).",
    )
    args = parser.parse_args()

    if not args.acra_csv.exists():
        logger.error("ACRA CSV not found: %s", args.acra_csv)
        sys.exit(1)
    if args.other_csv is not None and not args.other_csv.exists():
        logger.error("Other-agencies CSV not found: %s", args.other_csv)
        sys.exit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    build_db(args.acra_csv, args.other_csv, args.output)


if __name__ == "__main__":
    main()
