#!/usr/bin/env python3
"""Download and index Open Ownership BODS bulk data for local querying.

This script fetches the pre-processed BODS v0.4 Parquet archives from
Open Ownership's S3 bucket, extracts them to a local directory, and
builds a lightweight FTS5 SQLite index over entity names so the
``bods_gleif`` and ``bods_uk_psc`` adapters can serve sub-200 ms
name-search responses without loading millions of rows into memory.

Usage::

    # GLEIF only (default output: ./data/bods/)
    python scripts/setup_bods_data.py --source gleif

    # UK PSC only, custom output directory
    python scripts/setup_bods_data.py --source uk_psc --output-dir /data/bods

    # Both sources
    python scripts/setup_bods_data.py --source both

    # Rebuild FTS index from existing Parquet (skip download)
    python scripts/setup_bods_data.py --source gleif --skip-download

Environment variables (override S3 defaults)::

    BODS_GLEIF_S3_URL   — URL for the GLEIF Parquet zip
    BODS_UK_PSC_S3_URL  — URL for the UK PSC Parquet zip

After running, set these .env variables to activate the adapters::

    BODS_GLEIF_PARQUET_DIR=./data/bods/gleif/parquet
    BODS_GLEIF_FTS_DB=./data/bods/gleif/fts.db
    BODS_UK_PSC_PARQUET_DIR=./data/bods/uk_psc/parquet
    BODS_UK_PSC_FTS_DB=./data/bods/uk_psc/fts.db
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sqlite3
import sys
import time
import zipfile
from pathlib import Path

import httpx

# DuckDB is an optional dep but required for this script.
try:
    import duckdb
except ImportError:
    print("ERROR: duckdb is required.  Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# S3 source URLs
# ---------------------------------------------------------------------------

_DEFAULT_GLEIF_S3_URL = (
    "https://oo-bodsdata.s3.amazonaws.com/data/gleif_version_0_4/parquet.zip"
)
_DEFAULT_UK_PSC_S3_URL = (
    "https://oo-bodsdata.s3.amazonaws.com/data/uk_version_0_4/parquet.zip"
)

# ---------------------------------------------------------------------------
# FTS5 schema
# ---------------------------------------------------------------------------

_FTS_SCHEMA_ENTITY = """
CREATE VIRTUAL TABLE IF NOT EXISTS entity_fts USING fts5(
    statementid    UNINDEXED,
    name,
    entity_type    UNINDEXED,
    jurisdiction   UNINDEXED
);
"""

_FTS_SCHEMA_PERSON = """
CREATE VIRTUAL TABLE IF NOT EXISTS person_fts USING fts5(
    statementid    UNINDEXED,
    name,
    nationality    UNINDEXED
);
"""

_META_SCHEMA = """
CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------


def _download_zip(url: str, dest_zip: Path) -> None:
    """Stream-download a zip file from *url* to *dest_zip*."""
    log.info("Downloading %s → %s", url, dest_zip)
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    downloaded = 0
    with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest_zip, "wb") as fh:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                fh.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = 100 * downloaded // total
                    elapsed = time.time() - t0
                    mbps = (downloaded / 1e6) / max(elapsed, 0.1)
                    print(
                        f"\r  {pct:3d}%  {downloaded/1e6:.0f}/{total/1e6:.0f} MB"
                        f"  {mbps:.1f} MB/s",
                        end="",
                        flush=True,
                    )
    print()  # newline after progress
    log.info("Download complete: %.1f MB in %.1fs", downloaded / 1e6, time.time() - t0)


def _extract_zip(src_zip: Path, dest_dir: Path) -> None:
    """Extract all files from *src_zip* into *dest_dir*."""
    log.info("Extracting %s → %s", src_zip, dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with zipfile.ZipFile(src_zip) as zf:
        members = zf.infolist()
        total_bytes = sum(m.file_size for m in members)
        extracted = 0
        for member in members:
            zf.extract(member, dest_dir)
            extracted += member.file_size
            pct = 100 * extracted // max(total_bytes, 1)
            print(f"\r  {pct:3d}%  {extracted/1e9:.2f}/{total_bytes/1e9:.2f} GB", end="", flush=True)
    print()
    log.info("Extraction complete in %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# FTS5 index builders
# ---------------------------------------------------------------------------


def _build_entity_fts(parquet_dir: Path, fts_db: Path) -> None:
    """Build FTS5 entity-name index from Flatterer-format Parquet files.

    The BODS bulk data for GLEIF and UK PSC is produced by Flatterer which
    flattens nested JSON into one Parquet file per array level.  The main
    entity statement table is ``entity_statement.parquet``; names live in
    the ``recorddetails_name`` column (BODS 0.4 nesting → underscore flatten).
    """
    entity_p = parquet_dir / "entity_statement.parquet"
    if not entity_p.exists():
        log.warning("entity_statement.parquet not found in %s — skipping entity FTS", parquet_dir)
        return

    log.info("Building entity FTS5 index from %s", entity_p)
    t0 = time.time()

    conn = sqlite3.connect(str(fts_db))
    conn.execute(_META_SCHEMA)
    conn.execute("DROP TABLE IF EXISTS entity_fts")
    conn.execute(_FTS_SCHEMA_ENTITY)

    # Use DuckDB to scan the Parquet efficiently.
    duck = duckdb.connect(":memory:")
    count = 0
    batch: list[tuple[str, str, str, str]] = []

    rows = duck.execute(
        """
        SELECT
            statementid,
            COALESCE(recorddetails_name, '') AS name,
            COALESCE(recorddetails_entitytype_type, '') AS entity_type,
            COALESCE(recorddetails_jurisdiction_name, '') AS jurisdiction
        FROM read_parquet(?)
        WHERE recorddetails_name IS NOT NULL
          AND TRIM(recorddetails_name) != ''
        """,
        [str(entity_p)],
    ).fetchall()

    for row in rows:
        batch.append(row)
        count += 1
        if len(batch) >= 50_000:
            conn.executemany(
                "INSERT INTO entity_fts(statementid, name, entity_type, jurisdiction) VALUES (?, ?, ?, ?)",
                batch,
            )
            batch.clear()
            if count % 500_000 == 0:
                log.info("  indexed %s entity rows …", f"{count:,}")

    if batch:
        conn.executemany(
            "INSERT INTO entity_fts(statementid, name, entity_type, jurisdiction) VALUES (?, ?, ?, ?)",
            batch,
        )

    conn.execute("INSERT OR REPLACE INTO _meta VALUES ('entity_count', ?)", [str(count)])
    conn.commit()
    conn.close()
    duck.close()
    log.info("Entity FTS5 done: %s rows in %.1fs", f"{count:,}", time.time() - t0)


def _build_person_fts(parquet_dir: Path, fts_db: Path) -> None:
    """Build FTS5 person-name index for UK PSC person statements.

    Person names live in the sub-table ``person_recordDetails_names.parquet``
    (Flatterer flattens the ``recordDetails.names`` array into its own file).
    The column is ``fullName`` (camelCase), linked via ``_link_person_statement``.
    Nationalities are in ``person_recordDetails_nationalities.parquet``.
    """
    person_p = parquet_dir / "person_statement.parquet"
    if not person_p.exists():
        log.info("No person_statement.parquet found — skipping person FTS (GLEIF is entity-only)")
        return

    names_p = parquet_dir / "person_recordDetails_names.parquet"
    if not names_p.exists():
        log.warning("person_recordDetails_names.parquet not found in %s — skipping person FTS", parquet_dir)
        return

    nats_p = parquet_dir / "person_recordDetails_nationalities.parquet"

    log.info("Building person FTS5 index from %s", person_p)
    t0 = time.time()

    conn = sqlite3.connect(str(fts_db))
    conn.execute(_META_SCHEMA)
    conn.execute("DROP TABLE IF EXISTS person_fts")
    conn.execute(_FTS_SCHEMA_PERSON)

    duck = duckdb.connect(":memory:")
    count = 0
    batch: list[tuple[str, str, str]] = []

    # Name lives in the sub-table; use GROUP BY to get one row per person.
    if nats_p.exists():
        query = """
            SELECT
                ps.statementId,
                FIRST(n.fullName)  AS name,
                FIRST(nat.name)    AS nationality
            FROM read_parquet(?) ps
            JOIN read_parquet(?) n
              ON n._link_person_statement = ps._link
            LEFT JOIN read_parquet(?) nat
              ON nat._link_person_statement = ps._link
            WHERE n.fullName IS NOT NULL
              AND TRIM(n.fullName) != ''
            GROUP BY ps.statementId
        """
        rows = duck.execute(query, [str(person_p), str(names_p), str(nats_p)]).fetchall()
    else:
        query = """
            SELECT
                ps.statementId,
                FIRST(n.fullName) AS name,
                ''                AS nationality
            FROM read_parquet(?) ps
            JOIN read_parquet(?) n
              ON n._link_person_statement = ps._link
            WHERE n.fullName IS NOT NULL
              AND TRIM(n.fullName) != ''
            GROUP BY ps.statementId
        """
        rows = duck.execute(query, [str(person_p), str(names_p)]).fetchall()

    for row in rows:
        batch.append((row[0], row[1] or "", row[2] or ""))
        count += 1
        if len(batch) >= 50_000:
            conn.executemany(
                "INSERT INTO person_fts(statementid, name, nationality) VALUES (?, ?, ?)",
                batch,
            )
            batch.clear()
            if count % 500_000 == 0:
                log.info("  indexed %s person rows …", f"{count:,}")

    if batch:
        conn.executemany(
            "INSERT INTO person_fts(statementid, name, nationality) VALUES (?, ?, ?)",
            batch,
        )

    conn.execute("INSERT OR REPLACE INTO _meta VALUES ('person_count', ?)", [str(count)])
    conn.commit()
    conn.close()
    duck.close()
    log.info("Person FTS5 done: %s rows in %.1fs", f"{count:,}", time.time() - t0)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _setup_source(
    source: str,
    s3_url: str,
    output_dir: Path,
    skip_download: bool,
) -> None:
    source_dir = output_dir / source
    parquet_dir = source_dir / "parquet"
    fts_db = source_dir / "fts.db"
    zip_path = source_dir / "parquet.zip"

    if not skip_download:
        _download_zip(s3_url, zip_path)
        _extract_zip(zip_path, parquet_dir)
        log.info("Cleaning up zip …")
        zip_path.unlink(missing_ok=True)
    else:
        if not parquet_dir.exists():
            log.error("--skip-download set but %s does not exist", parquet_dir)
            sys.exit(1)
        log.info("Skipping download; using existing Parquet at %s", parquet_dir)

    _build_entity_fts(parquet_dir, fts_db)
    _build_person_fts(parquet_dir, fts_db)

    log.info(
        "Setup complete for %s.\n"
        "  Set these in your .env:\n"
        "    BODS_%s_PARQUET_DIR=%s\n"
        "    BODS_%s_FTS_DB=%s",
        source,
        source.upper(),
        parquet_dir,
        source.upper(),
        fts_db,
    )


def _create_bundle(source_dir: Path) -> Path:
    """Zip parquet/ + fts.db into a bundle.zip for upload to S3.

    Bundle layout (extracted relative to fts.db's parent dir)::

        parquet/entity_statement.parquet
        parquet/...
        fts.db

    Returns the path to the created bundle.zip.
    """
    parquet_dir = source_dir / "parquet"
    fts_db = source_dir / "fts.db"
    bundle_path = source_dir / "bundle.zip"

    if not parquet_dir.exists():
        log.error("parquet/ not found at %s", parquet_dir)
        sys.exit(1)
    if not fts_db.exists():
        log.error("fts.db not found at %s", fts_db)
        sys.exit(1)

    log.info("Creating bundle at %s …", bundle_path)
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Add fts.db at the top level
        zf.write(fts_db, "fts.db")
        # Add all parquet files under parquet/
        for pf in sorted(parquet_dir.iterdir()):
            if pf.suffix == ".parquet":
                zf.write(pf, f"parquet/{pf.name}")
                log.info("  + parquet/%s", pf.name)

    size_mb = bundle_path.stat().st_size / 1e6
    log.info("Bundle created: %.1f MB", size_mb)
    log.info(
        "Upload to S3 with:\n"
        "  aws s3 cp %s s3://YOUR_BUCKET/bods/%s/bundle.zip\n"
        "Then set:\n"
        "  BODS_%s_S3_URL=https://YOUR_BUCKET.s3.amazonaws.com/bods/%s/bundle.zip",
        bundle_path,
        source_dir.name,
        source_dir.name.upper(),
        source_dir.name,
    )
    return bundle_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and index Open Ownership BODS bulk data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["gleif", "uk_psc", "both"],
        default="both",
        help="Which dataset to set up (default: both)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./data/bods"),
        help="Root directory for extracted data (default: ./data/bods)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip S3 download; rebuild FTS index from existing Parquet files",
    )
    parser.add_argument(
        "--create-bundle",
        action="store_true",
        help=(
            "After setup, create a bundle.zip (parquet/ + fts.db) suitable "
            "for upload to S3 and use with BODS_*_S3_URL on Render"
        ),
    )
    args = parser.parse_args()

    gleif_url = os.environ.get("BODS_GLEIF_S3_URL", _DEFAULT_GLEIF_S3_URL)
    uk_psc_url = os.environ.get("BODS_UK_PSC_S3_URL", _DEFAULT_UK_PSC_S3_URL)
    output_dir: Path = args.output_dir.expanduser().resolve()

    sources_to_run = ["gleif", "uk_psc"] if args.source == "both" else [args.source]

    for source in sources_to_run:
        url = gleif_url if source == "gleif" else uk_psc_url
        log.info("=== Setting up %s ===", source)
        _setup_source(source, url, output_dir, skip_download=args.skip_download)
        if args.create_bundle:
            _create_bundle(output_dir / source)

    log.info("All done.")


if __name__ == "__main__":
    main()
