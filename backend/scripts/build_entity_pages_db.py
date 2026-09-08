"""Build (or monthly-refresh) entity_pages.sqlite from GLEIF Golden Copy files.

The database behind the SEO entity pages (``opencheck/entity_pages.py``).
Two modes:

* ``--full`` (default) — download the latest LEI2 + RR + REPEX Golden Copy
  **full** files (~480MB + ~23MB + ~59MB zipped CSV; ~3.4M LEI records,
  ~490k relationship records, ~6.3M reporting exceptions) and rebuild from
  scratch.
* ``--delta {LastMonth,LastWeek,LastDay,IntraDay}`` — download the matching
  delta files and upsert into an existing DB. ``--delta LastMonth`` is the
  monthly refresh: GLEIF's 31-day delta carries every new LEI issued and
  every record revised in the preceding month.

File discovery uses GLEIF's Golden Copy publish API
(``https://goldencopy.gleif.org/api/v2/golden-copies/publishes``) — free, no
registration, new publishes at 02:00/10:00/18:00 UTC daily. Offline / test
use: pass ``--lei2-file`` and ``--rr-file`` pointing at local CSVs (zipped
or plain) to skip the network entirely.

The build is streaming (csv → executemany batches), so memory stays flat
regardless of file size. A full build writes to a temp file and renames at
the end — an interrupted run never clobbers the live DB.

Phase 178 turned the file into a GLEIF mirror (see ``entity_pages.py``): each
entity row also carries ``detail_json`` — every Level 1 field the BODS mapper
reads — and the RR and REPEX files land in their own tables. The page columns
are unchanged, so entity pages, sitemaps and the browse hubs render exactly as
before. Applying a delta to a v1 file upgrades it in place.

The detail is loaded as JSON text and then, in ``compress_detail_column``,
deflated row-by-row against a dictionary sampled from the whole file and stored
in ``meta`` (697 → 149 bytes a row on the 2026-09-07 publish; the file goes
from 4.8 GB to 1.8 GB after the ``VACUUM`` a full build ends with). A delta
leaves its rows as text and the same pass compresses them with the dictionary
the file already carries. A full build of that publish took 12 minutes end to
end on a 2-vCPU container (downloads included; the compression pass is about
a quarter of it), and ``gzip -9`` of the result is 879 MB.

Usage (from backend/):
    uv run python scripts/build_entity_pages_db.py --out data/entity_pages.sqlite
    uv run python scripts/build_entity_pages_db.py --out data/entity_pages.sqlite --delta LastMonth
    uv run python scripts/build_entity_pages_db.py --out /tmp/sample.sqlite --sample 50000
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Phase 180: the loading code lives in the package so the in-process delta
# refresh shares it; everything the tests and this CLI used is re-exported.
from opencheck.entity_pages import SCHEMA_VERSION  # noqa: E402
from opencheck.mirror_build import (  # noqa: E402, F401
    ADDR_EXTRA_COLS,
    ADDR_HQ,
    ADDR_LEGAL,
    ADDR_LINE_COLS,
    BATCH,
    COL_DELETED_AT,
    COL_LEI,
    COL_NAME,
    COL_TRANSLIT,
    PUBLISHES_API,
    REPEX_CATEGORY,
    REPEX_LEI,
    RR_DIRECT,
    RR_ULTIMATE,
    _download,
    _iso_utc,
    _latest_publish,
    _open_csv,
    compress_detail_column,
    ensure_schema,
    entity_detail,
    load_lei2,
    load_repex,
    load_rr,
    recompute_parent_columns,
    write_meta,
)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--out", required=True, help="Path of the SQLite DB to write.")
    ap.add_argument(
        "--delta",
        choices=["LastMonth", "LastWeek", "LastDay", "IntraDay"],
        help="Upsert a delta into an existing DB instead of a full rebuild.",
    )
    ap.add_argument("--lei2-file", type=Path, help="Local LEI2 CSV (.csv or .zip); skips download.")
    ap.add_argument("--rr-file", type=Path, help="Local RR CSV (.csv or .zip); skips download.")
    ap.add_argument(
        "--repex-file", type=Path, help="Local REPEX CSV (.csv or .zip); skips download."
    )
    ap.add_argument("--sample", type=int, help="Stop after N entity records (dev/testing).")
    ap.add_argument("--skip-rr", action="store_true", help="Skip relationship (parent) data.")
    ap.add_argument(
        "--skip-repex", action="store_true", help="Skip reporting-exception data."
    )
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    publish: dict | None = None
    publish_date = ""
    needs_download = (
        args.lei2_file is None
        or (args.rr_file is None and not args.skip_rr)
        or (args.repex_file is None and not args.skip_repex)
    )
    if needs_download:
        publish = _latest_publish()
        publish_date = publish["publish_date"]
        print(f"latest GLEIF Golden Copy publish: {publish_date}")

    with tempfile.TemporaryDirectory(prefix="gleif-gc-") as tmp:
        tmp_dir = Path(tmp)

        def file_for(kind: str, local: Path | None) -> Path:
            if local is not None:
                return local
            assert publish is not None
            section = publish[kind]
            entry = (
                section["delta_files"][args.delta]["csv"]
                if args.delta
                else section["full_file"]["csv"]
            )
            return _download(entry["url"], tmp_dir, f"{kind} {args.delta or 'full'}")

        lei2_path = file_for("lei2", args.lei2_file)

        in_place = bool(args.delta and out.exists())
        if in_place:
            conn = sqlite3.connect(out)
            ensure_schema(conn)  # no-op on an up-to-date DB; upgrades a v1 file
        else:
            if args.delta:
                print("note: --delta but no existing DB; building fresh from the delta.")
            build_path = out.with_suffix(".building")
            build_path.unlink(missing_ok=True)
            conn = sqlite3.connect(build_path)
            ensure_schema(conn)

        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA journal_mode=MEMORY")

        n_entities = load_lei2(conn, lei2_path, sample=args.sample)
        print(f"entities loaded/updated: {n_entities:,}")

        if not args.skip_rr:
            rr_path = file_for("rr", args.rr_file)
            n_rel = load_rr(conn, rr_path, full=not in_place)
            print(f"relationship records applied: {n_rel:,}")

        if not args.skip_repex:
            repex_path = file_for("repex", args.repex_file)
            n_ex = load_repex(conn, repex_path)
            print(f"reporting exceptions applied: {n_ex:,}")

        n_compressed = compress_detail_column(conn)
        print(f"detail rows compressed: {n_compressed:,}")

        def _count(table: str) -> str:
            return str(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

        write_meta(
            conn,
            built_at=datetime.now(UTC).isoformat(timespec="seconds"),
            # Empty when built from local files — consumers (sitemap lastmod,
            # page footer) only render it when it is a real date.
            source_publish_date=publish_date,
            # Phase 178: the full publish timestamp is the mirror's watermark —
            # a delta refresh (Phase 180) picks the file whose window covers
            # the gap from here.
            source_publish_datetime=publish_date,
            source="publish API" if publish else "local files",
            mode=args.delta or "full",
            schema_version=SCHEMA_VERSION,
            detail_encoding="zlib+zdict",
            record_count=_count("entities"),
            relationship_count=_count("relationships"),
            exception_count=_count("reporting_exceptions"),
        )
        total = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        conn.execute("PRAGMA optimize")
        if not in_place:
            # The detail was written as text and compressed in place, which
            # leaves the freed space inside the file; a full build hands over
            # a compact one. (A delta touches too few rows to be worth it.)
            conn.execute("VACUUM")
        conn.close()

        if not in_place:
            # Atomic hand-over: the live DB is never a half-written file.
            out.with_suffix(".building").replace(out)

    print(json.dumps({"db": str(out), "entities": total, "publish": publish_date}))


if __name__ == "__main__":
    main()
