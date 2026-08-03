"""Build (or monthly-refresh) entity_pages.sqlite from GLEIF Golden Copy files.

The database behind the SEO entity pages (``opencheck/entity_pages.py``).
Two modes:

* ``--full`` (default) — download the latest LEI2 + RR Golden Copy **full**
  files (~475MB + ~23MB zipped CSV; ~3.4M records) and rebuild from scratch.
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

Usage (from backend/):
    uv run python scripts/build_entity_pages_db.py --out data/entity_pages.sqlite
    uv run python scripts/build_entity_pages_db.py --out data/entity_pages.sqlite --delta LastMonth
    uv run python scripts/build_entity_pages_db.py --out /tmp/sample.sqlite --sample 50000
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencheck.entity_pages import SCHEMA, slugify_name  # noqa: E402

PUBLISHES_API = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"

# Exact LEI2 CSV column names (LEI_3.1 CDF; verified against a live delta
# file 2026-08-03). The reader is name-based, so column reordering upstream
# is harmless; a renamed column fails loudly in _require().
COL_LEI = "LEI"
COL_NAME = "Entity.LegalName"
COL_TRANSLIT = "Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.1"
COL_CITY = "Entity.LegalAddress.City"
COL_REGION = "Entity.LegalAddress.Region"
COL_COUNTRY = "Entity.LegalAddress.Country"
COL_JURISDICTION = "Entity.LegalJurisdiction"
COL_STATUS = "Entity.EntityStatus"
COL_LEGAL_FORM = "Entity.LegalForm.EntityLegalFormCode"
COL_SUCCESSOR = "Entity.SuccessorEntity.1.SuccessorLEI"
COL_FIRST_REG = "Registration.InitialRegistrationDate"
COL_LAST_UPDATE = "Registration.LastUpdateDate"
COL_REG_STATUS = "Registration.RegistrationStatus"

RR_START = "Relationship.StartNode.NodeID"       # the child
RR_END = "Relationship.EndNode.NodeID"           # the parent
RR_TYPE = "Relationship.RelationshipType"
RR_REG_STATUS = "Registration.RegistrationStatus"

RR_DIRECT = "IS_DIRECTLY_CONSOLIDATED_BY"
RR_ULTIMATE = "IS_ULTIMATELY_CONSOLIDATED_BY"

BATCH = 5_000


def _require(row: dict, col: str) -> None:
    if col not in row:
        raise SystemExit(
            f"Expected column {col!r} missing from the CSV — has the GLEIF "
            "CDF version changed? Compare the header against the COL_* "
            "constants in this script."
        )


def _open_csv(path: Path) -> Iterator[dict[str, str]]:
    """DictReader over a CSV file, transparently unwrapping a .zip."""
    if path.suffix == ".zip":
        zf = zipfile.ZipFile(path)
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            raise SystemExit(f"No CSV inside {path}")
        stream = io.TextIOWrapper(zf.open(names[0]), encoding="utf-8-sig")
    else:
        stream = open(path, encoding="utf-8-sig")  # noqa: SIM115 — handed to DictReader; lives as long as iteration
    return csv.DictReader(stream)


def _download(url: str, dest_dir: Path, label: str) -> Path:
    import httpx

    dest = dest_dir / url.rsplit("/", 1)[-1]
    print(f"downloading {label}: {url}")
    with httpx.stream("GET", url, timeout=1800.0, follow_redirects=True) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)
    print(f"  -> {dest} ({dest.stat().st_size:,} bytes)")
    return dest


def _latest_publish() -> dict:
    import httpx

    resp = httpx.get(PUBLISHES_API, params={"page[size]": 1}, timeout=60.0)
    resp.raise_for_status()
    return resp.json()["data"][0]


def _entity_tuple(row: dict[str, str]) -> tuple:
    lei = row[COL_LEI].strip().upper()
    name = row.get(COL_NAME, "").strip() or lei
    slug = slugify_name(name, row.get(COL_TRANSLIT, "").strip() or None)
    return (
        lei,
        name,
        slug,
        row.get(COL_STATUS, "").strip() or None,
        row.get(COL_REG_STATUS, "").strip() or None,
        row.get(COL_JURISDICTION, "").strip() or None,
        row.get(COL_LEGAL_FORM, "").strip() or None,
        row.get(COL_CITY, "").strip() or None,
        row.get(COL_REGION, "").strip() or None,
        row.get(COL_COUNTRY, "").strip() or None,
        row.get(COL_FIRST_REG, "").strip() or None,
        row.get(COL_LAST_UPDATE, "").strip() or None,
        row.get(COL_SUCCESSOR, "").strip() or None,
    )


_UPSERT = """
INSERT INTO entities (
    lei, name, slug, entity_status, registration_status, jurisdiction,
    legal_form, city, region, country, first_registered, last_updated,
    successor_lei
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(lei) DO UPDATE SET
    name=excluded.name, slug=excluded.slug,
    entity_status=excluded.entity_status,
    registration_status=excluded.registration_status,
    jurisdiction=excluded.jurisdiction, legal_form=excluded.legal_form,
    city=excluded.city, region=excluded.region, country=excluded.country,
    first_registered=excluded.first_registered,
    last_updated=excluded.last_updated, successor_lei=excluded.successor_lei
"""


def load_lei2(conn: sqlite3.Connection, path: Path, sample: int | None = None) -> int:
    reader = _open_csv(path)
    count = 0
    batch: list[tuple] = []
    first = True
    for row in reader:
        if first:
            for col in (COL_LEI, COL_NAME, COL_REG_STATUS, COL_LAST_UPDATE):
                _require(row, col)
            first = False
        if not row.get(COL_LEI, "").strip():
            continue
        batch.append(_entity_tuple(row))
        count += 1
        if len(batch) >= BATCH:
            conn.executemany(_UPSERT, batch)
            batch.clear()
            if count % 200_000 == 0:
                conn.commit()
                print(f"  {count:,} entity records…")
        if sample is not None and count >= sample:
            break
    if batch:
        conn.executemany(_UPSERT, batch)
    conn.commit()
    return count


def load_rr(conn: sqlite3.Connection, path: Path) -> int:
    """Apply direct/ultimate parents. Runs after load_lei2 (UPDATE by child LEI)."""
    reader = _open_csv(path)
    count = 0
    direct: list[tuple[str, str]] = []
    ultimate: list[tuple[str, str]] = []
    first = True

    def flush() -> None:
        if direct:
            conn.executemany(
                "UPDATE entities SET direct_parent_lei = ? WHERE lei = ?", direct
            )
            direct.clear()
        if ultimate:
            conn.executemany(
                "UPDATE entities SET ultimate_parent_lei = ? WHERE lei = ?", ultimate
            )
            ultimate.clear()

    for row in reader:
        if first:
            for col in (RR_START, RR_END, RR_TYPE):
                _require(row, col)
            first = False
        # Only published relationship records carry a live parent link.
        status = row.get(RR_REG_STATUS, "").strip().upper()
        if status and status != "PUBLISHED":
            continue
        child = row.get(RR_START, "").strip().upper()
        parent = row.get(RR_END, "").strip().upper()
        rtype = row.get(RR_TYPE, "").strip().upper()
        if not child or not parent:
            continue
        if rtype == RR_DIRECT:
            direct.append((parent, child))
        elif rtype == RR_ULTIMATE:
            ultimate.append((parent, child))
        else:
            continue
        count += 1
        if len(direct) + len(ultimate) >= BATCH:
            flush()
    flush()
    conn.commit()
    return count


def write_meta(conn: sqlite3.Connection, **values: str) -> None:
    conn.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        list(values.items()),
    )
    conn.commit()


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
    ap.add_argument("--sample", type=int, help="Stop after N entity records (dev/testing).")
    ap.add_argument("--skip-rr", action="store_true", help="Skip relationship (parent) data.")
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    publish: dict | None = None
    publish_date = ""
    if args.lei2_file is None or (args.rr_file is None and not args.skip_rr):
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
            conn.executescript(SCHEMA)  # no-op on an up-to-date DB
        else:
            if args.delta:
                print("note: --delta but no existing DB; building fresh from the delta.")
            build_path = out.with_suffix(".building")
            build_path.unlink(missing_ok=True)
            conn = sqlite3.connect(build_path)
            conn.executescript(SCHEMA)

        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA journal_mode=MEMORY")

        n_entities = load_lei2(conn, lei2_path, sample=args.sample)
        print(f"entities loaded/updated: {n_entities:,}")

        if not args.skip_rr:
            rr_path = file_for("rr", args.rr_file)
            n_rel = load_rr(conn, rr_path)
            print(f"relationship records applied: {n_rel:,}")

        write_meta(
            conn,
            built_at=datetime.now(UTC).isoformat(timespec="seconds"),
            # Empty when built from local files — consumers (sitemap lastmod,
            # page footer) only render it when it is a real date.
            source_publish_date=publish_date,
            source="publish API" if publish else "local files",
            mode=args.delta or "full",
            record_count=str(conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]),
        )
        total = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        conn.execute("PRAGMA optimize")
        conn.close()

        if not in_place:
            # Atomic hand-over: the live DB is never a half-written file.
            out.with_suffix(".building").replace(out)

    print(json.dumps({"db": str(out), "entities": total, "publish": publish_date}))


if __name__ == "__main__":
    main()
