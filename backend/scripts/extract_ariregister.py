#!/usr/bin/env python3
"""Build the ariregister.db SQLite index from Estonian e-Business Register open data.

Downloads and extracts (or accepts pre-downloaded) the four open data files
published at https://avaandmed.ariregister.rik.ee/en/downloading-open-data
and builds a SQLite database indexed by ``registry_code`` (ariregistri_kood).

Files consumed:
  ettevotja_rekvisiidid__lihtandmed.csv             — entity basics (91 MB)
  ettevotja_rekvisiidid__osanikud.json              — shareholders (713 MB)
  ettevotja_rekvisiidid__kaardile_kantud_isikud.json — officers (956 MB)
  ettevotja_rekvisiidid__kasusaajad.json            — beneficial owners (326 MB)

Usage:
  # From a directory containing the four JSON/CSV files:
  python scripts/extract_ariregister.py \\
      --data-dir /path/to/ariregister_data \\
      --output   /path/to/ariregister.db

  # Or point to individual files:
  python scripts/extract_ariregister.py \\
      --lihtandmed  /path/to/ettevotja_rekvisiidid__lihtandmed.csv \\
      --osanikud    /path/to/ettevotja_rekvisiidid__osanikud.json \\
      --officers    /path/to/ettevotja_rekvisiidid__kaardile_kantud_isikud.json \\
      --kasusaajad  /path/to/ettevotja_rekvisiidid__kasusaajad.json \\
      --output      /path/to/ariregister.db

  # Exclude beneficial owners (for when public access rules change):
  python scripts/extract_ariregister.py --data-dir ... --output ... --no-beneficial-owners

Notes:
  - Requires ``ijson`` and ``pyarrow`` (pip install ijson pyarrow).
  - The lihtandmed file may be a .csv or .parquet; both are supported.
  - Files may be supplied as .zip archives — they are extracted automatically.
  - Processing all four files takes approximately 10-15 minutes on a laptop
    and produces a ~500 MB SQLite file.
  - Run ``ARIREGISTER_DB_FILE=/path/to/ariregister.db`` in your .env to
    activate the adapter.
"""

from __future__ import annotations

import argparse
import csv
import decimal
import io
import json
import logging
import sqlite3
import sys
import zipfile
from pathlib import Path
from typing import Any, Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------

def _json_dumps(obj: Any) -> str:
    """json.dumps that converts Decimal → float (ijson uses Decimal for all numbers)."""
    def _default(o: Any) -> Any:
        if isinstance(o, decimal.Decimal):
            return float(o)
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
    return json.dumps(obj, ensure_ascii=False, default=_default)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS entities (
    registry_code     TEXT PRIMARY KEY,
    name              TEXT,
    legal_form        TEXT,
    vat_number        TEXT,
    status            TEXT,
    registration_date TEXT,
    address           TEXT,
    link              TEXT,
    shareholders      TEXT,   -- JSON array
    officers          TEXT,   -- JSON array
    beneficial_owners TEXT    -- JSON array (may be empty if --no-beneficial-owners)
);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);
"""


# ---------------------------------------------------------------------------
# Helper: open file or zip member
# ---------------------------------------------------------------------------

def _open_path(path: Path) -> io.RawIOBase:
    """Open a regular file or the first member of a .zip archive."""
    if path.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(path)
        members = zf.namelist()
        if not members:
            raise ValueError(f"Empty zip archive: {path}")
        name = members[0]
        logger.info("Extracting %s from %s", name, path.name)
        return zf.open(name)
    return path.open("rb")


# ---------------------------------------------------------------------------
# Step 1: load entity basics from lihtandmed CSV / parquet
# ---------------------------------------------------------------------------

def _load_lihtandmed(path: Path) -> dict[str, dict[str, Any]]:
    """Return dict registry_code → entity basics from lihtandmed file."""
    suffix = path.suffix.lower()

    # Normalise: if zip, extract first to a temp buffer
    if suffix == ".zip":
        zf = zipfile.ZipFile(path)
        name = zf.namelist()[0]
        suffix = Path(name).suffix.lower()
        data = zf.read(name)
        fp: Any = io.BytesIO(data)
    else:
        fp = path.open("rb")

    entities: dict[str, dict[str, Any]] = {}

    if suffix == ".parquet":
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise SystemExit("pyarrow is required for .parquet files: pip install pyarrow")
        table = pq.read_table(fp)
        logger.info("lihtandmed parquet: %d rows, columns: %s", table.num_rows, table.schema.names)
        # Work with pyarrow columns directly — no pandas required.
        # Convert each column to a Python list once for fast indexed access.
        col_names = set(table.schema.names)
        cols = {name: table.column(name).to_pylist() for name in col_names}

        def _get(col: str, i: int) -> str:
            lst = cols.get(col)
            return (lst[i] if lst is not None and i < len(lst) else None) or ""

        for i in range(table.num_rows):
            raw_code = cols.get("ariregistri_kood", [None])[i] if "ariregistri_kood" in col_names else None
            code = str(int(raw_code)).zfill(8) if raw_code is not None else None
            if not code:
                continue
            # Address: newer exports use ads_normaliseeritud_taisaadress;
            # older parquet files use ettevotja_aadress.
            address = (
                _get("ads_normaliseeritud_taisaadress", i)
                or _get("ettevotja_aadress", i)
            ).strip()
            entities[code] = {
                "name": _get("nimi", i),
                "legal_form": _get("ettevotja_oiguslik_vorm", i),
                "vat_number": _get("kmkr_nr", i),
                "status": _get("ettevotja_staatus", i),
                "registration_date": _parse_date_ddmmyyyy(_get("ettevotja_esmakande_kpv", i)),
                "address": address,
                "link": _get("teabesysteemi_link", i),
            }
    else:
        # CSV — semicolon-separated, UTF-8 BOM
        text = fp.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        count = 0
        for row in reader:
            code = row.get("ariregistri_kood", "").strip().zfill(8)
            if not code:
                continue
            entities[code] = {
                "name": row.get("nimi") or "",
                "legal_form": row.get("ettevotja_oiguslik_vorm") or "",
                "vat_number": row.get("kmkr_nr") or "",
                "status": row.get("ettevotja_staatus") or "",
                "registration_date": _parse_date_ddmmyyyy(
                    row.get("ettevotja_esmakande_kpv") or ""
                ),
                "address": row.get("ads_normaliseeritud_taisaadress") or "",
                "link": row.get("teabesysteemi_link") or "",
            }
            count += 1
        logger.info("lihtandmed CSV: %d rows", count)

    return entities


# ---------------------------------------------------------------------------
# Step 2: stream osanikud / kaardile_kantud_isikud / kasusaajad JSON
# ---------------------------------------------------------------------------

def _stream_json_array(path: Path) -> Iterator[dict[str, Any]]:
    """Stream items from a top-level JSON array, memory-efficiently via ijson."""
    try:
        import ijson
    except ImportError:
        raise SystemExit("ijson is required: pip install ijson")

    fp = _open_path(path)
    for item in ijson.items(fp, "item"):
        yield item


def _build_lookup(path: Path, array_key: str, log_label: str) -> dict[str, list]:
    """Stream a JSON file and build registry_code → list[item] dict."""
    lookup: dict[str, list] = {}
    count = 0
    for company in _stream_json_array(path):
        code = str(company.get("ariregistri_kood") or "").zfill(8)
        if not code:
            continue
        data = company.get(array_key) or []
        if data:
            lookup[code] = data
        count += 1
        if count % 50_000 == 0:
            logger.info("%s: streamed %d companies …", log_label, count)
    logger.info("%s: total %d companies, %d with data", log_label, count, len(lookup))
    return lookup


# ---------------------------------------------------------------------------
# Date normalisation
# ---------------------------------------------------------------------------

def _parse_date_ddmmyyyy(s: str) -> str | None:
    """Convert DD.MM.YYYY → YYYY-MM-DD (ISO 8601), or return None."""
    if not s:
        return None
    s = s.strip()
    parts = s.split(".")
    if len(parts) == 3:
        d, m, y = parts
        if len(y) == 4 and d.isdigit() and m.isdigit() and y.isdigit():
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    # Maybe already ISO or another format — return as-is if plausible
    return s if len(s) >= 8 else None


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------

def build_db(
    lihtandmed_path: Path,
    osanikud_path: Path,
    officers_path: Path,
    kasusaajad_path: Path | None,
    output_path: Path,
) -> None:
    logger.info("=== Step 1: loading entity basics from lihtandmed ===")
    entities = _load_lihtandmed(lihtandmed_path)
    logger.info("Loaded %d entities", len(entities))

    logger.info("=== Step 2: streaming shareholders (osanikud) ===")
    shareholders = _build_lookup(osanikud_path, "osanikud", "osanikud")

    logger.info("=== Step 3: streaming officers (kaardile_kantud_isikud) ===")
    officers = _build_lookup(officers_path, "kaardile_kantud_isikud", "officers")

    beneficial_owners: dict[str, list] = {}
    if kasusaajad_path and kasusaajad_path.exists():
        logger.info("=== Step 4: streaming beneficial owners (kasusaajad) ===")
        beneficial_owners = _build_lookup(kasusaajad_path, "kasusaajad", "kasusaajad")
    else:
        logger.info("=== Step 4: skipping beneficial owners ===")

    logger.info("=== Step 5: writing SQLite database to %s ===", output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    conn = sqlite3.connect(str(output_path))
    conn.executescript(_DDL)

    batch: list[tuple] = []
    batch_size = 5_000
    written = 0

    # All registry codes: union of entity basics + shareholder/officer data
    all_codes = set(entities.keys()) | set(shareholders.keys()) | set(officers.keys())

    for code in all_codes:
        ent = entities.get(code, {})
        row = (
            code,
            ent.get("name") or "",
            ent.get("legal_form") or "",
            ent.get("vat_number") or "",
            ent.get("status") or "",
            ent.get("registration_date"),
            ent.get("address") or "",
            ent.get("link") or "",
            _json_dumps(shareholders.get(code) or []),
            _json_dumps(officers.get(code) or []),
            _json_dumps(beneficial_owners.get(code) or []),
        )
        batch.append(row)
        if len(batch) >= batch_size:
            conn.executemany(
                "INSERT OR REPLACE INTO entities VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                batch,
            )
            conn.commit()
            written += len(batch)
            batch = []
            logger.info("  written %d rows …", written)

    if batch:
        conn.executemany(
            "INSERT OR REPLACE INTO entities VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            batch,
        )
        conn.commit()
        written += len(batch)

    conn.close()
    logger.info("Done — %d rows written to %s", written, output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _find_file(data_dir: Path, patterns: list[str]) -> Path | None:
    for pattern in patterns:
        matches = list(data_dir.glob(pattern))
        if matches:
            return matches[0]
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build ariregister.db from Estonian e-Business Register open data."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Directory containing the open data files (auto-detected filenames).",
    )
    parser.add_argument("--lihtandmed", type=Path, help="Path to lihtandmed CSV or parquet.")
    parser.add_argument("--osanikud", type=Path, help="Path to osanikud JSON.")
    parser.add_argument("--officers", type=Path, help="Path to kaardile_kantud_isikud JSON.")
    parser.add_argument(
        "--kasusaajad", type=Path, help="Path to kasusaajad JSON (beneficial owners)."
    )
    parser.add_argument(
        "--no-beneficial-owners",
        action="store_true",
        help="Skip beneficial owner data entirely (for when public access rules change).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/ariregister.db"),
        help="Output SQLite file (default: data/ariregister.db).",
    )
    args = parser.parse_args()

    # Resolve file paths
    data_dir: Path | None = args.data_dir

    lihtandmed = args.lihtandmed or (
        data_dir
        and _find_file(
            data_dir,
            [
                "*lihtandmed*.parquet",
                "*lihtandmed*.csv",
                "*lihtandmed*.zip",
            ],
        )
    )
    osanikud = args.osanikud or (
        data_dir and _find_file(data_dir, ["*osanikud*.json", "*osanikud*.zip"])
    )
    officers = args.officers or (
        data_dir
        and _find_file(
            data_dir,
            ["*kaardile_kantud_isikud*.json", "*kaardile_kantud_isikud*.zip"],
        )
    )
    kasusaajad: Path | None = None
    if not args.no_beneficial_owners:
        kasusaajad = args.kasusaajad or (
            data_dir and _find_file(data_dir, ["*kasusaajad*.json", "*kasusaajad*.zip"])
        )

    missing = []
    if not lihtandmed:
        missing.append("lihtandmed (CSV or parquet)")
    if not osanikud:
        missing.append("osanikud (JSON)")
    if not officers:
        missing.append("kaardile_kantud_isikud (JSON)")
    if missing:
        logger.error("Could not find required files: %s", ", ".join(missing))
        logger.error("Pass --data-dir or individual --lihtandmed / --osanikud / --officers flags.")
        sys.exit(1)

    build_db(
        lihtandmed_path=lihtandmed,
        osanikud_path=osanikud,
        officers_path=officers,
        kasusaajad_path=kasusaajad,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
