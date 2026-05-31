"""Build the Cyprus DRCOR SQLite database from data.gov.cy CSV exports.

The Department of Registrar of Companies and Intellectual Property (DRCOR)
publishes the *Register of Registered Companies, Commercial Names and
Cooperatives in Cyprus* as three monthly CSV distributions on data.gov.cy
(CC BY 4.0).  data.gov.cy exposes **no working query API** for these files
(the datastore endpoint 404s and the large CSVs are download-only), so this
script bakes them into a local SQLite DB that the adapter queries.

Download the three CSVs from the dataset page:
  https://data.gov.cy/el/dataset/mitroo-eggegrammenon-etaireion-emporikon-eponymion-kai-synetairismon-stin-kypro

  organisations         (~92 MB)   — one row per organisation
  registered office     (~20 MB)   — registered address per organisation
  officials             (~126 MB)  — directors / secretaries per organisation

Usage
-----
    python scripts/extract_cyprus.py \\
      --organisations-csv organisations_95.csv \\
      --office-csv registered_office_98.csv \\
      --officials-csv organisation_officials_84.csv \\
      --output cyprus.db

Then set ``CYPRUS_DRCOR_DB_FILE=/path/to/cyprus.db`` in the backend ``.env``.

The script preserves each CSV row verbatim as JSON (so the adapter's
candidate-tolerant ``_field`` lookup keeps working regardless of the exact
header spellings) and adds a normalised numeric ``reg_no_norm`` key column so
lookups match the HE number GLEIF provides.  It prints the detected columns so
you can confirm them against the adapter's ``_COLS``.

License: CC BY 4.0 — https://creativecommons.org/licenses/by/4.0/
Attribution: Department of Registrar of Companies and Intellectual Property
(Republic of Cyprus), via data.gov.cy.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("extract_cyprus")

# Candidate header names (case-insensitive) for the registration-number and
# organisation-name columns.  Mirror of the adapter's ``_COLS``.
_REG_CANDIDATES = ("registration_no", "registration_number", "reg_no", "regno")
_NAME_CANDIDATES = ("organisation_name", "name", "org_name", "organization_name")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def _norm(value: str) -> str:
    return re.sub(r"\D", "", str(value or "").strip())


def _open_csv(path: Path):
    """Open a CSV with best-effort encoding + delimiter sniffing.

    Cyprus exports are usually UTF-8, but Greek text is occasionally CP1253.
    """
    for enc in ("utf-8-sig", "utf-8", "cp1253", "latin-1"):
        try:
            fh = path.open("r", encoding=enc, newline="")
            sample = fh.read(8192)
            fh.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
                delim = dialect.delimiter
            except csv.Error:
                delim = ","
            return fh, delim
        except UnicodeDecodeError:
            continue
    raise SystemExit(f"Could not decode {path} with any known encoding")


def _pick(header: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {h.lower(): h for h in header}
    for cand in candidates:
        if cand in lowered:
            return lowered[cand]
    return None


def _load_table(
    conn: sqlite3.Connection,
    table: str,
    csv_path: Path,
    *,
    with_name: bool = False,
) -> tuple[int, list[str]]:
    fh, delim = _open_csv(csv_path)
    with fh:
        reader = csv.DictReader(fh, delimiter=delim)
        header = reader.fieldnames or []
        reg_col = _pick(header, _REG_CANDIDATES)
        if reg_col is None:
            raise SystemExit(
                f"{csv_path.name}: no registration-number column found in {header!r}. "
                f"Update _REG_CANDIDATES."
            )
        name_col = _pick(header, _NAME_CANDIDATES) if with_name else None

        if with_name:
            conn.execute(
                f"CREATE TABLE {table} (reg_no_norm TEXT, name TEXT, data TEXT)"
            )
        else:
            conn.execute(f"CREATE TABLE {table} (reg_no_norm TEXT, data TEXT)")

        rows = 0
        batch: list[tuple] = []
        for row in reader:
            reg = _norm(row.get(reg_col, ""))
            if not reg:
                continue
            payload = json.dumps(dict(row), ensure_ascii=False)
            if with_name:
                batch.append((reg, (row.get(name_col) or "").strip() if name_col else "", payload))
            else:
                batch.append((reg, payload))
            rows += 1
            if len(batch) >= 5000:
                _flush(conn, table, batch, with_name)
                batch = []
        if batch:
            _flush(conn, table, batch, with_name)

    conn.execute(f"CREATE INDEX idx_{table}_reg ON {table}(reg_no_norm)")
    conn.commit()
    return rows, header


def _flush(conn: sqlite3.Connection, table: str, batch: list[tuple], with_name: bool) -> None:
    if with_name:
        conn.executemany(f"INSERT INTO {table}(reg_no_norm, name, data) VALUES (?,?,?)", batch)
    else:
        conn.executemany(f"INSERT INTO {table}(reg_no_norm, data) VALUES (?,?)", batch)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build the Cyprus DRCOR SQLite DB.")
    ap.add_argument("--organisations-csv", required=True, type=Path)
    ap.add_argument("--office-csv", type=Path)
    ap.add_argument("--officials-csv", type=Path)
    ap.add_argument("--output", required=True, type=Path)
    args = ap.parse_args(argv)

    if args.output.exists():
        args.output.unlink()
    conn = sqlite3.connect(str(args.output))

    n_org, org_hdr = _load_table(conn, "organisations", args.organisations_csv, with_name=True)
    log.info("organisations: %d rows · columns=%s", n_org, org_hdr)

    if args.office_csv:
        n_off, off_hdr = _load_table(conn, "registered_office", args.office_csv)
        log.info("registered_office: %d rows · columns=%s", n_off, off_hdr)
    else:
        conn.execute("CREATE TABLE registered_office (reg_no_norm TEXT, data TEXT)")

    if args.officials_csv:
        n_ofc, ofc_hdr = _load_table(conn, "officials", args.officials_csv)
        log.info("officials: %d rows · columns=%s", n_ofc, ofc_hdr)
    else:
        conn.execute("CREATE TABLE officials (reg_no_norm TEXT, data TEXT)")

    # FTS5 over organisation names for the standalone search path.
    try:
        conn.execute(
            "CREATE VIRTUAL TABLE organisations_fts USING fts5(reg_no_norm, name)"
        )
        conn.execute(
            "INSERT INTO organisations_fts(reg_no_norm, name) "
            "SELECT reg_no_norm, name FROM organisations"
        )
    except sqlite3.OperationalError as exc:
        log.warning("FTS5 unavailable (%s) — name search will use LIKE fallback", exc)
    conn.commit()
    conn.close()

    log.info("Done → %s", args.output)
    log.info(
        "Confirm the printed column names match the adapter's _COLS "
        "(opencheck/sources/cyprus_drcor.py)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
