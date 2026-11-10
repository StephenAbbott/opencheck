#!/usr/bin/env python3
"""Build the ESTMA payments index from Statistics Canada's SDMX-CSV.

    python3 scripts/build_estma_index.py --download --out estma.sqlite --gzip
    python3 scripts/build_estma_index.py --csv STC_DF_ESTMA-eng.csv \\
        --cut 2026-11-05 --out estma.sqlite

What it reads
-------------
Natural Resources Canada collects the reports filed under the Extractive
Sector Transparency Measures Act (ESTMA): each year an extractive company
reports its payments to governments, by payee and by project, in Canadian
dollars. Statistics Canada republishes every report as one SDMX-CSV
(dataflow ``STC:DF_ESTMA(1.0)``), rebuilt weekly, keyless, ~53 MB:

    https://www150.statcan.gc.ca/n1/pub/71-607-x/2022008/STC_DF_ESTMA-eng.csv

Licence: Statistics Canada Open Licence (attribution).

The file, measured on the 5 Nov 2026 release
--------------------------------------------
* 114,623 rows, 1,141 reporting entities keyed by the ESTMA ID
  (``E405435: SUNCOR ENERGY INC.`` — the ID is stable across renames). 36
  codes carry a suffix (``E425485_1: FOSTERVILLE GOLD MINE PTY LTD. (1)``):
  StatCan's second code for an entity whose reports moved to another
  parent. They are kept as their own entities, as filed.
* Every value is in **CAD, unit multiplier 0**; the builder refuses
  anything else rather than scale it.
* A payment is filed twice: once against its **payee** (a government) and
  once against its **project**. The two views are stored apart and never
  added together; a report's total is the sum of its payee rows.
* An entity whose payments are included in another entity's report has
  rows with a **blank value** that name the reporting entity in ``PARENT``.
  Valued rows never carry ``PARENT``. Those rows say "included in that
  report", never "paid nothing".
* One entity can file several reports for one year (a stub period beside a
  full year, a refiled report): each is its own row in ``report``, keyed by
  link, report type and period, and never summed with another.
* ``SUB_PAYEE_PROJECT`` (``F_1: Fees_1``…) numbers several lines of one
  payment type to one payee; they are summed into one line here.

The index carries what the adapter reads — entities, reports and payment
lines — and a ``meta`` table with the two clocks (``cut`` = the CSV's
``Last-Modified`` day, StatCan's publication; ``built_at`` = ours). It
does not carry the LEI: that join is the reviewed crosswalk committed at
``opencheck/data/estma_lei_crosswalk.csv``, so a crosswalk change needs no
rebuild and a rebuild cannot move a match.

Standard library only.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import re
import shutil
import sqlite3
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

log = logging.getLogger("build_estma_index")

SOURCE_URL = "https://www150.statcan.gc.ca/n1/pub/71-607-x/2022008/STC_DF_ESTMA-eng.csv"
DATAFLOW = "STC:DF_ESTMA(1.0)"

#: ``meta.schema_version`` written here and required by the adapter.
SCHEMA_VERSION = "1"

#: The header the builder accepts, exactly. A changed column is a changed
#: file, and the builder refuses rather than guess.
HEADER = [
    "STRUCTURE",
    "STRUCTURE_ID",
    "ACTION",
    "FREQ: Frequency",
    "REF_CONTINENT: Continent",
    "REF_AREA: Country",
    "LEGAL_ENTITY: Legal entity",
    "JURISDICTION: Jurisdiction",
    "PAYEE_PROJECT: Payee project name",
    "SUB_PAYEE_PROJECT: Name of the sub project payee",
    "PAYEE_PROJECT_TYPE: Payee project type",
    "PAYMENT_TYPE: Payment type",
    "TIME_PERIOD: Year",
    "OBS_VALUE",
    "REPYEARSTART: Fiscal year start",
    "REPYEAREND: Fiscal year end",
    "REPORT_TYPE: Report type",
    "TOTAL_NOTE: Total notes",
    "UNIT_MEASURE: Unit of measure",
    "UNIT_MULT: Unit multiplier",
    "WEB_LINK: Web link",
    "PARENT: Parent company",
    "NOTES_EXIST: Addtional notes",
]

#: Below these the file is not the ESTMA dataset (an error page, a cut file).
MIN_ROWS = 50_000
MIN_ENTITIES = 500

_ESTMA_ID = re.compile(r"^E\d{6}(?:_\d+)?$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class RefusedInput(Exception):
    """The input is not the dataset the builder knows; nothing is written."""


def split_code(value: str) -> tuple[str, str]:
    """``"E405435: SUNCOR ENERGY INC."`` → ``("E405435", "SUNCOR ENERGY INC.")``.

    SDMX-CSV writes each coded column as ``code: label``. A value with no
    ``": "`` is a bare code (or empty)."""
    value = (value or "").strip()
    code, sep, label = value.partition(": ")
    if not sep:
        return value, ""
    return code.strip(), label.strip()


def download(work: Path) -> tuple[Path, str]:
    """Fetch the CSV into *work*; the cut is its ``Last-Modified`` day."""
    work.mkdir(parents=True, exist_ok=True)
    dest = work / "STC_DF_ESTMA-eng.csv"
    req = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "OpenCheck ESTMA index builder"})
    with urllib.request.urlopen(req, timeout=300) as resp, open(dest, "wb") as out:  # noqa: S310
        shutil.copyfileobj(resp, out)
        lm = resp.headers.get("Last-Modified")
    if not lm:
        raise RefusedInput("no Last-Modified header on the download — pass --csv and --cut")
    cut = parsedate_to_datetime(lm).astimezone(timezone.utc).strftime("%Y-%m-%d")
    return dest, cut


def _schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE entity (
            estma_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            reports_payments INTEGER NOT NULL
        );
        CREATE TABLE report (
            report_id INTEGER PRIMARY KEY,
            estma_id TEXT NOT NULL,
            year TEXT NOT NULL,
            period_start TEXT,
            period_end TEXT,
            report_type TEXT,
            report_type_label TEXT,
            url TEXT,
            included_in_id TEXT,
            included_in_name TEXT,
            jurisdiction_code TEXT,
            jurisdiction_name TEXT
        );
        CREATE INDEX report_entity ON report (estma_id, year);
        CREATE INDEX report_included_in ON report (included_in_id, year);
        CREATE TABLE payment (
            report_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            country_code TEXT,
            country_name TEXT,
            payment_type TEXT NOT NULL,
            payment_type_label TEXT,
            amount REAL NOT NULL
        );
        CREATE INDEX payment_report ON payment (report_id);
        """
    )


def build(
    csv_path: Path,
    out: Path,
    cut: str,
    *,
    min_rows: int = MIN_ROWS,
    min_entities: int = MIN_ENTITIES,
) -> dict[str, object]:
    """Read the CSV and write the index to *out* (replacing it). Raises
    :class:`RefusedInput`, writing nothing, when the file is not the dataset."""
    if not _DAY.fullmatch(cut or ""):
        raise RefusedInput(f"cut {cut!r} is not YYYY-MM-DD")
    started = time.time()

    names: dict[str, str] = {}
    valued: set[str] = set()
    #: (estma_id, year, url, report_type, start, end) → report row fields
    reports: dict[tuple[str, ...], dict[str, str]] = {}
    #: (report key, kind, code, payment_type) → [name, country code, country
    #: name, payment-type label, amount]
    lines: dict[tuple, list] = defaultdict(lambda: ["", "", "", "", 0.0])
    rows = 0

    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header != HEADER:
            raise RefusedInput(f"unexpected header: {header!r}")
        col = {name: i for i, name in enumerate(header)}

        def get(row: list[str], name: str) -> str:
            return row[col[name]].strip() if len(row) > col[name] else ""

        for row in reader:
            if not row:
                continue
            rows += 1
            if get(row, "STRUCTURE_ID") != DATAFLOW:
                raise RefusedInput(f"row {rows}: dataflow {get(row, 'STRUCTURE_ID')!r}")
            unit, _ = split_code(get(row, "UNIT_MEASURE: Unit of measure"))
            mult, _ = split_code(get(row, "UNIT_MULT: Unit multiplier"))
            if unit != "CAD" or mult != "0":
                raise RefusedInput(f"row {rows}: unit {unit!r} × 10^{mult!r} — only CAD units are read")

            estma_id, name = split_code(get(row, "LEGAL_ENTITY: Legal entity"))
            if not _ESTMA_ID.fullmatch(estma_id):
                raise RefusedInput(f"row {rows}: ESTMA ID {estma_id!r}")
            names[estma_id] = name or names.get(estma_id, "")
            year = get(row, "TIME_PERIOD: Year")
            parent_id, parent_name = split_code(get(row, "PARENT: Parent company"))
            value = get(row, "OBS_VALUE")
            rtype, rlabel = split_code(get(row, "REPORT_TYPE: Report type"))
            juris, juris_name = split_code(get(row, "JURISDICTION: Jurisdiction"))
            rkey = (
                estma_id,
                year,
                get(row, "WEB_LINK: Web link"),
                rtype,
                get(row, "REPYEARSTART: Fiscal year start"),
                get(row, "REPYEAREND: Fiscal year end"),
            )
            if rkey not in reports:
                reports[rkey] = {
                    "report_type_label": rlabel,
                    "included_in_id": parent_id if value == "" else "",
                    "included_in_name": parent_name if value == "" else "",
                    "jurisdiction_code": juris,
                    "jurisdiction_name": juris_name,
                }
            if value == "":
                # Included in another entity's report: nothing to sum.
                continue
            valued.add(estma_id)
            kind_code, _ = split_code(get(row, "PAYEE_PROJECT_TYPE: Payee project type"))
            kind = {"PAYEE": "payee", "PROJECT": "project"}.get(kind_code)
            if kind is None:
                raise RefusedInput(f"row {rows}: payee/project type {kind_code!r}")
            code, pname = split_code(get(row, "PAYEE_PROJECT: Payee project name"))
            country, country_name = split_code(get(row, "REF_AREA: Country"))
            ptype, plabel = split_code(get(row, "PAYMENT_TYPE: Payment type"))
            try:
                amount = float(value)
            except ValueError as exc:
                raise RefusedInput(f"row {rows}: value {value!r}") from exc
            line = lines[(rkey, kind, code, country, ptype)]
            line[0] = pname or line[0]
            line[1] = country
            line[2] = country_name
            line[3] = plabel
            line[4] += amount

    if rows < min_rows or len(names) < min_entities:
        raise RefusedInput(f"{rows} rows / {len(names)} entities — not the ESTMA dataset")

    tmp = out.with_suffix(out.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(tmp)
    _schema(conn)
    conn.executemany(
        "INSERT INTO entity VALUES (?, ?, ?)",
        sorted((i, n, int(i in valued)) for i, n in names.items()),
    )
    report_ids: dict[tuple[str, ...], int] = {}
    for n, (rkey, r) in enumerate(sorted(reports.items()), start=1):
        report_ids[rkey] = n
        conn.execute(
            "INSERT INTO report VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                n, rkey[0], rkey[1], rkey[4] or None, rkey[5] or None, rkey[3] or None,
                r["report_type_label"] or None, rkey[2] or None,
                r["included_in_id"] or None, r["included_in_name"] or None,
                r["jurisdiction_code"] or None, r["jurisdiction_name"] or None,
            ),
        )
    conn.executemany(
        "INSERT INTO payment VALUES (?,?,?,?,?,?,?,?,?)",
        (
            (report_ids[k[0]], k[1], k[2], v[0], v[1] or None, v[2] or None, k[4], v[3] or None, round(v[4], 2))
            for k, v in sorted(lines.items(), key=lambda kv: (report_ids[kv[0][0]],) + kv[0][1:])
        ),
    )
    built_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.executemany(
        "INSERT INTO meta VALUES (?, ?)",
        [
            ("schema_version", SCHEMA_VERSION),
            ("cut", cut),
            ("built_at", built_at),
            ("source_url", SOURCE_URL),
            ("dataflow", DATAFLOW),
            ("rows", str(rows)),
            ("entities", str(len(names))),
            ("reporting_entities", str(len(valued))),
            ("reports", str(len(reports))),
        ],
    )
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    tmp.replace(out)
    return {
        "rows": rows,
        "entities": len(names),
        "reporting_entities": len(valued),
        "reports": len(reports),
        "lines": len(lines),
        "megabytes": round(out.stat().st_size / 1e6, 1),
        "seconds": round(time.time() - started, 1),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="fetch the CSV from Statistics Canada")
    ap.add_argument("--csv", type=Path, help="a CSV already on disk (needs --cut)")
    ap.add_argument("--cut", help="StatCan's publication day, YYYY-MM-DD (required with --csv)")
    ap.add_argument("--work", type=Path, default=Path("estma-work"), help="download directory")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        if args.download:
            csv_path, cut = download(args.work)
        else:
            if not (args.csv and args.cut):
                ap.error("--csv and --cut, or --download")
            csv_path, cut = args.csv, args.cut
        stats = build(csv_path, args.out, cut)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s: %s (cut %s)", args.out, stats, cut)
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst)
        log.info("wrote %s (%s MB)", gz_path, round(gz_path.stat().st_size / 1e6, 1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
