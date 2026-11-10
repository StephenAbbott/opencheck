"""Build the Israel Corporations Authority SQLite index from data.gov.il.

The Israel Corporations Authority (רשות התאגידים) publishes its registers of
companies (``ica_companies``, "Other (Open)") and partnerships
(``ica_partnerships``, CC BY) on data.gov.il, regenerated daily at about
02:10 UTC. The CSV download answers 403 to a datacentre address (CloudFront),
but the CKAN datastore API answers everywhere, so the builder pages
``datastore_search`` 32,000 rows at a time — the whole company register in
about three minutes. ``.github/workflows/refresh-ica-israel-index.yml``
rebuilds it daily and ships it as the ``ica-israel-index`` release asset.

Usage
-----
    python3 scripts/build_ica_israel_index.py --download --out ica_israel.sqlite --gzip

What the files hold (10 Nov 2026, measured)
-------------------------------------------
* companies — 733,463 rows: number (9 digits, 51/52/56…), Hebrew name, English
  name (46 %), type (Israeli private / public / bond company; foreign private
  / public), status and sub-status, incorporation date (``DD/MM/YYYY``),
  government-company flag (126), limitation, the violating-company mark
  (189,574 ``מפרה``, 30 ``התראה``), the last annual report filed, address.
* partnerships — 29,301 rows (53/54/55…), the same shape without the
  company-only fields.
* ``~`` stands for the Hebrew quotation mark in every text field; the builder
  writes ``"``.
* Left out on purpose: ``אצל`` (care-of — about 230,000 rows hold what look
  like people's names), the company purpose and its free-text description.

The index keeps the register's Hebrew wording; the adapter holds the glosses
and the status classes, so a reading can change without a rebuild.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.ica_israel import INDEX_SCHEMA_VERSION  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_ica_israel_index")

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"
API = "https://data.gov.il/api/3/action"
PAGE = 32_000

#: data.gov.il dataset name and datastore resource id, per register.
DATASETS = {
    "company": ("ica_companies", "f004176c-b85f-4542-8901-7b3176f9a054"),
    "partnership": ("ica_partnerships", "139aa193-fabb-4f6b-a71b-0bb40fd73eb2"),
}

#: The fields each register's rows must carry — a changed field set is a
#: changed format, so refuse. (Extra fields are allowed and ignored.)
FIELDS = {
    "company": (
        "מספר חברה", "שם חברה", "שם באנגלית", "סוג תאגיד", "סטטוס חברה",
        "תאריך התאגדות", "חברה ממשלתית", "מגבלות", "מפרה",
        "שנה אחרונה של דוח שנתי (שהוגש)", "שם עיר", "שם רחוב", "מספר בית",
        "מיקוד", "ת.ד.", "מדינה", "תת סטטוס", "קוד סטטוס חברה", "קוד סוג חברה",
    ),
    "partnership": (
        "מספר שותפות", "שם שותפות", "שם באנגלית", "סוג תאגיד", "סטטוס תאגיד",
        "תאריך התאגדות", "ישוב", "רחוב", "מספר בית", "מיקוד", "ת.ד", "מדינה",
        "קוד סטטוס שותפות", "קוד סוג חברה",
    ),
}

#: Below these the harvest is not the register.
MIN_ROWS = {"company": 600_000, "partnership": 20_000}
#: Rows whose number is not 9 digits, above which the format has changed.
MAX_MALFORMED = 200
#: The two files are regenerated together; a wider gap means one is stale.
MAX_CUT_GAP_DAYS = 1

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE entity (
  number TEXT PRIMARY KEY, kind TEXT NOT NULL,
  name_he TEXT, name_en TEXT, type_code INTEGER, type_words TEXT,
  status_code INTEGER, status_words TEXT, sub_status TEXT,
  incorporated TEXT, government INTEGER, limitation TEXT, violating TEXT,
  last_report_year INTEGER,
  city TEXT, street TEXT, house TEXT, postcode TEXT, po_box TEXT, country TEXT
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
# No name index: the adapter's search is a substring LIKE, which a b-tree
# index cannot serve (a scan of the 760k rows takes ~0.3 s).


class RefusedInput(Exception):
    """The input is not the register as this builder knows it."""


def _text(value: Any) -> str | None:
    """A cleaned text field: ``~`` back to ``"``, whitespace collapsed."""
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value).replace("~", '"')).strip()
    return text or None


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _iso_day(raw: Any) -> str | None:
    """``DD/MM/YYYY`` → ``YYYY-MM-DD``; anything else → None."""
    m = re.fullmatch(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*", str(raw or ""))
    if not m:
        return None
    try:
        return datetime(int(m[3]), int(m[2]), int(m[1])).date().isoformat()
    except ValueError:
        return None


def _government(raw: Any) -> int | None:
    text = _text(raw)
    return {"כן": 1, "לא": 0}.get(text or "")


def _postcode(raw: Any) -> str | None:
    if raw in (None, ""):
        return None
    text = str(raw).strip()
    if re.fullmatch(r"\d+(\.0)?", text):
        return text.split(".")[0]
    return _text(text)


def entity_row(kind: str, rec: dict[str, Any]) -> tuple | None:
    """One datastore record → the ``entity`` row, or None when malformed."""
    if kind == "company":
        number = re.sub(r"\D", "", str(rec.get("מספר חברה") or ""))
        name = rec.get("שם חברה")
        status = rec.get("סטטוס חברה")
        status_code = rec.get("קוד סטטוס חברה")
        city, street = rec.get("שם עיר"), rec.get("שם רחוב")
        po_box = rec.get("ת.ד.")
        government = _government(rec.get("חברה ממשלתית"))
        limitation = _text(rec.get("מגבלות"))
        violating = _text(rec.get("מפרה"))
        last_report = _int(rec.get("שנה אחרונה של דוח שנתי (שהוגש)"))
        sub_status = _text(rec.get("תת סטטוס"))
    else:
        number = re.sub(r"\D", "", str(rec.get("מספר שותפות") or ""))
        name = rec.get("שם שותפות")
        status = rec.get("סטטוס תאגיד")
        status_code = rec.get("קוד סטטוס שותפות")
        city, street = rec.get("ישוב"), rec.get("רחוב")
        po_box = rec.get("ת.ד")
        government = limitation = violating = last_report = sub_status = None
    if len(number) != 9:
        return None
    return (
        number, kind, _text(name), _text(rec.get("שם באנגלית")),
        _int(rec.get("קוד סוג חברה")), _text(rec.get("סוג תאגיד")),
        _int(status_code), _text(status), sub_status,
        _iso_day(rec.get("תאריך התאגדות")), government, limitation, violating,
        last_report,
        _text(city), _text(street), _text(rec.get("מספר בית")),
        _postcode(rec.get("מיקוד")), _text(po_box), _text(rec.get("מדינה")),
    )


def _fill(con: sqlite3.Connection, rows: dict[str, Iterable[dict[str, Any]]], floors: dict[str, int], stats: dict[str, int], t0: float) -> None:
    for kind in ("company", "partnership"):
        count = malformed = 0
        batch: list[tuple] = []
        for rec in rows[kind]:
            if count == 0 and malformed == 0:
                missing = [f for f in FIELDS[kind] if f not in rec]
                if missing:
                    raise RefusedInput(f"{kind}: fields missing from the records: {', '.join(missing)}")
            row = entity_row(kind, rec)
            if row is None:
                malformed += 1
                continue
            count += 1
            batch.append(row)
            if len(batch) >= 50_000:
                con.executemany(f"INSERT OR REPLACE INTO entity VALUES ({','.join('?' * 20)})", batch)
                batch.clear()
        con.executemany(f"INSERT OR REPLACE INTO entity VALUES ({','.join('?' * 20)})", batch)
        stats[f"{kind}_rows"] = count
        stats[f"{kind}_malformed"] = malformed
        log.info("%s: %s rows (%s malformed), %.0fs", kind, f"{count:,}", malformed, time.time() - t0)
        if count < floors.get(kind, 0):
            raise RefusedInput(f"{kind}: only {count:,} rows — not the register")
        if malformed > MAX_MALFORMED:
            raise RefusedInput(f"{kind}: {malformed:,} malformed rows — the format has changed")



def build(
    rows: dict[str, Iterable[dict[str, Any]]],
    out: Path,
    cut: str,
    *,
    min_rows: dict[str, int] | None = None,
) -> dict[str, int]:
    """Build the index from datastore-shaped records per register.

    ``min_rows`` overrides ``MIN_ROWS`` (tests pass ``{}``).
    """
    floors = MIN_ROWS if min_rows is None else min_rows
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cut):
        raise RefusedInput(f"cut {cut!r} is not YYYY-MM-DD")
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    con.executescript(SCHEMA)
    stats: dict[str, int] = {}

    try:
        _fill(con, rows, floors, stats, t0)
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        raise

    con.commit()
    con.executemany(
        "INSERT OR REPLACE INTO meta VALUES (?,?)",
        [
            ("schema_version", INDEX_SCHEMA_VERSION),
            ("source", "data.gov.il ica_companies + ica_partnerships (CKAN datastore)"),
            ("cut", cut),
            ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            ("licence", "companies: data.gov.il Other (Open); partnerships: CC BY — Israel Corporations Authority"),
            *[(f"count_{k}", str(v)) for k, v in stats.items()],
        ],
    )
    con.commit()
    con.execute("VACUUM")
    con.close()
    os.replace(tmp, out)
    stats["seconds"] = int(time.time() - t0)
    stats["megabytes"] = int(os.path.getsize(out) / 1e6)
    return stats


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------


def _get(action: str, params: dict[str, Any], *, attempts: int = 4) -> dict[str, Any]:
    url = f"{API}/{action}?{urllib.parse.urlencode(params)}"
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=180) as resp:
                body = json.load(resp)
            if not body.get("success"):
                raise RefusedInput(f"{action}: success=false")
            return body["result"]
        except (OSError, ValueError) as exc:
            if attempt == attempts - 1:
                raise
            wait = 5 * 2**attempt
            log.warning("%s failed (%s); retrying in %ss", action, type(exc).__name__, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def resource_cut(dataset: str, resource_id: str) -> str:
    """The resource's ``last_modified`` day (UTC) on data.gov.il."""
    result = _get("package_show", {"id": dataset})
    for res in result.get("resources", []):
        if res.get("id") == resource_id:
            stamp = res.get("last_modified") or res.get("metadata_modified") or ""
            if re.match(r"\d{4}-\d{2}-\d{2}", stamp):
                return stamp[:10]
    raise RefusedInput(f"{dataset}: no last_modified for resource {resource_id}")


def harvest(resource_id: str) -> Iterator[dict[str, Any]]:
    """Every record of one datastore resource, in ``_id`` order. Refuses when
    the rows read differ from the ``total`` the API reported."""
    offset = 0
    total: int | None = None
    read = 0
    while True:
        result = _get(
            "datastore_search",
            {"resource_id": resource_id, "limit": PAGE, "offset": offset, "sort": "_id"},
        )
        if total is None:
            total = int(result.get("total") or 0)
        records = result.get("records") or []
        yield from records
        read += len(records)
        offset += PAGE
        if len(records) < PAGE:
            break
        time.sleep(1)
    if read != total:
        raise RefusedInput(f"{resource_id}: read {read:,} rows, the API reported {total:,}")


def download() -> tuple[dict[str, Iterable[dict[str, Any]]], str]:
    cuts = {kind: resource_cut(ds, rid) for kind, (ds, rid) in DATASETS.items()}
    days = sorted(datetime.fromisoformat(c) for c in cuts.values())
    if (days[-1] - days[0]).days > MAX_CUT_GAP_DAYS:
        raise RefusedInput(f"the two files are from different days: {cuts}")
    rows = {kind: harvest(rid) for kind, (_, rid) in DATASETS.items()}
    return rows, days[-1].date().isoformat()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="harvest both registers from data.gov.il")
    ap.add_argument("--companies", type=Path, help="a JSON-lines file of company records (testing)")
    ap.add_argument("--partnerships", type=Path, help="a JSON-lines file of partnership records (testing)")
    ap.add_argument("--cut", help="the files' publication day, YYYY-MM-DD (required with given files)")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)

    def _jsonl(path: Path) -> Iterator[dict[str, Any]]:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)

    try:
        if args.download:
            rows, cut = download()
        else:
            if not (args.companies and args.partnerships and args.cut):
                ap.error("--companies, --partnerships and --cut, or --download")
            rows = {"company": _jsonl(args.companies), "partnership": _jsonl(args.partnerships)}
            cut = args.cut
        stats = build(rows, args.out, cut)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], cut)
    for key, value in stats.items():
        log.info("  %-22s %s", key, f"{value:,}")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
