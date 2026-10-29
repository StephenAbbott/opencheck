"""Build the Cyprus DRCOR SQLite index from data.gov.cy's three monthly CSVs.

The Department of Registrar of Companies and Intellectual Property publishes
the *Register of Registered Companies, Commercial Names and Cooperatives in
Cyprus* as an EU High-Value Dataset on data.gov.cy (CC BY 4.0): three CSV
distributions, replaced monthly, whose file names carry a counter that moves
with every release (``organisations_102.csv``, ``registered_office_101.csv``,
``organisation_officials_88.csv`` on 29 Sep 2026). The portal exposes no query
API for them — ``/api/1/datastore/query`` is a 404 — so the register is read
from this index, which ``.github/workflows/refresh-cyprus-drcor-index.yml``
rebuilds monthly and ships as the ``cyprus-drcor-index`` release asset
(data.gov.cy answers a datacentre address, unlike data.gov.ua or Registrų
centras, so this one *can* be a workflow).

Usage
-----
    python3 scripts/build_cyprus_drcor_index.py --download --out cyprus_drcor.sqlite --gzip
    python3 scripts/build_cyprus_drcor_index.py --organisations organisations_102.csv \\
        --office registered_office_101.csv --officials organisation_officials_88.csv \\
        --cut 2026-09-29 --out cyprus_drcor.sqlite

``--download`` reads the dataset page for the three current links (the
counters cannot be guessed) and takes the cut from their ``Last-Modified``
headers; given files need ``--cut``, the day the portal published them.

What the files hold (29 Sep 2026 release, measured)
---------------------------------------------------
* ``organisations`` — 574,132 rows, one per organisation, keyed by
  ``(ORGANISATION_TYPE_CODE, REGISTRATION_NO)``: the number alone is NOT a
  key — company 1 and overseas company 1 are different organisations. Type
  codes: ``C`` company (497,410; the HE numbers GLEIF files as ``ΗΕ 449149``),
  ``B`` business name (62,406), ``P`` partnership (10,351), ``O`` overseas
  company (3,727), ``N`` partnership BN (225). Sub-type (private / public /
  guarantee; general / limited partnership), registration date
  (``DD/MM/YYYY``), the register's status in Greek with the date it took
  effect, and an address sequence number (31,865 rows have none).
* ``registered_office`` — 208,849 rows keyed by ``ADDRESS_SEQ_NO``: street,
  building, territory.
* ``organisation_officials`` — 1,185,101 rows on 525,207 organisations: a name
  and one of nine positions (director, secretary, owner of a business name,
  general / limited partner, alternate director, authorised person, assistant
  / deputy secretary). **No person/organisation flag, no country, no date.**
* 13 organisation rows and ~30 official rows are malformed (an unquoted
  newline in a name shifts the columns): a row whose type code is not one of
  the five is skipped and counted. A jump in that count means the format
  changed; the builder refuses to write when it passes ``MAX_MALFORMED``.

The index keeps the register's Greek wording in its columns; the adapter
holds the glosses and the status classes, so a reading can change without a
rebuild. ~574k organisations, ~1.2M officials, ~209k addresses: about 150 MB
on disk, a quarter of that gzipped.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import html
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.cyprus_drcor import (  # noqa: E402
    DATASET_URL,
    INDEX_SCHEMA_VERSION,
    TYPE_CODES,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_cyprus_drcor_index")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"

#: The three distributions, by the stem their file name starts with.
FILES = ("organisations", "registered_office", "organisation_officials")

#: Expected headers — a changed header is a changed format, so refuse.
HEADERS = {
    "organisations": [
        "ORGANISATION_NAME", "REGISTRATION_NO", "ORGANISATION_TYPE_CODE",
        "ORGANISATION_TYPE", "ORGANISATION_SUB_TYPE", "NAME_STATUS_CODE",
        "NAME_STATUS", "REGISTRATION_DATE", "ORGANISATION_STATUS",
        "ORGANISATION_STATUS_DATE", "ADDRESS_SEQ_NO",
    ],
    "registered_office": ["ADDRESS_SEQ_NO", "STREET", "BUILDING", "TERRITORY"],
    "organisation_officials": [
        "ORGANISATION_NAME", "REGISTRATION_NO", "ORGANISATION_TYPE_CODE",
        "ORGANISATION_TYPE", "PERSON_OR_ORGANISATION_NAME", "OFFICIAL_POSITION",
    ],
}

#: Below these the file is not the register (a challenge page, a cut file).
MIN_ROWS = {"organisations": 400_000, "registered_office": 150_000, "organisation_officials": 800_000}
#: Malformed rows per file above which the format has changed, not a name.
MAX_MALFORMED = 500

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE entity (
  type_code TEXT NOT NULL, reg_no TEXT NOT NULL,
  name TEXT, org_type TEXT, sub_type TEXT,
  registered_on TEXT, status TEXT, status_date TEXT, address_seq TEXT,
  PRIMARY KEY (type_code, reg_no)
);
CREATE TABLE address (seq TEXT PRIMARY KEY, street TEXT, building TEXT, territory TEXT);
CREATE TABLE official (type_code TEXT, reg_no TEXT, seq INTEGER, name TEXT, position TEXT);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
INDEXES = """
CREATE INDEX ix_official ON official(type_code, reg_no);
CREATE INDEX ix_entity_name ON entity(name);
"""


class RefusedInput(Exception):
    """The input is not the register as this builder knows it."""


def _iso_day(raw: str) -> str | None:
    """``DD/MM/YYYY`` → ``YYYY-MM-DD``; anything else → None."""
    m = re.fullmatch(r"\s*(\d{2})/(\d{2})/(\d{4})\s*", raw or "")
    if not m:
        return None
    d, mo, y = m.groups()
    try:
        datetime(int(y), int(mo), int(d))
    except ValueError:
        return None
    return f"{y}-{mo}-{d}"


def _clean(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def current_links(page_html: str) -> dict[str, str]:
    """The three current CSV links on the dataset page, by stem."""
    found: dict[str, str] = {}
    for href in re.findall(r'href="([^"]+\.csv)"', page_html, re.I):
        href = html.unescape(href)
        name = href.rsplit("/", 1)[-1]
        for stem in FILES:
            if name.startswith(stem + "_") and stem not in found:
                found[stem] = href if href.startswith("http") else "https://www.data.gov.cy" + href
    missing = [s for s in FILES if s not in found]
    if missing:
        raise RefusedInput(f"dataset page names no current file for {missing}")
    return found


def download(work: Path) -> tuple[dict[str, Path], str]:
    """Fetch the three current files into ``work``; return their paths and
    the cut (the newest ``Last-Modified`` day among them)."""
    req = urllib.request.Request(DATASET_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        page = resp.read().decode("utf-8", "replace")
    links = current_links(page)
    paths: dict[str, Path] = {}
    cut: datetime | None = None
    for stem, url in links.items():
        dest = work / url.rsplit("/", 1)[-1]
        log.info("downloading %s", url)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=120) as resp, open(dest, "wb") as fh:
            shutil.copyfileobj(resp, fh, 1 << 20)
            lm = resp.headers.get("Last-Modified")
        if lm:
            try:
                when = datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=UTC)
                cut = when if cut is None or when > cut else cut
            except ValueError:
                pass
        paths[stem] = dest
    if cut is None:
        raise RefusedInput("no Last-Modified header on the downloads — pass --cut")
    return paths, cut.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _reader(path: Path, stem: str):
    fh = open(path, encoding="utf-8-sig", newline="")
    reader = csv.DictReader(fh)
    if reader.fieldnames != HEADERS[stem]:
        fh.close()
        raise RefusedInput(f"{path.name}: header {reader.fieldnames!r} is not {HEADERS[stem]!r}")
    return fh, reader


def build(
    paths: dict[str, Path], out: Path, cut: str, *, min_rows: dict[str, int] | None = None
) -> dict[str, int]:
    """Build the index; ``min_rows`` overrides ``MIN_ROWS`` (tests pass ``{}``)."""
    floors = MIN_ROWS if min_rows is None else min_rows
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cut):
        raise RefusedInput(f"cut {cut!r} is not YYYY-MM-DD")
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    con.executescript(SCHEMA)
    stats = {
        "organisations": 0, "organisations_malformed": 0,
        "addresses": 0, "officials": 0, "officials_malformed": 0,
    }

    batch: list[tuple] = []
    fh, reader = _reader(paths["organisations"], "organisations")
    with fh:
        for row in reader:
            code = _clean(row["ORGANISATION_TYPE_CODE"])
            reg = _clean(row["REGISTRATION_NO"])
            if code not in TYPE_CODES or not reg.isdigit():
                stats["organisations_malformed"] += 1
                continue
            stats["organisations"] += 1
            batch.append((
                code, reg, _clean(row["ORGANISATION_NAME"]),
                _clean(row["ORGANISATION_TYPE"]) or None,
                _clean(row["ORGANISATION_SUB_TYPE"]) or None,
                _iso_day(row["REGISTRATION_DATE"]),
                _clean(row["ORGANISATION_STATUS"]) or None,
                _iso_day(row["ORGANISATION_STATUS_DATE"]),
                _clean(row["ADDRESS_SEQ_NO"]) or None,
            ))
            if len(batch) >= 50_000:
                con.executemany("INSERT OR REPLACE INTO entity VALUES (?,?,?,?,?,?,?,?,?)", batch)
                batch.clear()
    con.executemany("INSERT OR REPLACE INTO entity VALUES (?,?,?,?,?,?,?,?,?)", batch)
    batch.clear()
    log.info("organisations: %s rows (%s malformed), %.0fs",
             f"{stats['organisations']:,}", stats["organisations_malformed"], time.time() - t0)

    fh, reader = _reader(paths["registered_office"], "registered_office")
    with fh:
        for row in reader:
            seq = _clean(row["ADDRESS_SEQ_NO"])
            if not seq:
                continue
            stats["addresses"] += 1
            batch.append((
                seq, _clean(row["STREET"]).rstrip(",") or None,
                _clean(row["BUILDING"]).rstrip(",") or None,
                _clean(row["TERRITORY"]).rstrip(",") or None,
            ))
            if len(batch) >= 50_000:
                con.executemany("INSERT OR REPLACE INTO address VALUES (?,?,?,?)", batch)
                batch.clear()
    con.executemany("INSERT OR REPLACE INTO address VALUES (?,?,?,?)", batch)
    batch.clear()
    log.info("addresses: %s rows, %.0fs", f"{stats['addresses']:,}", time.time() - t0)

    seq_by_org: dict[tuple[str, str], int] = {}
    fh, reader = _reader(paths["organisation_officials"], "organisation_officials")
    with fh:
        for row in reader:
            code = _clean(row["ORGANISATION_TYPE_CODE"])
            reg = _clean(row["REGISTRATION_NO"])
            name = _clean(row["PERSON_OR_ORGANISATION_NAME"])
            if code not in TYPE_CODES or not reg.isdigit() or not name:
                stats["officials_malformed"] += 1
                continue
            key = (code, reg)
            n = seq_by_org.get(key, 0)
            seq_by_org[key] = n + 1
            stats["officials"] += 1
            batch.append((code, reg, n, name, _clean(row["OFFICIAL_POSITION"]) or None))
            if len(batch) >= 50_000:
                con.executemany("INSERT INTO official VALUES (?,?,?,?,?)", batch)
                batch.clear()
    con.executemany("INSERT INTO official VALUES (?,?,?,?,?)", batch)
    batch.clear()
    log.info("officials: %s rows on %s organisations (%s malformed), %.0fs",
             f"{stats['officials']:,}", f"{len(seq_by_org):,}", stats["officials_malformed"],
             time.time() - t0)

    for stem, key in (("organisations", "organisations"), ("registered_office", "addresses"),
                      ("organisation_officials", "officials")):
        if stats[key] < floors.get(stem, 0):
            con.close()
            tmp.unlink()
            raise RefusedInput(f"{stem}: only {stats[key]:,} rows — not the register")
    for key in ("organisations_malformed", "officials_malformed"):
        if stats[key] > MAX_MALFORMED:
            con.close()
            tmp.unlink()
            raise RefusedInput(f"{key}: {stats[key]:,} rows — the format has changed")

    con.commit()
    con.executescript(INDEXES)
    con.executemany(
        "INSERT OR REPLACE INTO meta VALUES (?,?)",
        [
            ("schema_version", INDEX_SCHEMA_VERSION),
            ("source", "data.gov.cy DRCOR register (three CSV distributions)"),
            ("source_files", ", ".join(paths[s].name for s in FILES)),
            ("cut", cut),
            ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            ("licence", "CC-BY-4.0 — Department of Registrar of Companies and Intellectual Property"),
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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="fetch the three current files from data.gov.cy")
    ap.add_argument("--work", type=Path, help="where --download puts the files (default: beside --out)")
    ap.add_argument("--organisations", type=Path)
    ap.add_argument("--office", type=Path)
    ap.add_argument("--officials", type=Path)
    ap.add_argument("--cut", help="the portal's publication day, YYYY-MM-DD (required with given files)")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)

    try:
        if args.download:
            work = args.work or args.out.parent
            work.mkdir(parents=True, exist_ok=True)
            paths, cut = download(work)
        else:
            if not (args.organisations and args.office and args.officials and args.cut):
                ap.error("--organisations, --office, --officials and --cut, or --download")
            paths = {
                "organisations": args.organisations,
                "registered_office": args.office,
                "organisation_officials": args.officials,
            }
            cut = args.cut
        stats = build(paths, args.out, cut)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], cut)
    for key, value in stats.items():
        log.info("  %-26s %s", key, f"{value:,}")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
