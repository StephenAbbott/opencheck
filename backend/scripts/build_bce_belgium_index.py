#!/usr/bin/env python3
"""Build the Belgian BCE / KBO index from the monthly KBO Open Data ZIP.

The Crossroads Bank for Enterprises (Banque-Carrefour des Entreprises /
Kruispuntbank van Ondernemingen) publishes its open data as one full ZIP a
month — ``KboOpenData_<extract>_<year>_<month>_Full.zip`` (~240 MB, nine
CSVs, 1.5 GB unzipped) — plus interim update files, behind a free account
(``j_spring_security_check``, a plain form login; the portal answers a
datacentre address). This script reads the **full** file, the register's
monthly cut, into one SQLite index the backend downloads at boot — the
``jar_lithuania`` / ``cyprus_drcor`` arrangement (the Belgium wiring,
29 Oct 2026; replaces Phase 35's ``extract_bce.py``).

Files read: ``enterprise.csv`` (status, juridical situation, type, form,
start date), ``denomination.csv`` (names per language and type),
``address.csv`` (the registered office), ``code.csv`` (the register's own
wording for every code, in NL / FR / DE / EN) and ``meta.csv`` (KBO's
``SnapshotDate`` — the cut — kept verbatim). Not read: activity, branch,
contact, establishment.

Usage::

    # From a downloaded ZIP:
    uv run python scripts/build_bce_belgium_index.py --zip KboOpenData_0140_2026_10_Full.zip \\
        --out data/bce_belgium.sqlite --gzip

    # Logging in and fetching the newest full file (KBO_OPEN_DATA_USERNAME /
    # KBO_OPEN_DATA_PASSWORD in the environment — what the workflow does):
    uv run python scripts/build_bce_belgium_index.py --download --work /tmp/bce \\
        --out data/bce_belgium.sqlite --gzip

Refuses (writes nothing) a ZIP missing a file, a CSV whose header is not the
register's, a file far below the register's size, or a ``meta.csv`` with no
``SnapshotDate`` — the SPA / wrong-link traps of the other builders.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Iterator

log = logging.getLogger("build_bce_belgium_index")

PORTAL = "https://kbopub.economie.fgov.be/kbo-open-data"
LOGIN_URL = f"{PORTAL}/static/j_spring_security_check"
FILES_URL = f"{PORTAL}/affiliation/xml/?files"
FULL_ZIP_RE = re.compile(r"KboOpenData_(\d{4})_(\d{4})_(\d{2})_Full\.zip")

INDEX_SCHEMA_VERSION = "1"

#: The exact header of each file read (KBO Open Data cookbook, v1.x — the
#: shape has been stable since 2014; a header that differs is refused).
HEADERS: dict[str, list[str]] = {
    "enterprise.csv": [
        "EnterpriseNumber", "Status", "JuridicalSituation", "TypeOfEnterprise",
        "JuridicalForm", "JuridicalFormCAC", "StartDate",
    ],
    "denomination.csv": ["EntityNumber", "Language", "TypeOfDenomination", "Denomination"],
    "address.csv": [
        "EntityNumber", "TypeOfAddress", "CountryNL", "CountryFR", "Zipcode",
        "MunicipalityNL", "MunicipalityFR", "StreetNL", "StreetFR", "HouseNumber",
        "Box", "ExtraAddressInfo", "DateStrikingOff",
    ],
    "code.csv": ["Category", "Code", "Language", "Description"],
    "meta.csv": ["Variable", "Value"],
}
#: Far below these and it is not the register (1.9M enterprises, 3.2M names,
#: 2.7M addresses on the October 2024 file).
MIN_ROWS = {"enterprise.csv": 1_200_000, "denomination.csv": 2_000_000, "address.csv": 1_500_000}
MAX_MALFORMED = 1_000

#: KBO language codes.
LANGUAGES = {"1": "fr", "2": "nl", "3": "de", "4": "en"}
#: Denomination types, in the order a display name is chosen: the official
#: name, then the commercial name, then the abbreviation.
DENOMINATION_PRIORITY = {"001": 0, "003": 1, "002": 2}
#: The registered office; the other types are establishment units and
#: a foreign company's branch.
REGISTERED_OFFICE = "REGO"


class RefusedInput(RuntimeError):
    """The input is not the register's published shape."""


SCHEMA = """
PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE entity (
    number      TEXT PRIMARY KEY,   -- ten digits, no dots
    status      TEXT,               -- AC / ST
    situation   TEXT,               -- JuridicalSituation code
    type        TEXT,               -- TypeOfEnterprise: 1 natural person, 2 legal person
    form        TEXT,               -- JuridicalForm code
    form_cac    TEXT,               -- JuridicalFormCAC code
    start_date  TEXT,               -- ISO day
    name_nl     TEXT,
    name_fr     TEXT,
    name_de     TEXT,
    name_en     TEXT,
    name_type   TEXT,               -- TypeOfDenomination of the names kept
    street      TEXT,
    house       TEXT,
    box         TEXT,
    zipcode     TEXT,
    municipality TEXT,
    country     TEXT
);
CREATE TABLE denomination (number TEXT NOT NULL, name TEXT NOT NULL);  -- build-time only
CREATE TABLE code (
    category    TEXT NOT NULL,
    code        TEXT NOT NULL,
    language    TEXT NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (category, code, language)
);
"""
#: Every name a legal person has filed — official, commercial, abbreviation,
#: in every language — is searchable; the entity row keeps one per language
#: for display. Natural-person enterprises are not name-searchable.
INDEXES = """
CREATE VIRTUAL TABLE entity_fts USING fts5(number UNINDEXED, name, tokenize='unicode61');
INSERT INTO entity_fts (number, name)
    SELECT d.number, d.name FROM denomination d JOIN entity e ON e.number = d.number
    WHERE e.type = '2';
DROP TABLE denomination;
"""


def normalise_number(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    return digits.zfill(10) if digits else ""


def iso_day(raw: str) -> str | None:
    """KBO writes days first (``25-07-1919``); an ISO day is accepted too."""
    text = (raw or "").strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def newest_full_zip(listing_html: str) -> tuple[str, str] | None:
    """``(filename, year-month)`` of the newest full file named on the listing."""
    found = sorted({(m.group(1), m.group(0), f"{m.group(2)}-{m.group(3)}")
                    for m in FULL_ZIP_RE.finditer(listing_html)})
    if not found:
        return None
    _, name, ym = found[-1]
    return name, ym


def describe_listing(html: str) -> str:
    """The page's ``<title>``, or its first 200 characters of text."""
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
    if m:
        return " ".join(m.group(1).split())
    text = " ".join(re.sub(r"<[^>]+>", " ", html).split())
    return text[:200]


def listing_links(html: str) -> list[str]:
    """Every distinct ``href``/``src`` on the page, in order of appearance."""
    seen: list[str] = []
    for m in re.finditer(r"""(?:href|src)\s*=\s*["']([^"']+)["']""", html, re.I):
        if m.group(1) not in seen:
            seen.append(m.group(1))
    return seen


def download(work: Path, username: str, password: str) -> Path:
    """Log in, list the files, fetch the newest full ZIP into ``work``."""
    import http.cookiejar
    import urllib.parse
    import urllib.request

    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = [("User-Agent", "OpenCheck/1.0 (+https://opencheck.world)")]
    # The login page sets the session cookie the form posts against.
    opener.open(f"{PORTAL}/login", timeout=60).read()
    body = urllib.parse.urlencode({"j_username": username, "j_password": password}).encode()
    resp = opener.open(LOGIN_URL, data=body, timeout=60)
    log.info("login answered HTTP %s at %s", resp.status, resp.geturl())
    files = opener.open(FILES_URL, timeout=60)
    listing = files.read().decode("utf-8", "replace")
    if "j_spring_security_check" in listing or "j_username" in listing:
        raise RefusedInput(f"KBO open data: login refused (HTTP {resp.status})")
    newest = newest_full_zip(listing)
    if newest is None:
        # Say what the portal served instead, so a changed listing page or
        # file name can be read off the workflow log: the final URL, the
        # title and the links — never the credentials or the session.
        log.error("files listing: HTTP %s at %s (%d bytes)", files.status, files.geturl(), len(listing))
        log.error("files listing title: %s", describe_listing(listing))
        for href in listing_links(listing)[:60]:
            log.error("files listing link: %s", href)
        raise RefusedInput("KBO open data: no KboOpenData_*_Full.zip on the files listing")
    name, ym = newest
    work.mkdir(parents=True, exist_ok=True)
    target = work / name
    if not target.exists():
        log.info("downloading %s (%s)", name, ym)
        with opener.open(f"{PORTAL}/affiliation/xml/files/{name}", timeout=600) as r, \
                target.open("wb") as out:
            shutil.copyfileobj(r, out, length=1 << 20)
    with target.open("rb") as fh:
        if fh.read(2) != b"PK":
            target.unlink()
            raise RefusedInput(f"{name}: not a ZIP (the portal answered a page)")
    return target


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _open_member(zf: zipfile.ZipFile, name: str) -> tuple[IO[str], csv.DictReader]:
    try:
        raw = zf.open(name)
    except KeyError as exc:
        raise RefusedInput(f"{name}: not in the ZIP") from exc
    fh = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
    reader = csv.DictReader(fh)
    if reader.fieldnames != HEADERS[name]:
        fh.close()
        raise RefusedInput(f"{name}: header {reader.fieldnames!r} is not {HEADERS[name]!r}")
    return fh, reader


def read_meta(zf: zipfile.ZipFile) -> dict[str, str]:
    fh, reader = _open_member(zf, "meta.csv")
    with fh:
        pairs = {(r["Variable"] or "").strip(): (r["Value"] or "").strip() for r in reader}
    if not iso_day(pairs.get("SnapshotDate", "")):
        raise RefusedInput(f"meta.csv: no SnapshotDate ({pairs!r})")
    return pairs


def build(zip_path: Path, out: Path, *, min_rows: dict[str, int] | None = None) -> dict[str, int | str]:
    floors = MIN_ROWS if min_rows is None else min_rows
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    out.parent.mkdir(parents=True, exist_ok=True)
    stats: dict[str, int | str] = {
        "enterprises": 0, "enterprises_malformed": 0, "natural_persons": 0,
        "names": 0, "addresses": 0, "codes": 0,
    }
    with zipfile.ZipFile(zip_path) as zf:
        meta = read_meta(zf)
        con = sqlite3.connect(str(tmp))
        con.executescript(SCHEMA)

        fh, reader = _open_member(zf, "enterprise.csv")
        batch: list[tuple] = []
        with fh:
            for row in reader:
                number = normalise_number(row["EnterpriseNumber"])
                if len(number) != 10:
                    stats["enterprises_malformed"] += 1  # type: ignore[operator]
                    continue
                stats["enterprises"] += 1  # type: ignore[operator]
                if (row["TypeOfEnterprise"] or "").strip() == "1":
                    stats["natural_persons"] += 1  # type: ignore[operator]
                batch.append((
                    number, (row["Status"] or "").strip() or None,
                    (row["JuridicalSituation"] or "").strip() or None,
                    (row["TypeOfEnterprise"] or "").strip() or None,
                    (row["JuridicalForm"] or "").strip() or None,
                    (row["JuridicalFormCAC"] or "").strip() or None,
                    iso_day(row["StartDate"]),
                ))
                if len(batch) >= 20_000:
                    con.executemany(
                        "INSERT OR IGNORE INTO entity (number, status, situation, type, form,"
                        " form_cac, start_date) VALUES (?,?,?,?,?,?,?)", batch)
                    batch.clear()
        if batch:
            con.executemany(
                "INSERT OR IGNORE INTO entity (number, status, situation, type, form,"
                " form_cac, start_date) VALUES (?,?,?,?,?,?,?)", batch)
            batch.clear()
        log.info("enterprises: %s (%s natural persons, %s malformed), %.0fs",
                 f"{stats['enterprises']:,}", f"{stats['natural_persons']:,}",
                 stats["enterprises_malformed"], time.time() - t0)

        # Names: per entity and language the best type (official > commercial
        # > abbreviation). Rows for an establishment unit (number starts 2–8)
        # are the unit's, not the enterprise's, and are skipped.
        best: dict[tuple[str, str], tuple[int, str, str]] = {}
        fh, reader = _open_member(zf, "denomination.csv")
        with fh:
            for row in reader:
                number = normalise_number(row["EntityNumber"])
                if len(number) != 10 or number[0] not in "01":
                    continue
                lang = LANGUAGES.get((row["Language"] or "").strip())
                dtype = (row["TypeOfDenomination"] or "").strip().zfill(3)
                name = " ".join((row["Denomination"] or "").split())
                if not lang or not name:
                    continue
                stats["names"] += 1  # type: ignore[operator]
                batch.append((number, name))
                if len(batch) >= 20_000:
                    con.executemany("INSERT INTO denomination VALUES (?,?)", batch)
                    batch.clear()
                rank = DENOMINATION_PRIORITY.get(dtype, 9)
                key = (number, lang)
                if key not in best or rank < best[key][0]:
                    best[key] = (rank, dtype, name)
        if batch:
            con.executemany("INSERT INTO denomination VALUES (?,?)", batch)
            batch.clear()
        for (number, lang), (_rank, dtype, name) in best.items():
            con.execute(
                f"UPDATE entity SET name_{lang} = ?, name_type = COALESCE(name_type, ?) WHERE number = ?",
                (name, dtype, number),
            )
        best.clear()
        log.info("names: %s rows, %.0fs", f"{stats['names']:,}", time.time() - t0)

        fh, reader = _open_member(zf, "address.csv")
        with fh:
            for row in reader:
                number = normalise_number(row["EntityNumber"])
                if len(number) != 10 or number[0] not in "01":
                    continue
                if (row["TypeOfAddress"] or "").strip().upper() != REGISTERED_OFFICE:
                    continue
                if (row["DateStrikingOff"] or "").strip():
                    continue
                stats["addresses"] += 1  # type: ignore[operator]
                con.execute(
                    "UPDATE entity SET street = ?, house = ?, box = ?, zipcode = ?,"
                    " municipality = ?, country = ? WHERE number = ?",
                    (
                        (row["StreetNL"] or row["StreetFR"] or "").strip() or None,
                        (row["HouseNumber"] or "").strip() or None,
                        (row["Box"] or "").strip() or None,
                        (row["Zipcode"] or "").strip() or None,
                        (row["MunicipalityNL"] or row["MunicipalityFR"] or "").strip() or None,
                        (row["CountryNL"] or row["CountryFR"] or "").strip() or None,
                        number,
                    ),
                )
        log.info("addresses: %s registered offices, %.0fs", f"{stats['addresses']:,}", time.time() - t0)

        fh, reader = _open_member(zf, "code.csv")
        with fh:
            rows = [
                ((r["Category"] or "").strip(), (r["Code"] or "").strip(),
                 (r["Language"] or "").strip().upper(), " ".join((r["Description"] or "").split()))
                for r in reader
            ]
        rows = [r for r in rows if all(r)]
        con.executemany("INSERT OR REPLACE INTO code VALUES (?,?,?,?)", rows)
        stats["codes"] = len(rows)

        for name, key in (("enterprise.csv", "enterprises"), ("denomination.csv", "names"),
                          ("address.csv", "addresses")):
            if int(stats[key]) < floors.get(name, 0):
                con.close()
                tmp.unlink()
                raise RefusedInput(f"{name}: only {stats[key]:,} rows — not the register")
        if int(stats["enterprises_malformed"]) > MAX_MALFORMED:
            con.close()
            tmp.unlink()
            raise RefusedInput(f"enterprise.csv: {stats['enterprises_malformed']:,} malformed rows")

        con.commit()
        con.executescript(INDEXES)
        cut = iso_day(meta["SnapshotDate"]) or ""
        con.executemany(
            "INSERT OR REPLACE INTO meta VALUES (?,?)",
            [
                ("schema_version", INDEX_SCHEMA_VERSION),
                ("source", "KBO Open Data — monthly full extract (kbopub.economie.fgov.be)"),
                ("source_file", zip_path.name),
                ("cut", cut),
                ("built_at", datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")),
                ("licence", "Reuse of KBO public data (FPS Economy); attribution required"),
                *[(f"kbo_{k}", v) for k, v in meta.items()],
                *[(f"count_{k}", str(v)) for k, v in stats.items()],
            ],
        )
        con.commit()
        con.execute("VACUUM")
        con.close()
    os.replace(tmp, out)
    stats["cut"] = cut
    stats["seconds"] = int(time.time() - t0)
    stats["megabytes"] = int(os.path.getsize(out) / 1e6)
    return stats


def write_gzip(path: Path) -> Path:
    gz = path.with_name(path.name + ".gz")
    with path.open("rb") as src, gzip.open(gz, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=1 << 20)
    return gz


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="log in and fetch the newest full ZIP")
    ap.add_argument("--work", type=Path, help="where --download puts the ZIP (default: beside --out)")
    ap.add_argument("--zip", type=Path, help="a downloaded KboOpenData_*_Full.zip")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.download:
            user = os.environ.get("KBO_OPEN_DATA_USERNAME", "")
            pw = os.environ.get("KBO_OPEN_DATA_PASSWORD", "")
            if not (user and pw):
                raise RefusedInput("KBO_OPEN_DATA_USERNAME and KBO_OPEN_DATA_PASSWORD must be set")
            zip_path = download(args.work or args.out.parent / "kbo", user, pw)
        elif args.zip:
            zip_path = args.zip
        else:
            ap.error("--download or --zip")
        stats = build(zip_path, args.out)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], stats["cut"])
    for k, v in stats.items():
        log.info("  %-24s %s", k, f"{v:,}" if isinstance(v, int) else v)
    if args.gzip:
        gz = write_gzip(args.out)
        log.info("wrote %s (%s MB)", gz, int(os.path.getsize(gz) / 1e6))
    return 0


if __name__ == "__main__":
    sys.exit(main())
