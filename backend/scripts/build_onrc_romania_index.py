#!/usr/bin/env python3
"""Build the ONRC Romania SQLite index from the data.gov.ro monthly dump.

Usage
-----
Resolve and download the latest dump, then build the shippable index::

    python3 scripts/build_onrc_romania_index.py --out onrc_romania.sqlite

Build from CSVs already on disk (still needs GLEIF for the scope)::

    python3 scripts/build_onrc_romania_index.py --offline --csv-dir ./onrc --out …

Pin a specific monthly dataset::

    python3 scripts/build_onrc_romania_index.py --dataset firme-02-09-2026 --out …

The whole register, for research rather than for shipping::

    python3 scripts/build_onrc_romania_index.py --scope full --out onrc_full.sqlite

Scoped to the LEI population by default
---------------------------------------
The register holds **2,855,557** companies; the Romanian LEI population is
**9,033**. Every ONRC lookup is downstream of a GLEIF LEI record, so the other
2.85M rows cost 1.2 GB to serve no lookup that can occur. ``--scope leis``
(the default) keeps 8,491 companies and 16,743 representatives in **4.3 MB**.
``--scope full`` restores the old behaviour. See ``Scope``.

Why it resolves through CKAN
----------------------------
ONRC republishes the whole register roughly monthly as a **new dataset** whose
slug carries the cut date (``firme-02-09-2026``) and whose six resources get
**new UUIDs every time**. A hardcoded download URL works for one month and then
404s, so the resource ids are always resolved through
``data.gov.ro/api/3/action/package_show``.

The status nomenclature lives in a *separate* dataset, ``nomenclatoare-<date>``,
published the same day.

Two passes, because the representatives file is 337 MB
------------------------------------------------------
``OD_REPREZENTANTI_LEGALI.CSV`` is keyed on ``COD_INMATRICULARE`` and covers
sole traders as well as companies. Pass one loads the companies that survive
the corporate-form filter; pass two streams the representatives and keeps only
rows whose registration number is in that set. Holding both in memory at once
is what the two passes avoid.

What the filter removes
-----------------------
About 1.35M of the 4.22M rows are sole-trader forms (PFA, II, PF, AF, IF) —
natural persons trading under a business name, never the subject of an LEI
lookup. See ``CORPORATE_FORMS`` in ``opencheck.sources.onrc_romania``.

Measured on the 2 September 2026 export
---------------------------------------
* ``OD_FIRME.CSV`` 694 MB, 4,219,081 rows, 4,214,297 distinct registration
  numbers. **95,158 CUIs appear on more than one row** and 86,426 rows (2.0%)
  carry no CUI at all, which is why the company table is keyed on the
  registration number and ``cui`` is merely indexed.
* ``OD_REPREZENTANTI_LEGALI.CSV`` 337 MB, 3,689,931 rows over 2,758,033
  companies, 20 distinct roles.
* Every file is UTF-8 **with a BOM**, delimited by ``^``, not a comma, and
  **not quoted at all** — 1,116 rows open ``DENUMIRE`` with a ``"`` because
  ONRC registers the trade name in quotes, and a further 209 open some later
  field with one. Those quotes are data. See ``rows``.

Network note
------------
``data.gov.ro`` ignores HTTP ``Range`` headers and serves whole files, and it
is not reachable from every datacentre network — verify it resolves from
wherever this runs before scheduling it there. ``--csv-dir`` exists so the
download and the build can happen in different places.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.onrc_romania import (  # noqa: E402
    CORPORATE_FORMS,
    RO_RA_CODES,
    normalise_registration_number,
    parse_ro_date,
    prefix_for,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("onrc")

CKAN = "https://data.gov.ro/api/3/action"
ORG = "onrc"

#: ONRC's delimiter. Not a comma.
DELIM = "^"

#: Resource filenames within a ``firme-*`` dataset, by the role they play here.
FIRME_FILE = "od_firme.csv"
REPS_FILE = "od_reprezentanti_legali.csv"
STARE_FILE = "od_stare_firma.csv"
NOMEN_STARE_FILE = "n_stare_firma.csv"

SCHEMA = """
CREATE TABLE company (
    registration_number TEXT PRIMARY KEY,
    registration_prefix TEXT,
    cui                 TEXT,
    name                TEXT,
    legal_form          TEXT,
    registered_on       TEXT,
    status_code         TEXT,
    status              TEXT,
    country             TEXT,
    county              TEXT,
    locality            TEXT,
    address             TEXT,
    postal_code         TEXT,
    website             TEXT,
    parent_country      TEXT
);
CREATE TABLE representative (
    registration_number TEXT,
    seq                 INTEGER,
    name                TEXT,
    role                TEXT,
    role_slug           TEXT,
    is_entity           INTEGER,
    birth_date          TEXT,
    birth_locality      TEXT,
    birth_county        TEXT,
    birth_country       TEXT,
    locality            TEXT,
    county              TEXT,
    country             TEXT
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""

#: Only the two indexes anything actually queries.
#:
#: ``idx_company_cui`` and ``idx_company_name`` were dropped on 15 September
#: 2026 after measuring what they served: nothing. No query filters on ``cui``
#: at all, and the only query touching ``name`` is the adapter's
#: ``WHERE name LIKE '%…%'`` — a leading wildcard, which SQLite cannot answer
#: from an index. Together they cost 131 MB of a 1,242 MB index.
INDEXES = """
CREATE INDEX idx_company_prefix ON company(registration_prefix);
CREATE INDEX idx_rep_number     ON representative(registration_number);
"""

#: ``CALITATE`` → a stable slug the BODS mapper keys on, so the mapper never
#: matches on Romanian free text and a new role value fails loudly here rather
#: than silently mapping to a default three layers away.
#:
#: The register's vocabulary is closed: exactly these twenty values occur over
#: all 3,689,931 rows of the 2 September 2026 export. Counts are from that run.
ROLE_SLUGS: dict[str, str] = {
    "administrator": "administrator",                                    # 3,082,135
    "reprezentant al persoanei juridice": "entity_representative",       #   217,552
    "lichidator judiciar": "judicial_liquidator",                        #   191,730
    "lichidator": "liquidator",                                          #   122,623
    "lichidator provizoriu": "interim_liquidator",                       #    20,327
    "administrator si reprezentant": "administrator_representative",     #    18,403
    "administrator judiciar": "judicial_administrator",                  #    15,713
    "lichidator judiciar provizoriu": "interim_judicial_liquidator",     #     6,353
    "administrator special": "special_administrator",                    #     5,640
    "administrator judiciar provizoriu": "interim_judicial_administrator",  #  4,938
    "reprezentant administrator persoana juridica": "entity_administrator_representative",  # 2,158
    "director general unic": "sole_director_general",                    #       513
    "membru în consiliul de supraveghere": "supervisory_board_member",   #       477
    "reprezentant legal": "legal_representative",                        #       365
    "membru în directorat": "management_board_member",                   #       324
    "administrator provizoriu": "interim_administrator",                 #       262
    "administrator concordatar": "composition_administrator",            #       203
    "administrator si conducator": "administrator_manager",              #       148
    "mandatar": "agent",                                                 #        60
    "administrator repus în funcție": "reinstated_administrator",        #         7
}

#: Tokens that mark a representative as a legal entity rather than a person.
#: Romanian insolvency practitioners file as firms (IPURL, SPRL, CII), and
#: 265,799 rows name a company administrator.
_ENTITY_TOKENS = (
    "SRL", "S.R.L", "SA ", "S.A.", "IPURL", "SPRL", "SCA", "CII", "C.I.I",
    "SNC", "GMBH", "LTD", "B.V.", "N.V.", "A.G.", "S.P.A", "SOCIETATE",
)


def _looks_like_entity(name: str) -> bool:
    upper = f" {name.upper()} "
    return any(token in upper for token in _ENTITY_TOKENS)


# ---------------------------------------------------------------------------
# CKAN
# ---------------------------------------------------------------------------


def _ckan(action: str, **params: str) -> dict:
    url = f"{CKAN}/{action}?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url, headers={"User-Agent": "OpenCheck/1.0"})
    with urllib.request.urlopen(req, timeout=120) as fh:
        return json.load(fh)["result"]


def latest_datasets() -> tuple[str, str]:
    """Return ``(firme slug, nomenclatoare slug)`` for the newest monthly dump."""
    org = _ckan("organization_show", id=ORG, include_datasets="true")
    firme, nomen = [], []
    for pkg in org.get("packages", []):
        name = pkg.get("name", "")
        stamp = pkg.get("metadata_modified", "")
        if name.startswith("firme-"):
            firme.append((stamp, name))
        elif name.startswith("nomenclatoare-"):
            nomen.append((stamp, name))
    if not firme:
        raise SystemExit("no 'firme-*' dataset found on data.gov.ro")
    firme.sort(reverse=True)
    nomen.sort(reverse=True)
    return firme[0][1], (nomen[0][1] if nomen else "")


def resources(dataset: str) -> dict[str, tuple[str, int | None]]:
    """``{lowercased filename: (download url, published byte size)}``.

    CKAN's ``size`` is the only integrity figure data.gov.ro publishes — the
    ``hash`` field is empty on every ONRC resource — so it is what ``download``
    checks a transfer against.
    """
    pkg = _ckan("package_show", id=dataset)
    out: dict[str, tuple[str, int | None]] = {}
    for res in pkg.get("resources", []):
        url = res.get("url") or ""
        if not url:
            continue
        try:
            size = int(res["size"])
        except (KeyError, TypeError, ValueError):
            size = None
        out[url.rsplit("/", 1)[-1].lower()] = (url, size)
    return out


def resource_urls(dataset: str) -> dict[str, str]:
    """``{lowercased filename: download url}`` for one dataset."""
    return {name: url for name, (url, _) in resources(dataset).items()}


def download(url: str, dest: Path, *, expected_size: int | None = None) -> Path:
    """Fetch ``url`` to ``dest``, refusing to accept a short transfer.

    The size check is the point. data.gov.ro drops long transfers routinely —
    ``OD_FIRME.CSV`` is 694 MB and has cut out anywhere between 110 MB and
    340 MB — and it answers a ``Range`` request with ``HTTP 200`` and the whole
    file from byte 0 despite advertising ``Accept-Ranges: bytes``, so ordinary
    resume tooling cannot help. Without this check the *next* run sees a
    non-empty file, skips it, and builds an index from a fraction of the
    register while reporting success. A truncated CSV parses perfectly.
    """
    if dest.exists() and dest.stat().st_size > 0:
        have = dest.stat().st_size
        if expected_size is None or have == expected_size:
            log.info("have %s (%.0f MB)", dest.name, have / 1e6)
            return dest
        log.warning(
            "%s is %d bytes, expected %d — discarding the partial and refetching",
            dest.name,
            have,
            expected_size,
        )
        dest.unlink()
    log.info("downloading %s", dest.name)
    req = urllib.request.Request(url, headers={"User-Agent": "OpenCheck/1.0"})
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(req, timeout=900) as fh, dest.open("wb") as out:
        while chunk := fh.read(1 << 20):
            out.write(chunk)
    got = dest.stat().st_size
    if expected_size is not None and got != expected_size:
        raise SystemExit(
            f"{dest.name}: got {got} bytes, expected {expected_size}. The "
            "transfer was cut short; data.gov.ro ignores Range headers so this "
            "cannot be resumed — refetch the whole file."
        )
    log.info("  %.0f MB", got / 1e6)
    return dest


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


GLEIF_LEI_RECORDS = "https://api.gleif.org/api/v1/lei-records"


class Scope:
    """The set of ONRC companies worth keeping, derived from GLEIF.

    Why scope at all: the register holds **2,855,557** companies and the
    Romanian LEI population is **9,033**. Every ONRC lookup is downstream of a
    GLEIF LEI record — the pipeline is LEI-anchored, ``/resolve-national-id``
    resolves to an LEI first, and ONRC publishes no ownership edges, so nothing
    expands into a Romanian company that has no LEI. Keeping the other 2.85M
    rows costs 1.2 GB to serve no lookup that can occur.

    Scoped, the index is **4.3 MB** (1.3 MB gzipped) and covers 8,491
    companies with 16,743 representatives — small enough to ship as an ordinary
    release asset with no boot-time download.

    The cost is one new staleness mode, and it is worth stating plainly: a
    company issued an LEI *after* the build is not in the index and falls back
    to the coverage note, where a full index would have found it. A company
    newly *registered* is missed either way. Rebuild monthly, with the dump.
    """

    def __init__(self, numbers: set[str], prefixes: set[str], cuis: set[str]) -> None:
        self.numbers, self.prefixes, self.cuis = numbers, prefixes, cuis

    def wants(self, number: str, cui: str) -> bool:
        if number in self.numbers:
            return True
        if cui and cui in self.cuis:
            return True
        prefix = prefix_for(number)
        return bool(prefix and prefix in self.prefixes)

    def __len__(self) -> int:
        return len(self.numbers) + len(self.cuis)


def lei_scope() -> Scope:
    """Page the whole Romanian LEI population out of GLEIF.

    Filters on ``entity.legalAddress.country``, never ``entity.jurisdiction``:
    a jurisdiction filter of ``MD`` also matches ``US-MD``, and the same class
    of collision is not worth risking on ``RO``.

    Raises rather than returning a partial set. A half-fetched scope would
    build an index that is quietly missing companies — the same silent-wrong
    failure this script has already shipped twice.
    """
    numbers: set[str] = set()
    prefixes: set[str] = set()
    cuis: set[str] = set()
    seen = kept = 0
    page, last = 1, None
    while last is None or page <= last:
        query = urllib.parse.urlencode(
            {
                "filter[entity.legalAddress.country]": "RO",
                "page[size]": 200,
                "page[number]": page,
            }
        )
        payload = None
        for attempt in range(6):
            try:
                req = urllib.request.Request(
                    f"{GLEIF_LEI_RECORDS}?{query}",
                    headers={
                        "Accept": "application/vnd.api+json",
                        "User-Agent": "OpenCheck/1.0",
                    },
                )
                with urllib.request.urlopen(req, timeout=60) as fh:
                    payload = json.load(fh)
                break
            except (urllib.error.URLError, OSError, ValueError) as exc:
                log.warning("  GLEIF page %d attempt %d: %s", page, attempt + 1, exc)
                time.sleep(3 * (attempt + 1))
        if payload is None:
            raise SystemExit(
                f"GLEIF page {page} failed six times — refusing to build from a "
                "partial LEI scope. Re-run, or pass --scope full."
            )
        last = payload["meta"]["pagination"]["lastPage"]
        for record in payload["data"]:
            seen += 1
            entity = record["attributes"]["entity"]
            if (entity.get("registeredAt") or {}).get("id") not in RO_RA_CODES:
                continue
            raw = (entity.get("registeredAs") or "").strip()
            if not raw:
                continue
            kept += 1
            try:
                number = normalise_registration_number(raw)
            except ValueError:
                number = ""
            if number:
                numbers.add(number)
                prefix = prefix_for(number)
                if prefix:
                    prefixes.add(prefix)
                continue
            digits = raw.replace("RO", "").strip().lstrip("0")
            if digits.isdigit():
                cuis.add(digits)
        # 60 requests a minute per IP, no key and no higher tier.
        time.sleep(1.1)
        page += 1
    log.info(
        "GLEIF: %d RO records, %d dispatchable (%d numbers, %d fiscal codes)",
        seen,
        kept,
        len(numbers),
        len(cuis),
    )
    if not kept:
        raise SystemExit("GLEIF returned no dispatchable Romanian records")
    return Scope(numbers, prefixes, cuis)


def rows(path: Path) -> Iterator[dict[str, str]]:
    """Stream one ONRC CSV. ``utf-8-sig`` strips the BOM every file carries.

    ``QUOTE_NONE`` is load-bearing, not tidiness. ONRC's export is not a quoted
    CSV: it is ``^``-delimited (a character chosen because it cannot occur in
    the data) with no quoting of any kind, so a ``"`` in a company name or
    address is a literal character. Python's default reader treats it as an
    opening quote and swallows every line up to the next one, merging hundreds
    of records into a single row whose first field is a multi-kilobyte blob.

    Measured on the 2 September 2026 export of ``OD_FIRME.CSV``, both harms
    confirmed by rebuilding the index with and without this argument:

    * **667 companies lost.** Two rows open a field with a quote that has no
      closing partner, so the reader stayed in quoted mode and swallowed every
      following line until the next quote. Their columns having shifted, the
      two resulting blobs failed the corporate-form filter and were skipped as
      sole traders. The builder logged 4,218,414 rows against 4,219,081
      physical lines — a 0.016% shortfall that reads like a rounding
      difference. Fixing it moved the kept-company count from 2,855,135 to
      2,855,557 and representatives from 3,680,795 to 3,681,319.
    * **1,116 names altered.** That many rows open ``DENUMIRE`` with a quote,
      and the reader consumed it, storing ``LEMNLIND SRL`` where ONRC
      published ``"LEMNLIND" SRL``. 918 of those names now carry a quote that
      they did not before; the remaining 198 already held one elsewhere in the
      name, because a quote that does *not* start a field was passed through —
      the same character treated two ways in one column.

    With ``QUOTE_NONE`` the parsed record count equals the physical line count
    for every file in both datasets, which it did not before.
    """
    with path.open(encoding="utf-8-sig", errors="replace", newline="") as fh:
        for row in csv.DictReader(fh, delimiter=DELIM, quoting=csv.QUOTE_NONE):
            yield row


def _address(row: dict[str, str]) -> str:
    parts = [
        row.get("ADR_DEN_STRADA"),
        row.get("ADR_NR_STRADA"),
        row.get("ADR_BLOC"),
        row.get("ADR_SCARA"),
        row.get("ADR_ETAJ"),
        row.get("ADR_APARTAMENT"),
        row.get("ADR_COMPLETARE"),
    ]
    return ", ".join(p.strip() for p in parts if p and p.strip())


def build(
    csv_dir: Path,
    out: Path,
    *,
    dataset: str,
    nomen_dataset: str,
    scope: Scope | None = None,
) -> None:
    if out.exists():
        out.unlink()
    conn = sqlite3.connect(str(out))
    conn.executescript(SCHEMA)

    # --- status nomenclature ------------------------------------------
    statuses: dict[str, str] = {}
    nomen_path = csv_dir / NOMEN_STARE_FILE
    if nomen_path.exists():
        for row in rows(nomen_path):
            code = (row.get("COD") or "").strip()
            if code:
                statuses[code] = (row.get("DENUMIRE") or "").strip()
        log.info("status nomenclature: %d codes", len(statuses))
    else:
        log.warning("no %s — companies will carry a status code and no label", NOMEN_STARE_FILE)

    # --- company status -----------------------------------------------
    status_by_number: dict[str, str] = {}
    stare_path = csv_dir / STARE_FILE
    if stare_path.exists():
        for row in rows(stare_path):
            number = (row.get("COD_INMATRICULARE") or "").strip().upper()
            code = (row.get("COD") or "").strip()
            if number and code:
                status_by_number[number] = code
        log.info("statuses: %d companies", len(status_by_number))

    # --- pass one: companies ------------------------------------------
    kept: set[str] = set()
    seen = skipped_form = skipped_dupe = skipped_scope = 0
    batch: list[tuple] = []
    for row in rows(csv_dir / FIRME_FILE):
        seen += 1
        number = (row.get("COD_INMATRICULARE") or "").strip().upper()
        if not number:
            continue
        form = (row.get("FORMA_JURIDICA") or "").strip().upper()
        if form not in CORPORATE_FORMS:
            skipped_form += 1
            continue
        if number in kept:
            skipped_dupe += 1
            continue
        cui = (row.get("CUI") or "").strip()
        if cui in {"0", ""}:
            cui = ""
        if scope is not None and not scope.wants(number, cui):
            skipped_scope += 1
            continue
        kept.add(number)
        code = status_by_number.get(number, "")
        batch.append(
            (
                number,
                # Derived from EITHER format — see ``prefix_for``. Using
                # ``to_new_format`` here left every new-format row with a NULL
                # prefix, which is the one row an incoming old-format GLEIF
                # number has to reach, and cost 38.6% of J-number lookups.
                prefix_for(number),
                cui or None,
                (row.get("DENUMIRE") or "").strip(),
                form,
                parse_ro_date(row.get("DATA_INMATRICULARE")),
                code or None,
                statuses.get(code) or None,
                (row.get("ADR_TARA") or "").strip() or None,
                (row.get("ADR_JUDET") or "").strip() or None,
                (row.get("ADR_LOCALITATE") or "").strip() or None,
                _address(row) or None,
                (row.get("ADR_COD_POSTAL") or "").strip() or None,
                (row.get("WEB") or "").strip() or None,
                (row.get("TARA_FIRMA_MAMA") or "").strip() or None,
            )
        )
        if len(batch) >= 50_000:
            conn.executemany(
                "INSERT INTO company VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch
            )
            batch.clear()
            log.info("  companies: %d kept of %d read", len(kept), seen)
    if batch:
        conn.executemany(
            "INSERT INTO company VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch
        )
    conn.commit()
    log.info(
        "companies: %d kept, %d sole traders skipped, %d outside the LEI scope, "
        "%d duplicate numbers, %d read",
        len(kept), skipped_form, skipped_scope, skipped_dupe, seen,
    )

    # --- pass two: representatives ------------------------------------
    reps = rep_rows = 0
    unknown_roles: dict[str, int] = {}
    seq_by_number: dict[str, int] = {}
    batch.clear()
    for row in rows(csv_dir / REPS_FILE):
        rep_rows += 1
        number = (row.get("COD_INMATRICULARE") or "").strip().upper()
        if number not in kept:
            continue
        name = (row.get("PERSOANA_IMPUTERNICITA") or "").strip()
        if not name:
            continue
        role = (row.get("CALITATE") or "").strip()
        slug = ROLE_SLUGS.get(role.lower())
        if slug is None and role:
            unknown_roles[role] = unknown_roles.get(role, 0) + 1
        seq = seq_by_number.get(number, 0)
        seq_by_number[number] = seq + 1
        reps += 1
        batch.append(
            (
                number,
                seq,
                name,
                role or None,
                slug,
                1 if _looks_like_entity(name) else 0,
                parse_ro_date(row.get("DATA_NASTERE")),
                (row.get("LOCALITATE_NASTERE") or "").strip() or None,
                (row.get("JUDET_NASTERE") or "").strip() or None,
                (row.get("TARA_NASTERE") or "").strip() or None,
                (row.get("LOCALITATE") or "").strip() or None,
                (row.get("JUDET") or "").strip() or None,
                (row.get("TARA") or "").strip() or None,
            )
        )
        if len(batch) >= 50_000:
            conn.executemany(
                "INSERT INTO representative VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch
            )
            batch.clear()
    if batch:
        conn.executemany(
            "INSERT INTO representative VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch
        )
    conn.commit()
    log.info("representatives: %d kept of %d read", reps, rep_rows)

    if unknown_roles:
        # The register's role vocabulary is closed and ROLE_SLUGS covers all
        # twenty values. A new one is a real upstream change and must be
        # mapped deliberately, so it is reported loudly rather than defaulted.
        log.warning("UNMAPPED CALITATE VALUES — add them to ROLE_SLUGS:")
        for role, count in sorted(unknown_roles.items(), key=lambda x: -x[1]):
            log.warning("    %8d  %r", count, role)

    conn.executescript(INDEXES)
    conn.executemany(
        "INSERT INTO meta VALUES (?,?)",
        [
            ("dataset", dataset),
            ("nomenclature_dataset", nomen_dataset),
            ("companies", str(len(kept))),
            ("representatives", str(reps)),
            ("source", "https://data.gov.ro/dataset?organization=onrc"),
            ("licence", "CC-BY-4.0"),
            # Recorded so a reader can tell a scoped index from a full one
            # without counting rows, and knows which staleness applies.
            (
                "scope",
                "the GLEIF Romanian LEI population"
                if scope is not None
                else "the whole register",
            ),
            ("built_at", datetime.now(UTC).strftime("%Y-%m-%d")),
        ],
    )
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    log.info("wrote %s (%.0f MB)", out, out.stat().st_size / 1e6)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    ap.add_argument(
        "--csv-dir",
        type=Path,
        default=Path("onrc-csv"),
        help="where the CSVs are (or will be downloaded to)",
    )
    ap.add_argument("--dataset", help="firme-DD-MM-YYYY slug; default: newest")
    ap.add_argument("--nomenclature", help="nomenclatoare-DD-MM-YYYY slug")
    ap.add_argument(
        "--offline",
        action="store_true",
        help="build from --csv-dir without touching data.gov.ro",
    )
    ap.add_argument(
        "--scope",
        choices=("leis", "full"),
        default="leis",
        help=(
            "leis (default): keep only companies in the GLEIF Romanian LEI "
            "population — 4 MB, and the only rows a lookup can ever reach. "
            "full: the whole register, 1.2 GB, for research"
        ),
    )
    args = ap.parse_args()

    dataset = args.dataset or ""
    nomen = args.nomenclature or ""
    if not args.offline:
        if not dataset:
            dataset, auto_nomen = latest_datasets()
            nomen = nomen or auto_nomen
            log.info("latest dataset: %s (nomenclature %s)", dataset, nomen)
        found = resources(dataset)
        for filename in (FIRME_FILE, REPS_FILE, STARE_FILE):
            if filename not in found:
                raise SystemExit(f"{dataset} has no {filename}")
            url, size = found[filename]
            download(url, args.csv_dir / filename, expected_size=size)
        if nomen:
            for filename, (url, size) in resources(nomen).items():
                if filename == NOMEN_STARE_FILE:
                    download(url, args.csv_dir / filename, expected_size=size)

    scope = None
    if args.scope == "leis":
        log.info("resolving the Romanian LEI population from GLEIF")
        scope = lei_scope()

    build(
        args.csv_dir,
        args.out,
        dataset=dataset,
        nomen_dataset=nomen,
        scope=scope,
    )


if __name__ == "__main__":
    main()
