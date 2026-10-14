#!/usr/bin/env python3
"""Build the Form 13F index of institutional investment managers' positions (Phase 333).

The SEC publishes every Form 13F information table, flattened from the XML
filings, as quarterly data sets (US government work, public domain):

    https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets

A 13F is filed by an institutional investment manager with investment
discretion over at least $100 million of "Section 13(f) securities". It lists
the manager's long positions at a calendar quarter's end and is due 45 days
later. **A 13F filer is not a beneficial owner** and nothing built from this
index may be read as ownership: it never becomes a BODS statement, never sits
in the ownership graph and is never read by a risk signal (Stephen, 14 Oct
2026). The index only answers "which managers reported a position in this
security, and how large".

    python3 scripts/build_form13f_index.py --out /tmp/form13f.sqlite --gzip \\
        --contact you@example.org

``.github/workflows/refresh-form13f-index.yml`` runs this weekly and replaces
the ``form13f-index`` release asset only when the SEC has published a newer
data set. Standard library only, so it also runs with a bare ``python3``.

What it reads
-------------

* The listing page, for the data sets' links. They are read off the page,
  never built: the June–August 2026 set moved from ``/files/structureddata/``
  to ``/files/datastandardsinnovation/``.
* The **latest two** data sets. Each is cut by *filing date*, not by period,
  so late filers and amendments for a quarter land in the next set; the
  latest set also carries hundreds of late filings for older quarters (back
  to 2001 in the June–August 2026 set).
* ``SUBMISSION``, ``COVERPAGE``, ``SUMMARYPAGE`` and ``INFOTABLE``.
* The SEC's XBRL frames for ``dei:EntityCommonStockSharesOutstanding`` — the
  shares-outstanding figure on a filer's 10-Q/10-K cover — for the period's
  calendar quarter and the next, one keyless call each, kept per CIK. A
  multi-class issuer reports the figure per class and is absent from the
  frame, which is the intended outcome: no percentage is computed for it.

How a manager's position is chosen
----------------------------------

* **One period**: the period of report with the most holdings filings in the
  latest set (30 June 2026 for the June–August 2026 set).
* **One report per manager (CIK)** for that period: the latest original or
  RESTATEMENT amendment, plus every NEW HOLDINGS amendment filed after it.
  An amendment with no stated type is read as a restatement. Notices
  (``13F-NT``) carry no holdings and are skipped.
* **Shares only** (``SSHPRNAMTTYPE`` = ``SH``; principal amounts are bonds),
  **no options** (rows with ``PUTCALL`` are left out), summed per manager and
  CUSIP across the report's rows (a combination report lists one row per
  included manager).

Checks, and what they do
------------------------

Filers make mistakes the SEC does not correct. Per CUSIP:

* **Implied price.** The median of value ÷ shares over the positions that
  report both is the reference (at least ``MIN_PRICE_SAMPLE`` positions). A
  position within ``PRICE_BAND`` of it is ``ok``; within the band of a
  thousandth of it is ``thousands`` (value still reported in thousands, as
  before 3 January 2023 — T. Rowe Price, 71 other filings on Moody's); with
  too few positions to judge it is ``unchecked``. Anything else (shares and
  value transposed — CalSTRS reported 100,442,257 "shares" of Moody's worth
  $221,766) is **left out and counted**, never repaired.
* **Issuer name.** A position whose ``NAMEOFISSUER`` shares none of the
  CUSIP's core name tokens, and whose price does not vouch for it (anything
  but ``ok``), is a filer's wrong CUSIP and is left out and counted — "Axon
  Enterprise" under Berkshire A's CUSIP prices at a thousandth of it and
  would otherwise pass as value in thousands. A name mismatch at the
  issuer's own price is kept: "IBM", "RAYTHEON" and "SCHLUMBERGER" are the
  issuer under a former or short name.

Neither check repairs a figure. ``value_check`` records which held, so the
reader can decide what to show.

What is stored
--------------

The top ``TOP_N`` positions per CUSIP by shares, with the count of the rest —
never a total, because positions overlap (171 Moody's filings include a
manager that also filed the same position itself). The managers' names and
report details once each. The shares-outstanding facts. A ``meta`` table with
the period, the data sets read and ``built_at``.

The build refuses, and writes nothing, when the listing page has no data
set links, a download is not a zip, a member file or column is missing, or
the chosen period has fewer than ``MIN_FILINGS`` holdings reports.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

log = logging.getLogger("build_form13f_index")

SCHEMA_VERSION = "1"
LICENSE = "US-PD"
LISTING_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
SEC_HOST = "https://www.sec.gov"
FRAMES_URL = (
    "https://data.sec.gov/api/xbrl/frames/dei/EntityCommonStockSharesOutstanding/"
    "shares/{frame}.json"
)

TOP_N = 100
MIN_FILINGS = 5000
MIN_PRICE_SAMPLE = 5
MIN_NAME_SAMPLE = 3
PRICE_BAND = 2.0  # within a factor of two of the median implied price
CORE_TOKEN_SHARE = 0.4

REQUIRED_COLUMNS = {
    "SUBMISSION.tsv": ("ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"),
    "COVERPAGE.tsv": (
        "ACCESSION_NUMBER", "ISAMENDMENT", "AMENDMENTTYPE", "FILINGMANAGER_NAME", "REPORTTYPE",
    ),
    "SUMMARYPAGE.tsv": ("ACCESSION_NUMBER", "OTHERINCLUDEDMANAGERSCOUNT", "ISCONFIDENTIALOMITTED"),
    "INFOTABLE.tsv": (
        "ACCESSION_NUMBER", "NAMEOFISSUER", "TITLEOFCLASS", "CUSIP", "VALUE", "SSHPRNAMT",
        "SSHPRNAMTTYPE", "PUTCALL", "INVESTMENTDISCRETION",
    ),
}

HOLDINGS_TYPES = frozenset({"13F-HR", "13F-HR/A"})
REPORT_TYPES = frozenset({"13F HOLDINGS REPORT", "13F COMBINATION REPORT"})

# Name tokens that say nothing about which issuer a row names.
NAME_STOPWORDS = frozenset({
    "INC", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "LIMITED", "PLC", "LLC", "LP",
    "THE", "CL", "CLA", "CLB", "CLASS", "COM", "COMMON", "STOCK", "STK", "SHS", "SHARES",
    "ORD", "ORDINARY", "NEW", "DEL", "DE", "HLDGS", "HOLDINGS", "GROUP", "GRP", "SA", "NV",
    "AG", "SE", "ADR", "ADS", "SPONS", "SPON", "SPONSORED", "REP", "EACH", "USD", "PAR",
    "AND", "OF", "INTL", "INTERNATIONAL", "TR", "TRUST", "FUND", "ETF", "UNIT", "UNITS",
    "SER", "SERIES", "CMN", "REG", "REGISTERED", "FRMLY", "FORMERLY", "US", "USA", "AS",
    "NPV", "CAP", "CAPITAL",
})

_CUSIP_RE = re.compile(r"^[0-9A-Z]{9}$")
_ZIP_LINK_RE = re.compile(r'href="([^"]+_form13f\.zip)"', re.IGNORECASE)
_WINDOW_RE = re.compile(r"(\d{2}[a-z]{3}\d{4})-(\d{2}[a-z]{3}\d{4})_form13f\.zip$", re.IGNORECASE)
_QUARTER_RE = re.compile(r"(\d{4})q([1-4])_form13f\.zip$", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[A-Z0-9]+")


class BuildRefused(RuntimeError):
    """The inputs are not what the index can be built from; nothing is written."""


# ---------------------------------------------------------------------------
# The listing page and the downloads
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DataSet:
    name: str  # the zip's file name, e.g. 01jun2026-31aug2026_form13f.zip
    url: str
    filed_through: dt.date  # the last filing day the set covers


def _day(text: str) -> dt.date:
    return dt.datetime.strptime(text, "%d%b%Y").date()


def window_end(name: str) -> dt.date | None:
    """The last filing day a data set covers, read from its file name."""
    m = _WINDOW_RE.search(name)
    if m:
        return _day(m.group(2))
    m = _QUARTER_RE.search(name)
    if m:
        year, quarter = int(m.group(1)), int(m.group(2))
        if quarter == 4:
            return dt.date(year, 12, 31)
        return dt.date(year, 3 * quarter + 1, 1) - dt.timedelta(days=1)
    return None


def data_sets_from_listing(html: str) -> list[DataSet]:
    """Every data set the listing page links to, newest first."""
    seen: dict[str, DataSet] = {}
    for href in _ZIP_LINK_RE.findall(html):
        name = href.rsplit("/", 1)[-1]
        end = window_end(name)
        if end is None or name in seen:
            continue
        url = href if href.startswith("http") else SEC_HOST + href
        seen[name] = DataSet(name=name, url=url, filed_through=end)
    return sorted(seen.values(), key=lambda d: d.filed_through, reverse=True)


def user_agent(contact: str) -> str:
    # The SEC's fair-access policy asks for a contact address; requests from
    # datacentre addresses without one are refused (see sources/sec_edgar.py).
    return f"OpenCheck Form 13F index builder (+https://opencheck.world) {contact}"


def _get(url: str, contact: str, *, attempts: int = 4) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent(contact)})
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise
            if attempt == attempts:
                raise
            log.warning("HTTP %s for %s; retrying", exc.code, url)
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == attempts:
                raise
            log.warning("%s for %s; retrying", exc, url)
        time.sleep(10 * attempt)  # the SEC asks for no more than 10 requests a second
    raise AssertionError("unreachable")


def download(data_set: DataSet, dest: Path, contact: str) -> Path:
    body = _get(data_set.url, contact)
    if not body.startswith(b"PK"):
        raise BuildRefused(f"{data_set.name} is not a zip ({body[:40]!r})")
    path = dest / data_set.name
    path.write_bytes(body)
    log.info("downloaded %s (%d bytes)", data_set.name, len(body))
    return path


def fetch_frame(frame: str, contact: str) -> list[dict[str, Any]]:
    """One XBRL frame's facts, or [] when the SEC has none for it yet."""
    try:
        body = _get(FRAMES_URL.format(frame=frame), contact)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            log.info("no %s frame yet", frame)
            return []
        raise
    data = json.loads(body)
    return list(data.get("data") or [])


def frames_for(period: dt.date) -> tuple[str, str]:
    """The calendar-quarter instant frame of the period and the next one."""
    quarter = (period.month - 1) // 3 + 1
    nxt_year, nxt_quarter = (period.year + 1, 1) if quarter == 4 else (period.year, quarter + 1)
    return f"CY{period.year}Q{quarter}I", f"CY{nxt_year}Q{nxt_quarter}I"


# ---------------------------------------------------------------------------
# Reading the data sets
# ---------------------------------------------------------------------------


def _rows(zf: zipfile.ZipFile, member: str) -> Iterator[dict[str, str]]:
    try:
        raw = zf.open(member)
    except KeyError as exc:
        raise BuildRefused(f"{zf.filename}: no {member}") from exc
    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
    reader = csv.DictReader(text, delimiter="\t", quoting=csv.QUOTE_NONE)
    missing = [c for c in REQUIRED_COLUMNS[member] if c not in (reader.fieldnames or [])]
    if missing:
        raise BuildRefused(f"{zf.filename}: {member} lacks {', '.join(missing)}")
    yield from reader


def _sec_date(text: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(text.strip(), "%d-%b-%Y").date()
    except ValueError:
        return None


def _int(text: str | None) -> int | None:
    try:
        return int((text or "").strip().replace(",", ""))
    except ValueError:
        return None


@dataclass
class Filing:
    accession: str
    cik: str
    period: dt.date
    filed: dt.date
    amendment: str  # "" original, "restatement", "new_holdings"
    manager: str
    report_type: str
    included_managers: int | None = None
    confidential_omitted: bool = False


def read_filings(zips: Iterable[zipfile.ZipFile]) -> dict[str, Filing]:
    """Every holdings report in the data sets, by accession number."""
    filings: dict[str, Filing] = {}
    for zf in zips:
        submissions = {r["ACCESSION_NUMBER"]: r for r in _rows(zf, "SUBMISSION.tsv")}
        covers = {r["ACCESSION_NUMBER"]: r for r in _rows(zf, "COVERPAGE.tsv")}
        summaries = {r["ACCESSION_NUMBER"]: r for r in _rows(zf, "SUMMARYPAGE.tsv")}
        for acc, sub in submissions.items():
            cover = covers.get(acc)
            if cover is None or sub["SUBMISSIONTYPE"].strip() not in HOLDINGS_TYPES:
                continue
            if cover["REPORTTYPE"].strip().upper() not in REPORT_TYPES:
                continue
            period, filed = _sec_date(sub["PERIODOFREPORT"]), _sec_date(sub["FILING_DATE"])
            if period is None or filed is None:
                continue
            kind = cover["AMENDMENTTYPE"].strip().upper()
            is_amendment = (
                sub["SUBMISSIONTYPE"].strip().endswith("/A")
                or cover["ISAMENDMENT"].strip().upper() == "Y"
            )
            if kind == "NEW HOLDINGS":
                amendment = "new_holdings"
            elif is_amendment:
                amendment = "restatement"  # RESTATEMENT, or an amendment with no stated type
            else:
                amendment = ""
            summary = summaries.get(acc, {})
            filings[acc] = Filing(
                accession=acc,
                cik=sub["CIK"].strip().lstrip("0") or "0",
                period=period,
                filed=filed,
                amendment=amendment,
                manager=" ".join(cover["FILINGMANAGER_NAME"].split()),
                report_type=cover["REPORTTYPE"].strip().upper(),
                included_managers=_int(summary.get("OTHERINCLUDEDMANAGERSCOUNT")),
                confidential_omitted=(summary.get("ISCONFIDENTIALOMITTED") or "").strip().upper() == "Y",
            )
    return filings


def choose_period(latest: zipfile.ZipFile, filings: dict[str, Filing]) -> dt.date:
    """The period with the most holdings reports in the latest data set."""
    in_latest = {r["ACCESSION_NUMBER"] for r in _rows(latest, "SUBMISSION.tsv")}
    counts = Counter(f.period for acc, f in filings.items() if acc in in_latest)
    if not counts:
        raise BuildRefused(f"{latest.filename}: no holdings reports")
    return counts.most_common(1)[0][0]


def select_reports(filings: dict[str, Filing], period: dt.date) -> dict[str, list[Filing]]:
    """Per manager, the filings whose rows make its report for the period.

    The latest original or restatement is the base; NEW HOLDINGS amendments
    filed after it add to it. Order is filing day, then accession number.
    """
    by_cik: dict[str, list[Filing]] = {}
    for f in filings.values():
        if f.period == period:
            by_cik.setdefault(f.cik, []).append(f)
    chosen: dict[str, list[Filing]] = {}
    for cik, group in by_cik.items():
        group.sort(key=lambda f: (f.filed, f.accession))
        bases = [i for i, f in enumerate(group) if f.amendment != "new_holdings"]
        start = bases[-1] if bases else 0
        parts = [group[start]] + [f for f in group[start + 1:] if f.amendment == "new_holdings"]
        chosen[cik] = parts
    return chosen


# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------


def name_tokens(name: str) -> set[str]:
    return {t for t in _TOKEN_RE.findall(name.upper()) if len(t) >= 2 and t not in NAME_STOPWORDS}


def core_tokens(names: list[str]) -> set[str]:
    """Tokens carried by at least CORE_TOKEN_SHARE of the named positions."""
    named = [name_tokens(n) for n in names if n.strip()]
    if len(named) < MIN_NAME_SAMPLE:
        return set()
    counts: Counter[str] = Counter()
    for toks in named:
        counts.update(toks)
    floor = CORE_TOKEN_SHARE * len(named)
    return {t for t, c in counts.items() if c >= floor}


def names_issuer(name: str, core: set[str]) -> bool:
    """A position's issuer name carries a core token — as a word or inside a
    run-together name ("CHEVRONTEXACO CORP", "BERKSHIREHATHAWAYINCDEL")."""
    if not core or not name.strip():
        return True
    squashed = re.sub(r"[^A-Z0-9]", "", name.upper())
    return any(t in squashed for t in core)


def price_check(value: int | None, shares: int, median: float | None) -> str:
    """``ok`` / ``thousands`` / ``unchecked``, or ``implausible`` (left out)."""
    if median is None or not value or shares <= 0:
        return "unchecked"
    ratio = (value / shares) / median
    if 1 / PRICE_BAND <= ratio <= PRICE_BAND:
        return "ok"
    if 1 / PRICE_BAND <= ratio * 1000 <= PRICE_BAND:
        return "thousands"
    return "implausible"


# ---------------------------------------------------------------------------
# The build
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
CREATE TABLE managers (
    cik TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    accession TEXT NOT NULL,
    filed TEXT NOT NULL,
    report_type TEXT NOT NULL,
    amended INTEGER NOT NULL,
    included_managers INTEGER,
    confidential_omitted INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE securities (
    cusip TEXT PRIMARY KEY,
    issuer_name TEXT,
    title_of_class TEXT,
    positions INTEGER NOT NULL,
    stored INTEGER NOT NULL,
    excluded_price INTEGER NOT NULL,
    excluded_name INTEGER NOT NULL,
    median_price REAL
) WITHOUT ROWID;
CREATE TABLE positions (
    cusip TEXT NOT NULL,
    rank INTEGER NOT NULL,
    cik TEXT NOT NULL,
    shares INTEGER NOT NULL,
    value INTEGER,
    value_check TEXT NOT NULL,
    discretion TEXT,
    PRIMARY KEY (cusip, rank)
) WITHOUT ROWID;
CREATE TABLE shares_outstanding (
    cik TEXT PRIMARY KEY,
    shares INTEGER NOT NULL,
    as_of TEXT NOT NULL,
    accession TEXT,
    entity_name TEXT,
    frame TEXT NOT NULL
) WITHOUT ROWID;
"""


def _stamp(moment: dt.datetime) -> str:
    """``meta.built_at``, to the second in UTC (the Phase 318 rule)."""
    return moment.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()


def _most_common(values: Iterable[str]) -> str | None:
    counts = Counter(v for v in values if v)
    return counts.most_common(1)[0][0] if counts else None


def _shares_outstanding(
    facts_by_frame: dict[str, list[dict[str, Any]]], period: dt.date
) -> list[tuple[str, int, str, str | None, str | None, str]]:
    """Per CIK, the fact dated nearest the period end (ties to the later)."""
    best: dict[str, tuple[int, tuple[str, int, str, str | None, str | None, str]]] = {}
    for frame, facts in facts_by_frame.items():
        for fact in facts:
            try:
                cik = str(int(fact["cik"]))
                end = dt.date.fromisoformat(str(fact["end"]))
                shares = int(fact["val"])
            except (KeyError, TypeError, ValueError):
                continue
            if shares <= 0:
                continue
            distance = abs((end - period).days) * 2 - (1 if end >= period else 0)
            row = (cik, shares, end.isoformat(), fact.get("accn"), fact.get("entityName"), frame)
            if cik not in best or distance < best[cik][0]:
                best[cik] = (distance, row)
    return [row for _, row in best.values()]


def build_index(
    zip_paths: list[Path],
    out: Path,
    *,
    data_sets: list[DataSet],
    frames: dict[str, list[dict[str, Any]]] | None = None,
    min_filings: int = MIN_FILINGS,
    top_n: int = TOP_N,
    started: dt.datetime | None = None,
) -> dict[str, str]:
    """Build the index from the data-set zips (newest first) into ``out``."""
    started = started or dt.datetime.now(dt.timezone.utc)
    if not zip_paths:
        raise BuildRefused("no data sets")
    zips = [zipfile.ZipFile(p) for p in zip_paths]
    try:
        filings = read_filings(zips)
        period = choose_period(zips[0], filings)
        reports = select_reports(filings, period)
        if len(reports) < min_filings:
            raise BuildRefused(
                f"only {len(reports)} managers reported for {period}; expected at least {min_filings}"
            )
        accession_to_cik = {f.accession: cik for cik, parts in reports.items() for f in parts}
        log.info("period %s: %d managers, %d filings", period, len(reports), len(accession_to_cik))

        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(out.name + ".building")
        if tmp.exists():
            tmp.unlink()
        conn = sqlite3.connect(tmp)
        try:
            conn.executescript(_SCHEMA)
            conn.execute(
                "CREATE TEMP TABLE staging (cusip TEXT, cik TEXT, shares INTEGER, value INTEGER, "
                "discretion TEXT, issuer TEXT, title TEXT)"
            )
            rows = 0
            for zf in zips:
                batch: list[tuple[Any, ...]] = []
                for r in _rows(zf, "INFOTABLE.tsv"):
                    cik = accession_to_cik.get(r["ACCESSION_NUMBER"])
                    if cik is None or r["PUTCALL"].strip():
                        continue
                    if r["SSHPRNAMTTYPE"].strip().upper() != "SH":
                        continue
                    cusip = r["CUSIP"].strip().upper()
                    shares = _int(r["SSHPRNAMT"])
                    if not _CUSIP_RE.match(cusip) or cusip == "000000000" or not shares or shares <= 0:
                        continue
                    batch.append((
                        cusip, cik, shares, _int(r["VALUE"]),
                        r["INVESTMENTDISCRETION"].strip().upper() or None,
                        " ".join(r["NAMEOFISSUER"].split()), " ".join(r["TITLEOFCLASS"].split()),
                    ))
                    if len(batch) >= 50_000:
                        conn.executemany("INSERT INTO staging VALUES (?,?,?,?,?,?,?)", batch)
                        rows += len(batch)
                        batch.clear()
                conn.executemany("INSERT INTO staging VALUES (?,?,?,?,?,?,?)", batch)
                rows += len(batch)
            log.info("%d information-table rows kept", rows)

            cursor = conn.execute(
                "SELECT cusip, cik, SUM(shares), SUM(value), "
                "GROUP_CONCAT(DISTINCT discretion), MIN(issuer), MIN(title) "
                "FROM staging GROUP BY cusip, cik ORDER BY cusip"
            )
            stats = Counter[str]()
            group: list[tuple[Any, ...]] = []
            current: str | None = None
            for row in cursor:
                if row[0] != current and group:
                    _write_security(conn, group, top_n, stats)
                    group = []
                current = row[0]
                group.append(row)
            if group:
                _write_security(conn, group, top_n, stats)
            conn.execute("DROP TABLE staging")

            stored_ciks = {r[0] for r in conn.execute("SELECT DISTINCT cik FROM positions")}
            manager_rows = []
            for cik, parts in reports.items():
                if cik not in stored_ciks:
                    continue
                base = parts[0]
                included = [p.included_managers for p in parts if p.included_managers is not None]
                manager_rows.append((
                    cik, base.manager, base.accession, parts[-1].filed.isoformat(),
                    base.report_type, int(any(p.amendment for p in parts)),
                    max(included) if included else None,
                    int(any(p.confidential_omitted for p in parts)),
                ))
            conn.executemany("INSERT INTO managers VALUES (?,?,?,?,?,?,?,?)", manager_rows)

            outstanding = _shares_outstanding(frames or {}, period)
            conn.executemany("INSERT INTO shares_outstanding VALUES (?,?,?,?,?,?)", outstanding)

            meta = {
                "schema_version": SCHEMA_VERSION,
                "built_at": _stamp(started),
                "period": period.isoformat(),
                "filed_through": data_sets[0].filed_through.isoformat() if data_sets else "",
                "data_sets": json.dumps([d.name for d in data_sets]),
                "source_page": LISTING_URL,
                "license": LICENSE,
                "managers": str(len(reports)),
                "filings": str(len(accession_to_cik)),
                "securities": str(stats["securities"]),
                "positions": str(stats["positions"]),
                "positions_stored": str(stats["stored"]),
                "excluded_price": str(stats["excluded_price"]),
                "excluded_name": str(stats["excluded_name"]),
                "value_in_thousands": str(stats["thousands"]),
                "shares_outstanding": str(len(outstanding)),
                "top_n": str(top_n),
            }
            conn.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
            conn.commit()
            conn.execute("VACUUM")
        finally:
            conn.close()
        os.replace(tmp, out)
        return meta
    finally:
        for zf in zips:
            zf.close()


def _write_security(
    conn: sqlite3.Connection, group: list[tuple[Any, ...]], top_n: int, stats: Counter[str]
) -> None:
    """Check one CUSIP's positions and store its top ``top_n``."""
    cusip = group[0][0]
    core = core_tokens([g[5] or "" for g in group])
    named = [g for g in group if names_issuer(g[5] or "", core)]

    # The reference price comes from the rows that name the issuer, so a
    # cluster of wrong-CUSIP rows cannot move it.
    prices = [g[3] / g[2] for g in (named or group) if g[3] and g[2] > 0]
    median = statistics.median(prices) if len(prices) >= MIN_PRICE_SAMPLE else None
    kept: list[tuple[tuple[Any, ...], str]] = []
    excluded_price = excluded_name = 0
    for g in group:
        verdict = price_check(g[3], g[2], median)
        if verdict == "implausible":
            excluded_price += 1
            continue
        # A name with none of the core tokens is a wrong CUSIP only when the
        # price does not vouch for the row: "IBM", "RAYTHEON" or a fund's own
        # name under a trust's CUSIP are the issuer at the issuer's price,
        # while "Axon Enterprise" under Berkshire A's CUSIP prices a
        # thousandth of it and would otherwise read as value in thousands.
        if verdict != "ok" and not names_issuer(g[5] or "", core):
            excluded_name += 1
            continue
        kept.append((g, verdict))
    kept.sort(key=lambda item: (-item[0][2], item[0][1]))
    stored = kept[:top_n]

    conn.executemany(
        "INSERT INTO positions VALUES (?,?,?,?,?,?,?)",
        [
            (cusip, rank, g[1], g[2], g[3], verdict, g[4])
            for rank, (g, verdict) in enumerate(stored, start=1)
        ],
    )
    conn.execute(
        "INSERT INTO securities VALUES (?,?,?,?,?,?,?,?)",
        (
            cusip,
            _most_common(g[5] for g, _ in kept),
            _most_common(g[6] for g, _ in kept),
            len(kept),
            len(stored),
            excluded_price,
            excluded_name,
            median,
        ),
    )
    stats["securities"] += 1
    stats["positions"] += len(kept)
    stats["stored"] += len(stored)
    stats["excluded_price"] += excluded_price
    stats["excluded_name"] += excluded_name
    stats["thousands"] += sum(1 for _, v in kept if v == "thousands")


def gzip_file(path: Path) -> Path:
    out = path.with_name(path.name + ".gz")
    with open(path, "rb") as src, gzip.open(out, "wb", compresslevel=9) as dst:
        shutil.copyfileobj(src, dst, length=1 << 20)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, help="the SQLite file to write")
    parser.add_argument(
        "--contact", default=os.environ.get("OPENCHECK_EDGAR_CONTACT_EMAIL", ""),
        help="contact e-mail for the SEC's User-Agent (default: $OPENCHECK_EDGAR_CONTACT_EMAIL)",
    )
    parser.add_argument(
        "--zip", type=Path, action="append", default=[],
        help="build from these data-set zips (newest first) instead of downloading",
    )
    parser.add_argument(
        "--frames-dir", type=Path,
        help="read the shares-outstanding frames from <frame>.json files here instead",
    )
    parser.add_argument(
        "--latest", action="store_true",
        help="print the newest data set's file name and exit (the workflow's change check)",
    )
    parser.add_argument(
        "--schema", action="store_true",
        help="print the index schema version and exit (no network)",
    )
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    parser.add_argument("--min-filings", type=int, default=MIN_FILINGS, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.schema:
        print(SCHEMA_VERSION)
        return 0

    if not args.zip and "@" not in args.contact:
        log.error("refused: the SEC asks for a contact e-mail (--contact or OPENCHECK_EDGAR_CONTACT_EMAIL)")
        return 2
    started = dt.datetime.now(dt.timezone.utc)
    scratch: tempfile.TemporaryDirectory[str] | None = None
    try:
        if args.zip:
            zip_paths = list(args.zip)
            data_sets = [
                DataSet(p.name, str(p), window_end(p.name) or dt.date.min) for p in zip_paths
            ]
        else:
            listing = data_sets_from_listing(_get(LISTING_URL, args.contact).decode("utf-8", "replace"))
            if not listing:
                raise BuildRefused("the listing page links to no data sets")
            if args.latest:
                print(listing[0].name)
                return 0
            data_sets = listing[:2]
            scratch = tempfile.TemporaryDirectory(prefix="form13f-")
            zip_paths = [download(d, Path(scratch.name), args.contact) for d in data_sets]
        if args.out is None:
            parser.error("--out is required")

        # The period decides the frames; read it the same way build_index does.
        with zipfile.ZipFile(zip_paths[0]) as latest:
            period = choose_period(latest, read_filings([latest]))
        frames: dict[str, list[dict[str, Any]]] = {}
        for frame in frames_for(period):
            if args.frames_dir:
                path = args.frames_dir / f"{frame}.json"
                frames[frame] = json.loads(path.read_text())["data"] if path.exists() else []
            else:
                frames[frame] = fetch_frame(frame, args.contact)
        meta = build_index(
            zip_paths, args.out, data_sets=data_sets, frames=frames,
            min_filings=args.min_filings, started=started,
        )
    except BuildRefused as exc:
        log.error("refused: %s", exc)
        return 2
    finally:
        if scratch is not None:
            scratch.cleanup()
    if args.gzip:
        gz = gzip_file(args.out)
        log.info("gzipped: %s (%d bytes)", gz, gz.stat().st_size)
    print(" ".join(f"{k}={v}" for k, v in meta.items() if k not in {"source_page", "data_sets"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
