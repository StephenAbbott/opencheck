"""United Kingdom — Find a Tender, public contracts awarded to a company.

Find a Tender (FTS) is the UK government's procurement notice service. Since
the Procurement Act 2023 took effect on 24 February 2025 every notice for a
covered procurement is published there; before that it carried the notices
for above-threshold contracts that used to go to the EU's TED. FTS publishes
the notices in the Open Contracting Data Standard (OCDS) under the Open
Government Licence v3.0. This adapter answers one question from them: **has
this company been awarded UK public contracts, by whom, and for what**.

Why the build reads the OCP Data Registry, not the FTS API
----------------------------------------------------------
The FTS API (``/api/1.0/ocdsReleasePackages``) is keyless and paged, but it
has no supplier filter — its only parameters are stages, a page size, a
cursor and an update window — so it cannot answer "what did company X win"
at request time, and a year-wide crawl pages very slowly. The Open
Contracting Partnership's Data Registry (publication 41) collects the whole
feed every Saturday and publishes it as one file of **compiled releases**
(``full.jsonl.gz``, ~227 MB, one line per procurement, January 2021 on). The
weekly workflow ``refresh-find-tender-index.yml`` reads that file
(``scripts/build_find_tender_index.py``) into a small SQLite index —
~20 MB, ~4 MB gzipped — published as the ``find-tender-index`` release asset
and downloaded at boot by ``warm_index`` (the ChileCompra shape). Without the
file the source is not announced (``covers_lei``).

The join key is the Companies House number
------------------------------------------
Measured on the registry file of 7 November 2026 (Stephen's decisions A–F on
the Notion ticket "Find a Tender - UK procurement data", 12 Nov 2026):

* No supplier carries an LEI. 1,851 of the 62,935 ISSUED UK LEIs appear as
  suppliers; the index is keyed on the **Companies House number** and the
  lookup reaches it from GLEIF's ``registeredAs`` under the three Companies
  House registration authorities (or from a Companies House register lookup).
* Under the Procurement Act about half of supplier parties carry a
  ``GB-COH`` identifier and most carry a ``GB-PPON`` (the Central Digital
  Platform's own number). A PPON that the file pairs with exactly one CH
  number anywhere is read as that company (decision D) — 23 of 17,239 PPONs
  pair with more than one and are left out. A CH-shaped number with no
  scheme is ignored (decision F).
* Before the Act (``legalBasis.scheme`` ``CELEX``) only ~13% of suppliers
  carry a CH number. Those notices are kept (decision C) and the card says
  that coverage is thin.
* Only **awarded** contracts (decision B): an award that is ``cancelled`` or
  ``unsuccessful`` is never stored; ``pending`` (awarded, contract not yet
  signed) is kept and said. A contract the buyer later terminated or
  cancelled keeps its row with that status.
* ``parties[].control`` holds free-text "connected persons" — names,
  addresses and months of birth — on ~1,700 parties. **It is never read**
  (decision E), nor are contact points or addresses: the builder copies
  names and identifiers only.

What the values mean
--------------------
An award's value is the contract's (the award's, else the contract's), in
the notice's currency. A framework or a lot shared by several suppliers
carries one value for all of them, so a value is shown beside the number of
suppliers it was awarded to and is **never summed** — not per supplier, not
per buyer. Procurement is activity, not ownership: the entity statement
carries the activity, as TED's and ChileCompra's do, and nothing here raises
a signal.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import re
import sqlite3
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import provenance
from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.find_tender_uk import FindTenderBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Where the data is
# ---------------------------------------------------------------------------

REGISTRY_URL = "https://data.open-contracting.org/en/publication/41/download?name=full.jsonl.gz"
REGISTRY_PAGE = "https://data.open-contracting.org/en/publication/41"
_HOMEPAGE = "https://www.find-tender.service.gov.uk/"
SEARCH_URL = "https://www.find-tender.service.gov.uk/Search"
NOTICE_URL = "https://www.find-tender.service.gov.uk/Notice/{notice}"

LICENSE_ID = "OGL-3.0"

#: Bumped whenever the index tables change shape.
INDEX_SCHEMA_VERSION = "1"

#: GLEIF's three Companies House registration authorities (England & Wales,
#: Northern Ireland, Scotland) — the same codes ``ra_codes`` holds for GB.
GB_CH_RA_CODES: frozenset[str] = frozenset({"RA000585", "RA000586", "RA000587"})

#: Award statuses that are not an award to the supplier.
_SKIPPED_AWARD_STATUSES = frozenset({"cancelled", "unsuccessful"})

#: How many awards the card lists: the newest and the largest.
LATEST_KEPT = 5
LARGEST_KEPT = 3
TITLE_CHARS = 140

#: The builder refuses a file this small: the registry answering with an
#: error page or a truncated download, not a quiet week.
MIN_RELEASES = 100_000
MIN_SUPPLIERS = 10_000
MAX_MALFORMED = 1_000

_CH_SHAPE = re.compile(r"(?:\d{8}|[A-Z]{2}\d{6})")
_NOTICE_IN_URL = re.compile(r"/Notice/(\d{6}-\d{4})\b")
_NOTICE_PREFIX = re.compile(r"(\d{6}-\d{4})(?:-|$)")


def normalise_ch(raw: str | None) -> str:
    """A Companies House number as ``01234567`` / ``SC123456``, or raise.

    Whitespace and case are ignored, and an all-digit number is zero-padded to
    eight: FTS writes ``9483417`` and ``09483417`` for the same company.
    """
    text = re.sub(r"\s", "", str(raw or "")).upper()
    if text.isdigit() and 0 < len(text) <= 8:
        text = text.zfill(8)
    if not _CH_SHAPE.fullmatch(text) or text.strip("0") == "":
        raise ValueError(f"not a Companies House number: {raw!r}")
    return text


def is_ch(raw: str | None) -> bool:
    try:
        normalise_ch(raw)
    except ValueError:
        return False
    return True


def notice_url(notice: str | None) -> str:
    return NOTICE_URL.format(notice=notice) if notice else ""


# ---------------------------------------------------------------------------
# Reading the compiled releases
# ---------------------------------------------------------------------------


def _party_ids(party: dict[str, Any]) -> list[tuple[str, str]]:
    """``(scheme, id)`` for a party's identifier and additional identifiers.

    Reads ``identifier`` and ``additionalIdentifiers`` only — never
    ``contactPoint``, ``address`` or ``control`` (decision E).
    """
    out: list[tuple[str, str]] = []
    for ident in [party.get("identifier") or {}, *(party.get("additionalIdentifiers") or [])]:
        if not isinstance(ident, dict):
            continue
        scheme, value = str(ident.get("scheme") or "").strip(), str(ident.get("id") or "").strip()
        if scheme and value:
            out.append((scheme, value))
    return out


def _ch_of(ids: Iterable[tuple[str, str]]) -> str | None:
    for scheme, value in ids:
        if scheme == "GB-COH":
            try:
                return normalise_ch(value)
            except ValueError:
                continue
    return None


def _ppons_of(ids: Iterable[tuple[str, str]]) -> list[str]:
    return [value.upper() for scheme, value in ids if scheme == "GB-PPON"]


def _day(value: Any) -> str | None:
    text = str(value or "")
    return text[:10] if re.match(r"\d{4}-\d{2}-\d{2}", text) else None


def _amount(value: Any) -> tuple[float | None, str]:
    if not isinstance(value, dict):
        return None, ""
    amount = value.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, (int, float)) or amount < 0:
        return None, ""
    return float(amount), str(value.get("currency") or "")


def _notice_of(award: dict[str, Any], contract: dict[str, Any], tender: dict[str, Any]) -> str | None:
    """The FTS notice that records this award, as ``NNNNNN-YYYY``.

    Under the Act an award notice (UK5/UK6) is on the award, a contract
    details notice (UK7) on the contract; a direct award's UK14/UK15 sits on
    the tender. Pre-Act awards carry no document, but their id is the award
    notice's id with a lot suffix (``000102-2021-1``).
    """
    for docs in (award.get("documents"), contract.get("documents")):
        for doc in docs or []:
            match = _NOTICE_IN_URL.search(str((doc or {}).get("url") or ""))
            if match:
                return match.group(1)
    for doc in tender.get("documents") or []:
        if (doc or {}).get("documentType") == "awardNotice":
            match = _NOTICE_IN_URL.search(str(doc.get("url") or ""))
            if match:
                return match.group(1)
    match = _NOTICE_PREFIX.match(str(award.get("id") or ""))
    return match.group(1) if match else None


def _award_date(award: dict[str, Any], contract: dict[str, Any], release: dict[str, Any]) -> tuple[str | None, str]:
    """The award's day and what it is: awarded, signed, published or updated.

    Only 43% of Act awards carry ``award.date`` and almost no pre-Act award
    does, so the day falls back to the contract's signature, then the award
    notice's publication, then the release (the procurement's latest update).
    ``date_basis`` says which.
    """
    if day := _day(award.get("date")):
        return day, "awarded"
    if day := _day(contract.get("dateSigned")):
        return day, "signed"
    for doc in award.get("documents") or []:
        if day := _day((doc or {}).get("datePublished")):
            return day, "published"
    return _day(release.get("date")), "updated"


def iter_releases(path: Path | str, counts: Counter[str]) -> Iterator[dict[str, Any]]:
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
        for line in fh:
            if not line.strip():
                continue
            try:
                release = json.loads(line)
            except json.JSONDecodeError:
                counts["malformed_lines"] += 1
                continue
            if isinstance(release, dict):
                counts["releases"] += 1
                yield release


def ppon_crosswalk(path: Path | str, counts: Counter[str]) -> dict[str, str]:
    """PPON → CH number, from every party that carries both — one-to-one only.

    A PPON paired with two CH numbers anywhere in the file is left out: the
    file cannot say which company it is (decision D).
    """
    pairs: dict[str, set[str]] = {}
    for release in iter_releases(path, counts):
        for party in release.get("parties") or []:
            ids = _party_ids(party or {})
            ch = _ch_of(ids)
            if ch is None:
                continue
            for ppon in _ppons_of(ids):
                pairs.setdefault(ppon, set()).add(ch)
    walk = {ppon: next(iter(chs)) for ppon, chs in pairs.items() if len(chs) == 1}
    counts["ppon_pairs"] = len(walk)
    counts["ppon_ambiguous"] = len(pairs) - len(walk)
    return walk


# ---------------------------------------------------------------------------
# Building the index
# ---------------------------------------------------------------------------


class IndexBuildError(RuntimeError):
    """The input was not what the builder expects; nothing was written."""


_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE supplier (
    ch TEXT PRIMARY KEY,
    name TEXT,
    awards INTEGER NOT NULL,
    act_awards INTEGER NOT NULL,
    buyers INTEGER NOT NULL,
    first_date TEXT,
    last_date TEXT
) WITHOUT ROWID;
CREATE TABLE buyer (id INTEGER PRIMARY KEY, name TEXT, identifier TEXT);
CREATE TABLE award (
    ch TEXT NOT NULL,
    ocid TEXT NOT NULL,
    award_id TEXT NOT NULL,
    notice TEXT,
    date TEXT,
    date_basis TEXT,
    title TEXT,
    buyer INTEGER,
    value REAL,
    currency TEXT,
    award_status TEXT,
    contract_status TEXT,
    regime TEXT NOT NULL,
    above_threshold INTEGER,
    suppliers INTEGER NOT NULL,
    via TEXT NOT NULL,
    PRIMARY KEY (ch, ocid, award_id)
) WITHOUT ROWID;
"""


def build_index(
    source: Path | str,
    out_path: Path | str,
    *,
    source_as_of: str | None = None,
    source_url: str = REGISTRY_URL,
    min_releases: int = MIN_RELEASES,
    min_suppliers: int = MIN_SUPPLIERS,
) -> dict[str, str]:
    """Build the SQLite index from the registry's compiled-release file.

    Two passes: the PPON crosswalk, then the awards. Written beside
    ``out_path`` and renamed into place; refuses a short or broken file and
    writes nothing then. Returns the ``meta`` table.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    counts: Counter[str] = Counter()
    walk = ppon_crosswalk(source, Counter())

    buyers: dict[str, int] = {}
    buyer_rows: list[tuple[int, str, str]] = []
    rows: dict[tuple[str, str, str], tuple[Any, ...]] = {}
    names: dict[str, tuple[str, str]] = {}  # ch -> (latest day, name)
    dates: list[str] = []

    for release in iter_releases(source, counts):
        tender = release.get("tender") or {}
        regime = "act" if (tender.get("legalBasis") or {}).get("scheme") == "UKPGA" else "pre_act"
        parties = {str(p.get("id")): p for p in release.get("parties") or [] if isinstance(p, dict)}
        contracts = {str(c.get("awardID")): c for c in release.get("contracts") or [] if isinstance(c, dict)}
        if day := _day(release.get("date")):
            dates.append(day)
        buyer_ref = release.get("buyer") or {}
        buyer_key = str(buyer_ref.get("id") or buyer_ref.get("name") or "")
        buyer_id: int | None = None
        if buyer_key:
            if buyer_key not in buyers:
                buyers[buyer_key] = len(buyers) + 1
                party = parties.get(str(buyer_ref.get("id"))) or {}
                ident = next(iter(_party_ids(party)), None)
                buyer_rows.append((
                    buyers[buyer_key],
                    str(buyer_ref.get("name") or party.get("name") or "").strip(),
                    f"{ident[0]}:{ident[1]}" if ident else "",
                ))
            buyer_id = buyers[buyer_key]
        for award in release.get("awards") or []:
            if not isinstance(award, dict):
                continue
            counts["awards_read"] += 1
            status = str(award.get("status") or "")
            if status in _SKIPPED_AWARD_STATUSES:
                counts[f"awards_{status}"] += 1
                continue
            refs = [s for s in award.get("suppliers") or [] if isinstance(s, dict)]
            contract = contracts.get(str(award.get("id"))) or {}
            notice = _notice_of(award, contract, tender)
            day, basis = _award_date(award, contract, release)
            value, currency = _amount(award.get("value"))
            if value is None:
                value, currency = _amount(contract.get("value"))
            title = str(award.get("title") or tender.get("title") or "").strip()[:TITLE_CHARS]
            above = award.get("aboveThreshold", tender.get("aboveThreshold"))
            for ref in refs:
                counts["supplier_refs"] += 1
                party = parties.get(str(ref.get("id"))) or {}
                ids = _party_ids(party)
                ch, via = _ch_of(ids), "coh"
                if ch is None:
                    ch = next((walk[p] for p in _ppons_of(ids) if p in walk), None)
                    via = "ppon"
                if ch is None:
                    counts["supplier_refs_unidentified"] += 1
                    continue
                counts[f"supplier_refs_via_{via}"] += 1
                name = str(ref.get("name") or party.get("name") or "").strip()
                if name and (ch not in names or (day or "") >= names[ch][0]):
                    names[ch] = (day or "", name)
                rows[(ch, str(release.get("ocid") or ""), str(award.get("id") or ""))] = (
                    notice, day, basis, title, buyer_id, value, currency or None,
                    status or None, (contract.get("status") or None), regime,
                    None if above is None else int(bool(above)), len(refs), via,
                )

    if counts["malformed_lines"] > MAX_MALFORMED:
        raise IndexBuildError(f"{counts['malformed_lines']} malformed lines; refusing the file")
    if counts["releases"] < min_releases:
        raise IndexBuildError(f"only {counts['releases']} releases (< {min_releases}); refusing the file")
    suppliers: dict[str, list[Any]] = {}
    for (ch, _ocid, _aid), row in rows.items():
        entry = suppliers.setdefault(ch, [0, 0, set(), None, None])
        entry[0] += 1
        entry[1] += row[9] == "act"
        if row[4] is not None:
            entry[2].add(row[4])
        if row[1]:
            entry[3] = min(entry[3] or row[1], row[1])
            entry[4] = max(entry[4] or row[1], row[1])
    if len(suppliers) < min_suppliers:
        raise IndexBuildError(f"only {len(suppliers)} identified suppliers (< {min_suppliers}); refusing")

    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(str(tmp))
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO supplier VALUES (?,?,?,?,?,?,?)",
        [
            (ch, names.get(ch, ("", ""))[1], n, act, len(bs), first, last)
            for ch, (n, act, bs, first, last) in sorted(suppliers.items())
        ],
    )
    conn.executemany("INSERT INTO buyer VALUES (?,?,?)", buyer_rows)
    conn.executemany(
        "INSERT INTO award VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(*key, *row) for key, row in sorted(rows.items())],
    )
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "build_seconds": f"{time.monotonic() - started:.1f}",
        "source_url": source_url,
        "source_as_of": source_as_of or "",
        "data_from": min(dates) if dates else "",
        "data_to": max(dates) if dates else "",
        "suppliers": str(len(suppliers)),
        "awards": str(len(rows)),
        "buyers": str(len(buyer_rows)),
        **{k: str(v) for k, v in sorted(counts.items())},
        "ppon_pairs": str(len(walk)),
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp, out_path)
    logger.info("find_tender_uk: index built at %s: %s suppliers", out_path, len(suppliers))
    return meta


# ---------------------------------------------------------------------------
# Where the index lives, and getting it onto the host
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``FIND_TENDER_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().find_tender_db_file
    return Path(configured) if configured else data_root() / "find_tender_uk.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().find_tender_db_file:
            logger.warning("find_tender_uk: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("find_tender_uk: index at %s has an unexpected schema; ignoring", path)
        conn.close()
        return None
    return conn


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        _SHARED["conn"] = _connect()
    return _SHARED.get("conn")


def reset_connection() -> None:
    """Forget the cached connection (after a download, or in tests)."""
    conn = _SHARED.pop("conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - best effort
            pass


def index_available() -> bool:
    return _shared_conn() is not None


def read_meta() -> dict[str, str]:
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Two clocks (Phase 314): the registry's Saturday collection is the
    source's cut; when OpenCheck built the index from it is ours."""
    meta = read_meta() if meta is None else meta
    as_of = provenance.parse_moment(meta.get("source_as_of"))
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=as_of,
        detail=(
            f"Find a Tender OCDS notices, collected by the OCP Data Registry on "
            f"{as_of.date().isoformat()}" if as_of else "Find a Tender OCDS notices (OCP Data Registry)"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state this
    module models (``covers_lei`` is False and the source is not announced).
    """
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.find_tender_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"find_tender_uk": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"find_tender_uk": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"find_tender_uk": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("find_tender_uk: asset check/download failed: %s", exc)
        return {"find_tender_uk": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# Reading one supplier
# ---------------------------------------------------------------------------


def _award_record(row: sqlite3.Row, buyer_names: dict[int, str]) -> dict[str, Any]:
    return {
        "ocid": row["ocid"],
        "award_id": row["award_id"],
        "notice": row["notice"] or "",
        "url": notice_url(row["notice"]),
        "date": row["date"],
        "date_basis": row["date_basis"] or "",
        "title": row["title"] or "",
        "buyer": buyer_names.get(row["buyer"], "") if row["buyer"] is not None else "",
        "value": row["value"],
        "currency": row["currency"] or "",
        "award_status": row["award_status"] or "",
        "contract_status": row["contract_status"] or "",
        "regime": row["regime"],
        "above_threshold": None if row["above_threshold"] is None else bool(row["above_threshold"]),
        "suppliers": row["suppliers"],
        "via": row["via"],
    }


def supplier_record(ch: str) -> dict[str, Any] | None:
    """What the index holds for one Companies House number, or None.

    The card's list: the ``LATEST_KEPT`` newest procurements (one row each,
    however many lots the company won in it), then the ``LARGEST_KEPT``
    largest contracts awarded to the company alone (a reviewer asks about the
    big contracts, not last week's call-off), and the five buyers awarding
    most often.
    """
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM supplier WHERE ch = ?", (ch,)).fetchone()
    if row is None:
        return None
    award_rows = conn.execute("SELECT * FROM award WHERE ch = ?", (ch,)).fetchall()
    wanted = {r["buyer"] for r in award_rows if r["buyer"] is not None}
    buyer_names: dict[int, str] = {}
    if wanted:
        marks = ",".join("?" * len(wanted))
        buyer_names = {
            r["id"]: r["name"] or ""
            for r in conn.execute(f"SELECT id, name FROM buyer WHERE id IN ({marks})", sorted(wanted))
        }
    # One row per procurement: a framework awards a company several lots under
    # one notice, which would otherwise fill the list with one notice.
    by_ocid: dict[str, list[sqlite3.Row]] = {}
    for r in award_rows:
        by_ocid.setdefault(r["ocid"], []).append(r)

    def newest_of(rows: list[sqlite3.Row]) -> sqlite3.Row:
        return max(rows, key=lambda r: (r["date"] or "", r["award_id"]))

    procurements = sorted(
        (newest_of(rows) for rows in by_ocid.values()),
        key=lambda r: (r["date"] or "", r["ocid"]),
        reverse=True,
    )
    chosen = procurements[:LATEST_KEPT]
    seen = {r["ocid"] for r in chosen}
    # Largest = the largest contracts awarded to this company alone. A shared
    # framework's value is every supplier's ceiling, not this one's contract.
    largest = [
        r for r in sorted(award_rows, key=lambda r: (r["value"] or 0, r["date"] or ""), reverse=True)
        if r["value"] and r["suppliers"] == 1 and r["ocid"] not in seen
    ]
    picked: list[sqlite3.Row] = []
    for r in largest:
        if r["ocid"] not in {p["ocid"] for p in picked}:
            picked.append(r)
        if len(picked) == LARGEST_KEPT:
            break
    records = [
        {**_award_record(r, buyer_names), "lots": len(by_ocid[r["ocid"]]), "kept_as": "latest"}
        for r in chosen
    ]
    records += [
        {**_award_record(r, buyer_names), "lots": len(by_ocid[r["ocid"]]), "kept_as": "largest"}
        for r in picked
    ]
    per_buyer = Counter(r["buyer"] for r in award_rows if r["buyer"] is not None)
    buyers = [
        {"name": buyer_names.get(b, ""), "awards": n}
        for b, n in sorted(per_buyer.items(), key=lambda kv: (-kv[1], buyer_names.get(kv[0], "")))[:5]
    ]
    regimes = Counter(r["regime"] for r in award_rows)
    return {
        "supplier": dict(row),
        "records": records,
        "buyers": buyers,
        "pre_act_awards": regimes.get("pre_act", 0),
        "via_ppon": sum(1 for r in award_rows if r["via"] == "ppon"),
    }


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class FindTenderUkAdapter(SourceAdapter):
    """UK public contracts from Find a Tender, via the Companies House number."""

    id = "find_tender_uk"

    # No ``lookup_derivers``: procurement is activity, reached the way TED and
    # ChileCompra are — ``fetch_by_identifiers`` from ``_dispatch`` — never a
    # register adapter claiming the Companies House RA codes.

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Find a Tender (UK public procurement)",
            homepage=_HOMEPAGE,
            description=(
                "The UK government's procurement notice service, published in "
                "the Open Contracting Data Standard and collected weekly by the "
                "Open Contracting Partnership's Data Registry. For a company: "
                "the public contracts awarded to it since 2021, with buyer, "
                "value and a link to each notice. Matched on the Companies "
                "House number (or a PPON the notices pair with one). "
                "Procurement activity, not ownership."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains public sector information licensed under the Open "
                "Government Licence v3.0: Find a Tender service notices "
                "(Cabinet Office), as collected by the Open Contracting "
                "Partnership's Data Registry."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=False,
            # None, like TED and ChileCompra: ``country`` ties a source to the
            # knowability row, which is about what a register says of owners.
            country=None,
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file (the ChileCompra rule): a source that
        announces itself and cannot answer would count as consulted."""
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Not name-searchable: reached from the Companies House number."""
        return []

    def _bundle(self, ch: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "company_number": ch,
            "legal_name": legal_name or "",
            "supplier": None,
            "records": [],
            "buyers": [],
            "pre_act_awards": 0,
            "via_ppon": 0,
            "data_from": None,
            "data_to": None,
            "source_as_of": None,
            "link": SEARCH_URL,
            "not_found": False,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """What Find a Tender's notices say about one Companies House number."""
        try:
            ch = normalise_ch(hit_id)
        except ValueError:
            return self._bundle("", legal_name)
        if _shared_conn() is None:
            return self._bundle(ch, legal_name)
        meta = read_meta()
        declare_snapshot(meta)
        window = {
            "data_from": meta.get("data_from") or None,
            "data_to": meta.get("data_to") or None,
            "source_as_of": (meta.get("source_as_of") or "")[:10] or None,
        }
        found = supplier_record(ch)
        if found is None:
            return self._bundle(ch, legal_name, not_found=True, is_stub=False, **window)
        bundle = self._bundle(ch, legal_name, is_stub=False, **window, **found)
        validate_raw(self.id, FindTenderBundle, bundle)
        return bundle

    @staticmethod
    def applies_to(registered_at: str, registered_as: str) -> bool:
        """A Companies House registration authority and a CH-shaped number —
        asked before dispatch, so a non-UK company never lists this source."""
        return registered_at in GB_CH_RA_CODES and is_ch(registered_as)

    async def fetch_by_identifiers(
        self,
        lei: str,
        registered_as: str,
        registered_at: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any] | None:
        """The lookup pipeline's entry point, the ChileCompra shape."""
        if not self.applies_to(registered_at, registered_as):
            return None
        return await self.fetch(normalise_ch(registered_as), legal_name=legal_name)
