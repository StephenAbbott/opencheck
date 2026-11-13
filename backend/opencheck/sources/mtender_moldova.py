"""Moldova — MTender, the public procurement system run by the Ministry of Finance.

Under Law No. 131/2015 on public procurement every Moldovan public body buys
through MTender, and the system publishes each procurement as OCDS in real
time. The Open Contracting Partnership's Data Registry (publication 34)
compiles it into one ``full.jsonl.gz``, retrieved monthly. This adapter
answers one question from it: **which public procurements has this company
been awarded, by which buyers** — awards won, never ownership.

What the file holds (measured on the registry's retrieval of 21 Oct 2026)
-----------------------------------------------------------------------
125,774 compiled releases; 115,289 awards and **no contracts**: MTender
publishes no contract stage. Every award's ``status`` reads ``pending`` (a
quality note on the registry page); the award's real state is in
``awards/statusDetails`` — ``active`` is the winning award (68,105),
``unsuccessful`` a rejected bid, and ``empty`` / ``consideration`` /
``awaiting`` not decided. So only ``statusDetails == "active"`` is read, and
the card says *awarded*, never *contract signed*. 332 active awards sit on a
cancelled tender or lot; they are skipped.

Active awards are almost all from 2025 and 2026 (86 in 2023, 139 in 2024): the
registry notes that MTender's listing returns nothing older than about a year,
so earlier processes may be missing. And the latest award in the 21 Oct 2026
retrieval is dated 3 March 2026: every release the registry compiled since
May 2026 is a planning record with no tender or award stage — while
MTender's API holds awards made this week. OpenCheck's own harvest of the API
closes that gap (see "Index, not live API" below). The card says the period
it covers (``data_to``).

Three quarters of the awards are ``directAward`` — on MTender's own pages
these are reported low-value purchases ("Raportarea achiziției de valoare
mică"), not competitive procedures. They are kept and labelled by method
(Stephen, 12 Nov 2026).

Test procedures are skipped by their method (``testOpenTender``…) or OCID,
never by title: "teste de laborator" is a real purchase of laboratory tests.

The join key is the IDNO
------------------------
107,252 of 107,254 supplier parties carry ``MD-IDNO`` (two say ``AE-ACCI``
with an IDNO as the value; read the same way). Only a 13-digit number that
passes the 7-3-1 check digit **and starts with 1** — a legal person's IDNO —
is kept: numbers starting 0 or 2 are natural persons' IDNPs (named
individuals, patent holders) and are never stored. Sole enterprises, peasant
farms and independent entrepreneurs carry legal-person IDNOs but are a person
trading in their own name; they are recognised by the legal form in the name
(``sole_trader_name``) and dropped, as ``asp_moldova`` drops them (Stephen,
12 Nov 2026). 14 of the 42 ISSUED Moldovan LEIs are suppliers.

GLEIF files the IDNO under three authorities (``MD_RA_CODES``), so the LEI
reaches the index directly; a register-number lookup on ``MD-IDNO`` reaches
it with the number itself.

Never read: a party's ``contactPoint`` (a named person, e-mail and phone on
every party), its address, bids, documents and requirement responses.

Dates are converted to the Chișinău day. The link is the procurement's page
on MTender, ``https://mtender.gov.md/tenders/<ocid>`` (checked in a browser on
12 Nov 2026 for an open tender and a reported low-value purchase).

Index, not live API
-------------------
MTender's API (``public.mtender.gov.md/tenders/``) has no supplier filter,
so the index is built weekly by ``.github/workflows/refresh-mtender-index.yml``
(``scripts/build_mtender_index.py``), published as the ``mtender-index``
release asset and downloaded at boot (``warm_index``). Without it the source
is not announced (``covers_lei``).

Since the harvest ticket (13 Nov 2026) the build reads two inputs: OpenCheck's
own harvest of the API (``opencheck/mtender_harvest.py``, kept between runs
on the ``mtender-harvest`` release), which reaches the awards made since
March 2026, and the registry's file for anything older. Where both hold a
procedure, the harvest's reading wins. The API keeps a procedure's awards on
its stage records (``<ocid>-EV-…``), not on the procedure's own record;
``mtender_harvest.flatten`` puts them back into the one release this module
reads, the shape the registry's file already has.

Licence: none is named — not by the registry (which links
``opendefinition.org/licenses/``) and not by mtender.gov.md, whose terms cover
using the platform. MTender says it publishes "by default" as open data.
Recorded as ``MTender-Open-Data`` (verify before commercial reuse) while the
Ministry of Finance is asked to confirm (Stephen, 12 Nov 2026).
"""

from __future__ import annotations

import gzip
import heapq
import json
import logging
import os
import re
import sqlite3
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .. import provenance
from ..config import get_settings
from .asp_moldova import MD_RA_CODES, fold, idno_check_digit
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.mtender_moldova import MTenderBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Where the data is
# ---------------------------------------------------------------------------

REGISTRY_URL = "https://data.open-contracting.org/en/publication/34"
FULL_FILE_URL = "https://data.open-contracting.org/en/publication/34/download?name=full.jsonl.gz"
_HOMEPAGE = "https://mtender.gov.md/"
TENDER_URL_TEMPLATE = "https://mtender.gov.md/tenders/{ocid}"
LICENSE_ID = "MTender-Open-Data"

#: Bumped whenever the index tables change shape.
INDEX_SCHEMA_VERSION = "1"

TOP_BUYERS = 5
LATEST_KEPT = 8
LARGEST_KEPT = 4
TITLE_CHARS = 120

_CHISINAU = ZoneInfo("Europe/Chisinau")

#: ``tender/procurementMethodDetails`` as filed → a one-letter code.
_METHODS: dict[str, str] = {
    "directAward": "D",
    "microValue": "M",
    "smallValue": "S",
    "Procedură de valoare mică": "S",
    "openTender": "O",
    "Licitație deschisă": "O",
    "negotiatedProcedure": "N",
    "restrictedTender": "R",
    "requestForQuotations": "Q",
    "directCallOff": "C",
}
METHOD_WORDS: dict[str, str] = {
    "D": "Low-value purchase, reported (direct award)",
    "M": "Micro-value procurement",
    "S": "Small-value procedure",
    "O": "Open tender",
    "N": "Negotiated procedure",
    "R": "Restricted tender",
    "Q": "Request for quotations",
    "C": "Call-off from a framework",
    "X": "Other procedure",
}
#: The supplier-level tallies, in the order the card lists them.
METHOD_KEYS = ("D", "O", "S", "M", "X")


# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------


def legal_person_idno(raw: Any) -> str | None:
    """A legal person's IDNO (13 digits, check digit, leading 1), else None.

    Numbers starting 0 or 2 are natural persons' IDNPs and are never kept.
    """
    text = re.sub(r"\s+", "", str(raw or ""))
    if not re.fullmatch(r"1\d{12}", text):
        return None
    return text if idno_check_digit(text[:12]) == text[12] else None


#: The legal forms of a person trading in their own name, as MTender's
#: supplier names spell them (folded, quotes read as spaces): sole
#: enterprise (Î.I.), peasant farm (G.Ț., gospodăria țărănească) and
#: independent entrepreneur (A.I., antreprenor independent). Measured on the
#: 21 Oct 2026 file: 482 of 7,964 legal-person suppliers.
_SOLE_TRADER = re.compile(
    r"^(?:I\.?\s?I\b\.?|G\.?\s?T\b\.?|A\.\s?I\.)"
    r"|\bI\.\s?I\.?$|\bII$"
    r"|\bINTREPRINDEREA? INDIVIDUALA\b"
    r"|\bGOSPODARI[AE] TARANEASCA\b|\bGOSPODARIA DE FERMIER\b"
    r"|\bANTREPRENOR(?:UL)? INDEPENDENT\b"
)


def sole_trader_name(name: str | None) -> bool:
    """Whether a supplier's name gives a sole-trader legal form."""
    text = " ".join(fold(name).replace('"', " ").replace("«", " ").replace("»", " ").split())
    return bool(_SOLE_TRADER.search(text))


def candidate_idno(registered_at: str, registered_as: str) -> str | None:
    """The IDNO GLEIF's (registeredAt, registeredAs) pair names, or None."""
    if registered_at not in MD_RA_CODES:
        return None
    return legal_person_idno(registered_as)


def tender_url(ocid: str | None) -> str:
    return TENDER_URL_TEMPLATE.format(ocid=ocid) if ocid else _HOMEPAGE


def local_day(stamp: Any) -> str | None:
    """An OCDS timestamp (UTC) → the calendar day in Chișinău."""
    text = str(stamp or "").strip()
    if not text:
        return None
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", text) else None
    if moment.tzinfo is None:
        return moment.date().isoformat()
    return moment.astimezone(_CHISINAU).date().isoformat()


def _amount(value: Any) -> int | None:
    """A published amount → whole units, half up. Unreadable → None."""
    try:
        return int(Decimal(str(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Index build
# ---------------------------------------------------------------------------


class IndexBuildError(RuntimeError):
    """The file is not what the index builder understands."""


@dataclass
class _Supplier:
    name: str = ""
    name_date: str = ""
    awards: int = 0  # procurements won (one per OCID)
    lots: int = 0
    value_mdl: int = 0
    other_currency: int = 0
    first: str = "9999-99-99"
    last: str = "0000-00-00"
    methods: dict[str, int] = field(default_factory=dict)
    # buyer id → [awards, value]
    buyers: dict[int, list[int]] = field(default_factory=dict)
    latest: list[tuple[str, str, tuple[Any, ...]]] = field(default_factory=list)
    largest: list[tuple[int, str, tuple[Any, ...]]] = field(default_factory=list)


def _keep(heap: list[Any], entry: Any, size: int) -> None:
    if len(heap) < size:
        heapq.heappush(heap, entry)
    elif entry > heap[0]:
        heapq.heapreplace(heap, entry)


def iter_releases(path: Path | str) -> Iterator[dict[str, Any]]:
    """The compiled releases in a registry ``.jsonl`` or ``.jsonl.gz``."""
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _is_test(release: dict[str, Any], method_text: str) -> bool:
    """MTender's own test procedures (``testMiniCompetition`` and the like)."""
    return method_text.lower().startswith("test") or "test" in str(release.get("ocid") or "").lower()


def build_index(
    releases: Iterable[dict[str, Any]],
    out_path: Path | str,
    *,
    source_meta: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the SQLite index from compiled releases.

    Written beside ``out_path`` and renamed into place, so a reader never sees
    a half-built file. Returns the ``meta`` table.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    counts = {
        k: 0
        for k in (
            "releases", "awards", "awards_active", "awards_on_cancelled",
            "awards_test", "awards_person_or_invalid_id", "awards_sole_trader",
            "awards_kept",
        )
    }
    suppliers: dict[str, _Supplier] = {}
    buyers: dict[tuple[str, str], int] = {}
    buyer_rows: dict[int, tuple[str, str]] = {}
    data_from, data_to = "9999-99-99", ""

    def buyer_id(name: str, idno: str) -> int:
        key = (idno, name if not idno else "")
        if key not in buyers:
            buyers[key] = len(buyers) + 1
        bid = buyers[key]
        if name or bid not in buyer_rows:
            buyer_rows[bid] = (name, idno)
        return bid

    for release in releases:
        counts["releases"] += 1
        ocid = str(release.get("ocid") or "").strip()
        tender = release.get("tender") or {}
        method_text = str(tender.get("procurementMethodDetails") or "")
        parties = {p.get("id"): p for p in release.get("parties") or []}
        lots = {lot.get("id"): lot for lot in tender.get("lots") or []}
        awards = release.get("awards") or []
        counts["awards"] += len(awards)
        # (ocid, supplier) → [lots, value, other_currency, latest_day, currency]
        won: dict[str, list[Any]] = {}
        for award in awards:
            if award.get("statusDetails") != "active":
                continue
            counts["awards_active"] += 1
            related = [lots.get(lid) or {} for lid in award.get("relatedLots") or []]
            if tender.get("status") == "cancelled" or any(
                lot.get("status") == "cancelled" for lot in related
            ):
                counts["awards_on_cancelled"] += 1
                continue
            if _is_test(release, method_text):
                counts["awards_test"] += 1
                continue
            day = local_day(award.get("date"))
            value = award.get("value") or {}
            amount = _amount(value.get("amount"))
            currency = str(value.get("currency") or "")
            for ref in award.get("suppliers") or []:
                party = parties.get(ref.get("id")) or {}
                ident = party.get("identifier") or {}
                idno = legal_person_idno(ident.get("id"))
                if idno is None:
                    counts["awards_person_or_invalid_id"] += 1
                    continue
                name = str(party.get("name") or ident.get("legalName") or ref.get("name") or "").strip()
                if sole_trader_name(name):
                    counts["awards_sole_trader"] += 1
                    continue
                counts["awards_kept"] += 1
                entry = won.setdefault(idno, [0, 0, 0, "", currency or "MDL", name])
                entry[0] += 1
                if currency in ("", "MDL"):
                    entry[1] += amount or 0
                else:
                    entry[2] += 1
                    entry[4] = currency
                if day and day > entry[3]:
                    entry[3] = day
                if name:
                    entry[5] = name
        if not won:
            continue
        procuring = tender.get("procuringEntity") or {}
        bparty = parties.get(procuring.get("id")) or {}
        bident = (bparty.get("identifier") or {}).get("id") or ""
        buyer = buyer_id(
            str(procuring.get("name") or bparty.get("name") or "").strip(),
            re.sub(r"\D", "", str(bident)),
        )
        method = _METHODS.get(method_text, "X")
        title = str(release.get("title") or tender.get("title") or "").strip()[:TITLE_CHARS]
        category = str(tender.get("mainProcurementCategory") or "")
        for idno, (n_lots, value, other, day, currency, name) in won.items():
            sup = suppliers.setdefault(idno, _Supplier())
            sup.awards += 1
            sup.lots += n_lots
            sup.value_mdl += value
            sup.other_currency += 1 if other else 0
            sup.methods[method if method in METHOD_KEYS else "X"] = (
                sup.methods.get(method if method in METHOD_KEYS else "X", 0) + 1
            )
            if day:
                data_from = min(data_from, day)
                data_to = max(data_to, day)
                sup.first = min(sup.first, day)
                sup.last = max(sup.last, day)
                if name and day >= sup.name_date:
                    sup.name, sup.name_date = name, day
            elif name and not sup.name:
                sup.name = name
            tally = sup.buyers.setdefault(buyer, [0, 0])
            tally[0] += 1
            tally[1] += value
            record = (day, buyer, title, value if not other else None,
                      "MDL" if not other else currency, method, category, n_lots)
            _keep(sup.latest, (day or "", ocid, record), LATEST_KEPT)
            if not other:
                _keep(sup.largest, (value, ocid, record), LARGEST_KEPT)

    if counts["awards_kept"] < 1:
        raise IndexBuildError("no active award names a legal-person MD-IDNO — not the MTender file shape")

    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(str(tmp))
    conn.executescript(_SCHEMA)
    buyers_used: set[int] = set()
    for idno, sup in suppliers.items():
        conn.execute(
            "INSERT INTO supplier VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                int(idno), sup.name, sup.awards, sup.lots, sup.value_mdl,
                sup.other_currency, len(sup.buyers),
                sup.first if sup.first != "9999-99-99" else None,
                sup.last if sup.last != "0000-00-00" else None,
                *(sup.methods.get(k, 0) for k in METHOD_KEYS),
            ),
        )
        ranked = sorted(sup.buyers.items(), key=lambda kv: (-kv[1][1], -kv[1][0], kv[0]))
        for bid, (n, value) in ranked[:TOP_BUYERS]:
            buyers_used.add(bid)
            conn.execute("INSERT INTO supplier_buyer VALUES (?,?,?,?)", (int(idno), bid, n, value))
        kept: dict[str, tuple[Any, ...]] = {}
        for _key, ocid, row in (*sup.latest, *sup.largest):
            kept[ocid] = row
        for ocid, (day, bid, title, value, currency, method, category, n_lots) in kept.items():
            buyers_used.add(bid)
            conn.execute(
                "INSERT INTO award VALUES (?,?,?,?,?,?,?,?,?,?)",
                (int(idno), ocid, day, bid, title, value, currency, method, category, n_lots),
            )
    conn.executemany(
        "INSERT INTO buyer VALUES (?,?,?)",
        [(b, *buyer_rows.get(b, ("", ""))) for b in sorted(buyers_used)],
    )
    stored = conn.execute("SELECT COUNT(*) FROM supplier").fetchone()[0]
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "build_seconds": f"{time.monotonic() - started:.1f}",
        "data_from": data_from if data_from != "9999-99-99" else "",
        "data_to": data_to,
        "suppliers": str(stored),
        **{k: str(v) for k, v in counts.items()},
        **(source_meta or {}),
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp, out_path)
    logger.info("mtender_moldova: index built at %s: %s suppliers", out_path, stored)
    return meta


_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE supplier (
    idno INTEGER PRIMARY KEY,
    name TEXT,
    awards INTEGER NOT NULL,
    lots INTEGER NOT NULL,
    value_mdl INTEGER NOT NULL,
    other_currency INTEGER NOT NULL,
    buyers INTEGER NOT NULL,
    first_date TEXT,
    last_date TEXT,
    method_direct INTEGER NOT NULL,
    method_open INTEGER NOT NULL,
    method_small INTEGER NOT NULL,
    method_micro INTEGER NOT NULL,
    method_other INTEGER NOT NULL
);
CREATE TABLE buyer (id INTEGER PRIMARY KEY, name TEXT, idno TEXT);
CREATE TABLE supplier_buyer (
    idno INTEGER NOT NULL,
    buyer INTEGER NOT NULL,
    awards INTEGER NOT NULL,
    value_mdl INTEGER NOT NULL,
    PRIMARY KEY (idno, buyer)
) WITHOUT ROWID;
CREATE TABLE award (
    idno INTEGER NOT NULL,
    ocid TEXT NOT NULL,
    awarded TEXT,
    buyer INTEGER,
    title TEXT,
    value INTEGER,
    currency TEXT,
    method TEXT,
    category TEXT,
    lots INTEGER NOT NULL,
    PRIMARY KEY (idno, ocid)
) WITHOUT ROWID;
"""


# ---------------------------------------------------------------------------
# Where the index lives, and getting it onto the host
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``MTENDER_DB_FILE`` or the data-root default."""
    from ..cache import data_root

    configured = get_settings().mtender_db_file
    return Path(configured) if configured else data_root() / "mtender_moldova.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().mtender_db_file:
            logger.warning("mtender_moldova: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("mtender_moldova: index at %s has an unexpected schema; ignoring", path)
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


def _moment(text: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(str(text or "")).replace(tzinfo=UTC) if text else None
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """The cut is how far the harvest has read MTender's listing (every
    procedure changed before that moment was read), else the registry's
    retrieval day; the build is our clock."""
    meta = read_meta() if meta is None else meta
    cut = (
        provenance.parse_moment(meta.get("harvest_cursor"))
        or provenance.parse_moment(meta.get("source_last_modified"))
        or _moment(meta.get("data_to"))
    )
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut,
        detail="MTender procurement awards (OCDS), from MTender's API and the OCP Data Registry",
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.mtender_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"mtender_moldova": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"mtender_moldova": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"mtender_moldova": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("mtender_moldova: asset check/download failed: %s", exc)
        return {"mtender_moldova": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# Reading one supplier
# ---------------------------------------------------------------------------


def supplier_record(idno: str) -> dict[str, Any] | None:
    """Everything the index holds for one IDNO, or None."""
    conn = _shared_conn()
    if conn is None:
        return None
    key = int(idno)
    row = conn.execute("SELECT * FROM supplier WHERE idno = ?", (key,)).fetchone()
    if row is None:
        return None
    raw = dict(row)
    supplier = {
        "idno": idno,
        "name": raw["name"],
        "awards": raw["awards"],
        "lots": raw["lots"],
        "value_mdl": raw["value_mdl"],
        "other_currency": raw["other_currency"],
        "buyers": raw["buyers"],
        "first_date": raw["first_date"],
        "last_date": raw["last_date"],
        "methods": {
            METHOD_WORDS[k]: raw[col]
            for k, col in zip(
                METHOD_KEYS,
                ("method_direct", "method_open", "method_small", "method_micro", "method_other"),
            )
            if raw[col]
        },
    }
    award_rows = conn.execute("SELECT * FROM award WHERE idno = ?", (key,)).fetchall()
    buyer_rows = conn.execute(
        "SELECT * FROM supplier_buyer WHERE idno = ? ORDER BY value_mdl DESC, awards DESC",
        (key,),
    ).fetchall()
    wanted = {r["buyer"] for r in (*award_rows, *buyer_rows) if r["buyer"] is not None}
    names: dict[int, str] = {}
    if wanted:
        marks = ",".join("?" * len(wanted))
        names = {
            r["id"]: r["name"]
            for r in conn.execute(f"SELECT id, name FROM buyer WHERE id IN ({marks})", list(wanted))
        }
    buyers = [
        {"name": names.get(r["buyer"]) or "", "awards": r["awards"], "value_mdl": r["value_mdl"]}
        for r in buyer_rows
    ]
    awards = [
        {
            "ocid": r["ocid"],
            "awarded": r["awarded"],
            "buyer": names.get(r["buyer"]) or "",
            "title": r["title"] or "",
            "value": r["value"],
            "currency": r["currency"] or "MDL",
            "procedure": METHOD_WORDS.get(r["method"] or "X", METHOD_WORDS["X"]),
            "category": r["category"] or "",
            "lots": r["lots"],
            "url": tender_url(r["ocid"]),
        }
        for r in award_rows
    ]
    awards.sort(key=lambda a: (a["awarded"] or "", a["ocid"]), reverse=True)
    return {"supplier": supplier, "buyers": buyers, "awards": awards}


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class MTenderMoldovaAdapter(SourceAdapter):
    """MTender procurement awards, via the IDNO."""

    id = "mtender_moldova"

    # No ``lookup_derivers``: procurement is reached the way TED, ChileCompra
    # and AusTender are, from ``_dispatch`` via ``fetch_by_identifiers``.
    # ``asp_moldova`` owns the Moldovan RA codes as a register.

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="MTender — Moldovan public procurement",
            homepage=_HOMEPAGE,
            description=(
                "Public procurement awards Moldova publishes on MTender (Ministry "
                "of Finance), read weekly from MTender's API, with the Open "
                "Contracting Partnership's Data Registry copy for older "
                "procedures. For a supplier: its latest and "
                "largest awards with buyer, value, procedure and a link to each "
                "procurement on MTender, plus totals over every award in the "
                "file, whose period the card states. MTender publishes awards, "
                "not contracts. "
                "Matched on the IDNO. Awards won, not ownership."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains MTender public procurement data published by the "
                "Ministry of Finance of the Republic of Moldova, read from MTender's "
                "API and via the Open Contracting Partnership's Data Registry."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=False,
            country=None,
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file (the ChileCompra rule)."""
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Not name-searchable: reached from the IDNO GLEIF files."""
        return []

    def _bundle(self, idno: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "idno": idno,
            "legal_name": legal_name or "",
            "supplier": None,
            "buyers": [],
            "awards": [],
            "data_from": None,
            "data_to": None,
            "link": REGISTRY_URL,
            "not_found": False,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """What MTender's awards say about one IDNO."""
        idno = legal_person_idno(hit_id)
        if idno is None:
            return self._bundle("", legal_name)
        if _shared_conn() is None:
            return self._bundle(idno, legal_name)
        meta = read_meta()
        declare_snapshot(meta)
        extra = {
            "data_from": meta.get("data_from") or None,
            "data_to": meta.get("data_to") or None,
        }
        found = supplier_record(idno)
        if found is None:
            return self._bundle(idno, legal_name, not_found=True, is_stub=False, **extra)
        bundle = self._bundle(idno, legal_name, is_stub=False, **extra, **found)
        validate_raw(self.id, MTenderBundle, bundle)
        return bundle

    @staticmethod
    def applies_to(registered_at: str, registered_as: str) -> bool:
        """A Moldovan authority and a legal person's IDNO.

        Asked before dispatch, so a company outside Moldova is never announced
        as checked against MTender.
        """
        return candidate_idno(registered_at, registered_as) is not None

    async def fetch_by_identifiers(
        self,
        lei: str,
        registered_as: str,
        registered_at: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any] | None:
        """The pipeline's entry point (the TED / ChileCompra shape)."""
        idno = candidate_idno(registered_at, registered_as)
        if idno is None:
            return None
        return await self.fetch(idno, legal_name=legal_name)
