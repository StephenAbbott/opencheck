"""Australia — AusTender, the Commonwealth's published contract notices.

The Australian Government's Department of Finance runs AusTender, where every
non-corporate Commonwealth entity must report a contract (and every
amendment) worth AU$10,000 or more within 42 days, and prescribed corporate
entities above higher thresholds. AusTender publishes the notices as OCDS;
the Open Contracting Partnership's Data Registry (publication 19) compiles
them into one ``full.jsonl.gz``, retrieved monthly. This adapter answers one
question from it: **which Commonwealth contracts has this company been
awarded, from which agencies** — contracts won, never ownership.

The join key is the ABN
-----------------------
Measured on the registry file of 12 November 2026 (876,389 compiled
releases, one contract notice each, Jul 2004 – Nov 2026, almost all from
2013 on): 826,852 of 880,821 supplier parties (93.9%) carry an
``AU-ABN`` in ``parties/additionalIdentifiers`` (``parties/identifier`` is
empty, as the registry notes); every one is eleven digits and passes the ABN
check. 53,793 distinct ABNs. The rest are ``EXEMPTUUID-…`` parties with a
name and an address only — mostly overseas suppliers.

GLEIF files Australian companies under two authorities, so both lead here:

* ``RA000013`` (Australian Business Register) — ``registeredAs`` is the ABN.
* ``RA000014`` (ASIC) — ``registeredAs`` is the ACN. A company's ABN is its
  ACN with two check digits in front, so the ABN is **derived**
  (``abns_for_acn``). For one ACN in about a hundred both 10 and 99 are valid
  prefixes; both are looked up and an ACN that finds two suppliers is
  refused rather than guessed.

On 10,000 ISSUED Australian LEIs sampled from GLEIF the same day, 6,083 file
an ACN and 3,466 an ABN; 342 reach a supplier — 281 through the derived ABN,
61 through a filed one. Without the ACN route four in five matches would be
missed.

What the registry warns about, and what OpenCheck does with it
--------------------------------------------------------------
* **A cancelled amendment overwrites the contract's status, period and
  value.** 26,501 contracts (3%) read ``cancelled``, including some AusTender
  shows as active. So no contract status is shown: a contract marked
  cancelled carries ``marked_cancelled`` and the card says what the mark
  can mean. Values and periods are shown as published.
* **A supplier renamed mid-contract gets a new party** and the old one stays.
  A contract is counted once per ABN, so a rename under the same ABN is one
  contract; ``shared`` counts contracts that name more than one ABN (a joint
  award, or a rename that also changed the ABN — the data cannot say which).
* **Buyers are only in ``parties.roles``** (``procuringEntity``) — read there.
* **The supplier's ``contactPoint`` is the buyer's officer**, not the
  supplier's: never read.

Dates: ``contracts/dateSigned`` equals the award date on every record and
carries AusTender's publication time (business hours in Canberra), so it is
the notice's publish date, converted to the Canberra day. Period dates are
local midnights stored in UTC (13:00Z / 14:00Z), converted the same way.

The link is the notice's AusTender page, ``/Cn/Show/<uuid>``, the uuid being
the 32-hex suffix of ``awardID`` (checked against nine notice pages a web
search returned on 12 Nov 2026). The site refuses datacentre requests, so it
could not be fetched from here.

Index, not live API
-------------------
The registry file is 262 MB gzipped; reading it is ~30 s of CPU, and the
API cannot be searched by supplier. The index is built monthly by
``.github/workflows/refresh-austender-index.yml``
(``scripts/build_austender_index.py``), published as the
``austender-index`` release asset and downloaded at boot (``warm_index``).
Without it the source is not announced (``covers_lei``). Per supplier it
keeps totals, its five largest buyers and the ``LATEST_KEPT`` latest plus
``LARGEST_KEPT`` largest contracts — not every contract.

Licence: CC BY 3.0 AU (the registry's statement of AusTender's licence).
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
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.austender import AusTenderBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities, the ABN and the ACN
# ---------------------------------------------------------------------------

ABN_RA_CODE = "RA000013"  # Australian Business Register → ABN
ACN_RA_CODE = "RA000014"  # ASIC Register of Companies → ACN
AU_RA_CODES: frozenset[str] = frozenset({ABN_RA_CODE, ACN_RA_CODE})

_ABN_WEIGHTS = (10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19)


def _digits(raw: Any) -> str:
    return re.sub(r"\D", "", str(raw or ""))


def abn_valid(abn: str) -> bool:
    """The ATO's ABN check: subtract 1 from the first digit, weight, mod 89."""
    if not re.fullmatch(r"\d{11}", abn or "") or abn[0] == "0":
        return False
    digits = [int(c) for c in abn]
    digits[0] -= 1
    return sum(d * w for d, w in zip(digits, _ABN_WEIGHTS)) % 89 == 0


def abns_for_acn(acn: str) -> list[str]:
    """Every valid ABN made of two check digits and this ACN.

    A company's ABN is its ACN prefixed with two digits chosen so the ABN
    check passes. The prefix contributes ``prefix − 10`` to the weighted sum,
    so one prefix in 10–98 always works and 99 also works when 10 does.
    """
    acn = _digits(acn)
    if len(acn) != 9:
        return []
    return [f"{p}{acn}" for p in range(10, 100) if abn_valid(f"{p}{acn}")]


def candidate_abns(registered_at: str, registered_as: str) -> list[str]:
    """The ABNs GLEIF's (registeredAt, registeredAs) pair can name."""
    value = _digits(registered_as)
    if registered_at == ABN_RA_CODE:
        return [value] if abn_valid(value) else []
    if registered_at == ACN_RA_CODE:
        return abns_for_acn(value)
    return []


def format_abn(abn: str) -> str:
    """``33090555052`` → ``33 090 555 052``, the ABR's printed form."""
    return f"{abn[:2]} {abn[2:5]} {abn[5:8]} {abn[8:]}" if len(abn) == 11 else abn


# ---------------------------------------------------------------------------
# Where the data is
# ---------------------------------------------------------------------------

REGISTRY_URL = "https://data.open-contracting.org/en/publication/19"
FULL_FILE_URL = "https://data.open-contracting.org/en/publication/19/download?name=full.jsonl.gz"
_HOMEPAGE = "https://www.tenders.gov.au/"
NOTICE_URL_TEMPLATE = "https://www.tenders.gov.au/Cn/Show/{uuid}"
LICENSE_ID = "CC-BY-3.0-AU"

#: Bumped whenever the index tables change shape.
INDEX_SCHEMA_VERSION = "1"

TOP_BUYERS = 5
LATEST_KEPT = 8
LARGEST_KEPT = 4
TITLE_CHARS = 120

_CANBERRA = ZoneInfo("Australia/Sydney")

#: ``procurementMethodDetails`` as filed → a one-letter code in the index.
_METHODS = {"Open tender": "O", "Limited tender": "L", "Prequalified tender": "P"}
METHOD_WORDS = {"O": "Open tender", "L": "Limited tender", "P": "Prequalified tender"}


def local_day(stamp: Any) -> str | None:
    """An OCDS timestamp → the calendar day in Canberra.

    AusTender stores a local midnight as UTC (``2020-01-06T13:00:00Z`` is
    7 January in Canberra) and a publication time as UTC too, so a ``[:10]``
    trim is wrong for both.
    """
    text = str(stamp or "").strip()
    if not text:
        return None
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", text) else None
    if moment.tzinfo is None:
        return moment.date().isoformat()
    return moment.astimezone(_CANBERRA).date().isoformat()


def _amount(value: Any) -> int | None:
    """``"1249027.65"`` → whole dollars, half up. Unreadable → None."""
    try:
        return int(Decimal(str(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError):
        return None


def notice_uuid(award_id: str) -> str | None:
    """The notice page's uuid from ``awardID`` (``CN3827867-c8309686aa5a…``)."""
    tail = str(award_id or "").rsplit("-", 1)[-1].lower()
    if not re.fullmatch(r"[0-9a-f]{32}", tail):
        return None
    return f"{tail[:8]}-{tail[8:12]}-{tail[12:16]}-{tail[16:20]}-{tail[20:]}"


def notice_url(uuid: str | None) -> str:
    return NOTICE_URL_TEMPLATE.format(uuid=uuid) if uuid else _HOMEPAGE


# ---------------------------------------------------------------------------
# Index build
# ---------------------------------------------------------------------------


class IndexBuildError(RuntimeError):
    """The file is not what the index builder understands."""


@dataclass
class _Supplier:
    name: str = ""
    name_date: str = ""
    contracts: int = 0
    value: int = 0
    first: str = "9999-99-99"
    last: str = "0000-00-00"
    marked_cancelled: int = 0
    amended: int = 0
    shared: int = 0
    # buyer id → [contracts, value]
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
            "releases", "contracts", "supplier_parties", "supplier_parties_with_abn",
            "supplier_parties_invalid_abn", "contracts_without_abn",
            "contracts_marked_cancelled", "contracts_shared",
        )
    }
    suppliers: dict[str, _Supplier] = {}
    buyers: dict[tuple[str, str], int] = {}
    buyer_rows: dict[int, tuple[str, str]] = {}
    reasons: dict[str, int] = {}
    data_to = ""

    def buyer_id(name: str, abn: str) -> int:
        key = (abn, name if not abn else "")
        if key not in buyers:
            buyers[key] = len(buyers) + 1
        bid = buyers[key]
        if name:
            buyer_rows[bid] = (name, abn)
        else:
            buyer_rows.setdefault(bid, ("", abn))
        return bid

    for release in releases:
        counts["releases"] += 1
        parties = release.get("parties") or []
        buyer = None
        sup_abns: dict[str, str] = {}
        for party in parties:
            roles = party.get("roles") or []
            abns = [
                _digits(i.get("id"))
                for i in party.get("additionalIdentifiers") or []
                if i.get("scheme") == "AU-ABN"
            ]
            if "procuringEntity" in roles:
                buyer = buyer_id(str(party.get("name") or "").strip(), abns[0] if abns else "")
            if "supplier" in roles:
                counts["supplier_parties"] += 1
                if not abns:
                    continue
                if not abn_valid(abns[0]):
                    counts["supplier_parties_invalid_abn"] += 1
                    continue
                counts["supplier_parties_with_abn"] += 1
                sup_abns.setdefault(abns[0], str(party.get("name") or "").strip())
        tender = release.get("tender") or {}
        method = _METHODS.get(str(tender.get("procurementMethodDetails") or ""), "")
        reason_text = str(tender.get("limitedTenderReason") or "").strip()
        reason = reasons.setdefault(reason_text[:200], len(reasons) + 1) if reason_text else None
        for contract in release.get("contracts") or []:
            counts["contracts"] += 1
            if not sup_abns:
                counts["contracts_without_abn"] += 1
                continue
            cn = str(contract.get("id") or "").strip()
            day = local_day(contract.get("dateSigned"))
            if day and day > data_to:
                data_to = day
            value = _amount((contract.get("value") or {}).get("amount"))
            period = contract.get("period") or {}
            cancelled = str(contract.get("status") or "") == "cancelled"
            amendments = len(contract.get("amendments") or [])
            title = str(contract.get("description") or contract.get("title") or "").strip()
            shared = len(sup_abns) > 1
            counts["contracts_marked_cancelled"] += cancelled
            counts["contracts_shared"] += shared
            record = (
                notice_uuid(str(contract.get("awardID") or "")),
                buyer, title[:TITLE_CHARS], value,
                local_day(period.get("startDate")), local_day(period.get("endDate")),
                method, reason, amendments, int(cancelled), len(sup_abns) - 1,
            )
            for abn, name in sup_abns.items():
                sup = suppliers.setdefault(abn, _Supplier())
                sup.contracts += 1
                sup.value += value or 0
                sup.marked_cancelled += cancelled
                sup.amended += amendments > 0
                sup.shared += shared
                if day:
                    sup.first = min(sup.first, day)
                    sup.last = max(sup.last, day)
                    if name and day >= sup.name_date:
                        sup.name, sup.name_date = name, day
                elif name and not sup.name:
                    sup.name = name
                if buyer is not None:
                    tally = sup.buyers.setdefault(buyer, [0, 0])
                    tally[0] += 1
                    tally[1] += value or 0
                _keep(sup.latest, (day or "", cn, (day, *record)), LATEST_KEPT)
                _keep(sup.largest, (value or 0, cn, (day, *record)), LARGEST_KEPT)

    if counts["supplier_parties_with_abn"] < 1:
        raise IndexBuildError("no supplier carries an AU-ABN — not the AusTender file shape")

    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(str(tmp))
    conn.executescript(_SCHEMA)
    buyers_used: set[int] = set()
    for abn, sup in suppliers.items():
        conn.execute(
            "INSERT INTO supplier VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                int(abn), sup.name, sup.contracts, sup.value, len(sup.buyers),
                sup.first if sup.first != "9999-99-99" else None,
                sup.last if sup.last != "0000-00-00" else None,
                sup.marked_cancelled, sup.amended, sup.shared,
            ),
        )
        ranked = sorted(sup.buyers.items(), key=lambda kv: (-kv[1][1], -kv[1][0], kv[0]))
        for bid, (n, value) in ranked[:TOP_BUYERS]:
            buyers_used.add(bid)
            conn.execute("INSERT INTO supplier_buyer VALUES (?,?,?,?)", (int(abn), bid, n, value))
        kept: dict[str, tuple[Any, ...]] = {}
        for _key, cn, row in (*sup.latest, *sup.largest):
            kept[cn] = row
        for cn, row in kept.items():
            day, uuid, bid, title, value, start, end, method, reason, amendments, cancelled, others = row
            if bid is not None:
                buyers_used.add(bid)
            conn.execute(
                "INSERT INTO contract VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    int(abn), cn, uuid, day, bid, title, value, start, end,
                    method or None, reason, amendments, cancelled, others,
                ),
            )
    conn.executemany(
        "INSERT INTO buyer VALUES (?,?,?)",
        [(b, *buyer_rows.get(b, ("", ""))) for b in sorted(buyers_used)],
    )
    conn.executemany(
        "INSERT INTO tender_reason VALUES (?,?)", [(i, t) for t, i in sorted(reasons.items())]
    )
    stored = conn.execute("SELECT COUNT(*) FROM supplier").fetchone()[0]
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "build_seconds": f"{time.monotonic() - started:.1f}",
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
    logger.info("austender: index built at %s: %s suppliers", out_path, stored)
    return meta


_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE supplier (
    abn INTEGER PRIMARY KEY,
    name TEXT,
    contracts INTEGER NOT NULL,
    value_aud INTEGER NOT NULL,
    buyers INTEGER NOT NULL,
    first_date TEXT,
    last_date TEXT,
    marked_cancelled INTEGER NOT NULL,
    amended INTEGER NOT NULL,
    shared INTEGER NOT NULL
);
CREATE TABLE buyer (id INTEGER PRIMARY KEY, name TEXT, abn TEXT);
CREATE TABLE supplier_buyer (
    abn INTEGER NOT NULL,
    buyer INTEGER NOT NULL,
    contracts INTEGER NOT NULL,
    value_aud INTEGER NOT NULL,
    PRIMARY KEY (abn, buyer)
) WITHOUT ROWID;
CREATE TABLE contract (
    abn INTEGER NOT NULL,
    cn TEXT NOT NULL,
    uuid TEXT,
    published TEXT,
    buyer INTEGER,
    title TEXT,
    value_aud INTEGER,
    start_date TEXT,
    end_date TEXT,
    method TEXT,
    reason INTEGER,
    amendments INTEGER NOT NULL,
    marked_cancelled INTEGER NOT NULL,
    other_suppliers INTEGER NOT NULL,
    PRIMARY KEY (abn, cn)
) WITHOUT ROWID;
CREATE TABLE tender_reason (id INTEGER PRIMARY KEY, text TEXT);
"""


# ---------------------------------------------------------------------------
# Where the index lives, and getting it onto the host
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``AUSTENDER_DB_FILE`` or the data-root default."""
    from ..cache import data_root

    configured = get_settings().austender_db_file
    return Path(configured) if configured else data_root() / "austender.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().austender_db_file:
            logger.warning("austender: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("austender: index at %s has an unexpected schema; ignoring", path)
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
    """The registry's retrieval day is the cut; the build is our clock."""
    meta = read_meta() if meta is None else meta
    cut = provenance.parse_moment(meta.get("source_last_modified")) or _moment(meta.get("data_to"))
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut,
        detail="AusTender contract notices (OCDS), via the OCP Data Registry",
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
    url = settings.austender_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"austender": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"austender": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"austender": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("austender: asset check/download failed: %s", exc)
        return {"austender": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# Reading one supplier
# ---------------------------------------------------------------------------


def supplier_record(abn: str) -> dict[str, Any] | None:
    """Everything the index holds for one ABN, or None."""
    conn = _shared_conn()
    if conn is None:
        return None
    key = int(abn)
    row = conn.execute("SELECT * FROM supplier WHERE abn = ?", (key,)).fetchone()
    if row is None:
        return None
    supplier = dict(row)
    supplier["abn"] = abn
    contract_rows = conn.execute(
        "SELECT c.*, r.text AS reason_text FROM contract c"
        " LEFT JOIN tender_reason r ON r.id = c.reason WHERE c.abn = ?",
        (key,),
    ).fetchall()
    buyer_rows = conn.execute(
        "SELECT * FROM supplier_buyer WHERE abn = ? ORDER BY value_aud DESC, contracts DESC",
        (key,),
    ).fetchall()
    wanted = {r["buyer"] for r in (*contract_rows, *buyer_rows) if r["buyer"] is not None}
    names: dict[int, str] = {}
    if wanted:
        marks = ",".join("?" * len(wanted))
        names = {
            r["id"]: r["name"]
            for r in conn.execute(f"SELECT id, name FROM buyer WHERE id IN ({marks})", list(wanted))
        }
    buyers = [
        {
            "name": names.get(r["buyer"]) or "",
            "contracts": r["contracts"],
            "value_aud": r["value_aud"],
        }
        for r in buyer_rows
    ]
    contracts = [
        {
            "cn": r["cn"],
            "published": r["published"],
            "buyer": names.get(r["buyer"]) or "",
            "title": r["title"] or "",
            "value_aud": r["value_aud"],
            "start_date": r["start_date"],
            "end_date": r["end_date"],
            "procedure": METHOD_WORDS.get(r["method"] or "", ""),
            "limited_tender_reason": r["reason_text"] or "",
            "amendments": r["amendments"],
            "marked_cancelled": bool(r["marked_cancelled"]),
            "other_suppliers": r["other_suppliers"],
            "url": notice_url(r["uuid"]),
        }
        for r in contract_rows
    ]
    contracts.sort(key=lambda c: (c["published"] or "", c["cn"]), reverse=True)
    return {"supplier": supplier, "buyers": buyers, "contracts": contracts}


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class AusTenderAdapter(SourceAdapter):
    """AusTender contract notices, via the ABN (or the ABN an ACN implies)."""

    id = "austender"

    # No ``lookup_derivers``: procurement is reached the way TED and
    # ChileCompra are, from ``_dispatch`` via ``fetch_by_identifiers``.
    # ``abr_australia`` owns the Australian RA codes as a register.

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="AusTender — Australian Government contracts",
            homepage=_HOMEPAGE,
            description=(
                "Contract notices the Australian Government publishes on "
                "AusTender (contracts of AU$10,000 or more), from the Open "
                "Contracting Partnership's Data Registry copy, rebuilt monthly. "
                "For a supplier: its latest and largest contracts with agency, "
                "value, period, procedure and a link to each notice, plus "
                "totals since 2013. Matched on the ABN, or the ABN a company's "
                "ACN implies. Contracts won, not ownership."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains AusTender contract notice data © Commonwealth of "
                "Australia (Department of Finance), licensed under CC BY 3.0 AU, "
                "via the Open Contracting Partnership's Data Registry."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=False,
            # None, like every non-register source: knowability rows are about
            # what a register publishes on owners, not procurement.
            country=None,
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file (the ChileCompra rule)."""
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Not name-searchable: reached from the ABN or ACN GLEIF files."""
        return []

    def _bundle(self, abn: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "abn": abn,
            "abn_display": format_abn(abn) if abn else "",
            "matched_via": "",
            "legal_name": legal_name or "",
            "supplier": None,
            "buyers": [],
            "contracts": [],
            "data_to": None,
            "link": REGISTRY_URL,
            "not_found": False,
            "ambiguous": False,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "", matched_via: str = "abn") -> dict[str, Any]:
        """What AusTender's notices say about one ABN."""
        abn = _digits(hit_id)
        if not abn_valid(abn):
            return self._bundle("", legal_name)
        if _shared_conn() is None:
            return self._bundle(abn, legal_name)
        meta = read_meta()
        declare_snapshot(meta)
        extra = {"data_to": meta.get("data_to") or None, "matched_via": matched_via}
        found = supplier_record(abn)
        if found is None:
            return self._bundle(abn, legal_name, not_found=True, is_stub=False, **extra)
        bundle = self._bundle(abn, legal_name, is_stub=False, **extra, **found)
        validate_raw(self.id, AusTenderBundle, bundle)
        return bundle

    @staticmethod
    def applies_to(registered_at: str, registered_as: str) -> bool:
        """An Australian ABN or ACN that yields at least one valid ABN.

        Asked before dispatch, so a company outside Australia is never
        announced as checked against AusTender.
        """
        return bool(candidate_abns(registered_at, registered_as))

    async def fetch_by_identifiers(
        self,
        lei: str,
        registered_as: str,
        registered_at: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any] | None:
        """The pipeline's entry point (the TED / ChileCompra shape)."""
        candidates = candidate_abns(registered_at, registered_as)
        if not candidates:
            return None
        via = "abn" if registered_at == ABN_RA_CODE else "acn"
        if len(candidates) == 1:
            return await self.fetch(candidates[0], legal_name=legal_name, matched_via=via)
        # An ACN with two valid prefixes: look up both; refuse two suppliers.
        found = [a for a in candidates if _shared_conn() is not None and supplier_record(a)]
        if len(found) == 1:
            return await self.fetch(found[0], legal_name=legal_name, matched_via=via)
        if len(found) > 1:
            meta = read_meta()
            declare_snapshot(meta)
            return self._bundle(
                "", legal_name, ambiguous=True, is_stub=False,
                data_to=meta.get("data_to") or None, matched_via=via,
            )
        return await self.fetch(candidates[0], legal_name=legal_name, matched_via=via)
