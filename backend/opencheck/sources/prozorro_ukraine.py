"""Ukraine — Prozorro, the public procurement system.

Every Ukrainian public buyer publishes its procurements on Prozorro (Law of
Ukraine "On Public Procurement"), including the purchases it makes without
the electronic system, which it must still report there. The central
database answers keyless at ``public.api.openprocurement.org/api/2.5``. This
adapter answers one question from it: **which public contracts has this
company signed, with which buyers** — contracts signed, never ownership.

Index, not live API
-------------------
The API has no supplier filter: the supplier is only on the full contract
(see ``opencheck/prozorro_harvest.py``). So the index is built daily by
``.github/workflows/refresh-prozorro-index.yml`` from OpenCheck's own harvest
of the contracts feed (``scripts/harvest_prozorro.py``, kept between runs on
the ``prozorro-harvest`` release), published as the ``prozorro-index`` release
asset and downloaded at boot (``warm_index``). Without it the source is not
announced (``covers_lei``).

What is counted (Stephen's decisions A–D, 23 Nov 2026)
------------------------------------------------------
* The harvest reaches back twelve months from its first run and reads every
  contract *changed* since — so an older contract amended in that time is
  in it, and one untouched since is not. The card states the period by the
  harvest's start (``harvest_started_from``), never as "all contracts".
* ``active`` and ``terminated`` contracts are counted, terminated ones
  labelled; ``pending`` (not yet signed) and ``cancelled`` are left out.
  Prozorro gives a contract ``terminated`` when it ends, whether it was
  carried out or broken off (a completed contract carries ``amountPaid``
  and an empty ``terminationDetails``), so the card says "ended".
* A signing day before ``FIRST_SIGNING_DAY`` or after the build is not a
  real date (one contract in the harvest says 2004-03-16, three more 2004
  to 2012): the contract is counted with no date, and the day is left out
  of every period (``signed_date_implausible`` in ``meta``).
* Only a legal person's eight-digit EDRPOU is ever stored. A ten-digit
  РНОКПП — a natural person, sole traders included — is dropped by the
  harvester and is never in the store or the index.
* All values measured were in hryvnias (UAH). Values in another currency are
  counted and never converted or added.

The join key is the EDRPOU, which GLEIF files under three authorities
(``edr_ukraine.UA_RA_CODES``); a register-number lookup on ``UA-EDR`` reaches
the index with the number itself.

Never read: a party's address or contact point, the contract's
``signerInfo`` (a named signatory), items, documents and changes.

Dates are Kyiv calendar days. The link is the contract's page,
``https://prozorro.gov.ua/contract/<contractID>`` (checked in a browser on
23 Nov 2026).

Licence: CC BY 4.0 (Prozorro's open data terms, Stephen 23 Nov 2026).
"""

from __future__ import annotations

import heapq
import json
import logging
import os
import sqlite3
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .. import provenance
from ..config import get_settings
from ..prozorro_harvest import hex_id, offset_moment
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .edr_ukraine import UA_RA_CODES
from .schemas import validate_raw
from .schemas.prozorro_ukraine import ProzorroBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Where the data is
# ---------------------------------------------------------------------------

API_DOCS_URL = "https://prozorro-api-docs.readthedocs.io/en/master/"
_HOMEPAGE = "https://prozorro.gov.ua/"
CONTRACT_URL_TEMPLATE = "https://prozorro.gov.ua/contract/{ref}"
LICENSE_ID = "CC-BY-4.0"

#: Bumped whenever the index tables change shape.
INDEX_SCHEMA_VERSION = "1"

TOP_BUYERS = 5
LATEST_KEPT = 6
LARGEST_KEPT = 3

#: Decision C: the statuses counted. ``pending`` and ``cancelled`` are not.
COUNTED_STATUSES = frozenset({"active", "terminated"})
#: The earliest signing day taken as real: Prozorro opened in February 2015.
FIRST_SIGNING_DAY = "2015-01-01"


def plausible_day(day: str | None, latest: str) -> str | None:
    """A signing day, or None when it is before ``FIRST_SIGNING_DAY``, after
    *latest* (the build's day), or not a ``YYYY-MM-DD`` day at all."""
    text = str(day or "")
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        return None
    return text if FIRST_SIGNING_DAY <= text <= latest else None

#: ``procurementMethodType`` (on the tender) → a one-letter code.
_METHODS: dict[str, str] = {
    "reporting": "R",
    "belowThreshold": "S",
    "priceQuotation": "Q",
    "negotiation": "N",
    "negotiation.quick": "N",
    "closeFrameworkAgreementUA": "F",
    "closeFrameworkAgreementSelectionUA": "F",
    "aboveThreshold": "O",
    "aboveThresholdUA": "O",
    "aboveThresholdEU": "O",
    "aboveThresholdUA.defense": "O",
    "simple.defense": "O",
    "competitiveDialogueUA": "O",
    "competitiveDialogueEU": "O",
    "competitiveDialogueUA.stage2": "O",
    "competitiveDialogueEU.stage2": "O",
    "competitiveOrdering": "O",
    "esco": "O",
    "requestForProposal": "P",
}
METHOD_WORDS: dict[str, str] = {
    "R": "Purchase without the electronic system, reported (direct contract)",
    "S": "Simplified procurement",
    "Q": "Price quotation (electronic catalogue)",
    "O": "Open or competitive tender",
    "P": "Request for proposals",
    "N": "Negotiated procedure",
    "F": "Framework agreement",
    "X": "Other procedure",
    "U": "Procedure not read",
}


def method_code(method: str | None) -> str:
    """A tender's ``procurementMethodType`` → its code; none read → ``U``."""
    if not method:
        return "U"
    return _METHODS.get(method, "X")


def contract_url(ref: str | None) -> str:
    return CONTRACT_URL_TEMPLATE.format(ref=ref) if ref else _HOMEPAGE


def candidate_edrpou(registered_at: str, registered_as: str) -> str | None:
    """The EDRPOU GLEIF's (registeredAt, registeredAs) pair names, or None.

    Only eight digits (five to seven padded): a ten-digit number is a
    person's.
    """
    if registered_at not in UA_RA_CODES:
        return None
    return edrpou_of(registered_as)


def edrpou_of(raw: Any) -> str | None:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if 5 <= len(digits) <= 7:
        digits = digits.zfill(8)
    return digits if len(digits) == 8 else None


# ---------------------------------------------------------------------------
# Index build
# ---------------------------------------------------------------------------


class IndexBuildError(RuntimeError):
    """The harvest is not what the index builder understands."""


@dataclass
class _Supplier:
    name: str = ""
    name_date: str = ""
    contracts: int = 0
    terminated: int = 0
    value_uah: int = 0
    other_currency: int = 0
    first: str = "9999-99-99"
    last: str = "0000-00-00"
    methods: dict[str, int] = field(default_factory=dict)
    buyers: dict[int, list[int]] = field(default_factory=dict)
    latest: list[tuple[str, str, tuple[Any, ...]]] = field(default_factory=list)
    largest: list[tuple[int, str, tuple[Any, ...]]] = field(default_factory=list)


def _keep(heap: list[Any], entry: Any, size: int) -> None:
    if len(heap) < size:
        heapq.heappush(heap, entry)
    elif entry > heap[0]:
        heapq.heapreplace(heap, entry)


_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE supplier (
    edrpou INTEGER PRIMARY KEY,
    name TEXT,
    contracts INTEGER NOT NULL,
    terminated INTEGER NOT NULL,
    value_uah INTEGER NOT NULL,
    other_currency INTEGER NOT NULL,
    buyers INTEGER NOT NULL,
    first_date TEXT,
    last_date TEXT,
    methods TEXT NOT NULL
);
CREATE TABLE buyer (id INTEGER PRIMARY KEY, name TEXT, edrpou TEXT);
CREATE TABLE supplier_buyer (
    edrpou INTEGER NOT NULL,
    buyer INTEGER NOT NULL,
    contracts INTEGER NOT NULL,
    value_uah INTEGER NOT NULL,
    PRIMARY KEY (edrpou, buyer)
) WITHOUT ROWID;
CREATE TABLE contract (
    edrpou INTEGER NOT NULL,
    id TEXT NOT NULL,
    ref TEXT,
    signed TEXT,
    buyer INTEGER,
    title TEXT,
    value INTEGER,
    currency TEXT,
    method TEXT,
    category TEXT,
    status TEXT,
    PRIMARY KEY (edrpou, id)
) WITHOUT ROWID;
"""

_HARVEST_ROWS = """
SELECT c.edrpou, c.id, c.supplier_name, c.status, c.signed, c.value, c.currency,
       c.buyer, c.title, c.contract_ref, c.category, t.method
FROM contract c LEFT JOIN tender_method t ON t.id = c.tender_id
ORDER BY c.edrpou
"""


def _harvest_rows(harvest: sqlite3.Connection) -> Iterator[tuple[Any, ...]]:
    yield from harvest.execute(_HARVEST_ROWS)


def build_index(
    harvest_path: Path | str,
    out_path: Path | str,
    *,
    source_meta: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the supplier index from a harvest store.

    The store is read in EDRPOU order, one supplier at a time, so memory stays
    flat however large the harvest. Written beside ``out_path`` and renamed
    into place, so a reader never sees a half-built file. Returns ``meta``.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    harvest = sqlite3.connect(f"file:{harvest_path}?mode=ro", uri=True)
    try:
        state = {str(k): str(v) for k, v in harvest.execute("SELECT key, value FROM state")}
        harvest_buyers = {
            int(i): (str(e or ""), str(n or ""))
            for i, e, n in harvest.execute("SELECT id, edrpou, name FROM buyer")
        }
    except sqlite3.Error as exc:
        harvest.close()
        raise IndexBuildError(f"not a Prozorro harvest store: {type(exc).__name__}") from exc

    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(str(tmp))
    conn.executescript(_SCHEMA)
    counts = {
        k: 0
        for k in ("contracts_in_harvest", "contracts_counted", "contracts_pending",
                  "contracts_cancelled", "contracts_other_status", "method_not_read",
                  "signed_date_implausible")
    }
    buyers_used: set[int] = set()
    data_from, data_to = "9999-99-99", ""
    suppliers = 0
    latest_day = (datetime.now(UTC) + timedelta(days=1)).date().isoformat()

    def flush(edrpou: int, sup: _Supplier) -> None:
        nonlocal suppliers
        if sup.contracts == 0:
            return
        suppliers += 1
        conn.execute(
            "INSERT INTO supplier VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                edrpou, sup.name, sup.contracts, sup.terminated, sup.value_uah,
                sup.other_currency, len(sup.buyers),
                sup.first if sup.first != "9999-99-99" else None,
                sup.last if sup.last != "0000-00-00" else None,
                json.dumps(sup.methods, sort_keys=True),
            ),
        )
        ranked = sorted(sup.buyers.items(), key=lambda kv: (-kv[1][1], -kv[1][0], kv[0]))
        for bid, (n, value) in ranked[:TOP_BUYERS]:
            buyers_used.add(bid)
            conn.execute("INSERT INTO supplier_buyer VALUES (?,?,?,?)", (edrpou, bid, n, value))
        kept: dict[str, tuple[Any, ...]] = {}
        for _rank, cid, row in (*sup.latest, *sup.largest):
            kept[cid] = row
        for cid, row in kept.items():
            buyers_used.add(row[2])
            conn.execute("INSERT INTO contract VALUES (?,?,?,?,?,?,?,?,?,?,?)", (edrpou, cid, *row))

    current: int | None = None
    sup = _Supplier()
    for (edrpou, cid_key, name, status, signed, value, currency, buyer, title,
         ref, category, method) in _harvest_rows(harvest):
        counts["contracts_in_harvest"] += 1
        if edrpou != current:
            if current is not None:
                flush(current, sup)
            current, sup = int(edrpou), _Supplier()
        if status not in COUNTED_STATUSES:
            key = {"pending": "contracts_pending", "cancelled": "contracts_cancelled"}
            counts[key.get(str(status), "contracts_other_status")] += 1
            continue
        counts["contracts_counted"] += 1
        cid = hex_id(cid_key)
        code = method_code(method)
        if code == "U":
            counts["method_not_read"] += 1
        sup.contracts += 1
        if status == "terminated":
            sup.terminated += 1
        uah = currency in (None, "", "UAH")
        if uah:
            sup.value_uah += int(value or 0)
        else:
            sup.other_currency += 1
        sup.methods[code] = sup.methods.get(code, 0) + 1
        day = plausible_day(signed, latest_day) or ""
        if signed and not day:
            counts["signed_date_implausible"] += 1
        if day:
            data_from, data_to = min(data_from, day), max(data_to, day)
            sup.first, sup.last = min(sup.first, day), max(sup.last, day)
            if name and day >= sup.name_date:
                sup.name, sup.name_date = str(name), day
        elif name and not sup.name:
            sup.name = str(name)
        tally = sup.buyers.setdefault(int(buyer or 0), [0, 0])
        tally[0] += 1
        tally[1] += int(value or 0) if uah else 0
        record = (ref or "", day or None, int(buyer or 0), title or "",
                  value, currency or "UAH", code, category or "", status)
        _keep(sup.latest, (day, cid, record), LATEST_KEPT)
        if uah and value is not None:
            _keep(sup.largest, (int(value), cid, record), LARGEST_KEPT)
    if current is not None:
        flush(current, sup)
    harvest.close()

    if counts["contracts_counted"] < 1:
        conn.close()
        tmp.unlink()
        raise IndexBuildError("no active or terminated contract in the harvest")

    conn.executemany(
        "INSERT INTO buyer VALUES (?,?,?)",
        [(b, harvest_buyers.get(b, ("", ""))[1], harvest_buyers.get(b, ("", ""))[0])
         for b in sorted(buyers_used)],
    )
    cursor_at = offset_moment(state.get("contracts_cursor"))
    started_from = offset_moment(state.get("started_from"))
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "build_seconds": f"{time.monotonic() - started:.1f}",
        "data_from": data_from if data_from != "9999-99-99" else "",
        "data_to": data_to,
        "suppliers": str(suppliers),
        "harvest_started_from": started_from.isoformat(timespec="seconds") if started_from else "",
        "harvest_cursor": cursor_at.isoformat(timespec="seconds") if cursor_at else "",
        "harvest_caught_up_at": state.get("caught_up_at", ""),
        **{k: str(v) for k, v in counts.items()},
        **(source_meta or {}),
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp, out_path)
    logger.info("prozorro_ukraine: index built at %s: %s suppliers", out_path, suppliers)
    return meta


# ---------------------------------------------------------------------------
# Where the index lives, and getting it onto the host
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``PROZORRO_DB_FILE`` or the data-root default."""
    from ..cache import data_root

    configured = get_settings().prozorro_db_file
    return Path(configured) if configured else data_root() / "prozorro_ukraine.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().prozorro_db_file:
            logger.warning("prozorro_ukraine: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("prozorro_ukraine: index at %s has an unexpected schema; ignoring", path)
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
    """The cut is how far the harvest has read Prozorro's contracts feed
    (every contract changed before that moment was read); the build is our
    clock."""
    meta = read_meta() if meta is None else meta
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=provenance.parse_moment(meta.get("harvest_cursor")),
        detail="Prozorro public procurement contracts, from Prozorro's API",
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
    url = settings.prozorro_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"prozorro_ukraine": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"prozorro_ukraine": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"prozorro_ukraine": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("prozorro_ukraine: asset check/download failed: %s", exc)
        return {"prozorro_ukraine": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# Reading one supplier
# ---------------------------------------------------------------------------


def supplier_record(edrpou: str) -> dict[str, Any] | None:
    """Everything the index holds for one EDRPOU, or None."""
    conn = _shared_conn()
    if conn is None:
        return None
    key = int(edrpou)
    row = conn.execute("SELECT * FROM supplier WHERE edrpou = ?", (key,)).fetchone()
    if row is None:
        return None
    raw = dict(row)
    try:
        codes = {str(k): int(v) for k, v in json.loads(raw["methods"] or "{}").items()}
    except (ValueError, TypeError):
        codes = {}
    order = list(METHOD_WORDS)
    supplier = {
        "edrpou": edrpou,
        "name": raw["name"],
        "contracts": raw["contracts"],
        "terminated": raw["terminated"],
        "value_uah": raw["value_uah"],
        "other_currency": raw["other_currency"],
        "buyers": raw["buyers"],
        "first_date": raw["first_date"],
        "last_date": raw["last_date"],
        "methods": {
            METHOD_WORDS.get(code, METHOD_WORDS["X"]): n
            for code, n in sorted(codes.items(), key=lambda kv: (-kv[1], order.index(kv[0]) if kv[0] in order else 99))
            if n
        },
    }
    contract_rows = conn.execute("SELECT * FROM contract WHERE edrpou = ?", (key,)).fetchall()
    buyer_rows = conn.execute(
        "SELECT * FROM supplier_buyer WHERE edrpou = ? ORDER BY value_uah DESC, contracts DESC",
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
        {"name": names.get(r["buyer"]) or "", "contracts": r["contracts"], "value_uah": r["value_uah"]}
        for r in buyer_rows
    ]
    contracts = [
        {
            "id": r["id"],
            "ref": r["ref"] or "",
            "signed": r["signed"],
            "buyer": names.get(r["buyer"]) or "",
            "title": r["title"] or "",
            "value": r["value"],
            "currency": r["currency"] or "UAH",
            "procedure": METHOD_WORDS.get(r["method"] or "U", METHOD_WORDS["X"]),
            "category": r["category"] or "",
            "status": r["status"] or "",
            "url": contract_url(r["ref"]),
        }
        for r in contract_rows
    ]
    contracts.sort(key=lambda c: (c["signed"] or "", c["id"]), reverse=True)
    return {"supplier": supplier, "buyers": buyers, "contracts": contracts}


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class ProzorroUkraineAdapter(SourceAdapter):
    """Prozorro public contracts, via the EDRPOU."""

    id = "prozorro_ukraine"

    # No ``lookup_derivers``: procurement is reached the way TED, ChileCompra
    # and MTender are, from ``_dispatch`` via ``fetch_by_identifiers``.
    # ``edr_ukraine`` owns the Ukrainian RA codes as a register.

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Prozorro — Ukrainian public procurement",
            homepage=_HOMEPAGE,
            description=(
                "Public contracts Ukraine publishes on Prozorro, read daily from "
                "Prozorro's API by OpenCheck. For a supplier: its latest and "
                "largest contracts with buyer, value, procedure and a link to "
                "each contract on Prozorro, plus totals over the active and "
                "terminated contracts changed since the harvest began, a "
                "period the card states. Contracts with natural persons are "
                "not read. Matched on the EDRPOU. Contracts signed, not ownership."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains public procurement data from Prozorro, the "
                "electronic public procurement system of Ukraine, licensed "
                "under CC BY 4.0."
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
        """Not name-searchable: reached from the EDRPOU GLEIF files."""
        return []

    def _bundle(self, edrpou: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "edrpou": edrpou,
            "legal_name": legal_name or "",
            "supplier": None,
            "buyers": [],
            "contracts": [],
            "data_from": None,
            "data_to": None,
            "harvest_started_from": None,
            "link": API_DOCS_URL,
            "not_found": False,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """What Prozorro's contracts say about one EDRPOU."""
        edrpou = edrpou_of(hit_id)
        if edrpou is None:
            return self._bundle("", legal_name)
        if _shared_conn() is None:
            return self._bundle(edrpou, legal_name)
        meta = read_meta()
        declare_snapshot(meta)
        extra = {
            "data_from": meta.get("data_from") or None,
            "data_to": meta.get("data_to") or None,
            "harvest_started_from": (meta.get("harvest_started_from") or "")[:10] or None,
        }
        found = supplier_record(edrpou)
        if found is None:
            return self._bundle(edrpou, legal_name, not_found=True, is_stub=False, **extra)
        bundle = self._bundle(edrpou, legal_name, is_stub=False, **extra, **found)
        validate_raw(self.id, ProzorroBundle, bundle)
        return bundle

    @staticmethod
    def applies_to(registered_at: str, registered_as: str) -> bool:
        """A Ukrainian authority and an eight-digit EDRPOU.

        Asked before dispatch, so a company outside Ukraine is never announced
        as checked against Prozorro.
        """
        return candidate_edrpou(registered_at, registered_as) is not None

    async def fetch_by_identifiers(
        self,
        lei: str,
        registered_as: str,
        registered_at: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any] | None:
        """The pipeline's entry point (the TED / ChileCompra shape)."""
        edrpou = candidate_edrpou(registered_at, registered_as)
        if edrpou is None:
            return None
        return await self.fetch(edrpou, legal_name=legal_name)
