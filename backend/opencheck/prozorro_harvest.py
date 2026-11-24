"""OpenCheck's own harvest of Prozorro's contracts, kept between runs.

Why a harvest
-------------
Prozorro's central database API (``public.api.openprocurement.org/api/2.5``)
has no way to ask for a supplier's contracts. ``/contracts`` and ``/tenders``
are change feeds: each page lists up to 1,000 objects as ``id`` and
``dateModified``, ordered by when they last changed, and a parameter the API
does not know (``supplier=``) is ignored, not refused. ``opt_fields`` is a
fixed list per feed, and the supplier is on none of them, so the supplier is
only on the full contract, ``GET /contracts/{id}``. This module reads the
contracts feed forward from where the last run stopped, fetches each
contract, and keeps the few fields the supplier index needs.

The API (measured 23 Nov 2026)
------------------------------
* About 20,600 contracts change on a weekday (20 Nov 2026), roughly 15,000 of
  them new: around four million a year.
* One contract takes about 0.6 s; sixteen at a time read 15.5 a second with
  no 429. The documentation says an "aggressive" address is answered 429 and
  should back off and retry, which ``_get`` does.
* ``next_page.offset`` is the cursor (``{timestamp}.{skip}.{hash}``); a bare
  Unix timestamp is accepted as an offset too, which is how a first run
  starts ``FIRST_RUN_DAYS`` back. An empty ``data`` list is "caught up".
* The procedure type is on the tender, not the contract. ``/tenders`` takes
  ``opt_fields=procurementMethodType,contracts`` on the feed itself, so the
  procedure of every tender that has contracts is read a page at a time, with
  no per-tender request, and kept by tender id.
* A contract can change long after its tender last did — most often when it
  ends — so reading the tenders feed forward from the harvest's start missed
  the tender of 43% of the contracts after two runs (24 Nov 2026; the tenders
  had last changed a few days after signing, before the start).
  ``harvest_methods_back`` therefore also reads the feed backwards
  (``descending=1``) from the start to ``BACK_DAYS`` before it, and
  ``fill_missing_methods`` reads any tender still missing one at a time
  (``GET /tenders/{id}``, a whole tender: ~37 a second with sixteen workers
  on 24 Nov 2026; at most ``FILL_PER_RUN`` a run). The backward read keeps
  the procedures of tenders the backfill has yet to reach; the fill closes
  what is left now. A tender Prozorro no longer has is kept with an empty
  procedure, so it is not asked for again.

What is kept (Stephen's decisions A–D, 23 Nov 2026)
---------------------------------------------------
* A: a first run starts twelve months back (``FIRST_RUN_DAYS``) and spreads
  the backfill over runs by its time budget; after that, each run reads what
  changed since the last.
* B: **a natural person is never stored.** 46% of supplier rows in a 3,000-
  contract sample were a ten-digit РНОКПП — a person's tax number, a sole
  trader (ФОП) or a named individual — filed under scheme ``UA-EDR``. Only an
  eight-digit EDRPOU (a legal person) is kept; a contract whose supplier is
  anything else is not stored at all, and one read again with such a
  supplier is removed. Never a supplier's address, ``signerInfo`` (a named
  signatory) or the buyer's ``contactPoint``.
* A supplier the buyer withheld — the defence stand-in "Оборонний
  постачальник" under code 88888888 (5% of kept contracts on 23 Nov 2026) —
  is not a company and is not stored (``is_masked_supplier``).
* C: every status is kept here; the index counts ``active`` and
  ``terminated`` contracts and leaves ``pending`` and ``cancelled`` out.
* D: run daily by ``.github/workflows/refresh-prozorro-index.yml``.

The store is one SQLite file (``prozorro_harvest.sqlite``), gzipped as an
asset of the ``prozorro-harvest`` release between runs. The cursor moves past
a page only once every contract on it was read, so a run that stops — the
time budget, an outage — resumes where it stopped and loses nothing.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://public.api.openprocurement.org/api/2.5/"
STORE_NAME = "prozorro_harvest.sqlite"
#: Decision A: the first run reads the contracts changed in the last year.
FIRST_RUN_DAYS = 365
PAGE_LIMIT = 1000
WORKERS = 16
BACKOFF_S = (2, 5, 15, 30, 60, 120)
ATTEMPTS = len(BACKOFF_S) + 1
TIMEOUT_S = 60.0
TITLE_CHARS = 120
STORE_SCHEMA_VERSION = "1"
#: How far before the harvest's start the tenders feed is read backwards.
BACK_DAYS = 730
#: Tenders read one at a time on one run, once the backward read is done.
FILL_PER_RUN = 60000
_USER_AGENT = "OpenCheck (+https://github.com/StephenAbbott/opencheck)"
_KYIV = ZoneInfo("Europe/Kyiv")


class HarvestError(RuntimeError):
    """Prozorro answered something that is not its API's shape."""


# ---------------------------------------------------------------------------
# Identifiers and values
# ---------------------------------------------------------------------------


def supplier_edrpou(raw: Any) -> str | None:
    """An eight-digit EDRPOU from a supplier's ``identifier.id``, else None.

    Whitespace is removed and a code of five to seven digits is padded: the
    oldest EDRPOUs start with zeros (НАСК "ОРАНТА" is 00034186) and some
    buyers file them without, as ``edr_ukraine.normalise_edrpou`` pads. Ten
    digits is a natural person's РНОКПП and is never kept; any other shape
    is not read.
    """
    digits = re.sub(r"\s+", "", str(raw or ""))
    if not digits.isdigit():
        return None
    if 5 <= len(digits) <= 7:
        digits = digits.zfill(8)
    return digits if len(digits) == 8 else None


def is_person_code(raw: Any) -> bool:
    """Whether an identifier is a natural person's ten-digit РНОКПП."""
    digits = re.sub(r"\s+", "", str(raw or ""))
    return digits.isdigit() and len(digits) == 10


#: The stand-in a buyer files when the supplier is withheld: Prozorro shows
#: defence purchases under martial law with the supplier "Оборонний
#: постачальник" ("defence supplier") and code 88888888 — 61 of the first
#: 1,243 kept contracts read on 23 Nov 2026. Not a company; never stored.
MASKED_SUPPLIER_NAMES = ("оборонний постачальник",)


def is_masked_supplier(code: str, name: str) -> bool:
    """A withheld supplier: one repeated digit (88888888, 00000000…) or the
    defence stand-in's name."""
    folded = " ".join(name.casefold().split())
    return len(set(code)) == 1 or any(m in folded for m in MASKED_SUPPLIER_NAMES)


def kyiv_day(stamp: Any) -> str | None:
    """A Prozorro timestamp → the calendar day in Kyiv."""
    text = str(stamp or "").strip()
    if not text:
        return None
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", text) else None
    if moment.tzinfo is None:
        return moment.date().isoformat()
    return moment.astimezone(_KYIV).date().isoformat()


def whole(value: Any) -> int | None:
    """A published amount → whole units, half up. Unreadable → None."""
    try:
        return int(Decimal(str(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# One contract → one row
# ---------------------------------------------------------------------------


def _title(doc: dict[str, Any]) -> str:
    """The contract's title, else its first item's description (most
    reported purchases file no title)."""
    text = str(doc.get("title") or "")
    if not text.strip():
        items = doc.get("items") or []
        first = items[0] if items and isinstance(items[0], dict) else {}
        text = str(first.get("description") or "")
    return " ".join(text.split())[:TITLE_CHARS]


@dataclass(frozen=True)
class ContractRow:
    id: str
    edrpou: int
    supplier_name: str
    status: str
    signed: str | None
    value: int | None
    currency: str
    buyer_edrpou: str
    buyer_name: str
    title: str
    contract_ref: str
    tender_id: str
    category: str


def reduce_contract(doc: dict[str, Any]) -> tuple[ContractRow | None, str]:
    """The row kept for one contract, and why not when it is None.

    Reasons: ``kept``, ``person`` (a ten-digit supplier code: decision B),
    ``invalid_id`` (no eight-digit code), ``masked`` (the supplier is
    withheld: ``is_masked_supplier``), ``no_supplier``, ``joint`` (several
    suppliers — not seen in the sample; not attributed to any one).
    """
    suppliers = doc.get("suppliers") or []
    if not suppliers:
        return None, "no_supplier"
    if len(suppliers) > 1:
        return None, "joint"
    supplier = suppliers[0] or {}
    ident = supplier.get("identifier") or {}
    if is_person_code(ident.get("id")):
        return None, "person"
    edrpou = supplier_edrpou(ident.get("id"))
    if edrpou is None or (ident.get("scheme") not in (None, "", "UA-EDR")):
        return None, "invalid_id"
    supplier_name = str(ident.get("legalName") or supplier.get("name") or "").strip()
    if is_masked_supplier(edrpou, supplier_name):
        return None, "masked"
    value = doc.get("value") or {}
    buyer = doc.get("buyer") or doc.get("procuringEntity") or {}
    bident = buyer.get("identifier") or {}
    return (
        ContractRow(
            id=str(doc.get("id") or ""),
            edrpou=int(edrpou),
            supplier_name=supplier_name,
            status=str(doc.get("status") or ""),
            signed=kyiv_day(doc.get("dateSigned")),
            value=whole(value.get("amount")),
            currency=str(value.get("currency") or ""),
            buyer_edrpou=re.sub(r"\s+", "", str(bident.get("id") or "")),
            buyer_name=str(bident.get("legalName") or buyer.get("name") or "").strip(),
            title=_title(doc),
            contract_ref=str(doc.get("contractID") or ""),
            tender_id=str(doc.get("tender_id") or ""),
            category=str(doc.get("mainProcurementCategory") or ""),
        ),
        "kept",
    )


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS buyer (
    id INTEGER PRIMARY KEY,
    edrpou TEXT NOT NULL,
    name TEXT NOT NULL,
    UNIQUE (edrpou, name)
);
CREATE TABLE IF NOT EXISTS contract (
    id BLOB PRIMARY KEY,
    edrpou INTEGER NOT NULL,
    supplier_name TEXT,
    status TEXT,
    signed TEXT,
    value INTEGER,
    currency TEXT,
    buyer INTEGER,
    title TEXT,
    contract_ref TEXT,
    tender_id BLOB,
    category TEXT
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS contract_edrpou ON contract (edrpou);
CREATE TABLE IF NOT EXISTS tender_method (
    id BLOB PRIMARY KEY,
    method TEXT NOT NULL
) WITHOUT ROWID;
"""


def _key(hex_id: str) -> bytes | str:
    """Prozorro ids are 32 hex digits: stored as 16 bytes, else as text."""
    try:
        return bytes.fromhex(hex_id) if len(hex_id) == 32 else hex_id
    except ValueError:
        return hex_id


def hex_id(key: bytes | str | None) -> str:
    if isinstance(key, bytes):
        return key.hex()
    return str(key or "")


class Store:
    """The harvest's SQLite file: contracts, buyers, procedures, cursors."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.executescript(_SCHEMA)
        self._buyers: dict[tuple[str, str], int] = {
            (e, n): i for i, e, n in self.conn.execute("SELECT id, edrpou, name FROM buyer")
        }
        version = self.get("schema_version")
        if version is None:
            self.set("schema_version", STORE_SCHEMA_VERSION)
        elif version != STORE_SCHEMA_VERSION:
            raise HarvestError(f"store schema {version}, expected {STORE_SCHEMA_VERSION}")
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    # state -----------------------------------------------------------------

    def get(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])

    def set(self, key: str, value: str | None) -> None:
        if value is None:
            self.conn.execute("DELETE FROM state WHERE key = ?", (key,))
        else:
            self.conn.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (key, value))

    def state(self) -> dict[str, str]:
        return {str(k): str(v) for k, v in self.conn.execute("SELECT key, value FROM state")}

    # rows ------------------------------------------------------------------

    def _buyer(self, edrpou: str, name: str) -> int:
        key = (edrpou, name)
        found = self._buyers.get(key)
        if found is None:
            cur = self.conn.execute("INSERT INTO buyer (edrpou, name) VALUES (?, ?)", key)
            found = int(cur.lastrowid or 0)
            self._buyers[key] = found
        return found

    def put(self, contract_id: str, row: ContractRow | None) -> None:
        """Keep a contract's row, or remove it when the read kept none."""
        key = _key(contract_id)
        if row is None:
            self.conn.execute("DELETE FROM contract WHERE id = ?", (key,))
            return
        self.conn.execute(
            "INSERT OR REPLACE INTO contract VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                key, row.edrpou, row.supplier_name, row.status, row.signed, row.value,
                row.currency, self._buyer(row.buyer_edrpou, row.buyer_name), row.title,
                row.contract_ref, _key(row.tender_id) if row.tender_id else None, row.category,
            ),
        )

    def put_method(self, tender_id: str, method: str, *, replace: bool = True) -> None:
        verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
        self.conn.execute(f"{verb} INTO tender_method VALUES (?, ?)", (_key(tender_id), method))

    def missing_methods(self, limit: int) -> list[str]:
        """Tender ids of stored contracts whose procedure is not kept, the
        contracts signed most recently first."""
        rows = self.conn.execute(
            "SELECT c.tender_id, MAX(c.signed) AS day FROM contract c "
            "LEFT JOIN tender_method t ON t.id = c.tender_id "
            "WHERE c.tender_id IS NOT NULL AND t.id IS NULL "
            "GROUP BY c.tender_id ORDER BY day DESC LIMIT ?",
            (limit,),
        )
        return [hex_id(r[0]) for r in rows]

    def count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM contract").fetchone()[0])

    def commit(self) -> None:
        self.conn.commit()


# ---------------------------------------------------------------------------
# Reading the API
# ---------------------------------------------------------------------------


def client(workers: int = WORKERS) -> httpx.Client:
    return httpx.Client(
        timeout=TIMEOUT_S,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        follow_redirects=True,
        limits=httpx.Limits(max_connections=workers + 2, max_keepalive_connections=workers + 2),
    )


def _get(client: httpx.Client, url: str, params: dict[str, Any] | None = None) -> Any:
    """GET JSON with a backoff (``BACKOFF_S``) on 429, 5xx and network
    errors. A 404 is None (a contract removed since it was listed)."""
    last: Exception | None = None
    for attempt in range(ATTEMPTS):
        try:
            resp = client.get(url, params=params)
            if resp.status_code == 404:
                return None
            if resp.status_code == 429 or resp.status_code >= 500:
                raise HarvestError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPError, HarvestError, ValueError) as exc:
            last = exc
            if attempt + 1 < ATTEMPTS:
                time.sleep(BACKOFF_S[attempt])
    raise HarvestError(f"{url}: {type(last).__name__}: {last}")


def first_offset(now: datetime | None = None) -> str:
    """The feed offset a first run starts from: ``FIRST_RUN_DAYS`` back."""
    start = (now or datetime.now(UTC)) - timedelta(days=FIRST_RUN_DAYS)
    return str(int(start.timestamp()))


def offset_moment(offset: str | None) -> datetime | None:
    """The time a feed offset stands at (its leading Unix timestamp)."""
    try:
        return datetime.fromtimestamp(float(str(offset or "").split(".")[0]), UTC)
    except (TypeError, ValueError, OverflowError):
        return None


def _page(client: httpx.Client, feed: str, offset: str, opt_fields: str | None) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": PAGE_LIMIT, "offset": offset}
    if opt_fields:
        params["opt_fields"] = opt_fields
    page = _get(client, API_BASE + feed, params)
    if not isinstance(page, dict) or not isinstance(page.get("data"), list):
        raise HarvestError(f"{feed} listing is not the feed's shape")
    return page


def _next(page: dict[str, Any]) -> str | None:
    nxt = page.get("next_page") or {}
    return str(nxt.get("offset")) if nxt.get("offset") else None


def harvest_methods(
    http: httpx.Client, store: Store, *, deadline: float | None = None
) -> dict[str, Any]:
    """Read the tenders feed forward, keeping the procedure type of every
    tender that has contracts. Cheap: no per-tender request."""
    cursor = store.get("tenders_cursor") or store.get("started_from") or first_offset()
    counts = {"tender_pages": 0, "tenders_with_contracts": 0}
    outcome = "deadline"
    while deadline is None or time.monotonic() < deadline:
        try:
            page = _page(http, "tenders", cursor, "procurementMethodType,contracts")
        except HarvestError as exc:
            if counts["tender_pages"] == 0:
                raise
            logger.warning("prozorro harvest: tenders listing stopped at %s: %s", cursor, exc)
            outcome = "listing_failed"
            break
        rows = page["data"]
        if not rows:
            outcome = "caught_up"
            break
        for row in rows:
            if row.get("contracts") and row.get("id"):
                store.put_method(str(row["id"]), str(row.get("procurementMethodType") or ""))
                counts["tenders_with_contracts"] += 1
        counts["tender_pages"] += 1
        nxt = _next(page)
        if not nxt or nxt == cursor:
            outcome = "caught_up"
            break
        cursor = nxt
        store.set("tenders_cursor", cursor)
        store.commit()
    store.set("tenders_cursor", cursor)
    store.commit()
    return {"tenders_outcome": outcome, **counts}


def harvest_methods_back(
    http: httpx.Client, store: Store, *, deadline: float | None = None
) -> dict[str, Any]:
    """Read the tenders feed backwards from the harvest's start to
    ``BACK_DAYS`` before it, keeping each procedure a forward read has not
    already kept. Its cursor is ``tenders_back_cursor``; ``tenders_back_done``
    is set when it reaches the floor or the feed's beginning."""
    if store.get("tenders_back_done"):
        return {"tenders_back_outcome": "done", "tender_back_pages": 0}
    started = store.get("started_from")
    start_at = offset_moment(started)
    if start_at is None:
        return {"tenders_back_outcome": "not_started", "tender_back_pages": 0}
    floor = start_at - timedelta(days=BACK_DAYS)
    cursor = store.get("tenders_back_cursor") or str(started)
    pages = 0
    outcome = "deadline"
    while deadline is None or time.monotonic() < deadline:
        params: dict[str, Any] = {
            "limit": PAGE_LIMIT, "offset": cursor, "descending": 1,
            "opt_fields": "procurementMethodType,contracts",
        }
        try:
            page = _get(http, API_BASE + "tenders", params)
            if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                raise HarvestError("tenders listing is not the feed's shape")
        except HarvestError as exc:
            logger.warning("prozorro harvest: backward tenders read stopped at %s: %s", cursor, exc)
            outcome = "listing_failed"
            break
        rows = page["data"]
        for row in rows:
            if row.get("contracts") and row.get("id"):
                store.put_method(
                    str(row["id"]), str(row.get("procurementMethodType") or ""), replace=False
                )
        pages += 1
        nxt = _next(page)
        moment = offset_moment(nxt)
        if not rows or not nxt or nxt == cursor or moment is None or moment < floor:
            outcome = "done"
            store.set("tenders_back_done", datetime.now(UTC).isoformat(timespec="seconds"))
            store.set("tenders_back_cursor", nxt or cursor)
            store.commit()
            break
        cursor = nxt
        store.set("tenders_back_cursor", cursor)
        store.commit()
    back_at = offset_moment(store.get("tenders_back_cursor"))
    return {
        "tenders_back_outcome": outcome,
        "tender_back_pages": pages,
        "tenders_back_at": back_at.isoformat(timespec="seconds") if back_at else None,
    }


def fill_missing_methods(
    http: httpx.Client,
    store: Store,
    *,
    deadline: float | None = None,
    limit: int = FILL_PER_RUN,
    workers: int = WORKERS,
) -> dict[str, Any]:
    """Read, one at a time, the tenders of stored contracts whose procedure
    the feeds did not give, the most recently signed first. A tender that
    answers 404 is kept with an empty procedure (never asked again); one
    that fails is left for the next run."""
    counts = {"tenders_filled": 0, "tenders_gone": 0, "tenders_fill_failed": 0}
    ids = store.missing_methods(limit)
    if not ids:
        return {"fill_outcome": "none_missing", **counts}

    def one(tender_id: str) -> tuple[str, Any]:
        if deadline is not None and time.monotonic() >= deadline:
            return tender_id, "skipped"
        try:
            return tender_id, _get(http, f"{API_BASE}tenders/{tender_id}")
        except HarvestError:
            return tender_id, "failed"

    outcome = "done"
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for tender_id, doc in pool.map(one, ids):
            if doc == "skipped":
                outcome = "deadline"
                continue
            if doc == "failed":
                counts["tenders_fill_failed"] += 1
                continue
            data = doc.get("data") if isinstance(doc, dict) else None
            if isinstance(data, dict):
                store.put_method(tender_id, str(data.get("procurementMethodType") or ""))
                counts["tenders_filled"] += 1
            else:
                store.put_method(tender_id, "")
                counts["tenders_gone"] += 1
    store.commit()
    return {"fill_outcome": outcome, **counts}


def harvest(
    http: httpx.Client,
    store: Store,
    *,
    deadline: float | None = None,
    workers: int = WORKERS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Read the contracts feed forward from the store's cursor until caught
    up or past ``deadline`` (a ``time.monotonic()`` value). Never moves the
    cursor past a page that was not read whole. Raises ``HarvestError`` only
    when the listing cannot be read on the first page."""
    if store.get("contracts_cursor") is None:
        start = first_offset(now)
        store.set("contracts_cursor", start)
        store.set("started_from", start)
        store.commit()
    counts = {
        "pages": 0, "contracts_read": 0, "kept": 0, "removed": 0,
        "skipped_person": 0, "skipped_invalid_id": 0, "skipped_masked": 0,
        "skipped_other": 0,
    }
    outcome = "deadline"
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        while deadline is None or time.monotonic() < deadline:
            cursor = str(store.get("contracts_cursor"))
            try:
                page = _page(http, "contracts", cursor, None)
            except HarvestError as exc:
                if counts["pages"] == 0:
                    raise
                logger.warning("prozorro harvest: listing stopped at %s: %s", cursor, exc)
                outcome = "listing_failed"
                break
            rows = page["data"]
            if not rows:
                outcome = "caught_up"
                store.set("caught_up_at", datetime.now(UTC).isoformat(timespec="seconds"))
                break
            ids = list(dict.fromkeys(str(r.get("id") or "") for r in rows if r.get("id")))
            try:
                docs = list(pool.map(lambda i: _get(http, f"{API_BASE}contracts/{i}"), ids))
            except HarvestError as exc:
                logger.warning("prozorro harvest: a contract failed at %s: %s", cursor, exc)
                outcome = "contract_failed"
                break
            for contract_id, doc in zip(ids, docs, strict=True):
                data = doc.get("data") if isinstance(doc, dict) else None
                row, reason = reduce_contract(data) if isinstance(data, dict) else (None, "gone")
                had = store.conn.execute(
                    "SELECT 1 FROM contract WHERE id = ?", (_key(contract_id),)
                ).fetchone()
                store.put(contract_id, row)
                counts["contracts_read"] += 1
                if row is not None:
                    counts["kept"] += 1
                else:
                    if had:
                        counts["removed"] += 1
                    key = {
                        "person": "skipped_person",
                        "invalid_id": "skipped_invalid_id",
                        "masked": "skipped_masked",
                    }
                    counts[key.get(reason, "skipped_other")] += 1
            counts["pages"] += 1
            nxt = _next(page)
            if not nxt or nxt == cursor:
                outcome = "caught_up"
                store.set("caught_up_at", datetime.now(UTC).isoformat(timespec="seconds"))
                store.commit()
                break
            store.set("contracts_cursor", nxt)
            store.commit()
    store.commit()
    final_cursor = store.get("contracts_cursor")
    moment = offset_moment(final_cursor)
    return {
        "outcome": outcome,
        "cursor": final_cursor,
        "cursor_at": moment.isoformat(timespec="seconds") if moment else None,
        "contracts_stored": store.count(),
        **counts,
    }
