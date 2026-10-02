"""Chile — ChileCompra, public procurement on Mercado Público (monthly open data).

ChileCompra (Dirección de Compras y Contratación Pública) runs Mercado Público,
the platform through which some 850 Chilean public bodies buy. It publishes
every month's tenders (*licitaciones*) and purchase orders (*órdenes de
compra*) as keyless bulk CSVs behind ``datos-abiertos.chilecompra.cl``. This
adapter answers one question from them: **has this company sold to the
Chilean state, how much, and to whom** — over the last twelve months.

The join key is the RUT
-----------------------
Every supplier row carries the supplier's **RUT** (Rol Único Tributario, the
Chilean tax number): ``RutProveedor`` on tender bids, ``RutSucursal`` on
purchase orders. GLEIF files the RUT in ``registeredAs`` for most Chilean LEIs
— but not under one registration authority. Measured over the 1,700 LEI
records with a Chilean legal address on 2 October 2026:

==========  ==============================================  ====  ====  =====
RA code     authority (as GLEIF labels it)                  LEIs  RUTs  other
==========  ==============================================  ====  ====  =====
RA000787    Servicio de Impuestos Internos                   804   804      0
RA000090    "Registro de Comercio" / "Dirección General      249   198     51
            de Tributación (Ministerio de Hacienda)"
RA000846    Comisión para el Mercado Financiero              230     1    229
RA000091    Conservador de Bienes Raíces de Santiago         169    97     72
RA000785    Registro de Empresas y Sociedades                 17    17      0
==========  ==============================================  ====  ====  =====

RA000090 and RA000091 are used both for RUTs and for commercial-register
inscriptions (``fojas 59592 número 30591``), so the RA code does not say what
``registeredAs`` holds. The deriver therefore accepts **any Chilean RA** and
lets the value decide: it must be RUT-shaped and pass the mod-11 check digit.
Every one of the 1,119 RUT-shaped values in that population passes it.
``RA888888`` (a global "other" code) is deliberately not claimed: the
lookup pipeline lets the first deriver that names an RA code win, and a
global code is not Chile's to take.

Why the index is a release asset, not built on the host
-------------------------------------------------------
Twelve months of files are ~1.4 GB of zips and ~10 GB of CSV. Parsing that is
fifteen minutes or more of CPU-bound Python; on Render it would hold the GIL
against live lookups after every deploy. The *result* is small — about 5 MB
for a year of company suppliers — so it is built monthly by
``.github/workflows/refresh-chilecompra-index.yml``
(``scripts/build_chilecompra_index.py``), published as the release asset
``chilecompra-index/chilecompra.sqlite.gz``, and downloaded at boot by
``warm_index`` (the ONRC / MEIP rule). Without the file the source is not
announced at all (``covers_lei``).

The build is a **full rebuild** of the window every month rather than an
append: ChileCompra re-publishes older months (the December 2025 purchase-order
file was rewritten on 2 October 2026), so an appended month would go stale.

Scope — what Stephen decided on 2 October 2026
----------------------------------------------
* **Natural persons are dropped at build time.** About a third of supplier
  RUTs belong to sole traders, for whom the RUT is the national ID number
  (RUN). Chilean RUT numbers below 50,000,000 are issued to natural persons,
  company RUTs above it, so any row whose RUT body is below
  ``NATURAL_PERSON_RUT_MAX`` is never stored.
* **All company suppliers are kept**, not just today's LEI holders, so a
  newly issued LEI matches without a rebuild.
* Licence treated as **CC0** — what the Open Contracting Partnership's data
  registry lists for ChileCompra's OCDS publications — pending ChileCompra's
  confirmation that it covers the bulk CSVs too.

What the files say, and what they do not
----------------------------------------
Purchase orders: one row per item, the order's total converted to pesos
(``MontoTotalOC_PesosChilenos``), the buying body, and the order's state.
Cancelled orders are not counted. Tenders: one row per bid line, losing bids
included, with ``Oferta seleccionada`` marking the lines awarded. Tender
amounts are in the tender's own currency and are not summed. Nothing here is
ownership: the entity statement carries the activity, as TED's does.
"""

from __future__ import annotations

import csv
import heapq
import io
import json
import logging
import os
import re
import sqlite3
import time
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .. import provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.chilecompra import ChileCompraBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities and the RUT grammar
# ---------------------------------------------------------------------------

#: Every Chilean registration authority GLEIF lists (see the module docstring
#: for what each actually files). The value, not the code, decides.
CL_RA_CODES: frozenset[str] = frozenset(
    {"RA000787", "RA000090", "RA000091", "RA000785", "RA000846"}
)

#: RUT bodies below this are issued to natural persons (their RUN).
NATURAL_PERSON_RUT_MAX = 50_000_000

_RUT_SHAPE = re.compile(r"(\d{6,9})-?([\dK])")


def rut_check_digit(body: int) -> str:
    """The RUT check digit: mod 11 over weights 2..7 from the right."""
    total, weight = 0, 2
    for digit in reversed(str(body)):
        total += int(digit) * weight
        weight = 2 if weight == 7 else weight + 1
    value = 11 - total % 11
    return "0" if value == 11 else "K" if value == 10 else str(value)


def parse_rut(raw: str | None) -> tuple[int, str] | None:
    """``(body, check digit)`` for a well-formed RUT with a valid check digit.

    Dots, spaces and case are ignored: ChileCompra writes ``76.242.192-5``,
    its OCDS feed ``762421925`` and GLEIF either form.
    """
    text = re.sub(r"[.\s]", "", str(raw or "")).upper()
    match = _RUT_SHAPE.fullmatch(text)
    if not match:
        return None
    body, dv = int(match.group(1)), match.group(2)
    if body <= 0 or rut_check_digit(body) != dv:
        return None
    return body, dv


def normalise_rut(raw: str) -> str:
    """Return a company RUT as ``76242192-5``, or raise ``ValueError``.

    Natural-person RUTs are rejected too: the index never holds them, and an
    LEI is not issued to a person.
    """
    parsed = parse_rut(raw)
    if parsed is None:
        raise ValueError(f"not a valid Chilean RUT: {raw!r}")
    body, dv = parsed
    if body < NATURAL_PERSON_RUT_MAX:
        raise ValueError(f"RUT is in the natural-person range: {raw!r}")
    return f"{body}-{dv}"


def is_rut(raw: str) -> bool:
    try:
        normalise_rut(raw)
    except ValueError:
        return False
    return True


def format_rut(canonical: str) -> str:
    """``76242192-5`` → ``76.242.192-5``, the way Chilean documents print it."""
    body, _, dv = canonical.partition("-")
    return f"{int(body):,}".replace(",", ".") + f"-{dv}"


# ---------------------------------------------------------------------------
# Where the files are
# ---------------------------------------------------------------------------

DATASET_URL = "https://datos-abiertos.chilecompra.cl/descargas"
_HOMEPAGE = "https://www.mercadopublico.cl/"
TENDERS_URL_TEMPLATE = "https://transparenciachc.blob.core.windows.net/lic-da/{year}-{month}.zip"
ORDERS_URL_TEMPLATE = "https://transparenciachc.blob.core.windows.net/oc-da/{year}-{month}.zip"

LICENSE_ID = "CC0-1.0"

#: Bumped whenever the index tables change shape.
INDEX_SCHEMA_VERSION = "1"

#: Purchase-order states not counted: cancelled.
_CANCELLED_ORDER_STATES = frozenset({"9"})

#: Per supplier, how many buyers and records are kept for the card.
TOP_BUYERS = 5
TOP_ORDERS = 3
RECENT_WINS = 3


def month_urls(month: str) -> tuple[str, str]:
    """``2026-08`` → the tenders and purchase-order zip URLs for that month.

    The files are named without a leading zero (``2026-8.zip``).
    """
    year, mm = month.split("-")
    month_n = str(int(mm))
    return (
        TENDERS_URL_TEMPLATE.format(year=year, month=month_n),
        ORDERS_URL_TEMPLATE.format(year=year, month=month_n),
    )


def window_months(end: str, count: int) -> list[str]:
    """The ``count`` months ending at ``end`` (``2026-08``), oldest first."""
    year, month = (int(p) for p in end.split("-"))
    out: list[str] = []
    for _ in range(count):
        out.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return sorted(out)


def latest_complete_month(
    today: datetime | None = None,
    *,
    exists: Any = None,
    lookback: int = 6,
) -> str:
    """The newest month for which **both** files are published.

    ChileCompra publishes month-to-date files and regenerates the whole
    series as it goes (on 2 October 2026 every file from November 2025 to
    October 2026 carried that day's ``Last-Modified``, and the October tenders
    file was an empty 872 bytes). So the current month is never complete: the
    search starts at the previous calendar month and steps back only if a file
    is missing. ``exists`` is ``url -> bool``; the default asks with ``HEAD``.
    """
    if exists is None:
        import httpx

        def exists(url: str) -> bool:
            try:
                return httpx.head(url, timeout=30.0, follow_redirects=True).status_code == 200
            except httpx.HTTPError:
                return False

    now = today or datetime.now(UTC)
    year, month = now.year, now.month - 1
    if month == 0:
        year, month = year - 1, 12
    for _ in range(lookback):
        candidate = f"{year:04d}-{month:02d}"
        if all(exists(u) for u in month_urls(candidate)):
            return candidate
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    raise IndexBuildError(f"no month with both files in the last {lookback} months")


# ---------------------------------------------------------------------------
# Index build
# ---------------------------------------------------------------------------


class IndexBuildError(RuntimeError):
    """A file's shape is not what the index builder understands."""


@dataclass
class MonthInput:
    """One month's two files, as downloaded."""

    month: str  # "2026-08"
    tenders_zip: Path | None
    orders_zip: Path | None
    files: dict[str, Any] = field(default_factory=dict)  # url → {last_modified, bytes}


_ORDER_COLUMNS = (
    "Codigo", "codigoEstado", "FechaEnvio",
    "MontoTotalOC_PesosChilenos", "CodigoOrganismoPublico", "OrganismoPublico",
    "RutSucursal", "NombreProveedor",
)
_TENDER_COLUMNS = (
    "CodigoExterno", "CodigoOrganismo", "NombreOrganismo",
    "FechaAdjudicacion", "FechaPublicacion", "RutProveedor", "NombreProveedor",
    "RazonSocialProveedor", "Oferta seleccionada",
)


def _iter_rows(zip_path: Path, required: tuple[str, ...], counts: dict[str, int], key: str) -> Iterator[dict[str, str]]:
    """Stream the single CSV inside a monthly zip as dicts of the needed columns.

    Semicolon-separated, Latin-1, CRLF, with quoted multi-line descriptions —
    ``newline=""`` lets the csv module keep those inside one record. Rows with
    the wrong number of fields are counted and skipped.
    """
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise IndexBuildError(f"{zip_path.name}: expected one CSV, found {names}")
        with zf.open(names[0]) as raw:
            reader = csv.reader(
                io.TextIOWrapper(raw, encoding="latin-1", newline=""), delimiter=";"
            )
            header = [h.strip().strip('"') for h in next(reader)]
            missing = [c for c in required if c not in header]
            if missing:
                raise IndexBuildError(f"{zip_path.name}: missing columns {missing}")
            index = {c: header.index(c) for c in required}
            width = len(header)
            for row in reader:
                if len(row) != width:
                    counts[f"{key}_malformed_rows"] += 1
                    continue
                counts[f"{key}_rows"] += 1
                yield {c: row[i] for c, i in index.items()}


def _amount_clp(text: str) -> int:
    """A peso total as filed → whole pesos, half up. Unreadable → 0.

    The files write a decimal comma (``447888668,128906``) and, for some
    rows, exponent notation with one (``2,8e+07``, ``7e+08``); no thousands
    separator was seen in 1.5 million August 2026 rows.
    """
    value = str(text or "").strip().replace(",", ".")
    try:
        return int(Decimal(value).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return 0


def _iso_date(text: str) -> str | None:
    value = str(text or "").strip()[:10]
    return value if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) else None


ORDER_URL_TEMPLATE = (
    "https://www.mercadopublico.cl/PurchaseOrder/Modules/PO/DetailsPurchaseOrder.aspx?codigoOC={code}"
)
TENDER_URL_TEMPLATE = "https://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion={code}"


def record_url(kind: str, code: str) -> str:
    """The Mercado Público page for a purchase order or a tender.

    Built from the code rather than stored: the files' own ``Link`` column is
    exactly this URL (over http), and storing it was a third of the index.
    """
    template = ORDER_URL_TEMPLATE if kind == "order" else TENDER_URL_TEMPLATE
    return template.format(code=code)


@dataclass
class _Supplier:
    name: str = ""
    name_date: str = ""
    orders: int = 0
    order_value: int = 0
    first: str = "9999-99-99"
    last: str = "0000-00-00"
    tenders_bid: set[str] = field(default_factory=set)
    tenders_won: set[str] = field(default_factory=set)
    # buyer code → [orders, order value, tenders won]
    buyers: dict[int, list[int]] = field(default_factory=dict)
    top_orders: list[tuple[int, str, tuple[Any, ...]]] = field(default_factory=list)
    wins: dict[str, tuple[Any, ...]] = field(default_factory=dict)

    def seen(self, day: str | None, name: str) -> None:
        if not day:
            return
        self.first = min(self.first, day)
        self.last = max(self.last, day)
        if name and day >= self.name_date:
            self.name, self.name_date = name, day


def build_index(months: list[MonthInput], out_path: Path | str) -> dict[str, str]:
    """Build the SQLite index from a window of monthly files.

    Written beside ``out_path`` and renamed into place, so a reader never sees
    a half-built file. Returns the ``meta`` table.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    counts: dict[str, int] = {
        k: 0
        for k in (
            "orders_rows", "orders_malformed_rows", "tenders_rows",
            "tenders_malformed_rows", "rows_invalid_rut",
            "rows_natural_person", "orders_cancelled",
        )
    }
    natural_people: set[int] = set()
    suppliers: dict[int, _Supplier] = {}
    dv_of: dict[int, str] = {}
    buyer_names: dict[int, str] = {}

    def supplier_for(raw_rut: str) -> tuple[int, _Supplier] | None:
        parsed = parse_rut(raw_rut)
        if parsed is None:
            counts["rows_invalid_rut"] += 1
            return None
        body, dv = parsed
        if body < NATURAL_PERSON_RUT_MAX:
            counts["rows_natural_person"] += 1
            natural_people.add(body)
            return None
        dv_of[body] = dv
        return body, suppliers.setdefault(body, _Supplier())

    def buyer_code(code: str, name: str) -> int | None:
        try:
            n = int(str(code).strip())
        except ValueError:
            return None
        if name:
            buyer_names[n] = name.strip()
        return n

    for month in sorted(months, key=lambda m: m.month):
        if month.orders_zip is not None:
            orders_seen: set[str] = set()
            for row in _iter_rows(month.orders_zip, _ORDER_COLUMNS, counts, "orders"):
                code = row["Codigo"].strip()
                if not code or code in orders_seen:
                    continue  # one row per item; count the order once
                orders_seen.add(code)
                if row["codigoEstado"].strip() in _CANCELLED_ORDER_STATES:
                    counts["orders_cancelled"] += 1
                    continue
                found = supplier_for(row["RutSucursal"])
                if found is None:
                    continue
                _, sup = found
                day = _iso_date(row["FechaEnvio"])
                value = _amount_clp(row["MontoTotalOC_PesosChilenos"])
                sup.orders += 1
                sup.order_value += value
                sup.seen(day, row["NombreProveedor"].strip())
                buyer = buyer_code(row["CodigoOrganismoPublico"], row["OrganismoPublico"])
                if buyer is not None:
                    tally = sup.buyers.setdefault(buyer, [0, 0, 0])
                    tally[0] += 1
                    tally[1] += value
                record = (code, day, buyer)
                heapq.heappush(sup.top_orders, (value, code, record))
                if len(sup.top_orders) > TOP_ORDERS:
                    heapq.heappop(sup.top_orders)
        if month.tenders_zip is not None:
            for row in _iter_rows(month.tenders_zip, _TENDER_COLUMNS, counts, "tenders"):
                code = row["CodigoExterno"].strip()
                if not code:
                    continue
                found = supplier_for(row["RutProveedor"])
                if found is None:
                    continue
                _, sup = found
                sup.tenders_bid.add(code)
                name = (row["RazonSocialProveedor"] or row["NombreProveedor"]).strip()
                day = _iso_date(row["FechaAdjudicacion"]) or _iso_date(row["FechaPublicacion"])
                if row["Oferta seleccionada"].strip() != "Seleccionada":
                    sup.seen(_iso_date(row["FechaPublicacion"]), name)
                    continue
                sup.seen(day, name)
                buyer = buyer_code(row["CodigoOrganismo"], row["NombreOrganismo"])
                if code not in sup.tenders_won:
                    sup.tenders_won.add(code)
                    if buyer is not None:
                        sup.buyers.setdefault(buyer, [0, 0, 0])[2] += 1
                sup.wins[code] = (code, day, buyer)

    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(str(tmp))
    conn.executescript(_SCHEMA)
    buyers_used: set[int] = set()
    for body, sup in suppliers.items():
        if not (sup.orders or sup.tenders_bid):
            continue
        conn.execute(
            "INSERT INTO supplier VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                body, dv_of[body], sup.name, sup.orders, sup.order_value,
                len(sup.tenders_bid), len(sup.tenders_won), len(sup.buyers),
                sup.first if sup.first != "9999-99-99" else None,
                sup.last if sup.last != "0000-00-00" else None,
            ),
        )
        ranked = sorted(sup.buyers.items(), key=lambda kv: (-kv[1][1], -kv[1][2], -kv[1][0], kv[0]))
        for buyer_id, (n_orders, value, wins) in ranked[:TOP_BUYERS]:
            buyers_used.add(buyer_id)
            conn.execute(
                "INSERT INTO supplier_buyer VALUES (?,?,?,?,?)",
                (body, buyer_id, n_orders, value, wins),
            )
        for value, _code, (code, day, buyer) in sorted(sup.top_orders, reverse=True):
            if buyer is not None:
                buyers_used.add(buyer)
            conn.execute(
                "INSERT INTO record VALUES (?,?,?,?,?,?)",
                (body, "order", code, day, buyer, value),
            )
        latest_wins = sorted(sup.wins.values(), key=lambda w: (w[1] or "", w[0]), reverse=True)
        for code, day, buyer in latest_wins[:RECENT_WINS]:
            if buyer is not None:
                buyers_used.add(buyer)
            conn.execute(
                "INSERT INTO record VALUES (?,?,?,?,?,?)",
                (body, "award", code, day, buyer, None),
            )
    conn.executemany(
        "INSERT INTO buyer VALUES (?,?)",
        [(c, buyer_names.get(c, "")) for c in sorted(buyers_used)],
    )

    month_names = sorted(m.month for m in months)
    files: dict[str, Any] = {}
    for m in months:
        files.update(m.files)
    stored = conn.execute("SELECT COUNT(*) FROM supplier").fetchone()[0]
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "build_seconds": f"{time.monotonic() - started:.1f}",
        "months": json.dumps(month_names),
        "data_from": month_names[0] if month_names else "",
        "data_to": month_names[-1] if month_names else "",
        "files": json.dumps(files, sort_keys=True),
        "suppliers": str(stored),
        "natural_persons_dropped": str(len(natural_people)),
        **{k: str(v) for k, v in counts.items()},
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp, out_path)
    logger.info("chilecompra: index built at %s: %s suppliers", out_path, stored)
    return meta


_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE supplier (
    rut INTEGER PRIMARY KEY,
    dv TEXT NOT NULL,
    name TEXT,
    orders INTEGER NOT NULL,
    order_value_clp INTEGER NOT NULL,
    tenders_bid INTEGER NOT NULL,
    tenders_won INTEGER NOT NULL,
    buyers INTEGER NOT NULL,
    first_date TEXT,
    last_date TEXT
);
CREATE TABLE buyer (code INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE supplier_buyer (
    rut INTEGER NOT NULL,
    buyer INTEGER NOT NULL,
    orders INTEGER NOT NULL,
    order_value_clp INTEGER NOT NULL,
    tenders_won INTEGER NOT NULL,
    PRIMARY KEY (rut, buyer)
) WITHOUT ROWID;
CREATE TABLE record (
    rut INTEGER NOT NULL,
    kind TEXT NOT NULL,
    code TEXT NOT NULL,
    date TEXT,
    buyer INTEGER,
    value_clp INTEGER,
    PRIMARY KEY (rut, kind, code)
) WITHOUT ROWID;
"""


# ---------------------------------------------------------------------------
# Where the index lives, and getting it onto the host
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``CHILECOMPRA_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().chilecompra_db_file
    return Path(configured) if configured else data_root() / "chilecompra.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().chilecompra_db_file:
            logger.warning("chilecompra: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("chilecompra: index at %s has an unexpected schema; ignoring", path)
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


def window_label(meta: dict[str, str]) -> str:
    """``Sep 2025 – Aug 2026`` from the meta table's first and last month."""

    def label(month: str) -> str:
        try:
            return datetime.strptime(month, "%Y-%m").strftime("%b %Y")
        except ValueError:
            return month

    start, end = meta.get("data_from") or "", meta.get("data_to") or ""
    if not start or not end:
        return ""
    return label(start) if start == end else f"{label(start)} – {label(end)}"


def data_through(meta: dict[str, str]) -> datetime | None:
    """The first day of the latest month the index covers.

    What ``record_snapshot`` wants is the upstream extract date. The files are
    monthly and the latest one covers that whole month, so the month is what
    the data is "as of" — not when the asset happened to be rebuilt.
    """
    try:
        return datetime.strptime(meta.get("data_to") or "", "%Y-%m").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    meta = read_meta() if meta is None else meta
    label = window_label(meta)
    provenance.record_snapshot(
        data_through(meta),
        f"ChileCompra monthly open-data files, {label}" if label else "ChileCompra monthly open-data files",
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
    url = settings.chilecompra_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"chilecompra": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"chilecompra": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"chilecompra": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("chilecompra: asset check/download failed: %s", exc)
        return {"chilecompra": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# Reading one supplier
# ---------------------------------------------------------------------------


def supplier_record(rut: str) -> dict[str, Any] | None:
    """Everything the index holds for one canonical RUT, or None."""
    conn = _shared_conn()
    if conn is None:
        return None
    body = int(rut.split("-")[0])
    row = conn.execute("SELECT * FROM supplier WHERE rut = ?", (body,)).fetchone()
    if row is None:
        return None
    supplier = dict(row)
    names = {
        r["code"]: r["name"]
        for r in conn.execute(
            "SELECT b.code, b.name FROM buyer b WHERE b.code IN ("
            " SELECT buyer FROM supplier_buyer WHERE rut = ?"
            " UNION SELECT buyer FROM record WHERE rut = ?)",
            (body, body),
        )
    }
    buyers = [
        {
            "code": str(r["buyer"]),
            "name": names.get(r["buyer"]) or "",
            "orders": r["orders"],
            "order_value_clp": r["order_value_clp"],
            "tenders_won": r["tenders_won"],
        }
        for r in conn.execute(
            "SELECT * FROM supplier_buyer WHERE rut = ? ORDER BY order_value_clp DESC, tenders_won DESC",
            (body,),
        )
    ]
    records: dict[str, list[dict[str, Any]]] = {"order": [], "award": []}
    for r in conn.execute(
        "SELECT * FROM record WHERE rut = ? ORDER BY kind, value_clp DESC, date DESC", (body,)
    ):
        records[r["kind"]].append(
            {
                "code": r["code"],
                "date": r["date"],
                "buyer": names.get(r["buyer"]) or "" if r["buyer"] is not None else "",
                "value_clp": r["value_clp"],
                "url": record_url(r["kind"], r["code"]),
            }
        )
    records["award"].sort(key=lambda a: (a["date"] or "", a["code"]), reverse=True)
    return {
        "supplier": supplier,
        "buyers": buyers,
        "largest_orders": records["order"],
        "recent_awards": records["award"],
    }


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class ChileCompraAdapter(SourceAdapter):
    """ChileCompra public procurement (Mercado Público), via the RUT."""

    id = "chilecompra"

    lookup_derivers = (LookupDeriver(CL_RA_CODES, "cl_rut", normalise_rut),)
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="ChileCompra — Mercado Público (Chilean public procurement)",
            homepage=_HOMEPAGE,
            description=(
                "Chile's public procurement platform, run by the Dirección "
                "ChileCompra, published monthly as open data. For a company "
                "supplier: purchase orders and their value in pesos, tenders "
                "bid and won, and the public bodies it sold to, over the last "
                "twelve months. Matched on the RUT. Sole traders are not "
                "indexed. Procurement activity, not ownership."
            ),
            license=LICENSE_ID,
            attribution=(
                "Fuente: Dirección ChileCompra — monthly open data on tenders "
                "and purchase orders from Mercado Público "
                "(datos-abiertos.chilecompra.cl)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=False,
            # None, like every non-register source (TED, SEC EDGAR): ``country``
            # ties a source to its jurisdiction's knowability row, which is
            # about what a register publishes on owners — not procurement.
            country=None,
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file, as ``onrc_romania`` does.

        Without the release asset there is nothing to answer from, and a
        source that announces itself and then cannot answer would count in
        "N of N sources answered". Whether this company is a supplier is
        ``fetch``'s question.
        """
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Not name-searchable: reached from the RUT GLEIF files."""
        return []

    def _bundle(self, rut: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "rut": rut,
            "rut_display": format_rut(rut) if rut else "",
            "legal_name": legal_name or "",
            "supplier": None,
            "buyers": [],
            "largest_orders": [],
            "recent_awards": [],
            "window": "",
            "data_from": None,
            "data_to": None,
            "link": DATASET_URL,
            "not_found": False,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """What ChileCompra's files say about one company RUT."""
        try:
            rut = normalise_rut(hit_id)
        except ValueError:
            return self._bundle("", legal_name)
        if _shared_conn() is None:
            return self._bundle(rut, legal_name)
        meta = read_meta()
        declare_snapshot(meta)
        window = {
            "window": window_label(meta),
            "data_from": meta.get("data_from") or None,
            "data_to": meta.get("data_to") or None,
        }
        found = supplier_record(rut)
        if found is None:
            return self._bundle(rut, legal_name, not_found=True, is_stub=False, **window)
        bundle = self._bundle(rut, legal_name, is_stub=False, **window, **found)
        validate_raw(self.id, ChileCompraBundle, bundle)
        return bundle

