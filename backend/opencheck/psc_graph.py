"""The UK PSC graph — a local copy of Companies House's persons-with-
significant-control register, walked instead of fetched (Phase 186).

The register publishes a full snapshot of every PSC record every morning
(``persons-with-significant-control-snapshot-YYYY-MM-DD.zip``: one zip64
member, 2.2 GB deflated, about 12 GB of JSON lines, ~15 million records)
and streams every change to it continuously. The live corporate-PSC walk
in ``sources/companies_house.py`` costs four serial register calls per
company on a 600-per-five-minutes key — ASDA's chain is nine companies,
36 calls and 6.7 s — and Phase 184 measures exactly that. On this store
the same walk is nine indexed reads in well under a millisecond.

This module is the store and the seed:

* :func:`build_psc_graph` streams the snapshot straight through zlib into a
  compact SQLite file — the zip is never written to disk — keeping only
  what a chain walk needs: for every **active** PSC record, which company
  it controls, what kind of party it is, its name, the canonical UK company
  number when it is a UK corporate PSC (Phase 177's normaliser, applied at
  build time so the walk is a plain index lookup), its natures of control
  as one byte per code, and the dates the mapper reads. No addresses, no
  identity-verification blocks, no ceased records: 12.7 million rows,
  2.2 GB indexed, built in seven minutes. The daily GitHub Actions job
  (``refresh-psc-graph.yml``) builds it and publishes it as the
  ``psc-graph-latest`` release asset, the arrangement the GLEIF mirror
  uses.
* :class:`PscGraphStore` reads it: the active PSCs of a company, and a
  bounded walk up the corporate-PSC chain that mirrors the live adapter's
  rules (UK corporate PSCs only, depth and fan-out caps, cycles walked
  once). Phase 188 puts the walk behind the lookup; here it backs
  ``GET /pscgraph`` and the tests.
* :func:`warm_psc_graph_db` is the boot rule, the Phase 180/181 one: with
  a file path and an asset URL configured, download when the file is
  absent, replace when the asset is not the one the file came from (its
  ``Last-Modified``, stamped into ``meta``), otherwise keep. A file alone
  is used as found; a URL alone does nothing — a 2.2 GB file belongs on
  the persistent disk, not in ``/tmp`` on every boot. :func:`refresh_once`
  re-runs that check on a timer, so a new daily asset lands without a
  deploy, and :func:`state` says what happened for ``/pscgraph``.

The stream (Phase 187) will keep the file current between seeds; it writes
through :func:`write_meta`'s ``stream_timepoint`` and upserts rows keyed on
``psc_id``, the tail of the record's ``links.self`` — the one identifier a
PSC record keeps for life, and the key the stream's events carry.

Licence: Companies House data is published under the Open Government
Licence v3.0; ``ATTRIBUTIONS.md`` carries the attribution.
"""

from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
import struct
import threading
import time
import zlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Any

from .identifiers import ch_identification_is_uk, normalise_ch_company_number

log = logging.getLogger(__name__)

SCHEMA_VERSION = "1"

#: Where the register publishes the daily snapshot. The date is the day the
#: file was published; it is compiled to the end of the previous business day.
SNAPSHOT_URL_TEMPLATE = (
    "https://download.companieshouse.gov.uk/persons-with-significant-control-snapshot-{date}.zip"
)

#: The API's four PSC record kinds, one character each in the store.
KIND_CODES = {
    "individual-person-with-significant-control": "i",
    "corporate-entity-person-with-significant-control": "c",
    "legal-person-person-with-significant-control": "l",
    "super-secure-person-with-significant-control": "s",
}
KIND_NAMES = {code: kind for kind, code in KIND_CODES.items()}

#: Kinds the walk follows to another company — the live adapter's rule
#: (``"corporate" in kind or "legal-person" in kind``).
FOLLOWABLE_KINDS = frozenset({"c", "l"})

SCHEMA = """
CREATE TABLE IF NOT EXISTS psc (
    psc_id         TEXT PRIMARY KEY,   -- tail of links.self: stable per PSC record
    company_number TEXT NOT NULL,      -- the company this PSC controls (canonical)
    kind           TEXT NOT NULL,      -- i / c / l / s (KIND_CODES)
    name           TEXT,
    reg_number     TEXT,               -- canonical UK number of a corporate PSC, else NULL
    natures        BLOB,               -- one byte per nature-of-control code (nature_codes)
    notified_on    TEXT,
    ceased_on      TEXT,               -- NULL for every row a build writes; the stream may set it
    dob            TEXT,               -- YYYY-MM (or YYYY) for individuals
    nationality    TEXT,
    country_of_residence TEXT
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_psc_company ON psc(company_number);
CREATE INDEX IF NOT EXISTS idx_psc_reg ON psc(reg_number) WHERE reg_number IS NOT NULL;
CREATE TABLE IF NOT EXISTS nature_codes (
    code_byte INTEGER PRIMARY KEY,
    code      TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

#: ``meta`` keys. ``built_at`` is what the boot rule falls back to for an
#: unstamped file (the same rule as the GLEIF mirror); ``asset_last_modified``
#: is the stamp; ``stream_timepoint`` is Phase 187's.
META_BUILT_AT = "built_at"
META_SNAPSHOT_DATE = "snapshot_date"
META_SOURCE_URL = "source_url"
META_ROW_COUNT = "row_count"
META_COMPANY_COUNT = "company_count"
META_EDGE_COUNT = "edge_count"
META_STREAM_TIMEPOINT = "stream_timepoint"
META_STREAM_AT = "stream_published_at"


# ---------------------------------------------------------------------------
# Building the store from the snapshot
# ---------------------------------------------------------------------------


def snapshot_url(date: str) -> str:
    """The snapshot URL for a ``YYYY-MM-DD`` date."""
    return SNAPSHOT_URL_TEMPLATE.format(date=date)


def latest_snapshot_url(*, today: datetime | None = None, lookback_days: int = 7) -> str:
    """The most recent snapshot the register actually has, found by ``HEAD``
    from today backwards: the file is dated by its publication day and is not
    published on every calendar day (weekends and bank holidays), so the
    builder tries up to *lookback_days* dates before giving up."""
    import httpx

    day = (today or datetime.now(UTC)).date()
    tried: list[str] = []
    for back in range(lookback_days + 1):
        date = (day - timedelta(days=back)).isoformat()
        url = snapshot_url(date)
        tried.append(date)
        try:
            resp = httpx.head(url, timeout=30.0, follow_redirects=True)
        except httpx.HTTPError as exc:
            log.warning("psc_graph: HEAD %s failed: %s", url, exc)
            continue
        if resp.status_code == 200:
            return url
    raise RuntimeError(f"no PSC snapshot found for any of {tried}")


def snapshot_date_from_url(url: str) -> str | None:
    """``2026-09-08`` out of the snapshot URL, or ``None`` for another URL."""
    stem = url.rsplit("/", 1)[-1]
    prefix, suffix = "persons-with-significant-control-snapshot-", ".zip"
    if stem.startswith(prefix) and stem.endswith(suffix):
        return stem[len(prefix) : -len(suffix)]
    return None


def _open_snapshot(url: str) -> contextlib.AbstractContextManager[IO[bytes]]:
    """A binary stream of the snapshot zip — a local file for ``file://`` (the
    tests, and an operator with the zip already downloaded), else an HTTP
    stream that is never written to disk."""
    if url.startswith("file://"):
        return open(url[len("file://") :], "rb")

    import httpx

    @contextlib.contextmanager
    def _http() -> Iterator[IO[bytes]]:
        with httpx.stream("GET", url, timeout=600.0, follow_redirects=True) as resp:
            resp.raise_for_status()

            class _Reader:
                def __init__(self) -> None:
                    self._chunks = resp.iter_bytes(1 << 20)
                    self._buf = b""

                def read(self, n: int = -1) -> bytes:
                    while n < 0 or len(self._buf) < n:
                        try:
                            self._buf += next(self._chunks)
                        except StopIteration:
                            break
                    if n < 0:
                        out, self._buf = self._buf, b""
                    else:
                        out, self._buf = self._buf[:n], self._buf[n:]
                    return out

            yield _Reader()  # type: ignore[misc]

    return _http()


def iter_snapshot_records(stream: IO[bytes]) -> Iterator[dict[str, Any]]:
    """Yield each ``{"company_number", "data"}`` record of the snapshot zip
    read from *stream*, inflating on the fly.

    The zip holds one deflate member. Its local header's sizes are the zip64
    placeholders (``0xFFFFFFFF``), so they are not trusted: the member is
    inflated until the deflate stream ends, and the central directory that
    follows is left unread.
    """
    header = stream.read(30)
    if len(header) < 30:
        raise ValueError("snapshot: truncated zip header")
    sig, _ver, _flag, method, _mt, _md, _crc, _csz, _usz, fnl, exl = struct.unpack(
        "<IHHHHHIIIHH", header
    )
    if sig != 0x04034B50:
        raise ValueError("snapshot: not a zip local file header")
    if method != 8:
        raise ValueError(f"snapshot: expected a deflate member, got method {method}")
    stream.read(fnl + exl)
    inflate = zlib.decompressobj(-15)
    buf = b""
    while not inflate.eof:
        chunk = stream.read(1 << 20)
        if not chunk:
            break
        buf += inflate.decompress(chunk)
        *lines, buf = buf.split(b"\n")
        for line in lines:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except ValueError:
                log.debug("psc_graph: skipping an unparseable snapshot line")
    if buf.strip():
        with contextlib.suppress(ValueError):
            yield json.loads(buf)


def _row_from_record(record: dict[str, Any], codes: dict[str, int]) -> tuple[Any, ...] | None:
    """A ``psc`` row from one snapshot record, or ``None`` to skip it: a
    ceased record, an unknown kind, or one without the identifiers the walk
    keys on. *codes* is grown with any nature-of-control code not yet seen."""
    data = record.get("data") or {}
    if data.get("ceased_on") or data.get("ceased"):
        return None
    kind = KIND_CODES.get(data.get("kind") or "")
    if kind is None:
        return None
    company_number = normalise_ch_company_number(record.get("company_number"))
    if company_number is None:
        return None
    self_link = str((data.get("links") or {}).get("self") or "")
    psc_id = self_link.rsplit("/", 1)[-1] or str(data.get("etag") or "")
    if not psc_id:
        return None
    ident = data.get("identification") or {}
    reg_number = None
    if kind in FOLLOWABLE_KINDS and ch_identification_is_uk(ident):
        reg_number = normalise_ch_company_number(ident.get("registration_number"))
    natures = bytes(
        codes.setdefault(code, len(codes)) for code in (data.get("natures_of_control") or [])
    )
    dob = data.get("date_of_birth") or {}
    if dob.get("year") and dob.get("month"):
        dob_text: str | None = f"{int(dob['year']):04d}-{int(dob['month']):02d}"
    elif dob.get("year"):
        dob_text = f"{int(dob['year']):04d}"
    else:
        dob_text = None
    return (
        psc_id,
        company_number,
        kind,
        data.get("name"),
        reg_number,
        natures,
        data.get("notified_on"),
        None,
        dob_text,
        data.get("nationality"),
        data.get("country_of_residence"),
    )


_INSERT = "INSERT OR REPLACE INTO psc VALUES (?,?,?,?,?,?,?,?,?,?,?)"


def write_meta(conn: sqlite3.Connection, **values: str | None) -> None:
    conn.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [(k, v) for k, v in values.items() if v is not None],
    )
    conn.commit()


def build_psc_graph(
    out: Path,
    *,
    source_url: str,
    snapshot_date: str | None = None,
    progress_every: int = 1_000_000,
) -> dict[str, int]:
    """Build the store at *out* from the snapshot at *source_url*.

    Writes to ``<out>.build`` and renames at the end, so a half-built file
    never becomes the live one. Returns the counts that go into ``meta``.
    """
    tmp = out.with_suffix(out.suffix + ".build")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(tmp)
    conn.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA page_size=4096;")
    conn.executescript(SCHEMA)
    codes: dict[str, int] = {}
    rows = 0
    batch: list[tuple[Any, ...]] = []
    started = time.monotonic()
    with _open_snapshot(source_url) as stream:
        for record in iter_snapshot_records(stream):
            row = _row_from_record(record, codes)
            if row is None:
                continue
            batch.append(row)
            rows += 1
            if len(batch) >= 20_000:
                conn.executemany(_INSERT, batch)
                batch.clear()
            if progress_every and rows % progress_every == 0:
                log.info("psc_graph: %d rows in %.0fs", rows, time.monotonic() - started)
    if batch:
        conn.executemany(_INSERT, batch)
    conn.executemany(
        "INSERT OR REPLACE INTO nature_codes (code_byte, code) VALUES (?, ?)",
        [(byte, code) for code, byte in codes.items()],
    )
    conn.commit()
    counts = {
        "rows": rows,
        "companies": conn.execute("SELECT COUNT(DISTINCT company_number) FROM psc").fetchone()[0],
        "edges": conn.execute("SELECT COUNT(*) FROM psc WHERE reg_number IS NOT NULL").fetchone()[
            0
        ],
    }
    write_meta(
        conn,
        schema_version=SCHEMA_VERSION,
        **{
            META_BUILT_AT: datetime.now(UTC).isoformat(timespec="seconds"),
            META_SNAPSHOT_DATE: snapshot_date or snapshot_date_from_url(source_url),
            META_SOURCE_URL: source_url,
            META_ROW_COUNT: str(counts["rows"]),
            META_COMPANY_COUNT: str(counts["companies"]),
            META_EDGE_COUNT: str(counts["edges"]),
        },
    )
    conn.close()
    tmp.replace(out)
    log.info(
        "psc_graph: built %s — %d rows, %d companies, %d UK corporate edges in %.0fs",
        out,
        counts["rows"],
        counts["companies"],
        counts["edges"],
        time.monotonic() - started,
    )
    return counts


# ---------------------------------------------------------------------------
# Reading the store
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PscRow:
    """One active PSC record, as the walk and the mapper-shaped bundle read it."""

    psc_id: str
    company_number: str
    kind: str  # the API kind string, e.g. "corporate-entity-person-with-significant-control"
    name: str | None
    reg_number: str | None
    natures: tuple[str, ...]
    notified_on: str | None
    ceased_on: str | None
    dob: str | None
    nationality: str | None
    country_of_residence: str | None

    @property
    def followable(self) -> bool:
        return self.reg_number is not None and self.ceased_on is None


@dataclass
class Walk:
    """What a bounded chain walk found: the companies reached (canonical
    number → the PSC rows that name them), in discovery order, and why any
    corporate PSC was not followed — the live adapter's vocabulary."""

    subject: str
    companies: dict[str, list[PscRow]] = field(default_factory=dict)
    depth_reached: int = 0
    unfollowed: list[dict[str, Any]] = field(default_factory=list)
    reads: int = 0


class PscGraphStore:
    """Read-only accessor over ``psc_graph.sqlite`` — one locked connection,
    ``mode=ro`` so a writer (the stream, Phase 187) is seen without reopening.
    A wholesale replacement of the file needs :func:`reload_store`."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            f"file:{path}?mode=ro", uri=True, check_same_thread=False, timeout=10.0
        )
        self._conn.row_factory = sqlite3.Row
        self._codes: dict[int, str] = {
            int(r["code_byte"]): r["code"]
            for r in self._conn.execute("SELECT code_byte, code FROM nature_codes")
        }

    def close(self) -> None:
        with self._lock, contextlib.suppress(sqlite3.Error):
            self._conn.close()

    def meta(self) -> dict[str, str]:
        with self._lock:
            return dict(self._conn.execute("SELECT key, value FROM meta"))

    @property
    def schema_version(self) -> str | None:
        return self.meta().get("schema_version")

    def _codes_for(self, packed: bytes) -> tuple[str, ...]:
        """Decode packed nature bytes; a byte this reader has not seen (the
        stream, Phase 187, may add a code after the reader opened) reloads
        the table once before giving up on it."""
        if any(b not in self._codes for b in packed):
            with self._lock:
                self._codes = {
                    int(r["code_byte"]): r["code"]
                    for r in self._conn.execute("SELECT code_byte, code FROM nature_codes")
                }
        return tuple(self._codes.get(b, f"unknown-code-{b}") for b in packed)

    def _row(self, r: sqlite3.Row) -> PscRow:
        natures = self._codes_for(r["natures"] or b"")
        return PscRow(
            psc_id=r["psc_id"],
            company_number=r["company_number"],
            kind=KIND_NAMES.get(r["kind"], r["kind"]),
            name=r["name"],
            reg_number=r["reg_number"],
            natures=natures,
            notified_on=r["notified_on"],
            ceased_on=r["ceased_on"],
            dob=r["dob"],
            nationality=r["nationality"],
            country_of_residence=r["country_of_residence"],
        )

    def pscs(self, company_number: str, *, include_ceased: bool = False) -> list[PscRow]:
        """The PSC records of one company (canonical number), active ones
        unless *include_ceased*."""
        number = normalise_ch_company_number(company_number)
        if number is None:
            return []
        sql = "SELECT * FROM psc WHERE company_number = ?"
        if not include_ceased:
            sql += " AND ceased_on IS NULL"
        with self._lock:
            rows = self._conn.execute(
                sql + " ORDER BY notified_on, name, psc_id", (number,)
            ).fetchall()
        return [self._row(r) for r in rows]

    def walk(self, subject: str, *, max_depth: int = 6, max_related: int = 25) -> Walk:
        """Follow UK corporate PSCs up from *subject* — the live adapter's walk
        (``_fetch_company_data``) on the store: active corporate / legal-person
        PSCs with a canonical UK number are followed, up to *max_depth* hops
        and *max_related* companies, cycles and shared parents once; every
        corporate PSC not followed is listed with the adapter's reason. The
        subject's own PSCs are read too, so a caller has the whole chain."""
        number = normalise_ch_company_number(subject)
        walk = Walk(subject=number or str(subject))
        if number is None:
            return walk
        visited = {number}
        frontier = [number]
        depth = 0
        while frontier:
            next_frontier: list[str] = []
            for company in frontier:
                rows = self.pscs(company)
                walk.reads += 1
                if company != number:
                    walk.companies.setdefault(company, [])
                for row in rows:
                    if company == number:
                        walk.companies.setdefault(number, []).append(row)
                    else:
                        walk.companies[company].append(row)
                    if row.kind.split("-")[0] not in ("corporate", "legal"):
                        continue
                    if row.reg_number is None:
                        walk.unfollowed.append(
                            {
                                "subject_company_number": company,
                                "name": row.name,
                                "reason": "not_uk_registered_or_not_a_company_number",
                            }
                        )
                        continue
                    if row.reg_number in visited:
                        continue
                    if depth >= max_depth:
                        walk.unfollowed.append(
                            {
                                "subject_company_number": company,
                                "name": row.name,
                                "registration_number": row.reg_number,
                                "reason": "max_depth_reached",
                            }
                        )
                        continue
                    if len(visited) - 1 >= max_related:
                        walk.unfollowed.append(
                            {
                                "subject_company_number": company,
                                "name": row.name,
                                "registration_number": row.reg_number,
                                "reason": "related_companies_cap_reached",
                            }
                        )
                        continue
                    visited.add(row.reg_number)
                    next_frontier.append(row.reg_number)
            if not next_frontier:
                break
            depth += 1
            walk.depth_reached = depth
            frontier = next_frontier
        return walk


# ---------------------------------------------------------------------------
# The process-wide store, the boot rule and the periodic asset check
# ---------------------------------------------------------------------------

_store: PscGraphStore | None = None
_store_lock = threading.Lock()


def _db_path() -> Path | None:
    from .config import get_settings

    configured = get_settings().psc_graph_db_file
    return Path(configured) if configured else None


def get_store() -> PscGraphStore | None:
    """The store for the configured file, opened once; ``None`` when no file
    is configured or the file is not there (yet)."""
    global _store
    path = _db_path()
    if path is None or not path.exists():
        return None
    with _store_lock:
        if _store is None or _store.path != path:
            try:
                _store = PscGraphStore(path)
            except sqlite3.Error as exc:
                log.warning("psc_graph: could not open %s: %s", path, exc)
                return None
        return _store


def reload_store() -> None:
    """Drop the open store so the next :func:`get_store` reopens the file —
    after a wholesale replacement, whose new inode an open connection would
    never see."""
    global _store
    with _store_lock:
        old, _store = _store, None
    if old is not None:
        old.close()


def reset_store_for_tests() -> None:
    reload_store()
    reset_state_for_tests()


@dataclass
class RefreshState:
    enabled: bool = False
    runs: int = 0
    replaced: int = 0
    failures: int = 0
    last_checked_at: str | None = None
    last_outcome: str | None = None
    last_replaced_at: str | None = None
    last_error: str | None = None


_state = RefreshState()
_state_lock = threading.Lock()
_run_lock = threading.Lock()

OUTCOMES = ("downloaded", "replaced", "kept", "skipped_not_configured", "failed")


def reset_state_for_tests() -> None:
    global _state
    with _state_lock:
        _state = RefreshState()


def state() -> dict[str, Any]:
    with _state_lock:
        return dict(_state.__dict__)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def warm_psc_graph_db() -> dict[str, Any]:
    """The boot rule (and the body of every periodic check). Non-fatal.

    File and URL configured: download when the file is absent, replace when
    the asset is not the one the file came from, keep otherwise — the Phase
    181 ``asset_check``. File alone: used as found. URL alone, or nothing:
    nothing to do. Returns a one-line summary for the boot log; the outcome
    is also recorded in :func:`state`.
    """
    from .config import get_settings
    from .entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = _db_path()
    url = settings.psc_graph_db_url
    if path is None:
        _record("skipped_not_configured")
        return {"psc_graph": "not configured (OPENCHECK_PSC_GRAPH_DB_FILE unset)"}
    if not url:
        _record("kept" if path.exists() else "skipped_not_configured")
        return {"psc_graph": f"{'present' if path.exists() else 'absent'}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                _record("kept")
                return {"psc_graph": f"already present: {path}"}
            log.info("psc_graph: the release asset is not the one on disk; replacing")
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reload_store()
        _record(outcome)
        return {"psc_graph": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — a store a day old beats none
        log.warning("psc_graph: asset check/download failed: %s", exc)
        _record("failed", error=f"{type(exc).__name__}: {exc}"[:300])
        return {"psc_graph": f"failed: {exc}"}


def _record(outcome: str, *, error: str | None = None) -> None:
    with _state_lock:
        _state.runs += 1
        _state.last_checked_at = _now_iso()
        _state.last_outcome = outcome
        _state.last_error = error
        if outcome in ("downloaded", "replaced"):
            _state.replaced += 1
            _state.last_replaced_at = _state.last_checked_at
        if outcome == "failed":
            _state.failures += 1


def refresh_once() -> str:
    """One asset check, at most one at a time. Returns the outcome."""
    if not _run_lock.acquire(blocking=False):
        return "kept"
    try:
        warm_psc_graph_db()
        return state()["last_outcome"] or "kept"
    finally:
        _run_lock.release()


async def refresh_loop(interval_s: float) -> None:
    """Re-run the asset check every *interval_s* seconds (the daily seed
    lands without a deploy). Started by the app lifespan; the first run is one
    interval after boot, since boot already did one."""
    import asyncio

    with _state_lock:
        _state.enabled = True
    while True:
        await asyncio.sleep(interval_s)
        try:
            await asyncio.to_thread(refresh_once)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("psc_graph: refresh loop iteration failed: %s", exc)


def summary() -> dict[str, Any]:
    """The ``/pscgraph`` payload: whether a store is open, its ``meta`` (about
    the file — counts and dates, never a company or a person), and the
    refresh state. Aggregate only."""
    from .config import get_settings

    settings = get_settings()
    store = get_store()
    out: dict[str, Any] = {
        "configured": bool(settings.psc_graph_db_file),
        "available": store is not None,
        "flags": {"ch_graph_first": bool(settings.ch_graph_first)},
        "store": None,
        "refresh": state(),
    }
    # Phase 187: the stream consumer's counters ride on the same payload.
    from . import psc_stream

    out["stream"] = psc_stream.state()
    if store is not None:
        meta = store.meta()
        out["store"] = {
            "schema_version": meta.get("schema_version"),
            "snapshot_date": meta.get(META_SNAPSHOT_DATE),
            "built_at": meta.get(META_BUILT_AT),
            "asset_last_modified": meta.get("asset_last_modified"),
            "row_count": _int(meta.get(META_ROW_COUNT)),
            "company_count": _int(meta.get(META_COMPANY_COUNT)),
            "edge_count": _int(meta.get(META_EDGE_COUNT)),
            "stream_timepoint": _int(meta.get(META_STREAM_TIMEPOINT)),
            "stream_published_at": meta.get(META_STREAM_AT),
        }
    return out


def _int(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None
