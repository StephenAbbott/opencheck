"""The PSC stream consumer — keeps the UK PSC graph current between seeds
(Phase 187).

Companies House streams every change to the persons-with-significant-
control register as it happens: one JSON line per event on a long-lived
HTTP connection, blank lines as heartbeats, each event carrying the PSC
record as the REST API would return it plus an envelope — ``event.type``
(``changed`` or ``deleted``), ``event.timepoint`` (a monotonic cursor),
``event.published_at`` — and the record's ``resource_uri``
(``/company/{number}/persons-with-significant-control/{kind}/{id}``). The
seed (Phase 186, ``psc_graph.py``) is compiled to the end of a business
day; this consumer applies what the register has published since.

What it does with an event:

* ``changed`` with a live record → upsert the row, keyed on the PSC id
  the record keeps for life (the tail of ``links.self``, which is also the
  event's ``resource_id``), through the same row builder the seed uses —
  so a stream-applied record and a snapshot record are byte-for-byte the
  same shape, including the normalised UK company number of a corporate
  PSC and the packed natures (a code the file has never seen is added to
  ``nature_codes``; readers reload the table when they meet a new byte).
* ``changed`` with ``ceased_on`` → set ``ceased_on`` on the row. The seed
  drops ceased records and the walk ignores them, so this is equivalent to
  a delete for the walk, but keeps the date for the day the store also
  serves closed statements.
* ``deleted`` (the register removed the record; no data on the event) →
  delete the row.

The cursor is persisted in the file (``meta.stream_timepoint``, written
after every batch, alongside ``stream_published_at`` — the register's own
clock, which ``/pscgraph`` shows as the graph's watermark), so a restart
resumes where it left off. Resuming is the register's rule: the timepoint
of the last event processed, or nothing — a connection without a
timepoint delivers only what happens after it. A ``416`` means the
timepoint is too old (the window is undocumented) or the client fell
behind; the consumer reconnects live and records a **gap**, which the
next daily seed closes. ``429`` waits a minute before reconnecting (the
register lengthens the penalty otherwise); any other drop — the nightly
disconnect around 02:00 included — reconnects from the cursor with a
backoff from ten seconds to five minutes. ``401`` stops: a bad key is not
retried.

A new daily seed replacing the file is the one case the cursor cannot
simply carry over: the new file's rows are as of the snapshot's compile
time, and everything the stream applied to the *old* file since then is
gone with it. The consumer notices the file's seed identity (its
``snapshot_date`` and ``built_at`` together) change between batches and
reconnects from the first timepoint it saw on the day the snapshot was
compiled — the day before its date, then the date itself; it keeps a
first-timepoint-of-the-day mark for the last few days — replaying those
events onto the new file, which the upsert makes idempotent; or, without
such a mark, live with a gap.

Everything is counted for ``/pscgraph`` (``stream``): connections and
disconnections, events by type, rows upserted / ceased / deleted /
skipped, the cursor and the register's clock, the last error, and
whether the store currently carries a gap. Aggregate only — no company
number, no PSC id, no name.

The consumer is the only writer to the file while the app runs; the boot
rule and the six-hourly asset check replace the file wholesale (a rename),
which the per-batch connection sees as the seed change above.
Readers hold ``mode=ro`` connections and see each committed batch.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import threading
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from . import psc_graph
from .identifiers import normalise_ch_company_number

log = logging.getLogger(__name__)

#: How many events to apply per write batch, and how long to hold a partial
#: batch before writing it anyway. The register's rate is a few events a
#: second at busiest; a batch is one short transaction.
BATCH_SIZE = 50
BATCH_MAX_WAIT_S = 2.0

#: Reconnect policy from the register's streaming guide.
RECONNECT_AFTER_429_S = 60.0
RECONNECT_BACKOFF_START_S = 10.0
RECONNECT_BACKOFF_MAX_S = 300.0
#: No line at all — not even a heartbeat — for this long means the
#: connection is dead behind a proxy; reconnect rather than wait forever.
READ_TIMEOUT_S = 300.0
#: How long to wait for the graph file to appear at boot before checking again.
STORE_WAIT_S = 30.0
#: First-timepoint-of-the-day marks kept, for resuming onto a new seed.
DAY_MARKS_KEPT = 4


@dataclass
class StreamState:
    enabled: bool = False
    connected: bool = False
    connects: int = 0
    disconnects: int = 0
    events: int = 0
    by_type: Counter[str] = field(default_factory=Counter)
    upserted: int = 0
    ceased: int = 0
    deleted: int = 0
    skipped: int = 0
    batches: int = 0
    timepoint: int | None = None
    resumed_from: str | None = None  # "timepoint" | "live" | "day_mark"
    gap: bool = False
    gap_since: str | None = None
    last_event_at: str | None = None
    last_published_at: str | None = None
    last_error: str | None = None
    status_codes: Counter[str] = field(default_factory=Counter)
    reseeds: int = 0


_state = StreamState()
_state_lock = threading.Lock()


def reset_state_for_tests() -> None:
    global _state
    with _state_lock:
        _state = StreamState()


def state() -> dict[str, Any]:
    with _state_lock:
        out = dict(_state.__dict__)
    out["by_type"] = dict(out["by_type"])
    out["status_codes"] = dict(out["status_codes"])
    return out


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Applying events to the file
# ---------------------------------------------------------------------------


def company_number_from_uri(uri: str) -> str | None:
    """``/company/01234567/persons-with-significant-control/…`` → ``01234567``."""
    parts = (uri or "").strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "company":
        return normalise_ch_company_number(parts[1])
    return None


def psc_id_of(event: dict[str, Any]) -> str | None:
    """The record's stable id: the tail of ``data.links.self``, else the
    event's ``resource_id``, else the tail of ``resource_uri``."""
    data = event.get("data") or {}
    self_link = str((data.get("links") or {}).get("self") or "")
    if self_link:
        return self_link.rsplit("/", 1)[-1] or None
    rid = event.get("resource_id")
    if rid:
        return str(rid)
    uri = str(event.get("resource_uri") or "")
    return uri.rsplit("/", 1)[-1] or None


@dataclass
class Applied:
    upserted: int = 0
    ceased: int = 0
    deleted: int = 0
    skipped: int = 0


def apply_events(conn: sqlite3.Connection, events: list[dict[str, Any]]) -> Applied:
    """Apply a batch of stream events to an open write connection, in one
    transaction, and return what was done. Idempotent: an event replayed
    onto a file that already reflects it changes nothing."""
    out = Applied()
    codes: dict[str, int] = {
        code: byte for byte, code in conn.execute("SELECT code_byte, code FROM nature_codes")
    }
    known_codes = len(codes)
    for event in events:
        ev_type = (event.get("event") or {}).get("type")
        psc_id = psc_id_of(event)
        if not psc_id:
            out.skipped += 1
            continue
        if ev_type == "deleted":
            conn.execute("DELETE FROM psc WHERE psc_id = ?", (psc_id,))
            out.deleted += 1
            continue
        data = event.get("data") or {}
        if not data:
            out.skipped += 1
            continue
        company_number = company_number_from_uri(str(event.get("resource_uri") or ""))
        ceased_on = data.get("ceased_on")
        if ceased_on or data.get("ceased"):
            conn.execute(
                "UPDATE psc SET ceased_on = ? WHERE psc_id = ?",
                (str(ceased_on or _now_iso()[:10]), psc_id),
            )
            out.ceased += 1
            continue
        row = psc_graph._row_from_record({"company_number": company_number, "data": data}, codes)
        if row is None:
            out.skipped += 1
            continue
        # The seed keys the row on the same id; make sure they agree even if
        # links.self were missing on the event.
        row = (psc_id,) + row[1:]
        conn.execute(psc_graph._INSERT, row)
        out.upserted += 1
    if len(codes) > known_codes:
        conn.executemany(
            "INSERT OR IGNORE INTO nature_codes (code_byte, code) VALUES (?, ?)",
            [(byte, code) for code, byte in codes.items()],
        )
    conn.commit()
    return out


def _write_conn(path: Path) -> sqlite3.Connection:
    """A write connection for one batch. The default rollback journal, as
    the GLEIF mirror's delta refresh uses: readers are ``mode=ro`` with a
    busy timeout, and a batch's commit is a fraction of a second."""
    conn = sqlite3.connect(path, timeout=30.0)
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _apply_batch(
    path: Path, events: list[dict[str, Any]], timepoint: int | None, published_at: str | None
) -> tuple[Applied, str | None]:
    """Apply a batch and persist the cursor. Returns what was applied and
    the file's seed identity — the caller compares it to notice a wholesale
    replacement."""
    conn = _write_conn(path)
    try:
        seed = seed_identity(dict(conn.execute("SELECT key, value FROM meta")))
        applied = apply_events(conn, events)
        psc_graph.write_meta(
            conn,
            **{
                psc_graph.META_STREAM_TIMEPOINT: str(timepoint) if timepoint is not None else None,
                psc_graph.META_STREAM_AT: published_at,
            },
        )
        return applied, seed
    finally:
        conn.close()


def seed_identity(meta: dict[str, Any]) -> str | None:
    """What identifies one seed of the file: its snapshot date and build
    time together. Two builds in the same second from different snapshots
    (the tests) or a rebuild from the same snapshot (a manual workflow
    re-run) both read as a new seed."""
    built_at = meta.get(psc_graph.META_BUILT_AT)
    if not built_at:
        return None
    return f"{meta.get(psc_graph.META_SNAPSHOT_DATE) or ''}|{built_at}"


def read_cursor(path: Path) -> tuple[int | None, str | None, str | None]:
    """``(stream_timepoint, stream_published_at, seed identity)`` from the file."""
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            meta = dict(conn.execute("SELECT key, value FROM meta"))
        finally:
            conn.close()
    except sqlite3.Error:
        return None, None, None
    tp = meta.get(psc_graph.META_STREAM_TIMEPOINT)
    try:
        timepoint = int(tp) if tp else None
    except ValueError:
        timepoint = None
    return timepoint, meta.get(psc_graph.META_STREAM_AT), seed_identity(meta)


# ---------------------------------------------------------------------------
# The connection
# ---------------------------------------------------------------------------


class StreamRefusedError(Exception):
    """The register answered the connect with a status the caller must act
    on: 401 (stop), 416 (resume live), 429 (wait a minute)."""

    def __init__(self, status: int) -> None:
        super().__init__(f"stream refused: HTTP {status}")
        self.status = status


LineSource = Callable[[int | None], AsyncIterator[str]]


async def _http_lines(timepoint: int | None) -> AsyncIterator[str]:
    """Lines from the register's stream, resumed from *timepoint* when given.
    Raises :class:`StreamRefusedError` on a non-200 status."""
    import httpx

    from .config import get_settings

    settings = get_settings()
    key = settings.companies_house_stream_key or ""
    params = {"timepoint": timepoint} if timepoint is not None else {}
    timeout = httpx.Timeout(connect=15.0, read=READ_TIMEOUT_S, write=15.0, pool=15.0)
    async with (
        httpx.AsyncClient(timeout=timeout) as client,
        client.stream("GET", settings.psc_stream_url, params=params, auth=(key, "")) as resp,
    ):
        if resp.status_code != 200:
            raise StreamRefusedError(resp.status_code)
        async for line in resp.aiter_lines():
            yield line


class Consumer:
    """The consumer: connects, applies batches, keeps the cursor, reconnects.

    *lines* is injectable so the tests drive it with a scripted source.
    """

    def __init__(self, path: Path, *, lines: LineSource = _http_lines) -> None:
        self.path = path
        self._lines = lines
        self.timepoint: int | None = None
        self.published_at: str | None = None
        self.seed: str | None = None  # snapshot_date|built_at of the file being fed
        self.day_marks: dict[str, int] = {}
        self._resume_how: str | None = None  # set by the reseed path for the next connection
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    # -- state helpers -----------------------------------------------------

    def _set(self, **changes: Any) -> None:
        with _state_lock:
            for k, v in changes.items():
                setattr(_state, k, v)

    def _count(self, name: str, n: int = 1) -> None:
        with _state_lock:
            setattr(_state, name, getattr(_state, name) + n)

    # -- resume logic ------------------------------------------------------

    def _load_cursor(self) -> None:
        self.timepoint, self.published_at, self.seed = read_cursor(self.path)
        self._set(timepoint=self.timepoint, last_published_at=self.published_at)

    def _note_day_mark(self, timepoint: int, published_at: str | None) -> None:
        day = (published_at or _now_iso())[:10]
        if day not in self.day_marks:
            self.day_marks[day] = timepoint
            for old in sorted(self.day_marks)[:-DAY_MARKS_KEPT]:
                del self.day_marks[old]

    def resume_point_for_new_seed(self, snapshot_date: str | None) -> tuple[int | None, str]:
        """Where to resume after the file was replaced by a new seed: the
        first timepoint seen on the day the snapshot was compiled (the day
        before its date, then the date itself), else the earliest mark held,
        else live. Replaying events the new file already reflects is harmless
        — the upsert is idempotent."""
        candidates: list[str] = []
        if snapshot_date:
            try:
                d = date.fromisoformat(snapshot_date)
                candidates = [(d - timedelta(days=1)).isoformat(), snapshot_date]
            except ValueError:
                candidates = []
        for day in candidates:
            if day in self.day_marks:
                return self.day_marks[day], "day_mark"
        if self.day_marks:
            return self.day_marks[min(self.day_marks)], "day_mark"
        return None, "live"

    # -- one connection ----------------------------------------------------

    async def run_connection(self) -> str:
        """Consume one connection until it drops, the file is reseeded, or
        stop is requested. Returns why it ended: ``dropped``, ``reseeded``,
        ``stopped``, ``refused``, ``error``."""
        batch: list[dict[str, Any]] = []
        batch_started = time.monotonic()
        resumed = self._resume_how or ("timepoint" if self.timepoint is not None else "live")
        self._resume_how = None
        self._set(resumed_from=resumed)
        if resumed == "live":
            self._set(gap=True, gap_since=_now_iso())
        self._count("connects")
        self._set(connected=True, last_error=None)
        outcome = "dropped"
        try:
            async for line in self._lines(self.timepoint):
                if self._stop.is_set():
                    outcome = "stopped"
                    break
                if not line or not line.strip():
                    # heartbeat — a chance to flush a partial batch
                    if batch and time.monotonic() - batch_started >= BATCH_MAX_WAIT_S:
                        if await self._flush(batch):
                            outcome = "reseeded"
                            break
                        batch = []
                        batch_started = time.monotonic()
                    continue
                try:
                    event = json.loads(line)
                except ValueError:
                    self._count("skipped")
                    continue
                if not isinstance(event, dict):
                    self._count("skipped")
                    continue
                batch.append(event)
                ev = event.get("event") or {}
                tp = ev.get("timepoint")
                if isinstance(tp, int):
                    self.timepoint = tp
                    self._note_day_mark(tp, ev.get("published_at"))
                if ev.get("published_at"):
                    self.published_at = str(ev["published_at"])
                with _state_lock:
                    _state.events += 1
                    _state.by_type[str(ev.get("type") or "?")] += 1
                    _state.last_event_at = _now_iso()
                if len(batch) >= BATCH_SIZE or time.monotonic() - batch_started >= BATCH_MAX_WAIT_S:
                    if await self._flush(batch):
                        outcome = "reseeded"
                        break
                    batch = []
                    batch_started = time.monotonic()
        except StreamRefusedError as exc:
            self._set(last_error=str(exc))
            with _state_lock:
                _state.status_codes[str(exc.status)] += 1
            outcome = "refused"
            raise
        except asyncio.CancelledError:
            outcome = "stopped"
            raise
        except Exception as exc:  # noqa: BLE001 — network drops, proxies, the nightly reset
            self._set(last_error=f"{type(exc).__name__}: {exc}"[:300])
            outcome = "error"
        finally:
            if batch and outcome not in ("reseeded",):
                with contextlib.suppress(Exception):
                    await self._flush(batch)
            self._set(connected=False)
            self._count("disconnects")
        return outcome

    async def _flush(self, batch: list[dict[str, Any]]) -> bool:
        """Apply *batch*; return True when the file turned out to be a new
        seed (the caller reconnects from the day mark)."""
        applied, seed = await asyncio.to_thread(
            _apply_batch, self.path, list(batch), self.timepoint, self.published_at
        )
        with _state_lock:
            _state.upserted += applied.upserted
            _state.ceased += applied.ceased
            _state.deleted += applied.deleted
            _state.skipped += applied.skipped
            _state.batches += 1
            _state.timepoint = self.timepoint
            _state.last_published_at = self.published_at
        if self.seed is not None and seed != self.seed:
            log.info("psc_stream: the graph file was reseeded (%s → %s)", self.seed, seed)
            self.seed = seed
            return True
        self.seed = seed
        return False

    # -- the loop ----------------------------------------------------------

    async def run(self) -> None:
        """Connect and reconnect until stopped."""
        self._set(enabled=True)
        self._load_cursor()
        backoff = RECONNECT_BACKOFF_START_S
        while not self._stop.is_set():
            try:
                outcome = await self.run_connection()
            except StreamRefusedError as exc:
                if exc.status == 401:
                    log.error(
                        "psc_stream: 401 from the register — the streaming key is refused; stopping"
                    )
                    self._set(enabled=False)
                    return
                if exc.status == 416:
                    log.warning(
                        "psc_stream: 416 — timepoint too old or client too slow; resuming live"
                    )
                    self.timepoint = None
                    self._set(timepoint=None)
                    continue
                wait = RECONNECT_AFTER_429_S if exc.status == 429 else backoff
                await self._sleep(wait)
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_S)
                continue
            if outcome == "stopped":
                return
            if outcome == "reseeded":
                self._count("reseeds")
                snapshot_date = None
                with contextlib.suppress(Exception):
                    snapshot_date = dict(
                        sqlite3.connect(f"file:{self.path}?mode=ro", uri=True).execute(
                            "SELECT key, value FROM meta"
                        )
                    ).get(psc_graph.META_SNAPSHOT_DATE)
                self.timepoint, how = self.resume_point_for_new_seed(snapshot_date)
                self._resume_how = how
                self._set(timepoint=self.timepoint)
                if how != "live":
                    self._set(gap=False, gap_since=None)
                continue
            if outcome == "dropped":
                backoff = RECONNECT_BACKOFF_START_S
                await self._sleep(backoff)
                continue
            # error
            await self._sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_S)

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)


# ---------------------------------------------------------------------------
# Lifespan entry point
# ---------------------------------------------------------------------------

_consumer: Consumer | None = None


async def run_loop() -> None:
    """Started by the app lifespan when a graph file and a streaming key are
    configured. Waits for the file to exist (the boot download may take a
    few minutes), then runs the consumer until cancelled."""
    global _consumer
    from .config import get_settings

    settings = get_settings()
    path = Path(settings.psc_graph_db_file) if settings.psc_graph_db_file else None
    if path is None or not settings.companies_house_stream_key or not settings.psc_stream_enabled:
        return
    while not path.exists():
        await asyncio.sleep(STORE_WAIT_S)
    _consumer = Consumer(path)
    try:
        await _consumer.run()
    finally:
        _consumer.stop()
        _consumer = None
