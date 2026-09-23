"""saved_reports — a record of exactly what OpenCheck showed, on a given date.

Phase 216. A ``?lei=`` link is a *query*, not a *record*: it re-runs the
pipeline, so two people opening it a day apart can see different findings.
A saved report is the record — opt-in (a reader presses Save; nothing is
saved automatically, so CC-BY-NC data is never stored at volume for no
reader), addressable, and verifiable.

What is saved
-------------

The **event stream** of a completed lookup, verbatim — the same list the
replay cache holds and the React report is built from — plus, when the reader
generated them, the narrative and a frozen copy of its disposition sheet, and
the licence assessment as it stood at save time. The events are the payload
because the report page is a fold over them: replaying them through the same
handlers is what makes a saved report render as the same report, and
``routers.lookup.fold_lookup_events`` folds them into the same
``LookupResponse`` the PDF, Markdown and MCP views use.

Where it comes from — never from the client
-------------------------------------------

A save names a run (``lei`` + the ``run_completed_at`` its ``done`` event
carried) and the server copies **its own** held run out of the replay cache
(:func:`routers.lookup.replay_entry`). A client never posts a payload, so a
content hash means something. A run that has aged out of the 15-minute replay
window, or that a per-source retry or restart cleared, is not saved: the
reader is told to run the check again (Stephen, 16 Sept 2026 — never a silent
re-run whose findings the reader has not seen). A narrative is saved only if
this server generated it from that same run (``routers.narrative.held_narrative``).

Integrity
---------

The payload is serialised once, canonically (sorted keys, no insignificant
whitespace, UTF-8), and ``content_hash`` is the SHA-256 of those bytes. The
bytes are what is stored (gzipped) and what ``GET /saved-reports/{id}.json``
serves, so ``shasum -a 256`` on the download reproduces the hash printed on
the page and in the PDF footer. The hash is re-checked on every read; a
mismatch is refused, not rendered.

Two ids, two capabilities
-------------------------

``report_id`` — ``secrets.token_urlsafe(16)``, unguessable — is the **read**
capability that gets shared. ``manage_token`` — shown once, stored only as its
SHA-256 — is the **manage** capability (extend, delete) kept by the saver's
browser. There are no accounts.

Retention
---------

``OPENCHECK_SAVED_REPORTS_RETENTION_DAYS`` (90) from the save; extending resets
the clock to that many days from now. A background task prunes expired rows;
a read of an expired row answers 410 and deletes it.

Scope (v1)
----------

QuickCheck and FullCheck (both are folds over the lookup events), the
narrative and its dispositions. Background check, Subsidiaries, History, ESG,
securities and NZ associations fetch live and are not part of a saved report
— ``scope`` in the payload says so, for the page to word.

Dispositions
------------

The live analyst sheets (``dispositions.py``) live in this same file when it
is configured, so a sign-off survives a deploy; the JSON files under the data
root remain the fallback when it is not.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger("opencheck.saved_reports")

#: Payload schema. Bump when the payload's shape changes; a reader must keep
#: rendering every schema it has ever written.
SCHEMA = "opencheck.saved_report/1"

#: ``secrets.token_urlsafe(16)`` → 22 characters of the URL-safe alphabet.
REPORT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{22}$")
_LEI_RE = re.compile(r"^[A-Z0-9]{20}$")

#: What a v1 saved report holds, and what it deliberately does not — the
#: tabs and sections that fetch live data rather than folding the lookup.
SCOPE: dict[str, list[str]] = {
    "included": ["quick", "full", "narrative", "dispositions", "licensing"],
    "excluded": ["background", "subsidiaries", "history", "esg", "securities", "nz_associations"],
}


# ---------------------------------------------------------------------------
# Errors — each is one refusal the router words
# ---------------------------------------------------------------------------


class SavedReportError(Exception):
    status = 400
    code = "saved_report_error"


class RunNotHeldError(SavedReportError):
    status = 409
    code = "run_not_held"


class NarrativeNotHeldError(SavedReportError):
    status = 409
    code = "narrative_not_held"


class CapExceededError(SavedReportError):
    status = 507
    code = "cap_exceeded"


class NotFoundError(SavedReportError):
    status = 404
    code = "not_found"


class ExpiredError(SavedReportError):
    status = 410
    code = "expired"


class ClientQuotaError(SavedReportError):
    """Phase 234: this client has saved as many reports as it may for now.

    The instance cap (``saved_reports_max_total``) is shared by everyone, so
    without a per-client quota one address could fill it on its own."""

    status = 429
    code = "client_quota"

    def __init__(self, message: str, retry_after_s: float):
        super().__init__(message)
        self.retry_after_s = max(1, int(retry_after_s + 0.999))


def _save_quota() -> Any:
    from .config import get_settings
    from .lookup_budget import Quota

    global _SAVE_QUOTA
    if _SAVE_QUOTA is None:
        _SAVE_QUOTA = Quota("saved-reports", lambda: get_settings().saved_reports_per_ip)
    return _SAVE_QUOTA


_SAVE_QUOTA: Any = None


def check_client_quota() -> None:
    """Refuse (429) when the current client has used its save quota. Checked
    before the save and spent after it (:func:`spend_client_quota`), so a
    refused or failed save costs nothing."""
    quota = _save_quota()
    wait = quota.retry_after()
    if wait is not None:
        raise ClientQuotaError(
            f"This address has saved as many reports as OpenCheck allows "
            f"({quota.describe()}). Saved reports are shared capacity; try again later.",
            wait,
        )


def spend_client_quota() -> None:
    _save_quota().hit()


class ForbiddenError(SavedReportError):
    status = 403
    code = "forbidden"


class IntegrityError(SavedReportError):
    status = 500
    code = "integrity"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def token_hash(token: str) -> str:
    return hashlib.sha256(token.strip().encode("utf-8")).hexdigest()


def new_report_id() -> str:
    return secrets.token_urlsafe(16)


def new_manage_token() -> str:
    return secrets.token_urlsafe(24)


def canonical_bytes(obj: Any) -> bytes:
    """The one serialisation that is hashed, stored and served."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def content_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def serialise_events(events: Iterable[tuple[str, Any]]) -> list[dict[str, Any]]:
    """Replay-cache events → ``[{"event", "data"}]``, JSON-shaped.

    ``hit`` payloads are ``SourceHit`` models in-process (the SSE route sends
    them with ``model_dump_json``); everything else is already JSON-safe (the
    route sends it with ``json.dumps``). The internal ``deepen_result`` /
    ``deepen_error`` events are kept: the stream skips them, but the fold needs
    ``deepen_result`` for the BODS statements the exports carry.
    """
    out: list[dict[str, Any]] = []
    for name, payload in events:
        if hasattr(payload, "model_dump"):
            data = payload.model_dump(mode="json")
        else:
            data = json.loads(json.dumps(payload))
        out.append({"event": name, "data": data})
    return out


def deserialise_events(events: Iterable[dict[str, Any]]) -> list[tuple[str, Any]]:
    return [(e["event"], e["data"]) for e in events]


def _contributing_ids(response: Any) -> list[str]:
    """Same rule as ``/export``: the sources that actually returned data."""
    return sorted({h.source_id for h in response.hits if not getattr(h, "is_stub", False)})


def _generator() -> dict[str, Any]:
    from . import __version__

    return {
        "name": "OpenCheck",
        "version": __version__,
        # Render sets this on every deploy; absent locally.
        "commit": os.environ.get("RENDER_GIT_COMMIT") or None,
    }


def build_payload(
    *,
    report_id: str,
    lei: str,
    deepen_top: int,
    run_completed_at: str,
    saved_at: str,
    events: list[tuple[str, Any]],
    narrative: dict[str, Any] | None,
    dispositions: dict[str, Any] | None,
) -> dict[str, Any]:
    """The frozen record. Everything in it is covered by ``content_hash``."""
    from .licensing import assess
    from .routers.lookup import fold_lookup_events

    folded = fold_lookup_events(lei, events)
    return {
        "schema": SCHEMA,
        "report_id": report_id,
        "lei": lei,
        "legal_name": folded.legal_name,
        "jurisdiction": folded.jurisdiction,
        "deepen_top": deepen_top,
        # Clock 5 (the four-clock rule of Phases 99/100 gains one): when the
        # run finished, and when a reader chose to keep it.
        "run_completed_at": run_completed_at,
        "saved_at": saved_at,
        "events": serialise_events(events),
        "narrative": narrative,
        "dispositions": dispositions,
        # As it stood on the day: a source's terms can change later.
        "licensing": assess(_contributing_ids(folded)).model_dump(mode="json"),
        "scope": SCOPE,
        "generator": _generator(),
    }


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    lei TEXT NOT NULL,
    legal_name TEXT,
    content_hash TEXT NOT NULL,
    saved_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    extended_at TEXT,
    manage_token_hash TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    payload_gz BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS reports_expires ON reports (expires_at);
CREATE TABLE IF NOT EXISTS dispositions (
    lei TEXT NOT NULL,
    run_id TEXT NOT NULL,
    record_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (lei, run_id)
);
"""

_META_COLUMNS = "report_id, lei, legal_name, content_hash, saved_at, expires_at, extended_at, size_bytes"


class SavedReportsStore:
    """The SQLite file. Short connections per call, as the watchlist store:
    request handlers and the pruning task both write."""

    def __init__(self, path: Path, *, retention_days: int, max_total: int):
        self.path = Path(path)
        self.retention_days = retention_days
        self.max_total = max_total
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript(DDL)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            yield conn
        finally:
            conn.close()

    # -- reports -----------------------------------------------------------

    def insert(
        self,
        *,
        report_id: str,
        lei: str,
        legal_name: str | None,
        data: bytes,
        saved_at: datetime,
        manage_token: str,
    ) -> dict[str, Any]:
        expires_at = saved_at + timedelta(days=self.retention_days)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                total = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
                if total >= self.max_total:
                    raise CapExceededError(
                        f"This instance holds its maximum of {self.max_total} saved reports."
                    )
                conn.execute(
                    f"INSERT INTO reports ({_META_COLUMNS}, manage_token_hash, payload_gz) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        report_id,
                        lei,
                        legal_name,
                        content_hash(data),
                        _iso(saved_at),
                        _iso(expires_at),
                        None,
                        len(data),
                        token_hash(manage_token),
                        gzip.compress(data, mtime=0),
                    ),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return self.meta(report_id) or {}

    def meta(self, report_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                f"SELECT {_META_COLUMNS} FROM reports WHERE report_id = ?", (report_id,)
            ).fetchone()
        return dict(row) if row else None

    def read(self, report_id: str) -> tuple[dict[str, Any], bytes, str] | None:
        """``(meta, payload bytes, manage_token_hash)`` or ``None``."""
        with self._conn() as conn:
            row = conn.execute(
                f"SELECT {_META_COLUMNS}, manage_token_hash, payload_gz FROM reports WHERE report_id = ?",
                (report_id,),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        blob = d.pop("payload_gz")
        th = d.pop("manage_token_hash")
        return d, gzip.decompress(blob), th

    def set_expiry(self, report_id: str, expires_at: datetime, extended_at: datetime) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE reports SET expires_at = ?, extended_at = ? WHERE report_id = ?",
                (_iso(expires_at), _iso(extended_at), report_id),
            )

    def delete(self, report_id: str) -> bool:
        with self._conn() as conn:
            return conn.execute("DELETE FROM reports WHERE report_id = ?", (report_id,)).rowcount > 0

    def prune_expired(self, now: datetime | None = None) -> int:
        cutoff = _iso(now or datetime.now(UTC))
        with self._conn() as conn:
            return conn.execute("DELETE FROM reports WHERE expires_at <= ?", (cutoff,)).rowcount

    def count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]

    # -- live disposition sheets -----------------------------------------------

    def get_disposition(self, lei: str, run_id: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT record_json FROM dispositions WHERE lei = ? AND run_id = ?", (lei, run_id)
            ).fetchone()
        return row[0] if row else None

    def put_disposition(self, lei: str, run_id: str, record_json: str, updated_at: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO dispositions (lei, run_id, record_json, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(lei, run_id) DO UPDATE SET record_json = excluded.record_json, "
                "updated_at = excluded.updated_at",
                (lei, run_id, record_json, updated_at),
            )


_store: SavedReportsStore | None = None
_store_lock = threading.Lock()


def get_store() -> SavedReportsStore | None:
    """The configured store, or ``None`` when ``OPENCHECK_SAVED_REPORTS_DB_FILE``
    is unset — saved reports are then off and their routes say so."""
    global _store
    from .config import get_settings

    s = get_settings()
    path = s.saved_reports_db_file
    if not path:
        return None
    with _store_lock:
        if (
            _store is None
            or _store.path != Path(path)
            or _store.retention_days != s.saved_reports_retention_days
            or _store.max_total != s.saved_reports_max_total
        ):
            _store = SavedReportsStore(
                Path(path),
                retention_days=s.saved_reports_retention_days,
                max_total=s.saved_reports_max_total,
            )
        return _store


def reset_for_tests() -> None:
    global _store
    with _store_lock:
        _store = None


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _public_meta(meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_id": meta["report_id"],
        "lei": meta["lei"],
        "legal_name": meta.get("legal_name"),
        "content_hash": meta["content_hash"],
        "saved_at": meta["saved_at"],
        "expires_at": meta["expires_at"],
        "extended_at": meta.get("extended_at"),
        "size_bytes": meta.get("size_bytes"),
        "report_path": f"/report/{meta['report_id']}",
    }


def save_from_replay(
    store: SavedReportsStore,
    *,
    lei: str,
    run_completed_at: str,
    deepen_top: int = 5,
    narrative_run_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Freeze the held run ``(lei, deepen_top)`` completed at
    ``run_completed_at``. Returns the public metadata plus ``manage_token`` —
    the only time that token is seen."""
    from .dispositions import load_dispositions
    from .routers.lookup import replay_entry
    from .routers.narrative import held_narrative

    norm_lei = (lei or "").strip().upper()
    if not _LEI_RE.match(norm_lei):
        raise SavedReportError("lei must be a 20-character alphanumeric LEI")

    entry = replay_entry(norm_lei, deepen_top)
    if entry is None or entry.fetched_at != (run_completed_at or "").strip():
        raise RunNotHeldError(
            "This check is no longer held by the server, so it cannot be saved as you saw it. "
            "A finished check is held for 15 minutes; a per-source retry or a restart clears it. "
            "Run the check again, then save it."
        )

    narrative: dict[str, Any] | None = None
    dispositions: dict[str, Any] | None = None
    if narrative_run_id:
        held = held_narrative(narrative_run_id)
        if (
            held is None
            or held.lei != norm_lei
            or held.deepen_top != deepen_top
            or held.run_completed_at != entry.fetched_at
        ):
            raise NarrativeNotHeldError(
                "That summary was not written from this check (or is no longer held), so it "
                "cannot be saved with it. Save without the summary, or run the check and "
                "generate the summary again."
            )
        narrative = held.narrative
        try:
            sheet = load_dispositions(norm_lei, narrative_run_id)
        except ValueError:
            sheet = None
        dispositions = sheet.model_dump(mode="json") if sheet is not None else None

    saved = now or datetime.now(UTC)
    report_id = new_report_id()
    manage_token = new_manage_token()
    payload = build_payload(
        report_id=report_id,
        lei=norm_lei,
        deepen_top=deepen_top,
        run_completed_at=entry.fetched_at,
        saved_at=_iso(saved),
        events=list(entry.events),
        narrative=narrative,
        dispositions=dispositions,
    )
    meta = store.insert(
        report_id=report_id,
        lei=norm_lei,
        legal_name=payload.get("legal_name"),
        data=canonical_bytes(payload),
        saved_at=saved,
        manage_token=manage_token,
    )
    return {**_public_meta(meta), "manage_token": manage_token}


def _checked_read(
    store: SavedReportsStore, report_id: str, now: datetime | None = None
) -> tuple[dict[str, Any], bytes, str]:
    if not REPORT_ID_RE.match(report_id or ""):
        raise NotFoundError("No saved report with that id.")
    got = store.read(report_id)
    if got is None:
        raise NotFoundError("No saved report with that id.")
    meta, data, th = got
    if _parse_iso(meta["expires_at"]) <= (now or datetime.now(UTC)):
        store.delete(report_id)
        raise ExpiredError(
            f"This saved report expired on {meta['expires_at'][:10]} and has been deleted."
        )
    if content_hash(data) != meta["content_hash"]:
        log.error("saved report %s failed its integrity check", report_id)
        raise IntegrityError("This saved report failed its integrity check and will not be shown.")
    return meta, data, th


def load_report(
    store: SavedReportsStore, report_id: str, now: datetime | None = None
) -> tuple[dict[str, Any], bytes]:
    """``(public meta, canonical payload bytes)`` after the expiry and
    integrity checks."""
    meta, data, _ = _checked_read(store, report_id, now)
    return _public_meta(meta), data


def _authorise(th: str, manage_token: str | None) -> None:
    if not manage_token or not secrets.compare_digest(token_hash(manage_token), th):
        raise ForbiddenError("That manage token does not match this saved report.")


def extend_report(
    store: SavedReportsStore, report_id: str, manage_token: str | None, now: datetime | None = None
) -> dict[str, Any]:
    """Keep the report for another retention period, from now."""
    t = now or datetime.now(UTC)
    meta, _, th = _checked_read(store, report_id, t)
    _authorise(th, manage_token)
    store.set_expiry(report_id, t + timedelta(days=store.retention_days), t)
    return _public_meta(store.meta(report_id) or meta)


def delete_report(
    store: SavedReportsStore, report_id: str, manage_token: str | None
) -> bool:
    if not REPORT_ID_RE.match(report_id or ""):
        raise NotFoundError("No saved report with that id.")
    got = store.read(report_id)
    if got is None:
        raise NotFoundError("No saved report with that id.")
    _authorise(got[2], manage_token)
    return store.delete(report_id)


async def prune_loop(interval_s: float) -> None:
    """Delete expired reports every ``interval_s``. Never raises."""
    while True:
        try:
            store = get_store()
            if store is not None:
                n = await asyncio.to_thread(store.prune_expired)
                if n:
                    log.info("saved reports: pruned %d expired", n)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            log.exception("saved reports: prune failed")
        await asyncio.sleep(interval_s)
