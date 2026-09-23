"""watchlist — watch an LEI, be told when something about it changes.

Phase 215. A watchlist is a saved batch that re-runs on a delta. The design
problem was never the UI; it was the polling budget. "Re-run every watched
lookup daily" reproduces the crawl wave of 29 August 2026 — every watched
LEI a cold anchor of four to six GLEIF calls and ten upstream fetches, several
rate-limited and one CC-BY-NC at volume. So nothing here polls a company.

Two delta feeds, one shape
--------------------------

* **Tier 1 — GLEIF.** The Golden Copy delta that ``mirror_refresh`` already
  streams and applies in-process every hour is intersected with the
  watchlist. ``goldencopy.gleif.org`` is a different host from
  ``api.gleif.org``: the download costs nothing against the throttled
  window, regardless of watchlist size. A watched LEI in the delta is then
  read back from the mirror and its **material** fields — name, statuses,
  jurisdiction, legal form, parents, reporting exceptions, successor, expiry
  — are digested. A row whose only change is ``NextRenewalDate`` or
  ``LastUpdateDate`` has the same digest, so renewal churn (most of the
  16,000 rows a day) triggers nothing. Only a changed digest queues a re-run.
* **Tier 2 — OpenSanctions.** They publish an entity-level delta per
  version (``artifacts/default/<version>/entities.delta.json``, JSON-lines
  of ``{"op": ADD|MOD|DEL, "entity": {…}}``), about four a day. Every
  version since the last one seen is streamed and each organisation entity
  in it is compared with the watched entities' **own legal names** (the
  0.88 gate the rest of OpenCheck uses) and LEIs. Zero API calls, no
  re-screening, and the same public bulk file the licence already covers.
  Person screening deltas would put UBO names into this store; that waits
  for the GDPR ticket, as does email.

There is no third tier. National registers are re-fetched only as a
consequence of a Tier 1 or Tier 2 hit: a hit re-runs the whole lookup
pipeline (``refresh=True``, so the replay cache cannot answer for it), which
fetches them anyway. Freshness is a consequence of an observed change rather
than a clock, so "OpenCheck does not continuously monitor the registers" is
a description of the mechanism, not a disclaimer. The Phase 146 rule comes
for free: a source degraded during the re-run reports "could not check",
never "clean" — see :func:`diff_snapshots`.

Where state lives
-----------------

One SQLite file on the persistent disk that already holds the GLEIF mirror
(``OPENCHECK_WATCHLIST_DB_FILE``). A list is addressed by a capability token
— random, shown once, stored only as its SHA-256 — so holding the file never
yields a feed URL. There are no accounts, no credentials and no personal
data: LEIs, the legal names GLEIF publishes for them, digests, and the
diffs. Caps (``OPENCHECK_WATCHLIST_MAX_TOTAL`` / ``_MAX_PER_LIST``) are
cheap insurance, not what keeps the budget safe — at 0.47 % of LEI records
changing a day, two hundred watched LEIs cost under one re-run a day.

What a "change" is
------------------

:func:`snapshot_from_response` reduces a ``LookupResponse`` to the facts a
feed reader needs — the same helpers the batch row and the report use
(``subject_profile`` for register status, founding date and legal form;
``consistency.one_per_entity_identifiers`` for identifiers; the risk-signal
codes with the source that produced each; the Phase 156 coverage figures).
:func:`diff_snapshots` then names each difference with a closed vocabulary
(:data:`CHANGE_KINDS`). Two rules shape it:

* A signal code that was present and is now absent is **retired** only when
  the source that produced it answered this time. If that source is in
  ``degraded_sources``, the change is ``signal_unchecked`` — the code could
  not be re-assessed — never ``signal_retired``.
* Coverage that fell because sources were degraded is ``coverage_unchecked``,
  not ``coverage_changed``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import sqlite3
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import identifiers
from .names import name_similarity

log = logging.getLogger("opencheck.watchlist")

#: Tiers, as recorded on an entry. Closed vocabulary.
TIER_GLEIF = "gleif"
TIER_OPENSANCTIONS = "opensanctions"
TIER_MANUAL = "manual"
TIERS = (TIER_GLEIF, TIER_OPENSANCTIONS, TIER_MANUAL)

#: The kinds a diff can contain. Closed vocabulary; the frontend words them
#: (``lib/watchlist.ts``) and a test pins that every kind has words.
CHANGE_KINDS: tuple[str, ...] = (
    "gleif_field",  # a material GLEIF record field (Tier 1 facts)
    "legal_name",
    "jurisdiction",
    "register_status",
    "founding_date",
    "legal_form",
    "dissolution_date",
    "identifier",
    "signal_new",
    "signal_retired",
    "signal_unchecked",
    "context_new",
    "context_retired",
    "context_unchecked",
    "coverage_changed",
    "coverage_unchecked",
    "verdict",
)

#: The name-match gate — the one concept product-wide (``names.name_similarity``).
NAME_MATCH_THRESHOLD = 0.88

#: OpenSanctions schemata that are organisations. v1 matches the watched
#: entity's own names only, so Person and its kin are never read.
_OS_ORG_SCHEMATA = frozenset({"Company", "LegalEntity", "Organization", "PublicBody"})

OS_VERSIONS_URL = "https://data.opensanctions.org/artifacts/default/versions.json"
OS_DELTA_URL = "https://data.opensanctions.org/artifacts/default/{version}/entities.delta.json"

#: How many OpenSanctions versions one tick will catch up on. Four a day are
#: published; after a week down this bounds the work to ~130 MB.
OS_MAX_VERSIONS_PER_TICK = 12


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def token_hash(token: str) -> str:
    return hashlib.sha256(token.strip().encode("utf-8")).hexdigest()


def new_token() -> str:
    return secrets.token_urlsafe(24)


# ---------------------------------------------------------------------------
# Tier 1 facts: the material GLEIF fields
# ---------------------------------------------------------------------------

#: The GLEIF fields whose change is material. ``Registration.NextRenewalDate``
#: and ``Registration.LastUpdateDate`` are deliberately absent: a row whose
#: only change is one of those is renewal churn and must trigger nothing.
GLEIF_MATERIAL_FIELDS: tuple[str, ...] = (
    "legal_name",
    "entity_status",
    "registration_status",
    "jurisdiction",
    "legal_form",
    "successor_lei",
    "direct_parent_lei",
    "ultimate_parent_lei",
    "direct_exception",
    "ultimate_exception",
    "creation_date",
    "expiration_date",
    "expiration_reason",
)


def gleif_facts(store: Any, lei: str) -> dict[str, Any] | None:
    """The material fields of ``lei``'s mirror row, or ``None`` when the
    mirror does not hold it. ``store`` is an ``entity_pages.EntityStore``."""
    row = store.get(lei)
    if row is None:
        return None
    detail = row.detail or {}
    expiration = detail.get("expiration") or {}
    exceptions = {}
    try:
        exceptions = store.exceptions(lei) or {}
    except Exception:  # noqa: BLE001 — a v1 file has no exceptions table
        exceptions = {}

    def _reason(kind: str) -> str | None:
        ex = exceptions.get(kind)
        return getattr(ex, "reason", None) if ex is not None else None

    return {
        "legal_name": row.name or None,
        "entity_status": row.entity_status or None,
        "registration_status": row.registration_status or None,
        "jurisdiction": row.jurisdiction or None,
        "legal_form": row.legal_form or None,
        "successor_lei": row.successor_lei or None,
        "direct_parent_lei": row.direct_parent_lei or None,
        "ultimate_parent_lei": row.ultimate_parent_lei or None,
        "direct_exception": _reason("direct"),
        "ultimate_exception": _reason("ultimate"),
        "creation_date": detail.get("creationDate") or None,
        "expiration_date": expiration.get("date") or None,
        "expiration_reason": expiration.get("reason") or None,
    }


def digest(obj: Any) -> str:
    """A stable digest of a JSON-able value."""
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def diff_gleif_facts(before: dict[str, Any] | None, after: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The material GLEIF fields that differ, as ``gleif_field`` changes."""
    if before is None or after is None:
        return []
    out = []
    for name in GLEIF_MATERIAL_FIELDS:
        a, b = before.get(name), after.get(name)
        if a != b:
            out.append({"kind": "gleif_field", "field": name, "old": a, "new": b})
    return out


# ---------------------------------------------------------------------------
# The lookup snapshot and its diff
# ---------------------------------------------------------------------------


def _subject_statements(lei: str, bods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from .subject_profile import subject_statements

    return subject_statements(lei, bods)


def snapshot_from_response(resp: Any) -> dict[str, Any]:
    """Reduce a ``LookupResponse`` to the facts the feed compares.

    Every figure comes from the helpers the report and the batch row use, so
    the watchlist cannot disagree with either about what a company's status
    or findings are.
    """
    from .consistency import one_per_entity_identifiers
    from .mcp.shaping import shape_batch_row

    row = shape_batch_row(resp)
    profile = getattr(resp, "subject_profile", None) or {}
    bods = list(getattr(resp, "bods", None) or [])
    stmts = _subject_statements(resp.lei, bods)

    identifiers_: dict[str, str] = {}
    dissolution: str | None = None
    for stmt in stmts:
        for scheme, value in one_per_entity_identifiers(stmt).items():
            identifiers_.setdefault(scheme, value)
        rd = stmt.get("recordDetails") or {}
        d = rd.get("dissolutionDate")
        if isinstance(d, str) and d and not dissolution:
            dissolution = d

    signals: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for s in getattr(resp, "risk_signals", None) or []:
        code = str(s.get("code") or "")
        if not code:
            continue
        key = (code, str(s.get("source_id") or ""), str(s.get("kind") or "risk"))
        if key in seen:
            continue
        seen.add(key)
        signals.append({"code": key[0], "source_id": key[1], "kind": key[2]})

    degraded = []
    for d in getattr(resp, "degraded_sources", None) or []:
        degraded.append(
            {
                "source_id": str(d.get("source_id") or ""),
                "check": str(d.get("check") or ""),
                "affected_signals": list(d.get("affected_signals") or []),
            }
        )

    def _value(key: str) -> str | None:
        v = profile.get(key) or {}
        return v.get("value") if isinstance(v, dict) else None

    return {
        "legal_name": resp.legal_name,
        "jurisdiction": resp.jurisdiction,
        "register_status": row["register_status"],
        "founding_date": _value("founding_date"),
        "legal_form": _value("legal_form"),
        "dissolution_date": dissolution,
        "identifiers": dict(sorted(identifiers_.items())),
        "signals": sorted(signals, key=lambda s: (s["kind"], s["code"], s["source_id"])),
        "coverage": row["coverage"],
        "degraded_sources": degraded,
        "verdict": row["verdict"],
        # Which sources were actually reached, and when (Phase 99/100: the
        # retrieval clock, per source). The feed says "these sources were
        # checked on that date as a result".
        "checked": _checked(getattr(resp, "source_liveness", None) or {}),
    }


def _checked(source_liveness: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for sid, prov in sorted(source_liveness.items()):
        if not isinstance(prov, dict):
            continue
        out.append(
            {
                "source_id": sid,
                "liveness": prov.get("liveness"),
                "retrieved_at": prov.get("retrieved_at"),
            }
        )
    return out


def _codes(snapshot: dict[str, Any], kind: str) -> dict[str, set[str]]:
    """``code → {source_ids}`` for the signals of one kind."""
    out: dict[str, set[str]] = {}
    for s in snapshot.get("signals") or []:
        if s.get("kind", "risk") == kind:
            out.setdefault(s["code"], set()).add(s.get("source_id") or "")
    return out


def _degraded_index(snapshot: dict[str, Any]) -> tuple[set[str], set[str]]:
    """``(degraded source ids, codes those degradations affect)``."""
    ids: set[str] = set()
    codes: set[str] = set()
    for d in snapshot.get("degraded_sources") or []:
        if d.get("source_id"):
            ids.add(d["source_id"])
        codes.update(d.get("affected_signals") or [])
    return ids, codes


def diff_snapshots(before: dict[str, Any] | None, after: dict[str, Any]) -> list[dict[str, Any]]:
    """Name every difference between two snapshots. Empty when nothing the
    feed reports about has changed. ``before`` may be ``None`` (first run):
    then there is nothing to compare and the list is empty."""
    if not before:
        return []
    changes: list[dict[str, Any]] = []

    def _scalar(kind: str, key: str) -> None:
        a, b = before.get(key), after.get(key)
        if (a or None) != (b or None):
            changes.append({"kind": kind, "old": a, "new": b})

    _scalar("legal_name", "legal_name")
    _scalar("jurisdiction", "jurisdiction")
    _scalar("founding_date", "founding_date")
    _scalar("legal_form", "legal_form")
    _scalar("dissolution_date", "dissolution_date")

    rs_a = (before.get("register_status") or {}).get("liveness")
    rs_b = (after.get("register_status") or {}).get("liveness")
    if rs_a != rs_b:
        changes.append(
            {
                "kind": "register_status",
                "old": before.get("register_status"),
                "new": after.get("register_status"),
            }
        )

    ids_a = before.get("identifiers") or {}
    ids_b = after.get("identifiers") or {}
    for scheme in sorted(set(ids_a) | set(ids_b)):
        if scheme in ids_a and scheme in ids_b and ids_a[scheme] != ids_b[scheme]:
            changes.append(
                {"kind": "identifier", "scheme": scheme, "old": ids_a[scheme], "new": ids_b[scheme]}
            )

    degraded_ids, degraded_codes = _degraded_index(after)
    for kind, new_kind, retired_kind, unchecked_kind in (
        ("risk", "signal_new", "signal_retired", "signal_unchecked"),
        ("context", "context_new", "context_retired", "context_unchecked"),
    ):
        ca, cb = _codes(before, kind), _codes(after, kind)
        for code in sorted(set(cb) - set(ca)):
            changes.append({"kind": new_kind, "code": code, "sources": sorted(cb[code])})
        for code in sorted(set(ca) - set(cb)):
            producers = ca[code]
            # The Phase 146 rule: absence is a finding only when the source
            # that produced the code answered. A degraded producer, or a
            # degradation that names this code, means "could not check".
            if (producers & degraded_ids) or code in degraded_codes:
                changes.append(
                    {
                        "kind": unchecked_kind,
                        "code": code,
                        "sources": sorted(producers),
                        "degraded": sorted(producers & degraded_ids),
                    }
                )
            else:
                changes.append({"kind": retired_kind, "code": code, "sources": sorted(producers)})

    cov_a = before.get("coverage") or {}
    cov_b = after.get("coverage") or {}
    if (cov_a.get("applicable"), cov_a.get("answered")) != (
        cov_b.get("applicable"),
        cov_b.get("answered"),
    ):
        missing = sorted(set(cov_b.get("applicable_ids") or []) - set(cov_b.get("answered_ids") or []))
        unchecked = bool(degraded_ids & set(missing)) and (cov_b.get("answered") or 0) < (
            cov_a.get("answered") or 0
        )
        changes.append(
            {
                "kind": "coverage_unchecked" if unchecked else "coverage_changed",
                "old": {"applicable": cov_a.get("applicable"), "answered": cov_a.get("answered")},
                "new": {"applicable": cov_b.get("applicable"), "answered": cov_b.get("answered")},
                "missing": missing,
            }
        )

    if (before.get("verdict") or None) != (after.get("verdict") or None) and not any(
        c["kind"] in ("register_status", "signal_new", "signal_retired", "signal_unchecked")
        for c in changes
    ):
        # The verdict sentence is built from the signals and the degraded
        # list, so it usually changes *with* one of the kinds above. On its
        # own it is still worth a line (a degraded source recovering, say).
        changes.append({"kind": "verdict", "old": before.get("verdict"), "new": after.get("verdict")})
    return changes


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS lists (
    token_hash TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS watches (
    token_hash TEXT NOT NULL,
    lei TEXT NOT NULL,
    added_at TEXT NOT NULL,
    legal_name TEXT,
    jurisdiction TEXT,
    gleif_facts_json TEXT,
    gleif_digest TEXT,
    gleif_watermark TEXT,
    snapshot_json TEXT,
    snapshot_at TEXT,
    last_checked_at TEXT,
    PRIMARY KEY (token_hash, lei)
);
CREATE INDEX IF NOT EXISTS watches_lei ON watches (lei);
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash TEXT NOT NULL,
    lei TEXT NOT NULL,
    created_at TEXT NOT NULL,
    tier TEXT NOT NULL,
    trigger_json TEXT NOT NULL,
    changes_json TEXT NOT NULL,
    checked_json TEXT NOT NULL,
    degraded_json TEXT NOT NULL,
    legal_name TEXT
);
CREATE INDEX IF NOT EXISTS entries_list ON entries (token_hash, id);
CREATE TABLE IF NOT EXISTS pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lei TEXT NOT NULL,
    tier TEXT NOT NULL,
    trigger_json TEXT NOT NULL,
    queued_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


@dataclass
class Caps:
    max_total: int
    max_per_list: int


class WatchlistStore:
    """The SQLite file. Every method opens and closes its own short
    connection: the mirror-refresh thread, the worker task and request
    handlers all write, and short transactions are what keeps that safe."""

    def __init__(self, path: Path, caps: Caps):
        self.path = Path(path)
        self.caps = caps
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    # -- lists -------------------------------------------------------------

    def create_list(self) -> str:
        """Create a list and return its token (the only time it is seen)."""
        token = new_token()
        now = _now_iso()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO lists (token_hash, created_at, last_seen_at) VALUES (?, ?, ?)",
                (token_hash(token), now, now),
            )
        return token

    def list_exists(self, th: str) -> bool:
        with self._conn() as conn:
            return conn.execute("SELECT 1 FROM lists WHERE token_hash = ?", (th,)).fetchone() is not None

    def touch(self, th: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE lists SET last_seen_at = ? WHERE token_hash = ?", (_now_iso(), th))

    # -- watches -----------------------------------------------------------

    def counts(self, th: str | None = None) -> dict[str, int]:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM watches").fetchone()[0]
            distinct = conn.execute("SELECT COUNT(DISTINCT lei) FROM watches").fetchone()[0]
            mine = (
                conn.execute("SELECT COUNT(*) FROM watches WHERE token_hash = ?", (th,)).fetchone()[0]
                if th
                else 0
            )
        return {"total": total, "distinct_leis": distinct, "in_list": mine}

    def check_capacity(self, th: str | None, lei: str) -> None:
        """Raise :class:`CapExceededError` if adding ``lei`` to the list
        ``th`` (``None`` = a list not yet created) would break either cap.

        Phase 234: asked *before* the baseline lookup, so a full instance
        refuses without running a lookup it cannot keep. :meth:`add_watch`
        asks again inside its transaction, which is what makes it exact."""
        with self._conn() as conn:
            if th is not None:
                exists = conn.execute(
                    "SELECT 1 FROM watches WHERE token_hash = ? AND lei = ?", (th, lei)
                ).fetchone()
                if exists:
                    return
                mine = conn.execute(
                    "SELECT COUNT(*) FROM watches WHERE token_hash = ?", (th,)
                ).fetchone()[0]
                if mine >= self.caps.max_per_list:
                    raise CapExceededError("per_list", self.caps.max_per_list)
            total = conn.execute("SELECT COUNT(*) FROM watches").fetchone()[0]
            if total >= self.caps.max_total:
                raise CapExceededError("total", self.caps.max_total)

    def add_watch(
        self,
        th: str,
        lei: str,
        *,
        legal_name: str | None,
        jurisdiction: str | None,
        gleif_facts: dict[str, Any] | None,
        gleif_watermark: str | None,
        snapshot: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Add ``lei`` to the list. Raises ``CapExceeded`` on either cap.
        Re-adding a watched LEI refreshes its baseline and is not a second
        row."""
        now = _now_iso()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                exists = conn.execute(
                    "SELECT 1 FROM watches WHERE token_hash = ? AND lei = ?", (th, lei)
                ).fetchone()
                if not exists:
                    mine = conn.execute(
                        "SELECT COUNT(*) FROM watches WHERE token_hash = ?", (th,)
                    ).fetchone()[0]
                    if mine >= self.caps.max_per_list:
                        raise CapExceededError("per_list", self.caps.max_per_list)
                    total = conn.execute("SELECT COUNT(*) FROM watches").fetchone()[0]
                    if total >= self.caps.max_total:
                        raise CapExceededError("total", self.caps.max_total)
                conn.execute(
                    """
                    INSERT INTO watches (token_hash, lei, added_at, legal_name, jurisdiction,
                        gleif_facts_json, gleif_digest, gleif_watermark, snapshot_json, snapshot_at,
                        last_checked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(token_hash, lei) DO UPDATE SET
                        legal_name=excluded.legal_name, jurisdiction=excluded.jurisdiction,
                        gleif_facts_json=excluded.gleif_facts_json, gleif_digest=excluded.gleif_digest,
                        gleif_watermark=excluded.gleif_watermark, snapshot_json=excluded.snapshot_json,
                        snapshot_at=excluded.snapshot_at, last_checked_at=excluded.last_checked_at
                    """,
                    (
                        th,
                        lei,
                        now,
                        legal_name,
                        jurisdiction,
                        json.dumps(gleif_facts) if gleif_facts is not None else None,
                        digest(gleif_facts) if gleif_facts is not None else None,
                        gleif_watermark,
                        json.dumps(snapshot) if snapshot is not None else None,
                        now if snapshot is not None else None,
                        now if snapshot is not None else None,
                    ),
                )
                conn.execute("UPDATE lists SET last_seen_at = ? WHERE token_hash = ?", (now, th))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return self.get_watch(th, lei) or {}

    def remove_watch(self, th: str, lei: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM watches WHERE token_hash = ? AND lei = ?", (th, lei))
            return cur.rowcount > 0

    def get_watch(self, th: str, lei: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM watches WHERE token_hash = ? AND lei = ?", (th, lei)
            ).fetchone()
        return _watch_dict(row) if row else None

    def watches(self, th: str) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM watches WHERE token_hash = ? ORDER BY added_at, lei", (th,)
            ).fetchall()
        return [_watch_dict(r) for r in rows]

    def watched_leis(self) -> set[str]:
        with self._conn() as conn:
            return {r[0] for r in conn.execute("SELECT DISTINCT lei FROM watches")}

    def watched_names(self) -> dict[str, list[str]]:
        """``lei → [legal names]`` across every list — what Tier 2 matches on."""
        out: dict[str, list[str]] = {}
        with self._conn() as conn:
            for lei, name, facts in conn.execute(
                "SELECT lei, legal_name, gleif_facts_json FROM watches"
            ):
                names = out.setdefault(lei, [])
                for candidate in (name, (json.loads(facts) if facts else {}).get("legal_name")):
                    if candidate and candidate not in names:
                        names.append(candidate)
        return out

    def rows_for_lei(self, lei: str, *, with_hash: bool = False) -> list[dict[str, Any]]:
        """Every list's watch of ``lei``. ``with_hash`` adds the list's
        ``token_hash`` — for the re-run only; never serialise it."""
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM watches WHERE lei = ?", (lei,)).fetchall()
        out = []
        for r in rows:
            d = _watch_dict(r)
            if with_hash:
                d["token_hash"] = r["token_hash"]
            out.append(d)
        return out

    def update_baseline(
        self,
        th: str,
        lei: str,
        *,
        gleif_facts: dict[str, Any] | None = None,
        gleif_watermark: str | None = None,
        snapshot: dict[str, Any] | None = None,
        legal_name: str | None = None,
        jurisdiction: str | None = None,
    ) -> None:
        now = _now_iso()
        sets = ["last_checked_at = ?"]
        args: list[Any] = [now]
        if gleif_facts is not None:
            sets += ["gleif_facts_json = ?", "gleif_digest = ?", "gleif_watermark = ?"]
            args += [json.dumps(gleif_facts), digest(gleif_facts), gleif_watermark]
        if snapshot is not None:
            sets += ["snapshot_json = ?", "snapshot_at = ?"]
            args += [json.dumps(snapshot), now]
        if legal_name:
            sets.append("legal_name = ?")
            args.append(legal_name)
        if jurisdiction:
            sets.append("jurisdiction = ?")
            args.append(jurisdiction)
        args += [th, lei]
        with self._conn() as conn:
            conn.execute(f"UPDATE watches SET {', '.join(sets)} WHERE token_hash = ? AND lei = ?", args)

    # -- entries -----------------------------------------------------------

    def add_entry(
        self,
        th: str,
        lei: str,
        *,
        tier: str,
        trigger: dict[str, Any],
        changes: list[dict[str, Any]],
        checked: list[dict[str, Any]],
        degraded: list[dict[str, Any]],
        legal_name: str | None,
    ) -> int:
        assert tier in TIERS, tier
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO entries (token_hash, lei, created_at, tier, trigger_json, changes_json,
                    checked_json, degraded_json, legal_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    th,
                    lei,
                    _now_iso(),
                    tier,
                    json.dumps(trigger),
                    json.dumps(changes),
                    json.dumps(checked),
                    json.dumps(degraded),
                    legal_name,
                ),
            )
            return int(cur.lastrowid or 0)

    def entries(self, th: str, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM entries WHERE token_hash = ? ORDER BY id DESC LIMIT ?", (th, limit)
            ).fetchall()
        return [_entry_dict(r) for r in rows]

    # -- pending re-runs ---------------------------------------------------

    def enqueue(self, lei: str, tier: str, trigger: dict[str, Any]) -> bool:
        """Queue a re-run of ``lei``. One pending row per LEI: a second
        trigger before the first ran is folded into it."""
        with self._conn() as conn:
            existing = conn.execute("SELECT id FROM pending WHERE lei = ?", (lei,)).fetchone()
            if existing:
                return False
            conn.execute(
                "INSERT INTO pending (lei, tier, trigger_json, queued_at) VALUES (?, ?, ?, ?)",
                (lei, tier, json.dumps(trigger), _now_iso()),
            )
            return True

    def take_pending(self, limit: int) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pending ORDER BY id LIMIT ?", (limit,)
            ).fetchall()
            out = [
                {
                    "id": r["id"],
                    "lei": r["lei"],
                    "tier": r["tier"],
                    "trigger": json.loads(r["trigger_json"]),
                    "queued_at": r["queued_at"],
                }
                for r in rows
            ]
            if out:
                conn.executemany("DELETE FROM pending WHERE id = ?", [(r["id"],) for r in out])
        return out

    def pending_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM pending").fetchone()[0]

    # -- meta / housekeeping -------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def prune_empty_lists(self, *, older_than_s: float = 86400.0) -> int:
        """Drop lists that hold nothing and have not been opened for a day —
        a token minted by a visitor who never watched anything."""
        cutoff = datetime.fromtimestamp(time.time() - older_than_s, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._conn() as conn:
            cur = conn.execute(
                """
                DELETE FROM lists WHERE last_seen_at < ?
                  AND token_hash NOT IN (SELECT DISTINCT token_hash FROM watches)
                  AND token_hash NOT IN (SELECT DISTINCT token_hash FROM entries)
                """,
                (cutoff,),
            )
            return cur.rowcount


    def prune_stale_lists(self, *, older_than_days: int) -> int:
        """Phase 234: delete lists nobody has opened for ``older_than_days``
        — the page or the Atom feed; both touch ``last_seen_at`` — with their
        watches, entries and nothing else. Without it an abandoned list held
        its share of the instance cap, and was re-run on every delta, forever.
        Returns the number of lists deleted."""
        if older_than_days <= 0:
            return 0
        cutoff = datetime.fromtimestamp(
            time.time() - older_than_days * 86400.0, UTC
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                stale = [
                    r[0]
                    for r in conn.execute(
                        "SELECT token_hash FROM lists WHERE last_seen_at < ?", (cutoff,)
                    ).fetchall()
                ]
                for th in stale:
                    conn.execute("DELETE FROM watches WHERE token_hash = ?", (th,))
                    conn.execute("DELETE FROM entries WHERE token_hash = ?", (th,))
                    conn.execute("DELETE FROM lists WHERE token_hash = ?", (th,))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return len(stale)


class CapExceededError(Exception):
    def __init__(self, which: str, cap: int):
        super().__init__(f"{which} cap of {cap} reached")
        self.which = which
        self.cap = cap


def _watch_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "lei": row["lei"],
        "added_at": row["added_at"],
        "legal_name": row["legal_name"],
        "jurisdiction": row["jurisdiction"],
        "gleif_facts": json.loads(row["gleif_facts_json"]) if row["gleif_facts_json"] else None,
        "gleif_watermark": row["gleif_watermark"],
        "snapshot": json.loads(row["snapshot_json"]) if row["snapshot_json"] else None,
        "snapshot_at": row["snapshot_at"],
        "last_checked_at": row["last_checked_at"],
    }


def _entry_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "lei": row["lei"],
        "legal_name": row["legal_name"],
        "created_at": row["created_at"],
        "tier": row["tier"],
        "trigger": json.loads(row["trigger_json"]),
        "changes": json.loads(row["changes_json"]),
        "checked": json.loads(row["checked_json"]),
        "degraded": json.loads(row["degraded_json"]),
    }


# ---------------------------------------------------------------------------
# The configured store
# ---------------------------------------------------------------------------

_store: WatchlistStore | None = None
_store_lock = threading.Lock()


def get_store() -> WatchlistStore | None:
    """The configured store, or ``None`` when ``OPENCHECK_WATCHLIST_DB_FILE``
    is unset — in which case the feature is off and its routes say so."""
    global _store
    from .config import get_settings

    path = get_settings().watchlist_db_file
    if not path:
        return None
    with _store_lock:
        if _store is None or _store.path != Path(path):
            s = get_settings()
            _store = WatchlistStore(
                Path(path), Caps(s.watchlist_max_total, s.watchlist_max_per_list)
            )
        return _store


def reset_for_tests() -> None:
    global _store, _state
    with _store_lock:
        _store = None
    with _state_lock:
        _state = WatcherState()


# ---------------------------------------------------------------------------
# Tier 1 — the GLEIF delta hook
# ---------------------------------------------------------------------------


@dataclass
class WatcherState:
    """What ``/watchstats`` reports. Aggregate only — no LEI, no name."""

    enabled: bool = False
    gleif_deltas_seen: int = 0
    gleif_touched: int = 0  # watched LEIs present in a delta
    gleif_churn: int = 0  # …whose material digest did not change
    gleif_queued: int = 0
    gleif_last_publish: str | None = None
    os_versions_seen: int = 0
    os_last_version: str | None = None
    os_queued: int = 0
    os_last_checked_at: str | None = None
    os_last_error: str | None = None
    reruns: int = 0
    rerun_failures: int = 0
    entries_written: int = 0
    last_tick_at: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


_state = WatcherState()
_state_lock = threading.Lock()


def state() -> dict[str, Any]:
    with _state_lock:
        return _state.to_dict()


def _bump(**fields: Any) -> None:
    with _state_lock:
        for k, v in fields.items():
            if isinstance(v, bool) or not isinstance(v, int):
                setattr(_state, k, v)
            else:
                setattr(_state, k, getattr(_state, k) + v)


def on_gleif_delta(changed_leis: Iterable[str], publish_label: str) -> dict[str, int]:
    """Called by ``mirror_refresh.apply_delta`` after a delta landed, with
    the LEIs the delta's three files named. Runs on the refresh thread.

    Intersects with the watchlist, re-reads each touched LEI from the mirror
    and queues a re-run only where the material digest changed. Never
    raises: the mirror refresh must not fail because the watcher did.
    """
    try:
        store = get_store()
        if store is None:
            return {"touched": 0, "queued": 0, "churn": 0}
        from . import entity_pages as ep

        mirror = ep.get_store()
        watched = store.watched_leis()
        touched = sorted(watched & {str(x).strip().upper() for x in changed_leis})
        queued = churn = 0
        for lei in touched:
            facts = gleif_facts(mirror, lei) if mirror is not None else None
            new_digest = digest(facts) if facts is not None else None
            rows = store.rows_for_lei(lei)
            changed_fields: list[dict[str, Any]] = []
            material = False
            for row in rows:
                if facts is None or row.get("gleif_facts") is None:
                    # No baseline (or no mirror row) — the delta named it, so
                    # a re-run is the honest response; the re-run stores one.
                    material = True
                    continue
                if new_digest != digest(row["gleif_facts"]):
                    material = True
                    changed_fields = diff_gleif_facts(row["gleif_facts"], facts)
            if material:
                if store.enqueue(
                    lei,
                    TIER_GLEIF,
                    {
                        "tier": TIER_GLEIF,
                        "publish": publish_label,
                        "fields": [c["field"] for c in changed_fields],
                    },
                ):
                    queued += 1
            else:
                churn += 1
        _bump(
            gleif_deltas_seen=1,
            gleif_touched=len(touched),
            gleif_churn=churn,
            gleif_queued=queued,
            gleif_last_publish=publish_label,
        )
        if touched:
            log.info(
                "watchlist: GLEIF delta %s named %d watched LEI(s) — %d queued, %d renewal churn",
                publish_label, len(touched), queued, churn,
            )
        return {"touched": len(touched), "queued": queued, "churn": churn}
    except Exception as exc:  # noqa: BLE001
        log.warning("watchlist: GLEIF delta hook failed: %s", exc)
        _bump(last_error=f"gleif: {type(exc).__name__}: {exc}")
        return {"touched": 0, "queued": 0, "churn": 0}


# ---------------------------------------------------------------------------
# Tier 2 — the OpenSanctions entity delta
# ---------------------------------------------------------------------------


def _os_entity_names(entity: dict[str, Any]) -> list[str]:
    props = entity.get("properties") or {}
    names: list[str] = []
    for key in ("name", "alias", "previousName"):
        for v in props.get(key) or []:
            if isinstance(v, str) and v and v not in names:
                names.append(v)
    caption = entity.get("caption")
    if isinstance(caption, str) and caption and caption not in names:
        names.append(caption)
    return names


def match_os_entity(
    entity: dict[str, Any], watched: dict[str, list[str]], *, threshold: float = NAME_MATCH_THRESHOLD
) -> list[dict[str, Any]]:
    """The watched LEIs an OpenSanctions entity matches: by ``leiCode``
    exactly, else by name at the 0.88 gate. Organisation schemata only."""
    if entity.get("schema") not in _OS_ORG_SCHEMATA:
        return []
    props = entity.get("properties") or {}
    hits: list[dict[str, Any]] = []
    leis = {str(v).strip().upper() for v in props.get("leiCode") or [] if v}
    for lei in sorted(leis & set(watched)):
        hits.append({"lei": lei, "matched_on": "lei", "score": 1.0})
    matched = {h["lei"] for h in hits}
    names = _os_entity_names(entity)
    if not names:
        return hits
    for lei, own in watched.items():
        if lei in matched:
            continue
        best = 0.0
        for a in own:
            for b in names:
                best = max(best, name_similarity(a, b))
                if best >= 1.0:
                    break
        if best >= threshold:
            hits.append({"lei": lei, "matched_on": "name", "score": round(best, 3)})
    return hits


def _os_versions(client: Any) -> list[str]:
    r = client.get(OS_VERSIONS_URL, timeout=60.0)
    r.raise_for_status()
    items = r.json().get("items") or []
    return [str(v) for v in items]


def scan_os_delta(lines: Iterable[str], watched: dict[str, list[str]], version: str) -> list[dict[str, Any]]:
    """Every (LEI, trigger) a delta file yields, one per LEI (first match
    wins; the trigger names the entity and the op)."""
    triggers: dict[str, dict[str, Any]] = {}
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            rec = json.loads(raw)
        except json.JSONDecodeError:
            continue
        entity = rec.get("entity") or {}
        for hit in match_os_entity(entity, watched):
            triggers.setdefault(
                hit["lei"],
                {
                    "tier": TIER_OPENSANCTIONS,
                    "version": version,
                    "op": rec.get("op"),
                    "entity_id": entity.get("id"),
                    "caption": entity.get("caption"),
                    "schema": entity.get("schema"),
                    "datasets": list(entity.get("datasets") or [])[:8],
                    "topics": list((entity.get("properties") or {}).get("topics") or [])[:8],
                    "matched_on": hit["matched_on"],
                    "score": hit["score"],
                },
            )
    return [{"lei": lei, "trigger": t} for lei, t in triggers.items()]


def opensanctions_tick(client: Any | None = None) -> dict[str, Any]:
    """Catch up on every OpenSanctions version since the last one seen and
    queue a re-run for each watched entity a delta named. Runs on a thread.
    Downloads nothing when nothing is watched."""
    import httpx

    store = get_store()
    if store is None:
        return {"versions": 0, "queued": 0}
    watched = store.watched_names()
    result = {"versions": 0, "queued": 0}
    _bump(os_last_checked_at=_now_iso())
    if not watched:
        return result
    own_client = client is None
    client = client or httpx.Client(follow_redirects=True, headers={"User-Agent": "OpenCheck watchlist"})
    try:
        versions = _os_versions(client)
        if not versions:
            return result
        last = store.get_meta("opensanctions_version")
        if last is None:
            # First run: mark the current version and start from the next.
            store.set_meta("opensanctions_version", versions[-1])
            _bump(os_last_version=versions[-1])
            return result
        todo = [v for v in versions if v > last][-OS_MAX_VERSIONS_PER_TICK:]
        for version in todo:
            queued = 0
            with client.stream("GET", OS_DELTA_URL.format(version=version), timeout=600.0) as r:
                r.raise_for_status()
                for item in scan_os_delta(r.iter_lines(), watched, version):
                    if store.enqueue(item["lei"], TIER_OPENSANCTIONS, item["trigger"]):
                        queued += 1
            store.set_meta("opensanctions_version", version)
            result["versions"] += 1
            result["queued"] += queued
            _bump(os_versions_seen=1, os_queued=queued, os_last_version=version, os_last_error=None)
            if queued:
                log.info("watchlist: OpenSanctions %s named %d watched entit%s", version, queued, "y" if queued == 1 else "ies")
        return result
    except Exception as exc:  # noqa: BLE001
        log.warning("watchlist: OpenSanctions delta failed: %s", exc)
        _bump(os_last_error=f"{type(exc).__name__}: {exc}")
        return result
    finally:
        if own_client:
            client.close()


# ---------------------------------------------------------------------------
# The re-run
# ---------------------------------------------------------------------------


async def _lookup(lei: str) -> Any:
    from .routers.lookup import _lookup_impl

    return await _lookup_impl(lei, refresh=True)


async def rerun(lei: str, tier: str, trigger: dict[str, Any], *, only_token_hash: str | None = None) -> dict[str, Any]:
    """Re-run the lookup for ``lei`` and write an entry to every list that
    watches it (or just ``only_token_hash``'s). Returns what happened, for
    the manual re-check to show.

    An entry is written when the tier that fired reported a material GLEIF
    change, when the lookup snapshot differs, or when OpenSanctions named
    the entity — a Tier 2 hit is itself the news even if the screen came
    back the same. A manual re-check that finds nothing writes no entry.
    """
    store = get_store()
    if store is None:
        return {"lei": lei, "entries": 0, "changes": []}
    rows = store.rows_for_lei(lei, with_hash=True)
    if only_token_hash:
        rows = [r for r in rows if r["token_hash"] == only_token_hash]
    if not rows:
        return {"lei": lei, "entries": 0, "changes": []}

    try:
        resp = await _lookup(lei)
    except Exception as exc:  # noqa: BLE001 — a failed re-run is counted, the baseline stays
        log.warning("watchlist: re-run of a watched LEI failed: %s: %s", type(exc).__name__, exc)
        _bump(rerun_failures=1)
        return {"lei": lei, "entries": 0, "changes": [], "error": f"{type(exc).__name__}"}
    _bump(reruns=1)

    from . import entity_pages as ep

    mirror = ep.get_store()
    facts = gleif_facts(mirror, lei) if mirror is not None else None
    watermark = None
    if mirror is not None:
        wm = mirror.watermark()
        watermark = wm.strftime("%Y-%m-%d %H:%M:%S") if wm else None
    snapshot = snapshot_from_response(resp)

    written = 0
    all_changes: list[dict[str, Any]] = []
    for row in rows:
        th = row["token_hash"]
        gleif_changes = diff_gleif_facts(row.get("gleif_facts"), facts)
        changes = gleif_changes + diff_snapshots(row.get("snapshot"), snapshot)
        all_changes = changes
        store.update_baseline(
            th,
            lei,
            gleif_facts=facts,
            gleif_watermark=watermark,
            snapshot=snapshot,
            legal_name=resp.legal_name,
            jurisdiction=resp.jurisdiction,
        )
        if changes or tier == TIER_OPENSANCTIONS:
            store.add_entry(
                th,
                lei,
                tier=tier,
                trigger=trigger,
                changes=changes,
                checked=snapshot["checked"],
                degraded=snapshot["degraded_sources"],
                legal_name=resp.legal_name,
            )
            written += 1
    _bump(entries_written=written)
    return {
        "lei": lei,
        "legal_name": resp.legal_name,
        "entries": written,
        "changes": all_changes,
        "checked": snapshot["checked"],
        "degraded": snapshot["degraded_sources"],
        "snapshot": snapshot,
    }


# ---------------------------------------------------------------------------
# The worker task
# ---------------------------------------------------------------------------


async def tick() -> dict[str, Any]:
    """One pass: drain queued re-runs (bounded), catch up on OpenSanctions
    when due, prune empty lists."""
    from .config import get_settings

    settings = get_settings()
    store = get_store()
    out: dict[str, Any] = {"reruns": 0, "os": None, "pruned": 0, "expired": 0}
    if store is None:
        return out
    _bump(last_tick_at=_now_iso())
    try:
        for item in store.take_pending(settings.watchlist_reruns_per_tick):
            await rerun(item["lei"], item["tier"], item["trigger"])
            out["reruns"] += 1
        interval = settings.watchlist_opensanctions_interval_s
        if interval > 0:
            last = store.get_meta("opensanctions_checked_at")
            due = last is None or (
                datetime.now(UTC) - datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            ).total_seconds() >= interval
            if due:
                out["os"] = await asyncio.to_thread(opensanctions_tick)
                store.set_meta("opensanctions_checked_at", _now_iso())
                # Anything Tier 2 queued runs next tick, within the same bound.
        out["pruned"] = store.prune_empty_lists()
        out["expired"] = store.prune_stale_lists(older_than_days=settings.watchlist_stale_days)
    except Exception as exc:  # noqa: BLE001 — the loop outlives any one tick
        log.exception("watchlist: tick failed")
        _bump(last_error=f"tick: {type(exc).__name__}: {exc}")
    return out


async def watch_loop(interval_s: float) -> None:
    """Run :func:`tick` every ``interval_s`` seconds until cancelled. The
    first tick is delayed by one interval — boot is busy enough."""
    with _state_lock:
        _state.enabled = True
    try:
        while True:
            await asyncio.sleep(interval_s)
            try:
                await tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("watchlist: unexpected error")
    finally:
        with _state_lock:
            _state.enabled = False


# ---------------------------------------------------------------------------
# Adding a watch: the baseline
# ---------------------------------------------------------------------------


async def baseline(lei: str) -> tuple[Any, dict[str, Any] | None, str | None]:
    """The lookup response (usually replayed — the reader is on the report
    page), the mirror facts and the mirror watermark for a new watch."""
    from . import entity_pages as ep
    from .routers.lookup import _lookup_impl

    resp = await _lookup_impl(lei)
    mirror = ep.get_store()
    facts = gleif_facts(mirror, lei) if mirror is not None else None
    watermark = None
    if mirror is not None:
        wm = mirror.watermark()
        watermark = wm.strftime("%Y-%m-%d %H:%M:%S") if wm else None
    return resp, facts, watermark


def new_list_quota() -> Any:
    """Per-client quota on new lists (``OPENCHECK_WATCHLIST_NEW_LISTS_PER_IP``)."""
    from .config import get_settings
    from .lookup_budget import Quota

    return Quota("watch-lists", lambda: get_settings().watchlist_new_lists_per_ip)


def valid_lei(value: str) -> str | None:
    lei = identifiers.normalise_lei(value)
    return lei if identifiers.is_valid_lei(lei) else None
