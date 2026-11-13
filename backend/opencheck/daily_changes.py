"""daily_changes — every LEI whose GLEIF or OpenSanctions record changed today.

The "Daily deltas for OpenCheck" ticket (13 Nov 2026), in **shadow mode**: the
list is computed and served as JSON for two weeks, with no page and no
downloadable BODS file, and the decision to build those is taken from the
numbers. See ``docs/daily-changes.md``.

The watchlist (``watchlist.py``) intersects the same two delta feeds with a
handful of watched LEIs and re-runs a lookup for each hit. This module reads
the feeds for *every* LEI and never runs a lookup: it is a list of what GLEIF
and OpenSanctions changed, graded, not a list of OpenCheck findings. Rules
that will be re-derived otherwise:

* **Nothing here costs an upstream call per entity.** The GLEIF side reads the
  mirror before and after ``mirror_refresh.apply_delta`` upserts a delta (the
  ``previous_direct_parents`` arrangement, for every material field). The
  OpenSanctions side reads the entity delta files the watchlist already reads,
  against a small local table of what each LEI-bearing organisation looked
  like before.
* **Its own grading, stricter than the watchlist's.** Measured on 6–13 Nov
  2026: the watchlist's "recheck" tier names ~1,500 reporting-exception moves
  a day and bulk re-keyings of the register number (1,700 on 13 Nov). Fine for
  one watched company, noise across 3.5M. So the bands here are
  :data:`BANDS`: ``headline`` (ownership and sanctions — Stephen, 13 Nov
  2026), ``ceased``, ``ceased_lapsed`` (an LEI that had already lapsed being
  retired: housekeeping), ``record`` (name, form, jurisdiction, country,
  other completed events), ``note`` and ``new``. Renewal churn is counted,
  never stored.
* **OpenSanctions is matched on ``leiCode`` only** (a name match across 3.5M
  LEIs is not viable) and organisation schemata only — person schemata are
  never read. A row carries references (entity id, topic codes, dataset
  names), never OpenSanctions' entity data: it is CC BY-NC.
* **Sole proprietors are left out** (GLEIF category ``SOLE_PROPRIETOR``): the
  legal name is a person's.
* **Never raises into its callers.** A failure here must not fail the mirror
  refresh or the watchlist.

State is one SQLite file (``OPENCHECK_DAILY_CHANGES_DB_FILE``); unset, the
feature is off. Days are UTC: a GLEIF change belongs to the day of the
publish that carried it, an OpenSanctions change to the day of its version.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from . import sqlite_schema
from .bods import gleif_events as _gleif_events
from .secret_scrub import describe_exception

log = logging.getLogger("opencheck.daily_changes")

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

HEADLINE = "headline"
CEASED = "ceased"
CEASED_LAPSED = "ceased_lapsed"
RECORD = "record"
NOTE = "note"
NEW = "new"

#: The bands, most notable first. A row carries the first band any of its
#: reasons reaches. ``new`` (a first-time LEI) is not a change and is listed
#: apart.
BANDS: tuple[str, ...] = (HEADLINE, CEASED, CEASED_LAPSED, RECORD, NOTE, NEW)
_RANK = {b: i for i, b in enumerate(BANDS)}

BAND_WORDS: dict[str, str] = {
    HEADLINE: "Ownership and sanctions",
    CEASED: "Ceased",
    CEASED_LAPSED: "Ceased, on an LEI that had already lapsed",
    RECORD: "Record changed",
    NOTE: "Note only",
    NEW: "New LEI",
}

#: The bands ``GET /daily-changes/{day}.json`` serves unless asked for more.
DEFAULT_BANDS: tuple[str, ...] = (HEADLINE, CEASED, CEASED_LAPSED, RECORD)

SOURCE_GLEIF = "gleif"
SOURCE_OPENSANCTIONS = "opensanctions"

#: reason -> band. A closed vocabulary: the counters and the JSON use these
#: keys and nothing else.
REASON_BAND: dict[str, str] = {
    # GLEIF — ownership
    "parent_changed": HEADLINE,
    "successor_named": HEADLINE,
    "merger_event": HEADLINE,
    # GLEIF — the entity ceased
    "entity_inactive": CEASED,
    "terminal_event": CEASED,
    # GLEIF — the record
    "legal_name": RECORD,
    "legal_form": RECORD,
    "jurisdiction": RECORD,
    "address_country": RECORD,
    "entity_status": RECORD,
    "event_completed": RECORD,
    "terminal_event_in_progress": RECORD,
    # GLEIF — note only
    "reporting_exception": NOTE,
    "register_identifier": NOTE,
    "registration_status": NOTE,
    "classification": NOTE,
    "creation_date": NOTE,
    "expiration": NOTE,
    "event_other": NOTE,
    "new_lei": NEW,
    # OpenSanctions
    "os_topic_added": HEADLINE,
    "os_list_added": HEADLINE,
    "os_new_listing": HEADLINE,
    "os_topic_removed": RECORD,
    "os_dataset_added": RECORD,
    "os_removed": RECORD,
}

#: GLEIF material field -> reason, for the fields whose reason does not depend
#: on the value. ``entity_status`` and ``corporate_events`` are graded below.
_FIELD_REASON: dict[str, str] = {
    "direct_parent_lei": "parent_changed",
    "ultimate_parent_lei": "parent_changed",
    "successors": "successor_named",
    "legal_name": "legal_name",
    "legal_form": "legal_form",
    "jurisdiction": "jurisdiction",
    "legal_address_country": "address_country",
    "hq_address_country": "address_country",
    "direct_exception": "reporting_exception",
    "ultimate_exception": "reporting_exception",
    "registered_at": "register_identifier",
    "registered_as": "register_identifier",
    "validated_at": "register_identifier",
    "registration_status": "registration_status",
    "category": "classification",
    "sub_category": "classification",
    "conformity_flag": "classification",
    "creation_date": "creation_date",
    "expiration_date": "expiration",
    "expiration_reason": "expiration",
}

#: Legal Entity Event types that are a change of hands rather than an ending.
_MERGER_TYPES: frozenset[str] = frozenset(
    {
        "MERGERS_AND_ACQUISITIONS",
        "ABSORPTION",
        "DEMERGER",
        "SPINOFF",
        "BREAKUP",
        "REVERSE_TAKEOVER",
        "ACQUISITION_BRANCH",
        "TRANSFER_BRANCH",
    }
)

_SOLE_PROPRIETOR = "SOLE_PROPRIETOR"

#: OpenSanctions schemata that are organisations (the watchlist's set).
OS_ORG_SCHEMATA = frozenset({"Company", "LegalEntity", "Organization", "PublicBody"})

#: Topics whose arrival on an LEI-bearing organisation heads the list.
OS_RISK_TOPICS: frozenset[str] = frozenset(
    {
        "sanction",
        "sanction.linked",
        "sanction.control",
        "export.control",
        "export.control.linked",
        "export.risk",
        "debarment",
        "reg.action",
        "reg.warn",
        "crime",
        "crime.fin",
        "crime.fraud",
        "crime.terror",
        "corp.disqual",
        "poi",
    }
)

#: Dataset tags (OpenSanctions' catalogue) that make a newly added dataset a
#: listing. ``list.sanction.counter`` is deliberately absent (Phase 105).
OS_LIST_TAGS: frozenset[str] = frozenset(
    {"list.sanction", "list.export", "list.debarment", "list.enforcement"}
)

OS_VERSIONS_URL = "https://data.opensanctions.org/artifacts/default/versions.json"
OS_DELTA_URL = "https://data.opensanctions.org/artifacts/default/{version}/entities.delta.json"
OS_FULL_URL = "https://data.opensanctions.org/artifacts/default/{version}/entities.ftm.json"
OS_CATALOG_URL = "https://data.opensanctions.org/datasets/latest/index.json"
OS_ENTITY_URL = "https://www.opensanctions.org/entities/{id}/"
OS_MAX_VERSIONS_PER_TICK = 12

OS_NOTICE = (
    "OpenSanctions rows are references to OpenSanctions records (entity id, topic codes, "
    "dataset names), matched on the LEI OpenSanctions holds. OpenSanctions data is licensed "
    "CC BY-NC 4.0: https://www.opensanctions.org/licensing/"
)
WHAT_THIS_IS = (
    "LEIs whose GLEIF record (Golden Copy delta) or OpenSanctions record changed on this UTC "
    "day, graded by OpenCheck. It lists what those two publishers changed; no OpenCheck check "
    "was run for these entities. Shadow experiment: the shape may change."
)


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def worst(bands: Iterable[str]) -> str | None:
    """The most notable of ``bands`` (:data:`BANDS` order), or ``None``."""
    found = [b for b in bands if b in _RANK]
    return min(found, key=_RANK.__getitem__) if found else None


# ---------------------------------------------------------------------------
# GLEIF grading (pure)
# ---------------------------------------------------------------------------


def _event_key(e: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(e.get("type") or ""),
        str(e.get("status") or ""),
        str(e.get("effective") or ""),
        str(e.get("recorded") or ""),
    )


def added_events(old: Any, new: Any) -> list[dict[str, Any]]:
    """The events in ``new`` that ``old`` does not hold (a status moving from
    IN_PROGRESS to COMPLETED is a new event by this reading)."""
    seen = {_event_key(e) for e in (old or []) if isinstance(e, dict)}
    return [e for e in (new or []) if isinstance(e, dict) and _event_key(e) not in seen]


def event_reason(event: dict[str, Any]) -> str:
    etype = str(event.get("type") or "").upper()
    status = str(event.get("status") or "").upper()
    terminal = etype in _gleif_events.TERMINAL_TYPES
    if status == "COMPLETED":
        if etype in _MERGER_TYPES:
            return "merger_event"
        return "terminal_event" if terminal else "event_completed"
    if status == "WITHDRAWN_CANCELLED":
        return "event_other"
    # IN_PROGRESS, or a status this table does not know: never as completed.
    return "terminal_event_in_progress" if terminal else "event_other"


def grade_gleif(
    before: dict[str, Any] | None, after: dict[str, Any] | None
) -> dict[str, Any] | None:
    """One LEI's material facts before and after a delta, as a row to store:
    ``{"band", "reasons", "changes"}``. ``None`` when nothing material moved
    (renewal churn) or the mirror no longer holds the LEI. A LEI the mirror
    did not hold before is ``new``."""
    if after is None:
        return None
    if before is None:
        return {"band": NEW, "reasons": ["new_lei"], "changes": []}
    from .watchlist import diff_gleif_facts

    reasons: list[str] = []
    changes: list[dict[str, Any]] = []
    for change in diff_gleif_facts(before, after):
        field = str(change.get("field") or "")
        old, new = change.get("old"), change.get("new")
        if field == "direct_children":
            continue  # the child's own parent change is the row
        if field == "corporate_events":
            events = added_events(old, new)
            if not events:
                continue  # an event withdrawn from the record, or re-ordered
            reasons.extend(event_reason(e) for e in events)
            changes.append({"field": field, "new": events})
            continue
        if field == "entity_status":
            inactive = str(new or "").upper() == "INACTIVE"
            reasons.append("entity_inactive" if inactive else "entity_status")
        elif field == "successors":
            # A successor withdrawn is a correction, not a change of hands.
            reasons.append("successor_named" if new else "event_other")
        else:
            reasons.append(_FIELD_REASON.get(field, "event_other"))
        changes.append({"field": field, "old": old, "new": new})
    if not reasons:
        return None
    reasons = sorted(set(reasons), key=lambda r: (_RANK[REASON_BAND[r]], r))
    band = worst(REASON_BAND[r] for r in reasons) or NOTE
    if band == CEASED and str(before.get("registration_status") or "").upper() == "LAPSED":
        # The LEI had lapsed before GLEIF recorded the ending: the company
        # ceased some time ago and the record is catching up.
        band = CEASED_LAPSED
    return {"band": band, "reasons": reasons, "changes": changes}


def is_sole_proprietor(*facts: dict[str, Any] | None) -> bool:
    return any(str((f or {}).get("category") or "").upper() == _SOLE_PROPRIETOR for f in facts)


# ---------------------------------------------------------------------------
# OpenSanctions grading (pure)
# ---------------------------------------------------------------------------


def os_projection(entity: dict[str, Any]) -> dict[str, Any] | None:
    """What the store remembers of an OpenSanctions entity: its LEIs, topics
    and datasets. ``None`` unless it is an organisation carrying an LEI."""
    if entity.get("schema") not in OS_ORG_SCHEMATA:
        return None
    props = entity.get("properties") or {}
    leis = sorted({str(v).strip().upper() for v in props.get("leiCode") or [] if v})
    leis = [x for x in leis if len(x) == 20 and x.isalnum()]
    if not leis:
        return None
    return {
        "leis": leis,
        "topics": sorted({str(t) for t in props.get("topics") or [] if t}),
        "datasets": sorted({str(d) for d in entity.get("datasets") or [] if d}),
    }


def grade_os(
    op: str,
    prior: dict[str, Any] | None,
    current: dict[str, Any] | None,
    *,
    list_datasets: frozenset[str] | None = None,
) -> dict[str, Any] | None:
    """One delta record, as ``{"band", "reasons", "changes"}`` or ``None``
    when nothing this list reads moved. ``prior`` / ``current`` are
    :func:`os_projection` values; ``list_datasets`` the catalogue's datasets
    tagged as a list (``None`` when the catalogue could not be read — a new
    dataset is then ``os_dataset_added``, never a listing)."""
    op = str(op or "").upper()
    reasons: list[str] = []
    changes: dict[str, Any] = {}
    if op == "DEL" or current is None:
        if prior and set(prior["topics"]) & OS_RISK_TOPICS:
            reasons.append("os_removed")
            changes["topics_removed"] = sorted(set(prior["topics"]) & OS_RISK_TOPICS)
    elif prior is None:
        risk = sorted(set(current["topics"]) & OS_RISK_TOPICS)
        if risk:
            reasons.append("os_new_listing")
            changes["topics_added"] = risk
    else:
        added = sorted((set(current["topics"]) - set(prior["topics"])) & OS_RISK_TOPICS)
        removed = sorted((set(prior["topics"]) - set(current["topics"])) & OS_RISK_TOPICS)
        new_ds = sorted(
            d for d in set(current["datasets"]) - set(prior["datasets"]) if not d.startswith("ext_")
        )
        if added:
            reasons.append("os_topic_added")
            changes["topics_added"] = added
        if removed:
            reasons.append("os_topic_removed")
            changes["topics_removed"] = removed
        if new_ds:
            lists = [d for d in new_ds if list_datasets and d in list_datasets]
            reasons.append("os_list_added" if lists else "os_dataset_added")
            changes["datasets_added"] = new_ds
    if not reasons:
        return None
    reasons = sorted(set(reasons), key=lambda r: (_RANK[REASON_BAND[r]], r))
    return {
        "band": worst(REASON_BAND[r] for r in reasons) or RECORD,
        "reasons": reasons,
        "changes": [changes],
    }


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS changes (
    day TEXT NOT NULL,
    lei TEXT NOT NULL,
    source TEXT NOT NULL,
    stamp TEXT NOT NULL,
    band TEXT NOT NULL,
    reasons TEXT NOT NULL,
    name TEXT,
    jurisdiction TEXT,
    payload TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (day, lei, source, stamp)
);
CREATE INDEX IF NOT EXISTS idx_changes_day_band ON changes (day, band);
CREATE TABLE IF NOT EXISTS day_counts (
    day TEXT NOT NULL,
    source TEXT NOT NULL,
    key TEXT NOT NULL,
    n INTEGER NOT NULL,
    PRIMARY KEY (day, source, key)
);
CREATE TABLE IF NOT EXISTS os_entities (
    id TEXT PRIMARY KEY,
    state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
)
"""

MIGRATIONS: tuple[sqlite_schema.Migration, ...] = (
    sqlite_schema.Migration(1, "the shadow list", sqlite_schema.statements(_SCHEMA)),
)

#: The counter keys ``day_counts`` holds beside the bands. Closed.
COUNT_KEYS: tuple[str, ...] = (
    "named",  # LEIs a GLEIF delta named / OpenSanctions records read
    "churn",  # named, nothing material moved
    "sole_proprietor",  # left out
    "not_an_lei_here",  # an OpenSanctions LEI the mirror does not hold
    "gap",  # OpenSanctions versions that could not be read
)


class DailyChangesStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            sqlite_schema.migrate(conn, self.path, MIGRATIONS)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()

    # -- writes ---------------------------------------------------------------
    def record(
        self,
        day: str,
        source: str,
        stamp: str,
        rows: Iterable[dict[str, Any]],
        counts: dict[str, int] | None = None,
    ) -> int:
        """Store ``rows`` (``lei``, ``band``, ``reasons``, ``changes``, and
        optionally ``name`` / ``jurisdiction`` / ``ref``) and add ``counts``
        to the day. A row already stored under the same stamp is left alone,
        so a delta applied twice counts once."""
        now = _now_iso()
        written = 0
        bands: Counter[str] = Counter()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for row in rows:
                    payload = {"changes": row.get("changes") or []}
                    if row.get("ref"):
                        payload["ref"] = row["ref"]
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO changes (day, lei, source, stamp, band, reasons, "
                        "name, jurisdiction, payload, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (
                            day, row["lei"], source, stamp, row["band"],
                            json.dumps(row["reasons"]), row.get("name"), row.get("jurisdiction"),
                            json.dumps(payload, sort_keys=True, default=str), now,
                        ),
                    )
                    if cur.rowcount:
                        written += 1
                        bands[row["band"]] += 1
                first = conn.execute(
                    "SELECT 1 FROM day_counts WHERE day = ? AND source = ? AND key = ?",
                    (day, source, f"stamp:{stamp}"),
                ).fetchone() is None
                totals: Counter[str] = Counter({f"band:{b}": n for b, n in bands.items()})
                if first:
                    # The delta's own counters are added once per stamp.
                    totals.update({k: v for k, v in (counts or {}).items() if k in COUNT_KEYS and v})
                    totals[f"stamp:{stamp}"] = 1
                for key, n in totals.items():
                    conn.execute(
                        "INSERT INTO day_counts (day, source, key, n) VALUES (?,?,?,?) "
                        "ON CONFLICT(day, source, key) DO UPDATE SET n = n + excluded.n",
                        (day, source, key, n),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return written

    def prune(self, keep_days: int, *, today: str | None = None) -> int:
        if keep_days <= 0:
            return 0
        t = datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=UTC) if today else datetime.now(UTC)
        cutoff = (t - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        with self._conn() as conn:
            n = conn.execute("DELETE FROM changes WHERE day < ?", (cutoff,)).rowcount
            conn.execute("DELETE FROM day_counts WHERE day < ?", (cutoff,))
        return n

    def get_meta(self, key: str) -> str | None:
        with self._conn() as conn:
            r = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return r["value"] if r else None

    def set_meta(self, key: str, value: str | None) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    # -- OpenSanctions state --------------------------------------------------
    def os_get(self, entity_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            r = conn.execute("SELECT state FROM os_entities WHERE id = ?", (entity_id,)).fetchone()
        return json.loads(r["state"]) if r else None

    def os_put(self, entity_id: str, state: dict[str, Any] | None) -> None:
        with self._conn() as conn:
            if state is None:
                conn.execute("DELETE FROM os_entities WHERE id = ?", (entity_id,))
            else:
                conn.execute(
                    "INSERT INTO os_entities (id, state) VALUES (?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET state = excluded.state",
                    (entity_id, json.dumps(state, sort_keys=True)),
                )

    def os_replace_all(self, states: dict[str, dict[str, Any]]) -> None:
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM os_entities")
                conn.executemany(
                    "INSERT INTO os_entities (id, state) VALUES (?, ?)",
                    [(k, json.dumps(v, sort_keys=True)) for k, v in states.items()],
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def os_count(self) -> int:
        with self._conn() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM os_entities").fetchone()[0])

    # -- reads ----------------------------------------------------------------
    def days(self) -> list[dict[str, Any]]:
        """Every day held, newest first, with its counts by source."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT day, source, key, n FROM day_counts WHERE key NOT LIKE 'stamp:%' "
                "ORDER BY day DESC, source, key"
            ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            day = out.setdefault(r["day"], {"day": r["day"], "bands": {}, "sources": {}})
            src = day["sources"].setdefault(r["source"], {})
            if r["key"].startswith("band:"):
                band = r["key"][5:]
                src[band] = r["n"]
                day["bands"][band] = day["bands"].get(band, 0) + r["n"]
            else:
                src[r["key"]] = r["n"]
        return list(out.values())

    def day(self, day: str, bands: Iterable[str]) -> list[dict[str, Any]]:
        wanted = [b for b in bands if b in _RANK]
        if not wanted:
            return []
        marks = ",".join("?" * len(wanted))
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM changes WHERE day = ? AND band IN ({marks}) "  # noqa: S608 — marks only
                "ORDER BY lei, source, stamp",
                (day, *wanted),
            ).fetchall()
        out = []
        for r in rows:
            payload = json.loads(r["payload"])
            row = {
                "lei": r["lei"],
                "name": r["name"],
                "jurisdiction": r["jurisdiction"],
                "source": r["source"],
                "band": r["band"],
                "reasons": json.loads(r["reasons"]),
                "changes": payload.get("changes") or [],
                "as_of": r["stamp"],
            }
            if payload.get("ref"):
                row["ref"] = payload["ref"]
            out.append(row)
        out.sort(key=lambda x: (_RANK[x["band"]], x["lei"], x["source"], x["as_of"]))
        return out


_store: DailyChangesStore | None = None
_store_lock = threading.Lock()


def get_store() -> DailyChangesStore | None:
    """The configured store, or ``None`` when the feature is off."""
    global _store
    from .config import get_settings

    path = get_settings().daily_changes_db_file
    if not path:
        return None
    with _store_lock:
        if _store is None or _store.path != Path(path):
            _store = DailyChangesStore(Path(path))
        return _store


def reset_for_tests() -> None:
    global _store
    with _store_lock:
        _store = None
    with _state_lock:
        _state.clear()


_state: dict[str, Any] = {}
_state_lock = threading.Lock()


def _note(**fields: Any) -> None:
    with _state_lock:
        _state.update(fields)


# ---------------------------------------------------------------------------
# GLEIF: before and after a delta
# ---------------------------------------------------------------------------


class _FactsView:
    """The mirror, as :func:`watchlist.gleif_facts` reads it, without the
    direct-subsidiaries fact: one query per LEI that this list does not use
    (a child's own parent change is the row)."""

    has_relationships = False

    def __init__(self, store: Any) -> None:
        self._store = store
        self.carries_events = getattr(store, "carries_events", False)

    def get(self, lei: str) -> Any:
        return self._store.get(lei)

    def exceptions(self, lei: str) -> Any:
        return self._store.exceptions(lei)


def _facts(mirror: Any, leis: Iterable[str]) -> dict[str, dict[str, Any] | None]:
    from .watchlist import gleif_facts

    view = _FactsView(mirror)
    return {lei: gleif_facts(view, lei) for lei in leis}


def capture_before(named: Iterable[str]) -> dict[str, dict[str, Any] | None] | None:
    """The material facts of every LEI a delta is about to touch, read from
    the mirror before the upsert. ``None`` when the feature is off or the
    mirror is not there. Never raises."""
    try:
        if get_store() is None:
            return None
        from . import entity_pages as ep

        mirror = ep.get_store()
        if mirror is None:
            return None
        return _facts(mirror, sorted({str(x).strip().upper() for x in named if x}))
    except Exception as exc:  # noqa: BLE001 — the refresh must not fail because this did
        log.warning(
            "daily changes: could not read the mirror before the delta: %s", describe_exception(exc)
        )
        _note(last_error=f"gleif before: {describe_exception(exc)}")
        return None


def publish_day(publish_label: str) -> str:
    """The UTC day a publish belongs to (``2026-11-13 08:00:00`` → that day)."""
    return str(publish_label or "").strip()[:10]


def on_gleif_delta(
    before: dict[str, dict[str, Any] | None] | None, publish_label: str
) -> dict[str, int]:
    """Called by ``mirror_refresh.apply_delta`` after the watermark is
    written, with what :func:`capture_before` read. Re-reads every LEI,
    grades the difference and stores the day's rows. Never raises."""
    result = {"named": 0, "stored": 0, "churn": 0}
    if before is None:
        return result
    try:
        store = get_store()
        if store is None:
            return result
        from . import entity_pages as ep

        mirror = ep.get_store()
        if mirror is None:
            return result
        after = _facts(mirror, before.keys())
        rows: list[dict[str, Any]] = []
        counts: Counter[str] = Counter(named=len(before))
        for lei, old in before.items():
            new = after.get(lei)
            graded = grade_gleif(old, new)
            if graded is None:
                counts["churn"] += 1
                continue
            if is_sole_proprietor(old, new):
                counts["sole_proprietor"] += 1
                continue
            rows.append(
                {
                    "lei": lei,
                    "name": (new or {}).get("legal_name"),
                    "jurisdiction": (new or {}).get("jurisdiction"),
                    **graded,
                }
            )
        day = publish_day(publish_label)
        stored = store.record(day, SOURCE_GLEIF, publish_label, rows, dict(counts))
        result.update(named=len(before), stored=stored, churn=counts["churn"])
        _note(gleif_last_publish=publish_label, gleif_last_stored=stored, last_error=None)
        log.info(
            "daily changes: GLEIF %s named %d LEIs — %d stored, %d churn",
            publish_label, len(before), stored, counts["churn"],
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("daily changes: GLEIF delta hook failed: %s", describe_exception(exc))
        _note(last_error=f"gleif: {describe_exception(exc)}")
    return result


# ---------------------------------------------------------------------------
# OpenSanctions: the entity deltas
# ---------------------------------------------------------------------------


class _DeltaGone(Exception):
    pass


def version_day(version: str) -> str:
    """``20261113065434-idv`` → ``2026-11-13``."""
    v = str(version)
    return f"{v[0:4]}-{v[4:6]}-{v[6:8]}"


def _os_versions(client: Any) -> list[str]:
    r = client.get(OS_VERSIONS_URL, timeout=60.0)
    r.raise_for_status()
    return sorted({str(v) for v in (r.json().get("items") or [])})


def _os_list_datasets(client: Any) -> frozenset[str] | None:
    """The datasets the catalogue tags as a sanctions, export-control,
    debarment or enforcement list; ``None`` when it cannot be read."""
    try:
        r = client.get(OS_CATALOG_URL, timeout=120.0)
        r.raise_for_status()
        return frozenset(
            str(d.get("name"))
            for d in (r.json().get("datasets") or [])
            if set(d.get("tags") or []) & OS_LIST_TAGS
        )
    except Exception as exc:  # noqa: BLE001 — graded lower, never guessed
        log.warning("daily changes: OpenSanctions catalogue not read: %s", describe_exception(exc))
        return None


def seed_states(lines: Iterable[str | bytes]) -> dict[str, dict[str, Any]]:
    """Every LEI-bearing organisation in a full ``entities.ftm.json`` stream,
    as ``{entity id: projection}``. Lines without ``leiCode`` are not parsed."""
    out: dict[str, dict[str, Any]] = {}
    for raw in lines:
        text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        if '"leiCode"' not in text:
            continue
        try:
            entity = json.loads(text)
        except json.JSONDecodeError:
            continue
        state = os_projection(entity)
        if state and entity.get("id"):
            out[str(entity["id"])] = state
    return out


def _seed(client: Any, store: DailyChangesStore, version: str) -> int:
    with client.stream("GET", OS_FULL_URL.format(version=version), timeout=1800.0) as r:
        r.raise_for_status()
        states = seed_states(r.iter_lines())
    store.os_replace_all(states)
    store.set_meta("opensanctions_version", version)
    store.set_meta("opensanctions_seeded_at", _now_iso())
    log.info("daily changes: OpenSanctions baseline %s — %d LEI-bearing organisations", version, len(states))
    return len(states)


def scan_os_delta(
    lines: Iterable[str],
    store: DailyChangesStore,
    *,
    list_datasets: frozenset[str] | None,
    mirror: Any | None,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    """Grade one delta file against the stored projections and move them on.
    Returns the rows to store and the counters."""
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for raw in lines:
        raw = raw.strip() if isinstance(raw, str) else raw.decode("utf-8", "replace").strip()
        if not raw:
            continue
        try:
            rec = json.loads(raw)
        except json.JSONDecodeError:
            continue
        entity = rec.get("entity") or {}
        entity_id = str(entity.get("id") or "")
        op = str(rec.get("op") or "").upper()
        if not entity_id:
            continue
        prior = store.os_get(entity_id)
        current = None if op == "DEL" else os_projection(entity)
        if prior is None and current is None:
            continue  # not an LEI-bearing organisation, before or after
        counts["named"] += 1
        graded = grade_os(op, prior, current, list_datasets=list_datasets)
        store.os_put(entity_id, current)
        if graded is None:
            counts["churn"] += 1
            continue
        ref = {"opensanctions_id": entity_id, "url": OS_ENTITY_URL.format(id=entity_id)}
        for lei in sorted(set((current or prior or {}).get("leis") or [])):
            row = mirror.get(lei) if mirror is not None else None
            if mirror is not None and row is None:
                counts["not_an_lei_here"] += 1
                continue
            if row is not None and str((row.detail or {}).get("category") or "").upper() == _SOLE_PROPRIETOR:
                counts["sole_proprietor"] += 1
                continue
            rows.append(
                {
                    "lei": lei,
                    "name": getattr(row, "name", None),
                    "jurisdiction": getattr(row, "jurisdiction", None),
                    "ref": ref,
                    **graded,
                }
            )
    return rows, counts


def _read_os_delta(
    client: Any, store: DailyChangesStore, version: str, list_datasets: frozenset[str] | None
) -> int:
    from . import entity_pages as ep

    with client.stream("GET", OS_DELTA_URL.format(version=version), timeout=600.0) as r:
        if r.status_code == 404:
            raise _DeltaGone(version)
        r.raise_for_status()
        rows, counts = scan_os_delta(
            r.iter_lines(), store, list_datasets=list_datasets, mirror=ep.get_store()
        )
    return store.record(version_day(version), SOURCE_OPENSANCTIONS, version, rows, dict(counts))


def opensanctions_tick(client: Any | None = None) -> dict[str, Any]:
    """Read the OpenSanctions versions published since the last one seen,
    oldest first. The first run takes a baseline from the full export and
    stores no rows; a version that can no longer be read (aged out of
    ``versions.json``, or a 404) is a counted gap and a fresh baseline, since
    a difference across a gap cannot be dated. Runs on a thread; never
    raises."""
    result: dict[str, Any] = {"versions": 0, "stored": 0}
    store = get_store()
    if store is None:
        return result
    import httpx

    own = client is None
    client = client or httpx.Client(
        follow_redirects=True, headers={"User-Agent": "OpenCheck daily changes"}
    )
    try:
        versions = _os_versions(client)
        if not versions:
            return result
        last = store.get_meta("opensanctions_version")
        if last is None:
            result["seeded"] = _seed(client, store, versions[-1])
            _note(os_last_version=versions[-1], os_last_error=None)
            return result
        pending = [v for v in versions if v > last]
        if pending and last not in versions and last < versions[0]:
            store.record(version_day(versions[-1]), SOURCE_OPENSANCTIONS, f"gap:{last}", [], {"gap": 1})
            result["seeded"] = _seed(client, store, versions[-1])
            result["gaps"] = 1
            _note(os_last_version=versions[-1], os_last_error=None)
            return result
        list_datasets = _os_list_datasets(client) if pending else None
        for version in pending[:OS_MAX_VERSIONS_PER_TICK]:
            try:
                stored = _read_os_delta(client, store, version, list_datasets)
            except _DeltaGone:
                store.record(version_day(version), SOURCE_OPENSANCTIONS, f"gap:{version}", [], {"gap": 1})
                result["seeded"] = _seed(client, store, versions[-1])
                result["gaps"] = result.get("gaps", 0) + 1
                break
            store.set_meta("opensanctions_version", version)
            result["versions"] += 1
            result["stored"] += stored
            _note(os_last_version=version, os_last_error=None)
        result["backlog"] = max(0, len(pending) - OS_MAX_VERSIONS_PER_TICK)
    except Exception as exc:  # noqa: BLE001
        log.warning("daily changes: OpenSanctions tick failed: %s", describe_exception(exc))
        _note(os_last_error=describe_exception(exc))
    finally:
        if own:
            client.close()
    return result


# ---------------------------------------------------------------------------
# The worker
# ---------------------------------------------------------------------------


def tick() -> dict[str, Any]:
    """One pass: the OpenSanctions versions, then the retention prune."""
    from .config import get_settings

    store = get_store()
    if store is None:
        return {}
    settings = get_settings()
    result = opensanctions_tick() if settings.allow_live else {}
    try:
        result["pruned"] = store.prune(settings.daily_changes_keep_days)
    except Exception as exc:  # noqa: BLE001
        log.warning("daily changes: prune failed: %s", describe_exception(exc))
    _note(last_tick_at=_now_iso())
    return result


async def loop(interval_s: float) -> None:
    """Run :func:`tick` every ``interval_s`` seconds until cancelled; the
    first one after a minute, so a deploy does not wait hours for a baseline."""
    delay = min(60.0, interval_s)
    while True:
        await asyncio.sleep(delay)
        delay = interval_s
        try:
            await asyncio.to_thread(tick)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("daily changes: unexpected error")


# ---------------------------------------------------------------------------
# What the routes and /signalstats serve
# ---------------------------------------------------------------------------


def index_payload() -> dict[str, Any]:
    store = get_store()
    if store is None:
        return {"available": False, "days": []}
    return {
        "available": True,
        "what_this_is": WHAT_THIS_IS,
        "bands": [{"band": b, "label": BAND_WORDS[b]} for b in BANDS],
        "days": store.days(),
    }


def day_payload(day: str, bands: Iterable[str] | None = None) -> dict[str, Any] | None:
    """One day's list, or ``None`` when the feature is off. A day with no
    rows is answered (``rows: []``), never a 404: an empty weekend is a
    result."""
    store = get_store()
    if store is None:
        return None
    wanted = [b for b in (bands or DEFAULT_BANDS) if b in _RANK]
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    summary = next((d for d in store.days() if d["day"] == day), None)
    rows = store.day(day, wanted)
    return {
        "day": day,
        "complete": day < today,
        "what_this_is": WHAT_THIS_IS,
        "opensanctions_notice": OS_NOTICE,
        "bands_served": wanted,
        "band_labels": {b: BAND_WORDS[b] for b in BANDS},
        "counts": (summary or {}).get("bands", {}),
        "sources": (summary or {}).get("sources", {}),
        "rows": rows,
    }


def stats() -> dict[str, Any]:
    """Aggregate only, for ``/signalstats``: days and counts by closed keys —
    never an LEI or a name."""
    store = get_store()
    with _state_lock:
        state = dict(_state)
    if store is None:
        return {"enabled": False}
    try:
        days = store.days()[:14]
        baseline = store.os_count()
        version = store.get_meta("opensanctions_version")
    except Exception as exc:  # noqa: BLE001
        return {"enabled": True, "error": type(exc).__name__}
    return {
        "enabled": True,
        "days": days,
        "opensanctions_baseline": baseline,
        "opensanctions_version": version,
        "gleif_last_publish": state.get("gleif_last_publish"),
        "last_tick_at": state.get("last_tick_at"),
        "last_error": state.get("last_error"),
        "os_last_error": state.get("os_last_error"),
    }
