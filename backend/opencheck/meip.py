"""OECD-UNSD MEIP — the register of the 500 largest groups, as BODS.

MEIP (the OECD-UNSD Multinational Enterprise Information Platform) publishes an
annual "Global Register" of the subsidiaries of the world's 500 largest
multinational enterprises — 126,658 entities and 126,158 group-membership
relationships in the 31 December 2024 edition — and since September 2026
publishes it as **BODS v0.4** as well as a spreadsheet. Phase 208 made it a
registered source (``sources/meip.py``): the OECD's own statements, with their
own ``statementId`` / ``recordId`` / ``source`` / ``publicationDetails``, flow
through the lookup unmodified. This module is the store those statements are
read from, and the Subsidiaries tab's list.

Two backing files, one preferred:

* ``meip.sqlite`` — the whole register, built from the OECD's BODS file plus
  the spreadsheet by ``scripts/build_meip.py``, published as the
  ``meip-bods-2024`` release asset and downloaded at boot (the entity-pages /
  PSC-graph arrangement, :func:`warm_meip_db`). Statements are rebuilt from
  columns and a per-row ``recordDetails`` blob; the envelope every statement
  shares (``statementDate``, ``source``, ``publicationDetails``,
  ``recordStatus``) is stored once in ``meta`` and put back on each one.
* ``data/meip_subsidiaries.json`` + ``data/meip_mne_heads.json`` — the
  committed LEI-keyed subset from the spreadsheet (Phase 69). Only the
  Subsidiaries tab falls back to them, and it says so: without the SQLite
  file the ``meip`` *source* covers nothing, rather than inventing statements
  the OECD did not publish.

What a lookup gets (:meth:`MeipStore.bundle_for_lei`): every MEIP entity
record carrying the subject LEI (a LEI can sit on more than one record —
154 do, 113 of them in two different groups, and both memberships are kept
and said), the subject's upward relationship to its group head and the head's
entity statement in subsidiary mode, the head's own statement in head mode.
A head's children never enter the graph; they are the tab's list
(:func:`meip_declared`).
"""

from __future__ import annotations

import base64
import json
import logging
import sqlite3
import threading
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .cache import data_root

log = logging.getLogger(__name__)

_DATA = Path(__file__).parent / "data"

#: The register's home — where the OECD publishes the spreadsheet and the BODS
#: zip, and the ``source.url`` on every statement in the file.
MEIP_URL = (
    "https://www.oecd.org/en/data/dashboards/"
    "oecd-unsd-multinational-enterprise-information-platform.html"
)

#: Release asset the SQLite file is downloaded from when absent. Rebuilt by
#: ``scripts/build_meip.py`` when the OECD publishes a new edition.
MEIP_DB_URL_DEFAULT = (
    "https://github.com/StephenAbbott/opencheck/releases/download/"
    "meip-bods-2024/meip.sqlite.gz"
)

#: ``meta.schema`` this module reads. A file with another value is refused
#: (logged, treated as absent) rather than misread.
SCHEMA_VERSION = "1"

#: Number of MNE groups the register covers — the OECD's framing, and the
#: figure the head-mode finding sentence uses.
GROUP_COUNT = 500
#: The register edition this build serves, as shown in fixed text (the source
#: description). Bump with the release asset; the store's own ``edition`` meta
#: is what the finding and provenance use.
EDITION_LABEL = "31 December 2024"


# ---------------------------------------------------------------------------
# The SQLite store
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MeipEntity:
    """One entity record of the register (an LEI may have several)."""

    record_id: str
    statement_id: str
    group_rid: str
    is_head: bool
    name: str
    jurisdiction: str | None
    lei: str | None
    via_name: str | None
    via_rid: str | None


class MeipStore:
    """Read-only view of ``meip.sqlite``. One instance per process, opened
    lazily; every method takes and releases its own connection, so the store
    is safe to call from ``asyncio.to_thread``."""

    def __init__(self, path: Path):
        self.path = path
        self._meta: dict[str, str] = {}
        self._zdict = b""
        self._envelope: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._loaded = False

    # -- lifecycle ---------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def load(self) -> bool:
        """Read ``meta`` once. False when the file is absent or not ours."""
        with self._lock:
            if self._loaded:
                return bool(self._meta)
            self._loaded = True
            if not self.path.exists():
                return False
            try:
                conn = self._connect()
                try:
                    meta = dict(conn.execute("SELECT key, value FROM meta"))
                finally:
                    conn.close()
            except sqlite3.Error as exc:
                log.warning("meip: %s could not be opened: %s", self.path, exc)
                return False
            if meta.get("schema") != SCHEMA_VERSION:
                log.warning(
                    "meip: %s has schema %r, this build reads %r — ignoring it",
                    self.path, meta.get("schema"), SCHEMA_VERSION,
                )
                return False
            self._meta = meta
            self._zdict = base64.b64decode(meta.get("zdict", ""))
            self._envelope = json.loads(meta.get("envelope") or "{}")
            log.info(
                "meip: %s loaded — %s statements, edition %s, built %s",
                self.path, meta.get("statements"), meta.get("edition"), meta.get("built_at"),
            )
            return True

    @property
    def available(self) -> bool:
        return self.load()

    @property
    def meta(self) -> dict[str, str]:
        """The file's ``meta`` table (empty when the file is absent)."""
        self.load()
        return dict(self._meta)

    @property
    def edition(self) -> str:
        """Register reference date, ISO (``2024-12-31``)."""
        return self._meta.get("edition", "") if self.load() else ""

    @property
    def built_at(self) -> datetime | None:
        raw = self._meta.get("built_at") if self.load() else None
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    # -- rows ---------------------------------------------------------------

    @staticmethod
    def _entity(row: sqlite3.Row) -> MeipEntity:
        return MeipEntity(
            record_id=row["record_id"],
            statement_id=bytes(row["statement_id"]).hex(),
            group_rid=row["group_rid"],
            is_head=bool(row["is_head"]),
            name=row["name"] or "",
            jurisdiction=row["jurisdiction"],
            lei=row["lei"],
            via_name=row["via_name"],
            via_rid=row["via_rid"],
        )

    _ENTITY_COLS = (
        "record_id, statement_id, group_rid, is_head, name, jurisdiction, lei, via_name, via_rid"
    )

    def covers(self, lei: str | None) -> bool:
        key = (lei or "").strip().upper()
        if not key or not self.load():
            return False
        conn = self._connect()
        try:
            return conn.execute("SELECT 1 FROM entity WHERE lei = ? LIMIT 1", (key,)).fetchone() is not None
        finally:
            conn.close()

    def records(self, lei: str | None) -> list[MeipEntity]:
        """Every entity record carrying this LEI, file order."""
        key = (lei or "").strip().upper()
        if not key or not self.load():
            return []
        conn = self._connect()
        try:
            rows = conn.execute(
                f"SELECT {self._ENTITY_COLS} FROM entity WHERE lei = ? ORDER BY rowid", (key,)
            ).fetchall()
        finally:
            conn.close()
        return [self._entity(r) for r in rows]

    def entity(self, record_id: str) -> MeipEntity | None:
        if not self.load():
            return None
        conn = self._connect()
        try:
            row = conn.execute(
                f"SELECT {self._ENTITY_COLS} FROM entity WHERE record_id = ?", (record_id,)
            ).fetchone()
        finally:
            conn.close()
        return self._entity(row) if row else None

    def group_children(self, group_rid: str) -> list[MeipEntity]:
        """Every non-head entity of a group, file order (the register's order)."""
        if not self.load():
            return []
        conn = self._connect()
        try:
            rows = conn.execute(
                f"SELECT {self._ENTITY_COLS} FROM entity WHERE group_rid = ? AND is_head = 0 ORDER BY rowid",
                (group_rid,),
            ).fetchall()
        finally:
            conn.close()
        return [self._entity(r) for r in rows]

    def group_size(self, group_rid: str) -> tuple[int, int]:
        """(subsidiaries, subsidiaries with an LEI) for a group head."""
        if not self.load():
            return (0, 0)
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*), SUM(lei IS NOT NULL) FROM entity WHERE group_rid = ? AND is_head = 0",
                (group_rid,),
            ).fetchone()
        finally:
            conn.close()
        return (int(row[0] or 0), int(row[1] or 0))

    # -- statements ---------------------------------------------------------

    def _envelope_fields(self) -> dict[str, Any]:
        env = self._envelope
        out: dict[str, Any] = {}
        if env.get("statementDate"):
            out["statementDate"] = env["statementDate"]
        out["recordStatus"] = env.get("recordStatus") or "new"
        if env.get("source") is not None:
            out["source"] = json.loads(json.dumps(env["source"]))
        if env.get("publicationDetails") is not None:
            out["publicationDetails"] = json.loads(json.dumps(env["publicationDetails"]))
        return out

    def entity_statement(self, record_id: str) -> dict[str, Any] | None:
        """The OECD's entity statement for a record, exactly as published."""
        if not self.load():
            return None
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT statement_id, group_rid, details, annotations FROM entity WHERE record_id = ?",
                (record_id,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        d = zlib.decompressobj(zdict=self._zdict)
        details = json.loads(d.decompress(bytes(row["details"])) + d.flush())
        env = self._envelope_fields()
        stmt: dict[str, Any] = {
            "statementId": bytes(row["statement_id"]).hex(),
            "statementDate": env.get("statementDate"),
            "declarationSubject": row["group_rid"],
            "recordId": record_id,
            "recordType": "entity",
            "recordStatus": env["recordStatus"],
            "recordDetails": details,
        }
        if "source" in env:
            stmt["source"] = env["source"]
        if "publicationDetails" in env:
            stmt["publicationDetails"] = env["publicationDetails"]
        if row["annotations"]:
            stmt["annotations"] = json.loads(row["annotations"])
        if stmt["statementDate"] is None:
            del stmt["statementDate"]
        return stmt

    def relationships_for_subject(self, record_id: str) -> list[dict[str, Any]]:
        """The OECD's relationship statements whose subject is this record."""
        if not self.load():
            return []
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT statement_id, record_id, subject_rid, ip_rid, details_text, annotations "
                "FROM relationship WHERE subject_rid = ? ORDER BY rowid",
                (record_id,),
            ).fetchall()
        finally:
            conn.close()
        return [self._relationship(r) for r in rows]

    def _relationship(self, row: sqlite3.Row) -> dict[str, Any]:
        env = self._envelope_fields()
        stmt: dict[str, Any] = {
            "statementId": bytes(row["statement_id"]).hex(),
            "statementDate": env.get("statementDate"),
            "declarationSubject": row["ip_rid"],
            "recordId": row["record_id"],
            "recordType": "relationship",
            "recordStatus": env["recordStatus"],
            "recordDetails": {
                "isComponent": False,
                "subject": row["subject_rid"],
                "interestedParty": row["ip_rid"],
                "interests": [
                    {
                        "type": "unknownInterest",
                        "directOrIndirect": "unknown",
                        "details": row["details_text"] or "",
                    }
                ],
            },
        }
        if "source" in env:
            stmt["source"] = env["source"]
        if "publicationDetails" in env:
            stmt["publicationDetails"] = env["publicationDetails"]
        if row["annotations"]:
            stmt["annotations"] = json.loads(row["annotations"])
        if stmt["statementDate"] is None:
            del stmt["statementDate"]
        return stmt

    # -- the lookup's raw bundle -------------------------------------------

    def bundle_for_lei(self, lei: str) -> dict[str, Any] | None:
        """What the ``meip`` adapter returns for a subject LEI, or None.

        ``records`` describes each MEIP record with this LEI (its group, its
        hierarchy classification, the spreadsheet's immediate parent);
        ``bods_statements`` carries the OECD's statements the graph needs —
        the subject's entity statement(s), each upward relationship and its
        group head's entity statement, deduplicated by statementId.
        """
        recs = self.records(lei)
        if not recs:
            return None
        statements: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add(stmt: dict[str, Any] | None) -> None:
            if stmt and stmt["statementId"] not in seen:
                seen.add(stmt["statementId"])
                statements.append(stmt)

        records: list[dict[str, Any]] = []
        for rec in recs:
            head = rec if rec.is_head else self.entity(rec.group_rid)
            add(self.entity_statement(rec.record_id))
            hierarchy = ""
            if not rec.is_head:
                for rel in self.relationships_for_subject(rec.record_id):
                    add(rel)
                    add(self.entity_statement(rel["recordDetails"]["interestedParty"]))
                    hierarchy = _hierarchy_of(rel) or hierarchy
            total, with_lei = self.group_size(rec.group_rid) if rec.is_head else (0, 0)
            records.append(
                {
                    "record_id": rec.record_id,
                    "statement_id": rec.statement_id,
                    "name": rec.name,
                    "jurisdiction": rec.jurisdiction,
                    "mode": "mne_head" if rec.is_head else "subsidiary",
                    "group": {
                        "record_id": rec.group_rid,
                        "name": head.name if head else "",
                        "lei": head.lei if head else None,
                        "jurisdiction": head.jurisdiction if head else None,
                    },
                    "hierarchy": hierarchy,
                    "immediate_parent": rec.via_name,
                    "immediate_parent_record_id": rec.via_rid,
                    "subsidiaries_total": total if rec.is_head else None,
                    "subsidiaries_with_lei": with_lei if rec.is_head else None,
                }
            )
        return {
            "source_id": "meip",
            "hit_id": (lei or "").strip().upper(),
            "lei": (lei or "").strip().upper(),
            "is_stub": False,
            "edition": self.edition,
            "records": records,
            "bods_statements": statements,
        }


def _hierarchy_of(rel: dict[str, Any]) -> str:
    """``Known`` / ``Partial`` / ``Unknown`` / ``MNE Head`` from the interest's
    ``details`` text, the OECD's free-text carrier for its classification."""
    import re

    for i in (rel.get("recordDetails") or {}).get("interests") or []:
        m = re.search(r"Hierarchy classification:\s*([A-Za-z ]+?)\.", i.get("details") or "")
        if m:
            return m.group(1).strip()
    return ""


# ---------------------------------------------------------------------------
# Process-wide store + boot
# ---------------------------------------------------------------------------

_store: MeipStore | None = None
_store_lock = threading.Lock()


def db_path() -> Path:
    """Where ``meip.sqlite`` lives: ``OPENCHECK_MEIP_DB_FILE`` or
    ``<data root>/meip.sqlite``."""
    from .config import get_settings

    configured = get_settings().meip_db_file
    return Path(configured) if configured else data_root() / "meip.sqlite"


def store() -> MeipStore:
    global _store
    with _store_lock:
        if _store is None or _store.path != db_path():
            _store = MeipStore(db_path())
        return _store


def reload_store() -> None:
    """Drop the cached store (after a download, or in tests)."""
    global _store
    with _store_lock:
        _store = None


def warm_meip_db() -> dict[str, Any]:
    """The boot rule, shared with the PSC graph: download when absent, replace
    when the release asset is not the one the file came from, keep otherwise.
    Non-fatal — without the file the source simply covers nothing."""
    from .config import get_settings
    from .entity_pages import ASSET_STAMP_KEY, asset_check, download_db, read_meta, record_asset_stamp

    settings = get_settings()
    path = db_path()
    url = settings.meip_db_url
    if not url:
        return {"meip": f"{'present' if path.exists() else 'absent'}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"meip": f"already present: {path}"}
            log.info("meip: the release asset is not the one on disk; replacing")
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reload_store()
        return {"meip": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — a register a year old beats none
        log.warning("meip: asset check/download failed: %s", exc)
        return {"meip": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# The Subsidiaries tab's list (Phase 185, rebuilt on the store in Phase 208)
# ---------------------------------------------------------------------------

_json_subs: dict[str, dict] | None = None
_json_heads: dict[str, dict] | None = None


def _json_tables() -> tuple[dict[str, dict], dict[str, dict]]:
    """The committed LEI-keyed subset (Phase 69) — the tab's fallback."""
    global _json_subs, _json_heads
    if _json_subs is None or _json_heads is None:
        try:
            _json_subs = json.loads((_DATA / "meip_subsidiaries.json").read_text(encoding="utf-8"))
            _json_heads = json.loads((_DATA / "meip_mne_heads.json").read_text(encoding="utf-8"))
        except OSError as exc:  # pragma: no cover - committed files
            log.warning("meip: fallback tables unavailable: %s", exc)
            _json_subs, _json_heads = {}, {}
    return _json_subs, _json_heads


def _norm(v: str | None) -> str:
    return (v or "").strip().casefold()


def _declared_from_store(st: MeipStore, lei: str) -> dict[str, Any] | None:
    recs = st.records(lei)
    if not recs:
        return None
    # A LEI on several records: the tab lists the group of the first (file
    # order); the card on QuickCheck says there are more. Heads first.
    recs = sorted(recs, key=lambda r: (not r.is_head,))
    rec = recs[0]
    head = rec if rec.is_head else st.entity(rec.group_rid)
    rows: list[dict[str, Any]] = []
    if rec.is_head:
        children = st.group_children(rec.group_rid)
        for c in children:
            direct = c.via_rid == rec.record_id
            rows.append(
                {
                    "record_id": c.record_id,
                    "lei": c.lei,
                    "name": c.name,
                    "country": c.jurisdiction,
                    "immediate_parent": c.via_name,
                    "direct": direct,
                }
            )
        total = len(children)
        with_lei = sum(1 for c in children if c.lei)
    else:
        # A subsidiary's own direct children — the rows whose spreadsheet
        # immediate parent resolved to this record.
        for c in st.group_children(rec.group_rid):
            if c.via_rid == rec.record_id:
                rows.append(
                    {
                        "record_id": c.record_id,
                        "lei": c.lei,
                        "name": c.name,
                        "country": c.jurisdiction,
                        "immediate_parent": c.via_name,
                        "direct": True,
                    }
                )
        total = with_lei = None
    rows.sort(key=lambda r: (not r["direct"], (r["name"] or "").casefold()))
    return {
        "mode": "mne_head" if rec.is_head else "subsidiary",
        "name": rec.name,
        "parent_mne": head.name if head else "",
        "immediate_parent": rec.via_name,
        "total": total,
        "with_lei": with_lei,
        "rows": rows,
        "source_url": MEIP_URL,
        "edition": st.edition,
        "complete": True,
        "memberships": len(recs),
    }


def _declared_from_json(lei: str) -> dict[str, Any] | None:
    subs, heads = _json_tables()
    key = lei.strip().upper()
    head = heads.get(key)
    sub = subs.get(key)
    match = head or sub
    if match is None:
        return None
    mode = "mne_head" if head else "subsidiary"
    group = _norm(match.get("parent_mne"))
    subject_name = _norm(match.get("name"))
    rows: list[dict[str, Any]] = []
    for child_lei, s in subs.items():
        if child_lei == key or _norm(s.get("parent_mne")) != group:
            continue
        direct = _norm(s.get("immediate_parent")) == subject_name
        if mode == "subsidiary" and not direct:
            continue
        rows.append(
            {
                "record_id": None,
                "lei": child_lei,
                "name": s.get("name") or child_lei,
                "country": s.get("iso3") or None,
                "immediate_parent": s.get("immediate_parent") or None,
                "direct": direct,
            }
        )
    rows.sort(key=lambda r: (not r["direct"], r["name"].casefold()))
    return {
        "mode": mode,
        "name": match.get("name", ""),
        "parent_mne": match.get("parent_mne", ""),
        "immediate_parent": match.get("immediate_parent") or None,
        "total": match.get("subsidiaries_total") if head else None,
        "with_lei": match.get("subsidiaries_with_lei") if head else None,
        "rows": rows,
        "source_url": MEIP_URL,
        "edition": "2024-12-31",
        "complete": False,
        "memberships": 1,
    }


def meip_declared(lei: str | None) -> dict[str, Any] | None:
    """The MEIP subsidiaries the subject is the parent of, or ``None`` when
    the subject is not in the register.

    From the SQLite store every child of the group is listed, LEI or not
    (``complete: True``); from the committed JSON fallback only the
    LEI-carrying subset is (``complete: False``), with ``total`` still the
    register's own count so the tab can say how much it is not showing.
    """
    if not lei:
        return None
    st = store()
    if st.available:
        return _declared_from_store(st, lei)
    return _declared_from_json(lei)
