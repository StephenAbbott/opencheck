"""Entity pages — SEO-indexable per-LEI pages built from GLEIF bulk data only.

Phase 88. Every entity with a Legal Entity Identifier gets a stable,
crawlable URL::

    https://opencheck.world/entity/{LEI}-{name-slug}

The pages are rendered server-side from a local SQLite database
(``entity_pages.sqlite``) built out of the GLEIF Golden Copy files by
``scripts/build_entity_pages_db.py``. **No third-party adapter is ever
called for these pages** — with ~3.4M LEIs in sitemaps, a crawler fleet
rendering pages that auto-ran the lookup pipeline would obliterate the
Companies House / OpenSanctions / OpenCorporates rate limits. Live checks
happen only when a human clicks the page's "Run the full OpenCheck" CTA
(which deep-links to the SPA's ``/?lei=`` auto-run flow).

Layout of this module:

* :func:`slugify_name` — shared by the DB builder and the router so the
  slug stored at build time always equals the slug the redirect canonicaliser
  computes at request time.
* :class:`EntityStore` — read-only accessor over the SQLite file, resolved
  from ``OPENCHECK_ENTITY_PAGES_DB_FILE`` (or downloaded at boot from
  ``OPENCHECK_ENTITY_PAGES_DB_URL`` — Render's disk is ephemeral).
* :func:`render_entity_page` — the HTML, with the exact page furniture the
  SEO ticket specifies (title ``{name} - OpenCheck``, the fixed meta
  description, canonical URL, Open Graph / Twitter tags, and JSON-LD
  ``schema.org/Organization`` carrying ``leiCode``).

The DB schema is defined here (`SCHEMA`) and created by the builder script.

Phase 178 widened the store from "enough to render a page" into a **GLEIF
mirror**: the ``entities`` row keeps its page columns and gains ``detail_json``
(every Level 1 field the BODS mapper reads — other and transliterated names,
both addresses in full, the registration authority and its entity id, category,
legal-form detail, creation/expiration, successors, managing LOU, validation
authority, renewal dates), and two new tables hold Level 2: ``relationships``
(the RR file, with status and periods) and ``reporting_exceptions`` (the REPEX
file). :func:`gleif_record_from_row`, :func:`gleif_exception_record` and
:class:`EntityStore`'s relationship queries render all of it in the live API's
JSON:API shapes, so ``map_gleif`` and the subsidiary network cannot tell a
store-served record from a live one — except that they carry only what the
store holds; nothing is guessed. A v1 database (no ``detail_json``, no Level 2
tables) still opens: the reader treats the missing pieces as absent.

Size, because it decides whether the file can boot on an ephemeral disk at all
(measured on the 2026-09-07 16:00 full publish, 3,423,663 LEIs): the detail
written as plain JSON text averaged 697 bytes a row — 2.37 GB of a 4.8 GB file,
five times the v1 build. The same JSON compressed row-by-row with zlib against
a **shared dictionary** sampled from the file (:func:`build_detail_zdict`,
stored in ``meta.detail_zdict``) averages 158 bytes: the dictionary carries the
key names and the common values every row repeats, which per-row compression
without one cannot exploit (379 bytes). ``detail_json`` is therefore a zlib
BLOB in a built file and plain TEXT only transiently (the builder compresses
after the load) or in hand-made test rows — the reader accepts both. The
exceptions table is ``WITHOUT ROWID`` with the relation *kind* as its key
(6.3M rows: 364 MB against 1.05 GB with a rowid table plus its autoindex).
"""

from __future__ import annotations

import base64
import html
import json
import logging
import re
import sqlite3
import threading
import unicodedata
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Bump when the rendered HTML changes materially — part of the ETag, so
# crawlers re-fetch pages after a template change even when the row didn't.
TEMPLATE_VERSION = "1"

#: Max slug length. Cut at a word boundary; URLs stay readable and stable.
_SLUG_MAX = 60

#: Bumped when the tables or the ``detail_json`` contract change. Written to
#: ``meta.schema_version`` by the builder; v1 databases have no such key.
SCHEMA_VERSION = "2"

SCHEMA = """
CREATE TABLE IF NOT EXISTS entities (
    lei TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL DEFAULT '',
    entity_status TEXT,
    registration_status TEXT,
    jurisdiction TEXT,
    legal_form TEXT,
    city TEXT,
    region TEXT,
    country TEXT,
    first_registered TEXT,
    last_updated TEXT,
    successor_lei TEXT,
    direct_parent_lei TEXT,
    ultimate_parent_lei TEXT,
    detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_entities_country_name ON entities(country, name);
CREATE INDEX IF NOT EXISTS idx_entities_direct_parent
    ON entities(direct_parent_lei) WHERE direct_parent_lei IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entities_ultimate_parent
    ON entities(ultimate_parent_lei) WHERE ultimate_parent_lei IS NOT NULL;
CREATE TABLE IF NOT EXISTS relationships (
    child_lei TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    parent_lei TEXT NOT NULL,
    relationship_status TEXT,
    registration_status TEXT,
    period_start TEXT,
    period_end TEXT,
    last_updated TEXT,
    PRIMARY KEY (child_lei, relationship_type)
);
CREATE INDEX IF NOT EXISTS idx_relationships_parent ON relationships(parent_lei);
CREATE TABLE IF NOT EXISTS reporting_exceptions (
    lei TEXT NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT,
    reasons_json TEXT,
    reference TEXT,
    PRIMARY KEY (lei, kind)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""

#: Columns a v1 database lacks. The reader tolerates their absence; the
#: builder adds them in place when it upgrades a v1 file with a delta.
SCHEMA_V2_ENTITY_COLUMNS = ("detail_json",)

#: ``meta`` key holding the shared zlib dictionary (base64) that every
#: compressed ``detail_json`` BLOB in the file was written against. A file
#: without it holds no compressed rows.
DETAIL_ZDICT_META_KEY = "detail_zdict"

#: zlib's window is 32 KiB, so a longer dictionary is never consulted.
_ZDICT_MAX = 32 * 1024


def build_detail_zdict(samples: Iterable[str]) -> bytes:
    """A zlib preset dictionary from sample ``detail_json`` texts: their
    concatenation, cut to the last 32 KiB. Nothing cleverer is needed — the
    rows share key names and a small vocabulary of values (languages,
    country codes, statuses, LOU ids, authority codes), and a dictionary that
    simply *contains* them lets the compressor reference each as a back
    reference instead of spelling it out. The builder samples across the whole
    file so no single issuer's vocabulary dominates."""
    joined = "".join(samples).encode("utf-8")
    return joined[-_ZDICT_MAX:]


def compress_detail(text: str, zdict: bytes) -> bytes:
    co = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS, 8, zlib.Z_DEFAULT_STRATEGY, zdict)
    return co.compress(text.encode("utf-8")) + co.flush()


def decompress_detail(blob: bytes, zdict: bytes | None) -> str:
    do = zlib.decompressobj(zdict=zdict) if zdict else zlib.decompressobj()
    return (do.decompress(blob) + do.flush()).decode("utf-8")


def encode_zdict(zdict: bytes) -> str:
    """The ``meta.detail_zdict`` text form (``meta.value`` is TEXT)."""
    return base64.b64encode(zdict).decode("ascii")


def decode_zdict(value: str | None) -> bytes | None:
    if not value:
        return None
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        return None

#: The RR registration statuses that mean "this relationship record stands".
#: Phase 88 applied PUBLISHED only; Phase 178 widened it to LAPSED after
#: checking the live API: ``/lei-records/{lei}/direct-parent`` returns a
#: relationship whose registration lapsed (renewal overdue) as long as the
#: relationship itself is ACTIVE (verified 2026-09-07 on 21380036M4Q8X2V2LU77,
#: registration LAPSED, relationship ACTIVE, served live). RETIRED, ANNULLED
#: and DUPLICATE records are not.
RR_STANDING_REGISTRATION_STATUSES: frozenset[str] = frozenset({"PUBLISHED", "LAPSED"})

RR_DIRECT = "IS_DIRECTLY_CONSOLIDATED_BY"
RR_ULTIMATE = "IS_ULTIMATELY_CONSOLIDATED_BY"

#: Reporting-exception categories as GLEIF publishes them, keyed by the
#: relation kind the adapter asks for.
EXCEPTION_CATEGORIES: dict[str, str] = {
    "direct": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
    "ultimate": "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
}


def slugify_name(name: str | None, fallback: str | None = None) -> str:
    """URL slug for an entity name: ASCII-folded, lowercase, hyphenated.

    Non-Latin names that fold to nothing fall back to ``fallback`` (the
    GLEIF transliterated name when the builder has one), then to ``""`` —
    a slug-less URL (``/entity/{LEI}``) is valid and canonical.
    """
    for candidate in (name, fallback):
        if not candidate:
            continue
        folded = (
            unicodedata.normalize("NFKD", candidate)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        slug = re.sub(r"[^a-z0-9]+", "-", folded.lower()).strip("-")
        if not slug:
            continue
        if len(slug) > _SLUG_MAX:
            cut = slug[: _SLUG_MAX + 1]
            slug = cut[: cut.rfind("-")] if "-" in cut else slug[:_SLUG_MAX]
            slug = slug.strip("-")
        return slug
    return ""


@dataclass(frozen=True)
class EntityRow:
    lei: str
    name: str
    slug: str
    entity_status: str | None
    registration_status: str | None
    jurisdiction: str | None
    legal_form: str | None
    city: str | None
    region: str | None
    country: str | None
    first_registered: str | None
    last_updated: str | None
    successor_lei: str | None
    direct_parent_lei: str | None
    ultimate_parent_lei: str | None
    #: Phase 178: the Level 1 detail the page columns do not carry, parsed
    #: from ``detail_json``. ``None`` on a v1 database or a row built without
    #: it — every reader treats that as "not held", never as empty values.
    detail: dict | None = None

    @property
    def path(self) -> str:
        """Canonical site path for this entity."""
        return f"/entity/{self.lei}-{self.slug}" if self.slug else f"/entity/{self.lei}"


# The page columns, in schema order. ``detail`` is not a column — it is parsed
# from ``detail_json`` — so it is excluded here.
_COLUMNS = [f for f in EntityRow.__dataclass_fields__ if f != "detail"]


def _address_block(block: dict | None, *, city: str | None = None,
                   region: str | None = None, country: str | None = None) -> dict:
    """A GLEIF ``legalAddress`` / ``headquartersAddress`` object from the detail
    JSON (``language``, ``lines``, the number / number-within-building / mail
    routing keys, ``city``, ``region``, ``country``, ``postalCode``), falling
    back to the page columns for the legal address of a v1 row. Only keys the
    store actually holds are emitted, in the live API's key order."""
    block = block or {}
    out: dict = {}
    if block.get("language"):
        out["language"] = block["language"]
    lines = block.get("lines")
    if lines:
        out["addressLines"] = list(lines)
    for key in ("addressNumber", "addressNumberWithinBuilding", "mailRouting"):
        if block.get(key):
            out[key] = block[key]
    for key, fallback in (("city", city), ("region", region), ("country", country)):
        value = block.get(key) or fallback
        if value:
            out[key] = value
    if block.get("postalCode"):
        out["postalCode"] = block["postalCode"]
    return out


def gleif_record_from_row(row: EntityRow) -> dict:
    """An :class:`EntityRow` shaped as a GLEIF Level-1 ``data`` object.

    The one place that decides what a store row may claim. Phase 143 made it
    the anchor's rate-limit fallback, Phase 146 the subsidiary network's, and
    Phase 178 widened it into the mirror's record builder: with a v2 row it
    renders every Level 1 field the BODS mapper reads — names, both addresses,
    ``registeredAs``/``registeredAt``, category, legal form, creation and
    expiration, successors, the registration block — in exactly the JSON:API
    attribute shape ``api.gleif.org`` returns, so ``map_gleif`` cannot tell the
    difference. What a row does not hold (a v1 row's detail, the cross-reference
    ids ``bic``/``mic``/``ocid``/``spglobal`` that come from GLEIF's mapping
    files, not the Golden Copy) is absent rather than guessed at.
    """
    d = row.detail or {}
    entity: dict = {"legalName": {"name": row.name}}
    legal_name_lang = (d.get("legalName") or {}).get("language")
    if legal_name_lang:
        entity["legalName"]["language"] = legal_name_lang
    if d.get("otherNames"):
        entity["otherNames"] = [dict(n) for n in d["otherNames"]]
    if d.get("transliteratedOtherNames"):
        entity["transliteratedOtherNames"] = [dict(n) for n in d["transliteratedOtherNames"]]
    legal_address = _address_block(
        d.get("legalAddress"), city=row.city, region=row.region, country=row.country
    )
    if legal_address:
        entity["legalAddress"] = legal_address
    hq_address = _address_block(d.get("headquartersAddress"))
    if hq_address:
        entity["headquartersAddress"] = hq_address
    if d.get("registeredAt"):
        entity["registeredAt"] = dict(d["registeredAt"])
    if d.get("registeredAs"):
        entity["registeredAs"] = d["registeredAs"]
    if row.jurisdiction:
        entity["jurisdiction"] = row.jurisdiction
    if d.get("category"):
        entity["category"] = d["category"]
    if d.get("subCategory"):
        entity["subCategory"] = d["subCategory"]
    if row.legal_form or d.get("legalFormOther"):
        legal_form: dict = {}
        if row.legal_form:
            legal_form["id"] = row.legal_form
        if d.get("legalFormOther"):
            legal_form["other"] = d["legalFormOther"]
        entity["legalForm"] = legal_form
    if row.entity_status:
        entity["status"] = row.entity_status
    if d.get("creationDate"):
        entity["creationDate"] = d["creationDate"]
    expiration = d.get("expiration") or {}
    if expiration.get("date") or expiration.get("reason"):
        entity["expiration"] = {k: v for k, v in expiration.items() if v}
    successors = d.get("successorEntities") or []
    if row.successor_lei and not any(
        (x.get("lei") or "") == row.successor_lei for x in successors
    ):
        successors = [{"lei": row.successor_lei}, *successors]
    if successors:
        entity["successorEntities"] = [dict(x) for x in successors]
        # The live API also carries the first successor in the singular field.
        entity["successorEntity"] = dict(successors[0])

    attributes: dict = {"lei": row.lei, "entity": entity}
    registration: dict = {}
    if row.first_registered:
        registration["initialRegistrationDate"] = row.first_registered
    if row.last_updated:
        registration["lastUpdateDate"] = row.last_updated
    if row.registration_status:
        registration["status"] = row.registration_status
    for src, dst in (
        ("nextRenewalDate", "nextRenewalDate"),
        ("managingLou", "managingLou"),
        ("corroborationLevel", "corroborationLevel"),
        ("validatedAt", "validatedAt"),
        ("validatedAs", "validatedAs"),
        ("otherValidationAuthorities", "otherValidationAuthorities"),
    ):
        if d.get(src):
            registration[dst] = d[src]
    if registration:
        attributes["registration"] = registration
    if d.get("conformityFlag"):
        attributes["conformityFlag"] = d["conformityFlag"]
    return {"type": "lei-records", "id": row.lei, "attributes": attributes}


def gleif_exception_record(
    lei: str, category: str, reason: str | None, reference: str | None,
    reasons: list[str] | None = None,
) -> dict:
    """A store exception row shaped as the live
    ``/lei-records/{lei}/{kind}-parent-reporting-exception`` ``data`` object —
    the attribute names the mapper and the Phase 114 chip classifier read
    (``category`` / ``reason`` / ``reference``). GLEIF's file allows up to five
    reasons per category; the first is ``reason`` (what the live API shows) and
    the full list rides on ``reasons`` for anyone who wants it."""
    attributes: dict = {"lei": lei, "category": category}
    if reason:
        attributes["reason"] = reason
    if reasons and len(reasons) > 1:
        attributes["reasons"] = list(reasons)
    if reference:
        attributes["reference"] = reference
    return {
        "type": "reporting-exceptions",
        "id": f"{lei}-{category}",
        "attributes": attributes,
    }


def gleif_relationship_record(rel: RelationshipRow) -> dict:
    """A ``relationships`` row shaped as the live ``relationship-records``
    ``data`` object (start node = child, end node = parent). Periods are the
    one the builder kept — the RELATIONSHIP_PERIOD — when the file had it."""
    relationship: dict = {
        "startNode": {"id": rel.child_lei, "type": "LEI"},
        "endNode": {"id": rel.parent_lei, "type": "LEI"},
        "type": rel.relationship_type,
    }
    if rel.relationship_status:
        relationship["status"] = rel.relationship_status
    if rel.period_start or rel.period_end:
        period: dict = {"type": "RELATIONSHIP_PERIOD"}
        if rel.period_start:
            period["startDate"] = rel.period_start
        if rel.period_end:
            period["endDate"] = rel.period_end
        relationship["periods"] = [period]
    attributes: dict = {"relationship": relationship}
    registration: dict = {}
    if rel.registration_status:
        registration["status"] = rel.registration_status
    if rel.last_updated:
        registration["lastUpdateDate"] = rel.last_updated
    if registration:
        attributes["registration"] = registration
    return {
        "type": "relationship-records",
        "id": f"{rel.child_lei}|LEI|{rel.relationship_type}",
        "attributes": attributes,
    }


@dataclass(frozen=True)
class RelationshipRow:
    child_lei: str
    relationship_type: str
    parent_lei: str
    relationship_status: str | None
    registration_status: str | None
    period_start: str | None
    period_end: str | None
    last_updated: str | None

    @property
    def standing(self) -> bool:
        """Whether the live API would still serve this relationship — see
        :data:`RR_STANDING_REGISTRATION_STATUSES`."""
        reg = (self.registration_status or "PUBLISHED").upper()
        status = (self.relationship_status or "ACTIVE").upper()
        return reg in RR_STANDING_REGISTRATION_STATUSES and status == "ACTIVE"


@dataclass(frozen=True)
class ExceptionRow:
    lei: str
    category: str
    reason: str | None
    reasons: list[str]
    reference: str | None

    def record(self) -> dict:
        return gleif_exception_record(
            self.lei, self.category, self.reason, self.reference, self.reasons
        )


def _relationship_row(cursor_row: sqlite3.Row) -> RelationshipRow:
    return RelationshipRow(
        **{f: cursor_row[f] for f in RelationshipRow.__dataclass_fields__}
    )


def _row(cursor_row: sqlite3.Row, zdict: bytes | None = None) -> EntityRow:
    values = {k: cursor_row[k] for k in _COLUMNS}
    detail: dict | None = None
    if "detail_json" in cursor_row.keys():  # noqa: SIM118 — sqlite3.Row is not a dict
        raw = cursor_row["detail_json"]
        if raw:
            try:
                text = decompress_detail(raw, zdict) if isinstance(raw, bytes) else raw
                parsed = json.loads(text)
            except (ValueError, zlib.error):
                parsed = None
            if isinstance(parsed, dict):
                detail = parsed
    return EntityRow(**values, detail=detail)


class EntityStore:
    """Read-only accessor over ``entity_pages.sqlite``.

    A single connection guarded by a lock: every query here is a sub-ms
    indexed read, so contention is negligible and this avoids per-request
    connection churn. The file is opened ``immutable=1`` — the monthly
    refresh replaces the file wholesale, never writes in place.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            f"file:{path}?mode=ro&immutable=1", uri=True, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        # Phase 178: which Level 2 tables this file actually has. A v1 file
        # (built before the mirror) has neither; every Level 2 query then
        # answers "not held" rather than raising — the anchor fallback keeps
        # working on the old file until the next rebuild lands.
        tables = {
            r["name"]
            for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        self.has_relationships = "relationships" in tables
        self.has_exceptions = "reporting_exceptions" in tables
        columns = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(entities)").fetchall()
        }
        self.has_detail = "detail_json" in columns
        self._has_ultimate_index = any(
            r["name"] == "idx_entities_ultimate_parent"
            for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        )
        # The shared dictionary the compressed detail BLOBs were written
        # against. Read once: it is part of the file, fixed at build time.
        self._zdict = decode_zdict(self.meta().get(DETAIL_ZDICT_META_KEY))

    @property
    def schema_version(self) -> str:
        """``meta.schema_version``, or ``"1"`` for a file built before it existed."""
        return self.meta().get("schema_version") or "1"

    def _row(self, cursor_row: sqlite3.Row) -> EntityRow:
        return _row(cursor_row, self._zdict)

    # -- single entity ------------------------------------------------------
    def get(self, lei: str) -> EntityRow | None:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM entities WHERE lei = ?", (lei,))
            r = cur.fetchone()
        return self._row(r) if r else None

    def get_many(self, leis: Iterable[str | None]) -> dict[str, EntityRow]:
        wanted = [lei for lei in leis if lei]
        if not wanted:
            return {}
        marks = ",".join("?" * len(wanted))
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM entities WHERE lei IN ({marks})", wanted
            ).fetchall()
        return {r["lei"]: self._row(r) for r in rows}

    def children(
        self, lei: str, limit: int = 20, *, kind: str = "direct"
    ) -> tuple[list[EntityRow], int]:
        """Entities whose ``{kind}_parent_lei`` is ``lei`` — the store's answer
        to the live ``/{kind}-children`` endpoint. ``kind`` is ``"direct"``
        (default, as Phase 88 built it) or ``"ultimate"`` (Phase 178: the
        ultimate index that the Phase 146 fallback had to declare unavailable).
        """
        column = {"direct": "direct_parent_lei", "ultimate": "ultimate_parent_lei"}[kind]
        with self._lock:
            total = self._conn.execute(
                f"SELECT COUNT(*) FROM entities WHERE {column} = ?", (lei,)
            ).fetchone()[0]
            rows = self._conn.execute(
                f"SELECT * FROM entities WHERE {column} = ? ORDER BY name LIMIT ?",
                (lei, limit),
            ).fetchall()
        return [self._row(r) for r in rows], int(total)

    # -- Level 2 (Phase 178) --------------------------------------------------
    def relationship(self, lei: str, kind: str) -> RelationshipRow | None:
        """The RR record for ``lei``'s direct or ultimate parent, standing or
        not (callers check :attr:`RelationshipRow.standing`), or ``None`` when
        the file has no such record — or no ``relationships`` table at all."""
        if not self.has_relationships:
            return None
        rtype = {"direct": RR_DIRECT, "ultimate": RR_ULTIMATE}[kind]
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM relationships WHERE child_lei = ? AND relationship_type = ?",
                (lei, rtype),
            ).fetchone()
        return _relationship_row(r) if r else None

    def exceptions(self, lei: str) -> dict[str, ExceptionRow]:
        """Reporting exceptions filed for ``lei``, keyed by relation kind
        (``"direct"`` / ``"ultimate"``); empty when none are held — which on a
        v1 file means "not held", not "none filed"."""
        if not self.has_exceptions:
            return {}
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM reporting_exceptions WHERE lei = ?", (lei,)
            ).fetchall()
        out: dict[str, ExceptionRow] = {}
        for r in rows:
            # The table keys on the relation kind (``direct`` / ``ultimate``)
            # rather than GLEIF's 38-character category string — 6.3M rows
            # make that 230 MB — and a category the builder did not
            # recognise is stored verbatim, which the reader skips.
            kind = r["kind"]
            category = EXCEPTION_CATEGORIES.get(kind)
            if category is None:
                continue
            reasons: list[str] = []
            if r["reasons_json"]:
                try:
                    reasons = [str(x) for x in json.loads(r["reasons_json"])]
                except ValueError:
                    reasons = []
            out[kind] = ExceptionRow(
                lei=r["lei"],
                category=category,
                reason=r["reason"],
                reasons=reasons or ([r["reason"]] if r["reason"] else []),
                reference=r["reference"],
            )
        return out

    # -- sitemaps -----------------------------------------------------------
    def count(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0])

    def sitemap_slice(self, offset: int, limit: int) -> list[tuple[str, str, str | None]]:
        """(lei, slug, last_updated) ordered by LEI — stable shard contents."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT lei, slug, last_updated FROM entities ORDER BY lei LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [(r["lei"], r["slug"], r["last_updated"]) for r in rows]

    # -- browse hub ---------------------------------------------------------
    def countries(self) -> list[tuple[str, int]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT country, COUNT(*) AS n FROM entities "
                "WHERE country IS NOT NULL AND country != '' "
                "GROUP BY country ORDER BY country"
            ).fetchall()
        return [(r["country"], int(r["n"])) for r in rows]

    def browse(self, country: str, offset: int, limit: int) -> tuple[list[EntityRow], int]:
        with self._lock:
            total = self._conn.execute(
                "SELECT COUNT(*) FROM entities WHERE country = ?", (country,)
            ).fetchone()[0]
            rows = self._conn.execute(
                "SELECT * FROM entities WHERE country = ? ORDER BY name, lei LIMIT ? OFFSET ?",
                (country, limit, offset),
            ).fetchall()
        return [self._row(r) for r in rows], int(total)

    def meta(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute("SELECT key, value FROM meta").fetchall()
        return {r["key"]: r["value"] for r in rows}


# ---------------------------------------------------------------------------
# Store resolution + boot-time download (Render's disk is ephemeral).
# ---------------------------------------------------------------------------

_store: EntityStore | None = None
_store_lock = threading.Lock()


def _db_path() -> Path | None:
    from .config import get_settings

    settings = get_settings()
    if settings.entity_pages_db_file:
        return Path(settings.entity_pages_db_file)
    if settings.entity_pages_db_url:
        return Path("/tmp/opencheck-entity-pages/entity_pages.sqlite")
    return None


def get_store() -> EntityStore | None:
    """The process-wide store, or ``None`` when no DB is configured/present."""
    global _store
    path = _db_path()
    if path is None:
        return None
    with _store_lock:
        if _store is not None and _store.path == path:
            return _store
        if not path.exists():
            return None
        try:
            _store = EntityStore(path)
        except sqlite3.Error as exc:
            log.warning("entity_pages DB unusable at %s: %s", path, exc)
            return None
        return _store


def reset_store_for_tests() -> None:
    global _store
    with _store_lock:
        _store = None


def warm_entity_pages_db() -> dict[str, Any]:
    """Download the DB at boot when configured by URL. Non-fatal.

    Called from the app lifespan's background warm-up (same pattern as the
    climatetrace/securities warms). Supports plain ``.sqlite``
    and ``.gz`` artifacts; downloads to a temp name then renames, so a
    half-written file never becomes the live DB.
    """
    from .config import get_settings

    settings = get_settings()
    url = settings.entity_pages_db_url
    if not url or settings.entity_pages_db_file:
        return {"entity_pages": "not configured for download"}
    path = _db_path()
    assert path is not None
    if path.exists():
        return {"entity_pages": f"already present: {path}"}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".download")
    import time

    import httpx

    # Phase 178: the mirror is 1.8 GB on disk from a ~0.9 GB asset, so the
    # ``.gz`` is inflated *as it streams* — the disk never holds the archive
    # and the plain file at once (2.7 GB on an ephemeral disk), and the
    # inflate costs no second pass. The Phase 88 arrangement wrote the archive
    # out and gunzipped it afterwards.
    started = time.monotonic()
    downloaded = 0
    inflate = zlib.decompressobj(16 + zlib.MAX_WBITS) if url.endswith(".gz") else None
    with httpx.stream("GET", url, timeout=600.0, follow_redirects=True) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_bytes():
                downloaded += len(chunk)
                fh.write(inflate.decompress(chunk) if inflate else chunk)
            if inflate:
                fh.write(inflate.flush())
    tmp.rename(path)
    elapsed = time.monotonic() - started
    log.info(
        "entity_pages DB downloaded to %s (%d bytes on disk from %d fetched) in %.1fs",
        path, path.stat().st_size, downloaded, elapsed,
    )
    return {"entity_pages": f"downloaded: {path} ({downloaded} bytes in {elapsed:.1f}s)"}


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_STATUS_BADGES = {
    "ISSUED": ("Registration current", "#0f766e"),
    "LAPSED": ("Registration lapsed", "#a16207"),
    "RETIRED": ("Entity ceased", "#b91c1c"),
    "MERGED_INTO_SUCCESSOR": ("Merged into successor", "#b91c1c"),
    "DUPLICATE": ("Duplicate record", "#6b7280"),
    "ANNULLED": ("Registration annulled", "#6b7280"),
}


def country_name(code: str | None) -> str | None:
    if not code:
        return None
    try:
        import pycountry

        c = pycountry.countries.get(alpha_2=code.upper())
        return c.name if c else code
    except Exception:  # noqa: BLE001 — a code is better than a crash
        return code


def legal_form_label(code: str | None) -> str | None:
    if not code:
        return None
    try:
        from .elf import ELF_CODES

        return ELF_CODES.get(code.upper())
    except Exception:  # noqa: BLE001
        return None


def _date(value: str | None) -> str | None:
    """``2022-08-02T00:00:00Z`` → ``2022-08-02`` (sitemap + display form)."""
    if not value:
        return None
    return value[:10]


_PAGE_CSS = """
  :root { --navy:#0d1b3e; --ink:#1c2333; --muted:#5b6478; --line:#e3e6ee;
          --bg:#f7f8fb; --accent:#0f5eff; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:16px/1.55 "DM Sans","Segoe UI",system-ui,sans-serif; }
  a { color:var(--accent); }
  header { background:var(--navy); color:#fff; padding:14px 20px; }
  header a { color:#fff; text-decoration:none; font-weight:700;
             font-family:Bitter,Georgia,serif; font-size:20px; }
  header span { opacity:.75; font-size:13px; margin-left:10px; }
  main { max-width:880px; margin:0 auto; padding:28px 20px 48px; }
  h1 { font-family:Bitter,Georgia,serif; font-size:30px; line-height:1.2;
       margin:6px 0 4px; }
  .lei { font-family:"DM Mono",ui-monospace,monospace; color:var(--muted);
         font-size:14px; letter-spacing:.4px; }
  .badge { display:inline-block; padding:2px 10px; border-radius:999px;
           color:#fff; font-size:13px; font-weight:600; margin:10px 0; }
  .cta { display:inline-block; background:var(--accent); color:#fff;
         font-weight:700; padding:12px 22px; border-radius:8px;
         text-decoration:none; margin:18px 0 6px; }
  .cta-note { color:var(--muted); font-size:14px; margin-top:2px; }
  dl { display:grid; grid-template-columns:minmax(160px,max-content) 1fr;
       gap:8px 18px; background:#fff; border:1px solid var(--line);
       border-radius:10px; padding:18px 20px; margin:20px 0; }
  dt { color:var(--muted); font-size:14px; }
  dd { margin:0; }
  section h2 { font-family:Bitter,Georgia,serif; font-size:20px; margin:26px 0 8px; }
  ul.entities { list-style:none; padding:0; margin:8px 0; }
  ul.entities li { padding:5px 0; border-bottom:1px solid var(--line); }
  nav.pages { margin:18px 0; display:flex; gap:16px; }
  footer { border-top:1px solid var(--line); margin-top:36px; padding-top:14px;
           color:var(--muted); font-size:13px; }
  footer a { color:var(--muted); }
"""


def _goatcounter_snippet(bucket: str) -> str:
    """The GoatCounter loader for server-rendered pages (Phase 89).

    ``bucket`` overrides the recorded path (``/entity``, ``/browse``) so no
    LEI, slug or country code ever reaches analytics — the same privacy
    contract as the SPA's ``canonicalPath`` (frontend/src/lib/analytics.ts).
    Disabled when ``OPENCHECK_GOATCOUNTER_ENDPOINT`` is set to empty.
    """
    from .config import get_settings

    endpoint = (get_settings().goatcounter_endpoint or "").strip()
    if not endpoint:
        return ""
    return (
        f"<script>window.goatcounter={{path:{bucket!r}}}</script>"
        f'<script data-goatcounter="{html.escape(endpoint)}" async '
        'src="https://gc.zgo.at/count.js"></script>'
    )


def _head(
    *,
    title: str,
    description: str,
    canonical: str,
    og_image: str | None = None,
    extra: str = "",
    noindex: bool = False,
    analytics_bucket: str | None = None,
) -> str:
    robots = '<meta name="robots" content="noindex">' if noindex else ""
    if analytics_bucket:
        extra += _goatcounter_snippet(analytics_bucket)
    og_img = (
        f'<meta property="og:image" content="{html.escape(og_image)}">'
        f'<meta property="og:image:width" content="1200">'
        f'<meta property="og:image:height" content="630">'
        f'<meta name="twitter:card" content="summary_large_image">'
        f'<meta name="twitter:image" content="{html.escape(og_image)}">'
        if og_image
        else '<meta name="twitter:card" content="summary">'
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(title)}</title>
<meta name="description" content="{html.escape(description)}">
<link rel="canonical" href="{html.escape(canonical)}">
<meta name="theme-color" content="#0d1b3e">
<meta property="og:type" content="website">
<meta property="og:site_name" content="OpenCheck">
<meta property="og:title" content="{html.escape(title)}">
<meta property="og:description" content="{html.escape(description)}">
<meta property="og:url" content="{html.escape(canonical)}">
<meta name="twitter:title" content="{html.escape(title)}">
<meta name="twitter:description" content="{html.escape(description)}">
{og_img}{robots}
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet"
  href="https://fonts.googleapis.com/css2?family=Bitter:wght@400;600;700&family=DM+Mono:wght@400&family=DM+Sans:wght@400;500;700&display=swap">
<style>{_PAGE_CSS}</style>
{extra}
</head>"""


def _header_html(frontend: str) -> str:
    return (
        f'<header><a href="{html.escape(frontend)}/">OpenCheck</a>'
        "<span>LEI-anchored due diligence on open data</span></header>"
    )


def _footer_html(frontend: str, built: str | None) -> str:
    from .config import get_settings

    built_note = f" Reference data refreshed {html.escape(built)}." if built else ""
    # Privacy note only when analytics is actually on (Phase 89).
    privacy = (
        "<p>Privacy: visits are counted without cookies or fingerprinting via "
        '<a href="https://www.goatcounter.com/">GoatCounter</a>; no entity '
        "identifiers or query strings are recorded.</p>"
        if (get_settings().goatcounter_endpoint or "").strip()
        else ""
    )
    return (
        "<footer><p>Reference data: GLEIF Golden Copy, published without "
        'restriction by the <a href="https://www.gleif.org/">Global Legal Entity '
        f"Identifier Foundation</a>.{built_note}</p>"
        f"{privacy}"
        f'<p><a href="{html.escape(frontend)}/about">About OpenCheck</a> · '
        f'<a href="{html.escape(frontend)}/sources">Data sources</a> · '
        '<a href="/browse">Browse entities by country</a> · '
        '<a href="https://github.com/StephenAbbott/opencheck">Source code (MIT)</a>'
        "</p></footer>"
    )


def entity_title(name: str) -> str:
    """The exact title template from the SEO ticket."""
    return f"{name} - OpenCheck"


def entity_description(name: str) -> str:
    """The exact meta-description template from the SEO ticket."""
    return (
        f"Open-source customer due diligence risk checks on {name}, powered by "
        "the Legal Entity Identifier, open data and open standards"
    )


def _json_ld(row: EntityRow, canonical: str, parent_url: str | None) -> str:
    import json

    org: dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": row.name,
        "url": canonical,
        "leiCode": row.lei,
        "identifier": {
            "@type": "PropertyValue",
            "propertyID": "LEI",
            "value": row.lei,
        },
        "sameAs": f"https://search.gleif.org/#/record/{row.lei}",
    }
    address = {
        k: v
        for k, v in {
            "addressLocality": row.city,
            "addressRegion": row.region,
            "addressCountry": row.country,
        }.items()
        if v
    }
    if address:
        org["address"] = {"@type": "PostalAddress", **address}
    if parent_url:
        org["parentOrganization"] = {"@type": "Organization", "url": parent_url}
    return (
        '<script type="application/ld+json">'
        + json.dumps(org, ensure_ascii=False)
        + "</script>"
    )


def _entity_link(row: EntityRow) -> str:
    return f'<a href="{html.escape(row.path)}">{html.escape(row.name)}</a>'


def render_entity_page(
    row: EntityRow,
    *,
    frontend: str,
    api_base: str,
    related: dict[str, EntityRow],
    children: list[EntityRow],
    children_total: int,
    built: str | None,
) -> str:
    """The full entity page HTML (GLEIF reference data only — see module doc)."""
    canonical = f"{frontend}{row.path}"
    title = entity_title(row.name)
    description = entity_description(row.name)

    reg = (row.registration_status or "").upper()
    badge_label, badge_colour = _STATUS_BADGES.get(
        reg, (reg.replace("_", " ").title() or "Status unknown", "#6b7280")
    )

    def rel_link(lei: str | None) -> str | None:
        if not lei:
            return None
        found = related.get(lei)
        if found:
            return _entity_link(found)
        safe = html.escape(lei)
        return f'<a href="/entity/{safe}"><span class="lei">{safe}</span></a>'

    rows: list[tuple[str, str]] = []
    rows.append(("Legal Entity Identifier", f'<span class="lei">{html.escape(row.lei)}</span>'))
    if row.jurisdiction:
        rows.append(("Legal jurisdiction", html.escape(row.jurisdiction)))
    form = legal_form_label(row.legal_form)
    if form or row.legal_form:
        label = f"{form} ({row.legal_form})" if form else (row.legal_form or "")
        rows.append(("Legal form", html.escape(label)))
    place = ", ".join(p for p in (row.city, row.region, country_name(row.country)) if p)
    if place:
        rows.append(("Registered address", html.escape(place)))
    if row.entity_status:
        rows.append(("Entity status", html.escape(row.entity_status.title())))
    if row.registration_status:
        reg_label = row.registration_status.replace("_", " ").title()
        rows.append(("LEI registration", html.escape(reg_label)))
    if _date(row.first_registered):
        rows.append(("LEI first registered", _date(row.first_registered) or ""))
    if _date(row.last_updated):
        rows.append(("Record last updated", _date(row.last_updated) or ""))
    dp = rel_link(row.direct_parent_lei)
    if dp:
        rows.append(("Direct parent", dp))
    up = rel_link(row.ultimate_parent_lei)
    if up and row.ultimate_parent_lei != row.direct_parent_lei:
        rows.append(("Ultimate parent", up))
    successor = rel_link(row.successor_lei)
    if successor:
        rows.append(("Successor entity", successor))

    dl = "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in rows)

    children_html = ""
    if children:
        more = ""
        if children_total > len(children):
            more = (
                f"<p>…and {children_total - len(children)} more directly "
                "consolidated entities in the GLEIF relationship data.</p>"
            )
        items = "".join(f"<li>{_entity_link(c)}</li>" for c in children)
        children_html = (
            "<section><h2>Directly consolidated entities</h2>"
            f'<ul class="entities">{items}</ul>{more}</section>'
        )

    parent_url = None
    if row.direct_parent_lei and row.direct_parent_lei in related:
        parent_url = f"{frontend}{related[row.direct_parent_lei].path}"

    # rel="nofollow" on the CTA: with ~3.4M entity pages each linking to
    # /?lei={LEI}, a followed link would hand crawlers 3.4M parameter-URL
    # crawl targets (duplicate SPA shells). Humans are unaffected. The full
    # ?lei= → /entity URL unification is a ticketed future phase.
    cta_href = f"{frontend}/?lei={row.lei}"
    # CTA click → the entity_page_cta feature event (Phase 89) — only when
    # analytics is on, so a disabled endpoint leaves zero GoatCounter traces.
    cta_onclick = (
        " onclick=\"window.goatcounter&amp;&amp;window.goatcounter"
        ".count({path:'entity_page_cta',event:true})\""
        if _goatcounter_snippet("/entity")
        else ""
    )
    browse_link = (
        f'<a href="/browse/{row.country}">More entities registered in '
        f"{html.escape(country_name(row.country) or row.country)}</a>"
        if row.country
        else '<a href="/browse">Browse entities by country</a>'
    )

    head = _head(
        title=title,
        description=description,
        canonical=canonical,
        og_image=f"{api_base}/og/{row.lei}.png",
        extra=_json_ld(row, canonical, parent_url),
        analytics_bucket="/entity",
    )
    return f"""{head}
<body>
{_header_html(frontend)}
<main>
<p class="lei">{html.escape(row.lei)}</p>
<h1>{html.escape(row.name)}</h1>
<span class="badge" style="background:{badge_colour}">{html.escape(badge_label)}</span>
<p>Open-source customer due diligence on <strong>{html.escape(row.name)}</strong>,
anchored on its Legal Entity Identifier. The reference data below comes from the
GLEIF Golden Copy; a full OpenCheck runs live checks across 30+ open data
sources — company registries, sanctions and watchlists, beneficial ownership
registers and investigative datasets — mapped to the Beneficial Ownership Data
Standard (BODS).</p>
<a class="cta" href="{html.escape(cta_href)}" rel="nofollow"{cta_onclick}>Run the full OpenCheck</a>
<p class="cta-note">Free, no sign-up. Live lookups run only when you start them.</p>
<dl>{dl}</dl>
{children_html}
<section><h2>Explore</h2><p>{browse_link} ·
<a href="https://search.gleif.org/#/record/{row.lei}">This record at GLEIF</a></p></section>
</main>
{_footer_html(frontend, built)}
</body>
</html>"""


def render_not_found(frontend: str, token: str) -> str:
    head = _head(
        title="Entity not found - OpenCheck",
        description="No entity with this Legal Entity Identifier was found.",
        canonical=f"{frontend}/",
        noindex=True,
    )
    return f"""{head}
<body>
{_header_html(frontend)}
<main>
<h1>Entity not found</h1>
<p>No entity page exists for <span class="lei">{html.escape(token[:40])}</span>.
If this is a newly issued LEI it will appear after the next monthly refresh of
the GLEIF Golden Copy data.</p>
<p><a href="{html.escape(frontend)}/">Search OpenCheck</a> or
<a href="/browse">browse entities by country</a>.</p>
</main>
{_footer_html(frontend, None)}
</body>
</html>"""


def render_unavailable(frontend: str) -> str:
    head = _head(
        title="Entity pages unavailable - OpenCheck",
        description="Entity reference pages are temporarily unavailable.",
        canonical=f"{frontend}/",
        noindex=True,
    )
    return f"""{head}
<body>
{_header_html(frontend)}
<main>
<h1>Entity pages are temporarily unavailable</h1>
<p>The GLEIF reference dataset behind these pages is still loading.
Please try again shortly, or <a href="{html.escape(frontend)}/">run a live
check on OpenCheck</a>.</p>
</main>
{_footer_html(frontend, None)}
</body>
</html>"""
