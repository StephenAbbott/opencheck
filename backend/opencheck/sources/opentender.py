"""OpenTender (DIGIWHIST) adapter.

EU-wide procurement data from `https://opentender.eu`_. Built by the
DIGIWHIST project, the dataset uses the DIGIWHIST Public Procurement
Data Standard — a richer-than-OCDS schema covering tender, lot, bid
and body. Released under **CC BY-NC-SA 4.0**, which means:

* Re-use is permitted with attribution,
* But not for commercial purposes, and
* Derivatives must be re-licensed under the same terms.

This non-commercial-share-alike clause propagates: any /report or
/export bundle that includes OpenTender data must carry the same
restriction. ``app._NC_LICENSES`` already understands that prefix.

Mapping into BODS v0.4 (see ``bods/mapper.map_opentender``):

* Each ``Body`` (buyer / bidder / subcontractor) becomes an
  ``entityStatement``.
* Each ``BodyIdentifier`` is surfaced as a BODS ``identifier`` plus a
  cross-source bridge key when one applies (VAT → ``vat``, HEADER_ICO
  → ``registration_number``, ``ORGANIZATION_ID`` with GB scope →
  ``gb_coh``).
* Each winning bid produces a ``relationshipStatement`` linking the
  winning bidder (interestedParty) to the buyer (subject) with a
  ``otherInfluenceOrControl`` interest annotated with the tender id,
  award date, and contract value. This is **not** beneficial ownership
  — it's a commercial engagement — but representing it as a BODS
  relationship makes procurement data composable with the existing
  reconciler and risk service.

Live integration
----------------
Set ``OPENTENDER_DB_FILE`` to a local SQLite path built by
``scripts/extract_opentender.py`` to enable live search and fetch.

On Render (ephemeral filesystem), upload the DB to S3 and set
``OPENTENDER_S3_URL``; the adapter downloads it at first connection.

Without the DB the adapter falls back to fixture-cache and demo stubs.

.. _https://opentender.eu: https://opentender.eu/
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import unicodedata
from pathlib import Path
from typing import Any

import httpx

from ..cache import Cache
from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

logger = logging.getLogger(__name__)

_CACHE_NS = "opentender"

# BodyIdentifier types that genuinely carry a national company *registration*
# number, and are therefore safe to key an identifier lookup on (issue #29).
# Deliberately excludes:
#   * SOURCE_ID / ETALON_ID — DIGIWHIST-internal record keys, not registrations;
#   * BVD_ID — a Bureau van Dijk proprietary key, not a public registration;
#   * VAT — only a handful of rows in the artifact and collides across the EU
#     VIES space, so near-useless as a registration key here.
# The extractor stores the raw DIGIWHIST ``type`` in ``body_ids.id_type``, so
# these names match the on-disk values verbatim.
_REGISTRATION_ID_TYPES: tuple[str, ...] = (
    "ORGANIZATION_ID",
    "TRADE_REGISTER",
    "HEADER_ICO",
    "TAX_ID",
)

_NAME_PUNCT = re.compile(r"[^\w\s]", re.UNICODE)


def _normalise_name(text: str) -> str:
    """Casefold + strip punctuation/diacritics + collapse whitespace.

    Byte-for-byte the same discipline as ``sources.openaleph._normalise_name``
    (and ``reconcile._normalise_name``): names are *compared*, never scored.
    """
    if not text:
        return ""
    folded = unicodedata.normalize("NFKD", text)
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    folded = _NAME_PUNCT.sub(" ", folded.casefold())
    return " ".join(folded.split())


def _tender_bears_name(tender: dict[str, Any], wanted: str) -> bool:
    """True when some body in ``tender`` is actually named ``wanted``.

    The honest test for a *name* search over procurement records (issue #21,
    mirroring ``openaleph._bears_name``): a tender surfaced for the query
    "Orange S.A." must involve a body that really *is* Orange S.A. OpenTender's
    FTS matches the token "Orange" against "Red-Orange e.U." and "Orange
    controls s.r.o." too — neither of which is the subject — so a BM25 rank is
    not a match. Equality on a body's own name is the only reliable gate, and
    it keeps genuine name-recall for subjects whose registration id yields
    nothing. No relevance-score threshold (forbidden by #21).
    """
    if not wanted:
        return False
    for body in _walk_bodies(tender):
        if _normalise_name(str(body.get("name") or "")) == wanted:
            return True
    return False

# Serialises S3 downloads so concurrent first-requests — or the startup warm-up
# racing a request — can never stream into the same file at once and corrupt it.
_DOWNLOAD_LOCK = threading.Lock()


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


class OpenTenderAdapter(SourceAdapter):
    id = "opentender"

    def __init__(self) -> None:
        self._cache = Cache()
        self._db: sqlite3.Connection | None = None

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        db_path = settings.opentender_db_file
        live = bool(db_path and Path(db_path).exists())
        return SourceInfo(
            id=self.id,
            name="OpenTender",
            homepage="https://opentender.eu/all/download",
            description=(
                "Public procurement tender data covering buyers, bidders, "
                "award values, and integrity scores. Search surfaces winning "
                "suppliers and contracting authorities. Coverage is whatever "
                "the deployed opentender.db was built from — OpenTender "
                "publishes 35 jurisdictions, but an artifact is normally built "
                "from a subset (see docs/sources.md); a company outside the "
                "built countries simply returns no hits."
            ),
            license="CC-BY-NC-SA-4.0",
            attribution=(
                "Procurement data from OpenTender (DIGIWHIST), "
                "licensed CC BY-NC-SA 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # SQLite connection (lazy, with optional S3 bootstrap)
    # ------------------------------------------------------------------

    # SQLite files start with this 16-byte magic string.
    _SQLITE_MAGIC = b"SQLite format 3\x00"

    @staticmethod
    def _is_valid_sqlite(path: Path) -> bool:
        """Return True if *path* starts with the SQLite magic header."""
        try:
            with open(path, "rb") as fh:
                return fh.read(16) == OpenTenderAdapter._SQLITE_MAGIC
        except OSError:
            return False

    @staticmethod
    def _db_is_healthy(path: Path) -> bool:
        """Return True only if *path* is a genuine, non-corrupt SQLite database.

        The bare header check (``_is_valid_sqlite``) catches an HTML error page
        or an empty file, but a download that was **truncated mid-stream** keeps
        a valid 16-byte header while losing interior pages — which then raises
        ``database disk image is malformed`` at query time (the exact Render
        symptom). A read-only ``PRAGMA quick_check`` catches that up front so we
        can delete + re-download instead of serving a corpse.
        """
        if not OpenTenderAdapter._is_valid_sqlite(path):
            return False
        try:
            ro = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
            try:
                row = ro.execute("PRAGMA quick_check").fetchone()
            finally:
                ro.close()
        except sqlite3.DatabaseError:
            return False
        return bool(row) and str(row[0]).lower() == "ok"

    def _ensure_db(self) -> Path | None:
        """Return a path to a healthy local DB, downloading from S3 if needed.

        An existing local file is integrity-checked; a corrupt leftover is
        removed. When an S3 URL is configured, the download is atomic and
        verified (see ``_download_db``) so a partial or corrupt copy is never
        published. Returns ``None`` if no usable DB can be produced (the adapter
        then falls back to demo/stub mode).
        """
        settings = get_settings()
        db_path = settings.opentender_db_file
        if not db_path:
            return None
        path = Path(db_path)

        if path.exists():
            if self._db_is_healthy(path):
                return path
            size = path.stat().st_size if path.exists() else 0
            logger.error(
                "opentender: existing DB at %s failed the integrity check "
                "(%s bytes) — removing so it can be re-downloaded",
                path, size,
            )
            path.unlink(missing_ok=True)

        s3_url = settings.opentender_s3_url
        if not s3_url:
            logger.warning(
                "opentender: no usable DB at %s and no S3 URL configured", path
            )
            return None

        if _download_db(path, s3_url, settings.opentender_db_sha256):
            return path
        return None

    def _conn(self) -> sqlite3.Connection | None:
        if self._db is not None:
            return self._db
        path = self._ensure_db()
        if path is None:
            return None

        # Open read-only + immutable: the artifact never changes at runtime, so
        # this skips journal/WAL handling on the ephemeral /tmp filesystem and
        # never attempts a write-lock on the read path.
        conn = sqlite3.connect(
            f"file:{path}?mode=ro&immutable=1", uri=True, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        self._db = conn
        return self._db

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        conn = self._conn()
        if conn is not None:
            # Run synchronous sqlite3 FTS5 query in a thread so it doesn't
            # block the asyncio event loop while other sources run concurrently.
            return await asyncio.to_thread(self._db_search, conn, query)

        # Fixture-cache path (demo mode)
        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self._cache.has(cache_key):
            return self._stub_search(query)

        cached = self._cache.get_payload(cache_key)
        assert cached is not None  # _cache.has just told us so
        payload = cached[0]

        return [self._tender_hit(item) for item in payload.get("tenders", [])]

    def _db_search(self, conn: sqlite3.Connection, query: str) -> list[SourceHit]:
        """FTS5 search, hardened against a corrupt DB that slipped past the
        connect-time integrity check (defence in depth)."""
        try:
            return self._db_search_impl(conn, query)
        except sqlite3.DatabaseError as exc:
            logger.error(
                "opentender: DB error during search (%r) — returning no results "
                "and dropping the connection so it revalidates next request: %s",
                query, exc,
            )
            self._db = None
            return []

    def _db_search_impl(self, conn: sqlite3.Connection, query: str) -> list[SourceHit]:
        """FTS5 search over body names (buyers + bidders)."""
        # Escape FTS5 special characters.
        safe_q = re.sub(r'["\']', " ", query.strip())
        fts_query = f'"{safe_q}"'

        cur = conn.execute(
            """
            SELECT DISTINCT t.persistent_id, t.data
            FROM body_names_fts f
            JOIN tenders t ON t.persistent_id = f.persistent_id
            WHERE body_names_fts MATCH ?
            LIMIT 20
            """,
            (fts_query,),
        )
        rows = cur.fetchall()
        if not rows:
            # Fall back to prefix MATCH on individual tokens. Extract
            # alphanumeric word-runs (not a bare whitespace split): a token
            # like "S.A." would otherwise reach FTS5 as the prefix term
            # ``S.A.*`` and raise ``fts5: syntax error near "."``, so any name
            # with internal punctuation (S.A., e.U., A/S, s.r.o.) would silently
            # return nothing. ``\w+`` keeps only FTS-safe prefix terms.
            tokens = [w for w in re.findall(r"\w+", safe_q) if len(w) >= 2]
            if tokens:
                fts_query = " OR ".join(f"{w}*" for w in tokens)
                cur = conn.execute(
                    """
                    SELECT DISTINCT t.persistent_id, t.data
                    FROM body_names_fts f
                    JOIN tenders t ON t.persistent_id = f.persistent_id
                    WHERE body_names_fts MATCH ?
                    LIMIT 20
                    """,
                    (fts_query,),
                )
                rows = cur.fetchall()

        hits: list[SourceHit] = []
        for row in rows:
            try:
                tender = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                continue
            hits.append(self._tender_hit(tender))
        return hits

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        conn = self._conn()
        if conn is not None:
            return await asyncio.to_thread(self._db_fetch, conn, hit_id)

        # Fixture-cache path (demo mode)
        cache_key = f"{_CACHE_NS}/tender/{_slug(hit_id)}"
        if not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        cached = self._cache.get_payload(cache_key)
        assert cached is not None
        tender = cached[0]
        return {
            "source_id": self.id,
            "tender_id": hit_id,
            "tender": tender,
        }

    def _db_fetch(self, conn: sqlite3.Connection, hit_id: str) -> dict[str, Any]:
        """Look up a tender by persistentId, hardened against a corrupt DB."""
        try:
            return self._db_fetch_impl(conn, hit_id)
        except sqlite3.DatabaseError as exc:
            logger.error(
                "opentender: DB error during fetch (%s) — degrading to stub: %s",
                hit_id, exc,
            )
            self._db = None
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

    def _db_fetch_impl(self, conn: sqlite3.Connection, hit_id: str) -> dict[str, Any]:
        """Look up a tender by persistentId (primary key)."""
        cur = conn.execute(
            "SELECT data FROM tenders WHERE persistent_id = ?",
            (hit_id,),
        )
        row = cur.fetchone()
        if row is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        try:
            tender = json.loads(row["data"])
        except (json.JSONDecodeError, TypeError):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        return {
            "source_id": self.id,
            "tender_id": hit_id,
            "tender": tender,
        }

    # ------------------------------------------------------------------
    # Identifier-first lookup (issue #29)
    # ------------------------------------------------------------------

    async def fetch_by_registration(
        self, country: str, registration_number: str
    ) -> list[SourceHit]:
        """Return tenders whose bodies carry ``registration_number`` in ``country``.

        Queries the indexed ``body_ids`` table on ``id_value`` (using
        ``idx_body_ids_lookup``), restricted to the id_types that genuinely
        carry registration numbers (``_REGISTRATION_ID_TYPES``) and scoped to
        the tender's ``country`` — so a French SIREN can never collide with a
        same-digit identifier in another jurisdiction. Degrades to an empty
        list (never raises) when no DB is available.
        """
        reg = (registration_number or "").strip()
        cc = (country or "").strip().upper()
        if not reg or not cc:
            return []
        conn = self._conn()
        if conn is None:
            return []
        return await asyncio.to_thread(
            self._db_fetch_by_registration, conn, cc, reg
        )

    def _db_fetch_by_registration(
        self, conn: sqlite3.Connection, country: str, reg: str
    ) -> list[SourceHit]:
        try:
            return self._db_fetch_by_registration_impl(conn, country, reg)
        except sqlite3.DatabaseError as exc:
            logger.error(
                "opentender: DB error during registration lookup "
                "(%s/%s) — returning no results and dropping the connection: %s",
                country, reg, exc,
            )
            self._db = None
            return []

    def _db_fetch_by_registration_impl(
        self, conn: sqlite3.Connection, country: str, reg: str
    ) -> list[SourceHit]:
        placeholders = ",".join("?" for _ in _REGISTRATION_ID_TYPES)
        cur = conn.execute(
            f"""
            SELECT DISTINCT t.persistent_id, t.data
            FROM body_ids b
            JOIN tenders t ON t.persistent_id = b.persistent_id
            WHERE b.id_value = ?
              AND b.id_type IN ({placeholders})
              AND t.country = ?
            LIMIT 20
            """,
            (reg, *_REGISTRATION_ID_TYPES, country),
        )
        hits: list[SourceHit] = []
        for row in cur.fetchall():
            try:
                tender = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                continue
            hits.append(self._tender_hit(tender))
        return hits

    async def fetch_by_name(self, legal_name: str) -> list[SourceHit]:
        """Return tenders whose bodies are actually *named* ``legal_name``.

        The name-recall fallback for the lookup flow: run the FTS body-name
        search, then keep only tenders that bear the subject's legal name
        (``_tender_bears_name``) — gated on name equivalence exactly like
        ``openaleph.fetch_by_name`` (issue #21), never a relevance score.
        Degrades to an empty list when no DB is available.
        """
        name = (legal_name or "").strip()
        if not name:
            return []
        conn = self._conn()
        if conn is None:
            return []
        return await asyncio.to_thread(self._db_fetch_by_name, conn, name)

    def _db_fetch_by_name(
        self, conn: sqlite3.Connection, legal_name: str
    ) -> list[SourceHit]:
        try:
            candidates = self._db_search_impl(conn, legal_name)
        except sqlite3.DatabaseError as exc:
            logger.error(
                "opentender: DB error during name lookup (%r) — "
                "returning no results and dropping the connection: %s",
                legal_name, exc,
            )
            self._db = None
            return []
        wanted = _normalise_name(legal_name)
        return [h for h in candidates if _tender_bears_name(h.raw, wanted)]

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _tender_hit(item: dict[str, Any]) -> SourceHit:
        # Prefer persistentId as the stable hit_id; fall back to source id.
        tender_id = item.get("persistentId") or item.get("id") or ""
        title = item.get("title") or item.get("titleEnglish") or "Tender"
        country = item.get("country") or ""
        buyers = item.get("buyers") or []
        buyer_name = (buyers[0] or {}).get("name") if buyers else ""

        summary_bits: list[str] = []
        if buyer_name:
            summary_bits.append(f"buyer: {buyer_name}")
        if country:
            summary_bits.append(country)
        proc_type = item.get("procedureType")
        if proc_type:
            summary_bits.append(proc_type.replace("_", " ").lower())

        # Capture DIGIWHIST integrity/transparency scores when present.
        scores = item.get("ot", {}) or {}
        integrity = scores.get("integrity")
        if integrity is not None:
            summary_bits.append(f"integrity {integrity:.2f}")

        # Surface every BodyIdentifier we can reach as a flat identifier
        # map so the cross-source reconciler can bridge to GLEIF / CH /
        # OS on shared VAT, registration numbers, or LEI.
        identifiers: dict[str, str] = {"opentender_id": tender_id}
        for body in _walk_bodies(item):
            for ident in body.get("bodyIds") or []:
                key, value = _bridge_identifier(ident)
                if not (key and value):
                    continue
                # Don't let a buyer's identifier overwrite a bidder's
                # (or vice versa). Reconciler matches on equality, not
                # role — so the first seen wins.
                identifiers.setdefault(key, value)

        return SourceHit(
            source_id="opentender",
            hit_id=tender_id,
            kind=SearchKind.ENTITY,
            name=title,
            summary=" · ".join(summary_bits) or "Procurement record",
            identifiers=identifiers,
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="OT-stub-0001",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub OpenTender record — set OPENTENDER_DB_FILE to a "
                    "database built by scripts/extract_opentender.py to enable "
                    "live procurement search."
                ),
                identifiers={"opentender_id": "OT-stub-0001"},
                raw={
                    "id": "OT-stub-0001",
                    "title": f"{query} stub tender",
                    "buyers": [{"name": "Stub Authority"}],
                    "bidders": [{"name": f"{query} (stub)"}],
                },
            )
        ]


def _download_db(path: Path, s3_url: str, expected_sha256: str | None = None) -> bool:
    """Download the OpenTender DB to *path* atomically and verified.

    Streams to ``<path>.part``, verifies completeness (bytes vs
    ``Content-Length``), an optional pinned SHA-256, and a ``PRAGMA
    quick_check``, then atomically ``os.replace``-s it into place — so a partial
    or corrupt download is **never visible** at *path*. Serialised by
    ``_DOWNLOAD_LOCK`` so concurrent callers (a request and the startup warm-up)
    cannot collide. Returns True only if a healthy DB now exists at *path*.
    """
    with _DOWNLOAD_LOCK:
        # A concurrent caller may have finished while we waited for the lock.
        if path.exists() and OpenTenderAdapter._db_is_healthy(path):
            return True
        tmp = path.with_name(path.name + ".part")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp.unlink(missing_ok=True)
            logger.info("opentender: downloading DB from S3 …")
            hasher = hashlib.sha256()
            written = 0
            with httpx.stream("GET", s3_url, follow_redirects=True, timeout=600) as r:
                r.raise_for_status()
                try:
                    expected_len = int(r.headers.get("Content-Length") or 0)
                except (TypeError, ValueError):
                    expected_len = 0
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_bytes(chunk_size=1 << 20):
                        fh.write(chunk)
                        hasher.update(chunk)
                        written += len(chunk)

            if expected_len and written != expected_len:
                logger.error(
                    "opentender: download truncated (%s of %s bytes) — discarding",
                    written, expected_len,
                )
                tmp.unlink(missing_ok=True)
                return False
            if expected_sha256 and hasher.hexdigest().lower() != expected_sha256.strip().lower():
                logger.error("opentender: download SHA-256 mismatch — discarding")
                tmp.unlink(missing_ok=True)
                return False
            if not OpenTenderAdapter._db_is_healthy(tmp):
                logger.error(
                    "opentender: downloaded DB failed the integrity check — discarding"
                )
                tmp.unlink(missing_ok=True)
                return False

            os.replace(tmp, path)
            logger.info("opentender: DB downloaded & verified (%s bytes)", written)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("opentender: S3 download failed: %s", exc)
            tmp.unlink(missing_ok=True)
            return False


def warm_opentender_db() -> None:
    """Pre-download + verify the OpenTender DB at startup, off the request path.

    No-op unless OpenTender is BOTH registered (reachable via a lookup) AND
    ``OPENTENDER_DB_FILE`` is configured. The registry guard is essential: the
    OpenTender DB is a multi-hundred-MB SQLite artifact, and on Render it lands
    on the 2 GB-capped ephemeral ``/tmp``. Pre-downloading it for a source that
    isn't in the registry — i.e. that no lookup can ever query — is pure
    deadweight that, stacked on the GLEIF/UK-PSC FTS downloads, can tip ``/tmp``
    over its limit and get the instance evicted. So we only warm it when the
    adapter is actually live.

    Failures are logged, not raised — the adapter still degrades to demo/stub
    mode on first use.
    """
    from . import REGISTRY

    if "opentender" not in REGISTRY:
        return
    settings = get_settings()
    if not settings.opentender_db_file:
        return
    OpenTenderAdapter()._ensure_db()


_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


def _walk_bodies(tender: dict[str, Any]):
    """Yield every Body referenced by a DIGIWHIST tender.

    Buyers + onBehalfOf live at the top level; bidders + subcontractors
    live nested under lots → bids. We flatten both so the search-time
    identifier sweep doesn't miss the supplier-side bridges.
    """
    for body in tender.get("buyers") or []:
        if isinstance(body, dict):
            yield body
    for body in tender.get("onBehalfOf") or []:
        if isinstance(body, dict):
            yield body
    for lot in tender.get("lots") or []:
        for bid in lot.get("bids") or []:
            for body in bid.get("bidders") or []:
                if isinstance(body, dict):
                    yield body
            for body in bid.get("subcontractors") or []:
                if isinstance(body, dict):
                    yield body


def _looks_like_lei(value: str) -> bool:
    """LEIs are 20-character ISO 17442 alphanumeric strings.

    DIGIWHIST has no dedicated ``LEI`` BodyIdentifierType — practitioners
    record LEIs under ``ETALON_ID`` (scope=GLOBAL). We detect them by
    shape so the cross-source reconciler can bridge to GLEIF on the LEI
    regardless of how the publisher tagged it.
    """
    return bool(_LEI_SHAPE.match(value.upper()))


def _bridge_identifier(ident: dict[str, Any]) -> tuple[str | None, str | None]:
    """Map a DIGIWHIST ``BodyIdentifier`` to a cross-source bridge key.

    Returns ``(scheme, value)`` or ``(None, None)`` if we don't have a
    strong-bridge equivalent. The scheme uses the same names the rest
    of OpenCheck uses: ``vat``, ``registration_number``, ``gb_coh``,
    ``lei``.
    """
    type_ = (ident.get("type") or "").upper()
    scope = (ident.get("scope") or "").upper()
    value = ident.get("id")
    if not value:
        return None, None
    value = str(value).strip()
    if not value:
        return None, None

    # LEI detection trumps the declared type — if it walks like an LEI…
    if _looks_like_lei(value):
        return "lei", value.upper()

    if type_ == "VAT":
        return "vat", value

    if type_ == "ORGANIZATION_ID":
        # DIGIWHIST UK data publishes ORGANIZATION_ID with scope "UNKNOWN"
        # (publisher didn't set a country scope). We treat both "GB" and
        # "UNKNOWN" as GB Companies House numbers since UK is the only
        # jurisdiction in DIGIWHIST that consistently uses this type.
        if scope in {"GB", "UNKNOWN"}:
            # DIGIWHIST strips leading zeros; restore 8-digit CH format.
            clean = value
            if clean.isdigit() and len(clean) < 8:
                clean = clean.zfill(8)
            return "gb_coh", clean

    if type_ in {"HEADER_ICO", "STATISTICAL", "TAX_ID", "TRADE_REGISTER"}:
        # These are country-specific national registry IDs; surface
        # them under a generic key so reconciler doesn't lose them.
        return "registration_number", value
    if type_ == "BVD_ID":
        return "bvd_id", value
    return None, None
