"""Open Ownership BODS bulk data adapter — GLEIF.

Queries the pre-extracted Open Ownership BODS v0.4 Parquet files for the
GLEIF dataset.  The Parquet files are built by Open Ownership from the
GLEIF LEI-CDF bulk data and published at:
https://bods-data.openownership.org/source/gleif_version_0_4/

Files are produced by `Flatterer <https://github.com/kindly/flatterer>`_
which flattens nested JSON into one file per JSON array level.  Key files:

* ``entity_statement.parquet`` — one row per LEI entity
* ``entity_recorddetails_identifiers.parquet`` — one row per identifier
  (the LEI value itself lives here under ``scheme = "LEI"``)
* ``entity_recorddetails_addresses.parquet`` — postal/registered addresses
* ``relationship_statement.parquet`` — direct/ultimate parent links

Column naming convention: Flatterer uses snake_case with double-underscored
prefix for nested paths.  In practice the BODS 0.4 nesting
``recordDetails.name`` becomes ``recorddetails_name``.

Setup
-----
Run ``python scripts/setup_bods_data.py --source gleif`` once to download
and index the data, then set:

    BODS_GLEIF_PARQUET_DIR=/path/to/data/bods/gleif/parquet
    BODS_GLEIF_FTS_DB=/path/to/data/bods/gleif/fts.db

Without these vars the adapter returns stubs.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
import zipfile
from pathlib import Path
from typing import Any

import httpx

from ..cache import Cache
from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

logger = logging.getLogger(__name__)

_CACHE_NS = "bods_gleif"

# DuckDB is optional at import time — defer the import so the server starts
# even if duckdb is not yet installed.  The adapter will fall back to stubs.
try:
    import duckdb as _duckdb  # noqa: F401
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False
    logger.warning("bods_gleif: duckdb not installed — live queries unavailable")


def _escape_fts5(query: str) -> str:
    """Escape FTS5 special characters and build a safe query string."""
    cleaned = re.sub(r'["\']', " ", query.strip())
    return f'"{cleaned}"'


def _prefix_fts5(query: str) -> str:
    """Build a prefix OR-match FTS5 query from *query* tokens."""
    tokens = [w for w in re.sub(r'["\']', " ", query.strip()).split() if len(w) >= 2]
    return " OR ".join(f"{w}*" for w in tokens) if tokens else query.strip()


class BODSGleifAdapter(SourceAdapter):
    """Entity search/fetch over Open Ownership's GLEIF BODS bulk data.

    * ``search()`` — FTS5 name search (sub-50 ms for 4 M+ entities)
    * ``fetch()``  — DuckDB JOIN across Parquet files to reconstruct full
                     BODS 0.4 entity statement + linked relationship statements
    """

    id = "bods_gleif"

    def __init__(self) -> None:
        self._cache = Cache()
        self._fts_conn: sqlite3.Connection | None = None
        self._bootstrapped: bool = False

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        fts_ok = bool(
            settings.bods_gleif_fts_db
            and Path(settings.bods_gleif_fts_db).exists()
        )
        parquet_ok = bool(
            (settings.bods_gleif_parquet_dir and Path(settings.bods_gleif_parquet_dir).exists())
            or settings.bods_gleif_parquet_s3_base
        )
        live = bool(_DUCKDB_AVAILABLE and fts_ok and parquet_ok)
        return SourceInfo(
            id=self.id,
            name="Open Ownership GLEIF (BODS bulk)",
            homepage="https://bods-data.openownership.org/source/gleif_version_0_4/",
            description=(
                "GLEIF LEI data processed into BODS v0.4 by Open Ownership. "
                "Covers 4 M+ legal entities with LEI, registered name, "
                "jurisdiction, addresses, and direct/ultimate parent links."
            ),
            license="CC-BY-4.0",
            attribution=(
                "LEI data sourced from the Global Legal Entity Identifier "
                "Foundation (GLEIF), processed into BODS v0.4 by Open "
                "Ownership. Licensed under CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # S3 bootstrap (ephemeral-filesystem hosts such as Render)
    # ------------------------------------------------------------------

    def _bootstrap_from_s3(self) -> None:
        """Download the GLEIF FTS db (and optionally Parquet) from S3.

        Two modes, tried in order:

        **Option B — direct fts.db download** (preferred on Render):
          Set ``BODS_GLEIF_FTS_S3_URL`` to the public URL of just the
          ``fts.db`` file.  Only ~500 MB is downloaded; no Parquet files
          touch local disk.  Parquet is queried via HTTPFS using
          ``BODS_GLEIF_PARQUET_S3_BASE``.

        **Bundle zip** (legacy / local use):
          Set ``BODS_GLEIF_S3_URL`` to the bundle zip produced by
          ``setup_bods_data.py --create-bundle``.  Downloads zip + extracts
          ``parquet/`` + ``fts.db`` into the parent of ``BODS_GLEIF_FTS_DB``.
          Requires ~2 GB of /tmp which can trigger eviction on Render.
        """
        if self._bootstrapped:
            return

        settings = get_settings()
        fts_path = settings.bods_gleif_fts_db
        if not fts_path:
            return  # nowhere to put the db — wait for env var

        fts_p = Path(fts_path)
        if fts_p.exists():
            self._bootstrapped = True
            return  # already on disk

        fts_s3_url = settings.bods_gleif_fts_s3_url
        bundle_s3_url = settings.bods_gleif_s3_url

        if not fts_s3_url and not bundle_s3_url:
            return  # no S3 URL configured — nothing to do

        self._bootstrapped = True  # set before I/O; reset to False on failure

        dest_dir = fts_p.parent
        dest_dir.mkdir(parents=True, exist_ok=True)

        if fts_s3_url:
            # --- Option B: download fts.db directly ---
            logger.info("bods_gleif: downloading fts.db from %s …", fts_s3_url)
            try:
                with httpx.stream("GET", fts_s3_url, follow_redirects=True, timeout=600) as r:
                    r.raise_for_status()
                    with open(fts_p, "wb") as fh:
                        for chunk in r.iter_bytes(chunk_size=1 << 20):
                            fh.write(chunk)
                logger.info("bods_gleif: fts.db ready (%s MB)", fts_p.stat().st_size // 1_000_000)
            except Exception as exc:
                logger.warning("bods_gleif: fts.db download failed: %s", exc)
                fts_p.unlink(missing_ok=True)
                self._bootstrapped = False
        else:
            # --- Bundle zip: download + extract ---
            zip_path = dest_dir / "bundle.zip"
            logger.info("bods_gleif: downloading bundle from %s …", bundle_s3_url)
            try:
                with httpx.stream("GET", bundle_s3_url, follow_redirects=True, timeout=600) as r:
                    r.raise_for_status()
                    with open(zip_path, "wb") as fh:
                        for chunk in r.iter_bytes(chunk_size=1 << 20):
                            fh.write(chunk)
                logger.info("bods_gleif: extracting bundle …")
                with zipfile.ZipFile(zip_path) as zf:
                    zf.extractall(dest_dir)
                zip_path.unlink(missing_ok=True)
                logger.info("bods_gleif: S3 bootstrap complete")
            except Exception as exc:
                logger.warning("bods_gleif: S3 bootstrap failed: %s", exc)
                zip_path.unlink(missing_ok=True)
                self._bootstrapped = False

    # ------------------------------------------------------------------
    # FTS connection (lazy)
    # ------------------------------------------------------------------

    def _fts(self) -> sqlite3.Connection | None:
        settings = get_settings()
        fts_path = settings.bods_gleif_fts_db
        if not fts_path:
            return None
        if not Path(fts_path).exists():
            self._bootstrap_from_s3()
        if not Path(fts_path).exists():
            return None
        if self._fts_conn is not None:
            return self._fts_conn
        conn = sqlite3.connect(str(fts_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self._fts_conn = conn
        return conn

    # ------------------------------------------------------------------
    # Parquet directory / URL resolution
    # ------------------------------------------------------------------

    def _parquet_dir(self) -> Path | None:
        settings = get_settings()
        d = settings.bods_gleif_parquet_dir
        if not d:
            return None
        p = Path(d)
        if not p.exists():
            self._bootstrap_from_s3()
        return p if p.exists() else None

    def _parquet_url(self, filename: str) -> str | None:
        """Return a DuckDB-compatible path for a Parquet file.

        Returns a local path string when ``BODS_GLEIF_PARQUET_DIR`` is set and
        the file exists; otherwise builds an HTTPS URL from
        ``BODS_GLEIF_PARQUET_S3_BASE`` if that env var is configured.
        Returns ``None`` if neither source is available.
        """
        local = self._parquet_dir()
        if local is not None:
            p = local / filename
            return str(p) if p.exists() else None
        settings = get_settings()
        base = settings.bods_gleif_parquet_s3_base
        if base:
            return f"{base.rstrip('/')}/{filename}"
        return None

    def _parquet_available(self) -> bool:
        """True if at least one Parquet source (local or S3) is configured."""
        return self._parquet_url("entity_statement.parquet") is not None

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        fts_conn = self._fts()
        if fts_conn is not None:
            return await asyncio.to_thread(self._fts_search, fts_conn, query)

        # Stub path
        return self._stub_search(query)

    def _fts_search(self, conn: sqlite3.Connection, query: str) -> list[SourceHit]:
        # Try exact phrase match first.
        fts_q = _escape_fts5(query)
        cur = conn.execute(
            "SELECT statementid, name, entity_type, jurisdiction "
            "FROM entity_fts WHERE entity_fts MATCH ? LIMIT 20",
            (fts_q,),
        )
        rows = cur.fetchall()
        if not rows:
            # Fall back to prefix match on individual tokens.
            fts_q = _prefix_fts5(query)
            if fts_q:
                cur = conn.execute(
                    "SELECT statementid, name, entity_type, jurisdiction "
                    "FROM entity_fts WHERE entity_fts MATCH ? LIMIT 20",
                    (fts_q,),
                )
                rows = cur.fetchall()

        return [self._fts_row_to_hit(row) for row in rows]

    @staticmethod
    def _fts_row_to_hit(row: sqlite3.Row) -> SourceHit:
        statementid = row["statementid"]
        name = row["name"] or ""
        entity_type = row["entity_type"] or ""
        jurisdiction = row["jurisdiction"] or ""

        summary_bits: list[str] = []
        if jurisdiction:
            summary_bits.append(jurisdiction)
        if entity_type:
            summary_bits.append(entity_type.replace("_", " "))

        return SourceHit(
            source_id="bods_gleif",
            hit_id=statementid,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_bits) or "GLEIF entity",
            identifiers={"bods_gleif_statementid": statementid},
            raw={"statementid": statementid, "name": name},
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # LEI-keyed fetch (for /lookup integration)
    # ------------------------------------------------------------------

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Look up a GLEIF entity by LEI and return its full BODS bundle.

        Queries ``entity_recorddetails_identifiers.parquet`` for a row
        where ``scheme = 'LEI'`` and ``id = lei``, then delegates to
        ``_parquet_fetch`` for the full entity + relationship bundle.

        Returns ``None`` if the LEI is not found or data is not configured.
        """
        if not _DUCKDB_AVAILABLE or not self._parquet_available():
            return None
        return await asyncio.to_thread(self._parquet_fetch_by_lei, lei)

    def _parquet_fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Resolve LEI → statementId then return the full BODS fetch.

        Strategy (fast-path first):
        1. Query the ``lei_index`` SQLite table in the FTS db — populated at
           build time by ``setup_bods_data.py --source gleif``.  This is a
           sub-millisecond local lookup and avoids loading large Parquet files
           into memory.
        2. Fall back to a DuckDB HTTPFS JOIN if the index table is absent
           (e.g. old fts.db built before this feature was added).  Note: this
           JOIN requires ~700 MB RAM and will OOM on Render's free tier — the
           index table should always be present in production.
        """
        lei_upper = lei.upper()

        # ── Fast path: SQLite lei_index (built by setup_bods_data.py) ──────
        fts_conn = self._fts()
        if fts_conn:
            try:
                row = fts_conn.execute(
                    "SELECT statementid FROM lei_index WHERE lei = ?",
                    (lei_upper,),
                ).fetchone()
                if row:
                    statementid = row[0] if isinstance(row, tuple) else row["statementid"]
                    return self._parquet_fetch(statementid)
            except sqlite3.OperationalError:
                # lei_index table absent — fall through to HTTPFS JOIN
                logger.warning(
                    "bods_gleif: lei_index table not found in fts.db — "
                    "rebuild the FTS db with setup_bods_data.py --source gleif --skip-download"
                )

        # ── Slow path: DuckDB HTTPFS JOIN (may OOM on low-memory hosts) ────
        import duckdb

        ids_url = self._parquet_url("entity_recorddetails_identifiers.parquet")
        entity_url = self._parquet_url("entity_statement.parquet")
        if not ids_url or not entity_url:
            return None

        duck = duckdb.connect(":memory:")
        try:
            try:
                row = duck.execute(
                    """
                    SELECT i._link_entity_statement, es.statementid
                    FROM read_parquet(?) i
                    JOIN read_parquet(?) es ON es._link = i._link_entity_statement
                    WHERE i.scheme IN ('LEI', 'XI-LEI') AND i.id = ?
                    LIMIT 1
                    """,
                    [ids_url, entity_url, lei_upper],
                ).fetchone()
            except Exception as exc:
                logger.warning("bods_gleif: LEI HTTPFS lookup failed: %s", exc)
                return None
        finally:
            duck.close()

        if row is None:
            return None

        statementid = row[1]
        return self._parquet_fetch(statementid)

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        if _DUCKDB_AVAILABLE and self._parquet_available():
            return await asyncio.to_thread(self._parquet_fetch, hit_id)

        return {
            "source_id": self.id,
            "hit_id": hit_id,
            "is_stub": True,
        }

    def _parquet_fetch(self, statementid: str) -> dict[str, Any]:
        """Fetch a full BODS entity statement + relationships from Parquet.

        Parquet files may be local paths or HTTPS URLs (DuckDB HTTPFS).
        """
        import duckdb

        entity_url = self._parquet_url("entity_statement.parquet")
        ids_url = self._parquet_url("entity_recorddetails_identifiers.parquet")
        addrs_url = self._parquet_url("entity_recorddetails_addresses.parquet")
        rels_url = self._parquet_url("relationship_statement.parquet")
        rel_interests_url = self._parquet_url("relationship_recorddetails_interests.parquet")

        if not entity_url:
            return {"source_id": self.id, "hit_id": statementid, "is_stub": True}

        duck = duckdb.connect(":memory:")
        try:
            # Main entity row
            row = duck.execute(
                "SELECT * FROM read_parquet(?) WHERE statementid = ? LIMIT 1",
                [entity_url, statementid],
            ).fetchone()

            if row is None:
                return {"source_id": self.id, "hit_id": statementid, "is_stub": True}

            cols = [d[0] for d in duck.description]  # type: ignore[union-attr]
            entity_row = dict(zip(cols, row))
            link = entity_row.get("_link", "")

            # Identifiers sub-table — wrapped so a missing S3 file degrades
            # gracefully rather than crashing the whole fetch.
            identifiers: list[dict[str, str]] = []
            if ids_url and link:
                try:
                    id_rows = duck.execute(
                        "SELECT id, scheme, schemename, uri "
                        "FROM read_parquet(?) WHERE _link_entity_statement = ?",
                        [ids_url, link],
                    ).fetchall()
                    for id_row in id_rows:
                        ident: dict[str, str] = {}
                        if id_row[0]:
                            ident["id"] = str(id_row[0])
                        if id_row[1]:
                            ident["scheme"] = str(id_row[1])
                        if id_row[2]:
                            ident["schemeName"] = str(id_row[2])
                        if id_row[3]:
                            ident["uri"] = str(id_row[3])
                        if ident.get("id"):
                            identifiers.append(ident)
                except Exception as exc:
                    logger.warning("bods_gleif: identifiers sub-table unavailable (%s) — skipping", exc)

            # Addresses sub-table
            addresses: list[dict[str, Any]] = []
            if addrs_url and link:
                try:
                    addr_rows = duck.execute(
                        "SELECT type, address, postcode, country_name, country_code "
                        "FROM read_parquet(?) WHERE _link_entity_statement = ?",
                        [addrs_url, link],
                    ).fetchall()
                    for addr_row in addr_rows:
                        addr: dict[str, Any] = {}
                        if addr_row[0]:
                            addr["type"] = str(addr_row[0])
                        if addr_row[1]:
                            addr["address"] = str(addr_row[1])
                        if addr_row[2]:
                            addr["postcode"] = str(addr_row[2])
                        if addr_row[3] or addr_row[4]:
                            addr["country"] = {
                                k: v for k, v in [
                                    ("name", addr_row[3]),
                                    ("code", addr_row[4]),
                                ] if v
                            }
                        addresses.append(addr)
                except Exception as exc:
                    logger.warning("bods_gleif: addresses sub-table unavailable (%s) — skipping", exc)

            # Reconstruct BODS 0.4 entity statement
            entity_stmt = _build_entity_statement(entity_row, identifiers, addresses)

            # Relationship statements (direct/ultimate parents)
            relationship_stmts: list[dict[str, Any]] = []
            if rels_url:
                try:
                    rel_rows = duck.execute(
                        """
                        SELECT statementid,
                               recordDetails_subject,
                               recordDetails_interestedParty
                        FROM read_parquet(?)
                        WHERE recordDetails_subject = ?
                           OR recordDetails_interestedParty = ?
                        LIMIT 50
                        """,
                        [rels_url, statementid, statementid],
                    ).fetchall()
                except Exception as exc:
                    logger.warning("bods_gleif: relationship_statement unavailable (%s) — skipping", exc)
                    rel_rows = []
                for rel_row in rel_rows:
                    # Fetch interests for this relationship
                    rel_link_row = duck.execute(
                        "SELECT _link FROM read_parquet(?) WHERE statementid = ? LIMIT 1",
                        [rels_url, rel_row[0]],
                    ).fetchone() if rel_row[0] else None
                    rel_link = rel_link_row[0] if rel_link_row else ""
                    interests: list[dict[str, Any]] = []
                    if rel_interests_url and rel_link:
                        try:
                            int_rows = duck.execute(
                                """
                                SELECT directOrIndirect, type, beneficialOwnershipOrControl,
                                       details, startDate
                                FROM read_parquet(?)
                                WHERE _link_relationship_statement = ?
                                """,
                                [rel_interests_url, rel_link],
                            ).fetchall()
                        except Exception:
                            int_rows = []
                        for ir in int_rows:
                            interest: dict[str, Any] = {}
                            if ir[0]: interest["directOrIndirect"] = str(ir[0])
                            if ir[1]: interest["type"] = str(ir[1])
                            if ir[2] is not None: interest["beneficialOwnershipOrControl"] = bool(ir[2])
                            if ir[3]: interest["details"] = str(ir[3])
                            if ir[4]: interest["startDate"] = str(ir[4])[:10]
                            if interest:
                                interests.append(interest)
                    relationship_stmts.append(
                        _build_relationship_statement(rel_row, interests)
                    )

        finally:
            duck.close()

        # Build the cross-source identifier map (for reconciler)
        cross_ids: dict[str, str] = {"bods_gleif_statementid": statementid}
        for ident in identifiers:
            if ident.get("scheme") in {"LEI", "XI-LEI"} and ident.get("id"):
                cross_ids["lei"] = ident["id"]

        all_stmts = [entity_stmt] + relationship_stmts

        return {
            "source_id": self.id,
            "hit_id": statementid,
            "is_stub": False,
            "bods_statements": all_stmts,
            # Expose identifiers flat for reconciler cross-linking
            "identifiers": cross_ids,
        }

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="bods-gleif-stub-0001",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub BODS GLEIF record — run scripts/setup_bods_data.py "
                    "--source gleif and set BODS_GLEIF_PARQUET_DIR / BODS_GLEIF_FTS_DB "
                    "to enable live search."
                ),
                identifiers={"bods_gleif_statementid": "bods-gleif-stub-0001"},
                raw={"statementid": "bods-gleif-stub-0001"},
                is_stub=True,
            )
        ]


# ---------------------------------------------------------------------------
# BODS statement reconstruction helpers
# ---------------------------------------------------------------------------


def _build_entity_statement(
    row: dict[str, Any],
    identifiers: list[dict[str, str]],
    addresses: list[dict[str, Any]],
) -> dict[str, Any]:
    """Reconstruct a BODS 0.4 entityStatement from a Flatterer-flatten row."""
    statementid = row.get("statementid") or ""
    pub_date = row.get("publicationdetails_publicationdate") or row.get("publicationdetails_date") or ""

    record_details: dict[str, Any] = {
        "isComponent": False,
        "entityType": {"type": row.get("recorddetails_entitytype_type") or "unknownEntity"},
        "name": row.get("recorddetails_name") or "",
        "identifiers": identifiers,
    }

    # Jurisdiction
    jname = row.get("recorddetails_jurisdiction_name") or ""
    jcode = row.get("recorddetails_jurisdiction_code") or ""
    if jname or jcode:
        record_details["incorporatedInJurisdiction"] = {
            k: v for k, v in [("name", jname), ("code", jcode)] if v
        }

    # Dates
    founding = row.get("recorddetails_foundingdate") or ""
    if founding:
        record_details["foundingDate"] = founding[:10]  # trim to YYYY-MM-DD
    dissolution = row.get("recorddetails_dissolutiondate") or ""
    if dissolution:
        record_details["dissolutionDate"] = dissolution[:10]

    if addresses:
        record_details["addresses"] = addresses

    stmt: dict[str, Any] = {
        "statementId": statementid,
        "recordId": statementid,
        "statementType": "entityStatement",
        "recordDetails": record_details,
        "publicationDetails": {
            "bodsVersion": "0.4",
            "publisher": {"name": "OpenCheck (via Open Ownership GLEIF bulk data)"},
        },
    }
    if pub_date:
        stmt["publicationDetails"]["publicationDate"] = pub_date[:10]

    source_url = row.get("source_url") or ""
    if source_url:
        stmt["source"] = {"url": source_url, "type": "officialRegister"}

    return stmt


def _build_relationship_statement(
    row: tuple[Any, ...],
    interests: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Reconstruct a minimal BODS 0.4 relationshipStatement from a row.

    Row columns (as selected in _parquet_fetch):
      0 — statementid
      1 — recordDetails_subject (entity statementId)
      2 — recordDetails_interestedParty (entity statementId — GLEIF has entity-entity links)
    """
    statementid = row[0] or ""
    subject = row[1] or ""
    interested_party = row[2] or ""

    return {
        "statementId": statementid,
        "recordId": statementid,
        "statementType": "relationshipStatement",
        "recordDetails": {
            "isComponent": False,
            "subject": {"describedByEntityStatement": subject},
            "interestedParty": {"describedByEntityStatement": interested_party},
            "interests": interests or [],
        },
        "publicationDetails": {
            "bodsVersion": "0.4",
            "publisher": {"name": "OpenCheck (via Open Ownership GLEIF bulk data)"},
        },
    }
