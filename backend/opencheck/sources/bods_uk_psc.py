"""Open Ownership BODS bulk data adapter — UK PSC.

Queries the pre-extracted Open Ownership BODS v0.4 Parquet files for the
UK Persons with Significant Control (PSC) dataset.  The Parquet files are
built by Open Ownership from Companies House PSC bulk data and published at:
https://bods-data.openownership.org/source/uk_version_0_4/

Files are produced by Flatterer, same convention as GLEIF.  The UK PSC
dataset also includes ``person_statement.parquet`` because PSC records
identify natural persons as controllers — so this adapter supports both
``SearchKind.ENTITY`` (company names) and ``SearchKind.PERSON``.

Key Parquet files:

* ``entity_statement.parquet``           — one row per company
* ``entity_recordDetails_identifiers.parquet``
* ``entity_recordDetails_addresses.parquet``
* ``person_statement.parquet``           — one row per PSC individual
* ``person_recordDetails_names.parquet`` — name components (fullname, etc.)
* ``person_recordDetails_nationalities.parquet``
* ``relationship_statement.parquet``     — PSC → company ownership links

Setup
-----
Run ``python scripts/setup_bods_data.py --source uk_psc`` once, then set:

    BODS_UK_PSC_PARQUET_DIR=/path/to/data/bods/uk_psc/parquet
    BODS_UK_PSC_FTS_DB=/path/to/data/bods/uk_psc/fts.db
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

_CACHE_NS = "bods_uk_psc"

try:
    import duckdb as _duckdb  # noqa: F401
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False
    logger.warning("bods_uk_psc: duckdb not installed — live queries unavailable")


def _escape_fts5(query: str) -> str:
    cleaned = re.sub(r'["\']', " ", query.strip())
    return f'"{cleaned}"'


def _prefix_fts5(query: str) -> str:
    tokens = [w for w in re.sub(r'["\']', " ", query.strip()).split() if len(w) >= 2]
    return " OR ".join(f"{w}*" for w in tokens) if tokens else query.strip()


class BODSUKPSCAdapter(SourceAdapter):
    """Entity + person search/fetch over Open Ownership's UK PSC BODS bulk data.

    * ``search(kind=ENTITY)`` — FTS5 company-name search
    * ``search(kind=PERSON)`` — FTS5 PSC person-name search
    * ``fetch()``             — DuckDB Parquet reconstruction of full BODS
                                entity/person statement + ownership relationships
    """

    id = "bods_uk_psc"

    def __init__(self) -> None:
        self._cache = Cache()
        self._fts_conn: sqlite3.Connection | None = None
        self._bootstrapped: bool = False

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        fts_ok = bool(
            settings.bods_uk_psc_fts_db
            and Path(settings.bods_uk_psc_fts_db).exists()
        )
        parquet_ok = bool(
            (settings.bods_uk_psc_parquet_dir and Path(settings.bods_uk_psc_parquet_dir).exists())
            or settings.bods_uk_psc_parquet_s3_base
        )
        live = bool(_DUCKDB_AVAILABLE and fts_ok and parquet_ok)
        return SourceInfo(
            id=self.id,
            name="Open Ownership UK PSC (BODS bulk)",
            homepage="https://bods-data.openownership.org/source/uk_version_0_4/",
            description=(
                "UK Persons with Significant Control data processed into "
                "BODS v0.4 by Open Ownership. Covers millions of entities "
                "and PSC individuals with ownership and control interests."
            ),
            license="OGL-UK-3.0",
            attribution=(
                "UK PSC data sourced from Companies House, processed into "
                "BODS v0.4 by Open Ownership. "
                "Licensed under the Open Government Licence v3.0."
            ),
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=False,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # S3 bootstrap (ephemeral-filesystem hosts such as Render)
    # ------------------------------------------------------------------

    def _bootstrap_from_s3(self) -> None:
        """Download the UK PSC FTS db from S3.

        Mirrors the two-mode logic of BODSGleifAdapter._bootstrap_from_s3:

        * **Option B** — set ``BODS_UK_PSC_FTS_S3_URL`` to just the fts.db.
          Only ~500 MB downloaded; Parquet queried via HTTPFS.
        * **Bundle zip** (legacy) — set ``BODS_UK_PSC_S3_URL`` to the full
          bundle produced by ``setup_bods_data.py --create-bundle``.
        """
        if self._bootstrapped:
            return

        settings = get_settings()
        fts_path = settings.bods_uk_psc_fts_db
        if not fts_path:
            return

        fts_p = Path(fts_path)
        if fts_p.exists():
            self._bootstrapped = True
            return

        fts_s3_url = settings.bods_uk_psc_fts_s3_url
        bundle_s3_url = settings.bods_uk_psc_s3_url

        if not fts_s3_url and not bundle_s3_url:
            return

        self._bootstrapped = True

        dest_dir = fts_p.parent
        dest_dir.mkdir(parents=True, exist_ok=True)

        if fts_s3_url:
            logger.info("bods_uk_psc: downloading fts.db from %s …", fts_s3_url)
            try:
                with httpx.stream("GET", fts_s3_url, follow_redirects=True, timeout=600) as r:
                    r.raise_for_status()
                    with open(fts_p, "wb") as fh:
                        for chunk in r.iter_bytes(chunk_size=1 << 20):
                            fh.write(chunk)
                logger.info("bods_uk_psc: fts.db ready (%s MB)", fts_p.stat().st_size // 1_000_000)
            except Exception as exc:
                logger.warning("bods_uk_psc: fts.db download failed: %s", exc)
                fts_p.unlink(missing_ok=True)
                self._bootstrapped = False
        else:
            zip_path = dest_dir / "bundle.zip"
            logger.info("bods_uk_psc: downloading bundle from %s …", bundle_s3_url)
            try:
                with httpx.stream("GET", bundle_s3_url, follow_redirects=True, timeout=600) as r:
                    r.raise_for_status()
                    with open(zip_path, "wb") as fh:
                        for chunk in r.iter_bytes(chunk_size=1 << 20):
                            fh.write(chunk)
                logger.info("bods_uk_psc: extracting bundle …")
                with zipfile.ZipFile(zip_path) as zf:
                    zf.extractall(dest_dir)
                zip_path.unlink(missing_ok=True)
                logger.info("bods_uk_psc: S3 bootstrap complete")
            except Exception as exc:
                logger.warning("bods_uk_psc: S3 bootstrap failed: %s", exc)
                zip_path.unlink(missing_ok=True)
                self._bootstrapped = False

    # ------------------------------------------------------------------
    # FTS connection (lazy)
    # ------------------------------------------------------------------

    def _fts(self) -> sqlite3.Connection | None:
        settings = get_settings()
        fts_path = settings.bods_uk_psc_fts_db
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

    def _parquet_dir(self) -> Path | None:
        settings = get_settings()
        d = settings.bods_uk_psc_parquet_dir
        if not d:
            return None
        p = Path(d)
        if not p.exists():
            self._bootstrap_from_s3()
        return p if p.exists() else None

    def _parquet_url(self, filename: str) -> str | None:
        """Return a DuckDB-compatible path for a Parquet file.

        Returns a local path string if ``BODS_UK_PSC_PARQUET_DIR`` is set and
        exists; otherwise falls back to an HTTPS URL built from
        ``BODS_UK_PSC_PARQUET_S3_BASE`` if that is configured.  Returns ``None``
        if neither is available.
        """
        local = self._parquet_dir()
        if local is not None:
            p = local / filename
            return str(p) if p.exists() else None
        settings = get_settings()
        base = settings.bods_uk_psc_parquet_s3_base
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
        fts_conn = self._fts()
        if fts_conn is not None:
            if kind == SearchKind.ENTITY:
                return await asyncio.to_thread(self._fts_entity_search, fts_conn, query)
            elif kind == SearchKind.PERSON:
                return await asyncio.to_thread(self._fts_person_search, fts_conn, query)

        # Stub path
        return self._stub_search(query, kind)

    def _fts_entity_search(self, conn: sqlite3.Connection, query: str) -> list[SourceHit]:
        # Check entity_fts table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='entity_fts'"
        )
        if not cur.fetchone():
            return []

        fts_q = _escape_fts5(query)
        cur = conn.execute(
            "SELECT statementid, name, entity_type, jurisdiction "
            "FROM entity_fts WHERE entity_fts MATCH ? LIMIT 20",
            (fts_q,),
        )
        rows = cur.fetchall()
        if not rows:
            fts_q = _prefix_fts5(query)
            if fts_q:
                cur = conn.execute(
                    "SELECT statementid, name, entity_type, jurisdiction "
                    "FROM entity_fts WHERE entity_fts MATCH ? LIMIT 20",
                    (fts_q,),
                )
                rows = cur.fetchall()

        return [self._entity_row_to_hit(row) for row in rows]

    def _fts_person_search(self, conn: sqlite3.Connection, query: str) -> list[SourceHit]:
        # Check person_fts table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='person_fts'"
        )
        if not cur.fetchone():
            return []

        fts_q = _escape_fts5(query)
        cur = conn.execute(
            "SELECT statementid, name, nationality "
            "FROM person_fts WHERE person_fts MATCH ? LIMIT 20",
            (fts_q,),
        )
        rows = cur.fetchall()
        if not rows:
            fts_q = _prefix_fts5(query)
            if fts_q:
                cur = conn.execute(
                    "SELECT statementid, name, nationality "
                    "FROM person_fts WHERE person_fts MATCH ? LIMIT 20",
                    (fts_q,),
                )
                rows = cur.fetchall()

        return [self._person_row_to_hit(row) for row in rows]

    @staticmethod
    def _entity_row_to_hit(row: sqlite3.Row) -> SourceHit:
        statementid = row["statementid"]
        name = row["name"] or ""
        jurisdiction = row["jurisdiction"] or ""

        summary_bits: list[str] = []
        if jurisdiction:
            summary_bits.append(jurisdiction)
        summary_bits.append("UK PSC entity")

        return SourceHit(
            source_id="bods_uk_psc",
            hit_id=statementid,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_bits),
            identifiers={"bods_uk_psc_statementid": statementid},
            raw={"statementid": statementid, "name": name},
            is_stub=False,
        )

    @staticmethod
    def _person_row_to_hit(row: sqlite3.Row) -> SourceHit:
        statementid = row["statementid"]
        name = row["name"] or ""
        nationality = row["nationality"] or ""

        summary_bits: list[str] = []
        if nationality:
            summary_bits.append(nationality)
        summary_bits.append("UK PSC person")

        return SourceHit(
            source_id="bods_uk_psc",
            hit_id=statementid,
            kind=SearchKind.PERSON,
            name=name,
            summary=" · ".join(summary_bits),
            identifiers={"bods_uk_psc_statementid": statementid},
            raw={"statementid": statementid, "name": name},
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        if _DUCKDB_AVAILABLE and self._parquet_available():
            return await asyncio.to_thread(self._parquet_fetch, hit_id)
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

    def _parquet_fetch(self, statementid: str) -> dict[str, Any]:
        """Fetch a BODS statement by statementid.

        Tries entity_statement.parquet first; falls back to person_statement.parquet.
        Parquet files may be local paths or HTTPS URLs (DuckDB HTTPFS).
        """
        import duckdb

        entity_url = self._parquet_url("entity_statement.parquet")
        person_url = self._parquet_url("person_statement.parquet")
        ids_url = self._parquet_url("entity_recordDetails_identifiers.parquet")
        addrs_url = self._parquet_url("entity_recordDetails_addresses.parquet")
        rels_url = self._parquet_url("relationship_statement.parquet")
        rel_interests_url = self._parquet_url("relationship_recordDetails_interests.parquet")

        duck = duckdb.connect(":memory:")
        try:
            # Try entity first
            if entity_url:
                row = duck.execute(
                    "SELECT * FROM read_parquet(?) WHERE statementid = ? LIMIT 1",
                    [entity_url, statementid],
                ).fetchone()
                if row is not None:
                    cols = [d[0] for d in duck.description]  # type: ignore[union-attr]
                    entity_row = dict(zip(cols, row))
                    link = entity_row.get("_link", "")

                    identifiers = _fetch_identifiers(duck, ids_url, link)
                    addresses = _fetch_addresses(duck, addrs_url, link)
                    entity_stmt = _build_entity_statement_psc(entity_row, identifiers, addresses)

                    rel_stmts = _fetch_relationship_statements(
                        duck, rels_url, rel_interests_url, person_url, statementid, is_person=False
                    )

                    cross_ids: dict[str, str] = {"bods_uk_psc_statementid": statementid}
                    for ident in identifiers:
                        if ident.get("scheme") == "GB-COH" and ident.get("id"):
                            cross_ids["gb_coh"] = ident["id"]

                    return {
                        "source_id": self.id,
                        "hit_id": statementid,
                        "is_stub": False,
                        "bods_statements": [entity_stmt] + rel_stmts,
                        "identifiers": cross_ids,
                    }

            # Try person
            if person_url:
                row = duck.execute(
                    "SELECT * FROM read_parquet(?) WHERE statementid = ? LIMIT 1",
                    [person_url, statementid],
                ).fetchone()
                if row is not None:
                    cols = [d[0] for d in duck.description]  # type: ignore[union-attr]
                    person_row = dict(zip(cols, row))
                    link = person_row.get("_link", "")

                    person_stmt = _build_person_statement_psc(duck, self._parquet_url, person_row, link)
                    rel_stmts = _fetch_relationship_statements(
                        duck, rels_url, rel_interests_url, person_url, statementid, is_person=True
                    )

                    return {
                        "source_id": self.id,
                        "hit_id": statementid,
                        "is_stub": False,
                        "bods_statements": [person_stmt] + rel_stmts,
                        "identifiers": {"bods_uk_psc_statementid": statementid},
                    }

        finally:
            duck.close()

        return {"source_id": self.id, "hit_id": statementid, "is_stub": True}

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        label = "entity" if kind == SearchKind.ENTITY else "person"
        return [
            SourceHit(
                source_id=self.id,
                hit_id=f"bods-uk-psc-stub-0001-{label}",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub BODS UK PSC record — run scripts/setup_bods_data.py "
                    "--source uk_psc and set BODS_UK_PSC_PARQUET_DIR / "
                    "BODS_UK_PSC_FTS_DB to enable live search."
                ),
                identifiers={"bods_uk_psc_statementid": f"bods-uk-psc-stub-0001-{label}"},
                raw={"statementid": f"bods-uk-psc-stub-0001-{label}"},
                is_stub=True,
            )
        ]


# ---------------------------------------------------------------------------
# DuckDB sub-table helpers
# ---------------------------------------------------------------------------


def _fetch_identifiers(
    duck: Any,
    ids_url: str | None,
    link: str,
) -> list[dict[str, str]]:
    if not ids_url or not link:
        return []
    id_rows = duck.execute(
        "SELECT id, scheme, schemename, uri "
        "FROM read_parquet(?) WHERE _link_entity_statement = ?",
        [ids_url, link],
    ).fetchall()
    result: list[dict[str, str]] = []
    for r in id_rows:
        ident: dict[str, str] = {}
        if r[0]:
            ident["id"] = str(r[0])
        if r[1]:
            ident["scheme"] = str(r[1])
        if r[2]:
            ident["schemeName"] = str(r[2])
        if r[3]:
            ident["uri"] = str(r[3])
        if ident.get("id"):
            result.append(ident)
    return result


def _fetch_addresses(
    duck: Any,
    addrs_url: str | None,
    link: str,
) -> list[dict[str, Any]]:
    if not addrs_url or not link:
        return []
    addr_rows = duck.execute(
        "SELECT type, address, postcode, country_name, country_code "
        "FROM read_parquet(?) WHERE _link_entity_statement = ?",
        [addrs_url, link],
    ).fetchall()
    result: list[dict[str, Any]] = []
    for r in addr_rows:
        addr: dict[str, Any] = {}
        if r[0]:
            addr["type"] = str(r[0])
        if r[1]:
            addr["address"] = str(r[1])
        if r[2]:
            addr["postcode"] = str(r[2])
        if r[3] or r[4]:
            addr["country"] = {k: v for k, v in [("name", r[3]), ("code", r[4])] if v}
        result.append(addr)
    return result


def _fetch_relationship_statements(
    duck: Any,
    rels_url: str | None,
    rel_interests_url: str | None,
    person_url: str | None,
    statementid: str,
    is_person: bool = False,
) -> list[dict[str, Any]]:
    """Fetch relationship statements that reference *statementid*.

    In the Open Ownership UK PSC Parquet (Flatterer output) the relationship
    columns are plain VARCHAR statementId strings, not nested objects:

    * ``recordDetails_subject``        — entity statementId (always a company)
    * ``recordDetails_interestedParty`` — person or entity statementId (the PSC)

    ``is_person`` should be True when the caller fetched a person statement so
    the interestedParty reference is constructed correctly.  When False (entity
    fetch), the interestedParty could be a person or a corporate PSC — we do a
    quick point-lookup against person_statement.parquet to decide.
    """
    if not rels_url:
        return []

    rel_rows = duck.execute(
        """
        SELECT statementid, _link,
               recordDetails_subject,
               recordDetails_interestedParty
        FROM read_parquet(?)
        WHERE recordDetails_subject = ?
           OR recordDetails_interestedParty = ?
        LIMIT 50
        """,
        [rels_url, statementid, statementid],
    ).fetchall()

    stmts: list[dict[str, Any]] = []
    for rel_row in rel_rows:
        rel_statementid = rel_row[0] or ""
        rel_link = rel_row[1] or ""
        subject_id = rel_row[2] or ""
        ip_id = rel_row[3] or ""

        # ----- interests sub-table -----
        interests: list[dict[str, Any]] = []
        if rel_interests_url and rel_link:
            int_rows = duck.execute(
                """
                SELECT directOrIndirect, type, beneficialOwnershipOrControl,
                       details, startDate, share_minimum, share_maximum
                FROM read_parquet(?)
                WHERE _link_relationship_statement = ?
                """,
                [rel_interests_url, rel_link],
            ).fetchall()
            for ir in int_rows:
                interest: dict[str, Any] = {}
                if ir[0]:
                    interest["directOrIndirect"] = str(ir[0])
                if ir[1]:
                    interest["type"] = str(ir[1])
                if ir[2] is not None:
                    interest["beneficialOwnershipOrControl"] = bool(ir[2])
                if ir[3]:
                    interest["details"] = str(ir[3])
                if ir[4]:
                    interest["startDate"] = str(ir[4])[:10]
                if ir[5] is not None or ir[6] is not None:
                    share: dict[str, float] = {}
                    if ir[5] is not None:
                        share["minimum"] = float(ir[5])
                    if ir[6] is not None:
                        share["maximum"] = float(ir[6])
                    if share:
                        interest["share"] = share
                if interest:
                    interests.append(interest)

        # ----- subject reference (always entity in UK PSC) -----
        subject_ref: dict[str, str] = (
            {"describedByEntityStatement": subject_id} if subject_id else {}
        )

        # ----- interestedParty reference -----
        if is_person:
            # We fetched a person — ip_id IS our statementid or another person
            ip_ref: dict[str, str] = (
                {"describedByPersonStatement": ip_id} if ip_id else {}
            )
        else:
            # We fetched an entity — determine whether ip_id is a person or entity
            ip_is_person = False
            if ip_id and person_url:
                try:
                    hit = duck.execute(
                        "SELECT 1 FROM read_parquet(?) WHERE statementid = ? LIMIT 1",
                        [person_url, ip_id],
                    ).fetchone()
                    ip_is_person = hit is not None
                except Exception:
                    pass  # treat as entity if lookup fails
            ip_ref = (
                {"describedByPersonStatement": ip_id}
                if ip_is_person
                else {"describedByEntityStatement": ip_id}
            ) if ip_id else {}

        stmts.append({
            "statementId": rel_statementid,
            "recordId": rel_statementid,
            "statementType": "relationshipStatement",
            "recordDetails": {
                "isComponent": False,
                "subject": subject_ref,
                "interestedParty": ip_ref,
                "interests": interests,
            },
            "publicationDetails": {
                "bodsVersion": "0.4",
                "publisher": {"name": "OpenCheck (via Open Ownership UK PSC bulk data)"},
            },
        })

    return stmts


# ---------------------------------------------------------------------------
# Statement reconstruction helpers
# ---------------------------------------------------------------------------


def _build_entity_statement_psc(
    row: dict[str, Any],
    identifiers: list[dict[str, str]],
    addresses: list[dict[str, Any]],
) -> dict[str, Any]:
    statementid = row.get("statementid") or ""
    pub_date = row.get("publicationdetails_publicationdate") or ""

    record_details: dict[str, Any] = {
        "isComponent": False,
        "entityType": {"type": row.get("recorddetails_entitytype_type") or "registeredEntity"},
        "name": row.get("recorddetails_name") or "",
        "identifiers": identifiers,
    }

    jname = row.get("recorddetails_jurisdiction_name") or ""
    jcode = row.get("recorddetails_jurisdiction_code") or ""
    if jname or jcode:
        record_details["incorporatedInJurisdiction"] = {
            k: v for k, v in [("name", jname), ("code", jcode)] if v
        }

    founding = row.get("recorddetails_foundingdate") or ""
    if founding:
        record_details["foundingDate"] = str(founding)[:10]

    if addresses:
        record_details["addresses"] = addresses

    stmt: dict[str, Any] = {
        "statementId": statementid,
        "recordId": statementid,
        "statementType": "entityStatement",
        "recordDetails": record_details,
        "publicationDetails": {
            "bodsVersion": "0.4",
            "publisher": {"name": "OpenCheck (via Open Ownership UK PSC bulk data)"},
        },
    }
    if pub_date:
        stmt["publicationDetails"]["publicationDate"] = str(pub_date)[:10]

    return stmt


def _build_person_statement_psc(
    duck: Any,
    parquet_url_fn: Any,  # callable(filename) -> str | None
    row: dict[str, Any],
    link: str,
) -> dict[str, Any]:
    """Reconstruct a BODS 0.4 personStatement from Flatterer-flatten rows.

    Names live in ``person_recordDetails_names.parquet`` (column ``fullName``).
    Nationalities live in ``person_recordDetails_nationalities.parquet``.
    Birth date is a single VARCHAR column ``recordDetails_birthDate`` in
    ``person_statement.parquet`` with format ``YYYY-MM``.
    """
    statementid = row.get("statementid") or ""
    pub_date = row.get("publicationdetails_publicationdate") or ""

    # Names sub-table — actual column is ``fullName`` (DuckDB case-insensitive)
    names_url = parquet_url_fn("person_recordDetails_names.parquet")
    full_name = ""
    names: list[dict[str, Any]] = []
    if names_url and link:
        try:
            name_rows = duck.execute(
                "SELECT fullName, type FROM read_parquet(?) WHERE _link_person_statement = ?",
                [names_url, link],
            ).fetchall()
            for nr in name_rows:
                n: dict[str, Any] = {}
                if nr[0]:
                    n["fullName"] = str(nr[0])
                    if not full_name:
                        full_name = str(nr[0])
                if nr[1]:
                    n["type"] = str(nr[1])
                if n:
                    names.append(n)
        except Exception:
            pass

    # Nationalities sub-table
    nats_url = parquet_url_fn("person_recordDetails_nationalities.parquet")
    nationalities: list[dict[str, Any]] = []
    if nats_url and link:
        try:
            nat_rows = duck.execute(
                "SELECT name, code FROM read_parquet(?) WHERE _link_person_statement = ?",
                [nats_url, link],
            ).fetchall()
            for nr in nat_rows:
                n2 = {k: v for k, v in [("name", nr[0]), ("code", nr[1])] if v}
                if n2:
                    nationalities.append(n2)
        except Exception:
            pass

    record_details: dict[str, Any] = {
        "isComponent": False,
        "personType": row.get("recorddetails_persontype") or "knownPerson",
        "names": names or ([{"fullName": full_name, "type": "individual"}] if full_name else []),
    }

    if nationalities:
        record_details["nationalities"] = nationalities

    # Birth date: single VARCHAR ``recordDetails_birthDate`` → ``YYYY-MM`` format
    birth_date = row.get("recorddetails_birthdate") or ""
    if birth_date:
        record_details["birthDate"] = str(birth_date)[:7]  # keep YYYY-MM, trim any extra

    stmt: dict[str, Any] = {
        "statementId": statementid,
        "recordId": statementid,
        "statementType": "personStatement",
        "recordDetails": record_details,
        "publicationDetails": {
            "bodsVersion": "0.4",
            "publisher": {"name": "OpenCheck (via Open Ownership UK PSC bulk data)"},
        },
    }
    if pub_date:
        stmt["publicationDetails"]["publicationDate"] = str(pub_date)[:10]

    return stmt
