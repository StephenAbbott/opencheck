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
        live = bool(
            _DUCKDB_AVAILABLE
            and settings.bods_uk_psc_parquet_dir
            and Path(settings.bods_uk_psc_parquet_dir).exists()
            and settings.bods_uk_psc_fts_db
            and Path(settings.bods_uk_psc_fts_db).exists()
        )
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
        """Download and extract the UK PSC bundle zip from S3 if needed."""
        if self._bootstrapped:
            return
        self._bootstrapped = True

        settings = get_settings()
        s3_url = settings.bods_uk_psc_s3_url
        fts_path = settings.bods_uk_psc_fts_db
        if not s3_url or not fts_path:
            return

        fts_p = Path(fts_path)
        if fts_p.exists():
            return

        dest_dir = fts_p.parent
        zip_path = dest_dir / "bundle.zip"
        dest_dir.mkdir(parents=True, exist_ok=True)

        logger.info("bods_uk_psc: downloading bundle from S3 …")
        try:
            with httpx.stream("GET", s3_url, follow_redirects=True, timeout=600) as r:
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
        parquet_dir = self._parquet_dir()
        if parquet_dir is not None and _DUCKDB_AVAILABLE:
            return await asyncio.to_thread(self._parquet_fetch, parquet_dir, hit_id)
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

    def _parquet_fetch(self, parquet_dir: Path, statementid: str) -> dict[str, Any]:
        """Fetch a BODS statement by statementid.

        Tries entity_statement.parquet first; falls back to person_statement.parquet.
        """
        import duckdb

        entity_p = parquet_dir / "entity_statement.parquet"
        person_p = parquet_dir / "person_statement.parquet"
        ids_p = parquet_dir / "entity_recordDetails_identifiers.parquet"
        addrs_p = parquet_dir / "entity_recordDetails_addresses.parquet"
        rels_p = parquet_dir / "relationship_statement.parquet"
        rel_interests_p = parquet_dir / "relationship_recordDetails_interests.parquet"

        duck = duckdb.connect(":memory:")
        try:
            # Try entity first
            if entity_p.exists():
                row = duck.execute(
                    "SELECT * FROM read_parquet(?) WHERE statementid = ? LIMIT 1",
                    [str(entity_p), statementid],
                ).fetchone()
                if row is not None:
                    cols = [d[0] for d in duck.description]  # type: ignore[union-attr]
                    entity_row = dict(zip(cols, row))
                    link = entity_row.get("_link", "")

                    identifiers = _fetch_identifiers(duck, ids_p, link)
                    addresses = _fetch_addresses(duck, addrs_p, link)
                    entity_stmt = _build_entity_statement_psc(entity_row, identifiers, addresses)

                    rel_stmts = _fetch_relationship_statements(duck, rels_p, rel_interests_p, statementid)

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
            if person_p.exists():
                row = duck.execute(
                    "SELECT * FROM read_parquet(?) WHERE statementid = ? LIMIT 1",
                    [str(person_p), statementid],
                ).fetchone()
                if row is not None:
                    cols = [d[0] for d in duck.description]  # type: ignore[union-attr]
                    person_row = dict(zip(cols, row))
                    link = person_row.get("_link", "")

                    person_stmt = _build_person_statement_psc(duck, parquet_dir, person_row, link)
                    rel_stmts = _fetch_relationship_statements(duck, rels_p, rel_interests_p, statementid)

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
    ids_p: Path,
    link: str,
) -> list[dict[str, str]]:
    if not ids_p.exists() or not link:
        return []
    id_rows = duck.execute(
        "SELECT id, scheme, schemename, uri "
        "FROM read_parquet(?) WHERE _link_entity_statement = ?",
        [str(ids_p), link],
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
    addrs_p: Path,
    link: str,
) -> list[dict[str, Any]]:
    if not addrs_p.exists() or not link:
        return []
    addr_rows = duck.execute(
        "SELECT type, address, postcode, country_name, country_code "
        "FROM read_parquet(?) WHERE _link_entity_statement = ?",
        [str(addrs_p), link],
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
    rels_p: Path,
    rel_interests_p: Path,
    statementid: str,
) -> list[dict[str, Any]]:
    if not rels_p.exists():
        return []

    rel_rows = duck.execute(
        """
        SELECT statementid, _link,
               recorddetails_subject,
               recorddetails_interestedparty,
               recorddetails_subject_describedbyentitystatement,
               recorddetails_interestedparty_describedbyentitystatement,
               recorddetails_interestedparty_describedbynonentitystatement
        FROM read_parquet(?)
        WHERE recorddetails_subject = ?
           OR recorddetails_interestedparty = ?
           OR recorddetails_subject_describedbyentitystatement = ?
           OR recorddetails_interestedparty_describedbyentitystatement = ?
        LIMIT 50
        """,
        [str(rels_p), statementid, statementid, statementid, statementid],
    ).fetchall()

    stmts: list[dict[str, Any]] = []
    for rel_row in rel_rows:
        rel_link = rel_row[1] or ""
        interests: list[dict[str, Any]] = []
        if rel_interests_p.exists() and rel_link:
            int_rows = duck.execute(
                """
                SELECT directorindirect, type, beneficialownershiporcontrol,
                       details, startdate
                FROM read_parquet(?)
                WHERE _link_relationship_statement = ?
                """,
                [str(rel_interests_p), rel_link],
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
                if interest:
                    interests.append(interest)

        # Subject / interested party references
        subject_ref = rel_row[4] or rel_row[2] or ""
        ip_entity_ref = rel_row[5] or rel_row[3] or ""
        ip_person_ref = rel_row[6] or ""

        interested_party: dict[str, str] = {}
        if ip_entity_ref:
            interested_party["describedByEntityStatement"] = ip_entity_ref
        elif ip_person_ref:
            interested_party["describedByPersonStatement"] = ip_person_ref

        stmts.append({
            "statementId": rel_row[0] or "",
            "recordId": rel_row[0] or "",
            "statementType": "relationshipStatement",
            "recordDetails": {
                "isComponent": False,
                "subject": {"describedByEntityStatement": subject_ref},
                "interestedParty": interested_party,
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
    parquet_dir: Path,
    row: dict[str, Any],
    link: str,
) -> dict[str, Any]:
    statementid = row.get("statementid") or ""
    pub_date = row.get("publicationdetails_publicationdate") or ""

    # Names sub-table
    names_p = parquet_dir / "person_recordDetails_names.parquet"
    full_name = row.get("recorddetails_names_fullname") or ""
    names: list[dict[str, Any]] = []
    if names_p.exists() and link:
        name_rows = duck.execute(
            "SELECT fullname, type FROM read_parquet(?) WHERE _link_person_statement = ?",
            [str(names_p), link],
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

    # Nationalities sub-table
    nats_p = parquet_dir / "person_recordDetails_nationalities.parquet"
    nationalities: list[dict[str, Any]] = []
    if nats_p.exists() and link:
        nat_rows = duck.execute(
            "SELECT name, code FROM read_parquet(?) WHERE _link_person_statement = ?",
            [str(nats_p), link],
        ).fetchall()
        for nr in nat_rows:
            n = {k: v for k, v in [("name", nr[0]), ("code", nr[1])] if v}
            if n:
                nationalities.append(n)

    record_details: dict[str, Any] = {
        "isComponent": False,
        "personType": row.get("recorddetails_persontype") or "knownPerson",
        "names": names or [{"fullName": full_name, "type": "individual"}],
    }

    if nationalities:
        record_details["nationalities"] = nationalities

    dob_year = row.get("recorddetails_birthdate_year") or ""
    dob_month = row.get("recorddetails_birthdate_month") or ""
    if dob_year:
        dob = str(dob_year)
        if dob_month:
            dob += f"-{str(dob_month).zfill(2)}"
        record_details["birthDate"] = dob

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
