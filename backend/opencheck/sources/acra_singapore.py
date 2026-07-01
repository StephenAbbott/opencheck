"""Singapore Accounting and Corporate Regulatory Authority (ACRA) adapter.

ACRA is Singapore's national company registry, responsible for registering
businesses and public accountants. It also maintains the Unique Entity Number
(UEN) system used across all government agencies in Singapore.

Open data is published via data.gov.sg as monthly CSV files covering all
entities registered with ACRA and with other UEN-issuing agencies:

  Collection:  https://data.gov.sg/datasets?query=acra&resultId=1
  Dataset A:   d_3f960c10fed6145404ca7b821f263b87  (ACRA entities, ~230 MB)
  Dataset B:   d_b1d2b840ab9e993570c037b706b39bb8  (other UEN agencies, ~3 MB)

Both CSV files share the same schema:
  uen                — Unique Entity Number (primary key)
  issuance_agency_desc — "ACRA", "Registry of Societies", etc.
  uen_status_desc    — "Live", "Struck Off", "Cancelled", etc.
  entity_name        — Registered name of the entity
  entity_type_desc   — "PRIVATE COMPANY LIMITED BY SHARES", "SOLE-PROPRIETOR", …
  uen_issue_date     — YYYY-MM-DD
  reg_street_name    — Registered street address (optional)
  reg_postal_code    — Registered postal code (optional)

This adapter queries a pre-built SQLite database populated by
``scripts/extract_acra.py`` from the two CSV files.

GLEIF bridge:
  GLEIF records for Singapore entities frequently omit the ``registeredAt``
  (RA code) and ``registeredAs`` (UEN) fields.  This adapter is therefore
  activated by the GLEIF jurisdiction code ``"SG"`` rather than by an RA
  code.  The GLEIF legal name is used to perform an FTS5 name search in the
  local SQLite DB; the first exact-or-close match is returned.

GLEIF RA code: RA000523 (ACRA — Business Registry)

Activation: set ``ACRA_SINGAPORE_DB_FILE=/path/to/acra.db`` in .env.
Build the DB with:
  python scripts/extract_acra.py \\
    --acra-csv entities_with_acra.csv \\
    --other-csv entities_with_other_agencies.csv \\
    --output acra.db

License: Singapore Open Data Licence 1.0
  https://data.gov.sg/open-data-licence

Attribution:
  Data from the Accounting and Corporate Regulatory Authority (ACRA),
  Government of Singapore, published on data.gov.sg under the
  Singapore Open Data Licence 1.0.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.acra_singapore import ACRABundle

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for ACRA (Business Registry, Singapore).
ACRA_RA_CODE: str = "RA000523"

# Public BizFile+ search URL.
_SEARCH_URL = "https://www.bizfile.gov.sg/ngbbizfileinternet/faces/oracle/webcenter/portalapp/pages/TransactionMain.jspx"
# Direct entity lookup URL (UEN required).
_ENTITY_URL = "https://www.bizfile.gov.sg/ngbbizfileinternet/faces/oracle/webcenter/portalapp/pages/TransactionMain.jspx?selectedEnittyType=UEN&uen={uen}"


def normalise_uen(raw: str) -> str:
    """Strip whitespace and upper-case a UEN string.

    Singapore UENs take one of three forms:
      * 9-digit numeric              e.g. ``198401234W`` (businesses)
      * 10-char alphanumeric         e.g. ``200312345E`` (most companies)
      * 10-char T/S/R-prefixed       e.g. ``T08LL1234A`` (LLPs)

    We simply strip and upper-case; the format is not normalized further.
    """
    return re.sub(r"\s+", "", (raw or "").strip()).upper()


class AcraSingaporeAdapter(SourceAdapter):
    """Source adapter for the Singapore ACRA UEN registry (open data)."""

    id = "acra_singapore"

    def __init__(self) -> None:
        self._db: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        db_path = settings.acra_singapore_db_file
        live = bool(db_path and Path(db_path).exists())
        return SourceInfo(
            id=self.id,
            name="Singapore ACRA Business Registry",
            homepage="https://data.gov.sg/datasets?query=acra&resultId=1",
            description=(
                "Singapore company data including UEN, entity name, status, "
                "entity type, registration date, and registered address, from "
                "ACRA's open data publication on data.gov.sg."
            ),
            license="Singapore-OGL-1.0",
            attribution=(
                "Data from the Accounting and Corporate Regulatory Authority "
                "(ACRA), Government of Singapore, published on data.gov.sg "
                "under the Singapore Open Data Licence 1.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
            is_national_register=True,
            country="SG",
        )

    # ------------------------------------------------------------------
    # SQLite connection (lazy, cached per-instance)
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection | None:
        # Return cached connection immediately (allows tests to inject a DB).
        if self._db is not None:
            return self._db
        settings = get_settings()
        db_path = settings.acra_singapore_db_file
        if not db_path:
            return None
        if self._db is None:
            path = Path(db_path)
            if not path.exists():
                logger.warning("acra_singapore: DB file not found at %s", db_path)
                return None
            conn = sqlite3.connect(str(path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._db = conn
        return self._db

    def _query_by_uen(self, uen: str) -> dict[str, Any] | None:
        """Return the entity row for a given UEN."""
        conn = self._conn()
        if conn is None:
            return None
        cur = conn.execute("SELECT * FROM entities WHERE uen = ?", (uen,))
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def _search_by_name(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        """FTS5 name search.  Returns up to ``limit`` entity dicts."""
        conn = self._conn()
        if conn is None:
            return []

        # Check whether the FTS table exists.
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='entities_fts'"
        )
        if cur.fetchone() is None:
            # Fallback to LIKE search (slower on large tables).
            cur2 = conn.execute(
                "SELECT * FROM entities WHERE entity_name LIKE ? LIMIT ?",
                (f"%{query}%", limit),
            )
            return [dict(r) for r in cur2.fetchall()]

        try:
            # Attempt an exact FTS phrase match first for precision.
            cur3 = conn.execute(
                """
                SELECT e.*
                FROM entities_fts f
                JOIN entities e ON e.uen = f.uen
                WHERE entities_fts MATCH ?
                LIMIT ?
                """,
                (f'"{query}"', limit),
            )
            rows = [dict(r) for r in cur3.fetchall()]
            if rows:
                return rows
            # Fall back to non-phrase FTS match.
            cur4 = conn.execute(
                """
                SELECT e.*
                FROM entities_fts f
                JOIN entities e ON e.uen = f.uen
                WHERE entities_fts MATCH ?
                LIMIT ?
                """,
                (query, limit),
            )
            return [dict(r) for r in cur4.fetchall()]
        except sqlite3.OperationalError as exc:
            logger.warning("acra_singapore FTS search failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        conn = self._conn()
        if conn is None:
            return self._stub_search(query)
        rows = self._search_by_name(query)
        if not rows:
            return []
        hits: list[SourceHit] = []
        for row in rows:
            uen = row.get("uen") or ""
            name = row.get("entity_name") or uen
            status = row.get("uen_status_desc") or ""
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=uen,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"SG-UEN {uen} · {status}".strip(" ·"),
                    identifiers={"sg_uen": uen},
                    raw=row,
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(
        self,
        hit_id: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any]:
        """Return ACRA data for a Singapore entity.

        ``hit_id`` may be:
        - A UEN string (used for direct UEN-based lookup when available), or
        - A company name from GLEIF (used for name-based activation when the
          UEN is not known from the GLEIF record).

        The adapter tries a direct UEN lookup first.  If that fails (or the
        input doesn't look like a UEN), it falls back to an FTS5 name search
        using ``hit_id`` (and optionally ``legal_name``) as the query.

        ``legal_name`` is used as fallback display name when no DB record is
        found, and as an additional search term when ``hit_id`` is a name.
        """
        name_query = hit_id.strip()
        uen_candidate = normalise_uen(hit_id)

        stub_bundle: dict[str, Any] = {
            "source_id": self.id,
            "uen": "",
            "entity_name": legal_name or name_query,
            "issuance_agency_desc": None,
            "uen_status_desc": None,
            "entity_type_desc": None,
            "uen_issue_date": None,
            "reg_street_name": None,
            "reg_postal_code": None,
            "link": _SEARCH_URL,
            "is_stub": True,
        }

        conn = self._conn()
        if conn is None:
            return stub_bundle

        row: dict[str, Any] | None = None

        # Try direct UEN lookup first (works when hit_id is already a UEN).
        if uen_candidate:
            row = self._query_by_uen(uen_candidate)

        # Fall back to name search.
        if row is None:
            search_term = name_query or legal_name
            if search_term:
                results = self._search_by_name(search_term, limit=1)
                if results:
                    row = results[0]

        if row is None:
            return stub_bundle

        uen = row.get("uen") or ""
        name = row.get("entity_name") or legal_name or ""
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "uen": uen,
            "entity_name": name,
            "issuance_agency_desc": row.get("issuance_agency_desc"),
            "uen_status_desc": row.get("uen_status_desc"),
            "entity_type_desc": row.get("entity_type_desc"),
            "uen_issue_date": row.get("uen_issue_date"),
            "reg_street_name": row.get("reg_street_name"),
            "reg_postal_code": row.get("reg_postal_code"),
            "link": (
                _ENTITY_URL.format(uen=uen) if uen else _SEARCH_URL
            ),
            "is_stub": False,
        }
        validate_raw("acra_singapore", ACRABundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # Stub helpers
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="000000000A",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary="Stub ACRA record — build acra.db to enable live search.",
                identifiers={"sg_uen": "000000000A"},
                raw={"uen": "000000000A"},
                is_stub=True,
            )
        ]
