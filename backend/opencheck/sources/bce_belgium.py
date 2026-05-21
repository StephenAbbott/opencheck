"""Belgian Crossroads Bank for Enterprises (BCE / KBO) adapter.

The Banque-Carrefour des Entreprises (BCE, Dutch: Kruispuntbank van
Ondernemingen / KBO) is Belgium's central business register, administered
by the FPS Economy (FOD Economie / SPF Économie).

Open data is published as a monthly ZIP of CSV files at:
  https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/

The ZIP (~300 MB compressed) contains:
  enterprise.csv      — entity number, status, juridical form, start date
  denomination.csv    — names (official, commercial) in NL / FR / DE
  address.csv         — registered-office and other addresses

Note: Belgian UBO register data is NOT included here.  The UBO register
(UBO-register / Registre UBO) requires legitimate-interest access and is
not openly available.  Belgium is also in the process of legislating that
public access will be restricted to users with a demonstrable need, in
line with AMLD6 implementation.

This adapter queries a pre-built SQLite database that is populated by
``scripts/extract_bce.py`` from the three CSV files above.

GLEIF bridge:
  GLEIF records for Belgian entities carry:
    ``registeredAt.id  == "RA000025"``  (BCE/KBO RA code)
    ``registeredAs     == "NNNN.NNN.NNN"`` (10-digit dotted enterprise number)
  We normalise to 10 raw digits, stored as ``be_enterprise_number`` in the
  derived identifiers dict in app.py.

Activation: set ``BCE_BELGIUM_DB_FILE=/path/to/bce.db`` in .env.
Build the DB with: ``python scripts/extract_bce.py --zip-file kbo_open_data.zip --output bce.db``

GLEIF RA code: RA000025

License: Reuse of KBO public data (Réutilisation données publiques KBO).
  https://kbopub.economie.fgov.be/kbo-open-data/static/doc/Licentie/Licentie.pdf
Attribution:
  Data from the Belgian Crossroads Bank for Enterprises (BCE/KBO), made
  available by the FPS Economy, SMEs, Self-Employed and Energy, Belgium.
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
from .schemas.bce_belgium import BCEBundle

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for the BCE/KBO.
BCE_RA_CODE: str = "RA000025"

# Company portal URL template.
_COMPANY_URL = "https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html?searchWord={enterprise_number}&_target=0&lang=en"
# Direct link to a specific enterprise.
_ENTITY_URL = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={enterprise_number}"


def normalise_enterprise_number(raw: str) -> str:
    """Normalise a Belgian enterprise number to a 10-digit string (no dots).

    Accepts both dotted form ``0433.795.975`` and plain ``0433795975``.
    Returns empty string if the input contains no digits.
    """
    digits = re.sub(r"[^0-9]", "", (raw or "").strip())
    if not digits:
        return ""
    return digits.zfill(10)


def format_enterprise_number(raw: str) -> str:
    """Format a 10-digit enterprise number in the canonical dotted form.

    ``0433795975`` → ``0433.795.975``
    """
    digits = normalise_enterprise_number(raw)
    if len(digits) == 10:
        return f"{digits[:4]}.{digits[4:7]}.{digits[7:]}"
    return digits


class BceBelgiumAdapter(SourceAdapter):
    """Source adapter for the Belgian Crossroads Bank for Enterprises (BCE/KBO)."""

    id = "bce_belgium"

    def __init__(self) -> None:
        self._db: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        db_path = settings.bce_belgium_db_file
        live = bool(db_path and Path(db_path).exists())
        return SourceInfo(
            id=self.id,
            name="Belgian Crossroads Bank for Enterprises (BCE/KBO)",
            homepage="https://kbopub.economie.fgov.be/kbo-open-data/",
            description=(
                "Belgian company data including entity name, status, juridical "
                "form, start date, and registered address, from the BCE/KBO "
                "open data publication by FPS Economy."
            ),
            license="Custom-KBO-Reuse",
            attribution=(
                "Data from the Belgian Crossroads Bank for Enterprises (BCE/KBO), "
                "made available by the FPS Economy, SMEs, Self-Employed and "
                "Energy, Belgium."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
            is_national_register=True,
        )

    # ------------------------------------------------------------------
    # SQLite connection (lazy, cached per-instance)
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection | None:
        settings = get_settings()
        db_path = settings.bce_belgium_db_file
        if not db_path:
            return None
        if self._db is None:
            path = Path(db_path)
            if not path.exists():
                logger.warning("bce_belgium: DB file not found at %s", db_path)
                return None
            conn = sqlite3.connect(str(path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._db = conn
        return self._db

    def _query_by_number(self, enterprise_number: str) -> dict[str, Any] | None:
        """Return the entity row for a given enterprise number (no dots)."""
        conn = self._conn()
        if conn is None:
            return None
        cur = conn.execute(
            "SELECT * FROM entities WHERE enterprise_number = ?",
            (enterprise_number,),
        )
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def _search_by_name(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """FTS5 name search. Returns up to ``limit`` entity dicts."""
        conn = self._conn()
        if conn is None:
            return []
        # Check whether the FTS table exists.
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='entities_fts'"
        )
        if cur.fetchone() is None:
            # FTS not built — fall back to LIKE search (slow on large DBs).
            cur2 = conn.execute(
                """
                SELECT * FROM entities
                WHERE name_nl LIKE ? OR name_fr LIKE ? OR name_de LIKE ?
                LIMIT ?
                """,
                (f"%{query}%", f"%{query}%", f"%{query}%", limit),
            )
            return [dict(r) for r in cur2.fetchall()]
        try:
            cur3 = conn.execute(
                """
                SELECT e.*
                FROM entities_fts f
                JOIN entities e ON e.enterprise_number = f.enterprise_number
                WHERE entities_fts MATCH ?
                LIMIT ?
                """,
                (query, limit),
            )
            return [dict(r) for r in cur3.fetchall()]
        except sqlite3.OperationalError as exc:
            logger.warning("bce_belgium FTS search failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name-based search via FTS5 when the DB is loaded; stub otherwise."""
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
            num = row.get("enterprise_number") or ""
            name = row.get("name_nl") or row.get("name_fr") or row.get("name_de") or num
            dotted = format_enterprise_number(num)
            status = row.get("status") or ""
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=num,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"BE {dotted} · {status}".strip(" ·"),
                    identifiers={"be_enterprise_number": num},
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
        """Return BCE/KBO data for a Belgian enterprise number.

        ``hit_id`` is the 10-digit enterprise number (no dots, e.g. "0433795975").
        ``legal_name`` is used as fallback display name when the DB has no record.
        """
        enterprise_number = normalise_enterprise_number(hit_id)
        dotted = format_enterprise_number(enterprise_number) if enterprise_number else hit_id

        stub_bundle: dict[str, Any] = {
            "source_id": self.id,
            "enterprise_number": enterprise_number,
            "dotted": dotted,
            "name": legal_name,
            "name_nl": "",
            "name_fr": "",
            "name_de": "",
            "status": None,
            "juridical_form": None,
            "start_date": None,
            "address": None,
            "link": _ENTITY_URL.format(enterprise_number=enterprise_number),
            "is_stub": True,
        }

        if not enterprise_number:
            return stub_bundle

        row = self._query_by_number(enterprise_number)
        if row is None:
            return stub_bundle

        name = (
            row.get("name_nl")
            or row.get("name_fr")
            or row.get("name_de")
            or legal_name
            or ""
        )

        bundle = {
            "source_id": self.id,
            "enterprise_number": enterprise_number,
            "dotted": dotted,
            "name": name,
            "name_nl": row.get("name_nl") or "",
            "name_fr": row.get("name_fr") or "",
            "name_de": row.get("name_de") or "",
            "status": row.get("status"),
            "juridical_form": row.get("juridical_form"),
            "start_date": row.get("start_date"),
            "address": row.get("address"),
            "link": row.get("link") or _ENTITY_URL.format(enterprise_number=enterprise_number),
            "is_stub": False,
        }
        validate_raw("bce_belgium", BCEBundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # Stub
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="0000000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary="Stub BCE/KBO record — build bce.db to enable live search.",
                identifiers={"be_enterprise_number": "0000000000"},
                raw={"enterprise_number": "0000000000"},
                is_stub=True,
            )
        ]
