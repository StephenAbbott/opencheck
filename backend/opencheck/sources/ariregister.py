"""Estonian e-Business Register (e-Äriregister) adapter.

The Centre of Registers and Information Systems (RIK) publishes daily open
data exports for Estonia's e-Business Register under a Creative Commons
Attribution 4.0 International licence (CC BY 4.0).

This adapter queries a pre-built SQLite database that is populated by
``scripts/extract_ariregister.py`` from the four open data files published at:
  https://avaandmed.ariregister.rik.ee/en/downloading-open-data

Data files consumed (all updated daily, all CC BY 4.0):
  ettevotja_rekvisiidid__lihtandmed.csv       — entity basics
  ettevotja_rekvisiidid__osanikud.json        — shareholders (osanikud)
  ettevotja_rekvisiidid__kaardile_kantud_isikud.json — officers / persons on card
  ettevotja_rekvisiidid__kasusaajad.json      — beneficial owners

Note: Beneficial ownership data is included here because it is currently
published as open data. Estonian law is expected to restrict public access
to beneficial ownership records in the near future (in line with AMLD6
implementation). When that happens, ``include_beneficial_owners`` should be
set to False and the ``kasusaajad`` columns in the database should be
considered frozen/stale. The adapter is structured so that removing BO data
requires only changing the flag and dropping those columns from extraction.

The flow with GLEIF:
  1. GLEIF returns ``registeredAt.id == "RA000181"`` (Estonian e-Business
     Register RA code) and ``registeredAs = "<registry_code>"`` for Estonian
     entities.  Registry codes are numeric strings of 8 digits (e.g.
     "14064835" for Bolt Technology OÜ).
  2. app.py extracts ``derived["ee_registry_code"]`` and calls ``fetch()``
     here.
  3. We query the SQLite index and return shareholders, officers, and (for
     now) beneficial owners alongside entity basics.

Authentication: none — open bulk data, no API key required.
Activation: set ``ARIREGISTER_DB_FILE=/path/to/ariregister.db`` in .env.

GLEIF RA code: RA000181

License: CC BY 4.0
  https://creativecommons.org/licenses/by/4.0/
Attribution:
  Data from the Estonian e-Business Register (e-Äriregister), published by
  the Centre of Registers and Information Systems (RIK), CC BY 4.0.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Estonian e-Business Register.
EE_RA_CODE: str = "RA000181"

# Register URL template for a company page.
_COMPANY_URL = "https://ariregister.rik.ee/eng/company/{registry_code}"


class AriregisterAdapter(SourceAdapter):
    """Source adapter for the Estonian e-Business Register (e-Äriregister)."""

    id = "ariregister"

    def __init__(self) -> None:
        self._db: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        db_path = settings.ariregister_db_file
        live = bool(db_path and Path(db_path).exists())
        return SourceInfo(
            id=self.id,
            name="Estonian e-Business Register (e-Äriregister)",
            homepage="https://avaandmed.ariregister.rik.ee/en",
            description=(
                "Estonian company data including entity details, shareholders "
                "(with ownership percentages), board members, and beneficial "
                "owners, from the e-Business Register open data (RIK)."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Data from the Estonian e-Business Register (e-Äriregister), "
                "published by the Centre of Registers and Information Systems "
                "(RIK), CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # SQLite connection (lazy, cached per-instance)
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection | None:
        settings = get_settings()
        db_path = settings.ariregister_db_file
        if not db_path:
            return None
        if self._db is None:
            path = Path(db_path)
            if not path.exists():
                logger.warning("ariregister: DB file not found at %s", db_path)
                return None
            conn = sqlite3.connect(str(path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._db = conn
        return self._db

    def _query(self, registry_code: str) -> dict[str, Any] | None:
        """Return all data for a given registry code from the SQLite DB."""
        conn = self._conn()
        if conn is None:
            return None
        cur = conn.execute(
            "SELECT * FROM entities WHERE registry_code = ?",
            (registry_code,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        result: dict[str, Any] = dict(row)
        # Deserialise JSON columns
        for col in ("shareholders", "officers", "beneficial_owners"):
            raw = result.get(col)
            if raw:
                try:
                    result[col] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    result[col] = []
            else:
                result[col] = []
        return result

    # ------------------------------------------------------------------
    # Search — identifier-keyed via GLEIF, not name-based.
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name-based search is not supported — returns an empty list.

        Estonian entities are reached via their registry code derived from
        the GLEIF ``registeredAs`` field, not via free-text search.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(
        self,
        hit_id: str,
        *,
        legal_name: str = "",
        include_beneficial_owners: bool = True,
    ) -> dict[str, Any]:
        """Return the e-Äriregister data for an Estonian registry code.

        ``hit_id`` is the 8-digit Estonian registry code (e.g. "14064835").
        ``legal_name`` is used as a fallback display name when the local DB
        has no record for this code.

        Set ``include_beneficial_owners=False`` when the Estonian BO
        publication rules change and BO data should no longer be surfaced.
        """
        registry_code = hit_id.strip().lstrip("0") or hit_id.strip()
        # Re-zero-pad to canonical 8-digit form used in the DB.
        if registry_code.isdigit():
            registry_code = registry_code.zfill(8)

        stub_bundle: dict[str, Any] = {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": legal_name,
            "legal_form": None,
            "vat_number": None,
            "status": None,
            "registration_date": None,
            "address": None,
            "link": _COMPANY_URL.format(registry_code=registry_code),
            "shareholders": [],
            "officers": [],
            "beneficial_owners": [],
            "is_stub": True,
        }

        row = self._query(registry_code)
        if row is None:
            return stub_bundle

        bundle = {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": row.get("name") or legal_name,
            "legal_form": row.get("legal_form"),
            "vat_number": row.get("vat_number"),
            "status": row.get("status"),
            "registration_date": row.get("registration_date"),
            "address": row.get("address"),
            "link": row.get("link") or _COMPANY_URL.format(registry_code=registry_code),
            "shareholders": row.get("shareholders") or [],
            "officers": row.get("officers") or [],
            "beneficial_owners": (
                row.get("beneficial_owners") or []
                if include_beneficial_owners
                else []
            ),
            "is_stub": False,
        }
        return bundle
