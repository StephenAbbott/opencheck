"""BrightQuery / OpenData.org adapter.

BrightQuery publishes open US company and executive data on opendata.org
(https://opendata.org/) in Senzing JSON format.  The COMPANY dataset
contains 185,000+ records that carry an LEI, which makes it directly
addressable from OpenCheck's LEI-first lookup flow.

The bulk data is extracted once by ``scripts/extract_brightquery.py`` into
a SQLite database indexed by LEI.  This adapter reads from that database.

Entry point
-----------
``fetch(lei)`` — accepts an ISO 17442 LEI and returns a bundle containing:

* ``company``  — the raw Senzing COMPANY record (dict with ``FEATURES``).
* ``people``   — list of associated PEOPLE_BUSINESS records.

Authentication: none (open bulk data).
Activation: set ``BRIGHTQUERY_DB_FILE=/path/to/brightquery.db`` in .env.
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


class BrightQueryAdapter(SourceAdapter):
    id = "brightquery"

    def __init__(self) -> None:
        self._db: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        db_path = settings.brightquery_db_file
        live = bool(db_path and Path(db_path).exists())
        return SourceInfo(
            id=self.id,
            name="BrightQuery (OpenData.org)",
            homepage="https://opendata.org/",
            description=(
                "Open US company and executive data from BrightQuery, "
                "published on opendata.org. Covers 185,000+ US entities "
                "that hold a Legal Entity Identifier."
            ),
            license="ODC-By",
            attribution=(
                "Contains data from BrightQuery / OpenData.org, licensed "
                "under the Open Data Commons Attribution License (ODC-By)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # DB connection (lazy, cached per-instance)
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection | None:
        settings = get_settings()
        db_path = settings.brightquery_db_file
        if not db_path:
            return None
        if self._db is None:
            path = Path(db_path)
            if not path.exists():
                logger.warning("BrightQuery DB not found at %s", path)
                return None
            self._db = sqlite3.connect(str(path), check_same_thread=False)
            self._db.row_factory = sqlite3.Row
            logger.info("BrightQuery DB opened: %s", path)
        return self._db

    # ------------------------------------------------------------------
    # Search — not used (entered via LEI from app.py)
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """BrightQuery is entered via LEI; free-text search is not supported."""
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch company and people records for a given LEI.

        ``hit_id`` is an ISO 17442 LEI (20 alphanumeric characters), as
        used throughout OpenCheck's LEI-first lookup flow.
        """
        lei = hit_id.strip().upper()
        db = self._conn()
        if db is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        row = db.execute(
            "SELECT bq_id, name, raw_json FROM companies WHERE lei = ?", (lei,)
        ).fetchone()
        if not row:
            # LEI not in the BrightQuery dataset.
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        bq_id: str = row["bq_id"]
        name: str = row["name"] or ""
        company: dict = json.loads(row["raw_json"])

        people_rows = db.execute(
            "SELECT raw_json FROM people WHERE org_bq_id = ?", (bq_id,)
        ).fetchall()
        people: list[dict] = [json.loads(r["raw_json"]) for r in people_rows]

        return {
            "source_id": self.id,
            "hit_id": hit_id,
            "is_stub": False,
            "lei": lei,
            "bq_id": bq_id,
            "name": name,
            "company": company,
            "people": people,
        }
