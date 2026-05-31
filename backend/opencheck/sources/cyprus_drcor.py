"""Cyprus DRCOR adapter — Department of Registrar of Companies and
Intellectual Property (data.gov.cy open data).

DRCOR publishes the *Register of Registered Companies, Commercial Names and
Cooperatives in Cyprus* as a High-Value Dataset on the national open-data
portal data.gov.cy.  The dataset is three monthly CSV distributions:

  * organisations        — one row per registered organisation (company etc.)
  * registered office    — registered address per organisation
  * officials            — directors / secretaries / other role-holders

The dataset carries company + role-holder data but **no shareholders**, so
this adapter produces entity + officer statements but no ownership interests.

Why local SQLite (not a live API)
---------------------------------
data.gov.cy is a Drupal/EKAN portal that exposes **no working datastore query
API** for these resources — ``/api/1/datastore/query`` returns HTTP 404 and
the large CSVs (the officials file is ~126 MB) are not imported into a
queryable datastore.  Only bulk CSV download is available.  This adapter
therefore follows the same pattern as the ACRA (Singapore) and BCE (Belgium)
adapters: a pre-built SQLite database, queried locally.

Flow with GLEIF
---------------
1. GLEIF returns ``registeredAt.id == "RA000161"`` (DRCOR's RA code) and
   ``registeredAs == "ΗΕ 489243"`` (the HE number, in Greek script) for
   Cypriot entities.
2. ``routers/lookup.py`` extracts ``derived["cy_he"]`` via ``normalise_he_number``
   and calls ``fetch()`` here.
3. We look up the registration number in the three local tables and map the
   result to BODS in ``map_cyprus_drcor``.

Activation: set ``CYPRUS_DRCOR_DB_FILE=/path/to/cyprus.db`` in .env.
Build the DB with::

    python scripts/extract_cyprus.py \\
      --organisations-csv organisations_95.csv \\
      --office-csv registered_office_98.csv \\
      --officials-csv organisation_officials_84.csv \\
      --output cyprus.db

The three CSVs are downloadable (CC BY 4.0) from the dataset page:
  https://data.gov.cy/el/dataset/mitroo-eggegrammenon-etaireion-emporikon-eponymion-kai-synetairismon-stin-kypro

GLEIF RA code: RA000161

License: Creative Commons Attribution 4.0 (CC BY 4.0).
  https://creativecommons.org/licenses/by/4.0/
Attribution: Contains information from the Department of Registrar of Companies
  and Intellectual Property (Republic of Cyprus), published on data.gov.cy
  under CC BY 4.0.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.cyprus_drcor import CyprusBundle

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Cyprus Companies Section (DRCOR).
CY_DRCOR_RA_CODE: str = "RA000161"

# Candidate machine column names (case-insensitive) used by the mapper to read
# fields out of the stored CSV rows.  ``extract_cyprus.py`` preserves the
# original CSV headers (lower-cased), so these candidates absorb the common
# spelling variants.  Confirm against the CSV headers when building the DB.
_COLS: dict[str, tuple[str, ...]] = {
    "reg_no": ("registration_no", "registration_number", "reg_no", "regno"),
    "org_name": ("organisation_name", "name", "org_name", "organization_name"),
    "org_type": ("organisation_type", "organization_type", "org_type"),
    "org_type_code": ("organisation_type_code", "org_type_code", "type_code"),
    "org_status": ("organisation_status", "status", "org_status"),
    "reg_date": ("registration_date", "reg_date"),
    "street": ("street", "address_street"),
    "building": ("building", "address_building"),
    "territory": ("territory", "city", "area"),
    "official_name": ("person_or_organisation_name", "name", "official_name"),
    "official_position": ("official_position", "position", "official_type"),
}

# Public DRCOR search UI + dataset page.
_SEARCH_URL = "https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchForm.aspx?lang=EN"
_DATASET_URL = (
    "https://data.gov.cy/el/dataset/"
    "mitroo-eggegrammenon-etaireion-emporikon-eponymion-kai-synetairismon-stin-kypro"
)


def normalise_he_number(raw: str) -> str:
    """Return the numeric registration number from a Cyprus HE identifier.

    GLEIF stores the number in Greek script, e.g. ``"ΗΕ 489243"`` (the prefix
    is Greek capital Eta+Epsilon, not Latin "HE"). Other inputs may use Latin
    ``"HE489243"`` or a bare ``"489243"``. We strip any alphabetic prefix and
    whitespace and return the digits, which is the form the DB keys on.
    """
    return re.sub(r"\D", "", str(raw or "").strip())


def he_type_code(raw: str) -> str:
    """Best-effort organisation type code from the registration prefix.

    Maps the Greek ``ΗΕ`` (and Latin ``HE``) prefix to the Latin type code
    ``HE``.  Returns "" when no alphabetic prefix is present.
    """
    s = str(raw or "").strip()
    if s.startswith("ΗΕ") or s.upper().startswith("HE"):
        return "HE"
    m = re.match(r"\s*([A-Za-zΑ-Ωα-ω]{1,3})", s)
    return (m.group(1).upper() if m else "").translate(str.maketrans("ΗΕΒΣΑ", "HEBSA"))


def _field(row: dict[str, Any] | None, key: str) -> str:
    """Case-insensitive, candidate-tolerant string lookup over a stored row."""
    if not row:
        return ""
    lowered = {str(k).lower(): v for k, v in row.items()}
    for cand in _COLS.get(key, (key,)):
        val = lowered.get(cand.lower())
        if val not in (None, ""):
            return str(val).strip()
    return ""


class CyprusDrcorAdapter(SourceAdapter):
    """Source adapter for the Cyprus DRCOR open dataset (local SQLite)."""

    id = "cyprus_drcor"

    def __init__(self) -> None:
        self._db: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        db_path = getattr(settings, "cyprus_drcor_db_file", None)
        live = bool(db_path and Path(db_path).exists())
        return SourceInfo(
            id=self.id,
            name="Cyprus DRCOR — Registrar of Companies (data.gov.cy)",
            homepage="https://www.companies.gov.cy/en/",
            description=(
                "Cyprus company and role-holder data from the Department of "
                "Registrar of Companies and Intellectual Property, published "
                "as open data on data.gov.cy. Includes organisations, "
                "registered office and officials (directors/secretaries); no "
                "shareholder/ownership data."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains information from the Department of Registrar of "
                "Companies and Intellectual Property (Republic of Cyprus), "
                "published on data.gov.cy under CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
            is_national_register=True,
        )

    # ------------------------------------------------------------------
    # SQLite connection (lazy, cached per-instance; injectable for tests)
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection | None:
        # Return an injected/cached connection immediately (tests set _db).
        if self._db is not None:
            return self._db
        settings = get_settings()
        db_path = getattr(settings, "cyprus_drcor_db_file", None)
        if not db_path:
            return None
        path = Path(db_path)
        if not path.exists():
            logger.warning("cyprus_drcor: DB file not found at %s", db_path)
            return None
        conn = sqlite3.connect(str(path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self._db = conn
        return self._db

    @staticmethod
    def _loads(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        try:
            return json.loads(value) if value else {}
        except (TypeError, ValueError):
            return {}

    def _query_one(self, table: str, reg_no_norm: str) -> dict[str, Any] | None:
        conn = self._conn()
        if conn is None:
            return None
        try:
            cur = conn.execute(
                f"SELECT data FROM {table} WHERE reg_no_norm = ? LIMIT 1",
                (reg_no_norm,),
            )
        except sqlite3.OperationalError as exc:
            logger.warning("cyprus_drcor: query on %s failed: %s", table, exc)
            return None
        row = cur.fetchone()
        return self._loads(row["data"]) if row is not None else None

    def _query_many(self, table: str, reg_no_norm: str, limit: int = 100) -> list[dict[str, Any]]:
        conn = self._conn()
        if conn is None:
            return []
        try:
            cur = conn.execute(
                f"SELECT data FROM {table} WHERE reg_no_norm = ? LIMIT ?",
                (reg_no_norm, limit),
            )
        except sqlite3.OperationalError as exc:
            logger.warning("cyprus_drcor: query on %s failed: %s", table, exc)
            return []
        return [self._loads(r["data"]) for r in cur.fetchall()]

    def _search_by_name(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        conn = self._conn()
        if conn is None:
            return []
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='organisations_fts'"
        )
        if cur.fetchone() is None:
            try:
                cur2 = conn.execute(
                    "SELECT data FROM organisations WHERE name LIKE ? LIMIT ?",
                    (f"%{query}%", limit),
                )
                return [self._loads(r["data"]) for r in cur2.fetchall()]
            except sqlite3.OperationalError:
                return []
        try:
            cur3 = conn.execute(
                """
                SELECT o.data AS data
                FROM organisations_fts f
                JOIN organisations o ON o.reg_no_norm = f.reg_no_norm
                WHERE organisations_fts MATCH ?
                LIMIT ?
                """,
                (f'"{query}"', limit),
            )
            return [self._loads(r["data"]) for r in cur3.fetchall()]
        except sqlite3.OperationalError as exc:
            logger.warning("cyprus_drcor FTS search failed: %s", exc)
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
        for org in rows:
            reg = normalise_he_number(_field(org, "reg_no"))
            name = _field(org, "org_name") or reg
            type_code = _field(org, "org_type_code") or "HE"
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=reg,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"CY-{type_code} {reg}".strip(),
                    identifiers={"cy_he": reg},
                    raw=org,
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _stub(self, reg_no: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "reg_no": reg_no,
            "name": legal_name or "",
            "organisation": None,
            "address": None,
            "officials": [],
            "legal_name": legal_name,
            "link": _SEARCH_URL,
            "is_stub": True,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the Cyprus DRCOR entity + officials bundle for an HE number.

        ``hit_id`` is the registration number (Greek/Latin prefix or bare
        digits — it is normalised). ``legal_name`` is a GLEIF fallback used
        for the display name and stub.
        """
        reg_no = normalise_he_number(hit_id)
        if not reg_no:
            return self._stub(reg_no, legal_name)

        organisation = self._query_one("organisations", reg_no)
        if organisation is None:
            return self._stub(reg_no, legal_name)

        address = self._query_one("registered_office", reg_no)
        officials = self._query_many("officials", reg_no)

        name = _field(organisation, "org_name") or legal_name or f"CY {reg_no}"
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "reg_no": reg_no,
            "name": name,
            "organisation": organisation,
            "address": address,
            "officials": officials,
            "legal_name": legal_name,
            "link": _DATASET_URL,
            "is_stub": False,
        }
        validate_raw("cyprus_drcor", CyprusBundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # Stub
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub Cyprus DRCOR record — build cyprus.db with "
                    "scripts/extract_cyprus.py to enable live data."
                ),
                identifiers={"cy_he": "000000"},
                raw={"reg_no": "000000"},
                is_stub=True,
            )
        ]
