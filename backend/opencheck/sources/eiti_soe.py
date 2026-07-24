"""EITI State-Owned Enterprises (SOE) Database adapter — CDD category.

This is a **separate** EITI product from the existing ``eiti`` adapter. The
``eiti`` adapter targets the main EITI API (``eiti.org/api/v2.0``) and surfaces
company-level *payments to governments*. This adapter targets the **SOE
Database** — a Datasette instance at ``soe-database.eiti.org/eiti_database``
covering the ~100 state-owned enterprises reported through the EITI, with
SOE-specific attributes that live nowhere else in OpenCheck: an explicit
state-ownership classification, commodities produced, links to audited financial
statements, stock-exchange listings and an ``opencorporates_id`` per company.

Its distinctive value is therefore a **state-ownership context signal**
(``STATE_OWNED_ENTERPRISE`` — PEP-adjacent, higher corruption/sanctions nexus)
plus SOE enrichment, rather than a second payments feed.

How the lookup works (the ClimateTRACE/GEM pattern)
---------------------------------------------------
The SOE database is UUID/name-keyed and carries **no native LEI**, while
OpenCheck is anchored end-to-end on the LEI. So identity resolution is done
**once, offline**, by ``scripts/build_eiti_soe_index.py``:

1. Pull the SOE companies (Datasette JSON API).
2. Resolve each SOE to an LEI via GLEIF — ``opencorporates_id`` → GLEIF reverse
   lookup first, name+country search as a fallback — recording the match method
   and a confidence grade.
3. Commit a gzipped, LEI-keyed artifact ``opencheck/data/eiti_soe_index.json.gz``.

At runtime this adapter loads that committed index and answers ``fetch_by_lei``
as a dict lookup — no live call on the hot path. When ``allow_live`` is on, a
matched hit can be deepened with live payment/context rows from the Datasette
API, exactly as the ``eiti`` adapter gates its ``/revenue`` calls.

Identifier corroboration
------------------------
The SOE database does **not** publish the LEI — OpenCheck *derives* it at build
time. Per the corroboration rule in ``CLAUDE.md`` / ``routers/lookup.py``, this
adapter therefore must **not** assert ``lei`` in ``SourceHit.identifiers``. It
may assert the identifiers the source *does* publish (``eiti_id_company`` and,
where present, ``opencorporates_id``).

No API key required. Licence: EITI content-use policy — free republication with
credit to "EITI International Secretariat, eiti.org". (Note: do **not** ingest
the OpenSanctions ``eiti_soe`` mirror instead — it adds a CC-BY-NC restriction
that blocks any paid tier; going direct to EITI keeps the licence clean.)
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from pathlib import Path
from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.eiti_soe import EitiSoeBundle

log = logging.getLogger(__name__)

# Datasette JSON API for optional live enrichment of a matched hit.
_API_BASE = "https://soe-database.eiti.org/eiti_database"
_CACHE_NS = "eiti_soe"

#: Committed, LEI-keyed index artifact (built by scripts/build_eiti_soe_index.py).
#: Overridable via env for tests / alternative snapshots.
_INDEX_PATH = Path(
    os.environ.get("EITI_SOE_INDEX_PATH", "")
    or (Path(__file__).resolve().parent.parent / "data" / "eiti_soe_index.json.gz")
)

# Lazy module-level singleton (LEI -> index record). Tests may set this directly.
_index: dict[str, dict[str, Any]] | None = None


def _get_index() -> dict[str, dict[str, Any]]:
    """Load the committed LEI-keyed SOE index (cached in a module singleton)."""
    global _index
    if _index is None:
        try:
            with gzip.open(_INDEX_PATH, "rt", encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("index") or {}
            # Normalise keys to upper-case 20-char LEIs.
            _index = {
                str(k).strip().upper(): v
                for k, v in raw.items()
                if len(str(k).strip()) == 20
            }
            meta = data.get("meta") or {}
            log.info(
                "EITI SOE index loaded: %s SOEs resolved to LEI (%s source snapshot)",
                meta.get("resolved_lei", len(_index)),
                meta.get("source_snapshot"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI SOE index unavailable: %s", exc)
            _index = {}
    return _index


def _reset_index_for_tests() -> None:
    """Test helper — drop the cached singleton so a fresh index is loaded."""
    global _index
    _index = None


class EitiSoeAdapter(SourceAdapter):
    """EITI State-Owned Enterprises Database adapter — CDD category."""

    id = "eiti_soe"

    #: LEI-keyed source. Dispatched directly in routers/lookup.py alongside the
    #: other LEI-keyed sources (opensanctions, climatetrace, bods_gleif), not via
    #: an RA-code deriver.
    lookup_timeout_s = 20.0

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="EITI State-Owned Enterprises Database",
            homepage="https://soe-database.eiti.org/",
            description=(
                "State-owned enterprises reported through the EITI across "
                "implementing countries, with a state-ownership classification, "
                "commodities, audited-financial-statement links and stock "
                "listings. Surfaces a state-ownership context signal by LEI."
            ),
            license="EITI open data (free reuse with attribution)",
            attribution="EITI International Secretariat, eiti.org",
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            category="cdd",
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed source; free-text search intentionally empty
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # LEI-based lookup (called by the lookup pipeline)
    # ------------------------------------------------------------------

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Return the SOE bundle for a LEI, or ``None`` when not an SOE.

        The match itself is offline (committed index). When live mode is on, a
        matched hit is deepened with payment/context rows from the Datasette API.
        """
        lei_norm = (lei or "").strip().upper()
        index = await asyncio.to_thread(_get_index)
        record = index.get(lei_norm)
        if record is None:
            return None
        return await self._build_bundle(lei_norm, record)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        lei_norm = (hit_id or "").strip().upper()
        index = await asyncio.to_thread(_get_index)
        record = index.get(lei_norm)
        if record is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return await self._build_bundle(lei_norm, record)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _build_bundle(
        self, lei: str, record: dict[str, Any]
    ) -> dict[str, Any]:
        soe = dict(record.get("soe") or {})
        match_method = record.get("match_method")
        match_confidence = record.get("match_confidence") or "medium"

        payments: list[dict[str, Any]] = []
        if self.info.live_available and soe.get("eiti_id_company"):
            payments = await self._fetch_payments(str(soe["eiti_id_company"]))

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "lei": lei,
            "entity_name": soe.get("company_name") or soe.get("original_company_name"),
            "is_state_owned": True,
            "country": soe.get("country") or soe.get("iso_alpha2"),
            "sector": soe.get("sector"),
            "commodities": soe.get("commodities") or [],
            "company_type": soe.get("company_type"),
            "government_entity": soe.get("government_entity"),
            "opencorporates_id": soe.get("opencorporates_id"),
            "eiti_id_company": soe.get("eiti_id_company"),
            "eiti_id_government": soe.get("eiti_id_government"),
            "audited_financial_statement": soe.get("audited_financial_statement"),
            "public_listing_or_website": soe.get("public_listing_or_website"),
            "years": soe.get("years") or [],
            "match_method": match_method,
            "match_confidence": match_confidence,
            "payments": payments,
            "is_stub": False,
        }
        validate_raw("eiti_soe", EitiSoeBundle, bundle)
        return bundle

    async def _fetch_payments(self, eiti_id_company: str) -> list[dict[str, Any]]:
        """Best-effort live payment/context rows for one SOE (Datasette API).

        Uses the documented Datasette filter form
        ``…/companies.json?eiti_id_company__exact=<id>&_shape=array&_size=max``.
        Failures are swallowed — the offline match is the source of truth.
        """
        cache_key = f"{_CACHE_NS}/companies/{eiti_id_company}"
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]
        try:
            async with build_client() as client:
                response = await client.get(
                    f"{_API_BASE}/companies.json",
                    params={
                        "eiti_id_company__exact": eiti_id_company,
                        "_shape": "array",
                        "_size": "max",
                    },
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                rows = response.json()
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI SOE payment fetch failed for %s: %s", eiti_id_company, exc)
            rows = []
        out: list[dict[str, Any]] = []
        for r in rows if isinstance(rows, list) else []:
            out.append(
                {
                    "year": r.get("year"),
                    "revenue_stream": r.get("revenue_stream_name"),
                    "revenue_value": r.get("revenue_value"),
                    "currency": r.get("reporting_currency"),
                    "project": r.get("project_name"),
                }
            )
        self._cache.put(cache_key, out)
        return out
