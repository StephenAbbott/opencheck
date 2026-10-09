"""Ireland Companies Registration Office (CRO) adapter.

The CRO is Ireland's statutory register of companies, business names, and
limited partnerships, operating under the Companies Act 2014.

Two data access tiers are used:

1. **CRO Open Data Portal** (``opendata.cro.ie``) — a CKAN-based portal,
   free and key-less, containing a daily snapshot of all company records.
   This provides: company number, name, status, type, registration date,
   address, and NACE code. No officer/director data is available here.
   Endpoint used:
     ``GET https://opendata.cro.ie/api/3/action/datastore_search``
     with the companies resource ID ``3fef41bc-b8f4-4b10-8434-ce51c29b1bba``.

2. **CRO Open Services** (``services.cro.ie/cws``) — a REST API that provides
   richer data including officer/director records. Requires an API key issued
   by the CRO. Set ``CRO_API_KEY`` to enable. When present, the company
   detail endpoint is used for fetch; the Open Data Portal is always used
   for search (it is better suited to fuzzy name lookups).

The flow with GLEIF:

  1. GLEIF returns ``registeredAt.id == "RA000402"`` (CRO RA code) and
     ``registeredAs = "<company_num>"`` (numeric string) for Irish entities.
  2. app.py extracts ``derived["ie_crn"]`` and calls ``fetch()`` here.

Authentication: none for the Open Data Portal tier.
              ``CRO_API_KEY`` in env enables the Open Services tier.
License: CC BY 4.0 (Open Data Portal).
  https://creativecommons.org/licenses/by/4.0/
Attribution: Contains data from the Companies Registration Office of Ireland,
  licensed under CC BY 4.0.
Open Data Portal: https://opendata.cro.ie/
Open Services API docs: https://services.cro.ie/
"""

from __future__ import annotations

import hashlib
import re
import time
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo

# CRO Open Data Portal — CKAN endpoint (no auth).
_CKAN_BASE = "https://opendata.cro.ie/api/3/action"
_COMPANIES_RESOURCE_ID = "3fef41bc-b8f4-4b10-8434-ce51c29b1bba"

#: ``(monotonic time read, resource last_modified)`` — see
#: ``CroAdapter._extract_modified``.
_extract_meta: tuple[float, str | None] | None = None
_EXTRACT_META_TTL_S = 86_400.0

# CRO Open Services — authenticated REST API.
_OPEN_SERVICES_BASE = "https://services.cro.ie/cws"

_CACHE_NS = "cro"

# GLEIF Registration Authority code for the Irish CRO.
IE_RA_CODE: str = "RA000402"

# Irish company registration number — 1–6 digits (historically; newer
# numbers can be longer). GLEIF stores them as plain numeric strings.
_CRN_RE = re.compile(r"^\d{1,9}$")


def normalise_crn(crn: str) -> str:
    """Strip whitespace and leading zeros from a CRO company number.

    The Open Data Portal stores company_num as an integer; GLEIF may
    carry the value with or without leading zeros. We normalise to the
    plain integer string used by the CKAN filter.
    """
    cleaned = crn.strip()
    # Remove any letters (some GLEIF entries include a prefix like "IE")
    cleaned = re.sub(r"[^0-9]", "", cleaned)
    # Strip leading zeros — the CKAN column is an integer.
    return str(int(cleaned)) if cleaned else crn.strip()


def is_valid_crn(crn: str) -> bool:
    return bool(_CRN_RE.match(normalise_crn(crn)))


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _company_url(crn: str) -> str:
    return f"https://core.cro.ie/company/{crn}"


class CroAdapter(SourceAdapter):
    """Source adapter for the Irish Companies Registration Office (CRO)."""

    id = "cro"

    lookup_derivers = (
        LookupDeriver(frozenset({IE_RA_CODE}), "ie_crn", normalise_crn),
    )
    lookup_pass_legal_name = True


    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        # The Open Data Portal is always available when live mode is on.
        # The Open Services API requires a key for richer officer data.
        live = settings.allow_live
        return SourceInfo(
            id=self.id,
            name="CRO — Companies Registration Office Ireland",
            homepage="https://cro.ie/",
            description=(
                "Irish company data from the Companies Registration Office "
                "(CRO), sourced via the CRO Open Data Portal (CC BY 4.0). "
                "Provides entity details for all registered Irish companies."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Companies Registration Office of "
                "Ireland, available under CC BY 4.0 via the CRO Open Data "
                "Portal (opendata.cro.ie)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
            is_national_register=True,
            country="IE",
        )

    # ------------------------------------------------------------------
    # Search — CKAN full-text search across the companies dataset
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        payload = await self._ckan_search(query, cache_key=cache_key)
        records = (payload.get("result") or {}).get("records") or []
        return [self._company_hit(rec) for rec in records]

    # ------------------------------------------------------------------
    # Fetch — CKAN lookup by company number
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the CRO record for an Irish company registration number.

        ``hit_id`` may be the plain numeric CRN or have leading zeros stripped
        by GLEIF. Either form is accepted.
        """
        try:
            crn = normalise_crn(hit_id)
        except (ValueError, TypeError):
            crn = hit_id.strip()

        if not crn:
            return {
                "source_id": self.id,
                "crn": crn,
                "company": None,
                "legal_name": legal_name,
                "is_stub": True,
            }

        cache_key = f"{_CACHE_NS}/company/{crn}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {
                "source_id": self.id,
                "crn": crn,
                "company": None,
                "legal_name": legal_name,
                "is_stub": True,
            }

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            company = cached[0]
        else:
            import json as _json
            filters = _json.dumps({"company_num": int(crn)})
            url = (
                f"{_CKAN_BASE}/datastore_search"
                f"?resource_id={_COMPANIES_RESOURCE_ID}"
                f"&filters={quote(filters)}&limit=1"
            )
            async with build_client() as client:
                response = await client.get(url)
                if not response.is_success:
                    import logging
                    logging.getLogger(__name__).warning(
                        "CRO Open Data returned %s — skipping (url=%s)",
                        response.status_code,
                        response.url,
                    )
                    return {
                        "source_id": self.id,
                        "crn": crn,
                        "company": None,
                        "legal_name": legal_name,
                        "is_stub": False,
                    }
                data = response.json()

            records = (data.get("result") or {}).get("records") or []
            company = records[0] if records else {}
            self._cache.put(cache_key, company)

        return {
            "source_id": self.id,
            "crn": crn,
            "company": company,
            "legal_name": legal_name,
            "is_stub": False,
            "extract_modified": await self._extract_modified(),
        }

    async def _extract_modified(self) -> str | None:
        """When CRO last refreshed the Company Records resource (Phase 315).

        The datastore is a periodic extract (refreshed 30 Sep 2026 at the
        time of writing), so a row is CRO's claim as of that refresh, not of
        the moment OpenCheck queried it. Read from CKAN's ``resource_show``
        and held in memory for a day — deliberately not in the response
        cache, whose read would record a ``cached`` observation and downgrade
        the record's own live fetch. ``None`` if it cannot be read, so the row
        falls back to the retrieval date rather than failing the fetch.
        """
        global _extract_meta
        now = time.monotonic()
        if _extract_meta is not None and now - _extract_meta[0] < _EXTRACT_META_TTL_S:
            return _extract_meta[1]
        if not self.info.live_available:
            return None
        try:
            async with build_client() as client:
                response = await client.get(
                    f"{_CKAN_BASE}/resource_show?id={_COMPANIES_RESOURCE_ID}"
                )
            if not response.is_success:
                return None
            result = (response.json() or {}).get("result") or {}
        except Exception:  # noqa: BLE001 — a missing date must not cost the record
            return None
        value = result.get("last_modified")
        _extract_meta = (now, value if isinstance(value, str) else None)
        return _extract_meta[1]

    # ------------------------------------------------------------------
    # CKAN search helper
    # ------------------------------------------------------------------

    async def _ckan_search(
        self, query: str, *, cache_key: str, limit: int = 10
    ) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        url = (
            f"{_CKAN_BASE}/datastore_search"
            f"?resource_id={_COMPANIES_RESOURCE_ID}"
            f"&q={quote(query)}&limit={limit}"
        )
        async with build_client() as client:
            response = await client.get(url)
            if not response.is_success:
                import logging
                logging.getLogger(__name__).warning(
                    "CRO Open Data returned %s — skipping", response.status_code
                )
                return {}
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _company_hit(rec: dict[str, Any]) -> SourceHit:
        crn = str(rec.get("company_num") or "")
        name = (rec.get("company_name") or crn or "Unknown").strip()
        company_type = (rec.get("company_type") or "").strip()
        status = (rec.get("company_status") or "").strip()

        summary_parts = [f"IE-CRN {crn}"]
        if company_type:
            summary_parts.append(company_type)
        if status and status.lower() not in ("normal", "normal "):
            summary_parts.append(status)

        return SourceHit(
            source_id="cro",
            hit_id=crn,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_parts),
            identifiers={"ie_crn": crn},
            raw=rec,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="249885",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub CRO record — set OPENCHECK_ALLOW_LIVE=true to "
                    "query the live CRO Open Data Portal."
                ),
                identifiers={"ie_crn": "249885"},
                raw={"company_num": 249885, "company_name": f"{query} (stub)"},
            )
        ]
