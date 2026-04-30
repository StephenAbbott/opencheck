"""OpenCorporates adapter.

OpenCorporates is the world's largest open legal entity database,
providing a single unified set of company records from government
registries (https://opencorporates.com/).

The entry point is the ``ocid`` field that GLEIF returns on Level 1
records (``attributes.ocid``), formatted as
``{jurisdiction_code}/{company_number}`` (e.g. ``gb/00102498``).
This maps directly to the OpenCorporates API path:

    GET /v0.4/companies/{jurisdiction}/{company_number}
    GET /v0.4/companies/{jurisdiction}/{company_number}/officers

Authentication: ``api_token`` query parameter. Gated on
``allow_live=true`` + key. All responses are cached.

License: OC data is openly licensed per-jurisdiction; the API terms
of service permit re-use with attribution. Corporate records
themselves are public-domain government data. Officers data
carries a similar regime.
"""

from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://api.opencorporates.com/v0.4"
_CACHE_NS = "opencorporates"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _ocid_to_path(ocid: str) -> str:
    """Convert ``gb/00102498`` → ``/gb/00102498`` (URL-safe segments)."""
    parts = ocid.strip("/").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid ocid {ocid!r} — expected jurisdiction/number")
    jurisdiction, number = parts
    return f"/{quote(jurisdiction.lower())}/{quote(number)}"


class OpenCorporatesAdapter(SourceAdapter):
    id = "opencorporates"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenCorporates",
            homepage="https://opencorporates.com/",
            description=(
                "The world's largest open legal-entity database, providing "
                "a single unified set of company records from government "
                "registries."
            ),
            license="OC-Terms",
            attribution=(
                "Contains company data from OpenCorporates "
                "(https://opencorporates.com/). Licensed per-jurisdiction "
                "under the source government registry license."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(
                settings.opencorporates_api_key and settings.allow_live
            ),
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """OC is entered via ocid (from GLEIF), not free-text search.

        This method is provided for protocol compliance. It returns an
        empty list because the canonical entry point is ``fetch(ocid)``
        via the GLEIF-derived ``ocid`` identifier.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch company data + officers for a given ocid.

        ``hit_id`` is an OpenCorporates identifier in the format
        ``jurisdiction/company_number`` (e.g. ``gb/00102498``),
        exactly as returned by the GLEIF ``attributes.ocid`` field.
        """
        ocid = hit_id.strip()
        path = _ocid_to_path(ocid)
        cache_key = f"{_CACHE_NS}/company/{_slug(ocid)}"

        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        company_data = await self._get(f"/companies{path}", cache_key=cache_key)

        # Officers are on a child endpoint; optional — 404 is fine.
        officers_cache_key = f"{_CACHE_NS}/officers/{_slug(ocid)}"
        officers_data = await self._get_optional(
            f"/companies{path}/officers",
            cache_key=officers_cache_key,
        )

        # Network relationships — requires the OC Relationships Supplement
        # (a premium API tier). Returns None if not available (403/404).
        network_cache_key = f"{_CACHE_NS}/network/{_slug(ocid)}"
        network_data = await self._get_optional(
            f"/companies{path}/network",
            cache_key=network_cache_key,
        )

        return {
            "source_id": self.id,
            "hit_id": ocid,
            "ocid": ocid,
            "company": (company_data.get("results") or {}).get("company") or {},
            "officers": (
                ((officers_data or {}).get("results") or {}).get("officers") or []
            ),
            # Raw network payload from the Relationships Supplement.
            # Present only when the API key has access to that tier.
            "network": (network_data.get("results") or {}) if network_data else None,
            "raw_company": company_data,
            "raw_officers": officers_data,
        }

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        settings = get_settings()
        assert settings.opencorporates_api_key, "live_available should have been false"

        params = {"api_token": settings.opencorporates_api_key}
        async with build_client() as client:
            response = await client.get(f"{_API_BASE}{path}", params=params)
            response.raise_for_status()
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    async def _get_optional(
        self, path: str, *, cache_key: str
    ) -> dict[str, Any] | None:
        """Like ``_get`` but returns ``None`` on 404."""
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]
        if not self.info.live_available:
            return None

        settings = get_settings()
        assert settings.opencorporates_api_key, "live_available should have been false"

        params = {"api_token": settings.opencorporates_api_key}
        try:
            async with build_client() as client:
                response = await client.get(
                    f"{_API_BASE}{path}", params=params
                )
                if response.status_code in (402, 403, 404):
                    # 402/403: endpoint requires a premium API tier.
                    # 404:     no data for this company.
                    self._cache.put(cache_key, None)
                    return None
                response.raise_for_status()
                payload = response.json()
        except Exception:  # noqa: BLE001
            self._cache.put(cache_key, None)
            return None

        self._cache.put(cache_key, payload)
        return payload
