"""UK Companies House adapter.

Live endpoints (Phase 1):

* ``GET /search/companies?q=<query>`` — entity search
* ``GET /search/officers?q=<query>`` — person search
* ``GET /company/{number}`` — company profile
* ``GET /company/{number}/officers`` — officers list
* ``GET /company/{number}/persons-with-significant-control`` — PSCs

Authentication: HTTP Basic with the API key as the username and an empty
password (Companies House convention).

Live calls are gated on ``allow_live=true`` AND a configured API key. When
either is missing we fall back to the Phase 0 stub path. Every response is
cached under ``data/cache/live/companies_house/...`` so repeated lookups are
free and deterministic.
"""

from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import quote

import httpx

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://api.company-information.service.gov.uk"
_CACHE_NS = "companies_house"


def _slug(text: str) -> str:
    """Cache-safe slug for a free-text query."""
    digest = hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]
    return digest


def _looks_like_company_number(value: str) -> bool:
    """True for the 8-character alphanumeric Companies House number shape.

    UK company numbers are 8 chars, alphanumeric (e.g. ``00102498``,
    ``SC123456``, ``OC403762``). Officer ids are base64-shaped and
    longer (typically 27 chars) — no overlap.
    """
    return len(value) == 8 and value.replace(" ", "").isalnum()


class CompaniesHouseAdapter(SourceAdapter):
    id = "companies_house"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="UK Companies House",
            homepage="https://find-and-update.company-information.service.gov.uk/",
            description=(
                "Legal and beneficial ownership information from the UK "
                "corporate registry."
            ),
            license="OGL-3.0",
            attribution=(
                "Contains public sector information licensed under the "
                "Open Government Licence v3.0 (Companies House)."
            ),
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=True,
            live_available=bool(settings.companies_house_api_key and settings.allow_live),
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        bucket = "companies" if kind == SearchKind.ENTITY else "officers"
        cache_key = f"{_CACHE_NS}/search/{bucket}/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query, kind)

        if kind == SearchKind.ENTITY:
            payload = await self._get(
                f"/search/companies?q={quote(query)}&items_per_page=10",
                cache_key=cache_key,
            )
            return [self._entity_hit(item) for item in payload.get("items", [])]

        payload = await self._get(
            f"/search/officers?q={quote(query)}&items_per_page=10",
            cache_key=cache_key,
        )
        return [self._officer_hit(item) for item in payload.get("items", [])]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Return either a company bundle or an officer-appointments bundle.

        Companies House officer ids look like
        ``zS_RY9pRYlJ9XwGJEOFtkJgrf8s`` — base64-shaped, much longer than
        the 8-char company-number space and usually containing characters
        that aren't valid in company numbers (``_``, ``-``). We dispatch
        based on that shape.
        """
        # Pick the primary cache key based on dispatch shape so demo
        # fixtures override the stub path when present.
        if _looks_like_company_number(hit_id):
            primary_key = f"{_CACHE_NS}/company/{hit_id}"
        else:
            primary_key = f"{_CACHE_NS}/officer/{hit_id}/appointments"
        if not self.info.live_available and not self._cache.has(primary_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        if _looks_like_company_number(hit_id):
            return await self._fetch_company_bundle(hit_id)
        return await self._fetch_officer_bundle(hit_id)

    async def _fetch_company_bundle(self, number: str) -> dict[str, Any]:
        profile = await self._get(
            f"/company/{number}",
            cache_key=f"{_CACHE_NS}/company/{number}",
        )
        officers = await self._get(
            f"/company/{number}/officers",
            cache_key=f"{_CACHE_NS}/company/{number}/officers",
        )
        pscs = await self._get(
            f"/company/{number}/persons-with-significant-control",
            cache_key=f"{_CACHE_NS}/company/{number}/pscs",
        )
        return {
            "source_id": self.id,
            "company_number": number,
            "profile": profile,
            "officers": officers,
            "pscs": pscs,
        }

    async def _fetch_officer_bundle(self, officer_id: str) -> dict[str, Any]:
        """Return appointments for a Companies House officer id.

        Companies House does not expose a dedicated "officer profile"
        endpoint — instead, ``/officers/{id}/appointments`` returns
        every appointment for that officer along with that officer's
        canonical name, DOB year/month, nationality, occupation, and
        country of residence (encoded once on the appointment block).
        We package that into a neutral bundle the BODS mapper can turn
        into a ``personStatement`` plus one relationship per appointment.
        """
        appointments = await self._get(
            f"/officers/{quote(officer_id)}/appointments",
            cache_key=f"{_CACHE_NS}/officer/{officer_id}/appointments",
        )
        return {
            "source_id": self.id,
            "officer_id": officer_id,
            "appointments": appointments,
        }

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]  # unwrap (payload, tier)

        settings = get_settings()
        assert settings.companies_house_api_key, "live_available should have been false"

        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}{path}",
                auth=httpx.BasicAuth(settings.companies_house_api_key, ""),
            )
            response.raise_for_status()
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factories (live)
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(item: dict[str, Any]) -> SourceHit:
        number = str(item.get("company_number", ""))
        name = item.get("title", f"Company {number}")
        status = item.get("company_status", "unknown")
        address = item.get("address_snippet", "")
        summary = f"Company {number} · {status}" + (f" · {address}" if address else "")
        return SourceHit(
            source_id="companies_house",
            hit_id=number,
            kind=SearchKind.ENTITY,
            name=name,
            summary=summary,
            identifiers={"gb_coh": number},
            raw=item,
            is_stub=False,
        )

    @staticmethod
    def _officer_hit(item: dict[str, Any]) -> SourceHit:
        name = item.get("title", "Unknown officer")
        appointment_count = item.get("appointment_count", 0)
        summary = f"{appointment_count} appointment(s)"
        date_of_birth = item.get("date_of_birth")
        if isinstance(date_of_birth, dict) and "year" in date_of_birth:
            summary += f" · born {date_of_birth.get('year')}"
        # Officer self-links look like "/officers/<id>/appointments".
        # Extract the id segment, not the trailing "appointments".
        self_link = item.get("links", {}).get("self", "")
        parts = [p for p in self_link.split("/") if p]
        hit_id = (
            parts[parts.index("officers") + 1]
            if "officers" in parts and parts.index("officers") + 1 < len(parts)
            else f"officer-{_slug(name)}"
        )
        return SourceHit(
            source_id="companies_house",
            hit_id=hit_id,
            kind=SearchKind.PERSON,
            name=name,
            summary=summary,
            identifiers={},
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Phase 0 stub path — preserved for allow_live=false
    # ------------------------------------------------------------------

    def _stub_search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind == SearchKind.ENTITY:
            return [
                SourceHit(
                    source_id=self.id,
                    hit_id="00000000",
                    kind=kind,
                    name=f"{query} (stub)",
                    summary=(
                        "Stub company record — set OPENCHECK_ALLOW_LIVE=true + "
                        "COMPANIES_HOUSE_API_KEY to query live."
                    ),
                    identifiers={"gb_coh": "00000000"},
                    raw={"company_number": "00000000", "title": f"{query} (stub)"},
                )
            ]
        return [
            SourceHit(
                source_id=self.id,
                hit_id="officer-stub-0",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub officer record — set OPENCHECK_ALLOW_LIVE=true + "
                    "COMPANIES_HOUSE_API_KEY to query live."
                ),
                identifiers={},
                raw={"name": f"{query} (stub)", "kind": "officer"},
            )
        ]
