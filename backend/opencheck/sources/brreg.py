"""Brønnøysundregistrene (Brreg) adapter.

Brønnøysundregistrene (Brreg) is the Norwegian Register Centre, operated
under the Norwegian Ministry of Trade, Industry and Fisheries. The
Enhetsregisteret (Central Coordinating Register for Legal Entities) records
every Norwegian organisation with a 9-digit organisation number (orgnr).

This adapter uses the public, key-free REST API:

* ``GET /enhetsregisteret/api/enheter?navn=<query>&size=10``
  — name search; returns abbreviated entity records.
* ``GET /enhetsregisteret/api/enheter/{orgnr}``
  — full entity record by organisation number.
* ``GET /enhetsregisteret/api/enheter/{orgnr}/roller``
  — board members, daily managers, contact persons, and other roles.

Beneficial ownership data (reelle rettighetshavere) is restricted to users
in Norway who hold a legitimate interest (see brreg.no/en/use-of-data).
This adapter therefore maps only the publicly available entity and role data.

The flow with GLEIF:

  1. GLEIF returns ``registeredAt.id == "RA000270"`` (Brreg RA code) and
     ``registeredAs = "9XXXXXXXX"`` (9-digit orgnr) for Norwegian entities.
  2. app.py extracts ``derived["no_orgnr"]`` and calls ``fetch()`` here.
  3. We fetch entity + roles and map both to BODS statements.

Authentication: none — public API, no key required.
License: NLOD 2.0 (Norwegian Licence for Open Government Data).
  https://data.norge.no/nlod/en/2.0
Attribution: Data from Brønnøysundregistrene via Enhetsregisteret,
  licensed under NLOD 2.0.
API reference: https://data.brreg.no/enhetsregisteret/api/docs
"""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://data.brreg.no/enhetsregisteret/api"
_CACHE_NS = "brreg"

# GLEIF Registration Authority code for Brønnøysundregistrene.
# All Norwegian entities in GLEIF carry registeredAt.id == "RA000270".
NO_RA_CODE: str = "RA000270"

# Norwegian organisation number: exactly 9 digits.
_ORGNR_RE = re.compile(r"^\d{9}$")


def normalise_orgnr(orgnr: str) -> str:
    """Strip spaces/dots so ``923 609 016`` or ``923.609.016`` → ``923609016``."""
    cleaned = re.sub(r"[\s.]", "", orgnr.strip())
    return cleaned


def is_valid_orgnr(orgnr: str) -> bool:
    return bool(_ORGNR_RE.match(normalise_orgnr(orgnr)))


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _company_url(orgnr: str) -> str:
    return f"https://w2.brreg.no/enhet/sok/detalj.jsp?orgnr={orgnr}"


class BrregAdapter(SourceAdapter):
    """Source adapter for Brønnøysundregistrene — Norwegian Register Centre."""

    id = "brreg"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Brønnøysundregistrene — Norwegian Register Centre",
            homepage="https://www.brreg.no/en/",
            description=(
                "Norwegian company data from the Enhetsregisteret "
                "(Central Coordinating Register for Legal Entities), "
                "including entity details and role-holders."
            ),
            license="NLOD-2.0",
            attribution=(
                "Contains data from Brønnøysundregistrene via the "
                "Enhetsregisteret, licensed under NLOD 2.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        payload = await self._get(
            f"/enheter?navn={quote(query)}&size=10",
            cache_key=cache_key,
        )
        items = (
            (payload.get("_embedded") or {}).get("enheter") or []
        )
        return [self._entity_hit(item) for item in items]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the full Brreg entity record + roles for an org number.

        ``hit_id`` should be a 9-digit organisation number (spaces/dots stripped).
        ``legal_name`` is an optional fallback from GLEIF when the API is
        unavailable or returns no name.
        """
        orgnr = normalise_orgnr(hit_id)
        if not is_valid_orgnr(orgnr):
            return {
                "source_id": self.id,
                "orgnr": orgnr,
                "entity": None,
                "roles": [],
                "legal_name": legal_name,
                "is_stub": True,
            }

        entity_cache_key = f"{_CACHE_NS}/enheter/{orgnr}"
        roles_cache_key = f"{_CACHE_NS}/roller/{orgnr}"

        if not self.info.live_available and not self._cache.has(entity_cache_key):
            return {
                "source_id": self.id,
                "orgnr": orgnr,
                "entity": None,
                "roles": [],
                "legal_name": legal_name,
                "is_stub": True,
            }

        entity = await self._get(f"/enheter/{quote(orgnr)}", cache_key=entity_cache_key)
        roles_payload = await self._get_optional(
            f"/enheter/{quote(orgnr)}/roller",
            cache_key=roles_cache_key,
        )

        # Flatten the nested rollegrupper structure into a simple list of
        # individual role dicts, each carrying the group type info.
        roles: list[dict[str, Any]] = []
        for group in (roles_payload or {}).get("rollegrupper") or []:
            group_type = group.get("type") or {}
            for role in group.get("roller") or []:
                roles.append({**role, "_group_type": group_type})

        return {
            "source_id": self.id,
            "orgnr": orgnr,
            "entity": entity,
            "roles": roles,
            "legal_name": legal_name,
            "is_stub": False,
        }

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}{path}",
                headers={"Accept": "application/json"},
            )
            if not response.is_success:
                import logging
                logging.getLogger(__name__).warning(
                    "Brreg API returned %s — skipping (url=%s)",
                    response.status_code,
                    response.url,
                )
                return {}
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    async def _get_optional(
        self, path: str, *, cache_key: str
    ) -> dict[str, Any] | None:
        """Like ``_get`` but returns ``None`` on 404 (no roles filed)."""
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]
        if not self.info.live_available:
            return None

        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}{path}",
                headers={"Accept": "application/json"},
            )
            if response.status_code == 404:
                self._cache.put(cache_key, None)
                return None
            if not response.is_success:
                import logging
                logging.getLogger(__name__).warning(
                    "Brreg API returned %s — skipping (url=%s)",
                    response.status_code,
                    response.url,
                )
                return None
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(item: dict[str, Any]) -> SourceHit:
        orgnr = str(item.get("organisasjonsnummer") or "")
        name = item.get("navn") or orgnr or "Unknown"
        org_form = (item.get("organisasjonsform") or {}).get("kode") or ""
        municipality = (item.get("forretningsadresse") or {}).get("kommune") or ""
        status_parts = []
        if org_form:
            status_parts.append(org_form)
        if municipality:
            status_parts.append(municipality)
        if item.get("konkurs"):
            status_parts.append("bankrupt")
        if item.get("underAvvikling"):
            status_parts.append("in liquidation")

        summary = f"NO-ORGNR {orgnr}"
        if status_parts:
            summary += " · " + " · ".join(status_parts)

        return SourceHit(
            source_id="brreg",
            hit_id=orgnr,
            kind=SearchKind.ENTITY,
            name=name,
            summary=summary,
            identifiers={"no_orgnr": orgnr},
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="923609016",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub Brreg record — set OPENCHECK_ALLOW_LIVE=true "
                    "to query the live Enhetsregisteret API."
                ),
                identifiers={"no_orgnr": "923609016"},
                raw={"organisasjonsnummer": "923609016", "navn": f"{query} (stub)"},
            )
        ]
