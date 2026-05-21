"""Corporations Canada adapter.

Corporations Canada is the federal registry for companies incorporated under
Canadian federal statutes (Canada Business Corporations Act and others).
It is operated by Innovation, Science and Economic Development Canada (ISED).

This adapter uses the ISED API Gateway:

  * ``GET /v1/companies?lang=eng&corpId=<corpId>``
    — full corporation record by corporation number.
    Always returns HTTP 200; the body is either a two-element array
    ``[corpObject, null]`` (found) or ``["error string", "error en français"]``
    (not found).

  * ``GET /v2/director?lang=eng&corpId=<corpId>``
    — current directors for the corporation.
    Returns ``{"_embedded": {"directors": [...]}}`` or 404 when no
    directors are on file.

Authentication: ``user-key`` header (ISED API key).

The flow with GLEIF:

  1. GLEIF returns ``registeredAt.id == "RA000072"`` (Corporations Canada RA
     code) and ``registeredAs = "<corpId>"`` for Canadian federal entities.
  2. app.py extracts ``derived["ca_corp_id"]`` and calls ``fetch()`` here.
  3. We fetch the corporation record + directors and map both to BODS.

License: Open Government Licence – Canada (OGL-Canada 2.0).
  https://open.canada.ca/en/open-government-licence-canada
Attribution: Contains information licensed under the Open Government Licence
  – Canada. Source: Corporations Canada, Innovation, Science and Economic
  Development Canada.
API reference: https://api.ised-isde.canada.ca/corporations/api
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.corporations_canada import CorpCanadaBundle

_API_BASE = "https://apigateway-passerelledapi.ised-isde.canada.ca/corporations/api"
_CACHE_NS = "corporations_canada"
_LOG = logging.getLogger(__name__)

# GLEIF Registration Authority code for Corporations Canada.
# All Canadian federal entities in GLEIF carry registeredAt.id == "RA000072".
CA_CORP_RA_CODE: str = "RA000072"

# Corporation numbers are purely numeric strings.
_CORP_ID_RE = re.compile(r"^\d+$")


def normalise_corp_id(corp_id: str) -> str:
    """Strip whitespace and non-digit characters from a corporation number.

    Handles inputs like ``"1007"``, ``" 1007 "``, or ``"CA-1007"``.
    Returns just the numeric portion.
    """
    return re.sub(r"\D", "", str(corp_id).strip())


def is_valid_corp_id(corp_id: str) -> bool:
    normalised = normalise_corp_id(corp_id)
    return bool(_CORP_ID_RE.match(normalised)) and len(normalised) > 0


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _corp_url(corp_id: str) -> str:
    return (
        f"https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html"
        f"?corpId={corp_id}&V_TOKEN=null&LANGUAGE_ID=1"
    )


def extract_current_name(corp: dict[str, Any]) -> str:
    """Return the current primary name from a corporation's ``corporationNames`` list."""
    names = corp.get("corporationNames") or []
    # First preference: current=True + nameType="Primary"
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current") and (cn.get("nameType") or "").lower() == "primary":
            return (cn.get("name") or "").strip()
    # Second preference: any current name
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current"):
            return (cn.get("name") or "").strip()
    # Fall back to the last entry in the list
    if names:
        cn = (names[-1].get("CorporationName") or {})
        return (cn.get("name") or "").strip()
    return ""


class CorporationsCanadaAdapter(SourceAdapter):
    """Source adapter for Corporations Canada — ISED federal register."""

    id = "corporations_canada"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Corporations Canada — ISED federal register",
            homepage="https://ised-isde.canada.ca/site/corporations-canada/en",
            description=(
                "Federal Canadian company data from Corporations Canada "
                "(Innovation, Science and Economic Development Canada), "
                "covering companies incorporated under federal statutes "
                "including the Canada Business Corporations Act."
            ),
            license="OGL-Canada-2.0",
            attribution=(
                "Contains information licensed under the Open Government "
                "Licence – Canada. Source: Corporations Canada, Innovation, "
                "Science and Economic Development Canada."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(
                settings.allow_live and settings.corporations_canada_api_key
            ),
            is_national_register=True,
        )

    # ------------------------------------------------------------------
    # Search — lookup-only; name search is not available on the public
    # API key plan.  Return a representative stub so the test suite can
    # exercise the adapter path.
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        return [
            SourceHit(
                source_id=self.id,
                hit_id="1007",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub Corporations Canada record — activate via GLEIF LEI lookup "
                    "with a Canadian federal entity."
                ),
                identifiers={"ca_corp_id": "1007"},
                raw={"corporationId": "1007", "status": "Active"},
                is_stub=True,
            )
        ]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the full Corporations Canada entity + directors bundle.

        ``hit_id`` should be the numeric corporation number (with or without
        non-digit noise — it is normalised before use).
        ``legal_name`` is an optional fallback from GLEIF when the API is
        unavailable or returns no name.
        """
        corp_id = normalise_corp_id(hit_id)
        if not corp_id:
            return {
                "source_id": self.id,
                "corp_id": corp_id,
                "corporation": None,
                "directors": [],
                "legal_name": legal_name,
                "is_stub": True,
            }

        corp_cache_key = f"{_CACHE_NS}/v1/{corp_id}"
        dir_cache_key = f"{_CACHE_NS}/v2/directors/{corp_id}"

        if not self.info.live_available and not self._cache.has(corp_cache_key):
            return {
                "source_id": self.id,
                "corp_id": corp_id,
                "corporation": None,
                "directors": [],
                "legal_name": legal_name,
                "is_stub": True,
            }

        corporation = await self._get_corporation(corp_id, cache_key=corp_cache_key)
        if corporation is None:
            return {
                "source_id": self.id,
                "corp_id": corp_id,
                "corporation": None,
                "directors": [],
                "legal_name": legal_name,
                "is_stub": True,
            }

        directors_payload = await self._get_directors(corp_id, cache_key=dir_cache_key)
        directors: list[dict[str, Any]] = (
            (directors_payload or {}).get("_embedded", {}).get("directors") or []
        )

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "corp_id": corp_id,
            "corporation": corporation,
            "directors": directors,
            "legal_name": legal_name,
            "is_stub": False,
        }
        validate_raw("corporations_canada", CorpCanadaBundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        settings = get_settings()
        key = settings.corporations_canada_api_key or ""
        return {"user-key": key, "Accept": "application/json"}

    async def _get_corporation(
        self, corp_id: str, *, cache_key: str
    ) -> dict[str, Any] | None:
        """Fetch a single corporation record. Returns None for not-found or errors."""
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}/v1/companies",
                params={"lang": "eng", "corpId": corp_id},
                headers=self._auth_headers(),
            )
            if not response.is_success:
                _LOG.warning(
                    "Corporations Canada API returned %s — skipping (corpId=%s)",
                    response.status_code,
                    corp_id,
                )
                return None

            data = response.json()

        # The API always returns HTTP 200, even for unknown corporations.
        # Success:   [corpObject, null]
        # Not found: ["error string", "error en français"]
        if not isinstance(data, list) or not data:
            return None
        first = data[0]
        if not isinstance(first, dict):
            # Error response — corporation not found or invalid
            _LOG.debug(
                "Corporations Canada: no corporation found for corpId=%s (response: %r)",
                corp_id,
                first,
            )
            return None

        self._cache.put(cache_key, first)
        return first

    async def _get_directors(
        self, corp_id: str, *, cache_key: str
    ) -> dict[str, Any] | None:
        """Fetch directors for a corporation. Returns None on 404 or errors."""
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]
        if not self.info.live_available:
            return None

        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}/v2/director",
                params={"lang": "eng", "corpId": corp_id},
                headers=self._auth_headers(),
            )
            if response.status_code == 404:
                self._cache.put(cache_key, None)
                return None
            if not response.is_success:
                _LOG.warning(
                    "Corporations Canada directors API returned %s — skipping (corpId=%s)",
                    response.status_code,
                    corp_id,
                )
                return None
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(corp: dict[str, Any], corp_id: str) -> SourceHit:
        name = extract_current_name(corp) or corp_id or "Unknown"
        status = (corp.get("status") or "").strip()
        act = (corp.get("act") or "").strip()
        summary = f"CA-CORP {corp_id}"
        if status:
            summary += f" · {status}"
        if act:
            summary += f" · {act}"

        return SourceHit(
            source_id="corporations_canada",
            hit_id=corp_id,
            kind=SearchKind.ENTITY,
            name=name,
            summary=summary,
            identifiers={"ca_corp_id": corp_id},
            raw=corp,
            is_stub=False,
        )
