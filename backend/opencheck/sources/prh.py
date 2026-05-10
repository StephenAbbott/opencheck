"""Patentti- ja rekisterihallitus (PRH) adapter — Finnish Patent and
Registration Office.

PRH maintains the Finnish Trade Register (Kaupparekisteri) and the Business
Information System (YTJ — Yritys- ja yhteisötietojärjestelmä) jointly with
the Finnish Tax Administration. The YTJ Open Data API provides free, key-less
access to entity data for all organisations registered in Finland.

This adapter uses the YTJ Open Data REST API:

* ``GET /companies?name=<query>&maxResults=10``
  — fuzzy name search; returns summary entity records.
* ``GET /companies?businessId=<y-tunnus>``
  — single entity by Finnish Business ID (Y-tunnus).

Beneficial ownership data is not publicly available from PRH. The paid
**Virre Information Service** provides officer/role data at €4.02 per person
search (€2.01 for contract clients, who also pay a €125.50 one-time start-up
fee and €27.61/year per username). This adapter therefore maps entity data
only.

The flow with GLEIF:

  1. GLEIF returns ``registeredAt.id == "RA000188"`` (PRH RA code) and
     ``registeredAs = "<y-tunnus>"`` (with or without the hyphen) for
     Finnish entities.
  2. app.py extracts ``derived["fi_ytunnus"]`` and calls ``fetch()`` here.
  3. We fetch the entity record and map it to a BODS entity statement.

Authentication: none — public API, no key required.
License: CC BY 4.0 (Creative Commons Attribution 4.0 International).
  https://creativecommons.org/licenses/by/4.0/
Attribution: Contains data from Patentti- ja rekisterihallitus (PRH) /
  Finnish Patent and Registration Office, via the YTJ Open Data API,
  licensed under CC BY 4.0.
API reference: https://avoindata.prh.fi/en/ytj/swagger-ui
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

_API_BASE = "https://avoindata.prh.fi/opendata-ytj-api/v3"
_CACHE_NS = "prh"

# GLEIF Registration Authority code for the Finnish Trade Register (Kaupparekisteri).
# All Finnish entities in GLEIF carry registeredAt.id == "RA000188".
FI_RA_CODE: str = "RA000188"

# Finnish Business ID (Y-tunnus): 7 digits, hyphen, 1 check digit.
# e.g. "0112038-9" (Nokia Oyj) or "01120389" (same, hyphen stripped by GLEIF).
_YTUNNUS_RE = re.compile(r"^\d{7}-\d$")
_YTUNNUS_NOHYPHEN_RE = re.compile(r"^\d{8}$")


def normalise_ytunnus(ytunnus: str) -> str:
    """Normalise a Y-tunnus to canonical ``XXXXXXX-X`` format.

    Accepts:
    * ``0112038-9``  → ``0112038-9``  (already canonical)
    * ``01120389``   → ``0112038-9``  (GLEIF sometimes strips the hyphen)
    * ``112038-9``   → ``0112038-9``  (leading-zero padding)
    * ``01120389  `` → ``0112038-9``  (whitespace stripped)
    """
    cleaned = re.sub(r"\s", "", ytunnus.strip())
    if _YTUNNUS_NOHYPHEN_RE.match(cleaned):
        # 8 raw digits — insert hyphen before the check digit.
        cleaned = f"{cleaned[:7]}-{cleaned[7]}"
    # Ensure 7 digits before the hyphen (pad with leading zeros if needed).
    if "-" in cleaned:
        body, check = cleaned.split("-", 1)
        body = body.zfill(7)
        cleaned = f"{body}-{check}"
    return cleaned


def is_valid_ytunnus(ytunnus: str) -> bool:
    return bool(_YTUNNUS_RE.match(normalise_ytunnus(ytunnus)))


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _company_url(ytunnus: str) -> str:
    """Link to the company's page on the Finnish business information service."""
    return f"https://tietopalvelu.ytj.fi/yritystiedot.aspx?yavain={ytunnus}"


def _current_name(names: list[dict[str, Any]]) -> str:
    """Extract the current primary name from the PRH names array.

    PRH returns a list of name records. The active primary name has
    ``order == 0`` and no ``endDate``. We fall back to the first name
    with ``order == 0`` if no other is found, then any name.
    """
    candidates = [n for n in names if not n.get("endDate")]
    primary = [n for n in candidates if n.get("order") == 0]
    if primary:
        return (primary[0].get("name") or "").strip()
    if candidates:
        return (candidates[0].get("name") or "").strip()
    if names:
        return (names[0].get("name") or "").strip()
    return ""


class PrhAdapter(SourceAdapter):
    """Source adapter for the Finnish Patent and Registration Office (PRH)."""

    id = "prh"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="PRH — Finnish Patent and Registration Office",
            homepage="https://www.prh.fi/en/index.html",
            description=(
                "Finnish company data from the Patentti- ja rekisterihallitus "
                "(PRH) via the YTJ Open Data API, including entity details for "
                "all organisations registered in Finland. Officer data is not "
                "publicly available."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from Patentti- ja rekisterihallitus (PRH) / "
                "Finnish Patent and Registration Office, via the YTJ Open Data "
                "API (avoindata.prh.fi), licensed under CC BY 4.0."
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
            f"/companies?name={quote(query)}&maxResults=10",
            cache_key=cache_key,
        )
        companies = payload.get("companies") or []
        return [self._company_hit(c) for c in companies if c.get("businessId")]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the full PRH entity record for a Y-tunnus.

        ``hit_id`` should be a Y-tunnus in canonical or hyphen-stripped form.
        ``legal_name`` is an optional fallback from GLEIF when the API is
        unavailable or returns no name.
        """
        try:
            ytunnus = normalise_ytunnus(hit_id)
        except Exception:
            ytunnus = hit_id.strip()

        if not is_valid_ytunnus(ytunnus):
            return {
                "source_id": self.id,
                "ytunnus": ytunnus,
                "company": None,
                "legal_name": legal_name,
                "is_stub": True,
            }

        cache_key = f"{_CACHE_NS}/company/{ytunnus}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {
                "source_id": self.id,
                "ytunnus": ytunnus,
                "company": None,
                "legal_name": legal_name,
                "is_stub": True,
            }

        payload = await self._get(
            f"/companies?businessId={quote(ytunnus)}",
            cache_key=cache_key,
        )
        companies = payload.get("companies") or []
        company = companies[0] if companies else {}

        return {
            "source_id": self.id,
            "ytunnus": ytunnus,
            "company": company,
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
                    "PRH YTJ API returned %s — skipping (url=%s)",
                    response.status_code,
                    response.url,
                )
                return {}
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _company_hit(item: dict[str, Any]) -> SourceHit:
        business_id_block = item.get("businessId") or {}
        ytunnus = str(business_id_block.get("value") or "")
        names = item.get("names") or []
        name = _current_name(names) or ytunnus or "Unknown"

        company_form = (item.get("companyForm") or "").strip()
        municipality = ""
        offices = item.get("registeredOffices") or []
        if offices:
            municipality = (offices[0].get("name") or "").strip()

        summary_parts = [f"FI-YTUNNUS {ytunnus}"]
        if company_form:
            summary_parts.append(company_form)
        if municipality:
            summary_parts.append(municipality)

        # Flag dissolved or struck-off entities.
        if item.get("liquidations"):
            summary_parts.append("in liquidation")
        elif not business_id_block.get("registrationDate"):
            pass  # still forming

        return SourceHit(
            source_id="prh",
            hit_id=ytunnus,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_parts),
            identifiers={"fi_ytunnus": ytunnus},
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
                hit_id="0112038-9",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub PRH record — set OPENCHECK_ALLOW_LIVE=true "
                    "to query the live YTJ Open Data API."
                ),
                identifiers={"fi_ytunnus": "0112038-9"},
                raw={
                    "businessId": {"value": "0112038-9"},
                    "names": [{"name": f"{query} (stub)", "order": 0}],
                },
            )
        ]
