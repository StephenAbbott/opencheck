"""Slovak Register of Legal Persons (Register právnických osôb — RPO) adapter.

RPO is the authoritative business register for the Slovak Republic, operated
by the Statistical Office of the Slovak Republic (Štatistický úrad SR / ŠÚ SR).
It covers all legal persons registered in Slovakia: obchodné spoločnosti
(companies), živnostníci (sole traders), neziskové organizácie (non-profits),
and other entity types.

API: https://api.statistics.sk/rpo/v1/
Documentation: https://susrrpo.docs.apiary.io/

Endpoints used
--------------
* ``GET /search?fullName=<query>&limit=<n>``  — free-text name search.
* ``GET /search?identifier=<ico>&limit=1``    — fetch by IČO (identifier lookup).

There is no separate detail endpoint; the search response contains the full
record including name history, address history, establishment date,
termination date, source-register data, and registration numbers.

Response structure (key fields)
--------------------------------
* ``id``                              — internal RPO UUID
* ``identifiers[].value``            — IČO if ``identifiers[].type.value == "IČO"``
* ``fullNames[]``                     — name history; entry without ``validTo`` is current
* ``addresses[]``                     — address history; entry without ``validTo`` is current
* ``establishment``                   — ISO date of establishment (or null)
* ``termination``                     — ISO date of dissolution (null = active)
* ``sourceRegister.registrationNumbers[]``  — registration numbers in source register
* ``sourceRegister.registrationOffices[]``  — court / office name
* ``sourceRegister.value.value``      — register type (e.g. "Obchodný register")

GLEIF integration
-----------------
GLEIF Registration Authority code for the Slovak Obchodný register:
  RA000526  (Ministry of Justice SR, Obchodný register)

The ``registeredAs`` field in GLEIF Level 1 records for Slovak entities
contains the IČO.  ``app.py`` extracts ``sk_ico`` from this and passes it
to ``fetch()``.

Authentication: none — fully public API.
License: Creative Commons Attribution 4.0 International (CC BY 4.0)
Attribution: "Contains data from the Slovak Register of Legal Persons (RPO),
  published by the Statistical Office of the Slovak Republic (ŠÚ SR),
  CC BY 4.0. https://creativecommons.org/licenses/by/4.0/"
Portal: https://rpo.statistics.sk/
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_log = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Slovak Obchodný register.
SK_RPO_RA_CODE: str = "RA000526"

_BASE = "https://api.statistics.sk/rpo/v1"
_PORTAL_BASE = "https://rpo.statistics.sk"

_CACHE_NS = "rpo_slovakia"

# IČO identifier type key used in the RPO API response.
_ICO_TYPE = "IČO"


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def normalise_ico(ico: str | int) -> str:
    """Return IČO normalised to an 8-digit zero-padded string."""
    return str(ico).strip().zfill(8)


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _extract_current(items: list[dict], value_key: str = "value") -> str | None:
    """Extract the current (no ``validTo``) entry from a history list.

    RPO history arrays have entries of the form::

        {"validFrom": "YYYY-MM-DD", "validTo": null-or-date, <value_key>: {...}}

    The entry without ``validTo`` (or ``validTo == null``) is the current one.
    If multiple entries have no ``validTo``, take the one with the latest
    ``validFrom``.  Fall back to the most recent entry if all have a ``validTo``.
    """
    if not items:
        return None
    current = [i for i in items if not i.get("validTo")]
    pool = current if current else items
    latest = sorted(pool, key=lambda x: x.get("validFrom") or "", reverse=True)
    if not latest:
        return None
    entry = latest[0]
    val = entry.get(value_key)
    if val is None:
        return None
    if isinstance(val, dict):
        # e.g. fullNames[].value is a dict with "value" key
        return str(val.get("value") or "").strip() or None
    return str(val).strip() or None


def _extract_current_address(addresses: list[dict]) -> str | None:
    """Build a flat address string from the current address history entry."""
    if not addresses:
        return None
    current = [a for a in addresses if not a.get("validTo")]
    pool = current if current else addresses
    latest = sorted(pool, key=lambda x: x.get("validFrom") or "", reverse=True)
    if not latest:
        return None
    addr = latest[0].get("value") or {}
    # addr is a dict with keys: street, buildingNumber, municipality,
    # postalCode, country, etc.
    parts = []
    street = addr.get("street") or addr.get("streetName") or ""
    number = addr.get("buildingNumber") or addr.get("orientationNumber") or ""
    if street and number:
        parts.append(f"{street} {number}")
    elif street:
        parts.append(street)
    elif number:
        parts.append(number)
    municipality = addr.get("municipality") or addr.get("municipalityName") or ""
    if municipality:
        parts.append(municipality)
    postal = addr.get("postalCode") or ""
    if postal:
        parts.append(postal)
    country = addr.get("country") or addr.get("countryCode") or ""
    if country:
        parts.append(country)
    return ", ".join(parts) if parts else None


def _extract_ico(identifiers: list[dict]) -> str | None:
    """Extract the IČO from the identifiers list."""
    for ident in identifiers or []:
        type_info = ident.get("type") or {}
        type_val = type_info.get("value") or type_info.get("id") or ""
        if _ICO_TYPE in str(type_val).upper():
            val = ident.get("value")
            if val and str(val).strip().lower() not in ("neuvedené", ""):
                return normalise_ico(val)
    return None


def _entity_link(ico: str) -> str:
    return f"{_PORTAL_BASE}/#/search/detail/{ico}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class RpoSlovakiaAdapter(SourceAdapter):
    """Adapter for the Slovak Register of Legal Persons (RPO)."""

    id = "rpo_slovakia"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="RPO Slovakia",
            homepage="https://rpo.statistics.sk/",
            description=(
                "Slovak Register of Legal Persons (Register právnických osôb), "
                "operated by the Statistical Office of the Slovak Republic."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Slovak Register of Legal Persons (RPO), "
                "published by the Statistical Office of the Slovak Republic (ŠÚ SR), "
                "CC BY 4.0."
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

        url = f"{_BASE}/search?fullName={quote(query)}&limit=10"
        items = await self._get_list(url, cache_key=cache_key)
        return [hit for item in items if (hit := self._entity_hit(item)) is not None]

    # ------------------------------------------------------------------
    # Fetch (identifier-keyed — by IČO)
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch full RPO record for a given IČO.

        ``hit_id`` is an IČO (8-digit Slovak business identifier,
        zero-padded).  Returns a bundle dict or ``{"is_stub": True}``
        when live mode is off and no cache entry exists.
        """
        ico = normalise_ico(hit_id)
        cache_key = f"{_CACHE_NS}/ico/{ico}"

        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        url = f"{_BASE}/search?identifier={quote(ico)}&limit=1"
        items = await self._get_list(url, cache_key=cache_key)

        if not items:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        item = items[0]
        return self._build_bundle(item, ico)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _get_list(self, url: str, *, cache_key: str) -> list[dict[str, Any]]:
        """GET a URL expected to return a JSON array (or object with results).

        Caches the parsed list; returns [] on network/parse failure.
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            data = cached[0]
            return data if isinstance(data, list) else []

        try:
            async with build_client() as client:
                response = await client.get(url)
                response.raise_for_status()
                payload = response.json()
        except Exception as exc:  # noqa: BLE001
            _log.warning("RPO Slovakia request failed for %s: %s", url, exc)
            return []

        # API returns a raw list or an object containing a list.
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            # Try common wrapper keys.
            for key in ("results", "items", "data", "content"):
                if isinstance(payload.get(key), list):
                    items = payload[key]
                    break
            else:
                items = [payload] if payload else []
        else:
            items = []

        self._cache.put(cache_key, items)
        return items

    # ------------------------------------------------------------------
    # Bundle builder
    # ------------------------------------------------------------------

    def _build_bundle(self, item: dict[str, Any], ico: str) -> dict[str, Any]:
        """Build a normalised payload bundle from a raw RPO record."""
        name = _extract_current(item.get("fullNames") or [], value_key="value") or ""
        address = _extract_current_address(item.get("addresses") or [])
        establishment = item.get("establishment")
        termination = item.get("termination")
        status = "dissolved" if termination else "active"

        # Registration numbers from source register.
        src_reg = item.get("sourceRegister") or {}
        reg_numbers = [
            r.get("value") for r in (src_reg.get("registrationNumbers") or [])
            if r.get("value")
        ]
        reg_offices = [
            o.get("value") for o in (src_reg.get("registrationOffices") or [])
            if o.get("value")
        ]
        source_register_type = (
            (src_reg.get("value") or {}).get("value") or ""
        )

        link = _entity_link(ico)

        return {
            "source_id": self.id,
            "hit_id": ico,
            "sk_ico": ico,
            "name": name,
            "address": address,
            "establishment": establishment,
            "termination": termination,
            "status": status,
            "registration_numbers": reg_numbers,
            "registration_offices": reg_offices,
            "source_register": source_register_type,
            "link": link,
            "raw": item,
            "is_stub": False,
        }

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    def _entity_hit(self, item: dict[str, Any]) -> SourceHit | None:
        """Convert a raw RPO search result to a ``SourceHit``."""
        ico = _extract_ico(item.get("identifiers") or [])
        if not ico:
            # Entities without a usable IČO are unusable as hits.
            return None

        name = _extract_current(item.get("fullNames") or [], value_key="value") or "Unknown entity"
        address = _extract_current_address(item.get("addresses") or [])
        termination = item.get("termination")
        status = "dissolved" if termination else "active"

        summary_bits = [f"IČO {ico}"]
        if address:
            summary_bits.append(address)
        summary_bits.append(status)

        return SourceHit(
            source_id=self.id,
            hit_id=ico,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_bits),
            identifiers={"sk_ico": ico},
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
                hit_id="00000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary="Stub RPO Slovakia record — set OPENCHECK_ALLOW_LIVE=true to query live.",
                identifiers={"sk_ico": "00000000"},
                raw={"ico": "00000000", "name": f"{query} (stub)"},
            )
        ]
