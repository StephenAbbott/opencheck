"""KvK (Kamer van Koophandel) Open Data adapter.

KvK is the Netherlands Chamber of Commerce, operator of the Dutch
commercial registry (Handelsregister).  This adapter uses the
open-data endpoint published under the EU High-Value Datasets
obligation — no authentication is required.

Live endpoint:
  GET https://opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer/{kvkNummer}

The response is intentionally limited: it carries registration status,
legal form code, SBI activity codes, and a partial postal-code region.
Company name and full address are NOT present in the open-data tier;
the name is supplied by the caller (sourced from GLEIF) and stored in
an in-process name cache so that deepen/report calls can retrieve it.

The flow with GLEIF:
  1. GLEIF carries ``registeredAt.id == "RA000463"`` (KvK RA code) and
     ``registeredAs = "<8-digit KvK number>"`` for Dutch entities.
  2. app.py extracts ``derived["kvk_number"]`` and calls ``fetch()``
     here, passing ``legal_name`` from GLEIF.
  3. Subsequent internal calls (e.g. ``_safe_deepen``) retrieve the
     cached name via the in-process store.

License: CC BY 4.0 (EU Open Data Directive / High-Value Datasets)
API reference:
  https://developers.kvk.nl/nl/documentation/open-dataset-basis-bedrijfsgegevens-api
Press release (GLEIF partnership):
  https://www.gleif.org/en/newsroom/press-releases/gleif-and-the-netherlands-chamber-of-commerce-kvk-collaborate-to-enable-instant-due-diligence
"""

from __future__ import annotations

from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://opendata.kvk.nl/api/v1/hvds"
_CACHE_NS = "kvk"

# GLEIF Registration Authority code for KvK (Netherlands Chamber of Commerce).
KVK_RA_CODE: str = "RA000463"


def normalise_kvk(kvk_number: str) -> str:
    """Normalise a KvK number: strip whitespace, zero-pad to 8 digits."""
    return kvk_number.strip().zfill(8)


class KvKAdapter(SourceAdapter):
    """Source adapter for KvK — Netherlands Chamber of Commerce open data."""

    id = "kvk"

    def __init__(self) -> None:
        self._cache = Cache()
        # In-process name store: kvk_number -> legal_name.
        # KvK open data does not carry a company name, so we accept the
        # GLEIF-sourced name from app.py and cache it here for the lifetime
        # of the server process.  This ensures that _safe_deepen calls
        # (which happen immediately after /lookup within the same process)
        # can produce named BODS entity statements.
        self._names: dict[str, str] = {}

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="KvK — Netherlands Chamber of Commerce",
            homepage="https://www.kvk.nl/",
            description=(
                "Dutch company data from the Netherlands Chamber of Commerce "
                "(KvK) open-data API, sourced via the KvK registration number."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Netherlands Chamber of Commerce (KvK) "
                "via the KvK Open Data API (CC BY 4.0)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed, not name-searchable.
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search is not supported — returns an empty list.

        KvK entities are reached via their KvK number (from GLEIF
        ``registeredAs``), not by free-text name.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the KvK open-data record for a KvK number.

        ``hit_id`` must be an 8-digit KvK number (string, with or without
        leading zeros).  Pass ``legal_name`` to record the entity's name
        (from GLEIF) — it is stored in-process and included in subsequent
        fetches of the same number within the same server run.
        """
        kvk_number = normalise_kvk(hit_id)

        # Persist the name if provided.
        if legal_name:
            self._names[kvk_number] = legal_name

        cache_key = f"{_CACHE_NS}/number/{kvk_number}"
        cached = self._cache.get_payload(cache_key)

        if cached is not None:
            data = cached[0]
        elif not self.info.live_available:
            return {
                "source_id": self.id,
                "kvk_number": kvk_number,
                "company": None,
                "legal_name": self._names.get(kvk_number, ""),
                "is_stub": True,
            }
        else:
            async with build_client() as client:
                response = await client.get(
                    f"{_API_BASE}/basisbedrijfsgegevens/kvknummer/{kvk_number}",
                )
                response.raise_for_status()
                data = response.json()
            self._cache.put(cache_key, data)

        return {
            "source_id": self.id,
            "kvk_number": kvk_number,
            "company": data,
            "legal_name": self._names.get(kvk_number, ""),
            "is_stub": False,
        }
