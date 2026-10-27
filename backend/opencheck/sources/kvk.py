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

import asyncio
from typing import Any

import httpx

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..host_throttle import HostRateLimitedError
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.kvk import KvKBundle

_API_BASE = "https://opendata.kvk.nl/api/v1/hvds"
_CACHE_NS = "kvk"

# Request pacing lives in opencheck/host_throttle.py, installed by
# build_client(): KvK's open data allows one request per minute per IP and
# answers a spent quota with a bare 429 (no Retry-After). Until 27 Oct 2026 this
# adapter had its own retry loop — three tries over ~14 s against a 60 s
# window, each one another refused request — and nothing paced it at all.

# Wording when the throttle refused to send because the minute's request was
# already spent and no slot freed within the wait. Nothing reached KvK.
_QUOTA_SPENT: str = (
    "KvK open data allows one request a minute from this server; this "
    "minute's was already used and no slot freed in time, so the request was "
    "not sent (HTTP 429 avoided)"
)

# GLEIF Registration Authority code for KvK (Netherlands Chamber of Commerce).
KVK_RA_CODE: str = "RA000463"

# Wording surfaced when the KvK number is not in the open-data set (HTTP 404).
# The HVD ``basisbedrijfsgegevens`` set only carries companies registered as a
# BV (besloten vennootschap) or NV (naamloze vennootschap) with registered
# business activity; other legal forms (eenmanszaak, VOF, stichting, vereniging,
# coöperatie, etc.) and some pure holding entities are simply not in the set and
# the API answers 404. That is a coverage limit of the open dataset, not a
# lookup failure — so we degrade to a note instead of an error card.
_COVERAGE_404: str = (
    "Not in the KvK open-data set. The KvK 'basisbedrijfsgegevens' High-Value "
    "Dataset only covers companies registered as a BV (besloten vennootschap) "
    "or NV (naamloze vennootschap) with registered business activity; other "
    "legal forms and some holding entities are not included and the API returns "
    "404 for them. This is a coverage limit of the open dataset, not a lookup "
    "error."
)


def normalise_kvk(kvk_number: str) -> str:
    """Normalise a KvK number: strip whitespace, zero-pad to 8 digits."""
    return kvk_number.strip().zfill(8)


class KvKAdapter(SourceAdapter):
    """Source adapter for KvK — Netherlands Chamber of Commerce open data."""

    id = "kvk"

    lookup_derivers = (
        LookupDeriver(frozenset({KVK_RA_CODE}), "kvk_number", normalise_kvk),
    )
    lookup_pass_legal_name = True
    remembers_legal_name = True


    def __init__(self) -> None:
        self._cache = Cache()
        # In-process name store: kvk_number -> legal_name.
        # KvK open data does not carry a company name, so we accept the
        # GLEIF-sourced name from app.py and cache it here for the lifetime
        # of the server process.  This ensures that _safe_deepen calls
        # (which happen immediately after /lookup within the same process)
        # can produce named BODS entity statements.
        self._names: dict[str, str] = {}
        # One in-flight request per KvK number. A lookup and the /deepen that
        # follows it (or two readers of the same card) used to send the same
        # request twice inside one minute — the second guaranteed a 429. Each
        # caller still reads the cache itself, so its own provenance scope
        # records what it was served.
        self._locks: dict[str, asyncio.Lock] = {}

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
            is_national_register=True,
            country="NL",
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

    async def fetch(
        self, hit_id: str, *, legal_name: str = "", remember_name: bool = True
    ) -> dict[str, Any]:
        """Return the KvK open-data record for a KvK number.

        ``hit_id`` must be an 8-digit KvK number (string, with or without
        leading zeros).  Pass ``legal_name`` to record the entity's name
        (from GLEIF) — it is stored in-process and included in subsequent
        fetches of the same number within the same server run.
        """
        kvk_number = normalise_kvk(hit_id)

        # Persist the name if provided — unless the caller asked not to.
        # ``/deepen`` passes ``remember_name=False``: a name supplied there is
        # the caller's, not GLEIF's, and must not reach the next reader
        # (Phase 326).
        if legal_name and remember_name:
            self._names[kvk_number] = legal_name
        name = legal_name or self._names.get(kvk_number, "")

        cache_key = f"{_CACHE_NS}/number/{kvk_number}"
        if self._cache.get_payload(cache_key) is None and self.info.live_available:
            # Wait for any fetch of the same number already in flight; when it
            # lands, the cache read below serves its answer.
            lock = self._locks.setdefault(kvk_number, asyncio.Lock())
            async with lock:
                return await self._fetch_unlocked(kvk_number, name, cache_key)
        return await self._fetch_unlocked(kvk_number, name, cache_key)

    async def _fetch_unlocked(
        self, kvk_number: str, name: str, cache_key: str
    ) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)

        if cached is not None:
            data = cached[0]
        elif not self.info.live_available:
            return {
                "source_id": self.id,
                "kvk_number": kvk_number,
                "company": None,
                "legal_name": name,
                "is_stub": True,
            }
        else:
            try:
                async with build_client() as client:
                    response = await client.get(
                        f"{_API_BASE}/basisbedrijfsgegevens/kvknummer/{kvk_number}"
                    )
            except HostRateLimitedError:
                return self._unanswered(kvk_number, name, None)
            except httpx.HTTPError as exc:
                return self._unanswered(kvk_number, name, type(exc).__name__)
            if response.status_code == 404:
                # The KvK number is not in the open-data set (BV/NV-only).
                # Cache the miss so we don't re-hit the 1-req/minute limit,
                # and degrade gracefully rather than surfacing a 404 error.
                data = None
            elif not 200 <= response.status_code < 300:
                # Phase 324: a 429 the throttle could not wait out, or a 5xx, used
                # to go through raise_for_status() and out of fetch() —
                # /deepen answered HTTP 500 (BODS quality sweep, 12 Oct 2026).
                # It is an unanswered question, never "no record": degrade,
                # return an empty bundle, cache nothing (the Phase 262 rule).
                return self._unanswered(kvk_number, name, f"HTTP {response.status_code}")
            else:
                try:
                    data = response.json()
                except ValueError:
                    return self._unanswered(kvk_number, name, "a non-JSON body")
            self._cache.put(cache_key, data)

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "kvk_number": kvk_number,
            "company": data,
            "legal_name": name,
            "is_stub": False,
        }
        if data is None:
            # Live lookup returned no record (404) — explain the coverage gap.
            bundle["coverage_note"] = _COVERAGE_404
        validate_raw("kvk", KvKBundle, bundle)
        return bundle

    def _unanswered(self, kvk_number: str, name: str, failure: str | None) -> dict[str, Any]:
        """Record that KvK did not answer and return an empty, uncached bundle.

        ``failure`` is ``None`` when the throttle refused to send at all — the
        quota was spent, which is a rate limit, not an upstream fault. The
        detail names the failure only, never the KvK number.
        """
        if failure is None:
            degradation.record(
                self.id, _QUOTA_SPENT, reason=degradation.REASON_RATE_LIMITED
            )
        else:
            degradation.record(
                self.id,
                f"KvK open data did not answer ({failure})",
                reason=degradation.reason_for_failure(failure),
            )
        return {
            "source_id": self.id,
            "kvk_number": kvk_number,
            "company": None,
            "legal_name": name,
            "is_stub": True,
        }
