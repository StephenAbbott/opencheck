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
import logging
from typing import Any
from urllib.parse import quote

import httpx

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..identifiers import ch_identification_is_uk, normalise_ch_company_number
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.companies_house import CHBundle, CHOfficerBundle

_API_BASE = "https://api.company-information.service.gov.uk"
_CACHE_NS = "companies_house"

log = logging.getLogger(__name__)

# Phase 177: an upper bound on how many related companies one lookup will
# pull in, whatever the depth. A holding stack is a chain, so this is rarely
# approached; it exists so a subject with several corporate PSCs per layer
# cannot turn one lookup into an unbounded crawl of the register.
_MAX_RELATED_COMPANIES = 25

# Why a corporate PSC was not followed up the chain. Carried on the bundle
# (``unfollowed_pscs``) so the report can say the chain is truncated by what
# was filed, not by where the structure ends; the two register-side reasons
# are also recorded as degradation so the coverage line reflects them.
_SKIP_NOT_UK = "not_uk_registered"
_SKIP_BAD_NUMBER = "registration_number_not_a_company_number"
_SKIP_DEPTH = "max_depth_reached"
_SKIP_CAP = "related_companies_cap_reached"
_SKIP_FETCH_FAILED = "fetch_failed"


def _slug(text: str) -> str:
    """Cache-safe slug for a free-text query."""
    digest = hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]
    return digest


def _note_unfollowed(
    unfollowed: list[dict[str, Any]],
    subject_number: str,
    psc: dict[str, Any],
    reason: str,
) -> None:
    """Record a corporate PSC the walk did not follow, with the filed
    particulars and the reason, and log it so the residue is countable."""
    ident = psc.get("identification") or {}
    filed = (ident.get("registration_number") or "").strip()
    unfollowed.append(
        {
            "subject_company_number": subject_number,
            "name": psc.get("name"),
            "registration_number": filed,
            "country_registered": ident.get("country_registered"),
            "place_registered": ident.get("place_registered"),
            "reason": reason,
        }
    )
    log.info(
        "companies_house: corporate PSC of %s not followed (%s): "
        "registration_number=%r country_registered=%r",
        subject_number,
        reason,
        filed,
        ident.get("country_registered"),
    )


def _looks_like_company_number(value: str) -> bool:
    """True for the 8-character alphanumeric Companies House number shape.

    UK company numbers are 8 chars, alphanumeric (e.g. ``00102498``,
    ``SC123456``, ``OC403762``). Officer ids are base64-shaped and
    longer (typically 27 chars) — no overlap.
    """
    return len(value) == 8 and value.replace(" ", "").isalnum()


class CompaniesHouseAdapter(SourceAdapter):
    id = "companies_house"

    # Dispatched via the GB jurisdiction special case in the lookup pipeline.
    lookup_dispatch_keys = ("gb_coh",)


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
            is_national_register=True,
            country="GB",
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
        visited: set[str] = set()
        related: dict[str, dict[str, Any]] = {}
        unfollowed: list[dict[str, Any]] = []
        max_depth = max(0, int(get_settings().ch_psc_max_depth))
        root = await self._fetch_company_data(
            number,
            visited=visited,
            related=related,
            unfollowed=unfollowed,
            depth=0,
            max_depth=max_depth,
        )
        root["related_companies"] = related
        root["unfollowed_pscs"] = unfollowed
        self._record_unfollowed(unfollowed, max_depth)
        validate_raw("companies_house", CHBundle, root)
        return root

    @staticmethod
    def _record_unfollowed(unfollowed: list[dict[str, Any]], max_depth: int) -> None:
        """Turn the register-side reasons a chain was cut short into degradation
        notes (count-free, name-free — the bundle carries the particulars).

        A non-UK registrant is not a degradation: Companies House does not hold
        it, and the report's jurisdiction chips already say so. A filed number
        that is not a company number, the depth cap and the fan-out cap are
        OpenCheck's limits, so they are reported as such.
        """
        reasons = {u["reason"] for u in unfollowed}
        if _SKIP_BAD_NUMBER in reasons:
            degradation.record(
                "companies_house",
                (
                    "a corporate PSC filed as UK-registered carries a registration "
                    "number that is not a Companies House number, so the chain "
                    "above it was not followed"
                ),
            )
        if _SKIP_DEPTH in reasons:
            degradation.record(
                "companies_house",
                (
                    f"the UK corporate-PSC chain was followed to the configured "
                    f"depth of {max_depth} hops and further layers exist above it"
                ),
            )
        if _SKIP_CAP in reasons:
            degradation.record(
                "companies_house",
                (
                    f"the UK corporate-PSC chain was capped at "
                    f"{_MAX_RELATED_COMPANIES} related companies per lookup"
                ),
            )
        if _SKIP_FETCH_FAILED in reasons:
            degradation.record(
                "companies_house",
                "a UK corporate PSC's own register record could not be fetched",
            )

    async def _fetch_company_data(
        self,
        number: str,
        *,
        visited: set[str],
        related: dict[str, dict[str, Any]],
        unfollowed: list[dict[str, Any]],
        depth: int,
        max_depth: int,
    ) -> dict[str, Any]:
        """Recursively fetch a company bundle, following UK corporate PSC chains.

        Up to *max_depth* hops are followed (``OPENCHECK_CH_PSC_MAX_DEPTH``,
        default 6). Already-visited company numbers are skipped to break
        cycles. Only active (not ``ceased_on``) corporate / legal-person PSCs
        whose identification block says the UK register are followed, and
        their filed ``registration_number`` is normalised to the canonical
        eight-character Companies House number first — PSC filings routinely
        drop leading zeros (``2999029`` for ``02999029``), and until Phase
        177 that shape failed an "exactly 8 characters" gate, so the walk
        silently stopped at the first hop.

        Every corporate PSC that is *not* followed is appended to
        ``unfollowed`` with a reason, so the truncation is visible rather than
        indistinguishable from the top of the structure.
        """
        visited.add(number)
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
        # PSC *statements* ("no PSC exists", "PSC not yet identified", …) are on a
        # separate endpoint that 404s when the company has filed none.
        try:
            psc_statements = await self._get(
                f"/company/{number}/persons-with-significant-control-statements",
                cache_key=f"{_CACHE_NS}/company/{number}/psc-statements",
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                psc_statements = {"items": []}
            else:
                raise

        for psc in pscs.get("items") or []:
            if psc.get("ceased_on"):
                continue
            kind = (psc.get("kind") or "").lower()
            if "corporate" not in kind and "legal-person" not in kind:
                continue
            ident = psc.get("identification") or {}
            filed = (ident.get("registration_number") or "").strip()

            if not ch_identification_is_uk(ident):
                _note_unfollowed(unfollowed, number, psc, _SKIP_NOT_UK)
                continue
            reg_no = normalise_ch_company_number(filed)
            if reg_no is None:
                _note_unfollowed(unfollowed, number, psc, _SKIP_BAD_NUMBER)
                continue
            if reg_no in visited or reg_no in related:
                continue  # a cycle or a shared parent: already in the bundle
            if depth >= max_depth:
                _note_unfollowed(unfollowed, number, psc, _SKIP_DEPTH)
                continue
            if len(related) >= _MAX_RELATED_COMPANIES:
                _note_unfollowed(unfollowed, number, psc, _SKIP_CAP)
                continue
            # Only recurse if we have cached data or live is available.
            sub_key = f"{_CACHE_NS}/company/{reg_no}"
            if not self.info.live_available and not self._cache.has(sub_key):
                continue
            try:
                sub = await self._fetch_company_data(
                    reg_no,
                    visited=visited,
                    related=related,
                    unfollowed=unfollowed,
                    depth=depth + 1,
                    max_depth=max_depth,
                )
            except httpx.HTTPError as exc:
                # A parent whose record 404s (a mis-filed number that still
                # normalised) or a transient failure must not sink the
                # subject's own bundle; the gap is recorded instead.
                log.warning(
                    "companies_house: could not fetch related company %s (PSC of %s): %s",
                    reg_no,
                    number,
                    exc,
                )
                _note_unfollowed(unfollowed, number, psc, _SKIP_FETCH_FAILED)
                continue
            related[reg_no] = sub

        return {
            "source_id": self.id,
            "company_number": number,
            "profile": profile,
            "officers": officers,
            "pscs": pscs,
            "psc_statements": psc_statements,
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
        bundle = {
            "source_id": self.id,
            "officer_id": officer_id,
            "appointments": appointments,
        }
        validate_raw("companies_house", CHOfficerBundle, bundle)
        return bundle

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
