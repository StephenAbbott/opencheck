"""``GET /resolve-national-id`` — a register number and a country in, LEIs out.

Split out of ``routers/lookup.py`` in Phase 246, unchanged. It is the
endpoint behind the National ID search tab and the MCP
``opencheck_resolve_national_id`` tool; it never runs a lookup, only GLEIF's
reverse filters (``registeredAs`` / ``validatedAs`` / other validation
authorities), scoped by ``ra_codes.ra_code_for(country, number)``.
``routers/lookup.py`` re-exports its names.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from .. import identifiers
from ..gleif_throttle import GleifRateLimitedError, discretionary as gleif_discretionary
from ..ra_codes import RA_BY_COUNTRY, ra_code_for
from ..ratelimit import default_tier, limiter
from ..sources import REGISTRY

router = APIRouter()


# The country → RA code map and the sub-registry prefix rules now live together
# in ``opencheck.ra_codes``; this name is re-exported because callers and tests
# reference it. Phase 141: the map used to live here and the prefix rules in
# ``routers/search.py``, and this endpoint consulted only the map — so
# ``country="GB"`` scoped a Scottish or Northern Irish number to the England &
# Wales authority and returned nothing. Use ``ra_code_for(country, number)``
# rather than reading this map directly.
_RA_BY_COUNTRY = RA_BY_COUNTRY


class NationalIdMatch(BaseModel):
    """One LEI record carrying the queried national registration number."""

    lei: str
    name: str
    jurisdiction: str | None = None


class ResolveNationalIdResponse(BaseModel):
    """LEIs that carry a given national company-registration number."""

    number: str
    country: str | None = None
    ra_code: str | None = None
    matches: list[NationalIdMatch]
    # Advisory only (Phase A, rigour adoption): set when the number fails its
    # national scheme's check digit — the query still runs, this just explains
    # an otherwise-mystifying empty result. None when no validator applies.
    checksum_warning: str | None = None


@router.get("/resolve-national-id", response_model=ResolveNationalIdResponse)
@limiter.limit(default_tier)
async def resolve_national_id(
    request: Request,
    response: Response,
    number: str = Query(
        ...,
        min_length=1,
        description="National company-registration number, e.g. a UK Companies House number.",
    ),
    country: str = Query(
        "",
        description="ISO 3166-1 alpha-2 country code (e.g. 'GB'); resolved to a GLEIF RA code.",
    ),
    ra_code: str = Query(
        "",
        description="GLEIF Registration Authority code (e.g. 'RA000585'); overrides 'country' when set.",
    ),
) -> ResolveNationalIdResponse:
    """Resolve a local company-registration number to its LEI(s) via GLEIF.

    The inverse of OpenCheck's normal LEI-first flow: a caller who has a
    national registry number (but not the LEI) obtains it here, then feeds the
    LEI to ``/lookup``. Queries GLEIF's three local-id filter fields and
    de-duplicates by LEI. The RA code — resolved from ``country`` when not given
    explicitly — scopes the search to one registry, avoiding false matches from
    coincidental number collisions across jurisdictions.
    """
    return await _resolve_national_id_impl(number=number, country=country, ra_code=ra_code)


async def _resolve_national_id_impl(
    number: str, country: str = "", ra_code: str = ""
) -> ResolveNationalIdResponse:
    """Body of ``/resolve-national-id``, callable in-process (MCP tool)
    without going through the rate-limited route."""
    num = number.strip()
    # The number, not just the country, decides the authority: Companies House
    # files Scottish and Northern Irish companies under RA000587 and RA000586
    # rather than the England & Wales RA000585 that "GB" maps to. Reading the
    # country map alone — which this did until Phase 141 — scoped an SC number
    # to the wrong registry and returned an empty result, which reads as an
    # absent company rather than a bad query. An explicit ra_code still wins.
    code = (ra_code or ra_code_for(country, num)).strip()

    # Advisory check-digit validation (never blocks the query — the registry
    # is the authority): only for countries whose GLEIF registry number is
    # unambiguously a python-stdnum scheme (see identifiers.py).
    checksum_warning = identifiers.national_id_checksum_warning(country, num)

    adapter = REGISTRY.get("gleif")
    if adapter is None or not hasattr(adapter, "search_by_local_id"):
        raise HTTPException(status_code=503, detail="GLEIF adapter unavailable")

    # Phase 234: resolving a number is discretionary GLEIF traffic — refused
    # outright when the window is nearly spent, so the anchor resolution of a
    # lookup already under way is never the call that waits.
    try:
        with gleif_discretionary():
            hits = await adapter.search_by_local_id(num, code)
    except GleifRateLimitedError as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "GLEIF is busy answering lookups right now, so the number could "
                "not be resolved. Try again in a few seconds."
            ),
            headers={"Retry-After": "10"},
        ) from exc
    matches: list[NationalIdMatch] = []
    for h in hits:
        if not h.hit_id:
            continue
        jurisdiction = None
        if isinstance(h.raw, dict):
            entity = ((h.raw.get("attributes") or {}).get("entity") or {})
            jurisdiction = entity.get("jurisdiction")
        matches.append(
            NationalIdMatch(lei=h.hit_id, name=h.name, jurisdiction=jurisdiction)
        )

    return ResolveNationalIdResponse(
        number=num,
        country=(country.strip().upper() or None),
        ra_code=(code or None),
        matches=matches,
        checksum_warning=checksum_warning,
    )
