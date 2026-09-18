"""``GET /knowability`` — what each jurisdiction publishes and who can see it.

Phase 223. Pure and cacheable: it reads ``data/jurisdictions.json`` (Stephen's
Notion table, synced) and the adapter registry, never a source. FullCheck
will call it with the jurisdictions on an ownership path after each
``/expand-layer``; the entity-page build and the batch page read it the same
way. Unknown codes are answered with a stated-absence statement, never a 404 —
"OpenCheck holds no register notes for XX" is itself the fact the reader
needs.
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel

from ..knowability import GENERATED_AT, JURISDICTIONS, KnowabilityStatement, statements_for
from ..ratelimit import default_tier, limiter

router = APIRouter(tags=["knowability"])

_MAX_CODES = 40


class KnowabilityResponse(BaseModel):
    statements: list[KnowabilityStatement]
    #: Every code the table holds — so a client can tell "not in the table"
    #: from "not asked for".
    known_codes: list[str]
    generated_at: str | None
    as_of: str


@router.get("/knowability", response_model=KnowabilityResponse)
@limiter.limit(default_tier)
async def knowability(
    request: Request,
    response: Response,
    jurisdictions: str = Query(
        ...,
        description="Comma-separated ISO 3166-1 alpha-2 codes (US-DE style subdivisions accepted), subject first.",
        min_length=2,
        max_length=_MAX_CODES * 6,
    ),
) -> KnowabilityResponse:
    codes = [c.strip().upper() for c in jurisdictions.split(",") if c.strip()][:_MAX_CODES]
    today = date.today()
    response.headers["Cache-Control"] = "public, max-age=3600"
    return KnowabilityResponse(
        statements=statements_for(codes, today),
        known_codes=sorted(JURISDICTIONS),
        generated_at=GENERATED_AT,
        as_of=today.isoformat(),
    )
