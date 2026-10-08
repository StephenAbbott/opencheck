"""``GET /leads`` — leads for a retired LEI that names no successor (Phase 308).

UI-only, like ``/subsidiaries/declared``: nothing here enters the lookup
stream, the replay cache, a saved report, the MCP profile or the exports.
The identity band asks for it only when the profile says the company has
ended and GLEIF names no successor, and a saved report never asks.

The name is a query parameter rather than a server-side read of the anchor
because the band already holds it and a second anchor resolution would
spend a lookup; it goes only into GLEIF's fulltext search, as ``/search``'s
query does. The call is discretionary (Phase 234) — refused when the
reserve for lookups is short, which the response says.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from .. import identifiers
from ..gleif_throttle import discretionary as gleif_discretionary
from ..leads import assemble_leads
from ..ratelimit import default_tier, limiter

router = APIRouter()

_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


class LeadParent(BaseModel):
    lei: str
    name: str | None = None
    entity_status: str | None = None
    registration_status: str | None = None
    relationship_status: str | None = None
    #: Whether GLEIF still serves the relationship. A retired record's
    #: relationships are usually retired with it, so this is often False.
    standing: bool


class LeadCandidate(BaseModel):
    lei: str
    name: str
    jurisdiction: str | None = None
    registration_status: str | None = None
    #: ``search_rank.MATCH_TIERS`` word for the name: exact / same_name /
    #: all_tokens / distinctive_tokens. Never fuzzy.
    match: str
    #: The other name GLEIF files that the match rests on — a former legal
    #: name, most often — and GLEIF's type for it; both null when the legal
    #: name itself matched.
    matched_name: str | None = None
    matched_name_type: str | None = None
    #: Always True: only the name agrees. Carried so no consumer can forget.
    name_only: bool = True


class LeadsResponse(BaseModel):
    lei: str
    searched_name: str | None = None
    #: The direct parent the Golden Copy mirror last held for the record, or
    #: null — no mirror, or none filed.
    parent: LeadParent | None = None
    #: Active LEI records whose name matches, best tier first, at most five.
    candidates: list[LeadCandidate]
    #: Why GLEIF could not be searched (``held_for_lookups`` /
    #: ``rate_limited`` / ``unreachable``), or null when it answered — an
    #: empty list with null here means GLEIF found nothing similar.
    gleif_unavailable_reason: str | None = None
    #: The sentence every consumer must keep beside the list.
    note: str


@router.get("/leads", response_model=LeadsResponse)
@limiter.limit(default_tier)
async def leads(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    name: str = Query("", max_length=300, description="The subject's legal name, for the similar-name search."),
) -> Any:
    """Leads for a retired LEI that names no successor — never a successor."""
    norm_lei = lei.strip().upper()
    if not _LEI_SHAPE.match(norm_lei):
        raise HTTPException(
            status_code=400,
            detail=(
                f"{norm_lei!r} is not a valid LEI. ISO 17442 LEIs are 20-character "
                "alphanumeric strings (e.g. 7LTWFZYICNSX8D621K86)."
            ),
        )
    check_digit_error = identifiers.lei_check_digit_error(norm_lei)
    if check_digit_error:
        raise HTTPException(status_code=400, detail=check_digit_error)
    response.headers["Cache-Control"] = "public, max-age=3600"
    with gleif_discretionary():
        return await assemble_leads(norm_lei, name)
