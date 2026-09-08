"""Subsidiary-network endpoint — GLEIF direct + ultimate children (lazy).

``GET /subsidiaries?lei=<LEI>`` returns a count-first summary of the subject's
direct and ultimate children (with a render-mode hint); ``?format=bods`` also
returns the BODS statements for the graph / export. Never on the main lookup.

Always 200, including when GLEIF refuses: a rate-limited network is reported
as ``children_available: false`` with a ``degraded_detail`` sentence, never as
an empty list (Phase 146, extending the Phase 145 ``/securities`` pattern).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from .. import identifiers
from ..ratelimit import default_tier, limiter
from ..subsidiaries import assemble_subsidiaries
from ..subsidiaries_declared import assemble_declared

router = APIRouter()

_LEI_SHAPE = identifiers.LEI_PATH_SHAPE  # + check digits below when enforced


class SubChild(BaseModel):
    lei: str
    name: str | None = None
    jurisdiction: str | None = None
    status: str | None = None
    relation: str  # "direct" | "ultimate" | "both"
    link: str | None = None


class SubJurisdiction(BaseModel):
    code: str
    count: int


class SubsidiariesResponse(BaseModel):
    lei: str
    available: bool
    reason: str | None = None
    #: Phase 146. False = GLEIF refused both relation calls and no snapshot
    #: stood in, so an empty ``children`` list says nothing about the entity.
    #: The frontend must render the degraded notice, not "no network published".
    children_available: bool = True
    direct_available: bool = True
    ultimate_available: bool = True
    #: Children served from the entity-pages Golden Copy, not live.
    snapshot_fallback: bool = False
    snapshot_date: str | None = None
    #: Phase 179: why the snapshot answered — "mirror" (the mirror-first
    #: order chose it; nothing was refused) or "fallback" (GLEIF refused and
    #: the snapshot stood in); None when the network came live.
    snapshot_source: str | None = None
    #: One sentence naming what GLEIF did not answer (None when it answered).
    degraded_detail: str | None = None
    direct_total: int = 0
    ultimate_total: int = 0
    distinct_fetched: int = 0
    indirect_only: int = 0
    node_estimate: int = 0
    render_mode: str = "graph"  # "graph" | "table"
    truncated: bool = False
    jurisdictions: list[SubJurisdiction] = []
    children: list[SubChild] = []
    bods: list[dict] | None = None


@router.get("/subsidiaries", response_model=SubsidiariesResponse)
@limiter.limit(default_tier)
async def subsidiaries(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    format: str = Query("summary", description="'summary' or 'bods' (adds BODS statements)."),
) -> Any:
    """The subject's GLEIF subsidiary network (direct + ultimate children)."""
    norm = lei.strip().upper()
    if not _LEI_SHAPE.match(norm):
        raise HTTPException(
            status_code=400,
            detail=f"{norm!r} is not a valid LEI (20-character alphanumeric).",
        )
    check_digit_error = identifiers.lei_check_digit_error(norm)
    if check_digit_error:
        raise HTTPException(status_code=400, detail=check_digit_error)
    return await assemble_subsidiaries(norm, include_bods=(format == "bods"))


# ---------------------------------------------------------------------------
# Phase 185 — the non-GLEIF lists, for the Subsidiaries tab
# ---------------------------------------------------------------------------


class DeclaredRow(BaseModel):
    name: str
    #: Only where the source's own data carries one — never name-derived here.
    lei: str | None = None
    #: ISO 3166-1 alpha-3, as MEIP, EITI and GEM all publish it.
    country: str | None = None
    #: "direct" | "in group" (MEIP, via an intermediate) | "declared" (EITI).
    relation: str | None = None
    percent: float | None = None
    years: list[str] = []
    #: MEIP: the immediate parent's name. GEM: the GEM entity id.
    via: str | None = None


class DeclaredSource(BaseModel):
    id: str
    label: str
    measures: str
    homepage: str
    #: False = the data could not be read (not a finding about the company).
    available: bool = True
    #: False = the subject is not in this source's universe at all.
    covered: bool = False
    reason: str | None = None
    #: The source's own count where it publishes one larger than ``rows``
    #: (MEIP holds only the LEI-carrying subset of an MNE's subsidiaries).
    total: int | None = None
    listed: int = 0
    with_lei: int = 0
    context: dict | None = None
    rows: list[DeclaredRow] = []


class DeclaredSubsidiariesResponse(BaseModel):
    lei: str
    sources: list[DeclaredSource]
    covered: int
    listed: int
    with_lei: int


@router.get("/subsidiaries/declared", response_model=DeclaredSubsidiariesResponse)
@limiter.limit(default_tier)
async def declared_subsidiaries(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
) -> Any:
    """The subsidiary lists OpenCheck holds from sources other than GLEIF
    (OECD-UNSD MEIP, EITI, Global Energy Monitor), kept apart per source.

    Serves the Subsidiaries tab. Not part of the documented API: the
    reusable subsidiary network is ``GET /subsidiaries`` (GLEIF Level 2).
    """
    norm = lei.strip().upper()
    if not _LEI_SHAPE.match(norm):
        raise HTTPException(
            status_code=400,
            detail=f"{norm!r} is not a valid LEI (20-character alphanumeric).",
        )
    check_digit_error = identifiers.lei_check_digit_error(norm)
    if check_digit_error:
        raise HTTPException(status_code=400, detail=check_digit_error)
    return await assemble_declared(norm)
