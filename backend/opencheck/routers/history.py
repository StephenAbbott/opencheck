"""History (Time Machine) endpoint — a per-entity timeline of notable changes.

``GET /history?lei=<LEI>`` returns the de-duplicated, notable-only timeline
(GLEIF + Companies House where available). ``?include_noise=true`` additionally
returns every raw classified change, including administrative noise.

Lazy by design — fetched by the frontend on demand, never on the main lookup.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from ..ratelimit import default_tier, limiter
from ..timeline.service import fetch_timeline

router = APIRouter()

_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


class HistoryEntry(BaseModel):
    change_type: str
    label: str
    tier: int
    record_type: str
    date: str | None
    date_basis: str
    date_confidence: str
    value_old: str | None = None
    value_new: str | None = None
    sources: list[str]
    corroborating_sources: list[str] = []
    counterparty: str | None = None
    interest_start_date: str | None = None
    interest_end_date: str | None = None
    boosted: bool = False


class RawChange(BaseModel):
    source_id: str
    record_type: str
    raw_change_type: str
    raw_field: str | None = None
    value_old: str | None = None
    value_new: str | None = None
    change_type: str | None = None
    tier: int
    event_date: str | None = None
    date_basis: str


class HistoryResponse(BaseModel):
    lei: str
    company_number: str | None
    available: bool
    sources: list[str]
    notable_count: int
    notable: list[HistoryEntry]
    events: list[RawChange] = []


@router.get("/history", response_model=HistoryResponse)
@limiter.limit(default_tier)
async def history(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    include_noise: bool = Query(
        False, description="Also return every raw change, including Tier-3 noise."
    ),
) -> Any:
    """A simplified timeline of notable ownership / identity changes for an LEI."""
    norm_lei = lei.strip().upper()
    if not _LEI_SHAPE.match(norm_lei):
        raise HTTPException(
            status_code=400,
            detail=(
                f"{norm_lei!r} is not a valid LEI. ISO 17442 LEIs are 20-character "
                "alphanumeric strings (e.g. 213800IN6LSRGTZSOS29)."
            ),
        )

    tl = await fetch_timeline(norm_lei)

    notable = [
        HistoryEntry(
            change_type=e.change_type.value,
            label=e.label,
            tier=e.tier.value,
            record_type=e.record_type.value,
            date=e.date,
            date_basis=e.date_basis.value,
            date_confidence=e.date_confidence.value,
            value_old=e.primary.value_old,
            value_new=e.primary.value_new,
            sources=e.sources,
            corroborating_sources=sorted({c.source_id for c in e.corroborating}),
            counterparty=e.counterparty,
            interest_start_date=e.interest_start_date,
            interest_end_date=e.interest_end_date,
            boosted=e.boosted,
        )
        for e in tl.notable
    ]

    events: list[RawChange] = []
    if include_noise:
        events = [
            RawChange(
                source_id=ev.source_id,
                record_type=ev.record_type.value,
                raw_change_type=ev.raw_change_type,
                raw_field=ev.raw_field,
                value_old=ev.value_old,
                value_new=ev.value_new,
                change_type=ev.change_type.value if ev.change_type else None,
                tier=ev.tier.value,
                event_date=ev.event_date,
                date_basis=ev.date_basis.value,
            )
            for ev in tl.events
        ]

    return HistoryResponse(
        lei=norm_lei,
        company_number=tl.company_number,
        available=bool(tl.notable or tl.events),
        sources=sorted({ev.source_id for ev in tl.events}),
        notable_count=len(notable),
        notable=notable,
        events=events,
    )
