"""``GET /daily-changes`` — every LEI GLEIF or OpenSanctions changed, by day.

The "Daily deltas for OpenCheck" ticket, in shadow mode: JSON only, no page,
no downloadable BODS file. ``opencheck/daily_changes.py`` computes the list
from the two delta feeds; these routes only read its store. Nothing here runs
a lookup or calls an upstream.

The path is ``/daily-changes`` rather than ``/changes`` so a later page for
readers can take the shorter one.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response

from .. import daily_changes
from ..ratelimit import default_tier, limiter

router = APIRouter(tags=["daily-changes"])

_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
#: A complete day does not change; today's list grows with each delta.
_CACHE_COMPLETE = "public, max-age=3600"
_CACHE_OPEN = "no-store"
_OFF = "The daily changes list is not enabled on this server."


@router.get("/daily-changes")
@limiter.limit(default_tier)
async def daily_changes_index(request: Request, response: Response) -> dict[str, Any]:
    """The days held, newest first, with each day's counts by band and source."""
    payload = await asyncio.to_thread(daily_changes.index_payload)
    if not payload.get("available"):
        raise HTTPException(status_code=503, detail=_OFF)
    response.headers["Cache-Control"] = _CACHE_OPEN
    return payload


@router.get("/daily-changes/{day}.json")
@limiter.limit(default_tier)
async def daily_changes_day(
    request: Request,
    response: Response,
    day: str,
    band: list[str] | None = Query(  # noqa: B008
        default=None,
        description=(
            "Bands to include; repeatable. Default: headline, ceased, ceased_lapsed, record. "
            "`all` serves every band, including note-only changes and new LEIs."
        ),
    ),
) -> dict[str, Any]:
    """One UTC day's list. A day with nothing on it is answered with no rows."""
    if not _DAY.match(day):
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD")
    wanted: list[str] | None = None
    if band:
        if "all" in band:
            wanted = list(daily_changes.BANDS)
        else:
            unknown = sorted(set(band) - set(daily_changes.BANDS))
            if unknown:
                raise HTTPException(
                    status_code=400,
                    detail=f"unknown band; choose from {', '.join(daily_changes.BANDS)} or all",
                )
            wanted = [b for b in daily_changes.BANDS if b in band]
    payload = await asyncio.to_thread(daily_changes.day_payload, day, wanted)
    if payload is None:
        raise HTTPException(status_code=503, detail=_OFF)
    response.headers["Cache-Control"] = _CACHE_COMPLETE if payload["complete"] else _CACHE_OPEN
    return payload
