"""``GET /daily-changes`` — every LEI GLEIF or OpenSanctions changed, by day.

The "Daily deltas for OpenCheck" ticket, in shadow mode: JSON only, no page,
no downloadable BODS file. ``opencheck/daily_changes.py`` computes the list
from the two delta feeds; these routes only read its store. Nothing here runs
a lookup or calls an upstream.

The JSON lives at ``/daily-changes``; ``/changes`` is the readers' page
(the SPA), and ``/changes/<day>.jsonl.gz`` / ``/changes/latest.jsonl.gz`` /
``/changes/<day>.csv`` are the files it offers — registered here, before the
SPA fallback, which would otherwise answer them with the page.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, RedirectResponse

from .. import daily_changes, daily_changes_file
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


#: A complete day's file is final; today's is rebuilt after each delta.
_CACHE_FILE_COMPLETE = "public, max-age=86400"
_FILE = re.compile(r"^(?P<day>\d{4}-\d{2}-\d{2}|latest)\.(?P<ext>jsonl\.gz|csv)$")


def _today() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).strftime("%Y-%m-%d")


@router.get("/changes/{name}", include_in_schema=True, response_model=None)
@limiter.limit(default_tier)
async def daily_changes_file_route(
    request: Request, response: Response, name: str
) -> Any:
    """One day's changes as a file: ``<day>.jsonl.gz`` (BODS v0.4, GLEIF only,
    every band) or ``<day>.csv`` (both sources, every band). ``latest``
    redirects to the newest day held."""
    m = _FILE.match(name)
    if not m:
        raise HTTPException(status_code=404, detail="not a changes file")
    if daily_changes_file.files_dir() is None:
        raise HTTPException(status_code=503, detail=_OFF)
    day, ext = m.group("day"), m.group("ext")
    if day == "latest":
        held = await asyncio.to_thread(daily_changes_file.held_days)
        if not held:
            raise HTTPException(status_code=404, detail="no changes file is held yet")
        return RedirectResponse(
            f"/changes/{held[0]}.{ext}", status_code=302, headers={"Cache-Control": _CACHE_OPEN}
        )
    cache = _CACHE_FILE_COMPLETE if day < _today() else _CACHE_OPEN
    if ext == "csv":
        payload = await asyncio.to_thread(
            daily_changes.day_payload, day, list(daily_changes.BANDS)
        )
        if payload is None:
            raise HTTPException(status_code=503, detail=_OFF)
        body = daily_changes_file.csv_bytes(payload["rows"], day)
        return Response(
            content=body,
            media_type="text/csv; charset=utf-8",
            headers={
                "Cache-Control": cache,
                "Content-Disposition": f'attachment; filename="opencheck-changes-{day}.csv"',
            },
        )
    path = daily_changes_file.day_path(day)
    if path is None or not path.exists():
        raise HTTPException(status_code=404, detail="no BODS file for that day")
    return FileResponse(
        path,
        media_type="application/gzip",
        filename=f"opencheck-changes-{day}.jsonl.gz",
        headers={"Cache-Control": cache},
    )
