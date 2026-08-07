"""Health and sources endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .. import __version__, memwatch
from ..bo_access import notice_for
from ..config import get_settings
from ..sources import REGISTRY, SourceInfo

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    version: str
    allow_live: bool


class SourcesResponse(BaseModel):
    sources: list[SourceInfo]


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        version=__version__,
        allow_live=settings.allow_live,
    )


@router.get("/memstats")
async def memstats() -> JSONResponse:
    """Aggregate memory + traffic counters since the last deploy.

    Public and unauthenticated by design: it powers the weekly scheduled
    check that decides whether crawler traffic on ``/og`` justifies moving
    the entity pages to a static generic og:image (see the memwatch module
    doc). Contains ONLY aggregate counts per path bucket and the process
    memory figures — no IPs, User-Agents, LEIs or query strings, matching
    the GoatCounter path-bucket privacy contract. Undecorated (no rate
    limit), like /health: it is a single dict dump, cheap by construction.
    """
    return JSONResponse(memwatch.stats(), headers={"Cache-Control": "no-store"})


@router.get("/sources", response_model=SourcesResponse)
async def sources() -> SourcesResponse:
    # Attach the computed EU/EEA beneficial-ownership access notice per register.
    # Adapters declare only the static `country`; the (date-dependent) notice is
    # computed here so it flips on the restriction date without a code change.
    out: list[SourceInfo] = []
    for adapter in REGISTRY.values():
        info = adapter.info
        notice = notice_for(info.country)
        out.append(info.model_copy(update={"bo_access": notice}) if notice else info)
    return SourcesResponse(sources=out)
