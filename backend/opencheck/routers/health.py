"""Health and sources endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from .. import __version__
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
