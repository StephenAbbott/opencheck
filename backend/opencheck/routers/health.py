"""Health and sources endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from .. import __version__
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
    return SourcesResponse(sources=[adapter.info for adapter in REGISTRY.values()])
