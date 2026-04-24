"""FastAPI entry point for OpenCheck.

Phase 0 surface:

* ``GET /health`` — liveness probe.
* ``GET /sources`` — inventory of registered source adapters with live/stub status.
* ``GET /search?q=<query>&kind=<entity|person>`` — fan-out across adapters
  returning stub hits. Phase 1+ wires real adapters into this same shape.

No streaming yet — Phase 1 introduces the SSE endpoint that the chat UI
subscribes to.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from . import __version__
from .config import get_settings
from .sources import REGISTRY, SearchKind, SourceHit, SourceInfo

app = FastAPI(
    title="OpenCheck",
    version=__version__,
    description=(
        "Chatbot-style corporate intelligence over open data. "
        "Maps every source into BODS v0.4."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[get_settings().cors_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class HealthResponse(BaseModel):
    status: str
    version: str
    allow_live: bool


class SourcesResponse(BaseModel):
    sources: list[SourceInfo]


class SearchResponse(BaseModel):
    query: str
    kind: SearchKind
    hits: list[SourceHit]
    errors: dict[str, str]


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        version=__version__,
        allow_live=settings.allow_live,
    )


@app.get("/sources", response_model=SourcesResponse)
async def sources() -> SourcesResponse:
    return SourcesResponse(sources=[adapter.info for adapter in REGISTRY.values()])


@app.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(..., min_length=1, description="Search query."),
    kind: SearchKind = Query(
        SearchKind.ENTITY, description="entity or person"
    ),
) -> SearchResponse:
    """Fan-out search across registered adapters.

    Phase 0 returns stub hits only. Each adapter runs concurrently; one
    adapter failing does not break the response.
    """

    tasks = {
        source_id: asyncio.create_task(adapter.search(q, kind))
        for source_id, adapter in REGISTRY.items()
        if kind in adapter.info.supports
    }

    hits: list[SourceHit] = []
    errors: dict[str, str] = {}

    results: dict[str, Any] = {}
    for source_id, task in tasks.items():
        try:
            results[source_id] = await task
        except Exception as exc:  # noqa: BLE001 — surface the adapter error to the caller
            errors[source_id] = f"{type(exc).__name__}: {exc}"
            results[source_id] = []

    for adapter_hits in results.values():
        hits.extend(adapter_hits)

    return SearchResponse(query=q, kind=kind, hits=hits, errors=errors)
