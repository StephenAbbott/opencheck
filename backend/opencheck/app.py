"""FastAPI entry point for OpenCheck.

Surface (Phase 1):

* ``GET /health`` — liveness probe.
* ``GET /sources`` — inventory of registered source adapters with live/stub status.
* ``GET /search?q=<query>&kind=<entity|person>`` — fan-out search, returns all hits at once.
* ``GET /stream?q=<query>&kind=<entity|person>`` — same fan-out, streamed as SSE.
* ``GET /deepen?source=<id>&hit_id=<id>`` — "Go deeper" on a specific hit.
  Returns the full raw payload plus BODS v0.4 statements.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from . import __version__
from .bods import map_companies_house, validate_shape
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


class DeepenResponse(BaseModel):
    source_id: str
    hit_id: str
    raw: dict[str, Any]
    bods: list[dict[str, Any]]
    bods_issues: list[str]


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
    kind: SearchKind = Query(SearchKind.ENTITY, description="entity or person"),
) -> SearchResponse:
    """Fan-out search across registered adapters (non-streaming)."""

    results, errors = await _run_adapters(q, kind)
    hits = [hit for adapter_hits in results.values() for hit in adapter_hits]
    return SearchResponse(query=q, kind=kind, hits=hits, errors=errors)


@app.get("/stream")
async def stream(
    q: str = Query(..., min_length=1),
    kind: SearchKind = Query(SearchKind.ENTITY),
) -> EventSourceResponse:
    """Fan-out search streamed as SSE.

    Event types:

    * ``source_started`` — ``{source_id, source_name}`` (fired once per adapter)
    * ``hit`` — one ``SourceHit`` as it arrives
    * ``source_completed`` — ``{source_id, hit_count}``
    * ``source_error`` — ``{source_id, error}``
    * ``done`` — end of stream

    The frontend subscribes via ``EventSource`` and renders source cards as
    events arrive, with progressive disclosure per source.
    """
    return EventSourceResponse(_stream_events(q, kind))


async def _stream_events(q: str, kind: SearchKind) -> AsyncIterator[dict[str, Any]]:
    adapters = [
        (source_id, adapter)
        for source_id, adapter in REGISTRY.items()
        if kind in adapter.info.supports
    ]

    async def run_one(source_id: str, adapter: Any) -> tuple[str, list[SourceHit] | Exception]:
        try:
            hits = await adapter.search(q, kind)
            return source_id, hits
        except Exception as exc:  # noqa: BLE001
            return source_id, exc

    # Fire started events up front so the UI can render placeholders.
    for source_id, adapter in adapters:
        yield {
            "event": "source_started",
            "data": json.dumps(
                {"source_id": source_id, "source_name": adapter.info.name}
            ),
        }

    pending = {asyncio.create_task(run_one(sid, a)) for sid, a in adapters}
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            source_id, result = task.result()
            if isinstance(result, Exception):
                yield {
                    "event": "source_error",
                    "data": json.dumps(
                        {
                            "source_id": source_id,
                            "error": f"{type(result).__name__}: {result}",
                        }
                    ),
                }
                continue
            for hit in result:
                yield {
                    "event": "hit",
                    "data": hit.model_dump_json(),
                }
            yield {
                "event": "source_completed",
                "data": json.dumps(
                    {"source_id": source_id, "hit_count": len(result)}
                ),
            }

    yield {"event": "done", "data": json.dumps({"query": q, "kind": kind.value})}


@app.get("/deepen", response_model=DeepenResponse)
async def deepen(
    source: str = Query(..., description="Adapter id, e.g. 'companies_house'"),
    hit_id: str = Query(..., description="Adapter-local hit id"),
) -> DeepenResponse:
    """Fetch the full record for a single hit and map to BODS v0.4."""

    adapter = REGISTRY.get(source)
    if adapter is None:
        raise HTTPException(status_code=404, detail=f"unknown source {source!r}")

    raw = await adapter.fetch(hit_id)

    # Phase 1 mapper coverage: Companies House only.
    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    if source == "companies_house" and not raw.get("is_stub"):
        bundle = map_companies_house(raw)
        bods = list(bundle)
        issues = validate_shape(bods)

    return DeepenResponse(
        source_id=source,
        hit_id=hit_id,
        raw=raw,
        bods=bods,
        bods_issues=issues,
    )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


async def _run_adapters(
    q: str, kind: SearchKind
) -> tuple[dict[str, list[SourceHit]], dict[str, str]]:
    tasks = {
        source_id: asyncio.create_task(adapter.search(q, kind))
        for source_id, adapter in REGISTRY.items()
        if kind in adapter.info.supports
    }

    results: dict[str, list[SourceHit]] = {}
    errors: dict[str, str] = {}
    for source_id, task in tasks.items():
        try:
            results[source_id] = await task
        except Exception as exc:  # noqa: BLE001
            errors[source_id] = f"{type(exc).__name__}: {exc}"
            results[source_id] = []
    return results, errors
