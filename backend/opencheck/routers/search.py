"""Search endpoints — /search and /stream."""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from ..ra_codes import ch_ra_code
from ..ratelimit import limiter, lookup_tier
from ..reconcile import reconcile
from ..risk import assess_hits
from ..sources import REGISTRY, SearchKind, SourceHit
from ..sources.schemas import SourceSchemaError

router = APIRouter()


class SearchResponse(BaseModel):
    query: str
    kind: SearchKind
    hits: list[SourceHit]
    errors: dict[str, str]
    cross_source_links: list[dict[str, Any]]
    risk_signals: list[dict[str, Any]]


def _fmt_source_error(exc: Exception) -> str:
    """Format a source fetch exception for the errors dict and SSE events."""
    if isinstance(exc, SourceSchemaError):
        return f"Source API changed — {exc}"
    return f"{type(exc).__name__}: {exc}"


# The Companies House → LEI bridge needs the GLEIF Registration Authority code
# for a company number, and Companies House files under three of them:
#
#     RA000585  England and Wales   (no prefix, or OC/FC/…)
#     RA000586  Northern Ireland    (NI, and NC/R0 for older registrations)
#     RA000587  Scotland            (SC, and SO/SF)
#
# The rule lived here until Phase 141, which was half the problem: the other
# entry point that scopes a GLEIF reverse lookup — ``/resolve-national-id`` —
# read a flat country map instead and never saw it, so ``country="GB"`` sent a
# Scottish number to the England & Wales authority and found nothing. Both
# paths now go through ``opencheck.ra_codes``, which holds the country map and
# the prefix rules together. ``opencheck.app`` re-exports this name.
_ch_ra_code = ch_ra_code


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


async def _search_impl(q: str, kind: SearchKind) -> SearchResponse:
    """Body of ``/search``, callable in-process (MCP tool) without going
    through the rate-limited route."""
    results, errors = await _run_adapters(q, kind)
    hits = [hit for adapter_hits in results.values() for hit in adapter_hits]
    links = [link.to_dict() for link in reconcile(hits)]
    signals = [s.to_dict() for s in assess_hits(hits)]
    return SearchResponse(
        query=q,
        kind=kind,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=signals,
    )


@router.get("/search", response_model=SearchResponse)
@limiter.limit(lookup_tier)
async def search(
    request: Request,
    response: Response,
    q: str = Query(..., min_length=1, description="Search query."),
    kind: SearchKind = Query(SearchKind.ENTITY, description="entity or person"),
) -> SearchResponse:
    """Fan-out search across registered adapters (non-streaming)."""
    return await _search_impl(q=q, kind=kind)


@router.get("/stream")
@limiter.limit(lookup_tier)
async def stream(
    request: Request,
    q: str = Query(..., min_length=1),
    kind: SearchKind = Query(SearchKind.ENTITY),
) -> EventSourceResponse:
    """Fan-out search streamed as SSE."""
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
    all_hits: list[SourceHit] = []
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
                            "error": _fmt_source_error(result),
                            "error_type": "schema_changed" if isinstance(result, SourceSchemaError) else "fetch_error",
                        }
                    ),
                }
                continue
            for hit in result:
                yield {
                    "event": "hit",
                    "data": hit.model_dump_json(),
                }
                all_hits.append(hit)
            yield {
                "event": "source_completed",
                "data": json.dumps(
                    {"source_id": source_id, "hit_count": len(result)}
                ),
            }

    # GLEIF bridge (CH → LEI reverse lookup)
    if kind == SearchKind.ENTITY:
        gleif_adapter = REGISTRY.get("gleif")
        if gleif_adapter and hasattr(gleif_adapter, "search_by_local_id") and gleif_adapter.info.live_available:
            existing_leis = {
                h.identifiers.get("lei")
                for h in all_hits
                if h.identifiers.get("lei")
            }
            ch_hits = [h for h in all_hits if h.source_id == "companies_house"]
            for ch_hit in ch_hits:
                ra_code = _ch_ra_code(ch_hit.hit_id)
                try:
                    bridge_hits = await gleif_adapter.search_by_local_id(  # type: ignore[attr-defined]
                        ch_hit.hit_id, ra_code=ra_code
                    )
                    for bh in bridge_hits:
                        lei = bh.identifiers.get("lei", "")
                        if not lei or lei in existing_leis:
                            continue
                        existing_leis.add(lei)
                        bridged = bh.model_copy(
                            update={
                                "identifiers": {**bh.identifiers, "gb_coh": ch_hit.hit_id}
                            }
                        )
                        all_hits.append(bridged)
                        yield {"event": "hit", "data": bridged.model_dump_json()}
                except Exception:  # noqa: BLE001
                    pass

    links = [link.to_dict() for link in reconcile(all_hits)]
    if links:
        yield {
            "event": "cross_source_links",
            "data": json.dumps({"links": links}),
        }

    signals = [s.to_dict() for s in assess_hits(all_hits)]
    if signals:
        yield {
            "event": "risk_signals",
            "data": json.dumps({"signals": signals}),
        }

    yield {"event": "done", "data": json.dumps({"query": q, "kind": kind.value})}
