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
from .bods import (
    BODSBundle,
    map_companies_house,
    map_everypolitician,
    map_gleif,
    map_openaleph,
    map_opensanctions,
    map_wikidata,
    validate_shape,
)
from .config import get_settings
from .reconcile import reconcile
from .risk import RiskSignal, assess_bundle, assess_hits
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
    cross_source_links: list[dict[str, Any]]
    risk_signals: list[dict[str, Any]]


class DeepenResponse(BaseModel):
    source_id: str
    hit_id: str
    raw: dict[str, Any]
    bods: list[dict[str, Any]]
    bods_issues: list[str]
    license: str
    license_notice: str | None = None
    risk_signals: list[dict[str, Any]] = []


class ReportResponse(BaseModel):
    """Aggregate post-search synthesis for a single subject.

    Pulls everything together: per-source hits, cross-source bridges,
    risk signals (search-time + per-deepened-bundle), and the BODS
    statements emitted along the way. The frontend uses this to render
    the right-hand "report" panel — one tidy view of what every source
    asserts about the same subject.
    """

    query: str
    kind: SearchKind
    hits: list[SourceHit]
    errors: dict[str, str]
    cross_source_links: list[dict[str, Any]]
    risk_signals: list[dict[str, Any]]
    bods: list[dict[str, Any]]
    bods_issues: list[str]
    license_notices: list[dict[str, str]]


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
                all_hits.append(hit)
            yield {
                "event": "source_completed",
                "data": json.dumps(
                    {"source_id": source_id, "hit_count": len(result)}
                ),
            }

    # Once every adapter has reported, run reconciliation and emit any
    # cross-source bridges as a single event for the UI to render.
    links = [link.to_dict() for link in reconcile(all_hits)]
    if links:
        yield {
            "event": "cross_source_links",
            "data": json.dumps({"links": links}),
        }

    # Risk signals derived from search-time data — surfaced as chips.
    signals = [s.to_dict() for s in assess_hits(all_hits)]
    if signals:
        yield {
            "event": "risk_signals",
            "data": json.dumps({"signals": signals}),
        }

    yield {"event": "done", "data": json.dumps({"query": q, "kind": kind.value})}


_MAPPERS = {
    "companies_house": map_companies_house,
    "gleif": map_gleif,
    "opensanctions": map_opensanctions,
    "openaleph": map_openaleph,
    "wikidata": map_wikidata,
    "everypolitician": map_everypolitician,
}

# Licenses that forbid commercial re-use. Anything in this set triggers
# a license notice on /deepen so exporters / downstream consumers know.
_NC_LICENSES = {"CC-BY-NC-4.0", "CC-BY-NC-SA-4.0"}


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

    # Phase 2 mapper coverage: CH, GLEIF, OpenSanctions, OpenAleph.
    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    mapper = _MAPPERS.get(source)
    if mapper and not raw.get("is_stub"):
        bundle: BODSBundle = mapper(raw)
        bods = list(bundle)
        issues = validate_shape(bods)

    info = adapter.info
    license_notice = _license_notice_for(info, raw)
    signals = [s.to_dict() for s in assess_bundle(source, raw, bods)]

    return DeepenResponse(
        source_id=source,
        hit_id=hit_id,
        raw=raw,
        bods=bods,
        bods_issues=issues,
        license=info.license,
        license_notice=license_notice,
        risk_signals=signals,
    )


@app.get("/report", response_model=ReportResponse)
async def report(
    q: str = Query(..., min_length=1),
    kind: SearchKind = Query(SearchKind.ENTITY),
    deepen_top: int = Query(
        3, ge=0, le=10, description="How many top hits to deepen+map+assess."
    ),
) -> ReportResponse:
    """One-shot synthesis: search, reconcile, deepen top N, assess risk.

    Designed for the report panel and for headless callers (e.g. a CLI
    or a future export). All four phases run concurrently where it's
    safe to do so — the deepen phase is parallelised across the top N
    hits, but only after search has resolved (we need the hits first).
    """
    results, errors = await _run_adapters(q, kind)
    hits = [hit for adapter_hits in results.values() for hit in adapter_hits]
    links = [link.to_dict() for link in reconcile(hits)]
    search_signals = [s.to_dict() for s in assess_hits(hits)]

    # Deepen the top N hits (skipping stubs) and run BODS + risk on each.
    deep_hits = [h for h in hits if not h.is_stub][:deepen_top]
    bods_all: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    deepen_signals: list[dict[str, Any]] = []
    license_notices: list[dict[str, str]] = []

    deepen_tasks = {
        (h.source_id, h.hit_id): asyncio.create_task(
            _safe_deepen(h.source_id, h.hit_id)
        )
        for h in deep_hits
    }
    for (source_id, hit_id), task in deepen_tasks.items():
        try:
            bundle = await task
        except Exception as exc:  # noqa: BLE001
            errors.setdefault(source_id, f"{type(exc).__name__}: {exc}")
            continue
        if bundle is None:
            continue
        bods_all.extend(bundle["bods"])
        bods_issues.extend(bundle["bods_issues"])
        deepen_signals.extend(bundle["risk_signals"])
        if bundle.get("license_notice"):
            license_notices.append(
                {
                    "source_id": source_id,
                    "hit_id": hit_id,
                    "notice": bundle["license_notice"],
                }
            )

    # Merge + dedupe risk signals across the two rounds.
    #
    # Per-source signals (PEP / SANCTIONED / OFFSHORE_LEAKS / OPAQUE) are
    # legitimately one-per-hit — a sanctioned record on OpenSanctions
    # and a separately-sanctioned record on EveryPolitician are two
    # distinct assertions. Dedupe key: (code, source_id, hit_id).
    #
    # Structural BODS signals (TRUST / NON_EU / NOMINEE / LAYERS /
    # COMPLEX / OBFUSCATION) describe the merged ownership chain, not
    # one source's view of it. Each deepened bundle re-asserts the same
    # fact, which inflates the chip strip. Collapse those by code only.
    structural_codes = {
        "TRUST_OR_ARRANGEMENT",
        "NON_EU_JURISDICTION",
        "NOMINEE",
        "COMPLEX_OWNERSHIP_LAYERS",
        "COMPLEX_CORPORATE_STRUCTURE",
        "POSSIBLE_OBFUSCATION",
    }
    merged: dict[tuple, dict[str, Any]] = {}
    for sig in search_signals + deepen_signals:
        if sig["code"] in structural_codes:
            key: tuple = (sig["code"],)
        else:
            key = (sig["code"], sig["source_id"], sig["hit_id"])
        # Prefer deepen-derived (richer evidence) over search-derived.
        merged[key] = sig
    all_signals = list(merged.values())

    return ReportResponse(
        query=q,
        kind=kind,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=all_signals,
        bods=bods_all,
        bods_issues=bods_issues,
        license_notices=license_notices,
    )


async def _safe_deepen(source_id: str, hit_id: str) -> dict[str, Any] | None:
    """Internal helper used by /report — does what /deepen does, but
    returns a plain dict and swallows nothing (caller handles errors)."""
    adapter = REGISTRY.get(source_id)
    if adapter is None:
        return None
    raw = await adapter.fetch(hit_id)
    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    mapper = _MAPPERS.get(source_id)
    if mapper and not raw.get("is_stub"):
        bundle: BODSBundle = mapper(raw)
        bods = list(bundle)
        issues = validate_shape(bods)
    license_notice = _license_notice_for(adapter.info, raw)
    signals = [s.to_dict() for s in assess_bundle(source_id, raw, bods)]
    return {
        "raw": raw,
        "bods": bods,
        "bods_issues": issues,
        "license_notice": license_notice,
        "risk_signals": signals,
    }


def _license_notice_for(
    info: SourceInfo, raw: dict[str, Any]
) -> str | None:
    """Return a human-readable warning when the payload is NC-licensed.

    Two cases:
    * The adapter itself declares an NC license (OpenSanctions).
    * OpenAleph — license is per-collection; we inspect the collection
      metadata that was fetched alongside the entity.
    """
    if info.license in _NC_LICENSES:
        return (
            f"{info.name} is licensed under {info.license}. Commercial "
            "re-use of this data is not permitted under the source license."
        )
    if info.id == "openaleph":
        collection = raw.get("collection") or {}
        license_ = (
            collection.get("license")
            or (collection.get("data") or {}).get("license")
            or ""
        ).upper().replace(" ", "-")
        if license_ and any(nc in license_ for nc in ("NC", "NON-COMMERCIAL")):
            label = collection.get("label") or collection.get("foreign_id") or "collection"
            return (
                f"OpenAleph collection '{label}' is licensed under "
                f"{collection.get('license') or license_}. Commercial re-use "
                "is not permitted under the source license."
            )
    return None


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
