"""Health and sources endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .. import __version__, consistencystats, memwatch, signalstats, source_health
from ..bo_access import notice_for
from ..config import get_settings
from ..sources import REGISTRY, SourceInfo, lineage

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


@router.get("/signalstats")
async def signal_stats() -> JSONResponse:
    """Which sources contributed which risk signals, since the last deploy.

    Answers questions that previously required running lookups by hand and
    counting: does OpenAleph screening contribute in production and how
    often relative to OpenSanctions; which sources produce signals so
    rarely they may not be earning their latency budget; how often each
    screen is degrading; and whether a change moved the new code's share of
    signals as predicted. Counted server-side rather than by sweeping
    ``/lookup``, which would load a free-tier instance, risk rate limits
    that make degraded results read as "signal absent", pull CC BY-NC data
    at volume for analytics, and sample whichever LEIs were chosen.

    Same contract as /memstats: public, unauthenticated, undecorated (no
    rate limit — a single dict dump), and aggregate only. Keys are closed
    vocabularies — signal codes, adapter ids, check names, degradation
    reasons — so entity names, LEIs and related-party names cannot appear,
    matching the ``degraded_sources`` privacy rule. In-process, so the
    counters reset on deploy and on Render spin-down.
    """
    return JSONResponse(signalstats.stats(), headers={"Cache-Control": "no-store"})


@router.get("/consistencystats")
async def consistency_stats() -> JSONResponse:
    """How often independent sources agree about the same entity, per field
    and source pair, since the last deploy (Phase 152, shadow mode).

    Same contract as /signalstats: public, unauthenticated, aggregate only,
    closed-vocabulary keys (field names, adapter ids, relation names), so no
    entity name, identifier or date can appear. Read ``disagree_rate`` per
    ``field|source_a|source_b`` row: under 10 % after two weeks of traffic
    is the gate for showing that comparison (see opencheck/consistency.py).
    """
    return JSONResponse(consistencystats.stats(), headers={"Cache-Control": "no-store"})


@router.get("/sources", response_model=SourcesResponse)
async def sources() -> SourcesResponse:
    # Attach the computed EU/EEA beneficial-ownership access notice per register.
    # Adapters declare only the static `country`; the (date-dependent) notice is
    # computed here so it flips on the restriction date without a code change.
    out: list[SourceInfo] = []
    for source_id, adapter in REGISTRY.items():
        info = adapter.info
        notice = notice_for(info.country)
        update: dict = {"derived_from": sorted(lineage.derived_from(source_id))}
        if notice:
            update["bo_access"] = notice
        out.append(info.model_copy(update=update))
    return SourcesResponse(sources=out)


@router.get("/source-health")
async def source_health_report() -> JSONResponse:
    """The last weekly sweep's verdict on every source (Phase 161).

    Read from the ``source-health-latest`` release asset the sweep uploads,
    refreshed at most hourly, served stale (and marked so) when the asset
    cannot be re-read, and ``available: false`` when no sweep has published
    one. Nothing here contacts a source: see ``opencheck/source_health.py``.
    Same contract as /sources: public, unauthenticated, undecorated — a
    cached dict dump.
    """
    payload = await source_health.load()
    return JSONResponse(payload, headers={"Cache-Control": "public, max-age=3600"})
