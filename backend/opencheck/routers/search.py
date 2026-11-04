"""Search endpoints — /search and /stream."""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from ..ra_codes import ch_ra_code
from ..ratelimit import default_tier, limiter, lookup_tier
from ..reconcile import reconcile
from ..risk import assess_hits
from .. import search_filters as sf
from ..search_rank import rank_hits
from ..secret_scrub import describe_exception, scrub
from ..sources import REGISTRY, SearchKind, SourceHit
from ..sources.schemas import SourceSchemaError

router = APIRouter()


class SearchResponse(BaseModel):
    """A name search across every source that supports it, ranked."""

    #: The search string as given.
    query: str
    #: ``entity`` or ``person``.
    kind: SearchKind
    #: Every candidate, ranked across sources by match tier, register status
    #: and LEI (``opencheck.search_rank``), one ``SourceHit`` each.
    hits: list[SourceHit]
    #: Sources that failed, keyed by source id, each a secret-scrubbed
    #: one-line error.
    errors: dict[str, str]
    #: Confirmed bridges between candidates that share an identifier — one
    #: ``CrossSourceLink`` each.
    cross_source_links: list[dict[str, Any]]
    #: Signals the candidates' own records carry (a sanctioned hit); the
    #: cross-source screens need a lookup.
    risk_signals: list[dict[str, Any]]
    # The advanced-search-filters ticket: present only when a filter was
    # set — the filters, the sources that applied them and the sources left
    # out (``reason``: ``cannot_apply`` | ``outside_jurisdiction``).
    filters: dict[str, Any] | None = None


def _fmt_source_error(exc: Exception) -> str:
    """Format a source fetch exception for the errors dict and SSE events.

    Never ``str(exc)`` directly: an httpx status error's message is the full
    request URL, and two adapters authenticate in the query string. See
    :mod:`opencheck.secret_scrub`.
    """
    if isinstance(exc, SourceSchemaError):
        return f"Source API changed — {scrub(str(exc))}"
    return describe_exception(exc)


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


def _plan(kind: SearchKind, filters: sf.SearchFilters | None) -> tuple[
    list[tuple[str, Any, bool]], dict[str, Any] | None
]:
    """Which adapters run (and whether filtered), and the response's
    ``filters`` block. See :func:`opencheck.search_filters.filter_plan`."""
    run: list[tuple[str, Any, bool]] = []
    applied_by: list[str] = []
    left_out: list[dict[str, Any]] = []
    for source_id, adapter in REGISTRY.items():
        if kind not in adapter.info.supports:
            continue
        if not filters:
            run.append((source_id, adapter, False))
            continue
        plan = sf.filter_plan(adapter, kind.value, filters)
        if plan.run:
            run.append((source_id, adapter, plan.filtered))
            applied_by.append(source_id)
        else:
            left_out.append(
                {"source_id": source_id, "reason": plan.reason, "filters": plan.unsupported}
            )
    block = None
    if filters:
        block = {"applied": filters.to_dict(), "applied_by": applied_by, "left_out": left_out}
    return run, block


async def _search_one(
    adapter: Any, q: str, kind: SearchKind, filtered: bool, filters: sf.SearchFilters | None
) -> list[SourceHit]:
    if filtered and filters:
        return await adapter.search_filtered(q, kind, filters)
    return await adapter.search(q, kind)


async def _run_adapters(
    q: str, kind: SearchKind
) -> tuple[dict[str, list[SourceHit]], dict[str, str]]:
    """Unfiltered fan-out (``/lookup``'s name search, ``/person-check``)."""
    results, errors, _ = await _run_filtered_adapters(q, kind, None)
    return results, errors


async def _run_filtered_adapters(
    q: str, kind: SearchKind, filters: sf.SearchFilters | None
) -> tuple[dict[str, list[SourceHit]], dict[str, str], dict[str, Any] | None]:
    plan, block = _plan(kind, filters)
    tasks = {
        source_id: asyncio.create_task(_search_one(adapter, q, kind, filtered, filters))
        for source_id, adapter, filtered in plan
    }

    results: dict[str, list[SourceHit]] = {}
    errors: dict[str, str] = {}
    for source_id, task in tasks.items():
        try:
            results[source_id] = await task
        except Exception as exc:  # noqa: BLE001
            errors[source_id] = _fmt_source_error(exc)
            results[source_id] = []
    return results, errors, block


async def _search_impl(
    q: str, kind: SearchKind, filters: sf.SearchFilters | None = None
) -> SearchResponse:
    """Body of ``/search``, callable in-process (MCP tool) without going
    through the rate-limited route."""
    if filters is None:
        # Through ``_run_adapters`` so a test (or caller) patching it still
        # sees the unfiltered search.
        results, errors = await _run_adapters(q, kind)
        block = None
    else:
        results, errors, block = await _run_filtered_adapters(q, kind, filters)
    # Phase 292: rank across sources (exact → same name → all tokens →
    # distinctive tokens → fuzzy; ties by register status, then LEI) rather
    # than returning hits grouped by source in registry order. The web
    # picker does not read /search (it queries GLEIF directly), so this
    # orders the REST response and the MCP tool, which is where the
    # grouped order buried exact matches. See ``opencheck.search_rank``.
    hits = rank_hits(
        q,
        [hit for adapter_hits in results.values() for hit in adapter_hits],
        person=kind == SearchKind.PERSON,
    )
    links = [link.to_dict() for link in reconcile(hits)]
    signals = [s.to_dict() for s in assess_hits(hits)]
    return SearchResponse(
        query=q,
        kind=kind,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=signals,
        filters=block,
    )


def _filters_from_query(
    jurisdiction: str | None,
    legal_form: str | None,
    entity_status: str | None,
    registration_status: str | None,
    category: str | None,
    nationality: str | None,
    birth_year: str | None,
) -> sf.SearchFilters | None:
    try:
        f = sf.parse_filters(
            jurisdiction=jurisdiction,
            legal_form=legal_form,
            entity_status=entity_status,
            registration_status=registration_status,
            category=category,
            nationality=nationality,
            birth_year=birth_year,
        )
    except sf.FilterError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return f or None


_FILTER_DOC = {
    "jurisdiction": "ISO 3166 code of the jurisdiction of formation (GB, DE, US, US-DE). Exact: DE never returns Delaware.",
    "legal_form": "ISO 20275 ELF code(s), comma-separated, or 'no-elf-code' (see /elf-forms).",
    "entity_status": "ACTIVE or INACTIVE, as GLEIF records it.",
    "registration_status": "LEI registration status(es): ISSUED, LAPSED, RETIRED…",
    "category": "GENERAL, BRANCH, FUND, SOLE_PROPRIETOR, RESIDENT_GOVERNMENT_ENTITY, INTERNATIONAL_ORGANIZATION.",
    "nationality": "Person search: ISO 3166-1 alpha-2 nationality or citizenship.",
    "birth_year": "Person search: year of birth.",
}


@router.get("/search", response_model=SearchResponse)
@limiter.limit(lookup_tier)
async def search(
    request: Request,
    response: Response,
    q: str = Query(..., min_length=1, description="Search query."),
    kind: SearchKind = Query(SearchKind.ENTITY, description="entity or person"),
    jurisdiction: str | None = Query(None, description=_FILTER_DOC["jurisdiction"]),
    legal_form: str | None = Query(None, description=_FILTER_DOC["legal_form"]),
    entity_status: str | None = Query(None, description=_FILTER_DOC["entity_status"]),
    registration_status: str | None = Query(None, description=_FILTER_DOC["registration_status"]),
    category: str | None = Query(None, description=_FILTER_DOC["category"]),
    nationality: str | None = Query(None, description=_FILTER_DOC["nationality"]),
    birth_year: str | None = Query(None, description=_FILTER_DOC["birth_year"]),
) -> SearchResponse:
    """Fan-out search across registered adapters (non-streaming).

    With a filter set, only sources that can apply it run; the others are
    named under ``filters.left_out`` rather than shown unfiltered.
    """
    filters = _filters_from_query(
        jurisdiction, legal_form, entity_status, registration_status, category, nationality, birth_year
    )
    return await _search_impl(q=q, kind=kind, filters=filters)


@router.get("/stream")
@limiter.limit(lookup_tier)
async def stream(
    request: Request,
    q: str = Query(..., min_length=1),
    kind: SearchKind = Query(SearchKind.ENTITY),
    jurisdiction: str | None = Query(None, description=_FILTER_DOC["jurisdiction"]),
    legal_form: str | None = Query(None, description=_FILTER_DOC["legal_form"]),
    entity_status: str | None = Query(None, description=_FILTER_DOC["entity_status"]),
    registration_status: str | None = Query(None, description=_FILTER_DOC["registration_status"]),
    category: str | None = Query(None, description=_FILTER_DOC["category"]),
    nationality: str | None = Query(None, description=_FILTER_DOC["nationality"]),
    birth_year: str | None = Query(None, description=_FILTER_DOC["birth_year"]),
) -> EventSourceResponse:
    """Fan-out search streamed as SSE. Filters as for ``/search``; a
    ``filters`` event first names the sources applying them and those left out."""
    filters = _filters_from_query(
        jurisdiction, legal_form, entity_status, registration_status, category, nationality, birth_year
    )
    return EventSourceResponse(_stream_events(q, kind, filters))


async def _stream_events(
    q: str, kind: SearchKind, filters: sf.SearchFilters | None = None
) -> AsyncIterator[dict[str, Any]]:
    plan, block = _plan(kind, filters)
    adapters = [(source_id, adapter) for source_id, adapter, _ in plan]
    filtered_ids = {source_id for source_id, _, filtered in plan if filtered}

    async def run_one(source_id: str, adapter: Any) -> tuple[str, list[SourceHit] | Exception]:
        try:
            hits = await _search_one(adapter, q, kind, source_id in filtered_ids, filters)
            return source_id, hits
        except Exception as exc:  # noqa: BLE001
            return source_id, exc

    if block is not None:
        yield {"event": "filters", "data": json.dumps(block)}

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

    # GLEIF bridge (CH → LEI reverse lookup). Not under filters: the
    # bridged GLEIF record has not been through them.
    if kind == SearchKind.ENTITY and not filters:
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


@router.get("/elf-forms")
@limiter.limit(default_tier)
async def elf_forms(
    request: Request,
    response: Response,
    jurisdiction: str = Query(..., min_length=2, description="ISO 3166 code: DE, GB, US-DE."),
) -> dict[str, Any]:
    """The ISO 20275 legal forms of one jurisdiction, for the legal-form filter.

    Each form carries its ELF code, display name and abbreviation, every name
    and abbreviation (for type-ahead) and, where the Golden Copy mirror is
    present, the number of issued LEIs with that form — the list is then
    ordered most used first (``counts_available``). ``has_subdivision_forms``
    says a country's list is incomplete until a state or province is chosen
    (the United States and Canada).
    """
    counts = await asyncio.to_thread(sf.legal_form_counts)
    try:
        payload = sf.forms_for(jurisdiction, counts)
    except sf.FilterError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response.headers["Cache-Control"] = "public, max-age=3600"
    return payload
