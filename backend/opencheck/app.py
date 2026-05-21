"""FastAPI entry point for OpenCheck.

Surface:

* ``GET /health`` — liveness probe.
* ``GET /sources`` — inventory of registered source adapters with live/stub status.
* ``GET /lookup?lei=<LEI>`` — **primary entry point**. Driven by the
  Legal Entity Identifier: GLEIF first, then dispatch to every other
  source using the LEI (and any cross-references GLEIF carries).
* ``GET /search?q=<query>&kind=<entity|person>`` — free-text fan-out
  search. Kept as a power-user / debugging endpoint; the LEI-driven
  flow is the supported UX.
* ``GET /stream?q=<query>&kind=<entity|person>`` — same fan-out, streamed as SSE.
* ``GET /deepen?source=<id>&hit_id=<id>`` — "Go deeper" on a specific hit.
* ``GET /report?q=<query>&kind=<entity|person>`` — free-text synthesis.
* ``GET /export?q=<query>&kind=<...>&format=<json|jsonl|zip>`` — downloadable BODS bundle.
"""

from __future__ import annotations

import asyncio
import io
import json
import re
import zipfile
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from . import __version__
from .bods import (
    BODSBundle,
    map_ares,
    map_ariregister,
    map_bce_belgium,
    map_bolagsverket,
    map_brightquery,
    map_brreg,
    map_climatetrace,
    map_companies_house,
    map_cro,
    map_everypolitician,
    map_gleif,
    map_inpi,
    map_openaleph,
    map_opencorporates,
    map_opensanctions,
    map_prh,
    map_jar_lithuania,
    map_krs_poland,
    map_firmenbuch,
    map_rpo_slovakia,
    map_rpvs_slovakia,
    map_sec_edgar,
    map_ur_latvia,
    map_wikidata,
    map_kvk,
    map_zefix,
    validate_shape,
)
from .sources.ares import CZ_RA_CODE as _CZ_RA_CODE, normalise_ico as _normalise_ico
from .sources.bce_belgium import BCE_RA_CODE as _BCE_RA_CODE, normalise_enterprise_number as _normalise_enterprise_number
from .sources.krs_poland import PL_KRS_RA_CODE as _PL_KRS_RA_CODE, normalise_krs as _normalise_krs
from .sources.firmenbuch import AT_FB_RA_CODE as _AT_FB_RA_CODE, normalise_fn as _normalise_fn
from .sources.rpo_slovakia import SK_RPO_RA_CODE as _SK_RPO_RA_CODE, normalise_ico as _normalise_sk_ico
from .sources.ariregister import EE_RA_CODE as _EE_RA_CODE
from .sources.bolagsverket import BV_RA_CODE as _BV_RA_CODE, normalise_org_number as _normalise_org_number
from .sources.brreg import NO_RA_CODE as _BRREG_RA_CODE, normalise_orgnr as _normalise_orgnr
from .sources.cro import IE_RA_CODE as _CRO_RA_CODE, normalise_crn as _normalise_crn
from .sources.prh import FI_RA_CODE as _PRH_RA_CODE, normalise_ytunnus as _normalise_ytunnus
from .sources.ur_latvia import LV_RA_CODE as _LV_RA_CODE, normalise_regcode as _normalise_lv_regcode
from .sources.jar_lithuania import LT_RA_CODE as _LT_RA_CODE, normalise_code as _normalise_lt_code
from .sources.inpi import INPI_RA_CODE as _INPI_RA_CODE, normalise_siren as _normalise_siren
from .sources.kvk import KVK_RA_CODE as _KVK_RA_CODE, normalise_kvk as _normalise_kvk
from .sources.zefix import CH_RA_CODES as _ZEFIX_RA_CODES, normalise_uid as _zefix_normalise_uid
from . import bods_data
from .config import get_settings
from .cross_check import assess_cross_source_names
from .icij_check import assess_icij_names
from .reconcile import reconcile
from .risk import RiskSignal, assess_bundle, assess_hits
from .sources import REGISTRY, SearchKind, SourceHit, SourceInfo

@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan hook (reserved for future startup tasks)."""
    yield  # server runs here


app = FastAPI(
    title="OpenCheck",
    version=__version__,
    description=(
        "Customer due diligence risk checks driven by the Legal Entity "
        "Identifier (LEI) and open data — mapped to the "
        "Beneficial Ownership Data Standard (BODS)."
    ),
    lifespan=_lifespan,
)


_cors_origin = get_settings().cors_origin
app.add_middleware(
    CORSMiddleware,
    # When the env var is "*" (e.g. Render public demo), allow all origins
    # and disable allow_credentials — browsers reject wildcard + credentials.
    allow_origins=["*"] if _cors_origin == "*" else [_cors_origin],
    allow_credentials=(_cors_origin != "*"),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unhandled exceptions.

    Handles two problems:
    1. Without this, uvicorn catches the exception and returns a plain-text 500
       *before* the CORSMiddleware can add ``Access-Control-Allow-Origin``.
       Browsers block the cross-origin response; iOS/Safari reports it as
       "Load failed" rather than a useful error message.
    2. CORSMiddleware does not reliably add ACAO to 500 responses produced
       by @app.exception_handler, so we add the header explicitly here.
    """
    import logging
    logging.getLogger(__name__).exception("Unhandled exception in %s %s", request.method, request.url)
    # Explicitly add CORS header — CORSMiddleware doesn't reliably forward
    # it for error responses, and without it browsers silently block the 500.
    origin = request.headers.get("origin")
    extra_headers: dict[str, str] = {"access-control-allow-origin": "*"} if origin else {}
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {exc}"},
        headers=extra_headers,
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

    # ── GLEIF bridge (CH → LEI reverse lookup) ────────────────────────────
    # For entity searches: any Companies House hit that doesn't already
    # have a matching GLEIF hit gets a reverse lookup via GLEIF's
    # filter[entity.registeredAs] / filter[registration.validatedAs] /
    # filter[registration.otherValidationAuthorities.validatedAs] API.
    # This means users who search by name get GLEIF Level 1 + Level 2
    # data automatically — no need to search GLEIF separately.
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
                        # Attach gb_coh so the reconciler can bridge GLEIF ↔ CH
                        bridged = bh.model_copy(
                            update={
                                "identifiers": {**bh.identifiers, "gb_coh": ch_hit.hit_id}
                            }
                        )
                        all_hits.append(bridged)
                        yield {"event": "hit", "data": bridged.model_dump_json()}
                except Exception:  # noqa: BLE001
                    pass

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


def _ch_ra_code(company_number: str) -> str:
    """Return the GLEIF Registration Authority code for a Companies House number.

    The prefix of the company number identifies which sub-registry issued it:
    ``SC`` → Scotland (RA000586), ``NI`` → Northern Ireland (RA000591),
    anything else → England & Wales (RA000585, the large majority).

    Passing the correct RA code to GLEIF prevents false positives when
    different registries share the same local number format.
    """
    upper = (company_number or "").strip().upper()
    if upper.startswith("SC"):
        return "RA000586"
    if upper.startswith("NI"):
        return "RA000591"
    return "RA000585"


_MAPPERS = {
    "ariregister": map_ariregister,
    "bce_belgium": map_bce_belgium,
    "bolagsverket": map_bolagsverket,
    "brreg": map_brreg,
    "climatetrace": map_climatetrace,
    "companies_house": map_companies_house,
    "cro": map_cro,
    "gleif": map_gleif,
    "inpi": map_inpi,
    "opencorporates": map_opencorporates,
    "brightquery": map_brightquery,
    "opensanctions": map_opensanctions,
    "openaleph": map_openaleph,
    "sec_edgar": map_sec_edgar,
    "wikidata": map_wikidata,
    "everypolitician": map_everypolitician,
    "kvk": map_kvk,
    "prh": map_prh,
    "ares": map_ares,
    "krs_poland": map_krs_poland,
    "firmenbuch": map_firmenbuch,
    "rpo_slovakia": map_rpo_slovakia,
    "rpvs_slovakia": map_rpvs_slovakia,
    "jar_lithuania": map_jar_lithuania,
    "ur_latvia": map_ur_latvia,
    "zefix": map_zefix,
}

# Licenses that forbid commercial re-use. Anything in this set triggers
# a license notice on /deepen so exporters / downstream consumers know.
# OpenTender's CC-BY-NC-SA-4.0 also imposes a share-alike clause: any
# derivative must be re-licensed under the same terms.
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

    # Consult the Open Ownership override bundle for GLEIF / CH before
    # the live mapper. Same shared helper as /lookup uses.
    override = _bods_data_override(source, hit_id)
    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    if override is not None:
        bods = override
        issues = validate_shape(bods)
    else:
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
    or the export endpoint). All four phases run concurrently where
    it's safe to do so — the deepen phase is parallelised across the
    top N hits, but only after search has resolved (we need the hits
    first).
    """
    return await _build_report(q, kind, deepen_top)


async def _build_report(
    q: str, kind: SearchKind, deepen_top: int
) -> ReportResponse:
    """Shared by /report and /export. Same algorithm; same response shape."""
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

    # Cross-check related-party names against OpenSanctions and
    # EveryPolitician — emits RELATED_PEP / RELATED_SANCTIONED scoped
    # to the matching person/entity statement.
    cross_signals = [
        s.to_dict() for s in await assess_cross_source_names(bods_all)
    ]

    # Cross-check entity and officer names against the ICIJ Offshore Leaks
    # reconciliation API — emits OFFSHORE_LEAKS signals scoped to the
    # matching statement, no API key required.
    icij_signals = [
        s.to_dict() for s in await assess_icij_names(bods_all)
    ]

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
    #
    # RELATED_* and ICIJ OFFSHORE_LEAKS signals are scoped to the matched
    # statement — include subject_statement_id in the dedup key so a single
    # ICIJ node matched against two different statements produces two chips.
    structural_codes = {
        "TRUST_OR_ARRANGEMENT",
        "NON_EU_JURISDICTION",
        "NOMINEE",
        "COMPLEX_OWNERSHIP_LAYERS",
        "COMPLEX_CORPORATE_STRUCTURE",
        "POSSIBLE_OBFUSCATION",
    }
    _statement_scoped = {"RELATED_PEP", "RELATED_SANCTIONED"}
    merged: dict[tuple, dict[str, Any]] = {}
    for sig in search_signals + deepen_signals + cross_signals + icij_signals:
        if sig["code"] in structural_codes:
            key: tuple = (sig["code"],)
        elif sig["code"] in _statement_scoped or (
            sig["code"] == "OFFSHORE_LEAKS" and sig.get("source_id") == "icij"
        ):
            # Scoped to (code, source_id, hit_id, subject_statement_id) —
            # one chip per matched record + related party combination.
            key = (
                sig["code"],
                sig["source_id"],
                sig["hit_id"],
                sig.get("evidence", {}).get("subject_statement_id", ""),
            )
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


# ----------------------------------------------------------------------
# /lookup — LEI-anchored lookup (the primary entry point)
# ----------------------------------------------------------------------

# 20-char ISO 17442 LEI, alphanumeric uppercase.
_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


class LookupResponse(ReportResponse):
    """Same shape as /report, with the LEI echoed back and the GLEIF
    bundle surfaced separately so the UI doesn't have to dig for it.

    The bridge fields (``derived_identifiers``) tell the caller exactly
    which secondary identifiers GLEIF (and Wikidata) gave us, so a user
    can see *why* each downstream source got hit.
    """

    lei: str
    legal_name: str | None = None
    jurisdiction: str | None = None
    derived_identifiers: dict[str, str] = {}


@app.get("/lookup", response_model=LookupResponse)
async def lookup(
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
) -> LookupResponse:
    """Driver endpoint: LEI in, full cross-source synthesis out.

    Workflow:

    1. Validate the LEI shape (20-char alphanumeric).
    2. ``GLEIF.fetch(lei)`` — primary source. We need the legal name,
       jurisdiction, and any registered-as identifier (e.g. UK CH
       number when jurisdiction=GB).
    3. Look up Wikidata Q-ID via SPARQL on property P1278.
    4. Dispatch to every other adapter using the LEI as the query
       (OpenSanctions, OpenAleph, OpenTender), or via the derived
       identifier (Companies House gets the GB-COH directly,
       Wikidata gets the resolved Q-ID).
    5. Map each result through BODS, run the cross-source reconciler,
       run the risk-signal service, and return everything.

    Bypasses the Phase 0 ``/search`` / ``/report`` free-text path
    entirely — this is the supported, deterministic flow.
    """
    lei = lei.strip().upper()
    if not _LEI_SHAPE.match(lei):
        raise HTTPException(
            status_code=400,
            detail=(
                f"{lei!r} is not a valid LEI. ISO 17442 LEIs are "
                "20-character alphanumeric strings (e.g. "
                "213800LH1BZH3DI6G760)."
            ),
        )

    gleif = REGISTRY["gleif"]

    # If we have a pre-extracted Open Ownership bundle for this LEI,
    # skip the live GLEIF fetch entirely and read the subject metadata
    # straight from the bundle. That makes the demo subjects work
    # offline (no API keys, no network) once the extraction script
    # has populated ``data/cache/bods_data/gleif/<LEI>.jsonl``.
    override_bundle = bods_data.gleif_bundle_for_lei(lei)
    legal_name = ""
    jurisdiction = ""
    registered_as = ""
    registered_at_id = ""
    gleif_bundle: dict[str, Any] = {}

    if override_bundle:
        legal_name, jurisdiction, registered_as = _subject_metadata_from_bundle(
            override_bundle, lei
        )
        if not legal_name:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Found a BODS bundle for {lei} but couldn't locate "
                    "the subject entity statement. Re-run the extraction "
                    "script."
                ),
            )
        # Synthesise a minimal GLEIF "raw" bundle so the rest of the
        # pipeline (deepen / mapping / hit construction) sees the same
        # shape it would on a live fetch.
        gleif_bundle = {"source_id": "gleif", "lei": lei, "_from_bundle": True}
    else:
        gleif_bundle = await gleif.fetch(lei)
        if gleif_bundle.get("is_stub") or not gleif_bundle.get("record"):
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No GLEIF record found for {lei}. Either the LEI is "
                    "not registered, live mode is disabled, or no Open "
                    "Ownership bundle has been extracted for this LEI "
                    "(see backend/scripts/extract_bods_subgraphs.py)."
                ),
            )
        record_attrs = (gleif_bundle.get("record") or {}).get("attributes") or {}
        entity_block = record_attrs.get("entity") or {}
        legal_name = (entity_block.get("legalName") or {}).get("name") or ""
        jurisdiction = entity_block.get("jurisdiction") or ""
        registered_as = entity_block.get("registeredAs") or ""
        registered_at_id = (entity_block.get("registeredAt") or {}).get("id") or ""

    derived: dict[str, str] = {"lei": lei}
    if jurisdiction.upper() == "GB" and registered_as:
        derived["gb_coh"] = registered_as
    if registered_at_id in _ZEFIX_RA_CODES and registered_as:
        derived["che_uid"] = _zefix_normalise_uid(registered_as)
    if registered_at_id == _KVK_RA_CODE and registered_as:
        derived["kvk_number"] = _normalise_kvk(registered_as)
    if registered_at_id == _INPI_RA_CODE and registered_as:
        derived["siren"] = _normalise_siren(registered_as)
    if registered_at_id == _BV_RA_CODE and registered_as:
        try:
            derived["se_org_number"] = _normalise_org_number(registered_as)
        except ValueError:
            pass
    if registered_at_id == _EE_RA_CODE and registered_as:
        # Estonian registry code — 8-digit numeric string (e.g. "14064835")
        derived["ee_registry_code"] = registered_as.strip().zfill(8)
    if registered_at_id == _BRREG_RA_CODE and registered_as:
        # Norwegian organisation number — 9-digit string (e.g. "923609016")
        derived["no_orgnr"] = _normalise_orgnr(registered_as)
    if registered_at_id == _CRO_RA_CODE and registered_as:
        # Irish company registration number (e.g. "249885")
        derived["ie_crn"] = _normalise_crn(registered_as)
    if registered_at_id == _PRH_RA_CODE and registered_as:
        # Finnish Business ID / Y-tunnus (e.g. "0112038-9")
        derived["fi_ytunnus"] = _normalise_ytunnus(registered_as)
    if registered_at_id == _LV_RA_CODE and registered_as:
        # Latvian registration number — 11-digit string (e.g. "40003521600")
        derived["lv_regcode"] = _normalise_lv_regcode(registered_as)
    if registered_at_id == _LT_RA_CODE and registered_as:
        # Lithuanian entity code — 9-digit string (e.g. "111950694")
        derived["lt_code"] = _normalise_lt_code(registered_as)
    if registered_at_id == _CZ_RA_CODE and registered_as:
        # Czech IČO — 8-digit zero-padded string (e.g. "27082440")
        derived["cz_ico"] = _normalise_ico(registered_as)
    if registered_at_id == _PL_KRS_RA_CODE and registered_as:
        # Polish KRS number — 10-digit zero-padded string (e.g. "0000028860")
        derived["pl_krs"] = _normalise_krs(registered_as)
    if registered_at_id == _AT_FB_RA_CODE and registered_as:
        # Austrian Firmenbuchnummer — digits + lowercase letter (e.g. "093363z")
        derived["at_fn"] = _normalise_fn(registered_as)
    if registered_at_id == _SK_RPO_RA_CODE and registered_as:
        # Slovak IČO — 8-digit zero-padded string (e.g. "00000000")
        derived["sk_ico"] = _normalise_sk_ico(registered_as)
    if registered_at_id == _BCE_RA_CODE and registered_as:
        # Belgian enterprise number — 10-digit string (e.g. "0433795975")
        derived["be_enterprise_number"] = _normalise_enterprise_number(registered_as)

    # OpenCorporates identifier — comes from GLEIF Level 1 ``attributes.ocid``
    # (format: ``{jurisdiction_code}/{company_number}``).
    # When the lookup used a pre-extracted BODS bundle, ``gleif_bundle``
    # doesn't carry the API response. We fetch the GLEIF Level 1 record
    # separately in that case; the result is cached so the call is free
    # on subsequent lookups.
    ocid: str | None = None
    if gleif.info.live_available:
        try:
            _gleif_src = (
                gleif_bundle
                if not gleif_bundle.get("_from_bundle")
                else await gleif.fetch(lei)
            )
            if not _gleif_src.get("is_stub"):
                _attrs = (_gleif_src.get("record") or {}).get("attributes") or {}
                ocid = _attrs.get("ocid") or None
        except Exception:  # noqa: BLE001
            pass
    if ocid:
        derived["ocid"] = ocid

    # Wikidata Q-ID from LEI (P1278).
    wikidata_adapter = REGISTRY["wikidata"]
    qid = None
    if hasattr(wikidata_adapter, "find_qid_by_lei"):
        qid = await wikidata_adapter.find_qid_by_lei(lei)  # type: ignore[attr-defined]
    if qid:
        derived["wikidata_qid"] = qid

    # Now dispatch to every other source. We collect SourceHit objects
    # so the existing reconciler / risk pipeline can consume them.
    hits: list[SourceHit] = []
    errors: dict[str, str] = {}
    deepened_bundles: list[tuple[str, str]] = []  # (source_id, hit_id)

    # GLEIF: we already fetched the record; surface it as a hit.
    gleif_hit = SourceHit(
        source_id="gleif",
        hit_id=lei,
        kind=SearchKind.ENTITY,
        name=legal_name or f"LEI {lei}",
        summary=f"LEI {lei} · {jurisdiction}",
        identifiers={
            "lei": lei,
            **({"gb_coh": registered_as} if "gb_coh" in derived else {}),
            **({"che_uid": derived["che_uid"]} if "che_uid" in derived else {}),
            **({"kvk_number": derived["kvk_number"]} if "kvk_number" in derived else {}),
            **({"siren": derived["siren"]} if "siren" in derived else {}),
            **({"se_org_number": derived["se_org_number"]} if "se_org_number" in derived else {}),
            **({"ee_registry_code": derived["ee_registry_code"]} if "ee_registry_code" in derived else {}),
            **({"no_orgnr": derived["no_orgnr"]} if "no_orgnr" in derived else {}),
            **({"ie_crn": derived["ie_crn"]} if "ie_crn" in derived else {}),
            **({"fi_ytunnus": derived["fi_ytunnus"]} if "fi_ytunnus" in derived else {}),
            **({"lv_regcode": derived["lv_regcode"]} if "lv_regcode" in derived else {}),
            **({"lt_code": derived["lt_code"]} if "lt_code" in derived else {}),
            **({"cz_ico": derived["cz_ico"]} if "cz_ico" in derived else {}),
            **({"pl_krs": derived["pl_krs"]} if "pl_krs" in derived else {}),
            **({"at_fn": derived["at_fn"]} if "at_fn" in derived else {}),
            **({"sk_ico": derived["sk_ico"]} if "sk_ico" in derived else {}),
            **({"be_enterprise_number": derived["be_enterprise_number"]} if "be_enterprise_number" in derived else {}),
            **({"wikidata_qid": qid} if qid else {}),
        },
        raw=gleif_bundle.get("record") or {},
        is_stub=False,
    )
    hits.append(gleif_hit)
    deepened_bundles.append(("gleif", lei))

    # ── Wave 1: all source fetches in parallel ────────────────────────────────
    # Build one coroutine per applicable source, gather them all at once, then
    # do the (fast, CPU-only) result-processing loop sequentially.
    #
    # SEC EDGAR may need an edgar_cik extracted from the OpenCorporates result,
    # so it runs in a short Wave 2 after OC has been processed below.
    # -------------------------------------------------------------------------

    bq_adapter = REGISTRY.get("brightquery")
    oa_adapter = REGISTRY.get("openaleph")
    ct_adapter = REGISTRY.get("climatetrace")
    bods_gleif_adapter = REGISTRY.get("bods_gleif")

    # OpenAleph uses sequential fallback strategies internally; collapse them
    # into a single awaitable so the whole chain is one gather slot.
    async def _openaleph_strategies() -> list[SourceHit]:
        if oa_adapter is None:
            return []
        _oa: list[SourceHit] = await oa_adapter.fetch_by_lei(lei)  # type: ignore[attr-defined]
        if not _oa and "ocid" in derived:
            _oa = await oa_adapter.fetch_by_oc_url(derived["ocid"])  # type: ignore[attr-defined]
        if not _oa:
            for _jur, _reg in [
                ("gb", derived.get("gb_coh")),
                ("fr", derived.get("siren")),
                ("nl", derived.get("kvk_number")),
                ("se", derived.get("se_org_number")),
                ("ch", derived.get("che_uid")),
            ]:
                if _reg:
                    _oa = await oa_adapter.fetch_by_registration(_jur, _reg)  # type: ignore[attr-defined]
                    if _oa:
                        break
        if not _oa and legal_name:
            _oa = await oa_adapter.fetch_by_name(legal_name)  # type: ignore[attr-defined]
        return _oa

    _w1: list[tuple[str, Any]] = []  # (label, coroutine)
    if "gb_coh" in derived:
        _w1.append(("companies_house", REGISTRY["companies_house"].fetch(derived["gb_coh"])))
    if "che_uid" in derived:
        _w1.append(("zefix", REGISTRY["zefix"].fetch(derived["che_uid"])))
    if "kvk_number" in derived:
        _w1.append(("kvk", REGISTRY["kvk"].fetch(derived["kvk_number"], legal_name=legal_name)))
    if "siren" in derived:
        _w1.append(("inpi", REGISTRY["inpi"].fetch(derived["siren"])))
    if "se_org_number" in derived:
        _w1.append(("bolagsverket", REGISTRY["bolagsverket"].fetch(derived["se_org_number"], legal_name=legal_name)))
    if "ee_registry_code" in derived:
        _w1.append(("ariregister", REGISTRY["ariregister"].fetch(derived["ee_registry_code"], legal_name=legal_name)))
    if "no_orgnr" in derived:
        _w1.append(("brreg", REGISTRY["brreg"].fetch(derived["no_orgnr"], legal_name=legal_name)))
    if "ie_crn" in derived:
        _w1.append(("cro", REGISTRY["cro"].fetch(derived["ie_crn"], legal_name=legal_name)))
    if "fi_ytunnus" in derived:
        _w1.append(("prh", REGISTRY["prh"].fetch(derived["fi_ytunnus"], legal_name=legal_name)))
    if "lv_regcode" in derived:
        _w1.append(("ur_latvia", REGISTRY["ur_latvia"].fetch(derived["lv_regcode"], legal_name=legal_name)))
    if "lt_code" in derived:
        _w1.append(("jar_lithuania", REGISTRY["jar_lithuania"].fetch(derived["lt_code"], legal_name=legal_name)))
    if "cz_ico" in derived:
        _w1.append(("ares", REGISTRY["ares"].fetch(derived["cz_ico"], legal_name=legal_name)))
    if "pl_krs" in derived:
        _w1.append(("krs_poland", REGISTRY["krs_poland"].fetch(derived["pl_krs"], legal_name=legal_name)))
    if "at_fn" in derived:
        _w1.append(("firmenbuch", REGISTRY["firmenbuch"].fetch(derived["at_fn"], legal_name=legal_name)))
    if "sk_ico" in derived:
        _w1.append(("rpo_slovakia", REGISTRY["rpo_slovakia"].fetch(derived["sk_ico"])))
        _w1.append(("rpvs_slovakia", REGISTRY["rpvs_slovakia"].fetch(derived["sk_ico"])))
    if "be_enterprise_number" in derived:
        _w1.append(("bce_belgium", REGISTRY["bce_belgium"].fetch(derived["be_enterprise_number"], legal_name=legal_name)))
    if ocid:
        _w1.append(("opencorporates", REGISTRY["opencorporates"].fetch(ocid)))
    if qid:
        _w1.append(("wikidata", wikidata_adapter.fetch(qid)))
    if bq_adapter and bq_adapter.info.live_available:
        _w1.append(("brightquery", bq_adapter.fetch(lei)))
    for _src_id in ("opensanctions",):
        _adp = REGISTRY.get(_src_id)
        if _adp and SearchKind.ENTITY in _adp.info.supports:
            _w1.append((_src_id, _adp.search(lei, SearchKind.ENTITY)))
    if oa_adapter is not None:
        _w1.append(("openaleph", _openaleph_strategies()))
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei"):
        _w1.append(("climatetrace", ct_adapter.fetch_by_lei(lei)))
    if bods_gleif_adapter is not None and hasattr(bods_gleif_adapter, "fetch_by_lei"):
        _w1.append(("bods_gleif", bods_gleif_adapter.fetch_by_lei(lei)))

    _w1_labels = [_lbl for _lbl, _ in _w1]
    _w1_raw = await asyncio.gather(*[_c for _, _c in _w1], return_exceptions=True)
    _r: dict[str, Any] = dict(zip(_w1_labels, _w1_raw))

    # ── Process Wave 1 results (CPU-only, no I/O) ─────────────────────────────

    # Companies House
    if "gb_coh" in derived:
        _b = _r.get("companies_house")
        if isinstance(_b, Exception):
            errors["companies_house"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _profile = _b.get("profile") or {}
            hits.append(SourceHit(
                source_id="companies_house",
                hit_id=derived["gb_coh"],
                kind=SearchKind.ENTITY,
                name=_profile.get("company_name", legal_name or ""),
                summary=f"GB-COH {derived['gb_coh']}",
                identifiers={"gb_coh": derived["gb_coh"], "lei": lei, **({"wikidata_qid": qid} if qid else {})},
                raw=_profile,
                is_stub=False,
            ))
            deepened_bundles.append(("companies_house", derived["gb_coh"]))

    # Zefix
    if "che_uid" in derived:
        _b = _r.get("zefix")
        if isinstance(_b, Exception):
            errors["zefix"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _company = _b.get("company") or {}
            hits.append(SourceHit(
                source_id="zefix",
                hit_id=derived["che_uid"],
                kind=SearchKind.ENTITY,
                name=_company.get("name") or legal_name or "",
                summary=f"CHE {derived['che_uid']}",
                identifiers={"che_uid": derived["che_uid"], "lei": lei},
                raw=_company,
                is_stub=False,
            ))
            deepened_bundles.append(("zefix", derived["che_uid"]))

    # KvK
    if "kvk_number" in derived:
        _b = _r.get("kvk")
        if isinstance(_b, Exception):
            errors["kvk"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="kvk",
                hit_id=derived["kvk_number"],
                kind=SearchKind.ENTITY,
                name=legal_name or "",
                summary=f"KvK {derived['kvk_number']}",
                identifiers={"kvk_number": derived["kvk_number"], "lei": lei},
                raw=_b.get("company") or {},
                is_stub=False,
            ))
            deepened_bundles.append(("kvk", derived["kvk_number"]))

    # INPI
    if "siren" in derived:
        _b = _r.get("inpi")
        if isinstance(_b, Exception):
            errors["inpi"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _inpi_company = _b.get("company") or {}
            _inpi_name = (
                (((_inpi_company.get("identite") or {}).get("entreprise") or {}).get("denomination"))
                or legal_name or ""
            )
            hits.append(SourceHit(
                source_id="inpi",
                hit_id=derived["siren"],
                kind=SearchKind.ENTITY,
                name=_inpi_name,
                summary=f"FR-SIREN {derived['siren']}",
                identifiers={"siren": derived["siren"], "lei": lei},
                raw=_inpi_company,
                is_stub=False,
            ))
            deepened_bundles.append(("inpi", derived["siren"]))

    # Bolagsverket
    if "se_org_number" in derived:
        _b = _r.get("bolagsverket")
        if isinstance(_b, Exception):
            errors["bolagsverket"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _bv_company = _b.get("company") or {}
            _bv_name = _bv_company.get("namn") or _bv_company.get("name") or legal_name or ""
            _org_display = (
                f"{derived['se_org_number'][:6]}-{derived['se_org_number'][6:]}"
                if len(derived["se_org_number"]) == 10
                else derived["se_org_number"]
            )
            hits.append(SourceHit(
                source_id="bolagsverket",
                hit_id=derived["se_org_number"],
                kind=SearchKind.ENTITY,
                name=_bv_name,
                summary=f"SE-BLV {_org_display}",
                identifiers={"se_org_number": derived["se_org_number"], "lei": lei},
                raw=_bv_company,
                is_stub=False,
            ))
            deepened_bundles.append(("bolagsverket", derived["se_org_number"]))

    # e-Äriregister
    if "ee_registry_code" in derived:
        _b = _r.get("ariregister")
        if isinstance(_b, Exception):
            errors["ariregister"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="ariregister",
                hit_id=derived["ee_registry_code"],
                kind=SearchKind.ENTITY,
                name=_b.get("name") or legal_name or "",
                summary=f"EE-ARIREGISTER {derived['ee_registry_code']}",
                identifiers={"ee_registry_code": derived["ee_registry_code"], "lei": lei},
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("ariregister", derived["ee_registry_code"]))

    # Brreg
    if "no_orgnr" in derived:
        _b = _r.get("brreg")
        if isinstance(_b, Exception):
            errors["brreg"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _brreg_entity = _b.get("entity") or {}
            hits.append(SourceHit(
                source_id="brreg",
                hit_id=derived["no_orgnr"],
                kind=SearchKind.ENTITY,
                name=_brreg_entity.get("navn") or legal_name or "",
                summary=f"NO-ORGNR {derived['no_orgnr']}",
                identifiers={"no_orgnr": derived["no_orgnr"], "lei": lei},
                raw=_brreg_entity,
                is_stub=False,
            ))
            deepened_bundles.append(("brreg", derived["no_orgnr"]))

    # CRO
    if "ie_crn" in derived:
        _b = _r.get("cro")
        if isinstance(_b, Exception):
            errors["cro"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _cro_company = _b.get("company") or {}
            _cro_name = (_cro_company.get("company_name") or "").strip() or legal_name or ""
            hits.append(SourceHit(
                source_id="cro",
                hit_id=derived["ie_crn"],
                kind=SearchKind.ENTITY,
                name=_cro_name,
                summary=f"IE-CRN {derived['ie_crn']}",
                identifiers={"ie_crn": derived["ie_crn"], "lei": lei},
                raw=_cro_company,
                is_stub=False,
            ))
            deepened_bundles.append(("cro", derived["ie_crn"]))

    # PRH
    if "fi_ytunnus" in derived:
        _b = _r.get("prh")
        if isinstance(_b, Exception):
            errors["prh"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _prh_company = _b.get("company") or {}
            _prh_name = ""
            for _n in (_prh_company.get("names") or []):
                if not _n.get("endDate") and _n.get("order") == 0:
                    _prh_name = (_n.get("name") or "").strip()
                    break
            hits.append(SourceHit(
                source_id="prh",
                hit_id=derived["fi_ytunnus"],
                kind=SearchKind.ENTITY,
                name=_prh_name or legal_name or "",
                summary=f"FI-YTUNNUS {derived['fi_ytunnus']}",
                identifiers={"fi_ytunnus": derived["fi_ytunnus"], "lei": lei},
                raw=_prh_company,
                is_stub=False,
            ))
            deepened_bundles.append(("prh", derived["fi_ytunnus"]))

    # UR Latvia
    if "lv_regcode" in derived:
        _b = _r.get("ur_latvia")
        if isinstance(_b, Exception):
            errors["ur_latvia"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _lv_entity = _b.get("entity") or {}
            hits.append(SourceHit(
                source_id="ur_latvia",
                hit_id=derived["lv_regcode"],
                kind=SearchKind.ENTITY,
                name=(_lv_entity.get("name") or "").strip() or legal_name or "",
                summary=f"LV-UR {derived['lv_regcode']}",
                identifiers={"lv_regcode": derived["lv_regcode"], "lei": lei},
                raw=_lv_entity,
                is_stub=False,
            ))
            deepened_bundles.append(("ur_latvia", derived["lv_regcode"]))

    # JAR Lithuania
    if "lt_code" in derived:
        _b = _r.get("jar_lithuania")
        if isinstance(_b, Exception):
            errors["jar_lithuania"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="jar_lithuania",
                hit_id=derived["lt_code"],
                kind=SearchKind.ENTITY,
                name=_b.get("name") or legal_name or "",
                summary=f"LT-JAR {derived['lt_code']}",
                identifiers={"lt_code": derived["lt_code"], "lei": lei},
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("jar_lithuania", derived["lt_code"]))

    # ARES
    if "cz_ico" in derived:
        _b = _r.get("ares")
        if isinstance(_b, Exception):
            errors["ares"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _cz_entity = _b.get("entity") or {}
            hits.append(SourceHit(
                source_id="ares",
                hit_id=derived["cz_ico"],
                kind=SearchKind.ENTITY,
                name=(_cz_entity.get("name") or "").strip() or legal_name or "",
                summary=f"CZ-ARES IČO {derived['cz_ico']}",
                identifiers={"cz_ico": derived["cz_ico"], "lei": lei},
                raw=_cz_entity,
                is_stub=False,
            ))
            deepened_bundles.append(("ares", derived["cz_ico"]))

    # KRS Poland
    if "pl_krs" in derived:
        _b = _r.get("krs_poland")
        if isinstance(_b, Exception):
            errors["krs_poland"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="krs_poland",
                hit_id=derived["pl_krs"],
                kind=SearchKind.ENTITY,
                name=(_b.get("name") or "").strip() or legal_name or "",
                summary=f"KRS {derived['pl_krs']}",
                identifiers={"pl_krs": derived["pl_krs"], "lei": lei},
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("krs_poland", derived["pl_krs"]))

    # Firmenbuch
    if "at_fn" in derived:
        _b = _r.get("firmenbuch")
        if isinstance(_b, Exception):
            errors["firmenbuch"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="firmenbuch",
                hit_id=derived["at_fn"],
                kind=SearchKind.ENTITY,
                name=(_b.get("name") or "").strip() or legal_name or "",
                summary=f"FN {derived['at_fn']}",
                identifiers={"at_fn": derived["at_fn"], "lei": lei},
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("firmenbuch", derived["at_fn"]))

    # RPO Slovakia + RPVS Slovakia (both keyed by sk_ico)
    if "sk_ico" in derived:
        _b = _r.get("rpo_slovakia")
        if isinstance(_b, Exception):
            errors["rpo_slovakia"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="rpo_slovakia",
                hit_id=derived["sk_ico"],
                kind=SearchKind.ENTITY,
                name=(_b.get("name") or "").strip() or legal_name or "",
                summary=f"SK-IČO {derived['sk_ico']}",
                identifiers={"sk_ico": derived["sk_ico"], "lei": lei},
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("rpo_slovakia", derived["sk_ico"]))

        _b = _r.get("rpvs_slovakia")
        if isinstance(_b, Exception):
            errors["rpvs_slovakia"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="rpvs_slovakia",
                hit_id=derived["sk_ico"],
                kind=SearchKind.ENTITY,
                name=(_b.get("name") or "").strip() or legal_name or "",
                summary=f"SK-IČO {derived['sk_ico']} · RPVS #{_b.get('partner_id', '')}",
                identifiers={
                    "sk_ico": derived["sk_ico"],
                    "lei": lei,
                    **({"rpvs_id": str(_b["partner_id"])} if _b.get("partner_id") else {}),
                },
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("rpvs_slovakia", derived["sk_ico"]))

    # BCE Belgium
    if "be_enterprise_number" in derived:
        _b = _r.get("bce_belgium")
        if isinstance(_b, Exception):
            errors["bce_belgium"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="bce_belgium",
                hit_id=derived["be_enterprise_number"],
                kind=SearchKind.ENTITY,
                name=_b.get("name") or legal_name or "",
                summary=f"BE {_b.get('dotted') or derived['be_enterprise_number']}",
                identifiers={"be_enterprise_number": derived["be_enterprise_number"], "lei": lei},
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("bce_belgium", derived["be_enterprise_number"]))

    # OpenCorporates — also extract edgar_cik if present for Wave 2
    if ocid:
        _b = _r.get("opencorporates")
        if isinstance(_b, Exception):
            errors["opencorporates"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _oc_company = _b.get("company") or {}
            hits.append(SourceHit(
                source_id="opencorporates",
                hit_id=ocid,
                kind=SearchKind.ENTITY,
                name=_oc_company.get("name") or legal_name or "",
                summary=f"OC {ocid} · {_oc_company.get('current_status', '')}",
                identifiers={
                    "ocid": ocid,
                    "lei": lei,
                    **({"gb_coh": derived["gb_coh"]} if "gb_coh" in derived else {}),
                },
                raw=_oc_company,
                is_stub=False,
            ))
            deepened_bundles.append(("opencorporates", ocid))
            # Extract EDGAR CIK so Wave 2 can skip the unreliable name-search.
            _oc_data = _oc_company.get("data") or {}
            for _entry in (_oc_data.get("most_recent") or []):
                _datum = (_entry.get("datum") or {}) if isinstance(_entry, dict) else {}
                if _datum.get("title") == "SEC Edgar entry" and _datum.get("description"):
                    _desc: str = _datum["description"]
                    if "register id:" in _desc:
                        _raw_cik = _desc.split("register id:")[-1].strip()
                        if _raw_cik.isdigit():
                            derived["edgar_cik"] = _raw_cik.lstrip("0") or "0"
                    break

    # Wikidata
    if qid:
        _b = _r.get("wikidata")
        if isinstance(_b, Exception):
            errors["wikidata"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _wd_summary = _b.get("summary") or {}
            hits.append(SourceHit(
                source_id="wikidata",
                hit_id=qid,
                kind=SearchKind.ENTITY,
                name=_wd_summary.get("label") or qid,
                summary=_wd_summary.get("description") or "",
                identifiers={
                    "wikidata_qid": qid,
                    "lei": lei,
                    **({"gb_coh": registered_as} if "gb_coh" in derived else {}),
                },
                raw=_wd_summary,
                is_stub=False,
            ))
            deepened_bundles.append(("wikidata", qid))

    # BrightQuery
    if bq_adapter and bq_adapter.info.live_available:
        _b = _r.get("brightquery")
        if isinstance(_b, Exception):
            errors["brightquery"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            hits.append(SourceHit(
                source_id="brightquery",
                hit_id=lei,
                kind=SearchKind.ENTITY,
                name=_b.get("name") or legal_name or "",
                summary=f"BrightQuery US entity · BQ ID {_b.get('bq_id', '')}",
                identifiers={"lei": lei, **({"bq_id": _b["bq_id"]} if _b.get("bq_id") else {})},
                raw=_b.get("company") or {},
                is_stub=False,
            ))
            deepened_bundles.append(("brightquery", lei))

    # OpenSanctions
    for _src_id in ("opensanctions",):
        _adp = REGISTRY.get(_src_id)
        if _adp and SearchKind.ENTITY in _adp.info.supports:
            _res = _r.get(_src_id)
            if isinstance(_res, Exception):
                errors[_src_id] = f"{type(_res).__name__}: {_res}"
            elif _res:
                for _hit in _res:
                    if not _hit.is_stub:
                        hits.append(_hit)
                        deepened_bundles.append((_src_id, _hit.hit_id))

    # OpenAleph
    if oa_adapter is not None:
        _res = _r.get("openaleph")
        if isinstance(_res, Exception):
            errors["openaleph"] = f"{type(_res).__name__}: {_res}"
        elif _res:
            for _hit in _res:
                if not _hit.is_stub:
                    hits.append(_hit)
                    deepened_bundles.append(("openaleph", _hit.hit_id))

    # Climate TRACE
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei"):
        _b = _r.get("climatetrace")
        if isinstance(_b, Exception):
            errors["climatetrace"] = f"{type(_b).__name__}: {_b}"
        elif _b and _b.get("entity_id"):
            _entity_id = _b.get("entity_id") or lei
            _emissions = _b.get("emissions") or {}
            _total_co2e = _emissions.get("total_co2e_tonnes")
            _summary_parts = [f"GEM entity {_entity_id}"]
            if _total_co2e is not None and _total_co2e > 0:
                if _total_co2e >= 1_000_000:
                    _summary_parts.append(f"{_total_co2e / 1_000_000:.1f} Mt CO₂e (2024)")
                else:
                    _summary_parts.append(f"{_total_co2e:,.0f} t CO₂e (2024)")
            hits.append(SourceHit(
                source_id="climatetrace",
                hit_id=_entity_id,
                kind=SearchKind.ENTITY,
                name=_b.get("entity_name") or legal_name or _entity_id,
                summary=" · ".join(_summary_parts),
                identifiers={"gem_entity_id": _entity_id, "lei": lei},
                raw=_b,
                is_stub=bool(_b.get("is_stub")),
            ))
            deepened_bundles.append(("climatetrace", _entity_id))

    # Open Ownership BODS GLEIF
    if bods_gleif_adapter is not None and hasattr(bods_gleif_adapter, "fetch_by_lei"):
        _b = _r.get("bods_gleif")
        if isinstance(_b, Exception):
            errors["bods_gleif"] = f"{type(_b).__name__}: {_b}"
        elif _b and not _b.get("is_stub"):
            _statementid = _b.get("hit_id") or lei
            _bg_name = legal_name or lei
            for _stmt in _b.get("bods_statements", []):
                if _stmt.get("statementType") == "entityStatement":
                    _bg_name = _stmt.get("recordDetails", {}).get("name") or _bg_name
                    break
            hits.append(SourceHit(
                source_id="bods_gleif",
                hit_id=_statementid,
                kind=SearchKind.ENTITY,
                name=_bg_name,
                summary="Open Ownership BODS v0.4 (bulk) · LEI match",
                identifiers={"lei": lei, "bods_gleif_statementid": _statementid},
                raw=_b,
                is_stub=False,
            ))
            deepened_bundles.append(("bods_gleif", _statementid))

    # ── Wave 2: SEC EDGAR ─────────────────────────────────────────────────────
    # Runs after OC result is processed so we can use the edgar_cik it may have
    # extracted. Direct-CIK path needs no I/O; name-search path needs one await.
    # -------------------------------------------------------------------------
    # SEC EDGAR — 13D/13G filings expose major shareholders (>5 %) of US-listed
    # companies (mandatory XML from December 2024 onward).
    # GLEIF jurisdiction codes for US entities are state-level (e.g. US-DE).
    #
    # Preferred path: use the EDGAR CIK extracted from OpenCorporates data above
    # (stored in derived["edgar_cik"]).  This avoids an unreliable name-search
    # round-trip and is unambiguous.
    # Fallback: name-based EDGAR company search if no CIK was derived.
    _edgar_cik = derived.get("edgar_cik")
    if jurisdiction.upper().startswith("US") and (_edgar_cik or legal_name):
        se_adapter = REGISTRY.get("sec_edgar")
        if se_adapter and se_adapter.info.live_available:
            try:
                if _edgar_cik:
                    # Direct CIK — no search round-trip needed.
                    hits.append(
                        SourceHit(
                            source_id="sec_edgar",
                            hit_id=_edgar_cik,
                            kind=SearchKind.ENTITY,
                            name=legal_name or "",
                            summary=f"CIK {_edgar_cik} · US listed company",
                            identifiers={"edgar_cik": _edgar_cik, "lei": lei},
                            raw={"cik": _edgar_cik, "name": legal_name or ""},
                            is_stub=False,
                        )
                    )
                    deepened_bundles.append(("sec_edgar", _edgar_cik))
                else:
                    # Fallback: search by company name.
                    se_hits = await se_adapter.search(legal_name, SearchKind.ENTITY)
                    if se_hits:
                        se_hit = se_hits[0]
                        hits.append(
                            SourceHit(
                                source_id="sec_edgar",
                                hit_id=se_hit.hit_id,
                                kind=SearchKind.ENTITY,
                                name=se_hit.name,
                                summary=se_hit.summary,
                                identifiers={
                                    "edgar_cik": se_hit.hit_id,
                                    "lei": lei,
                                },
                                raw=se_hit.raw,
                                is_stub=False,
                            )
                        )
                        deepened_bundles.append(("sec_edgar", se_hit.hit_id))
            except Exception as exc:  # noqa: BLE001
                errors["sec_edgar"] = f"{type(exc).__name__}: {exc}"

    # OpenSanctions, OpenTender — search by LEI (free-text, LEI is indexed).
    for source_id in ("opensanctions",):
        adapter = REGISTRY.get(source_id)
        if adapter is None or SearchKind.ENTITY not in adapter.info.supports:
            continue
        try:
            adapter_hits = await adapter.search(lei, SearchKind.ENTITY)
            for hit in adapter_hits:
                if hit.is_stub:
                    continue
                hits.append(hit)
                deepened_bundles.append((source_id, hit.hit_id))
        except Exception as exc:  # noqa: BLE001
            errors[source_id] = f"{type(exc).__name__}: {exc}"

    # OpenAleph — identifier-keyed lookup (LEI-anchored flow).
    # Tries three progressively broader strategies and stops at the first
    # that returns results, avoiding the noise of free-text name search:
    #   1. leiCode exact-match (most precise)
    #   2. opencorporatesUrl exact-match (GLEIF-derived ocid)
    #   3. registrationNumber + jurisdiction for each derived national ID
    oa_adapter = REGISTRY.get("openaleph")
    if oa_adapter is not None:
        try:
            oa_hits: list[SourceHit] = []

            # Strategy 1: leiCode filter
            oa_hits = await oa_adapter.fetch_by_lei(lei)  # type: ignore[attr-defined]

            # Strategy 2: opencorporatesUrl filter (GLEIF ocid → full OC URL)
            if not oa_hits and "ocid" in derived:
                oa_hits = await oa_adapter.fetch_by_oc_url(derived["ocid"])  # type: ignore[attr-defined]

            # Strategy 3: registrationNumber + jurisdiction for each national ID
            if not oa_hits:
                _reg_candidates = [
                    ("gb", derived.get("gb_coh")),
                    ("fr", derived.get("siren")),
                    ("nl", derived.get("kvk_number")),
                    ("se", derived.get("se_org_number")),
                    ("ch", derived.get("che_uid")),
                ]
                for _jur, _reg in _reg_candidates:
                    if _reg:
                        oa_hits = await oa_adapter.fetch_by_registration(_jur, _reg)  # type: ignore[attr-defined]
                        if oa_hits:
                            break

            # Strategy 4: legal name search (last resort — fuzzy, may return
            # multiple entities; useful for companies without identifier
            # properties in OpenAleph, e.g. procurement transparency entries).
            if not oa_hits and legal_name:
                oa_hits = await oa_adapter.fetch_by_name(legal_name)  # type: ignore[attr-defined]

            for hit in oa_hits:
                if hit.is_stub:
                    continue
                hits.append(hit)
                deepened_bundles.append(("openaleph", hit.hit_id))
        except Exception as exc:  # noqa: BLE001
            errors["openaleph"] = f"{type(exc).__name__}: {exc}"

    # Climate TRACE / GEM — ESG emissions data keyed by LEI.
    ct_adapter = REGISTRY.get("climatetrace")
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei"):
        try:
            ct_bundle = await ct_adapter.fetch_by_lei(lei)  # type: ignore[attr-defined]
            # Surface the hit whenever the LEI was found in the GEM index —
            # both live bundles (with emissions data) and GEM-only stubs
            # (LEI matched but live=false or emissions unavailable).
            # Stubs still show the entity name and GEM ID in the ESG panel;
            # the frontend renders the large CO₂e metric only when the
            # emissions dict is populated.
            if ct_bundle and ct_bundle.get("entity_id"):
                entity_id = ct_bundle.get("entity_id") or lei
                emissions = ct_bundle.get("emissions") or {}
                total_co2e = emissions.get("total_co2e_tonnes")
                summary_parts = [f"GEM entity {entity_id}"]
                if total_co2e is not None and total_co2e > 0:
                    # Express in Mt (megatonnes) for display if large enough.
                    if total_co2e >= 1_000_000:
                        summary_parts.append(
                            f"{total_co2e / 1_000_000:.1f} Mt CO₂e (2024)"
                        )
                    else:
                        summary_parts.append(
                            f"{total_co2e:,.0f} t CO₂e (2024)"
                        )
                is_stub = bool(ct_bundle.get("is_stub"))
                hits.append(
                    SourceHit(
                        source_id="climatetrace",
                        hit_id=entity_id,
                        kind=SearchKind.ENTITY,
                        name=ct_bundle.get("entity_name") or legal_name or entity_id,
                        summary=" · ".join(summary_parts),
                        identifiers={
                            "gem_entity_id": entity_id,
                            "lei": lei,
                        },
                        raw=ct_bundle,
                        is_stub=is_stub,
                    )
                )
                deepened_bundles.append(("climatetrace", entity_id))
        except Exception as exc:  # noqa: BLE001
            errors["climatetrace"] = f"{type(exc).__name__}: {exc}"

    # Open Ownership BODS GLEIF (bulk) — compare OO-processed BODS mapping
    # against the live GLEIF adapter to audit statement quality side-by-side.
    bods_gleif_adapter = REGISTRY.get("bods_gleif")
    if bods_gleif_adapter is not None and hasattr(bods_gleif_adapter, "fetch_by_lei"):
        try:
            bg_bundle = await bods_gleif_adapter.fetch_by_lei(lei)  # type: ignore[attr-defined]
            if bg_bundle and not bg_bundle.get("is_stub"):
                statementid = bg_bundle.get("hit_id") or lei
                # Derive a human-readable name from the first entity statement
                bg_name = legal_name or lei
                for stmt in bg_bundle.get("bods_statements", []):
                    if stmt.get("statementType") == "entityStatement":
                        bg_name = stmt.get("recordDetails", {}).get("name") or bg_name
                        break
                hits.append(
                    SourceHit(
                        source_id="bods_gleif",
                        hit_id=statementid,
                        kind=SearchKind.ENTITY,
                        name=bg_name,
                        summary="Open Ownership BODS v0.4 (bulk) · LEI match",
                        identifiers={"lei": lei, "bods_gleif_statementid": statementid},
                        raw=bg_bundle,
                        is_stub=False,
                    )
                )
                deepened_bundles.append(("bods_gleif", statementid))
        except Exception as exc:  # noqa: BLE001
            errors["bods_gleif"] = f"{type(exc).__name__}: {exc}"

    # Reconcile + run risk over search-time data.
    links = [link.to_dict() for link in reconcile(hits)]
    search_signals = [s.to_dict() for s in assess_hits(hits)]

    # Deepen up to N of the gathered hits and run BODS + risk.
    bods_all: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    deepen_signals: list[dict[str, Any]] = []
    license_notices: list[dict[str, str]] = []

    _deepen_pairs = deepened_bundles[:deepen_top]
    _deepen_raw = await asyncio.gather(
        *[_safe_deepen(_dsrc, _dhit) for _dsrc, _dhit in _deepen_pairs],
        return_exceptions=True,
    )
    for (_dsrc, _dhit), _deep in zip(_deepen_pairs, _deepen_raw):
        if isinstance(_deep, Exception):
            errors.setdefault(_dsrc, f"{type(_deep).__name__}: {_deep}")
            continue
        if _deep is None:
            continue
        bods_all.extend(_deep["bods"])
        bods_issues.extend(_deep["bods_issues"])
        deepen_signals.extend(_deep["risk_signals"])
        if _deep.get("license_notice"):
            license_notices.append({"source_id": _dsrc, "hit_id": _dhit, "notice": _deep["license_notice"]})

    # Cross-check names against OpenSanctions / EP and ICIJ — run in parallel.
    _cross_raw, _icij_raw = await asyncio.gather(
        assess_cross_source_names(bods_all),
        assess_icij_names(bods_all),
    )
    cross_signals = [s.to_dict() for s in _cross_raw]
    icij_signals = [s.to_dict() for s in _icij_raw]

    # Same merge logic as /report — collapse structural BODS signals
    # by code, keep per-source signals scoped, and scope RELATED_*
    # and ICIJ OFFSHORE_LEAKS signals to the matched statement.
    structural_codes = {
        "TRUST_OR_ARRANGEMENT",
        "NON_EU_JURISDICTION",
        "NOMINEE",
        "COMPLEX_OWNERSHIP_LAYERS",
        "COMPLEX_CORPORATE_STRUCTURE",
        "POSSIBLE_OBFUSCATION",
    }
    _statement_scoped = {"RELATED_PEP", "RELATED_SANCTIONED"}
    merged: dict[tuple, dict[str, Any]] = {}
    for sig in search_signals + deepen_signals + cross_signals + icij_signals:
        if sig["code"] in structural_codes:
            key: tuple = (sig["code"],)
        elif sig["code"] in _statement_scoped or (
            sig["code"] == "OFFSHORE_LEAKS" and sig.get("source_id") == "icij"
        ):
            key = (
                sig["code"],
                sig["source_id"],
                sig["hit_id"],
                sig.get("evidence", {}).get("subject_statement_id", ""),
            )
        else:
            key = (sig["code"], sig["source_id"], sig["hit_id"])
        merged[key] = sig

    return LookupResponse(
        query=lei,
        kind=SearchKind.ENTITY,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=list(merged.values()),
        bods=bods_all,
        bods_issues=bods_issues,
        license_notices=license_notices,
        lei=lei,
        legal_name=legal_name or None,
        jurisdiction=jurisdiction or None,
        derived_identifiers=derived,
    )


# ----------------------------------------------------------------------
# /lookup-stream — LEI-anchored lookup, streamed as SSE
# ----------------------------------------------------------------------
#
# Same logic as /lookup but results are emitted progressively as each
# source completes.  Event sequence:
#
#   gleif_done          — GLEIF resolved; carries lei, legal_name, jurisdiction,
#                         derived_identifiers.
#   sources_applicable  — list of source_ids that will be queried (use to
#                         render skeleton cards in the UI).
#   source_started      — one per source, fired immediately after tasks are
#                         created (before the result arrives).
#   hit                 — SourceHit JSON, emitted as each source returns data.
#   source_completed    — {source_id, hit_count}, one per source.
#   source_error        — {source_id, error}, one per source that fails.
#   cross_source_links  — after all sources have resolved and reconciliation ran.
#   risk_signals        — after deepen + risk pipeline.
#   done                — {lei, bods_issues, license_notices}; stream ends here.
#
# /lookup is preserved as a non-streaming endpoint for /export and other
# batch consumers that want the full response in a single JSON payload.


async def _lookup_stream_events(
    lei: str,
    deepen_top: int = 5,
) -> AsyncIterator[dict[str, Any]]:
    """SSE generator for /lookup-stream."""

    lei = lei.strip().upper()
    if not _LEI_SHAPE.match(lei):
        yield {
            "event": "error",
            "data": json.dumps(
                {
                    "detail": (
                        f"{lei!r} is not a valid LEI. ISO 17442 LEIs are "
                        "20-character alphanumeric strings."
                    )
                }
            ),
        }
        return

    gleif = REGISTRY["gleif"]
    wikidata_adapter = REGISTRY["wikidata"]

    # ── Step 1: GLEIF (must resolve before we know which sources apply) ───────
    yield {
        "event": "source_started",
        "data": json.dumps({"source_id": "gleif", "source_name": gleif.info.name}),
    }

    override_bundle = bods_data.gleif_bundle_for_lei(lei)
    legal_name = ""
    jurisdiction = ""
    registered_as = ""
    registered_at_id = ""
    gleif_bundle: dict[str, Any] = {}

    try:
        if override_bundle:
            legal_name, jurisdiction, registered_as = _subject_metadata_from_bundle(
                override_bundle, lei
            )
            if not legal_name:
                yield {
                    "event": "error",
                    "data": json.dumps(
                        {"detail": f"Found a BODS bundle for {lei} but couldn't locate the subject entity statement."}
                    ),
                }
                return
            gleif_bundle = {"source_id": "gleif", "lei": lei, "_from_bundle": True}
        else:
            gleif_bundle = await gleif.fetch(lei)
            if gleif_bundle.get("is_stub") or not gleif_bundle.get("record"):
                yield {
                    "event": "error",
                    "data": json.dumps(
                        {
                            "detail": (
                                f"No GLEIF record found for {lei}. Either the LEI is not registered, "
                                "live mode is disabled, or no Open Ownership bundle has been extracted."
                            )
                        }
                    ),
                }
                return
            record_attrs = (gleif_bundle.get("record") or {}).get("attributes") or {}
            entity_block = record_attrs.get("entity") or {}
            legal_name = (entity_block.get("legalName") or {}).get("name") or ""
            jurisdiction = entity_block.get("jurisdiction") or ""
            registered_as = entity_block.get("registeredAs") or ""
            registered_at_id = (entity_block.get("registeredAt") or {}).get("id") or ""
    except Exception as exc:  # noqa: BLE001
        yield {
            "event": "error",
            "data": json.dumps({"detail": f"GLEIF fetch failed: {type(exc).__name__}: {exc}"}),
        }
        return

    # Build derived identifiers (same logic as /lookup).
    derived: dict[str, str] = {"lei": lei}
    if jurisdiction.upper() == "GB" and registered_as:
        derived["gb_coh"] = registered_as
    if registered_at_id in _ZEFIX_RA_CODES and registered_as:
        derived["che_uid"] = _zefix_normalise_uid(registered_as)
    if registered_at_id == _KVK_RA_CODE and registered_as:
        derived["kvk_number"] = _normalise_kvk(registered_as)
    if registered_at_id == _INPI_RA_CODE and registered_as:
        derived["siren"] = _normalise_siren(registered_as)
    if registered_at_id == _BV_RA_CODE and registered_as:
        try:
            derived["se_org_number"] = _normalise_org_number(registered_as)
        except ValueError:
            pass
    if registered_at_id == _EE_RA_CODE and registered_as:
        derived["ee_registry_code"] = registered_as.strip().zfill(8)
    if registered_at_id == _BRREG_RA_CODE and registered_as:
        derived["no_orgnr"] = _normalise_orgnr(registered_as)
    if registered_at_id == _CRO_RA_CODE and registered_as:
        derived["ie_crn"] = _normalise_crn(registered_as)
    if registered_at_id == _PRH_RA_CODE and registered_as:
        derived["fi_ytunnus"] = _normalise_ytunnus(registered_as)
    if registered_at_id == _LV_RA_CODE and registered_as:
        derived["lv_regcode"] = _normalise_lv_regcode(registered_as)
    if registered_at_id == _LT_RA_CODE and registered_as:
        derived["lt_code"] = _normalise_lt_code(registered_as)
    if registered_at_id == _CZ_RA_CODE and registered_as:
        derived["cz_ico"] = _normalise_ico(registered_as)
    if registered_at_id == _PL_KRS_RA_CODE and registered_as:
        derived["pl_krs"] = _normalise_krs(registered_as)
    if registered_at_id == _AT_FB_RA_CODE and registered_as:
        derived["at_fn"] = _normalise_fn(registered_as)
    if registered_at_id == _SK_RPO_RA_CODE and registered_as:
        derived["sk_ico"] = _normalise_sk_ico(registered_as)
    if registered_at_id == _BCE_RA_CODE and registered_as:
        derived["be_enterprise_number"] = _normalise_enterprise_number(registered_as)

    # OC ID from GLEIF Level-1 (same as /lookup).
    ocid: str | None = None
    if gleif.info.live_available:
        try:
            _gleif_src = (
                gleif_bundle
                if not gleif_bundle.get("_from_bundle")
                else await gleif.fetch(lei)
            )
            if not _gleif_src.get("is_stub"):
                _attrs = (_gleif_src.get("record") or {}).get("attributes") or {}
                ocid = _attrs.get("ocid") or None
        except Exception:  # noqa: BLE001
            pass
    if ocid:
        derived["ocid"] = ocid

    # Wikidata QID.
    qid = None
    if hasattr(wikidata_adapter, "find_qid_by_lei"):
        qid = await wikidata_adapter.find_qid_by_lei(lei)  # type: ignore[attr-defined]
    if qid:
        derived["wikidata_qid"] = qid

    # Emit GLEIF resolved.
    yield {
        "event": "gleif_done",
        "data": json.dumps(
            {
                "lei": lei,
                "legal_name": legal_name or None,
                "jurisdiction": jurisdiction or None,
                "derived_identifiers": derived,
            }
        ),
    }

    # Surface GLEIF as the first hit.
    gleif_hit = SourceHit(
        source_id="gleif",
        hit_id=lei,
        kind=SearchKind.ENTITY,
        name=legal_name or f"LEI {lei}",
        summary=f"LEI {lei} · {jurisdiction}",
        identifiers={
            "lei": lei,
            **({"gb_coh": registered_as} if "gb_coh" in derived else {}),
            **({"che_uid": derived["che_uid"]} if "che_uid" in derived else {}),
            **({"kvk_number": derived["kvk_number"]} if "kvk_number" in derived else {}),
            **({"siren": derived["siren"]} if "siren" in derived else {}),
            **({"se_org_number": derived["se_org_number"]} if "se_org_number" in derived else {}),
            **({"ee_registry_code": derived["ee_registry_code"]} if "ee_registry_code" in derived else {}),
            **({"no_orgnr": derived["no_orgnr"]} if "no_orgnr" in derived else {}),
            **({"ie_crn": derived["ie_crn"]} if "ie_crn" in derived else {}),
            **({"fi_ytunnus": derived["fi_ytunnus"]} if "fi_ytunnus" in derived else {}),
            **({"lv_regcode": derived["lv_regcode"]} if "lv_regcode" in derived else {}),
            **({"lt_code": derived["lt_code"]} if "lt_code" in derived else {}),
            **({"cz_ico": derived["cz_ico"]} if "cz_ico" in derived else {}),
            **({"pl_krs": derived["pl_krs"]} if "pl_krs" in derived else {}),
            **({"at_fn": derived["at_fn"]} if "at_fn" in derived else {}),
            **({"sk_ico": derived["sk_ico"]} if "sk_ico" in derived else {}),
            **({"be_enterprise_number": derived["be_enterprise_number"]} if "be_enterprise_number" in derived else {}),
            **({"wikidata_qid": qid} if qid else {}),
        },
        raw=gleif_bundle.get("record") or {},
        is_stub=False,
    )
    yield {"event": "hit", "data": gleif_hit.model_dump_json()}
    yield {"event": "source_completed", "data": json.dumps({"source_id": "gleif", "hit_count": 1})}

    # ── Step 2: Announce which sources will be queried ────────────────────────
    bq_adapter = REGISTRY.get("brightquery")
    oa_adapter = REGISTRY.get("openaleph")
    ct_adapter = REGISTRY.get("climatetrace")
    bods_gleif_adapter = REGISTRY.get("bods_gleif")
    se_adapter = REGISTRY.get("sec_edgar")

    applicable_ids: list[str] = []
    if "gb_coh" in derived:
        applicable_ids.append("companies_house")
    if "che_uid" in derived:
        applicable_ids.append("zefix")
    if "kvk_number" in derived:
        applicable_ids.append("kvk")
    if "siren" in derived:
        applicable_ids.append("inpi")
    if "se_org_number" in derived:
        applicable_ids.append("bolagsverket")
    if "ee_registry_code" in derived:
        applicable_ids.append("ariregister")
    if "no_orgnr" in derived:
        applicable_ids.append("brreg")
    if "ie_crn" in derived:
        applicable_ids.append("cro")
    if "fi_ytunnus" in derived:
        applicable_ids.append("prh")
    if "lv_regcode" in derived:
        applicable_ids.append("ur_latvia")
    if "lt_code" in derived:
        applicable_ids.append("jar_lithuania")
    if "cz_ico" in derived:
        applicable_ids.append("ares")
    if "pl_krs" in derived:
        applicable_ids.append("krs_poland")
    if "at_fn" in derived:
        applicable_ids.append("firmenbuch")
    if "sk_ico" in derived:
        applicable_ids.extend(["rpo_slovakia", "rpvs_slovakia"])
    if "be_enterprise_number" in derived:
        applicable_ids.append("bce_belgium")
    if ocid:
        applicable_ids.append("opencorporates")
    if qid:
        applicable_ids.append("wikidata")
    if bq_adapter and bq_adapter.info.live_available:
        applicable_ids.append("brightquery")
    _os_adp = REGISTRY.get("opensanctions")
    if _os_adp and SearchKind.ENTITY in _os_adp.info.supports:
        applicable_ids.append("opensanctions")
    if oa_adapter is not None:
        applicable_ids.append("openaleph")
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei"):
        applicable_ids.append("climatetrace")
    if bods_gleif_adapter is not None and hasattr(bods_gleif_adapter, "fetch_by_lei"):
        applicable_ids.append("bods_gleif")
    if jurisdiction.upper().startswith("US") and (derived.get("edgar_cik") or legal_name):
        if se_adapter and se_adapter.info.live_available:
            applicable_ids.append("sec_edgar")

    yield {
        "event": "sources_applicable",
        "data": json.dumps({"source_ids": applicable_ids}),
    }

    # ── Step 3: Create tasks and fire source_started events ───────────────────
    # Each task wraps a coroutine and returns (source_id, result_or_exception).
    async def _run(src_id: str, coro: Any) -> tuple[str, Any]:
        try:
            return src_id, await coro
        except Exception as exc:  # noqa: BLE001
            return src_id, exc

    # OpenAleph: wrap the sequential fallback chain into one task.
    async def _openaleph_strategies() -> list[SourceHit]:
        if oa_adapter is None:
            return []
        _oa: list[SourceHit] = await oa_adapter.fetch_by_lei(lei)  # type: ignore[attr-defined]
        if not _oa and "ocid" in derived:
            _oa = await oa_adapter.fetch_by_oc_url(derived["ocid"])  # type: ignore[attr-defined]
        if not _oa:
            for _jur, _reg in [
                ("gb", derived.get("gb_coh")),
                ("fr", derived.get("siren")),
                ("nl", derived.get("kvk_number")),
                ("se", derived.get("se_org_number")),
                ("ch", derived.get("che_uid")),
            ]:
                if _reg:
                    _oa = await oa_adapter.fetch_by_registration(_jur, _reg)  # type: ignore[attr-defined]
                    if _oa:
                        break
        if not _oa and legal_name:
            _oa = await oa_adapter.fetch_by_name(legal_name)  # type: ignore[attr-defined]
        return _oa

    tasks: dict[asyncio.Task[tuple[str, Any]], None] = {}

    def _add_task(src_id: str, coro: Any) -> None:
        tasks[asyncio.create_task(_run(src_id, coro))] = None

    adapterIndex: dict[str, str] = {
        s.id: s.info.name for s in REGISTRY.values()  # type: ignore[attr-defined]
    }

    if "gb_coh" in derived:
        _add_task("companies_house", REGISTRY["companies_house"].fetch(derived["gb_coh"]))
    if "che_uid" in derived:
        _add_task("zefix", REGISTRY["zefix"].fetch(derived["che_uid"]))
    if "kvk_number" in derived:
        _add_task("kvk", REGISTRY["kvk"].fetch(derived["kvk_number"], legal_name=legal_name))
    if "siren" in derived:
        _add_task("inpi", REGISTRY["inpi"].fetch(derived["siren"]))
    if "se_org_number" in derived:
        _add_task("bolagsverket", REGISTRY["bolagsverket"].fetch(derived["se_org_number"], legal_name=legal_name))
    if "ee_registry_code" in derived:
        _add_task("ariregister", REGISTRY["ariregister"].fetch(derived["ee_registry_code"], legal_name=legal_name))
    if "no_orgnr" in derived:
        _add_task("brreg", REGISTRY["brreg"].fetch(derived["no_orgnr"], legal_name=legal_name))
    if "ie_crn" in derived:
        _add_task("cro", REGISTRY["cro"].fetch(derived["ie_crn"], legal_name=legal_name))
    if "fi_ytunnus" in derived:
        _add_task("prh", REGISTRY["prh"].fetch(derived["fi_ytunnus"], legal_name=legal_name))
    if "lv_regcode" in derived:
        _add_task("ur_latvia", REGISTRY["ur_latvia"].fetch(derived["lv_regcode"], legal_name=legal_name))
    if "lt_code" in derived:
        _add_task("jar_lithuania", REGISTRY["jar_lithuania"].fetch(derived["lt_code"], legal_name=legal_name))
    if "cz_ico" in derived:
        _add_task("ares", REGISTRY["ares"].fetch(derived["cz_ico"], legal_name=legal_name))
    if "pl_krs" in derived:
        _add_task("krs_poland", REGISTRY["krs_poland"].fetch(derived["pl_krs"], legal_name=legal_name))
    if "at_fn" in derived:
        _add_task("firmenbuch", REGISTRY["firmenbuch"].fetch(derived["at_fn"], legal_name=legal_name))
    if "sk_ico" in derived:
        _add_task("rpo_slovakia", REGISTRY["rpo_slovakia"].fetch(derived["sk_ico"]))
        _add_task("rpvs_slovakia", REGISTRY["rpvs_slovakia"].fetch(derived["sk_ico"]))
    if "be_enterprise_number" in derived:
        _add_task("bce_belgium", REGISTRY["bce_belgium"].fetch(derived["be_enterprise_number"], legal_name=legal_name))
    if ocid:
        _add_task("opencorporates", REGISTRY["opencorporates"].fetch(ocid))
    if qid:
        _add_task("wikidata", wikidata_adapter.fetch(qid))
    if bq_adapter and bq_adapter.info.live_available:
        _add_task("brightquery", bq_adapter.fetch(lei))
    if _os_adp and SearchKind.ENTITY in _os_adp.info.supports:
        _add_task("opensanctions", _os_adp.search(lei, SearchKind.ENTITY))
    if oa_adapter is not None:
        _add_task("openaleph", _openaleph_strategies())
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei"):
        _add_task("climatetrace", ct_adapter.fetch_by_lei(lei))
    if bods_gleif_adapter is not None and hasattr(bods_gleif_adapter, "fetch_by_lei"):
        _add_task("bods_gleif", bods_gleif_adapter.fetch_by_lei(lei))

    # Emit source_started for all tasks at once (before any complete).
    for src_id in applicable_ids:
        if src_id == "sec_edgar":
            continue  # sec_edgar is Wave 2; started separately below
        src_name = REGISTRY[src_id].info.name if src_id in REGISTRY else src_id
        yield {
            "event": "source_started",
            "data": json.dumps({"source_id": src_id, "source_name": src_name}),
        }

    # ── Step 4: Stream results as tasks complete ──────────────────────────────
    hits: list[SourceHit] = [gleif_hit]
    errors: dict[str, str] = {}
    deepened_bundles: list[tuple[str, str]] = [("gleif", lei)]
    oc_result_processed = False

    pending = set(tasks.keys())
    while pending:
        done_set, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done_set:
            source_id, result = task.result()

            if isinstance(result, Exception):
                errors[source_id] = f"{type(result).__name__}: {result}"
                yield {
                    "event": "source_error",
                    "data": json.dumps({"source_id": source_id, "error": errors[source_id]}),
                }
                continue

            # ── per-source result → SourceHit ────────────────────────────────
            hit: SourceHit | None = None

            if source_id == "companies_house" and "gb_coh" in derived:
                if result and not result.get("is_stub"):
                    _p = result.get("profile") or {}
                    hit = SourceHit(
                        source_id="companies_house", hit_id=derived["gb_coh"],
                        kind=SearchKind.ENTITY,
                        name=_p.get("company_name", legal_name or ""),
                        summary=f"GB-COH {derived['gb_coh']}",
                        identifiers={"gb_coh": derived["gb_coh"], "lei": lei, **({"wikidata_qid": qid} if qid else {})},
                        raw=_p, is_stub=False,
                    )

            elif source_id == "zefix" and "che_uid" in derived:
                if result and not result.get("is_stub"):
                    _c = result.get("company") or {}
                    hit = SourceHit(
                        source_id="zefix", hit_id=derived["che_uid"],
                        kind=SearchKind.ENTITY,
                        name=_c.get("name") or legal_name or "",
                        summary=f"CHE {derived['che_uid']}",
                        identifiers={"che_uid": derived["che_uid"], "lei": lei},
                        raw=_c, is_stub=False,
                    )

            elif source_id == "kvk" and "kvk_number" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="kvk", hit_id=derived["kvk_number"],
                        kind=SearchKind.ENTITY, name=legal_name or "",
                        summary=f"KvK {derived['kvk_number']}",
                        identifiers={"kvk_number": derived["kvk_number"], "lei": lei},
                        raw=result.get("company") or {}, is_stub=False,
                    )

            elif source_id == "inpi" and "siren" in derived:
                if result and not result.get("is_stub"):
                    _ic = result.get("company") or {}
                    _in = ((((_ic.get("identite") or {}).get("entreprise") or {}).get("denomination")) or legal_name or "")
                    hit = SourceHit(
                        source_id="inpi", hit_id=derived["siren"],
                        kind=SearchKind.ENTITY, name=_in,
                        summary=f"FR-SIREN {derived['siren']}",
                        identifiers={"siren": derived["siren"], "lei": lei},
                        raw=_ic, is_stub=False,
                    )

            elif source_id == "bolagsverket" and "se_org_number" in derived:
                if result and not result.get("is_stub"):
                    _bc = result.get("company") or {}
                    _bn = _bc.get("namn") or _bc.get("name") or legal_name or ""
                    _od = (f"{derived['se_org_number'][:6]}-{derived['se_org_number'][6:]}"
                           if len(derived["se_org_number"]) == 10 else derived["se_org_number"])
                    hit = SourceHit(
                        source_id="bolagsverket", hit_id=derived["se_org_number"],
                        kind=SearchKind.ENTITY, name=_bn, summary=f"SE-BLV {_od}",
                        identifiers={"se_org_number": derived["se_org_number"], "lei": lei},
                        raw=_bc, is_stub=False,
                    )

            elif source_id == "ariregister" and "ee_registry_code" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="ariregister", hit_id=derived["ee_registry_code"],
                        kind=SearchKind.ENTITY,
                        name=result.get("name") or legal_name or "",
                        summary=f"EE-ARIREGISTER {derived['ee_registry_code']}",
                        identifiers={"ee_registry_code": derived["ee_registry_code"], "lei": lei},
                        raw=result, is_stub=False,
                    )

            elif source_id == "brreg" and "no_orgnr" in derived:
                if result and not result.get("is_stub"):
                    _be = result.get("entity") or {}
                    hit = SourceHit(
                        source_id="brreg", hit_id=derived["no_orgnr"],
                        kind=SearchKind.ENTITY,
                        name=_be.get("navn") or legal_name or "",
                        summary=f"NO-ORGNR {derived['no_orgnr']}",
                        identifiers={"no_orgnr": derived["no_orgnr"], "lei": lei},
                        raw=_be, is_stub=False,
                    )

            elif source_id == "cro" and "ie_crn" in derived:
                if result and not result.get("is_stub"):
                    _cc = result.get("company") or {}
                    hit = SourceHit(
                        source_id="cro", hit_id=derived["ie_crn"],
                        kind=SearchKind.ENTITY,
                        name=(_cc.get("company_name") or "").strip() or legal_name or "",
                        summary=f"IE-CRN {derived['ie_crn']}",
                        identifiers={"ie_crn": derived["ie_crn"], "lei": lei},
                        raw=_cc, is_stub=False,
                    )

            elif source_id == "prh" and "fi_ytunnus" in derived:
                if result and not result.get("is_stub"):
                    _pc = result.get("company") or {}
                    _pn = ""
                    for _n in (_pc.get("names") or []):
                        if not _n.get("endDate") and _n.get("order") == 0:
                            _pn = (_n.get("name") or "").strip()
                            break
                    hit = SourceHit(
                        source_id="prh", hit_id=derived["fi_ytunnus"],
                        kind=SearchKind.ENTITY, name=_pn or legal_name or "",
                        summary=f"FI-YTUNNUS {derived['fi_ytunnus']}",
                        identifiers={"fi_ytunnus": derived["fi_ytunnus"], "lei": lei},
                        raw=_pc, is_stub=False,
                    )

            elif source_id == "ur_latvia" and "lv_regcode" in derived:
                if result and not result.get("is_stub"):
                    _le = result.get("entity") or {}
                    hit = SourceHit(
                        source_id="ur_latvia", hit_id=derived["lv_regcode"],
                        kind=SearchKind.ENTITY,
                        name=(_le.get("name") or "").strip() or legal_name or "",
                        summary=f"LV-UR {derived['lv_regcode']}",
                        identifiers={"lv_regcode": derived["lv_regcode"], "lei": lei},
                        raw=_le, is_stub=False,
                    )

            elif source_id == "jar_lithuania" and "lt_code" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="jar_lithuania", hit_id=derived["lt_code"],
                        kind=SearchKind.ENTITY,
                        name=result.get("name") or legal_name or "",
                        summary=f"LT-JAR {derived['lt_code']}",
                        identifiers={"lt_code": derived["lt_code"], "lei": lei},
                        raw=result, is_stub=False,
                    )

            elif source_id == "ares" and "cz_ico" in derived:
                if result and not result.get("is_stub"):
                    _ce = result.get("entity") or {}
                    hit = SourceHit(
                        source_id="ares", hit_id=derived["cz_ico"],
                        kind=SearchKind.ENTITY,
                        name=(_ce.get("name") or "").strip() or legal_name or "",
                        summary=f"CZ-ARES IČO {derived['cz_ico']}",
                        identifiers={"cz_ico": derived["cz_ico"], "lei": lei},
                        raw=_ce, is_stub=False,
                    )

            elif source_id == "krs_poland" and "pl_krs" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="krs_poland", hit_id=derived["pl_krs"],
                        kind=SearchKind.ENTITY,
                        name=(result.get("name") or "").strip() or legal_name or "",
                        summary=f"KRS {derived['pl_krs']}",
                        identifiers={"pl_krs": derived["pl_krs"], "lei": lei},
                        raw=result, is_stub=False,
                    )

            elif source_id == "firmenbuch" and "at_fn" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="firmenbuch", hit_id=derived["at_fn"],
                        kind=SearchKind.ENTITY,
                        name=(result.get("name") or "").strip() or legal_name or "",
                        summary=f"FN {derived['at_fn']}",
                        identifiers={"at_fn": derived["at_fn"], "lei": lei},
                        raw=result, is_stub=False,
                    )

            elif source_id == "rpo_slovakia" and "sk_ico" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="rpo_slovakia", hit_id=derived["sk_ico"],
                        kind=SearchKind.ENTITY,
                        name=(result.get("name") or "").strip() or legal_name or "",
                        summary=f"SK-IČO {derived['sk_ico']}",
                        identifiers={"sk_ico": derived["sk_ico"], "lei": lei},
                        raw=result, is_stub=False,
                    )

            elif source_id == "rpvs_slovakia" and "sk_ico" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="rpvs_slovakia", hit_id=derived["sk_ico"],
                        kind=SearchKind.ENTITY,
                        name=(result.get("name") or "").strip() or legal_name or "",
                        summary=f"SK-IČO {derived['sk_ico']} · RPVS #{result.get('partner_id', '')}",
                        identifiers={
                            "sk_ico": derived["sk_ico"], "lei": lei,
                            **({"rpvs_id": str(result["partner_id"])} if result.get("partner_id") else {}),
                        },
                        raw=result, is_stub=False,
                    )

            elif source_id == "bce_belgium" and "be_enterprise_number" in derived:
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="bce_belgium", hit_id=derived["be_enterprise_number"],
                        kind=SearchKind.ENTITY,
                        name=result.get("name") or legal_name or "",
                        summary=f"BE {result.get('dotted') or derived['be_enterprise_number']}",
                        identifiers={"be_enterprise_number": derived["be_enterprise_number"], "lei": lei},
                        raw=result, is_stub=False,
                    )

            elif source_id == "opencorporates" and ocid:
                if result and not result.get("is_stub"):
                    _oc = result.get("company") or {}
                    hit = SourceHit(
                        source_id="opencorporates", hit_id=ocid,
                        kind=SearchKind.ENTITY,
                        name=_oc.get("name") or legal_name or "",
                        summary=f"OC {ocid} · {_oc.get('current_status', '')}",
                        identifiers={"ocid": ocid, "lei": lei, **({"gb_coh": derived["gb_coh"]} if "gb_coh" in derived else {})},
                        raw=_oc, is_stub=False,
                    )
                    # Extract edgar_cik for Wave 2.
                    _od = _oc.get("data") or {}
                    for _entry in (_od.get("most_recent") or []):
                        _datum = (_entry.get("datum") or {}) if isinstance(_entry, dict) else {}
                        if _datum.get("title") == "SEC Edgar entry" and _datum.get("description"):
                            _desc: str = _datum["description"]
                            if "register id:" in _desc:
                                _raw_cik = _desc.split("register id:")[-1].strip()
                                if _raw_cik.isdigit():
                                    derived["edgar_cik"] = _raw_cik.lstrip("0") or "0"
                            break
                    if not oc_result_processed and jurisdiction.upper().startswith("US"):
                        oc_result_processed = True
                        # If we now have a CIK, add EDGAR task.
                        _edgar_cik = derived.get("edgar_cik")
                        if _edgar_cik and se_adapter and se_adapter.info.live_available:
                            # Direct CIK hit: no I/O needed, emit immediately.
                            _edgar_hit = SourceHit(
                                source_id="sec_edgar", hit_id=_edgar_cik,
                                kind=SearchKind.ENTITY, name=legal_name or "",
                                summary=f"CIK {_edgar_cik} · US listed company",
                                identifiers={"edgar_cik": _edgar_cik, "lei": lei},
                                raw={"cik": _edgar_cik, "name": legal_name or ""},
                                is_stub=False,
                            )
                            hits.append(_edgar_hit)
                            deepened_bundles.append(("sec_edgar", _edgar_cik))
                            yield {"event": "source_started", "data": json.dumps({"source_id": "sec_edgar", "source_name": se_adapter.info.name})}
                            yield {"event": "hit", "data": _edgar_hit.model_dump_json()}
                            yield {"event": "source_completed", "data": json.dumps({"source_id": "sec_edgar", "hit_count": 1})}

            elif source_id == "wikidata" and qid:
                if result and not result.get("is_stub"):
                    _ws = result.get("summary") or {}
                    hit = SourceHit(
                        source_id="wikidata", hit_id=qid,
                        kind=SearchKind.ENTITY,
                        name=_ws.get("label") or qid,
                        summary=_ws.get("description") or "",
                        identifiers={"wikidata_qid": qid, "lei": lei, **({"gb_coh": registered_as} if "gb_coh" in derived else {})},
                        raw=_ws, is_stub=False,
                    )

            elif source_id == "brightquery":
                if result and not result.get("is_stub"):
                    hit = SourceHit(
                        source_id="brightquery", hit_id=lei,
                        kind=SearchKind.ENTITY,
                        name=result.get("name") or legal_name or "",
                        summary=f"BrightQuery US entity · BQ ID {result.get('bq_id', '')}",
                        identifiers={"lei": lei, **({"bq_id": result["bq_id"]} if result.get("bq_id") else {})},
                        raw=result.get("company") or {}, is_stub=False,
                    )

            elif source_id == "opensanctions":
                # result is a list[SourceHit]
                if isinstance(result, list):
                    for _sh in result:
                        if not _sh.is_stub:
                            hits.append(_sh)
                            deepened_bundles.append(("opensanctions", _sh.hit_id))
                            yield {"event": "hit", "data": _sh.model_dump_json()}
                    yield {"event": "source_completed", "data": json.dumps({"source_id": "opensanctions", "hit_count": sum(1 for _sh in result if not _sh.is_stub)})}
                    continue  # already emitted hits + completed; skip the standard block below

            elif source_id == "openaleph":
                # result is list[SourceHit]
                if isinstance(result, list):
                    for _sh in result:
                        if not _sh.is_stub:
                            hits.append(_sh)
                            deepened_bundles.append(("openaleph", _sh.hit_id))
                            yield {"event": "hit", "data": _sh.model_dump_json()}
                    yield {"event": "source_completed", "data": json.dumps({"source_id": "openaleph", "hit_count": sum(1 for _sh in result if not _sh.is_stub)})}
                    continue

            elif source_id == "climatetrace":
                if result and result.get("entity_id"):
                    _eid = result.get("entity_id") or lei
                    _em = result.get("emissions") or {}
                    _tco2 = _em.get("total_co2e_tonnes")
                    _sp = [f"GEM entity {_eid}"]
                    if _tco2 is not None and _tco2 > 0:
                        if _tco2 >= 1_000_000:
                            _sp.append(f"{_tco2 / 1_000_000:.1f} Mt CO₂e (2024)")
                        else:
                            _sp.append(f"{_tco2:,.0f} t CO₂e (2024)")
                    hit = SourceHit(
                        source_id="climatetrace", hit_id=_eid,
                        kind=SearchKind.ENTITY,
                        name=result.get("entity_name") or legal_name or _eid,
                        summary=" · ".join(_sp),
                        identifiers={"gem_entity_id": _eid, "lei": lei},
                        raw=result, is_stub=bool(result.get("is_stub")),
                    )
                    if hit:
                        deepened_bundles.append(("climatetrace", _eid))

            elif source_id == "bods_gleif":
                if result and not result.get("is_stub"):
                    _sid = result.get("hit_id") or lei
                    _bgn = legal_name or lei
                    for _stmt in result.get("bods_statements", []):
                        if _stmt.get("statementType") == "entityStatement":
                            _bgn = _stmt.get("recordDetails", {}).get("name") or _bgn
                            break
                    hit = SourceHit(
                        source_id="bods_gleif", hit_id=_sid,
                        kind=SearchKind.ENTITY, name=_bgn,
                        summary="Open Ownership BODS v0.4 (bulk) · LEI match",
                        identifiers={"lei": lei, "bods_gleif_statementid": _sid},
                        raw=result, is_stub=False,
                    )
                    if hit:
                        deepened_bundles.append(("bods_gleif", _sid))

            # Standard hit emit + source_completed.
            if hit is not None:
                hits.append(hit)
                deepened_bundles.append((source_id, hit.hit_id))
                yield {"event": "hit", "data": hit.model_dump_json()}
                yield {"event": "source_completed", "data": json.dumps({"source_id": source_id, "hit_count": 1})}
            else:
                yield {"event": "source_completed", "data": json.dumps({"source_id": source_id, "hit_count": 0})}

    # Wave 2 fallback: EDGAR name-search if OC never fired / no CIK derived.
    _edgar_cik = derived.get("edgar_cik")
    if (
        jurisdiction.upper().startswith("US")
        and not _edgar_cik
        and legal_name
        and se_adapter
        and se_adapter.info.live_available
    ):
        try:
            se_hits = await se_adapter.search(legal_name, SearchKind.ENTITY)
            if se_hits:
                se_hit = se_hits[0]
                _edgar_hit2 = SourceHit(
                    source_id="sec_edgar", hit_id=se_hit.hit_id,
                    kind=SearchKind.ENTITY, name=se_hit.name, summary=se_hit.summary,
                    identifiers={"edgar_cik": se_hit.hit_id, "lei": lei},
                    raw=se_hit.raw, is_stub=False,
                )
                hits.append(_edgar_hit2)
                deepened_bundles.append(("sec_edgar", se_hit.hit_id))
                yield {"event": "source_started", "data": json.dumps({"source_id": "sec_edgar", "source_name": se_adapter.info.name})}
                yield {"event": "hit", "data": _edgar_hit2.model_dump_json()}
                yield {"event": "source_completed", "data": json.dumps({"source_id": "sec_edgar", "hit_count": 1})}
        except Exception as exc:  # noqa: BLE001
            errors["sec_edgar"] = f"{type(exc).__name__}: {exc}"
            yield {"event": "source_error", "data": json.dumps({"source_id": "sec_edgar", "error": errors["sec_edgar"]})}

    # ── Step 5: Reconcile + deepen + risk ─────────────────────────────────────
    links = [link.to_dict() for link in reconcile(hits)]
    search_signals = [s.to_dict() for s in assess_hits(hits)]

    yield {
        "event": "cross_source_links",
        "data": json.dumps({"links": links}),
    }

    bods_all: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    deepen_signals: list[dict[str, Any]] = []
    license_notices: list[dict[str, str]] = []

    _deepen_pairs = deepened_bundles[:deepen_top]
    _deepen_raw = await asyncio.gather(
        *[_safe_deepen(_dsrc, _dhit) for _dsrc, _dhit in _deepen_pairs],
        return_exceptions=True,
    )
    for (_dsrc, _dhit), _deep in zip(_deepen_pairs, _deepen_raw):
        if isinstance(_deep, Exception):
            errors.setdefault(_dsrc, f"{type(_deep).__name__}: {_deep}")
            continue
        if _deep is None:
            continue
        bods_all.extend(_deep["bods"])
        bods_issues.extend(_deep["bods_issues"])
        deepen_signals.extend(_deep["risk_signals"])
        if _deep.get("license_notice"):
            license_notices.append({"source_id": _dsrc, "hit_id": _dhit, "notice": _deep["license_notice"]})

    _cross_raw, _icij_raw = await asyncio.gather(
        assess_cross_source_names(bods_all),
        assess_icij_names(bods_all),
    )
    cross_signals = [s.to_dict() for s in _cross_raw]
    icij_signals = [s.to_dict() for s in _icij_raw]

    structural_codes = {
        "TRUST_OR_ARRANGEMENT", "NON_EU_JURISDICTION", "NOMINEE",
        "COMPLEX_OWNERSHIP_LAYERS", "COMPLEX_CORPORATE_STRUCTURE", "POSSIBLE_OBFUSCATION",
    }
    _statement_scoped = {"RELATED_PEP", "RELATED_SANCTIONED"}
    merged: dict[tuple, dict[str, Any]] = {}
    for sig in search_signals + deepen_signals + cross_signals + icij_signals:
        if sig["code"] in structural_codes:
            key: tuple = (sig["code"],)
        elif sig["code"] in _statement_scoped or (sig["code"] == "OFFSHORE_LEAKS" and sig.get("source_id") == "icij"):
            key = (sig["code"], sig["source_id"], sig["hit_id"], sig.get("evidence", {}).get("subject_statement_id", ""))
        else:
            key = (sig["code"], sig["source_id"], sig["hit_id"])
        merged[key] = sig

    yield {
        "event": "risk_signals",
        "data": json.dumps({"signals": list(merged.values())}),
    }

    yield {
        "event": "done",
        "data": json.dumps(
            {
                "lei": lei,
                "bods_issues": bods_issues,
                "license_notices": license_notices,
            }
        ),
    }


@app.get("/lookup-stream")
async def lookup_stream(
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
) -> EventSourceResponse:
    """LEI-anchored lookup streamed as SSE.

    Results arrive progressively as each source completes.  See the
    ``_lookup_stream_events`` docstring for the full event sequence.
    """
    return EventSourceResponse(_lookup_stream_events(lei, deepen_top=deepen_top))


# ----------------------------------------------------------------------
# /export — downloadable BODS bundle
# ----------------------------------------------------------------------


_EXPORT_FORMATS = {"json", "jsonl", "zip"}


@app.get("/export")
async def export(
    lei: str | None = Query(
        None,
        description=(
            "ISO 17442 LEI. When provided the export uses the same "
            "LEI-anchored synthesis as /lookup; ``q`` is ignored."
        ),
    ),
    q: str | None = Query(
        None,
        min_length=1,
        description=(
            "Free-text query; only used when ``lei`` is absent. Kept "
            "for backward compatibility."
        ),
    ),
    kind: SearchKind = Query(SearchKind.ENTITY),
    deepen_top: int = Query(3, ge=0, le=10),
    format: str = Query(
        "zip",
        pattern="^(json|jsonl|zip)$",
        description="json (pretty array) | jsonl (newline-delimited) | zip (bundle)",
    ),
) -> Response:
    """Download a BODS v0.4 bundle for a subject.

    Two entry shapes — ``lei`` (the supported flow) or ``q`` (free-text
    fallback). The ``zip`` form is the canonical shareable artefact:
    ``bods.json`` (pretty array), ``bods.jsonl`` (newline-delimited —
    the BODS v0.4 idiomatic shape for large datasets), ``manifest.json``
    (provenance: query, source list with licenses, cross-source links,
    risk signals, validation issues, retrieved-at), and ``LICENSES.md``
    (per-source license notes including any NC-licensed sources whose
    obligations propagate to the combined dataset).
    """
    if format not in _EXPORT_FORMATS:  # FastAPI validates via pattern but be explicit
        raise HTTPException(status_code=400, detail=f"Unknown format {format!r}")
    if lei is None and (q is None or not q.strip()):
        raise HTTPException(
            status_code=400,
            detail="Provide either ?lei=<LEI> or ?q=<free-text query>.",
        )

    if lei is not None:
        # Reuse the /lookup synthesis so the export contains the same
        # LEI-anchored cross-source data the user just saw on screen.
        payload = await lookup(lei, deepen_top)
        slug = _filename_slug(payload.lei)
        export_query = payload.lei
    else:
        assert q is not None
        payload = await _build_report(q, kind, deepen_top)
        slug = _filename_slug(q)
        export_query = q
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")

    if format == "json":
        body = json.dumps(payload.bods, indent=2).encode("utf-8")
        return Response(
            content=body,
            media_type="application/json",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}.json"'
                ),
            },
        )

    if format == "jsonl":
        body = ("\n".join(json.dumps(s) for s in payload.bods) + "\n").encode("utf-8")
        return Response(
            content=body,
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}.jsonl"'
                ),
            },
        )

    # format == "zip"
    body = _build_export_zip(
        payload, q=export_query, kind=kind, slug=slug, stamp=stamp
    )
    return Response(
        content=body,
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="opencheck-{slug}-{stamp}.zip"'
            ),
        },
    )


def _filename_slug(query: str) -> str:
    """Make a query safe to embed in a download filename."""
    slug = re.sub(r"[^a-z0-9]+", "-", query.lower()).strip("-")
    return slug or "export"


def _build_export_zip(
    payload: ReportResponse,
    *,
    q: str,
    kind: SearchKind,
    slug: str,
    stamp: str,
) -> bytes:
    """Assemble the canonical export bundle: BODS + manifest + licenses."""
    sources_consulted = [
        adapter.info.model_dump()
        for adapter in REGISTRY.values()
    ]
    # Sources that actually contributed real (non-stub) hits to this export.
    contributing_ids = sorted({h.source_id for h in payload.hits if not h.is_stub})

    manifest = {
        "opencheck_version": __version__,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "query": q,
        "kind": kind.value,
        "deepen_top": len(payload.license_notices) + len(
            [h for h in payload.hits if not h.is_stub]
        ),
        "sources_consulted": sources_consulted,
        "contributing_source_ids": contributing_ids,
        "hits": [h.model_dump() for h in payload.hits],
        "cross_source_links": payload.cross_source_links,
        "risk_signals": payload.risk_signals,
        "bods_statement_count": len(payload.bods),
        "bods_validation_issues": payload.bods_issues,
        "license_notices": payload.license_notices,
        "errors": payload.errors,
    }

    licenses_md = _build_licenses_md(
        contributing_ids=contributing_ids,
        license_notices=payload.license_notices,
        query=q,
        kind=kind,
    )

    bods_json = json.dumps(payload.bods, indent=2)
    bods_jsonl = "\n".join(json.dumps(s) for s in payload.bods) + "\n"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.json", bods_json)
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.jsonl", bods_jsonl)
        zf.writestr(
            f"opencheck-{slug}-{stamp}/manifest.json",
            json.dumps(manifest, indent=2, default=str),
        )
        zf.writestr(f"opencheck-{slug}-{stamp}/LICENSES.md", licenses_md)
    return buf.getvalue()


def _build_licenses_md(
    *,
    contributing_ids: list[str],
    license_notices: list[dict[str, str]],
    query: str,
    kind: SearchKind,
) -> str:
    """Markdown summary of every source's license + any NC notices.

    Rule of thumb baked into the text: when a bundle combines multiple
    licenses, the most-restrictive applies for downstream re-use.
    """
    lines: list[str] = []
    lines.append("# OpenCheck export — license notes")
    lines.append("")
    lines.append(
        f"This bundle was generated for query `{query}` (kind: {kind.value}). "
        "It combines data from multiple open-data sources, each with its "
        "own license. The **most restrictive** license applies to the "
        "combined dataset for re-use purposes."
    )
    lines.append("")
    lines.append("## Sources consulted")
    lines.append("")
    for source_id in contributing_ids:
        adapter = REGISTRY.get(source_id)
        if adapter is None:
            continue
        info = adapter.info
        lines.append(f"### {info.name} (`{info.id}`)")
        lines.append("")
        lines.append(f"- **License**: {info.license}")
        lines.append(f"- **Homepage**: {info.homepage}")
        lines.append(f"- **Attribution**: {info.attribution}")
        lines.append("")
    if license_notices:
        lines.append("## Specific notices")
        lines.append("")
        for n in license_notices:
            lines.append(
                f"- **{n['source_id']}/{n['hit_id']}** — {n['notice']}"
            )
        lines.append("")
    lines.append("## Re-use guidance")
    lines.append("")
    lines.append(
        "If any source above is non-commercial (CC BY-NC, CC BY-NC-SA), "
        "the combined bundle inherits that restriction: re-publication "
        "or commercial use of derivative works is not permitted under "
        "the source license. Strip the relevant statements (filter on "
        "the `source.description` field) before commercial use."
    )
    return "\n".join(lines) + "\n"


def _subject_metadata_from_bundle(
    bundle: list[dict[str, Any]], lei: str
) -> tuple[str, str, str]:
    """Extract ``(legal_name, jurisdiction_code, registered_as)`` from
    the entity statement in ``bundle`` whose identifiers list carries
    the given LEI under ``XI-LEI``.

    Returns ``("", "", "")`` if the bundle has no matching subject —
    the caller raises 404 in that case.
    """
    target = lei.strip().upper()
    for stmt in bundle:
        if (stmt.get("recordType") or "") != "entity":
            continue
        rd = stmt.get("recordDetails") or {}
        ids = rd.get("identifiers") or []
        has_lei = any(
            (i.get("scheme") == "XI-LEI" and (i.get("id") or "").upper() == target)
            for i in ids
            if isinstance(i, dict)
        )
        if not has_lei:
            continue
        legal_name = rd.get("name") or ""
        jur = rd.get("incorporatedInJurisdiction") or {}
        jurisdiction = (jur.get("code") or "").upper() if isinstance(jur, dict) else ""
        # Pull GB-COH (or analogous registered-as for other jurisdictions)
        # off the same identifier list.
        registered_as = ""
        for i in ids:
            if not isinstance(i, dict):
                continue
            scheme = (i.get("scheme") or "").upper()
            if scheme == "GB-COH":
                registered_as = i.get("id") or ""
                break
        return legal_name, jurisdiction, registered_as
    return "", "", ""


def _bods_data_override(source_id: str, hit_id: str) -> list[dict[str, Any]] | None:
    """Return the Open Ownership canonical BODS bundle for this
    ``(source_id, hit_id)`` pair, if one is on disk under
    ``data/cache/bods_data/``.

    Used by both ``/deepen`` and ``/lookup`` to override the live
    mapper output. Open Ownership's processed bundles carry
    interconnected subject ↔ interestedParty relationships that the
    live transformation under-produces — important for the dagre
    visualisation and the AMLA layer-counting rule.
    """
    if source_id == "gleif":
        return bods_data.gleif_bundle_for_lei(hit_id)
    if source_id == "companies_house":
        # ``hit_id`` may be either a company number (which is what the
        # UK PSC bundles index on) or an officer-appointments id; only
        # the company-number shape has a UK PSC override.
        if hit_id.isalnum() and len(hit_id) == 8:
            return bods_data.uk_bundle_for_company_number(hit_id)
    return None


async def _safe_deepen(source_id: str, hit_id: str) -> dict[str, Any] | None:
    """Internal helper used by /report — does what /deepen does, but
    returns a plain dict and swallows nothing (caller handles errors).
    """
    adapter = REGISTRY.get(source_id)
    if adapter is None:
        return None
    raw = await adapter.fetch(hit_id)

    override = _bods_data_override(source_id, hit_id)
    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    if override is not None:
        bods = override
        issues = validate_shape(bods)
    else:
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
