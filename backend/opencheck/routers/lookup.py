"""Lookup endpoints — /lookup, /lookup-stream, /deepen, /report."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from .. import __version__
from .. import bods as _bods
from .. import identifiers
from .. import degradation as _degradation
from .. import outbound_rate as _outbound_rate
from .. import provenance as _provenance
from .. import consistency, consistencystats, signalstats
from ..provenance import Provenance
from ..bods import BODSBundle, unique_statements, validate_shape
from ..sources.base import LookupDeriver, raw_redaction_notice
from .. import bods_data
from ..config import get_settings
from ..secret_scrub import describe_exception, scrub
from ..cross_check import NameScreen, assess_cross_source_names
from ..findings import (
    finding_bods_gleif,
    finding_climatetrace,
    finding_companies_house,
    finding_everypolitician,
    finding_gleif,
    finding_openaleph,
    finding_opencorporates,
    finding_ted_eu,
    finding_wikidata,
)
from ..ftm import subject_to_ftm_entity
from ..gleif_throttle import GleifRateLimitedError
from ..memwatch import is_bot
from ..icij_check import assess_icij_names
from ..names import normalise_name
from ..openaleph_check import assess_openaleph_names
from .. import lei_registration as _lei_registration
from ..subject_profile import build_subject_profile
from ..knowability import chain_for_lei as knowability_chain_for_lei
from .. import listing as _listing
from ..verdict import build_verdict
from ..reconcile import possibly_same_entities, reconcile
from ..risk import DegradedSource, RiskSignal, assess_bundle, assess_hits
from ..risk import merge_state_controlled as _risk_merge_state_controlled
from ..bods.state_bodies import classify_government_entities
from ..ratelimit import default_tier, limiter, lookup_tier
from ..sources import REGISTRY, SearchKind, SourceHit, SourceInfo
from ..sources.eiti import us_ein_for_lei as eiti_us_ein_for_lei
from ..sources.schemas import SourceSchemaError

# Phase 168: the per-source hit builders and the context they read moved to
# `hit_builders.py`. Imported back by name — tests and the pipeline below
# both reach for these directly, and moving code is not moving its address.
from .hit_builders import (  # noqa: F401
    _bh_acra_singapore,
    _bh_anaf_romania,
    _bh_apr_serbia,
    _bh_asp_moldova,
    _EITI_IDENTIFIER_KEY_BY_COUNTRY,
    _LookupCtx,
    _PERSON_CAPABLE_SOURCES,
    _REGISTRY_SOURCES,
    _REGISTRY_SOURCE_INDEX,
    _RegistrySource,
    _WIKIRATE_IDENTIFIER_KEYS,
    _bh_abr_australia,
    _bh_ares,
    _bh_ariregister,
    _bh_bce_belgium,
    _bh_bods_gleif,
    _bh_bolagsverket,
    _bh_brreg,
    _bh_cac_nigeria,
    _bh_eiti_assessment,
    _bh_climatetrace,
    _bh_cnpj_brazil,
    _bh_companies_house,
    _bh_corporations_canada,
    _bh_cr_hongkong,
    _bh_cro,
    _bh_cvr_denmark,
    _bh_dlcp_dc,
    _bh_eiti,
    _bh_eiti_bo,
    _bh_eiti_soe,
    _bh_firmenbuch,
    _bh_gemi_greece,
    _bh_inpi,
    _bh_jar_lithuania,
    _bh_krs_poland,
    _bh_kvk,
    _bh_malta_mbr,
    _bh_meip,
    _bh_mca_india,
    _bh_nz_companies,
    _bh_opencorporates,
    _bh_prh,
    _bh_rpo_slovakia,
    _bh_rpvs_slovakia,
    _bh_sudreg_croatia,
    _bh_ted_eu,
    _bh_ur_latvia,
    _bh_wikidata,
    _bh_wikirate,
    _bh_zefix,
    _collect_registry_sources,
    _extract_edgar_cik,
    _hit,
    _local_id_for,
)

# Phase 246: the replay cache, the pipeline gate, the flights and the fold live
# in ``opencheck/lookup_replay.py``; the FullCheck expansion endpoints in
# ``routers/expand.py``. Re-exported here because saved reports, share cards,
# batch, the MCP tools and the tests reach them through this module. The cache
# and in-flight dicts are the same objects under both names; a module-level
# setting (``_QUEUE_POLL_S``) must be patched where it is read, in
# ``lookup_replay``.
from ..lookup_replay import (  # noqa: F401  (re-exported)
    DEEPEN_TOP_MAX,
    LookupEvent,
    _Flight,
    _GATE,
    _IN_FLIGHT,
    _PipelineBusyError,
    _PipelineGate,
    _QUEUE_POLL_S,
    _REFUNDED_STATUSES,
    _REPLAY_CACHE,
    _REPLAY_MAX_ENTRIES,
    _REPLAY_TTL_SECONDS,
    _ReplayEntry,
    _TRANSIENT_EVENTS,
    _Waiter,
    _gate,
    _invalidate_replay,
    _knowability_payload,
    _lookup_pipeline_cached,
    _queue_wait_for,
    _replay_key,
    _run_flight,
    clamp_deepen_top,
    fold_lookup_events,
    pipelines_queued,
    pipelines_running,
    replay_entry,
)
from ..graph_shape import (  # noqa: F401  (re-exported, Phase 246)
    _count_parties,
    _graph_shape,
    _identifier_keys,
)
from .national_id import (  # noqa: F401  (re-exported, Phase 246)
    NationalIdMatch,
    ResolveNationalIdResponse,
    _RA_BY_COUNTRY,
    _resolve_national_id_impl,
    resolve_national_id,
)

router = APIRouter()

_LOG = logging.getLogger(__name__)


def _fmt_source_error(exc: Exception) -> str:
    """Format a source fetch exception for the errors dict and SSE events.

    Never ``str(exc)`` directly: an httpx status error's message is the full
    request URL, and two adapters authenticate in the query string. See
    :mod:`opencheck.secret_scrub`.
    """
    if isinstance(exc, SourceSchemaError):
        return f"Source API changed — {scrub(str(exc))}"
    return describe_exception(exc)


def _mapper_for(source_id: str) -> Any | None:
    """BODS mapper for a source, by convention: ``opencheck.bods.map_<id>``.

    Adding ``map_<name>()`` to bods/mapper.py (exported via bods/__init__)
    is all it takes to wire a mapper — there is no hand-maintained dict.
    """
    return getattr(_bods, f"map_{source_id}", None)

async def _fetch_with_provenance(
    adapter: Any, hit_id: str, **kwargs: Any
) -> tuple[dict[str, Any], Provenance]:
    """Fetch a source payload and record where it actually came from.

    The recorder is populated by ``Cache`` reads and ``build_client()`` calls
    made anywhere beneath this await, so adapters need no changes. Each source
    is dispatched in its own asyncio task, and a task copies the context on
    creation, so concurrent fetches cannot see each other's observations.
    """
    with _provenance.recording() as recorder:
        raw = await adapter.fetch(hit_id, **kwargs)
    is_stub = bool(raw.get("is_stub")) if isinstance(raw, dict) else False
    return raw, recorder.resolve(is_stub=is_stub)


def _stamp(hit: SourceHit | None, prov: Provenance | None) -> SourceHit | None:
    """Attach resolved provenance to a hit before it leaves the pipeline."""
    if hit is None:
        return None
    if prov is not None:
        hit.liveness = prov.liveness
        hit.retrieved_at = prov.retrieved_at
    return hit


_NC_LICENSES = {"CC-BY-NC-4.0", "CC-BY-NC-SA-4.0"}

# 20-char ISO 17442 LEI shape (shared; see opencheck/identifiers.py). Check
# digits are additionally enforced via lei_check_digit_error when
# OPENCHECK_IDENTIFIER_CHECKSUMS_ENFORCED is on (the default).
_LEI_SHAPE = identifiers.LEI_PATH_SHAPE


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
    """Aggregate post-search synthesis for a single subject."""

    query: str
    kind: SearchKind
    hits: list[SourceHit]
    errors: dict[str, str]
    cross_source_links: list[dict[str, Any]]
    risk_signals: list[dict[str, Any]]
    bods: list[dict[str, Any]]
    bods_issues: list[str]
    license_notices: list[dict[str, str]]
    #: Name-only "likely same" entity candidates (same name + jurisdiction, no
    #: shared identifier) — human-review suggestions, never auto-merges.
    possibly_same_entities: list[dict[str, Any]] = []
    #: Derived risk checks that did not fully run for this result (issue
    #: #50) — empty when every screen completed. Each record carries
    #: source_id / check / affected_signals / detail / reason (closed
    #: vocabulary: upstream_error, timeout, not_configured, rate_limited).
    #: An empty risk_signals list with a non-empty degraded_sources list
    #: is NOT a clean screen. Never contains related-party names.
    degraded_sources: list[dict[str, Any]] = []
    #: Informational related-party matches from OpenAleph percolation
    #: (Phase 96): attributed, similarity-gated hits whose topics map to no
    #: RELATED_* code — leak/court collections, poi, corp.disqual. Each
    #: entry carries statement_id / matched_name / collection / url /
    #: surface_form. Name-derived — never identifier corroboration.
    openaleph_screening: list[dict[str, Any]] = []
    #: How current each source's payload is, keyed by source_id: liveness
    #: ('live' / 'cached' / 'snapshot' / 'curated' / 'stub'), a display label,
    #: the retrieval time OpenCheck actually observed (null when nothing was
    #: fetched) and a short detail string. Sibling to degraded_sources: data
    #: that is not current must not read as live, just as a check that could
    #: not run must not read as clean. Per-hit values ride on each SourceHit.
    source_liveness: dict[str, dict[str, Any]] = {}
    #: How big the mapped graph is: companies / people / relationships across
    #: the sources that answered (deduplicated by statementId), plus the
    #: longest ownership chain the risk layer measured, or null when it did
    #: not. Counts what this check holds — never what a deeper one might find.
    graph_shape: dict[str, Any] = {}
    #: One deterministic sentence stating what the check found — see
    #: ``opencheck.verdict``. Rendered at the top of the report, above the
    #: evidence and above the AI summary. Template-built, never a model
    #: call, and defaulted so replayed payloads recorded before Phase 122
    #: still validate.
    verdict: str | None = None
    #: What the registers say the subject *is* (Phase 154): legal form,
    #: register status, founding date and registered address, each with the
    #: sources stating it and their independent count — read from the
    #: subject's own entity statements only (``opencheck.subject_profile``).
    #: Facts, never findings. None for a name search or before any source
    #: has been deepened.
    subject_profile: dict[str, Any] | None = None
    #: What is knowable about a company in the subject's jurisdiction
    #: (Phase 224): the dated statement from ``opencheck.knowability`` —
    #: who may see beneficial owners there, what the register records, and
    #: what OpenCheck reads of it — as the ``knowability`` stream event
    #: carried it, plus ``as_of``. Describes, never judges; nothing here is
    #: a signal. Frozen at run time so a saved report replays the sentence
    #: that was true that day. None for a name search, for a payload recorded
    #: before this field existed, or when the subject's jurisdiction is unknown.
    knowability: dict[str, Any] | None = None
    #: What is knowable along the ownership path (Phase 226): ``subject``
    #: (the subject's code), ``codes`` (every jurisdiction on the upward walk
    #: from the subject, subject first then path order, ended links included)
    #: and one ``statements[]`` entry per code, plus ``as_of`` — as the
    #: ``knowability_chain`` stream event carried it, frozen at run time. The
    #: chain is the run's own depth (the deepened sources); FullCheck extends
    #: it client-side as it expands. Describes, never judges. None for a
    #: payload recorded before this field existed.
    knowability_chain: dict[str, Any] | None = None
    #: The subject's primary stock-exchange listing (Phase 236), from LSEG
    #: PermID: ``status`` ("listed" / "not_listed"), ``quote`` (ticker, MIC,
    #: RIC, security name, PermID ids), ``exchange`` (name + country, or None
    #: for a venue OpenCheck has no name for), ``link`` (a verified venue
    #: page, or None) and ``as_of`` — as the ``listing`` stream event carried
    #: it, frozen at run time (``opencheck.listing``). None when no PermID key
    #: is configured, when PermID did not answer (a degraded source, never
    #: "not listed"), or for a payload recorded before this field existed.
    listing: dict[str, Any] | None = None


class LookupResponse(ReportResponse):
    """Same shape as /report, with the LEI echoed back and the GLEIF
    bundle surfaced separately so the UI doesn't have to dig for it."""

    lei: str
    legal_name: str | None = None
    jurisdiction: str | None = None
    derived_identifiers: dict[str, str] = {}
    # Provenance: True when served from the short-lived replay cache rather
    # than a fresh run, with the wall-clock completion time of the original
    # run. ``?refresh=true`` always yields a fresh run.
    replayed: bool = False
    fetched_at: str | None = None
    # Phase 164: the source ids the pipeline announced as applicable to this
    # company (the ``sources_applicable`` stream event; the GLEIF anchor is
    # never in it — Phase 126/156). Lets a JSON consumer build the Phase 156
    # coverage sentence without the stream.
    sources_applicable: list[str] = []
    # Phase 216: the wall-clock UTC completion time of the run this response
    # was folded from — set for live and replayed runs alike (``fetched_at``
    # stays replay-only, so a live run is never badged as cached). It names
    # the run: a saved report is taken from the replay cache only when the
    # caller's ``run_completed_at`` matches the held run's, and the narrative
    # cache records which run a summary was written from.
    run_completed_at: str | None = None


@router.get("/deepen", response_model=DeepenResponse)
@limiter.limit(default_tier)
async def deepen(
    request: Request,
    response: Response,
    source: str = Query(..., description="Adapter id, e.g. 'companies_house'"),
    hit_id: str = Query(..., description="Adapter-local hit id"),
) -> DeepenResponse:
    """Fetch the full record for a single hit and map to BODS v0.4."""

    adapter = REGISTRY.get(source)
    if adapter is None:
        raise HTTPException(status_code=404, detail=f"unknown source {source!r}")

    # Stored OO bundle is canonical — consult it first so a live-fetch failure
    # (e.g. a Companies House outage) still serves the stored graph.
    override = _bods_data_override(source, hit_id)
    try:
        raw, prov = await _fetch_with_provenance(adapter, hit_id)
    except Exception:
        if override is None:
            raise
        raw, prov = {"is_stub": True}, _provenance.STUB_PROVENANCE

    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    if override is not None:
        bods = unique_statements(override)
        issues = validate_shape(bods)
        prov = _stored_bundle_provenance(source, hit_id)
    else:
        mapper = _mapper_for(source)
        if mapper and not raw.get("is_stub"):
            with _provenance.mapping_provenance(prov):
                bundle: BODSBundle = mapper(raw)
            # One statement per statementId (Phase 235).
            bods = unique_statements(bundle)
            issues = validate_shape(bods)

    # Phase 240: an owner whose LEI GLEIF files as a government is a state
    # body, whichever source named it — before the risk rules read the types.
    bods = classify_government_entities(bods, source_id=source)
    info = adapter.info
    license_notice = _license_notice_for(info, raw)
    signals = [s.to_dict() for s in assess_bundle(source, raw, bods, hit_id=hit_id)]

    # Sources whose licence forbids raw re-publication (OpenCorporates) return a
    # redaction notice in place of the raw bundle; the BODS output is unaffected.
    response_raw = raw if adapter.republish_raw else raw_redaction_notice(source)

    return DeepenResponse(
        source_id=source,
        hit_id=hit_id,
        raw=response_raw,
        bods=bods,
        bods_issues=issues,
        license=info.license,
        license_notice=license_notice,
        risk_signals=signals,
    )


@router.get("/report", response_model=ReportResponse)
@limiter.limit(lookup_tier)
async def report(
    request: Request,
    response: Response,
    q: str = Query(..., min_length=1),
    kind: SearchKind = Query(SearchKind.ENTITY),
    deepen_top: int = Query(
        3, ge=0, le=10, description="How many top hits to deepen+map+assess."
    ),
) -> ReportResponse:
    """One-shot synthesis: search, reconcile, deepen top N, assess risk."""
    return await _build_report(q, kind, deepen_top)


async def _build_report(
    q: str, kind: SearchKind, deepen_top: int
) -> ReportResponse:
    """Shared by /report and /export. Same algorithm; same response shape."""
    from .search import _run_adapters  # avoid circular at module level

    # Open the scope BEFORE any adapter runs: a source that could not answer
    # from its own data records that here, and it is collected below alongside
    # the derived screens' degradations.
    _degradation.begin()
    # Likewise the outbound call budgets, for the same reason and with the same
    # lifetime: this function fans out across every adapter and then deepens
    # the top hits, so a rate-capped source can be asked several times in one
    # request and needs a per-request counter to spend from.
    _outbound_rate.begin()
    results, errors = await _run_adapters(q, kind)
    hits = [hit for adapter_hits in results.values() for hit in adapter_hits]
    links = [link.to_dict() for link in reconcile(hits)]
    search_signals = [s.to_dict() for s in assess_hits(hits)]

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
            errors.setdefault(source_id, _fmt_source_error(exc))
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

    bods_all = unique_statements(bods_all)  # one statement per id (Phase 235)

    # Seeded with whatever the source adapters recorded during the fetches
    # above — a source that could not answer from its own data says so there,
    # not only in the server log. The derived screens append to the same list.
    degraded: list[DegradedSource] = _degradation.collect()
    # Phase 241: and every source error, as in the lookup pipeline.
    _degradation.add_source_errors(
        degraded, errors, with_data={h.source_id for h in hits if not h.is_stub}
    )
    # The register fan-out is over, so the call budgets close with it. The
    # derived screens below talk to name-screening services, none of which
    # publish a per-minute quota this module governs.
    _outbound_rate.end()
    oa_screening: list[dict[str, Any]] = []
    cross_signals = [
        s.to_dict()
        for s in await assess_cross_source_names(bods_all, degraded=degraded)
    ]
    icij_signals = [
        s.to_dict() for s in await assess_icij_names(bods_all, degraded=degraded)
    ]
    oa_signals = [
        s.to_dict()
        for s in await assess_openaleph_names(
            bods_all, degraded=degraded, screening=oa_screening
        )
    ]

    all_signals = _merge_signals(
        search_signals, deepen_signals, cross_signals, icij_signals, oa_signals
    )

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
        possibly_same_entities=[p.to_dict() for p in possibly_same_entities(bods_all)],
        degraded_sources=[d.to_dict() for d in degraded],
        openaleph_screening=oa_screening,
        verdict=build_verdict(all_signals, [d.to_dict() for d in degraded]),
    )


# ---------------------------------------------------------------------------
# LEI-anchored lookup — one pipeline drives both /lookup and /lookup-stream
# ---------------------------------------------------------------------------
#
# ``_lookup_pipeline()`` is the ONLY place that resolves the GLEIF anchor,
# builds derived identifiers, dispatches adapters, converts results to
# SourceHits, deepens, and assesses risk. It yields ``(event, payload)``
# tuples; /lookup-stream serialises them as SSE and /lookup collects them
# into a LookupResponse. Until this refactor the two endpoints were
# hand-synchronised copies of each other — forgetting to edit both was a
# recurring bug (see the Corporations Canada regression fixed in 603c086).
#
# Adapters are self-describing: each national-register adapter declares its
# RA-code derivers (``lookup_derivers``), dispatch keys and legal-name flag
# on its class (see sources/base.py). The deriver table and dispatch specs
# below are built from the REGISTRY at import time, so wiring a new adapter
# into the lookup flow means declaring the spec on the adapter class and
# adding a ``_bh_<id>()`` hit builder here — nothing else.


# RA-code derivers declared by the adapters themselves, collected from the
# registry. GB is special-cased on jurisdiction in _build_derived() because
# UK records reliably carry registeredAs. Normalisers may raise ValueError
# for malformed local IDs — the source is then skipped.
_RA_DERIVERS: list[LookupDeriver] = [
    deriver
    for adapter in REGISTRY.values()
    for deriver in adapter.lookup_derivers
]


def _build_derived(ctx: _LookupCtx, registered_at_id: str) -> None:
    """Populate ctx.derived from the GLEIF anchor record."""
    ctx.derived["lei"] = ctx.lei
    if ctx.jurisdiction.upper() == "GB" and ctx.registered_as:
        ctx.derived["gb_coh"] = ctx.registered_as
    # US federal EIN. Not on the GLEIF record -- GLEIF publishes a US
    # registeredAs as the *state* file number -- so it comes from the
    # committed EITI crosswalk. Derived here, before dispatch, because the
    # EITI adapter takes us_ein as a dispatch argument; the CIK-carrying
    # OpenCorporates result arrives long after that point.
    # startswith, not ==: GLEIF publishes a US jurisdiction as the ISO 3166-2
    # subdivision ("US-NJ" for Exxon Mobil), which is why the == test shipped
    # in Phase 155 never fired in production. The EDGAR paths further down
    # have always used startswith for the same reason.
    if ctx.jurisdiction.upper().startswith("US"):
        us_ein = eiti_us_ein_for_lei(ctx.lei)
        if us_ein:
            ctx.derived["us_ein"] = us_ein
    if ctx.registered_as and registered_at_id:
        for deriver in _RA_DERIVERS:
            if registered_at_id in deriver.ra_codes:
                try:
                    ctx.derived[deriver.derived_key] = deriver.normalise(
                        ctx.registered_as
                    )
                except ValueError:
                    pass  # malformed local ID on the LEI record — skip source
                break


def _edgar_hit(cik: str, legal_name: str) -> SourceHit:
    return _hit(
        "sec_edgar", cik,
        name=legal_name or "",
        summary=f"CIK {cik} · US listed company",
        identifiers={"edgar_cik": cik},
        raw={"cik": cik, "name": legal_name or ""},
    )


def _build_gleif_hit(ctx: _LookupCtx, gleif_bundle: dict[str, Any]) -> SourceHit:
    # wikidata_qid (and ocid / edgar_cik) are intentionally omitted from the
    # GLEIF hit identifiers: they are sourced from Wikidata / OpenCorporates,
    # not GLEIF. Including them would make the reconciler show "gleif" as a
    # confirmer of identifiers it does not actually publish.
    identifiers = {"lei": ctx.lei}
    for key, value in ctx.derived.items():
        if key not in ("lei", "ocid", "wikidata_qid", "edgar_cik"):
            identifiers[key] = value
    return _hit(
        "gleif", ctx.lei,
        name=ctx.legal_name or f"LEI {ctx.lei}",
        summary=f"LEI {ctx.lei} · {ctx.jurisdiction}",
        # Reads the whole bundle (Level 2 parents, reporting exceptions,
        # children count), not just the Level 1 record that becomes ``raw``.
        finding=finding_gleif(gleif_bundle),
        identifiers=identifiers,
        raw={
            **(gleif_bundle.get("record") or {}),
            # Children metadata — read by the frontend to display
            # "Showing X of N direct subsidiaries (GLEIF)".
            "direct_children_total": gleif_bundle.get("direct_children_total", 0),
            "direct_children_fetched": len(gleif_bundle.get("direct_children") or []),
        },
    )


async def _openaleph_strategies(ctx: _LookupCtx) -> list[SourceHit]:
    """OpenAleph cascade: LEI → OC URL → registration numbers →
    FtM match → percolate name → q= name fallback."""
    oa_adapter = REGISTRY.get("openaleph")
    if oa_adapter is None:
        return []
    oa: list[SourceHit] = await oa_adapter.fetch_by_lei(ctx.lei)  # type: ignore[attr-defined]
    if not oa and "ocid" in ctx.derived:
        oa = await oa_adapter.fetch_by_oc_url(ctx.derived["ocid"])  # type: ignore[attr-defined]
    if not oa:
        for jur, reg in [
            ("gb", ctx.derived.get("gb_coh")),
            ("fr", ctx.derived.get("siren")),
            ("nl", ctx.derived.get("kvk_number")),
            ("se", ctx.derived.get("se_org_number")),
            ("ch", ctx.derived.get("che_uid")),
        ]:
            if reg:
                oa = await oa_adapter.fetch_by_registration(jur, reg)  # type: ignore[attr-defined]
                if oa:
                    break
    if not oa and ctx.legal_name:
        # Before falling back to free-text name search, try native FtM
        # matching — POST /api/2/match with the subject converted to an FtM
        # entity (bods-ftm when installed, equivalent built-in shape
        # otherwise). Identifier-aware (leiCode / registrationNumber /
        # jurisdiction participate), so precision is far better than the
        # Lucene q= fallback. Needs OPENALEPH_API_KEY; degrades to []
        # without one, and the q= fallback still runs after it.
        ftm_entity = subject_to_ftm_entity(
            ctx.lei, ctx.legal_name, ctx.jurisdiction, ctx.registered_as
        )
        if ftm_entity and hasattr(oa_adapter, "match_entity"):
            oa = await oa_adapter.match_entity(ftm_entity)  # type: ignore[attr-defined]
    if not oa and ctx.legal_name and hasattr(oa_adapter, "fetch_by_name_percolate"):
        # Percolation-based reverse name lookup (OpenAleph 5.3.1,
        # POST /api/2/beta/percolate — the endpoint requested in
        # openaleph/openaleph#105). The legal name travels as raw JSON
        # body text, never through the Lucene query_string parser, so
        # the reserved-syntax bug class (quotes, A/S, dangling +)
        # cannot occur on this path, and only entities whose own stored
        # names fire on the text come back (still _bears_name-gated).
        # Key-gated like /match; degrades to no hits without one, and
        # the q= fallback below still runs.
        oa = await oa_adapter.fetch_by_name_percolate(ctx.legal_name)
    if not oa and ctx.legal_name:
        oa = await oa_adapter.fetch_by_name(ctx.legal_name)  # type: ignore[attr-defined]
    # OpenAleph can index the same entity under multiple collection aliases,
    # causing duplicate hit_ids — deduplicate before returning.
    seen: set[str] = set()
    deduped: list[SourceHit] = []
    for h in oa:
        if h.hit_id not in seen:
            seen.add(h.hit_id)
            deduped.append(h)

    # Informational enrichment (OpenAleph 5.3): count the documents in the
    # instance that mention each matched entity, via the /mentions endpoint
    # (the inverse of percolation/Screening). Name-derived — never treated
    # as identifier corroboration. Failures degrade silently.
    #
    # Phase 158: fetched once per distinct *name*, and applied to every hit
    # that carries it. Mentions are name-derived, so two records for the same
    # name get the same documents (Shell plc's GLEIF and Companies House
    # records both read "mentioned in 110 documents"); fetching for the top
    # two *hits* meant the third record of the same name rendered without the
    # mentions line and so could not group with the two above it — three rows
    # where the reader could tell apart one. At most two fetches still, to
    # stay inside the adapter's lookup time budget.
    if hasattr(oa_adapter, "fetch_mentions"):
        by_name: dict[str, list[SourceHit]] = {}
        for h in deduped:
            by_name.setdefault(normalise_name(h.name) or h.hit_id, []).append(h)
        fetched = 0
        for same_name in by_name.values():
            if fetched >= 2:
                break
            fetched += 1
            try:
                mentions = await oa_adapter.fetch_mentions(same_name[0].hit_id)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                continue
            if not mentions or not mentions.get("total"):
                continue
            total = mentions["total"]
            for h in same_name:
                h.raw["openaleph_mentions"] = mentions
                h.summary = (
                    f"{h.summary} · mentioned in {total} "
                    f"document{'s' if total != 1 else ''}"
                )
                # The adapter built the finding without mentions (it fetches
                # them here, after the hit exists) — rebuild so the sentence
                # leads with the document count.
                h.finding = finding_openaleph(h.raw, mentions)
    return deduped


def _name_screen_can_run() -> bool:
    """Whether the related-party name screen will actually query anything.

    The same two conditions ``assess_cross_source_names`` checks before it
    runs: live mode, and the OpenSanctions key that the EveryPolitician
    adapter shares. Announcing the source when neither holds would put a
    source in "N of N answered" that was never going to be asked — the
    failure mode this whole applicability idea exists to prevent.
    """
    settings = get_settings()
    return bool(settings.allow_live and settings.opensanctions_api_key)


def _offline_index_covers(adapter: Any, lei: str) -> bool:
    """Whether an offline LEI-keyed adapter's committed index holds this LEI.

    These three adapters — the CAC BOR example set, the EITI SOE database, the
    pooled EITI BO registers — answer from a file in the repository rather than
    from a call. Dispatching them regardless meant every lookup announced them
    as sources being queried and counted them in "N of N sources answered":
    a British oil major and a Russian one both listed the Nigerian beneficial
    ownership register among the registers consulted, which is not a claim
    anyone can defend. Membership in the index is exactly the applicability
    test the RA-derived adapters get for free from a jurisdiction code.

    Reads the index directly rather than through the adapter's declare-and-load
    path: this is a question about the file, not a fetch, and recording curated
    provenance for a source that never ran would put a freshness note on
    nothing.

    Fails **open**. An unreadable index is a fault worth surfacing through the
    normal source-error path, not a reason to quietly drop a source.
    """
    covers = getattr(adapter, "covers_lei", None)
    if covers is None:
        return True
    try:
        return bool(covers(lei))
    except Exception:  # pragma: no cover - defensive
        _LOG.warning("%s: index membership check failed; dispatching anyway", adapter)
        return True


async def _reraise(exc: Exception) -> Any:
    """An awaitable that fails with ``exc`` — a failure found before dispatch,
    replayed through the dispatch loop so it is reported like any other."""
    raise exc


def _dispatch(ctx: _LookupCtx, only: str | None = None) -> list[tuple[str, Any]]:
    """Build the (source_id, awaitable) dispatch list for this lookup.

    ``only`` restricts dispatch to a single source — used by the
    /lookup-source per-source retry endpoint.
    """
    tasks: list[tuple[str, Any]] = []

    def _want(source_id: str) -> bool:
        return only is None or source_id == only

    for spec in _REGISTRY_SOURCES:
        if not _want(spec.source_id):
            continue
        local_id = _local_id_for(spec, ctx.derived)
        if not local_id:
            continue
        adapter = REGISTRY[spec.source_id]
        if spec.pass_legal_name:
            tasks.append((spec.source_id, adapter.fetch(local_id, legal_name=ctx.legal_name)))
        else:
            tasks.append((spec.source_id, adapter.fetch(local_id)))
    if ctx.ocid and _want("opencorporates"):
        tasks.append(("opencorporates", REGISTRY["opencorporates"].fetch(ctx.ocid)))
    if ctx.qid and _want("wikidata"):
        tasks.append(("wikidata", REGISTRY["wikidata"].fetch(ctx.qid)))
    elif ctx.qid_error is not None and _want("wikidata"):
        # Phase 244: the QID lookup itself failed. Wikidata is applicable —
        # we asked and it did not answer — so it is dispatched as a task that
        # re-raises that failure, and the ordinary path turns it into a
        # `source_error` (and, through `add_source_errors`, a degradation).
        tasks.append(("wikidata", _reraise(ctx.qid_error)))
    os_adapter = REGISTRY.get("opensanctions")
    if os_adapter and SearchKind.ENTITY in os_adapter.info.supports and _want("opensanctions"):
        tasks.append(("opensanctions", os_adapter.search(ctx.lei, SearchKind.ENTITY)))
    if REGISTRY.get("openaleph") is not None and _want("openaleph"):
        tasks.append(("openaleph", _openaleph_strategies(ctx)))
    ct_adapter = REGISTRY.get("climatetrace")
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei") and _want("climatetrace"):
        tasks.append(("climatetrace", ct_adapter.fetch_by_lei(ctx.lei)))
    # Wikirate keys on LEI with a Wikidata-QID fallback — both resolve via
    # the same company_identifier filter. The adapter returns None without
    # a WIKIRATE_API_KEY (Cloudflare blocks anonymous server-side calls).
    wr_adapter = REGISTRY.get("wikirate")
    if wr_adapter is not None and hasattr(wr_adapter, "fetch_by_lei") and _want("wikirate"):
        tasks.append((
            "wikirate",
            wr_adapter.fetch_by_lei(ctx.lei, qid=ctx.qid, legal_name=ctx.legal_name),
        ))
    # EITI keys on the GLEIF anchor's (jurisdiction, registeredAs) pair — the
    # identification numbers EITI publishes are national registry numbers, so
    # this matches any LEI holder in any of EITI's 65 implementing countries,
    # not just those with a dedicated OpenCheck register adapter.
    eiti_adapter = REGISTRY.get("eiti")
    if (
        eiti_adapter is not None
        and hasattr(eiti_adapter, "fetch_by_registration")
        and (ctx.registered_as or ctx.derived.get("us_ein"))
        and ctx.jurisdiction
        and _want("eiti")
    ):
        tasks.append((
            "eiti",
            eiti_adapter.fetch_by_registration(
                ctx.jurisdiction,
                ctx.registered_as,
                legal_name=ctx.legal_name,
                us_ein=ctx.derived.get("us_ein", ""),
            ),
        ))
    bg_adapter = REGISTRY.get("bods_gleif")
    if bg_adapter is not None and hasattr(bg_adapter, "fetch_by_lei") and _want("bods_gleif"):
        tasks.append(("bods_gleif", bg_adapter.fetch_by_lei(ctx.lei)))
    # EITI SOE Database — LEI-keyed offline match against the committed index.
    # A hit means the LEI is a state-owned enterprise; its BODS (a stateBody
    # government + control relationship) drives the STATE_CONTROLLED signal.
    soe_adapter = REGISTRY.get("eiti_soe")
    if (
        soe_adapter is not None
        and hasattr(soe_adapter, "fetch_by_lei")
        and _want("eiti_soe")
        and _offline_index_covers(soe_adapter, ctx.lei)
    ):
        tasks.append(("eiti_soe", soe_adapter.fetch_by_lei(ctx.lei)))
    # Nigeria CAC — LEI-keyed offline match against the committed PSC index
    # (curated example set; the CAC's official API is government-only). A hit
    # means the LEI is in the curated set; its BODS carries the CAC-published
    # beneficial ownership.
    cac_adapter = REGISTRY.get("cac_nigeria")
    if (
        cac_adapter is not None
        and hasattr(cac_adapter, "fetch_by_lei")
        and _want("cac_nigeria")
        and _offline_index_covers(cac_adapter, ctx.lei)
    ):
        tasks.append(("cac_nigeria", cac_adapter.fetch_by_lei(ctx.lei)))
    # OECD-UNSD MEIP — LEI-keyed offline match against the register's own
    # BODS release (Phase 208). A hit means the LEI is one of the 500 group
    # heads or a member of one of their groups; its statements are the
    # OECD's, passed through unmodified.
    meip_adapter = REGISTRY.get("meip")
    if (
        meip_adapter is not None
        and hasattr(meip_adapter, "fetch_by_lei")
        and _want("meip")
        and _offline_index_covers(meip_adapter, ctx.lei)
    ):
        tasks.append(("meip", meip_adapter.fetch_by_lei(ctx.lei)))
    # Pooled EITI national BO registers — LEI-keyed offline match against the
    # committed pooled index (DRC ITIE-RDC / Armenia State Register / Nigeria
    # CAC∩NEITI). A hit means the LEI is an extractive company with register-
    # published beneficial ownership; its BODS carries that ownership.
    eiti_bo_adapter = REGISTRY.get("eiti_bo")
    if (
        eiti_bo_adapter is not None
        and hasattr(eiti_bo_adapter, "fetch_by_lei")
        and _want("eiti_bo")
        and _offline_index_covers(eiti_bo_adapter, ctx.lei)
    ):
        tasks.append(("eiti_bo", eiti_bo_adapter.fetch_by_lei(ctx.lei)))
    # EITI Company Assessment — LEI-keyed offline match against the committed
    # snapshot of EITI's assessment of its supporting companies. A hit means
    # the LEI is one of the ~99 companies EITI assesses; the bundle carries
    # its beneficial-ownership DISCLOSURE assessment and its declared
    # subsidiary list. Neither is ownership data and neither raises a signal.
    eiti_assess_adapter = REGISTRY.get("eiti_assessment")
    if (
        eiti_assess_adapter is not None
        and hasattr(eiti_assess_adapter, "fetch_by_lei")
        and _want("eiti_assessment")
        and _offline_index_covers(eiti_assess_adapter, ctx.lei)
    ):
        tasks.append(("eiti_assessment", eiti_assess_adapter.fetch_by_lei(ctx.lei)))
    # TED keys on the GLEIF anchor's identifiers (LEI + registeredAs + derived
    # national numbers) — eForms BT-501 values are national registration
    # numbers today (LEI fill rate is zero as of 2026-08), so this matches any
    # LEI holder in a TED-relevant jurisdiction, not just those with a
    # dedicated register adapter. The jurisdiction gate lives in the adapter.
    ted_adapter = REGISTRY.get("ted_eu")
    if (
        ted_adapter is not None
        and hasattr(ted_adapter, "fetch_by_identifiers")
        and (ctx.registered_as or ctx.lei)
        and _want("ted_eu")
    ):
        tasks.append((
            "ted_eu",
            ted_adapter.fetch_by_identifiers(
                ctx.lei,
                ctx.registered_as,
                ctx.jurisdiction,
                derived=ctx.derived,
                legal_name=ctx.legal_name,
            ),
        ))
    return tasks


def _build_result_hit(source_id: str, result: Any, ctx: _LookupCtx) -> SourceHit | None:
    """Convert one adapter result to a SourceHit (None → no hit)."""
    if not isinstance(result, dict) or not result:
        return None
    if source_id == "climatetrace":
        # Climate TRACE stubs still carry GEM CSV data worth showing.
        return _bh_climatetrace(result, ctx) if result.get("entity_id") else None
    if source_id == "eiti":
        return _bh_eiti(result, ctx) if result.get("identification") else None
    if source_id == "eiti_soe":
        return _bh_eiti_soe(result, ctx) if result.get("is_state_owned") else None
    if source_id == "cac_nigeria":
        return _bh_cac_nigeria(result, ctx) if result.get("record") else None
    if source_id == "meip":
        return _bh_meip(result, ctx) if result.get("records") else None
    if source_id == "eiti_assessment":
        # A bundle with no assessment years is not a hit: the company is in
        # the index but EITI recorded nothing about it.
        return _bh_eiti_assessment(result, ctx) if result.get("assessments") else None
    if source_id == "eiti_bo":
        return _bh_eiti_bo(result, ctx) if result.get("record") else None
    if source_id == "wikirate":
        return _bh_wikirate(result, ctx) if result.get("card_id") else None
    if source_id == "ted_eu":
        # A zero-notice result is a legitimate absence, not a hit.
        return _bh_ted_eu(result, ctx) if result.get("total_notice_count") else None
    if result.get("is_stub"):
        return None
    if source_id == "opencorporates":
        return _bh_opencorporates(result, ctx) if ctx.ocid else None
    if source_id == "wikidata":
        return _bh_wikidata(result, ctx) if ctx.qid else None
    if source_id == "bods_gleif":
        return _bh_bods_gleif(result, ctx)
    spec = _REGISTRY_SOURCE_INDEX.get(source_id)
    if spec is None:
        return None
    local_id = _local_id_for(spec, ctx.derived)
    if not local_id:
        return None
    return spec.build(result, local_id, ctx)


#: Resolvers for globally-collapsing codes where "last source wins" is
#: wrong. Called as ``resolver(incumbent, candidate) -> winner``.
#:
#: COMPLEX_OWNERSHIP_LAYERS is the case that needs one. Statement ids are
#: namespaced per source (``_stable_id(source_id, ...)``), so no ownership
#: edge ever bridges two sources and ``bods_all`` is a concatenation of
#: disjoint subgraphs rather than one connected graph. Computing the depth
#: over the merged bundle is therefore *exactly* the maximum of the
#: per-source depths — verified — which means the correct merged answer is
#: reachable here without restructuring the per-source pipeline.
#:
#: Without this, the depth reported is whichever source happened to be
#: processed last: a lookup where one source finds a 5-layer chain and
#: another finds 3 reports 3 or 5 depending purely on ordering, and the
#: surviving ``longest_path`` is not the chain that justifies the number.
def _prefer_deeper_chain(
    incumbent: dict[str, Any], candidate: dict[str, Any]
) -> dict[str, Any]:
    inc = (incumbent.get("evidence") or {}).get("layers") or 0
    cand = (candidate.get("evidence") or {}).get("layers") or 0
    return candidate if cand > inc else incumbent


_COLLAPSE_RESOLVERS = {
    "COMPLEX_OWNERSHIP_LAYERS": _prefer_deeper_chain,
    # Phase 240: pools every source's state parties and regroups them by
    # state, so one 67% holding named by two sources is one holding.
    "STATE_CONTROLLED": _risk_merge_state_controlled,
}


# Codes that collapse GLOBALLY, on ``(code,)`` alone.
#
# Membership is not "is this a structural claim?" but a narrower test:
# **does the signal's evidence identify particular nodes?** The merge below
# assigns rather than combines, so a globally-collapsed code keeps only the
# last-processed source's evidence — and every node named by an earlier
# source loses the graph badge that ``buildSignalMap`` would have drawn from
# it. A code may therefore only live here if losing the other sources'
# evidence costs nothing.
#
# TRUST_OR_ARRANGEMENT and NOMINEE were moved OUT for that reason: both carry
# per-node ``statement_id``s in ``evidence.matches[]``, so GLEIF finding a
# Stiftung at E1 and Companies House finding a nominee at E7 collapsed to E7
# alone. They now dedup per source, like the jurisdiction signals.
#
# The three that remain are whole-structure claims and must NOT be moved
# out: per source they would fire once per source with conflicting values
# (different layer counts), and the chip strip picks its winner by
# confidence rather than depth, so the number shown would be arbitrary.
# COMPLEX_OWNERSHIP_LAYERS keeps its global collapse and resolves the
# conflict with ``_prefer_deeper_chain`` above instead.
#
# STATE_CONTROLLED (Phase 240) collapses globally too, and its resolver
# COMBINES rather than picks: ``merge_state_controlled`` pools every
# source's ``evidence.matches``, so no node loses its badge — the one
# condition the paragraph above sets for living here.
_STRUCTURAL_SIGNAL_CODES = {
    "COMPLEX_OWNERSHIP_LAYERS",
    "STATE_CONTROLLED",
    "COMPLEX_CORPORATE_STRUCTURE",
    "POSSIBLE_OBFUSCATION",
    "SANCTIONED_SECURITY",
}
_STATEMENT_SCOPED_SIGNAL_CODES = {
    "RELATED_PEP",
    "RELATED_SANCTIONED",
    "RELATED_COUNTER_SANCTIONED",
    "RELATED_SANCTIONS_CONTROLLED",
    "RELATED_SANCTIONS_LINKED",
    "RELATED_DEBARMENT",
    "RELATED_EXPORT_CONTROLLED",
    "RELATED_EXPORT_CONTROL_LINKED",
    "RELATED_EXPORT_RISK",
}


def _source_budget(source_id: str) -> float:
    """Wall-clock budget for one source inside a lookup (adapter-declared)."""
    adapter = REGISTRY.get(source_id)
    return getattr(adapter, "lookup_timeout_s", 30.0) if adapter else 30.0


def _merge_signals(
    *signal_lists: list[dict[str, Any]], record_as: str | None = None
) -> list[dict[str, Any]]:
    """Deduplicate risk signals: structural codes collapse globally,
    statement-scoped codes key on the subject statement, the rest on
    (code, source, hit).

    ``record_as`` opts this call into the ``signalstats`` instrumentation.
    Counting happens *here* rather than at the call site so that "count
    after dedup" is true by construction: the rules deciding what a
    distinct signal even is live in this function, and related-party paths
    now emit several signals per hit, so pre-dedup numbers would overstate.

    It defaults to ``None`` (don't count) rather than always counting
    because this helper has two callers — the lookup pipeline, which is the
    traffic worth measuring, and ``/report``, a hand-run free-text
    debugging endpoint. Counting both would inflate the per-lookup
    denominator with debugging runs and quietly corrupt the one ratio the
    instrumentation exists to produce. An opt-in default also means a
    future caller cannot skew the numbers merely by existing.
    """
    merged: dict[tuple, dict[str, Any]] = {}
    for signals in signal_lists:
        for sig in signals:
            if sig["code"] in _STRUCTURAL_SIGNAL_CODES:
                key: tuple = (sig["code"],)
            elif sig["code"] in _STATEMENT_SCOPED_SIGNAL_CODES or (
                sig["code"] == "OFFSHORE_LEAKS" and sig.get("source_id") == "icij"
            ):
                key = (
                    sig["code"],
                    sig["source_id"],
                    sig["hit_id"],
                    # A subject-level ICIJ match (Phase 235) is anchored on
                    # ``statement_id``; a related party's on ``subject_statement_id``.
                    sig.get("evidence", {}).get("subject_statement_id")
                    or sig.get("evidence", {}).get("statement_id", ""),
                )
            else:
                key = (sig["code"], sig["source_id"], sig["hit_id"])
            incumbent = merged.get(key)
            resolver = _COLLAPSE_RESOLVERS.get(sig["code"])
            merged[key] = (
                resolver(incumbent, sig) if incumbent is not None and resolver else sig
            )
    out = list(merged.values())
    if record_as:
        signalstats.record_signals(out)
    return out


# --- anchor resolution --------------------------------------------------------


class _LookupAbort(Exception):
    """Fatal lookup failure: HTTP status for /lookup, error event for SSE."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


async def _resolve_ctx(lei: str) -> tuple[_LookupCtx, dict[str, Any]]:
    """Resolve the GLEIF anchor and build the lookup context.

    Returns ``(ctx, gleif_bundle)`` with derived identifiers, OpenCorporates
    ID and Wikidata QID populated. Raises :class:`_LookupAbort` when the LEI
    cannot be resolved. Shared by the pipeline and /lookup-source.

    The anchor fetch runs inside a provenance scope and the result is stashed
    on ``ctx.provenance``. Every other source gets one via ``_run()``; GLEIF
    is resolved here, before that loop exists, which is why it needs its own.
    """
    gleif = REGISTRY["gleif"]
    ctx = _LookupCtx(lei=lei)
    registered_at_id = ""
    gleif_bundle: dict[str, Any] = {}
    override_bundle = bods_data.gleif_bundle_for_lei(lei)
    try:
        if override_bundle:
            ctx.legal_name, ctx.jurisdiction, ctx.registered_as = (
                _subject_metadata_from_bundle(override_bundle, lei)
            )
            if not ctx.legal_name:
                raise _LookupAbort(
                    404,
                    (
                        f"Found a BODS bundle for {lei} but couldn't locate "
                        "the subject entity statement. Re-run the extraction "
                        "script."
                    ),
                )
            gleif_bundle = {"source_id": "gleif", "lei": lei, "_from_bundle": True}
            # The same helper every other stored-bundle row uses, keyed, so the
            # anchor states Open Ownership's own publicationDate. A first pass
            # hardcoded `curated` here as "the same claim the stored-bundle
            # hits make" — it is not: `curated` describes a fixture committed
            # to the repo, and the row then showed no date at all next to
            # sibling rows reading "Snapshot, published <date>" off the very
            # same dataset.
            ctx.provenance = _stored_bundle_provenance("gleif", lei)
        else:
            # Only this line observes the cache or the network, so it is the
            # only part that needs the scope; the bundle branch above never
            # contacts GLEIF at all.
            with _provenance.recording() as recorder:
                gleif_bundle = await gleif.fetch(lei)
            ctx.provenance = recorder.resolve(
                is_stub=bool(gleif_bundle.get("is_stub"))
            )
            if gleif_bundle.get("is_stub") or not gleif_bundle.get("record"):
                raise _LookupAbort(
                    404,
                    (
                        f"No GLEIF record found for {lei}. Either the LEI is "
                        "not registered, live mode is disabled, or no Open "
                        "Ownership bundle has been extracted for this LEI "
                        "(see backend/scripts/extract_bods_subgraphs.py)."
                    ),
                )
            record_attrs = (gleif_bundle.get("record") or {}).get("attributes") or {}
            entity_block = record_attrs.get("entity") or {}
            ctx.legal_name = (entity_block.get("legalName") or {}).get("name") or ""
            ctx.jurisdiction = entity_block.get("jurisdiction") or ""
            ctx.registered_as = entity_block.get("registeredAs") or ""
            registered_at_id = (entity_block.get("registeredAt") or {}).get("id") or ""
            ctx.lei_registration = _lei_registration.from_gleif_record(
                gleif_bundle.get("record")
            )
    except _LookupAbort:
        raise
    except GleifRateLimitedError as exc:
        # Phase 143: the throttle refused to send (or GLEIF answered 429 twice)
        # and every fallback — stale cache, Golden Copy snapshot — came up
        # empty. A 503 with retry advice, not a 502: nothing is broken, the
        # shared upstream budget is momentarily spent.
        raise _LookupAbort(
            503,
            (
                "GLEIF is rate-limiting OpenCheck's shared connection and no "
                f"cached or snapshot copy of {lei} is available yet. "
                "Please retry in about a minute."
            ),
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise _LookupAbort(
            502, f"GLEIF fetch failed: {describe_exception(exc)}"
        ) from exc

    _build_derived(ctx, registered_at_id)

    # OpenCorporates ID from the GLEIF Level-1 record. When the anchor came
    # from a curated Open Ownership bundle (`_from_bundle`), this is also the
    # only live GLEIF call the anchor makes — so when it succeeds, its
    # provenance replaces the bundle's "Snapshot, published <date>" claim on
    # the GLEIF source card: the card is now honestly describing the live
    # fetch that actually produced ocid/spglobal, rather than the date Open
    # Ownership harvested its bulk dataset. The curated bundle's own BODS
    # ownership statements — the richer multi-layer chain GLEIF's live API
    # can't produce (see bods_data.py) — are untouched; only this one badge
    # changes. A failed or unavailable live call leaves the bundle's snapshot
    # provenance in place, same as before.
    if gleif.info.live_available:
        try:
            if gleif_bundle.get("_from_bundle"):
                with _provenance.recording() as live_recorder:
                    gleif_src = await gleif.fetch(lei)
                if not gleif_src.get("is_stub"):
                    ctx.provenance = live_recorder.resolve(is_stub=False)
            else:
                gleif_src = gleif_bundle
            if not gleif_src.get("is_stub"):
                if ctx.lei_registration is None:
                    # A curated Open Ownership bundle holds no registration
                    # block; the live call is the one place it can come from.
                    ctx.lei_registration = _lei_registration.from_gleif_record(
                        gleif_src.get("record")
                    )
                attrs = (gleif_src.get("record") or {}).get("attributes") or {}
                ctx.ocid = attrs.get("ocid") or None
                sp = attrs.get("spglobal")
                ctx.spglobal = (sp[0] if isinstance(sp, list) and sp else sp) or None
        except Exception as exc:  # noqa: BLE001
            # Non-fatal, but not free: without ocid the OpenCorporates
            # dispatch is skipped. Log so a GLEIF outage doesn't read as
            # "this entity has no OpenCorporates record".
            _LOG.warning(
                "GLEIF identifier extraction failed for %s: %s: %s — "
                "OpenCorporates dispatch will be skipped for this lookup.",
                lei,
                type(exc).__name__,
                exc,
            )
    if ctx.ocid:
        ctx.derived["ocid"] = ctx.ocid

    wikidata_adapter = REGISTRY["wikidata"]
    if hasattr(wikidata_adapter, "find_qid_by_lei"):
        # Phase 244: the QID is optional enrichment, so a slow or failing
        # Wikidata Query Service must never fail the lookup. Unguarded, a WDQS
        # ReadTimeout escaped the pipeline and every fresh /lookup answered
        # HTTP 500 (Equinor and Rosneft, 24 Sept 2026). The failure is kept on
        # the context, not swallowed: `_dispatch` reports it as Wikidata's
        # `source_error`, so the report says Wikidata did not answer instead
        # of implying the company has no Wikidata record.
        try:
            ctx.qid = await wikidata_adapter.find_qid_by_lei(lei)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            ctx.qid = None
            ctx.qid_error = exc
            _LOG.warning(
                "Wikidata QID lookup failed for %s: %s — Wikidata will be "
                "reported as not answering for this lookup.",
                lei,
                describe_exception(exc),
            )
    if ctx.qid:
        ctx.derived["wikidata_qid"] = ctx.qid

    return ctx, gleif_bundle


# --- the pipeline -----------------------------------------------------------


async def _lookup_pipeline(
    lei: str, deepen_top: int = 5
) -> AsyncIterator[LookupEvent]:
    """Single source of truth for the LEI-anchored lookup.

    Yields ``(event, payload)`` tuples. Events mirror the SSE vocabulary
    (source_started, gleif_done, hit, source_completed, source_error,
    sources_applicable, cross_source_links, bods_counts, risk_signals,
    done, error) plus two internal events consumed only by the sync
    collector: deepen_result and deepen_error. ``hit`` payloads are
    SourceHit objects; everything else is JSON-serialisable dicts.
    """
    _degradation.begin()
    # One outbound call budget per lookup, opened here because this is the
    # outermost frame of the fan-out: the source tasks below copy this context
    # on creation, so all of them — the subject's sources and every deepened
    # related party — spend from one counter per rate-capped source. Without
    # this call the budgets in gemi_greece.py are inert (see outbound_rate).
    _outbound_rate.begin()
    lei = lei.strip().upper()
    if not _LEI_SHAPE.match(lei):
        yield ("error", {
            "status": 400,
            "detail": (
                f"{lei!r} is not a valid LEI. ISO 17442 LEIs are "
                "20-character alphanumeric strings (e.g. "
                "213800LH1BZH3DI6G760)."
            ),
        })
        return
    check_digit_error = identifiers.lei_check_digit_error(lei)
    if check_digit_error:
        yield ("error", {"status": 400, "detail": check_digit_error})
        return

    gleif = REGISTRY["gleif"]
    yield ("source_started", {"source_id": "gleif", "source_name": gleif.info.name})

    try:
        ctx, gleif_bundle = await _resolve_ctx(lei)
    except _LookupAbort as abort:
        yield ("error", {"status": abort.status, "detail": abort.detail})
        return

    yield ("gleif_done", {
        "lei": lei,
        "legal_name": ctx.legal_name or None,
        "jurisdiction": ctx.jurisdiction or None,
        "derived_identifiers": ctx.derived,
    })

    # Phase 224: what is knowable about a company *here*, as soon as "here" is
    # known. Pure (reads data/jurisdictions.json, never a source), so it costs
    # nothing to emit before the fan-out; frozen into the event so a saved
    # report replays the sentence that was true on the day it ran — the
    # sentence is dated (``next_change_expected``) and a saved render reads
    # no clock (Phase 218). No jurisdiction → no event: there is nothing to
    # make a statement about, and the strip says nothing rather than guessing.
    if ctx.jurisdiction:
        yield ("knowability", _knowability_payload(ctx.jurisdiction))

    # Phase 236: the subject's primary stock-exchange listing, from PermID.
    # Started here so its three requests overlap the fan-out, and awaited
    # just before `subject_profile`. A PermID failure rides the same event
    # as `status: "unavailable"` — said on the listing line, never in
    # `degraded_sources`, which every reader treats as a screen that did not
    # run. No key → no task and no event.
    listing_task: asyncio.Task[dict[str, Any] | None] | None = (
        asyncio.create_task(_listing.fetch_listing(ctx.lei)) if _listing.available() else None
    )

    gleif_hit = _build_gleif_hit(ctx, gleif_bundle)
    _stamp(gleif_hit, ctx.provenance)
    hits: list[SourceHit] = [gleif_hit]
    deepened_bundles: list[tuple[str, str]] = [("gleif", lei)]
    yield ("hit", gleif_hit)
    yield ("source_completed", {"source_id": "gleif", "hit_count": 1})

    dispatch = _dispatch(ctx)
    se_adapter = REGISTRY.get("sec_edgar")
    sec_applicable = bool(
        ctx.jurisdiction.upper().startswith("US")
        and (ctx.derived.get("edgar_cik") or ctx.legal_name)
        and se_adapter
        and se_adapter.info.live_available
    )
    # EveryPolitician is screened from the risk stage rather than the dispatch
    # loop — it is keyed on the *names* of related parties, which do not exist
    # until the BODS bundle is assembled. It was therefore queried on every
    # lookup and announced on none of them, so the report listed OpenSanctions
    # among the sources checked and not the PEP dataset checked beside it.
    # It is applicable exactly when the screen can run; the terminal event
    # comes late, which is honest — it is still running.
    ep_applicable = _name_screen_can_run()
    applicable_ids = (
        [sid for sid, _ in dispatch]
        + (["sec_edgar"] if sec_applicable else [])
        + (["everypolitician"] if ep_applicable else [])
    )
    yield ("sources_applicable", {"source_ids": applicable_ids})
    for sid in applicable_ids:
        if sid == "sec_edgar":
            continue  # announced only once a CIK has actually been resolved
        src_name = REGISTRY[sid].info.name if sid in REGISTRY else sid
        yield ("source_started", {"source_id": sid, "source_name": src_name})

    async def _run(src_id: str, coro: Any) -> tuple[str, Any, Provenance]:
        budget = _source_budget(src_id)
        # One provenance scope per source. Cache reads and HTTP client
        # construction beneath this await record themselves into it, so the
        # resolved value describes what this source actually did.
        with _provenance.recording() as recorder:
            try:
                result = await asyncio.wait_for(coro, timeout=budget)
            except asyncio.TimeoutError:
                return src_id, TimeoutError(
                    f"source exceeded its {budget:.0f}s time budget"
                ), _provenance.STUB_PROVENANCE
            except Exception as exc:  # noqa: BLE001
                return src_id, exc, _provenance.STUB_PROVENANCE
        is_stub = bool(result.get("is_stub")) if isinstance(result, dict) else False
        return src_id, result, recorder.resolve(is_stub=is_stub)

    errors: dict[str, str] = {}
    # Seeded with the anchor, which was resolved before this loop existed.
    provenances: dict[str, Provenance] = (
        {"gleif": ctx.provenance} if ctx.provenance is not None else {}
    )
    oc_result_processed = False
    pending = {asyncio.create_task(_run(sid, coro)) for sid, coro in dispatch}
    while pending:
        done_set, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )
        for task in done_set:
            source_id, result, source_prov = task.result()
            provenances[source_id] = source_prov

            if isinstance(result, Exception):
                # A stored OO bundle is canonical — serve it instead of
                # surfacing the live error (e.g. a Companies House outage).
                bkey = _stored_bundle_key(source_id, ctx)
                if bkey is not None:
                    sh = _stored_bundle_hit(source_id, bkey, ctx)
                    hits.append(sh)
                    deepened_bundles.append((source_id, bkey))
                    yield ("hit", sh)
                    yield ("source_completed", {"source_id": source_id, "hit_count": 1})
                    continue
                errors[source_id] = _fmt_source_error(result)
                if isinstance(result, SourceSchemaError):
                    _err_type = "schema_changed"
                elif isinstance(result, TimeoutError):
                    _err_type = "timeout"
                else:
                    _err_type = "fetch_error"
                yield ("source_error", {
                    "source_id": source_id,
                    "error": errors[source_id],
                    "error_type": _err_type,
                })
                continue

            # List-result sources (search-style adapters).
            if source_id in ("opensanctions", "openaleph"):
                list_hits = (
                    [h for h in result if not h.is_stub]
                    if isinstance(result, list)
                    else []
                )
                for sh in list_hits:
                    _stamp(sh, source_prov)
                    hits.append(sh)
                    deepened_bundles.append((source_id, sh.hit_id))
                    yield ("hit", sh)
                yield ("source_completed", {
                    "source_id": source_id, "hit_count": len(list_hits),
                })
                continue

            hit = _stamp(_build_result_hit(source_id, result, ctx), source_prov)
            if hit is None:
                # No live hit (stub / not found). If a stored OO bundle exists,
                # surface it anyway so the source card isn't lost to a live outage.
                bkey = _stored_bundle_key(source_id, ctx)
                if bkey is not None:
                    hit = _stored_bundle_hit(source_id, bkey, ctx)
                    # Keyed: without `bkey` the helper cannot open the bundle
                    # and falls back to a dateless "Open Ownership bulk
                    # dataset", so the one thing a snapshot most needs to say
                    # — when it was published — went missing.
                    provenances[source_id] = _stored_bundle_provenance(source_id, bkey)
                    _stamp(hit, provenances[source_id])
            if hit is not None:
                hits.append(hit)
                deepened_bundles.append((source_id, hit.hit_id))
                yield ("hit", hit)
                yield ("source_completed", {"source_id": source_id, "hit_count": 1})
            else:
                yield ("source_completed", {"source_id": source_id, "hit_count": 0})

            # OpenCorporates may reveal a SEC EDGAR CIK — surface immediately.
            if (
                source_id == "opencorporates"
                and hit is not None
                and not oc_result_processed
            ):
                cik = _extract_edgar_cik(result.get("company") or {})
                if cik:
                    ctx.derived["edgar_cik"] = cik
                if ctx.jurisdiction.upper().startswith("US"):
                    oc_result_processed = True
                    if cik and se_adapter and se_adapter.info.live_available:
                        edgar_hit = _edgar_hit(cik, ctx.legal_name)
                        hits.append(edgar_hit)
                        deepened_bundles.append(("sec_edgar", cik))
                        yield ("source_started", {
                            "source_id": "sec_edgar",
                            "source_name": se_adapter.info.name,
                        })
                        yield ("hit", edgar_hit)
                        yield ("source_completed", {
                            "source_id": "sec_edgar", "hit_count": 1,
                        })

    # SEC EDGAR fallback: resolve the CIK from the legal name.
    if (
        ctx.jurisdiction.upper().startswith("US")
        and not ctx.derived.get("edgar_cik")
        and ctx.legal_name
        and se_adapter
        and se_adapter.info.live_available
    ):
        try:
            cik2 = await asyncio.wait_for(
                se_adapter.resolve_cik(ctx.legal_name),  # type: ignore[attr-defined]
                timeout=_source_budget("sec_edgar"),
            )
            if cik2:
                edgar_hit = _edgar_hit(cik2, ctx.legal_name)
                hits.append(edgar_hit)
                deepened_bundles.append(("sec_edgar", cik2))
                yield ("source_started", {
                    "source_id": "sec_edgar",
                    "source_name": se_adapter.info.name,
                })
                yield ("hit", edgar_hit)
                yield ("source_completed", {"source_id": "sec_edgar", "hit_count": 1})
            else:
                # A name that resolves to no CIK is a completed search with no
                # result, not an unfinished one. Announcing it matters because
                # ``sec_edgar`` is in ``sources_applicable`` from the moment a
                # US jurisdiction and a legal name exist: without this the
                # client's progress counter can never reach its own total, and
                # a finished lookup ends reading "11 of 12 sources answered".
                yield ("source_started", {
                    "source_id": "sec_edgar",
                    "source_name": se_adapter.info.name,
                })
                yield ("source_completed", {"source_id": "sec_edgar", "hit_count": 0})
        except Exception as exc:  # noqa: BLE001
            errors["sec_edgar"] = _fmt_source_error(exc)
            yield ("source_error", {
                "source_id": "sec_edgar",
                "error": errors["sec_edgar"],
                "error_type": (
                    "schema_changed"
                    if isinstance(exc, SourceSchemaError)
                    else "fetch_error"
                ),
            })

    # Reconcile + search-time risk.
    links = [link.to_dict() for link in reconcile(hits)]
    search_signals = [s.to_dict() for s in assess_hits(hits)]
    yield ("cross_source_links", {"links": links})

    # Deepen the top N bundles (BODS mapping + per-bundle risk).
    bods_all: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    deepen_signals: list[dict[str, Any]] = []
    license_notices: list[dict[str, str]] = []
    bods_counts: dict[str, int] = {}
    # Per-hit entity / relationship split, so the UI can show the graph shape
    # ("N entities · M relationships") before the source is deepened on demand.
    bods_breakdown: dict[str, dict[str, int]] = {}

    # Person-capable registers + stored OO bundles are always deepened, even
    # past the top-N cap, so the connected-people list and canonical graphs
    # don't depend on a nondeterministic completion-order race (issue #73).
    deepen_pairs = _select_deepen_pairs(deepened_bundles, deepen_top, ctx)
    # Phase 241: a read that failed after the source answered is a partial
    # answer, recorded as a degradation below (``add_source_errors``).
    deepen_errors: dict[str, str] = {}
    deepen_raw = await asyncio.gather(
        *[
            # Deepen usually replays the adapter's cached fetch, but give it
            # the same wall-clock protection as dispatch (+ mapping headroom).
            asyncio.wait_for(
                _safe_deepen(dsrc, dhit), timeout=_source_budget(dsrc) + 15.0
            )
            for dsrc, dhit in deepen_pairs
        ],
        return_exceptions=True,
    )
    for (dsrc, dhit), deep in zip(deepen_pairs, deepen_raw):
        if isinstance(deep, Exception):
            deepen_errors.setdefault(dsrc, _fmt_source_error(deep))
            yield ("deepen_error", {
                "source_id": dsrc,
                "error": deepen_errors[dsrc],
            })
            continue
        if deep is None:
            continue
        bods_all.extend(deep["bods"])
        bods_issues.extend(deep["bods_issues"])
        deepen_signals.extend(deep["risk_signals"])
        if deep.get("license_notice"):
            license_notices.append({
                "source_id": dsrc, "hit_id": dhit, "notice": deep["license_notice"],
            })
        stmts = deep["bods"]
        # Only for a source the dispatch loop never saw: a dispatched source's
        # own fetch is the better claim, and a deepen usually replays it from
        # cache, which would downgrade a live row to "cached".
        deep_prov = deep.get("provenance")
        if dsrc not in provenances and isinstance(deep_prov, Provenance):
            provenances[dsrc] = deep_prov
        bods_counts[f"{dsrc}:{dhit}"] = len(stmts)
        bods_breakdown[f"{dsrc}:{dhit}"] = {
            "entities": sum(1 for s in stmts if s.get("recordType") == "entity"),
            # Counted separately because the row chip is labelled by the
            # entity figure alone: calling that total "parties" hid every
            # natural person the source disclosed behind a number that
            # excluded them.
            "persons": sum(1 for s in stmts if s.get("recordType") == "person"),
            "relationships": sum(1 for s in stmts if s.get("recordType") == "relationship"),
        }
        # Phase 217: the rest of what /deepen would answer for this pair,
        # minus the raw record. The stream skips this event; it rides in the
        # replay cache and so in a saved report, whose source drawers render
        # from it instead of calling /deepen for today's record.
        _dinfo = REGISTRY[dsrc].info if dsrc in REGISTRY else None
        yield ("deepen_result", {
            "source_id": dsrc, "hit_id": dhit, "bods": deep["bods"],
            "bods_issues": deep["bods_issues"],
            "risk_signals": deep["risk_signals"],
            "license": _dinfo.license if _dinfo is not None else "",
            "license_notice": deep.get("license_notice"),
        })

    # Lightweight counts for the remaining (non-deepened) sources, so every
    # source can show its entity/relationship split up front — map-only on the
    # cached bundle, decoupled from the deepen_top cap on full deepens.
    _counted = set(bods_counts)
    _remaining = [
        pair for pair in deepened_bundles if f"{pair[0]}:{pair[1]}" not in _counted
    ]
    if _remaining:
        count_raw = await asyncio.gather(
            *[_count_only(dsrc, dhit) for dsrc, dhit in _remaining],
            return_exceptions=True,
        )
        for (dsrc, dhit), cnt in zip(_remaining, count_raw):
            if isinstance(cnt, BaseException) or not cnt:
                continue
            key = f"{dsrc}:{dhit}"
            count_prov = cnt.get("provenance")
            if dsrc not in provenances and isinstance(count_prov, Provenance):
                provenances[dsrc] = count_prov
            bods_counts[key] = cnt["total"]
            bods_breakdown[key] = {
                "entities": cnt["entities"],
                "persons": cnt.get("persons", 0),
                "relationships": cnt["relationships"],
            }

    yield ("bods_counts", {"counts": bods_counts, "breakdown": bods_breakdown})

    # Two deepened results from one source can carry the same party (two
    # OpenSanctions records naming one subsidiary): every consumer below —
    # the profile, the screens, the risk engine — reads one statement per id.
    bods_all = unique_statements(bods_all)

    if listing_task is not None:
        try:
            listing_payload = await listing_task
        except Exception:  # noqa: BLE001 — fetch_listing reports its own failures
            listing_payload = None
        if listing_payload is not None:
            yield ("listing", listing_payload)

    # The subject's profile, from its own statements across the deepened
    # sources. Its own event rather than a rider on `risk_signals`: it is
    # identity, and the verdict event is the answer.
    yield (
        "subject_profile",
        {
            "profile": build_subject_profile(
                ctx.lei, bods_all, lei_registration=ctx.lei_registration
            )
        },
    )

    # Phase 226: what is knowable along the ownership path — one statement
    # per jurisdiction on the upward walk from the subject's own statements,
    # in path order. The subject's statement already rode the `knowability`
    # event right after gleif_done; this one needs the deepened graph, so it
    # comes here. Pure (reads jurisdictions.json), frozen into the event so a
    # saved report replays the chain that was true on the day it ran. The
    # FullCheck view extends it client-side after each /expand-layer through
    # GET /knowability; a saved report shows this, the run's own depth.
    yield ("knowability_chain", knowability_chain_for_lei(ctx.lei, bods_all))

    yield (
        "possibly_same_entities",
        {"pairs": [p.to_dict() for p in possibly_same_entities(bods_all)]},
    )

    # Seeded with whatever the source adapters recorded during the fetches
    # above — a source that could not answer from its own data says so there,
    # not only in the server log. The derived screens append to the same list.
    degraded: list[DegradedSource] = _degradation.collect()
    # Phase 241: every source error is a degradation — one that returned
    # nothing, and one that returned a record and then failed to be read.
    _degradation.add_source_errors(
        degraded,
        {**deepen_errors, **errors},
        with_data={h.source_id for h in hits if not h.is_stub},
    )
    # As in _build_report: the fan-out is done, so the budgets close with the
    # degradation scope and the derived screens run outside both.
    _outbound_rate.end()
    oa_screening: list[dict[str, Any]] = []
    name_screen = NameScreen()
    cross_raw, icij_raw, oa_raw = await asyncio.gather(
        # Phase 235: the looked-up company is never its own related party.
        assess_cross_source_names(
            bods_all, degraded=degraded, screen=name_screen, subject_lei=ctx.lei
        ),
        assess_icij_names(bods_all, degraded=degraded, subject_lei=ctx.lei),
        assess_openaleph_names(
            bods_all, degraded=degraded, screening=oa_screening, subject_lei=ctx.lei
        ),
    )
    # EveryPolitician's terminal event, and its rows.
    #
    # The screen queried it once per related person; the report can now say so.
    # A row is a *name match on a related party*, not a fact about the subject
    # — `finding_everypolitician` says exactly that on every row, because a
    # card headed with the subject's name is otherwise an invitation to read
    # it as one. Deduplicated by hit id: two related parties sharing a name
    # match the same record, and the same politician twice is not two findings.
    if ep_applicable:
        seen_ep: set[str] = set()
        ep_count = 0
        for match in name_screen.matches:
            if match.source_id != "everypolitician" or match.hit.hit_id in seen_ep:
                continue
            seen_ep.add(match.hit.hit_id)
            ep_count += 1
            yield ("hit", match.hit.model_copy(update={
                "finding": finding_everypolitician(
                    match.hit.summary, match.target_name, former=match.former
                ),
            }))
        yield ("source_completed", {
            "source_id": "everypolitician",
            "hit_count": ep_count,
            "names_screened": name_screen.names_screened,
        })

    # Sanctioned-securities chip: cheap in-memory lookup of the subject LEI in
    # the OpenSanctions securities index (no network). No-op when the index
    # isn't configured.
    from .. import securities as _securities

    sec_sig = _securities.sanctioned_securities_signal(lei)
    sec_signals = [sec_sig] if sec_sig else []

    merged = _merge_signals(
        search_signals,
        deepen_signals,
        [s.to_dict() for s in cross_raw],
        [s.to_dict() for s in icij_raw],
        [s.to_dict() for s in oa_raw],
        sec_signals,
        record_as="lookup",
    )
    degraded_dicts = [d.to_dict() for d in degraded]
    # The degradation counters are recorded next to the signal counters for
    # the same reason degraded_sources rides on the same event as the
    # signals: a signal count without the count of screens that failed to
    # run is not a low number, it is an unknown one.
    signalstats.record_degraded(degraded_dicts)
    signalstats.record_lookup()
    # Record consistency, shadow mode (Phase 152): compare what independent
    # sources said about the same entity and count the outcomes. CPU-only,
    # runs after the network stage, emits nothing to the client yet — the
    # counters at /consistencystats decide which comparisons earn a place
    # on the page (see opencheck/consistency.py).
    consistencystats.record(consistency.assess_consistency(bods_all))
    # degraded_sources rides on the same event as the signals so every
    # consumer (SSE UI, sync /lookup, replay cache, narrative, exports)
    # sees the two together — an empty signals list plus a non-empty
    # degraded list must never be split apart into "clean screen".
    # openaleph_screening rides here too: the informational (sub-signal)
    # percolation matches belong with the signals they didn't become.
    # The verdict rides the same event as the signals and the degradations
    # for the same reason they ride together: a sentence about what was
    # found is only honest next to the count of screens that did not run,
    # and this way a replayed run replays the sentence too.
    yield (
        "risk_signals",
        {
            "signals": merged,
            "degraded_sources": degraded_dicts,
            "verdict": build_verdict(merged, degraded_dicts),
            "openaleph_screening": oa_screening,
            "source_liveness": {
                sid: prov.to_dict() for sid, prov in sorted(provenances.items())
            },
            "graph_shape": _graph_shape(bods_all, merged),
        },
    )

    yield ("done", {
        "lei": lei,
        "bods_issues": bods_issues,
        "license_notices": license_notices,
    })


# --- endpoints ---------------------------------------------------------------


async def _lookup_impl(
    lei: str, deepen_top: int = 5, refresh: bool = False
) -> LookupResponse:
    """Body of ``/lookup``, callable in-process (MCP tools, /narrative,
    /export, layer expansion) without going through the rate-limited route.

    Not free, though: a fresh run is charged to the current client's lookup
    budget (Phase 234) wherever it is called from, and a spent budget raises
    429 with ``Retry-After``."""
    norm_lei = lei.strip().upper()
    events: list[LookupEvent] = []
    async for event in _lookup_pipeline_cached(
        norm_lei, deepen_top=deepen_top, refresh=refresh
    ):
        if event[0] == "error":
            # Raise at once, exactly as before the fold was factored out.
            retry = event[1].get("retry_after_s")
            raise HTTPException(
                status_code=event[1]["status"],
                detail=event[1]["detail"],
                headers={"Retry-After": str(retry)} if retry else None,
            )
        events.append(event)
    return fold_lookup_events(norm_lei, events)


@router.get("/lookup", response_model=LookupResponse)
@limiter.limit(lookup_tier)
async def lookup(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
) -> LookupResponse:
    """Driver endpoint: LEI in, full cross-source synthesis out.

    Collects the events of :func:`_lookup_pipeline` into one response —
    identical data to /lookup-stream, without the streaming.
    """
    return await _lookup_impl(lei=lei, deepen_top=deepen_top, refresh=refresh)


@router.get("/lookup-stream")
@limiter.limit(lookup_tier)
async def lookup_stream(
    request: Request,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
) -> Response:
    """LEI-anchored lookup streamed as SSE — same pipeline as /lookup.

    Phase 144: declared automated clients are refused here. This endpoint
    exists for the interactive app; robots.txt has always disallowed it, and
    a crawler running it anyway triggers the full adapter fan-out and drains
    the shared upstream budgets (GLEIF's 60 req/min IP cap first among them)
    that human lookups queue behind — measured live on 2026-08-29, bots were
    ~19 lookup-streams/min while human cold anchors stalled ~15–21s. The
    per-IP slowapi limit cannot catch a distributed crawler fleet; the
    User-Agent gate at least removes every honest bot. The plain ``/lookup``
    JSON API is deliberately NOT gated — `python`/`curl`/`httpx` UAs are its
    legitimate callers — and the ``/entity`` pages serve crawlers from bulk
    data at any volume.
    """
    if get_settings().bot_gate_lookup_stream and is_bot(
        request.headers.get("user-agent")
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "/lookup-stream serves the interactive OpenCheck app and is "
                "disallowed for automated clients (see /robots.txt). Use the "
                "JSON API at /lookup?lei=<LEI> (rate-limited), or the "
                "crawlable per-entity pages at /entity/<LEI>."
            ),
        )
    return EventSourceResponse(
        _lookup_sse_events(lei, deepen_top=deepen_top, refresh=refresh)
    )


async def _lookup_sse_events(
    lei: str, deepen_top: int = 5, refresh: bool = False
) -> AsyncIterator[dict[str, Any]]:
    """Serialise pipeline events as SSE frames."""
    async for event, payload in _lookup_pipeline_cached(
        lei, deepen_top=deepen_top, refresh=refresh
    ):
        if event in ("deepen_result", "deepen_error"):
            continue  # internal events for the sync collector only
        if event == "hit":
            yield {"event": "hit", "data": payload.model_dump_json()}
        else:
            yield {"event": event, "data": json.dumps(payload)}


class LookupSourceResponse(BaseModel):
    """Result of re-running a single source within an existing lookup."""

    lei: str
    source_id: str
    hits: list[SourceHit]
    error: str | None = None


@router.get("/lookup-source", response_model=LookupSourceResponse)
@limiter.limit(default_tier)
async def lookup_source(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    source_id: str = Query(..., description="Adapter id to re-run, e.g. 'kvk'."),
) -> LookupSourceResponse:
    """Re-run one source for a LEI — powers the per-source retry button.

    Resolves the GLEIF anchor (cheap — adapter-cached), dispatches just the
    requested source, and invalidates the replay cache so the next full
    lookup reflects the fresh result.
    """
    norm_lei = lei.strip().upper()
    if not _LEI_SHAPE.match(norm_lei):
        raise HTTPException(
            status_code=400,
            detail=(
                f"{norm_lei!r} is not a valid LEI. ISO 17442 LEIs are "
                "20-character alphanumeric strings (e.g. "
                "213800LH1BZH3DI6G760)."
            ),
        )
    check_digit_error = identifiers.lei_check_digit_error(norm_lei)
    if check_digit_error:
        raise HTTPException(status_code=400, detail=check_digit_error)

    try:
        ctx, _gleif_bundle = await _resolve_ctx(norm_lei)
    except _LookupAbort as abort:
        raise HTTPException(status_code=abort.status, detail=abort.detail)

    tasks = _dispatch(ctx, only=source_id)
    if not tasks:
        raise HTTPException(
            status_code=404,
            detail=(
                f"source {source_id!r} is not applicable to {norm_lei} "
                "(no derived identifier for it on this LEI record)"
            ),
        )

    hits: list[SourceHit] = []
    error: str | None = None
    for sid, coro in tasks:
        try:
            result = await asyncio.wait_for(coro, timeout=_source_budget(sid))
        except asyncio.TimeoutError:
            error = (
                f"TimeoutError: source exceeded its "
                f"{_source_budget(sid):.0f}s time budget"
            )
            continue
        except Exception as exc:  # noqa: BLE001
            error = _fmt_source_error(exc)
            continue
        if sid in ("opensanctions", "openaleph"):
            if isinstance(result, list):
                hits.extend(h for h in result if not h.is_stub)
        else:
            hit = _build_result_hit(sid, result, ctx)
            if hit is not None:
                hits.append(hit)

    _invalidate_replay(norm_lei)
    return LookupSourceResponse(
        lei=norm_lei, source_id=source_id, hits=hits, error=error
    )


def _subject_metadata_from_bundle(
    bundle: list[dict[str, Any]], lei: str
) -> tuple[str, str, str]:
    """Extract ``(legal_name, jurisdiction_code, registered_as)`` from the entity statement."""
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
        # v0.4 field is "jurisdiction"; OO bulk BODS pass-through still uses
        # the legacy "incorporatedInJurisdiction" — accept both.
        jur = rd.get("jurisdiction") or rd.get("incorporatedInJurisdiction") or {}
        jurisdiction = (jur.get("code") or "").upper() if isinstance(jur, dict) else ""
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
    """Return the Open Ownership canonical BODS bundle for this (source_id, hit_id) pair."""
    if source_id == "gleif":
        return bods_data.gleif_bundle_for_lei(hit_id)
    if source_id == "companies_house":
        if hit_id.isalnum() and len(hit_id) == 8:
            return bods_data.uk_bundle_for_company_number(hit_id)
    return None


# Sources that ship pre-extracted Open Ownership BODS bundles, keyed by their
# derived id: (bundle subdir under data/cache/bods_data, key extractor). These
# must surface from the stored bundle regardless of the *live* source's health
# — the bundle is canonical, not a fallback — so a Companies House outage can't
# blank out a curated example's UK-PSC graph.
_STORED_BUNDLE_SOURCES: dict[str, tuple[str, Any]] = {
    "gleif": ("gleif", lambda ctx: ctx.lei),
    "companies_house": ("uk", lambda ctx: ctx.derived.get("gb_coh")),
}


def _stored_bundle_key(source_id: str, ctx: "_LookupCtx") -> str | None:
    """The deepen hit_id for *source_id* iff a stored OO bundle exists for it."""
    spec = _STORED_BUNDLE_SOURCES.get(source_id)
    if spec is None:
        return None
    subdir, key_fn = spec
    key = key_fn(ctx)
    if key and bods_data.has_bundle(subdir, key):
        return key
    return None


def _stored_bundle_hit(source_id: str, key: str, ctx: "_LookupCtx") -> SourceHit:
    """Minimal hit so a stored-bundle source still shows a card when its live
    fetch failed; the deepen step serves the OO bundle for the graph."""
    summary, ids = key, {}
    if source_id == "companies_house":
        summary, ids = f"GB-COH {key}", {"gb_coh": key}
    hit = _hit(source_id, key, name=ctx.legal_name or "",
               summary=summary, identifiers=ids, raw={})
    return _stamp(hit, _stored_bundle_provenance(source_id, key)) or hit


def _stored_bundle_provenance(source_id: str, key: str | None = None) -> Provenance:
    """Provenance for a pre-extracted Open Ownership bundle.

    These are a bulk snapshot, not a live call, and they carry Open Ownership's
    own ``publicationDetails.publicationDate`` — the date that dataset was
    published, which is a far better statement of currency than the date we
    happen to serve it. The latest publication date across the bundle is used.
    """
    spec = _STORED_BUNDLE_SOURCES.get(source_id)
    if spec is None or not key:
        return Provenance(liveness="snapshot", detail="Open Ownership bulk dataset")
    subdir, _ = spec
    published: str | None = None
    try:
        for statement in bods_data.load_bundle(subdir, key) or []:
            candidate = (statement.get("publicationDetails") or {}).get(
                "publicationDate"
            )
            if isinstance(candidate, str) and (
                published is None or candidate > published
            ):
                published = candidate
    except Exception:  # noqa: BLE001 - provenance must never sink a lookup
        published = None
    retrieved: datetime | None = None
    if published:
        try:
            retrieved = datetime.fromisoformat(published).replace(tzinfo=timezone.utc)
        except ValueError:
            retrieved = None
    return Provenance(
        liveness="snapshot",
        retrieved_at=retrieved,
        detail="Open Ownership bulk dataset"
        + (f", published {published}" if published else ""),
    )


def _select_deepen_pairs(
    deepened_bundles: list[tuple[str, str]],
    deepen_top: int,
    ctx: "_LookupCtx",
) -> list[tuple[str, str]]:
    """Choose which (source_id, hit_id) bundles to deepen (map + risk-assess).

    The top ``deepen_top`` by arrival order, plus two carve-outs that are always
    deepened even past the cap so results don't depend on a nondeterministic
    completion-order race (issue #73):

    * person-capable sources — official company registers + OpenCorporates,
      which emit the officers / PSCs / beneficial owners the people list is
      built from;
    * stored OO bundles (GLEIF, UK PSC), whose canonical graph must never drop
      behind other hits.

    Arrival order is preserved and duplicates are removed, so the selection is a
    deterministic function of the (deduplicated) input.
    """
    deepen_pairs = list(deepened_bundles[:deepen_top])
    seen = set(deepen_pairs)
    for pair in deepened_bundles[deepen_top:]:
        if pair in seen:
            continue
        if pair[0] in _PERSON_CAPABLE_SOURCES or _stored_bundle_key(pair[0], ctx) == pair[1]:
            deepen_pairs.append(pair)
            seen.add(pair)
    return deepen_pairs


async def _count_only(source_id: str, hit_id: str) -> dict[str, Any] | None:
    """Map a (cached) bundle just to count BODS statements — no risk/validate.

    Lets every source surface its graph shape ("N entities · M relationships")
    up front, without the cost of a full deepen (which stays capped at
    ``deepen_top``). The fetch is a cache hit from dispatch, so this is map-only."""
    adapter = REGISTRY.get(source_id)
    if adapter is None:
        return None
    override = _bods_data_override(source_id, hit_id)
    if override is not None:
        bods: list[dict[str, Any]] = override
        prov = _stored_bundle_provenance(source_id, hit_id)
    else:
        try:
            raw, prov = await _fetch_with_provenance(adapter, hit_id)
        except Exception:  # noqa: BLE001
            return None
        mapper = _mapper_for(source_id)
        if mapper is None or raw.get("is_stub"):
            return None
        with _provenance.mapping_provenance(prov):
            bods = unique_statements(mapper(raw))
    return {
        "total": len(bods),
        "entities": sum(1 for s in bods if s.get("recordType") == "entity"),
        "persons": sum(1 for s in bods if s.get("recordType") == "person"),
        "relationships": sum(1 for s in bods if s.get("recordType") == "relationship"),
        # Carried for the same reason `_safe_deepen` carries it: a source the
        # dispatch loop never saw (sec_edgar, resolved from a CIK) has no other
        # route into `provenances`, and past `deepen_top` this is the only pass
        # that touches it at all. Without this its row was the one with no
        # freshness note, next to rows reading "Checked today".
        "provenance": prov,
    }


async def _safe_deepen(source_id: str, hit_id: str) -> dict[str, Any] | None:
    """Internal helper — does what /deepen does, returns plain dict."""
    adapter = REGISTRY.get(source_id)
    if adapter is None:
        return None

    # Consult the stored OO bundle FIRST. When one exists it is the canonical
    # output, so a live-fetch failure must not sink the deepen — the bundle
    # stands in for the (unavailable) live record.
    override = _bods_data_override(source_id, hit_id)
    try:
        raw, prov = await _fetch_with_provenance(adapter, hit_id)
    except Exception:
        if override is None:
            raise
        raw, prov = {"is_stub": True}, _provenance.STUB_PROVENANCE

    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    if override is not None:
        bods = unique_statements(override)
        issues = validate_shape(bods)
        prov = _stored_bundle_provenance(source_id, hit_id)
    else:
        mapper = _mapper_for(source_id)
        if mapper and not raw.get("is_stub"):
            with _provenance.mapping_provenance(prov):
                bundle: BODSBundle = mapper(raw)
            # One statement per statementId (Phase 235).
            bods = unique_statements(bundle)
            issues = validate_shape(bods)

    bods = classify_government_entities(bods, source_id=source_id)  # Phase 240, as in _deepen
    license_notice = _license_notice_for(adapter.info, raw)
    signals = [s.to_dict() for s in assess_bundle(source_id, raw, bods, hit_id=hit_id)]
    return {
        "raw": raw,
        "bods": bods,
        "bods_issues": issues,
        "license_notice": license_notice,
        "risk_signals": signals,
        # Sources dispatched outside the `_run` loop — sec_edgar, resolved
        # from a CIK rather than from `_dispatch` — never reach the loop that
        # fills `provenances`, so their row is the one with no freshness note.
        # The deepen is a real fetch of the source; carrying its provenance
        # back is the only place that fact exists.
        "provenance": prov,
    }


def _license_notice_for(
    info: SourceInfo, raw: dict[str, Any]
) -> str | None:
    """Return a human-readable warning when the payload is NC-licensed."""
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
