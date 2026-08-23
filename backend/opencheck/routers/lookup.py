"""Lookup endpoints — /lookup, /lookup-stream, /deepen, /report."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from typing import Any, Literal, AsyncIterator, NamedTuple

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from .. import __version__
from .. import bods as _bods
from .. import identifiers
from .. import degradation as _degradation
from .. import provenance as _provenance
from .. import signalstats
from ..provenance import Provenance
from ..bods import BODSBundle, validate_shape
from ..sources.base import LookupDeriver, raw_redaction_notice
from .. import bods_data
from ..cross_check import assess_cross_source_names
from ..findings import (
    finding_bods_gleif,
    finding_companies_house,
    finding_gleif,
    finding_openaleph,
    finding_opencorporates,
    finding_ted_eu,
    finding_wikidata,
)
from ..ftm import subject_to_ftm_entity
from ..icij_check import assess_icij_names
from ..openaleph_check import assess_openaleph_names
from ..verdict import build_verdict
from ..meip import meip_lookup
from ..reconcile import possibly_same_entities, reconcile
from ..risk import DegradedSource, RiskSignal, assess_bundle, assess_hits
from ..ratelimit import default_tier, limiter, lookup_tier
from ..sources import REGISTRY, SearchKind, SourceHit, SourceInfo
from ..sources.schemas import SourceSchemaError

router = APIRouter()

_LOG = logging.getLogger(__name__)


def _fmt_source_error(exc: Exception) -> str:
    """Format a source fetch exception for the errors dict and SSE events."""
    if isinstance(exc, SourceSchemaError):
        return f"Source API changed — {exc}"
    return f"{type(exc).__name__}: {exc}"


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
    #: OECD-UNSD MEIP signpost match for the subject LEI, or None. Not BODS.
    meip: dict[str, Any] | None = None
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
        bods = override
        issues = validate_shape(bods)
        prov = _stored_bundle_provenance(source, hit_id)
    else:
        mapper = _mapper_for(source)
        if mapper and not raw.get("is_stub"):
            with _provenance.mapping_provenance(prov):
                bundle: BODSBundle = mapper(raw)
            bods = list(bundle)
            issues = validate_shape(bods)

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

    # Seeded with whatever the source adapters recorded during the fetches
    # above — a source that could not answer from its own data says so there,
    # not only in the server log. The derived screens append to the same list.
    degraded: list[DegradedSource] = _degradation.collect()
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

LookupEvent = tuple[str, Any]


@dataclass
class _LookupCtx:
    """Mutable context threaded through one lookup run."""

    lei: str
    legal_name: str = ""
    jurisdiction: str = ""
    registered_as: str = ""
    derived: dict[str, str] = dc_field(default_factory=dict)
    ocid: str | None = None
    #: GLEIF-published S&P Global / Capital IQ id — corroborates MEIP's CapIQ id.
    spglobal: str | None = None
    qid: str | None = None
    #: Where the GLEIF anchor payload actually came from. The anchor is
    #: resolved *before* the dispatch loop that fills ``provenances``, so
    #: without carrying it here GLEIF is the one source with no entry in
    #: ``source_liveness`` — and the one row on the report with no freshness
    #: note, which reads as "we don't know" for the source everything else is
    #: anchored to.
    provenance: Provenance | None = None


# RA-code derivers declared by the adapters themselves, collected from the
# registry. GB is special-cased on jurisdiction in _build_derived() because
# UK records reliably carry registeredAs. Normalisers may raise ValueError
# for malformed local IDs — the source is then skipped.
_RA_DERIVERS: list[LookupDeriver] = [
    deriver
    for adapter in REGISTRY.values()
    for deriver in adapter.lookup_derivers
]
# NOTE: ACRA Singapore (RA000523) adapter is implemented but not wired into
# lookup dispatch. The data.gov.sg dataset is bulk CSV only (no live API),
# which doesn't fit the fast-API pattern used by the other national registers.
# To enable: declare lookup_derivers on AcraSingaporeAdapter, add a
# _bh_acra_singapore() builder, and build the DB with scripts/extract_acra.py.


def _build_derived(ctx: _LookupCtx, registered_at_id: str) -> None:
    """Populate ctx.derived from the GLEIF anchor record."""
    ctx.derived["lei"] = ctx.lei
    if ctx.jurisdiction.upper() == "GB" and ctx.registered_as:
        ctx.derived["gb_coh"] = ctx.registered_as
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


def _hit(
    source_id: str,
    hit_id: str,
    *,
    name: str,
    summary: str,
    identifiers: dict[str, str],
    raw: dict[str, Any],
    is_stub: bool = False,
    finding: str | None = None,
) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=SearchKind.ENTITY,
        name=name,
        summary=summary,
        finding=finding,
        identifiers=identifiers,
        raw=raw,
        is_stub=is_stub,
    )


# --- per-source hit builders (dict-result registry adapters) ---------------
# Each takes (result, local_id, ctx) and returns a SourceHit. They are only
# called for non-stub dict results; stub/None results yield no hit.


def _bh_companies_house(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    p = r.get("profile") or {}
    # wikidata_qid is intentionally omitted: the QID is sourced exclusively
    # from Wikidata; Companies House does not publish Wikidata mappings, so
    # including it would falsely imply CH corroborates the identifier.
    return _hit(
        "companies_house", local_id,
        name=p.get("company_name", ctx.legal_name or ""),
        summary=f"GB-COH {local_id}",
        # The finding reads the whole bundle (PSCs, PSC statements, officers),
        # not just the profile that becomes ``raw``.
        finding=finding_companies_house(r),
        identifiers={"gb_coh": local_id}, raw=p,
    )


def _bh_zefix(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "zefix", local_id,
        name=c.get("name") or ctx.legal_name or "",
        summary=f"CHE {local_id}",
        identifiers={"che_uid": local_id}, raw=c,
    )


def _bh_kvk(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    raw = dict(r.get("company") or {})
    note = r.get("coverage_note")
    if note and "coverage_note" not in raw:
        # Not in the KvK open-data set (BV/NV-only 404) — pass the note through
        # so the card explains the gap instead of looking empty/broken.
        raw["coverage_note"] = note
    return _hit(
        "kvk", local_id,
        name=ctx.legal_name or "",
        summary=f"KvK {local_id}",
        identifiers={"kvk_number": local_id}, raw=raw,
    )


def _bh_inpi(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    name = (
        (((c.get("identite") or {}).get("entreprise") or {}).get("denomination"))
        or ctx.legal_name or ""
    )
    return _hit(
        "inpi", local_id,
        name=name, summary=f"FR-SIREN {local_id}",
        identifiers={"siren": local_id}, raw=c,
    )


def _bh_bolagsverket(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    display = (
        f"{local_id[:6]}-{local_id[6:]}" if len(local_id) == 10 else local_id
    )
    return _hit(
        "bolagsverket", local_id,
        name=c.get("namn") or c.get("name") or ctx.legal_name or "",
        summary=f"SE-BLV {display}",
        identifiers={"se_org_number": local_id}, raw=c,
    )


def _bh_ariregister(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "ariregister", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"EE-ARIREGISTER {local_id}",
        identifiers={"ee_registry_code": local_id}, raw=r,
    )


def _bh_brreg(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    e = r.get("entity") or {}
    return _hit(
        "brreg", local_id,
        name=e.get("navn") or ctx.legal_name or "",
        summary=f"NO-ORGNR {local_id}",
        identifiers={"no_orgnr": local_id}, raw=e,
    )


def _bh_cro(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "cro", local_id,
        name=(c.get("company_name") or "").strip() or ctx.legal_name or "",
        summary=f"IE-CRN {local_id}",
        identifiers={"ie_crn": local_id}, raw=c,
    )


def _bh_cnpj_brazil(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "cnpj_brazil", local_id,
        name=(c.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"BR-CNPJ {local_id}",
        identifiers={"br_cnpj": local_id}, raw=c,
    )


def _bh_nz_companies(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = dict(r.get("company") or {})
    identifiers = {"nz_company_number": local_id}
    if r.get("nzbn"):
        identifiers["nzbn"] = str(r["nzbn"])
    if r.get("link") and "link" not in c:
        # Surface the public NZBN entity page so the source card links out.
        c["link"] = r["link"]
    return _hit(
        "nz_companies", local_id,
        name=(c.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"NZ-COH {local_id}",
        identifiers=identifiers, raw=c,
    )


def _bh_malta_mbr(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "malta_mbr", local_id,
        name=(c.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"MT-MBR {local_id}",
        identifiers={"mt_crn": local_id}, raw=c,
    )


def _bh_prh(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    name = ""
    for n in (c.get("names") or []):
        if not n.get("endDate") and n.get("order") == 0:
            name = (n.get("name") or "").strip()
            break
    return _hit(
        "prh", local_id,
        name=name or ctx.legal_name or "",
        summary=f"FI-YTUNNUS {local_id}",
        identifiers={"fi_ytunnus": local_id}, raw=c,
    )


def _bh_ur_latvia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    e = r.get("entity") or {}
    return _hit(
        "ur_latvia", local_id,
        name=(e.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"LV-UR {local_id}",
        identifiers={"lv_regcode": local_id}, raw=e,
    )


def _bh_jar_lithuania(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "jar_lithuania", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"LT-JAR {local_id}",
        identifiers={"lt_code": local_id}, raw=r,
    )


def _bh_ares(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    e = r.get("entity") or {}
    return _hit(
        "ares", local_id,
        name=(e.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"CZ-ARES IČO {local_id}",
        identifiers={"cz_ico": local_id}, raw=e,
    )


def _bh_krs_poland(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "krs_poland", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"KRS {local_id}",
        identifiers={"pl_krs": local_id}, raw=r,
    )


def _bh_firmenbuch(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "firmenbuch", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"FN {local_id}",
        identifiers={"at_fn": local_id}, raw=r,
    )


def _bh_rpo_slovakia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "rpo_slovakia", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"SK-IČO {local_id}",
        identifiers={"sk_ico": local_id}, raw=r,
    )


def _bh_rpvs_slovakia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "rpvs_slovakia", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"SK-IČO {local_id} · RPVS #{r.get('partner_id', '')}",
        identifiers={
            "sk_ico": local_id,
            **({"rpvs_id": str(r["partner_id"])} if r.get("partner_id") else {}),
        },
        raw=r,
    )


def _bh_bce_belgium(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "bce_belgium", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"BE {r.get('dotted') or local_id}",
        identifiers={"be_enterprise_number": local_id}, raw=r,
    )


def _bh_corporations_canada(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    corp = r.get("corporation") or {}
    name = ""
    for entry in (corp.get("corporationNames") or []):
        cn = entry.get("CorporationName") or {}
        if cn.get("current"):
            name = (cn.get("name") or "").strip()
            if (cn.get("nameType") or "").lower() == "primary":
                break
    return _hit(
        "corporations_canada", local_id,
        name=name or ctx.legal_name or "",
        summary=f"CA-CORP {local_id}",
        identifiers={"ca_corp_id": local_id}, raw=corp,
    )


def _bh_cvr_denmark(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "cvr_denmark", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"DK-CVR {local_id}",
        identifiers={"dk_cvr": local_id}, raw=r,
    )


def _bh_sudreg_croatia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    subject = r.get("subject") or {}
    return _hit(
        "sudreg_croatia", local_id,
        name=(subject.get("tvrtka") or {}).get("ime") or ctx.legal_name or "",
        summary=f"HR-MBS {local_id}",
        identifiers={
            "hr_mbs": local_id,
            **({"hr_oib": r["oib"]} if r.get("oib") else {}),
        },
        raw=subject,
    )


def _bh_abr_australia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "abr_australia", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"AU-ABN {r.get('abn') or local_id}".strip(),
        identifiers={
            **({"au_abn": r["abn"]} if r.get("abn") else {}),
            **({"au_acn": r["acn"]} if r.get("acn") else {}),
        },
        raw=r,
    )


def _bh_mca_india(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "mca_india", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"IN-CIN {r.get('cin') or local_id}".strip(),
        identifiers={"in_cin": r.get("cin") or local_id},
        raw=r,
    )


@dataclass(frozen=True)
class _RegistrySource:
    """Dispatch + hit-build spec for a derived-identifier registry adapter."""

    source_id: str
    derived_keys: tuple[str, ...]  # first present key wins (ABR: ACN over ABN)
    pass_legal_name: bool
    build: Any  # Callable[[dict, str, _LookupCtx], SourceHit]


def _collect_registry_sources() -> list[_RegistrySource]:
    """Build dispatch specs from the adapters' own lookup declarations.

    Any adapter that declares lookup keys (via ``lookup_derivers`` or
    ``lookup_dispatch_keys``) MUST have a matching ``_bh_<id>()`` hit
    builder in this module — enforced here at import time so a missing
    builder fails the whole test suite, not one lookup at runtime.
    """
    specs: list[_RegistrySource] = []
    for source_id, adapter in REGISTRY.items():
        keys = adapter.lookup_keys()
        if not keys:
            continue
        builder = globals().get(f"_bh_{source_id}")
        if builder is None:
            raise RuntimeError(
                f"adapter {source_id!r} declares lookup keys {keys} but "
                f"routers/lookup.py has no _bh_{source_id}() hit builder"
            )
        specs.append(
            _RegistrySource(source_id, keys, adapter.lookup_pass_legal_name, builder)
        )
    return specs


_REGISTRY_SOURCES: list[_RegistrySource] = _collect_registry_sources()

_REGISTRY_SOURCE_INDEX: dict[str, _RegistrySource] = {
    s.source_id: s for s in _REGISTRY_SOURCES
}

# Official company registers emit officers / PSCs / beneficial owners into the
# entity bundle; OpenCorporates likewise contributes officer person statements.
# These "person-capable" sources are always deepened, even past the deepen_top
# cap — otherwise the connected-people list depends on a nondeterministic
# completion-order race (issue #73). The leak/sanctions list-search sources
# (OpenSanctions, OpenAleph, EveryPolitician) are deliberately NOT here: they
# have no registry hit builder, can be numerous/slow, and are not authoritative
# registers.
_PERSON_CAPABLE_SOURCES: frozenset[str] = frozenset(_REGISTRY_SOURCE_INDEX) | {
    "opencorporates",
    # Wikidata is not a company register but carries person identity data
    # (Q-ids, the identifier spine for people) and contributes person records
    # to the bundle, so it is kept in the always-deepen set too.
    "wikidata",
}


def _local_id_for(spec: _RegistrySource, derived: dict[str, str]) -> str | None:
    for key in spec.derived_keys:
        if key in derived:
            return derived[key]
    return None


# --- special hit builders ---------------------------------------------------


def _bh_opencorporates(r: dict, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "opencorporates", ctx.ocid or "",
        name=c.get("name") or ctx.legal_name or "",
        summary=f"OC {ctx.ocid} · {c.get('current_status', '')}",
        finding=finding_opencorporates(r),
        identifiers={
            "ocid": ctx.ocid or "",
            "lei": ctx.lei,
            **({"gb_coh": ctx.derived["gb_coh"]} if "gb_coh" in ctx.derived else {}),
        },
        raw=c,
    )


def _extract_edgar_cik(oc_company: dict[str, Any]) -> str | None:
    """Pull a SEC EDGAR CIK out of an OpenCorporates company payload."""
    data = oc_company.get("data") or {}
    for entry in (data.get("most_recent") or []):
        datum = (entry.get("datum") or {}) if isinstance(entry, dict) else {}
        if datum.get("title") == "SEC Edgar entry" and datum.get("description"):
            desc: str = datum["description"]
            if "register id:" in desc:
                raw_cik = desc.split("register id:")[-1].strip()
                if raw_cik.isdigit():
                    return raw_cik.lstrip("0") or "0"
            break
    return None


def _bh_wikidata(r: dict, ctx: _LookupCtx) -> SourceHit:
    s = r.get("summary") or {}
    return _hit(
        "wikidata", ctx.qid or "",
        name=s.get("label") or ctx.qid or "",
        summary=s.get("description") or "",
        finding=finding_wikidata(s),
        identifiers={
            "wikidata_qid": ctx.qid or "",
            "lei": ctx.lei,
            **({"gb_coh": ctx.registered_as} if "gb_coh" in ctx.derived else {}),
        },
        raw=s,
    )


def _bh_climatetrace(r: dict, ctx: _LookupCtx) -> SourceHit:
    entity_id = r.get("entity_id") or ctx.lei
    emissions = r.get("emissions") or {}
    total_co2e = emissions.get("total_co2e_tonnes")
    parts = [f"GEM entity {entity_id}"]
    if total_co2e is not None and total_co2e > 0:
        if total_co2e >= 1_000_000:
            parts.append(f"{total_co2e / 1_000_000:.1f} Mt CO₂e (2024)")
        else:
            parts.append(f"{total_co2e:,.0f} t CO₂e (2024)")
    return _hit(
        "climatetrace", entity_id,
        name=r.get("entity_name") or ctx.legal_name or entity_id,
        summary=" · ".join(parts),
        identifiers={"gem_entity_id": entity_id},
        raw=r, is_stub=bool(r.get("is_stub")),
    )


def _bh_bods_gleif(r: dict, ctx: _LookupCtx) -> SourceHit:
    statement_id = r.get("hit_id") or ctx.lei
    name = ctx.legal_name or ctx.lei
    for stmt in r.get("bods_statements", []):
        if stmt.get("statementType") == "entityStatement":
            name = stmt.get("recordDetails", {}).get("name") or name
            break
    return _hit(
        "bods_gleif", statement_id,
        name=name,
        summary="Open Ownership BODS v0.4 (bulk) · LEI match",
        finding=finding_bods_gleif(r, statement_id),
        identifiers={"lei": ctx.lei, "bods_gleif_statementid": statement_id},
        raw=r,
    )


# Countries where the EITI identification format has been verified to match
# an OpenCheck derived-identifier key — lets the reconciler show legitimate
# cross-source corroboration (EITI independently publishes these numbers).
_EITI_IDENTIFIER_KEY_BY_COUNTRY = {
    "GB": "gb_coh",
    "NO": "no_orgnr",
    "NL": "kvk_number",
    # US EITI identifications are federal EINs; matching a US subject means an
    # EIN was derived for it, so the corroboration key is that EIN.
    "US": "us_ein",
}


def _bh_eiti(r: dict, ctx: _LookupCtx) -> SourceHit:
    country = r.get("country") or ""
    ident = r.get("identification") or ""
    years = r.get("years") or []
    total_usd = r.get("total_usd") or 0.0
    parts = [f"EITI {country}"]
    if years:
        parts.append(
            f"{len(years)} reporting year{'s' if len(years) != 1 else ''} "
            f"({years[-1]}–{years[0]})" if len(years) > 1 else f"reported {years[0]}"
        )
    if total_usd > 0:
        if total_usd >= 1_000_000:
            parts.append(f"${total_usd / 1_000_000:.1f}M USD to governments")
        else:
            parts.append(f"${total_usd:,.0f} USD to governments")
    ident_key = _EITI_IDENTIFIER_KEY_BY_COUNTRY.get(country, "eiti_identification")
    return _hit(
        "eiti", f"{country}:{ident}",
        name=r.get("entity_name") or ctx.legal_name or ident,
        summary=" · ".join(parts),
        identifiers={ident_key: ident},
        raw=r,
    )


def _bh_eiti_soe(r: dict, ctx: _LookupCtx) -> SourceHit:
    parts = ["State-owned enterprise"]
    if r.get("sector"):
        parts.append(str(r["sector"]))
    commodities = r.get("commodities") or []
    if commodities:
        parts.append(", ".join(commodities[:3]))
    if r.get("country"):
        parts.append(str(r["country"]))
    if (r.get("match_confidence") or "").lower() == "low":
        parts.append("possible name match")
    # Corroboration rule: the SOE database does NOT publish the LEI (OpenCheck
    # derives it at index-build time), so `lei` is intentionally omitted from
    # identifiers. Only the identifiers EITI itself publishes are asserted.
    # `eiti_soe_id` is informational (EITI's own id). `ocid` is intentionally
    # NOT asserted from the SOE database's opencorporates_id: like the Wikirate
    # precedent, its format may differ from OpenCheck's jurisdiction-scoped
    # `ocid`, and a mismatched assert would create a false corroboration.
    identifiers: dict[str, str] = {}
    if r.get("eiti_id_company"):
        identifiers["eiti_soe_id"] = str(r["eiti_id_company"])
    return _hit(
        "eiti_soe", ctx.lei,
        name=r.get("entity_name") or ctx.legal_name or ctx.lei,
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )


def _bh_cac_nigeria(r: dict, ctx: _LookupCtx) -> SourceHit:
    record = r.get("record") or {}
    pscs = record.get("pscs") or []
    n = len(pscs)
    # Count of PSC declaration rows in the register, not distinct owners: the
    # declared parties may be people or companies, and may be listed by virtue
    # of control rather than ownership, so "filings" is the accurate framing
    # (matches the CAC's own `numberOfPsc` field). The BODS diagram may show
    # fewer nodes because map_cac_nigeria dedupes owners by canonical name.
    parts = [f"{n} PSC filing{'s' if n != 1 else ''}"]
    parts.append("Nigeria CAC public register")
    # Corroboration rule: the CAC BOR publishes the RC number, NOT the LEI
    # (OpenCheck derives the LEI via GLEIF at build time). Assert only the RC —
    # the identifier the register itself publishes — never `lei`.
    identifiers: dict[str, str] = {}
    rc = (r.get("identifiers") or {}).get("ng_cac_rc") or record.get("rc")
    if rc:
        identifiers["ng_cac_rc"] = str(rc)
    return _hit(
        "cac_nigeria", ctx.lei,
        name=record.get("company") or ctx.legal_name or ctx.lei,
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )


def _bh_eiti_bo(r: dict, ctx: _LookupCtx) -> SourceHit:
    record = r.get("record") or {}
    register_id = str(record.get("register_id") or "")
    parts: list[str] = []
    if register_id == "drc_itie":
        owners = (record.get("drc") or {}).get("owners") or []
        peps = sum(1 for o in owners if o.get("pep"))
        parts.append(f"{len(owners)} beneficial owner{'s' if len(owners) != 1 else ''}")
        if peps:
            parts.append(f"{peps} PEP{'s' if peps != 1 else ''}")
        parts.append("ITIE-RDC register")
    elif register_id == "armenia_eregister":
        arm = record.get("armenia") or {}
        n = len(arm.get("bods_v02") or [])
        parts.append(f"BODS v0.2 declaration, {n} statements")
        parts.append("Armenia State Register")
    elif register_id == "nigeria_cac":
        pscs = (record.get("nigeria") or {}).get("pscs") or []
        parts.append(f"{len(pscs)} PSC filing{'s' if len(pscs) != 1 else ''}")
        parts.append("Nigeria CAC (NEITI solid-minerals subset)")
    if record.get("source_date"):
        parts.append(f"register data {str(record['source_date'])[:10]}")
    if (record.get("match") or {}).get("confidence") == "medium":
        parts.append("possible name match")
    # Corroboration rule: no pooled register publishes the LEI (OpenCheck
    # derives it at index-build time), so `lei` is intentionally omitted —
    # only the identifiers the register itself publishes are asserted.
    identifiers = {k: str(v) for k, v in (r.get("identifiers") or {}).items() if v}
    return _hit(
        "eiti_bo", ctx.lei,
        name=record.get("company_latin")
        or record.get("company")
        or ctx.legal_name
        or ctx.lei,
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )


def _bh_ted_eu(r: dict, ctx: _LookupCtx) -> SourceHit:
    total = int(r.get("total_notice_count") or 0)
    wins = int(r.get("confirmed_wins") or 0)
    notices = r.get("notices") or []
    parts = [f"{total} EU award notice{'s' if total != 1 else ''}"]
    if wins:
        parts.append(f"{wins} confirmed win{'s' if wins != 1 else ''}")
    latest = next(
        (n.get("publication_date") for n in notices if n.get("publication_date")),
        "",
    )
    if latest:
        parts.append(f"latest {latest}")
    parts.append("eForms era (≈2024+) only")
    # Corroboration rule: TED publishes whatever identifier the buyer entered
    # (eForms BT-501) — usually a national registration number whose scheme is
    # not machine-readable, so national ids are NOT asserted back. The LEI is
    # asserted only when a matched notice actually carried the LEI string
    # (fill rate is zero as of 2026-08, so today this never fires — it exists
    # for when LEI adoption in eForms materialises).
    identifiers: dict[str, str] = {}
    matched = [str(v) for v in (r.get("matched_company_ids") or [])]
    if ctx.lei and any(v.strip().upper() == ctx.lei.upper() for v in matched):
        identifiers["lei"] = ctx.lei
    hit_id = "|".join(r.get("identifiers_queried") or []) or ctx.lei
    return _hit(
        "ted_eu", hit_id,
        name=r.get("legal_name") or ctx.legal_name or ctx.lei,
        summary=" · ".join(parts),
        finding=finding_ted_eu(r),
        identifiers=identifiers,
        raw=r,
    )


# Wikirate Company-card identifier fields → OpenCheck identifier keys, for
# reconciler corroboration. Wikirate independently publishes these on the
# card, so asserting them is legitimate under the corroboration rule.
# ``open_corporates_id`` is deliberately excluded: it is the bare OC company
# number, not OpenCheck's jurisdiction-scoped ``ocid``.
_WIKIRATE_IDENTIFIER_KEYS = {
    "legal_entity_identifier": "lei",
    "wikidata_id": "wikidata_qid",
    "uk_company_number": "gb_coh",
    "sec_central_index_key": "edgar_cik",
}


def _bh_wikirate(r: dict, ctx: _LookupCtx) -> SourceHit:
    card_id = r.get("card_id")
    total = r.get("total_answers") or 0
    parts = ["Wikirate ESG metrics"]
    if total > 0:
        parts.append(f"{total:,} data point{'s' if total != 1 else ''}")
    years = [a.get("year") for a in r.get("latest_answers") or [] if a.get("year")]
    if years:
        parts.append(f"latest {max(years)}")
    identifiers: dict[str, str] = {}
    for field, key in _WIKIRATE_IDENTIFIER_KEYS.items():
        value = (r.get("identifiers") or {}).get(field)
        if isinstance(value, list):
            value = value[0] if value else None
        if value:
            identifiers[key] = str(value)
    return _hit(
        "wikirate", str(card_id),
        name=r.get("name") or ctx.legal_name or str(card_id),
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )


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
    # as identifier corroboration. Only the top hits are enriched to stay
    # inside the adapter's lookup time budget; failures degrade silently.
    if hasattr(oa_adapter, "fetch_mentions"):
        for h in deduped[:2]:
            try:
                mentions = await oa_adapter.fetch_mentions(h.hit_id)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                continue
            if mentions and mentions.get("total"):
                h.raw["openaleph_mentions"] = mentions
                total = mentions["total"]
                h.summary = (
                    f"{h.summary} · mentioned in {total} "
                    f"document{'s' if total != 1 else ''}"
                )
                # The adapter built the finding without mentions (it fetches
                # them here, after the hit exists) — rebuild so the sentence
                # leads with the document count.
                h.finding = finding_openaleph(h.raw, mentions)
    return deduped


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
    if soe_adapter is not None and hasattr(soe_adapter, "fetch_by_lei") and _want("eiti_soe"):
        tasks.append(("eiti_soe", soe_adapter.fetch_by_lei(ctx.lei)))
    # Nigeria CAC — LEI-keyed offline match against the committed PSC index
    # (curated example set; the CAC's official API is government-only). A hit
    # means the LEI is in the curated set; its BODS carries the CAC-published
    # beneficial ownership.
    cac_adapter = REGISTRY.get("cac_nigeria")
    if cac_adapter is not None and hasattr(cac_adapter, "fetch_by_lei") and _want("cac_nigeria"):
        tasks.append(("cac_nigeria", cac_adapter.fetch_by_lei(ctx.lei)))
    # Pooled EITI national BO registers — LEI-keyed offline match against the
    # committed pooled index (DRC ITIE-RDC / Armenia State Register / Nigeria
    # CAC∩NEITI). A hit means the LEI is an extractive company with register-
    # published beneficial ownership; its BODS carries that ownership.
    eiti_bo_adapter = REGISTRY.get("eiti_bo")
    if eiti_bo_adapter is not None and hasattr(eiti_bo_adapter, "fetch_by_lei") and _want("eiti_bo"):
        tasks.append(("eiti_bo", eiti_bo_adapter.fetch_by_lei(ctx.lei)))
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
_STRUCTURAL_SIGNAL_CODES = {
    "COMPLEX_OWNERSHIP_LAYERS",
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
                    sig.get("evidence", {}).get("subject_statement_id", ""),
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
            # A committed Open Ownership extract, not a call to GLEIF. Saying
            # "curated" is the same claim the stored-bundle hits make below.
            ctx.provenance = Provenance(liveness="curated")
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
    except _LookupAbort:
        raise
    except Exception as exc:  # noqa: BLE001
        raise _LookupAbort(
            502, f"GLEIF fetch failed: {type(exc).__name__}: {exc}"
        ) from exc

    _build_derived(ctx, registered_at_id)

    # OpenCorporates ID from the GLEIF Level-1 record.
    if gleif.info.live_available:
        try:
            gleif_src = (
                gleif_bundle
                if not gleif_bundle.get("_from_bundle")
                else await gleif.fetch(lei)
            )
            if not gleif_src.get("is_stub"):
                attrs = (gleif_src.get("record") or {}).get("attributes") or {}
                ctx.ocid = attrs.get("ocid") or None
                sp = attrs.get("spglobal")
                ctx.spglobal = (sp[0] if isinstance(sp, list) and sp else sp) or None
        except Exception as exc:  # noqa: BLE001
            # Non-fatal, but not free: without ocid the OpenCorporates
            # dispatch is skipped and without spglobal the MEIP CapIQ
            # corroboration silently downgrades. Log so a GLEIF outage
            # doesn't read as "this entity has no OpenCorporates record".
            _LOG.warning(
                "GLEIF identifier extraction failed for %s: %s: %s — "
                "OpenCorporates dispatch and MEIP CapIQ corroboration "
                "will be skipped for this lookup.",
                lei,
                type(exc).__name__,
                exc,
            )
    if ctx.ocid:
        ctx.derived["ocid"] = ctx.ocid

    wikidata_adapter = REGISTRY["wikidata"]
    if hasattr(wikidata_adapter, "find_qid_by_lei"):
        ctx.qid = await wikidata_adapter.find_qid_by_lei(lei)  # type: ignore[attr-defined]
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

    # OECD-UNSD MEIP signpost — fires when the subject LEI is in the MEIP Global
    # Register (a subsidiary of, or one of, the 500 largest MNEs). Not mapped to
    # BODS; corroborated against GLEIF's own OpenCorporates / S&P Capital IQ ids.
    meip_match = meip_lookup(
        lei, {"opencorporates": ctx.ocid or "", "capiq": ctx.spglobal or ""}
    )
    yield ("meip", {"match": meip_match.model_dump() if meip_match else None})

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
    applicable_ids = [sid for sid, _ in dispatch] + (
        ["sec_edgar"] if sec_applicable else []
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
                    provenances[source_id] = _stored_bundle_provenance(source_id)
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
            yield ("deepen_error", {
                "source_id": dsrc,
                "error": f"{type(deep).__name__}: {deep}",
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
        yield ("deepen_result", {
            "source_id": dsrc, "hit_id": dhit, "bods": deep["bods"],
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
            bods_counts[key] = cnt["total"]
            bods_breakdown[key] = {
                "entities": cnt["entities"],
                "persons": cnt.get("persons", 0),
                "relationships": cnt["relationships"],
            }

    yield ("bods_counts", {"counts": bods_counts, "breakdown": bods_breakdown})

    yield (
        "possibly_same_entities",
        {"pairs": [p.to_dict() for p in possibly_same_entities(bods_all)]},
    )

    # Seeded with whatever the source adapters recorded during the fetches
    # above — a source that could not answer from its own data says so there,
    # not only in the server log. The derived screens append to the same list.
    degraded: list[DegradedSource] = _degradation.collect()
    oa_screening: list[dict[str, Any]] = []
    cross_raw, icij_raw, oa_raw = await asyncio.gather(
        assess_cross_source_names(bods_all, degraded=degraded),
        assess_icij_names(bods_all, degraded=degraded),
        assess_openaleph_names(bods_all, degraded=degraded, screening=oa_screening),
    )
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


# --- replay cache --------------------------------------------------------------
#
# Completed lookup runs are kept in memory for a short window so a page
# refresh, a shared URL, or an SSE reconnect replays instantly instead of
# re-querying every source. Only runs that reached the "done" event are
# cached; per-source retries and ?refresh=true invalidate/bypass.
#
# Replays are never allowed to masquerade as live runs: a replayed stream is
# prefixed with a "replayed" event carrying the wall-clock completion time of
# the original run, and the sync /lookup response mirrors it as
# ``replayed`` / ``fetched_at`` so the UI can badge the result and offer a
# fresh check.


class _ReplayEntry(NamedTuple):
    stored: float  # monotonic clock, for the TTL check
    fetched_at: str  # wall-clock UTC ISO 8601 completion time, for display
    events: list[LookupEvent]


_REPLAY_TTL_SECONDS = 15 * 60.0
_REPLAY_MAX_ENTRIES = 64
_REPLAY_CACHE: dict[str, _ReplayEntry] = {}


def _invalidate_replay(lei: str) -> None:
    prefix = f"{lei.strip().upper()}:"
    for key in [k for k in _REPLAY_CACHE if k.startswith(prefix)]:
        _REPLAY_CACHE.pop(key, None)


async def _lookup_pipeline_cached(
    lei: str, deepen_top: int = 5, refresh: bool = False
) -> AsyncIterator[LookupEvent]:
    """Replay a cached completed run, or run the pipeline and cache it."""
    key = f"{lei.strip().upper()}:{deepen_top}"
    now = time.monotonic()

    if not refresh:
        entry = _REPLAY_CACHE.get(key)
        if entry is not None and now - entry.stored < _REPLAY_TTL_SECONDS:
            # Provenance first, so the UI knows before any result arrives.
            yield (
                "replayed",
                {
                    "fetched_at": entry.fetched_at,
                    "age_seconds": round(now - entry.stored, 1),
                },
            )
            for event in entry.events:
                yield event
            return

    buffer: list[LookupEvent] = []
    completed = False
    async for event in _lookup_pipeline(lei, deepen_top=deepen_top):
        buffer.append(event)
        if event[0] == "done":
            completed = True
        yield event

    if completed:
        while len(_REPLAY_CACHE) >= _REPLAY_MAX_ENTRIES:
            _REPLAY_CACHE.pop(next(iter(_REPLAY_CACHE)), None)
        _REPLAY_CACHE[key] = _ReplayEntry(
            stored=now,
            fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            events=buffer,
        )


# --- endpoints ---------------------------------------------------------------


async def _lookup_impl(
    lei: str, deepen_top: int = 5, refresh: bool = False
) -> LookupResponse:
    """Body of ``/lookup``, callable in-process (MCP tools, /narrative,
    /export, layer expansion) without going through the rate-limited route."""
    norm_lei = lei.strip().upper()
    hits: list[SourceHit] = []
    errors: dict[str, str] = {}
    links: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    degraded_sources: list[dict[str, Any]] = []
    source_liveness: dict[str, dict[str, Any]] = {}
    graph_shape: dict[str, Any] = {}
    verdict: str | None = None
    oa_screening: list[dict[str, Any]] = []
    bods_all: list[dict[str, Any]] = []
    same_pairs: list[dict[str, Any]] = []
    meip_match: dict[str, Any] | None = None
    bods_issues: list[str] = []
    license_notices: list[dict[str, str]] = []
    legal_name: str | None = None
    jurisdiction: str | None = None
    derived: dict[str, str] = {}
    replayed = False
    fetched_at: str | None = None

    async for event, payload in _lookup_pipeline_cached(
        norm_lei, deepen_top=deepen_top, refresh=refresh
    ):
        if event == "replayed":
            replayed = True
            fetched_at = payload["fetched_at"]
        elif event == "error":
            raise HTTPException(
                status_code=payload["status"], detail=payload["detail"]
            )
        elif event == "gleif_done":
            legal_name = payload["legal_name"]
            jurisdiction = payload["jurisdiction"]
            derived = payload["derived_identifiers"]
        elif event == "hit":
            hits.append(payload)
        elif event == "source_error":
            errors[payload["source_id"]] = payload["error"]
        elif event == "deepen_error":
            errors.setdefault(payload["source_id"], payload["error"])
        elif event == "deepen_result":
            bods_all.extend(payload["bods"])
        elif event == "cross_source_links":
            links = payload["links"]
        elif event == "possibly_same_entities":
            same_pairs = payload["pairs"]
        elif event == "meip":
            meip_match = payload["match"]
        elif event == "risk_signals":
            signals = payload["signals"]
            degraded_sources = payload.get("degraded_sources") or []
            verdict = payload.get("verdict")
            oa_screening = payload.get("openaleph_screening") or []
            source_liveness = payload.get("source_liveness") or {}
            graph_shape = payload.get("graph_shape") or {}
        elif event == "done":
            bods_issues = payload["bods_issues"]
            license_notices = payload["license_notices"]

    return LookupResponse(
        query=norm_lei,
        kind=SearchKind.ENTITY,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=signals,
        bods=bods_all,
        bods_issues=bods_issues,
        license_notices=license_notices,
        possibly_same_entities=same_pairs,
        meip=meip_match,
        degraded_sources=degraded_sources,
        openaleph_screening=oa_screening,
        source_liveness=source_liveness,
        graph_shape=graph_shape,
        verdict=verdict,
        lei=norm_lei,
        legal_name=legal_name,
        jurisdiction=jurisdiction,
        derived_identifiers=derived,
        replayed=replayed,
        fetched_at=fetched_at,
    )


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


def _entity_idents(stmt: dict[str, Any]) -> set[str]:
    """Upper-cased identifier values carried by a BODS entity statement."""
    ids = (stmt.get("recordDetails") or {}).get("identifiers") or []
    return {(i.get("id") or "").strip().upper() for i in ids if (i.get("id") or "").strip()}


def _anchor_replacements(bods: list[dict[str, Any]], lei: str, anchor: str) -> dict[str, str]:
    """The statementId → ``anchor`` rewrites that collapse every representation of
    the LEI-identified entity onto the existing graph node.

    Fix for the spike's cross-source finding: a national register keys its entity
    statement on the company number, not the LEI, so matching on the LEI alone
    left a floating duplicate. We seed the identifier set from every statement
    that asserts the LEI (GLEIF ties the LEI to the company number), then mark any
    entity statement sharing one of those identifier values for rewrite.
    """
    from ..bods.mapper import _stable_id

    norm = lei.strip().upper()
    subj_idents: set[str] = {norm}
    for s in bods:
        if s.get("recordType") == "entity" and norm in _entity_idents(s):
            subj_idents |= _entity_idents(s)

    subject_ids = {_stable_id("gleif", "entity", norm)}
    for s in bods:
        if s.get("recordType") == "entity" and (_entity_idents(s) & subj_idents):
            subject_ids.add(s["statementId"])
    subject_ids.discard(anchor)
    return {sid: anchor for sid in subject_ids}


def _apply_id_remap(items: list[dict[str, Any]], repl: dict[str, str]) -> list[dict[str, Any]]:
    """Rewrite statement ids over a serialised list (BODS *or* risk signals). A
    blunt string replace is safe — opencheck statement ids are unique 24-hex
    tokens with no collision risk — and it catches every reference field uniformly
    (including the ``evidence.statement_id`` fields risk signals carry)."""
    if not repl:
        return items
    raw = json.dumps(items)
    for old, new in repl.items():
        raw = raw.replace(old, new)
    return json.loads(raw)


def _collapse_onto_anchor(bods: list[dict[str, Any]], lei: str, anchor: str) -> list[dict[str, Any]]:
    """Collapse every representation of the LEI-identified entity onto ``anchor``."""
    return _apply_id_remap(bods, _anchor_replacements(bods, lei, anchor))


async def _expand_one_layer(
    lei: str, anchor: str, *, deepen_top: int = 3
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Owner-ward hop: re-anchor a standard ``lookup`` (the entity's owners) and
    stitch it onto ``anchor`` (reusing the replay cache). Returns the new layer's
    BODS **and** the risk signals the sub-lookup already screened for the expanded
    entity — both with ids remapped onto the anchor — so FullCheck accumulates
    network-wide risk as it expands."""
    norm = lei.strip().upper()
    resp = await _lookup_impl(lei=norm, deepen_top=deepen_top)  # raises 400/404
    repl = _anchor_replacements(resp.bods, norm, anchor)
    return _apply_id_remap(resp.bods, repl), _apply_id_remap(resp.risk_signals, repl)


async def _subsidiaries_one_layer(
    lei: str, anchor: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Subsidiary-ward hop: fetch the entity's GLEIF Level-2 children and stitch
    them under ``anchor``. GLEIF L2 children aren't risk-screened, so no signals."""
    from ..subsidiaries import assemble_subsidiaries

    norm = lei.strip().upper()
    data = await assemble_subsidiaries(norm, include_bods=True)
    bods = (data or {}).get("bods") or []
    return _collapse_onto_anchor(bods, norm, anchor), []


@router.get("/expand")
@limiter.limit(default_tier)
async def expand(
    request: Request,
    response: Response,
    lei: str = Query(..., description="LEI of the corporate node to expand."),
    anchor: str = Query(
        ...,
        description=(
            "statementId of the existing graph node being expanded. The "
            "looked-up entity's identity statements are remapped onto it so the "
            "new owners layer stitches onto the existing node, not a duplicate."
        ),
    ),
    deepen_top: int = Query(3, ge=0, le=10),
) -> dict[str, Any]:
    """Progressive discovery: resolve one corporate node a hop deeper.

    Live-only and corporate-hops only (person nodes are terminal and the caller
    never expands them); not part of the main lookup synthesis. The owner-ward
    traversal foundation that FullCheck's network exploration builds on. See
    ``/expand-layer`` for the batch (whole-frontier) variant.
    """
    bods, _signals = await _expand_one_layer(lei, anchor, deepen_top=deepen_top)
    return {"lei": lei.strip().upper(), "anchor": anchor, "bods": bods}


_MAX_LAYER_ITEMS = 25  # cap concurrent hops per "add layer" so it can't fan out the register


class _ExpandItem(BaseModel):
    lei: str
    anchor: str


class ExpandLayerRequest(BaseModel):
    items: list[_ExpandItem]
    # Context-aware direction: an ownership graph digs up (owners); a subsidiary
    # tree digs down (GLEIF Level-2 children). The view tells us which.
    direction: Literal["owners", "subsidiaries"] = "owners"


@router.post("/expand-layer")
@limiter.limit(default_tier)
async def expand_layer(
    request: Request, response: Response, req: ExpandLayerRequest
) -> dict[str, Any]:
    """Progressive discovery (batch): take the whole current frontier and go one
    layer deeper on every node at once, in the graph's existing direction.

    Each item is a ``(lei, anchor)`` pair (the caller selects the frontier).
    ``direction`` picks the hop: ``owners`` re-anchors a standard lookup (up the
    ownership chain); ``subsidiaries`` fetches GLEIF Level-2 children (down the
    subsidiary tree). Hops run concurrently (bounded), each stitched onto its
    anchor, and the results are merged + de-duplicated by ``statementId``. Capped
    at ``_MAX_LAYER_ITEMS`` so a click can't fan out the whole register.
    """
    items = req.items[:_MAX_LAYER_ITEMS]
    sem = asyncio.Semaphore(5)
    hop = _subsidiaries_one_layer if req.direction == "subsidiaries" else _expand_one_layer

    async def _one(item: _ExpandItem) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        async with sem:
            try:
                return await hop(item.lei, item.anchor)
            except Exception:  # noqa: BLE001 — a bad node must not sink the batch
                return [], []

    chunks = await asyncio.gather(*[_one(i) for i in items])

    seen: set[str] = set()
    merged: list[dict[str, Any]] = []
    seen_sig: set[str] = set()
    merged_sig: list[dict[str, Any]] = []
    for bods_chunk, sig_chunk in chunks:
        for s in bods_chunk:
            sid = s.get("statementId")
            if sid and sid not in seen:
                seen.add(sid)
                merged.append(s)
        for sig in sig_chunk:
            key = json.dumps(sig, sort_keys=True, default=str)
            if key not in seen_sig:
                seen_sig.add(key)
                merged_sig.append(sig)

    return {
        "bods": merged,
        "risk_signals": merged_sig,
        "expanded": [i.anchor for i in items],
        "count": len(items),
        "truncated": len(req.items) > _MAX_LAYER_ITEMS,
    }


@router.get("/lookup-stream")
@limiter.limit(lookup_tier)
async def lookup_stream(
    request: Request,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
) -> EventSourceResponse:
    """LEI-anchored lookup streamed as SSE — same pipeline as /lookup."""
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


# ISO 3166-1 alpha-2 → primary GLEIF Registration Authority code, used by the
# national-ID → LEI reverse lookup. Mirrors frontend/src/lib/raCodes.ts and the
# RA table in CLAUDE.md — keep in sync when adding a register. Countries with
# sub-registries (e.g. GB) map to the dominant one; pass ``ra_code`` explicitly
# to target a specific sub-registry.
_RA_BY_COUNTRY: dict[str, str] = {
    "GB": "RA000585",  # UK Companies House (England & Wales)
    "NL": "RA000463",  # KvK (Netherlands)
    "NO": "RA000472",  # Brønnøysund / Brreg (Norway)
    "IE": "RA000215",  # CRO (Ireland)
    "LV": "RA000327",  # UR (Latvia)
    "LT": "RA000330",  # JAR (Lithuania)
    "FR": "RA000580",  # INPI / SIREN (France)
    "SE": "RA000544",  # Bolagsverket (Sweden)
    "EE": "RA000181",  # ariregister (Estonia)
    "BE": "RA000143",  # BCE/KBO (Belgium)
    "AT": "RA000128",  # Firmenbuch (Austria)
    "PL": "RA000439",  # KRS (Poland)
    "SK": "RA000476",  # RPO (Slovakia)
    "SG": "RA000509",  # ACRA (Singapore)
    "CA": "RA000072",  # Corporations Canada
    "DK": "RA000170",  # CVR (Denmark)
    "HR": "RA000156",  # Sudski registar (Croatia)
    "MT": "RA000443",  # Malta Business Registry
    "BR": "RA000681",  # Receita Federal CNPJ (Brazil)
}


class NationalIdMatch(BaseModel):
    """One LEI record carrying the queried national registration number."""

    lei: str
    name: str
    jurisdiction: str | None = None


class ResolveNationalIdResponse(BaseModel):
    """LEIs that carry a given national company-registration number."""

    number: str
    country: str | None = None
    ra_code: str | None = None
    matches: list[NationalIdMatch]
    # Advisory only (Phase A, rigour adoption): set when the number fails its
    # national scheme's check digit — the query still runs, this just explains
    # an otherwise-mystifying empty result. None when no validator applies.
    checksum_warning: str | None = None


@router.get("/resolve-national-id", response_model=ResolveNationalIdResponse)
@limiter.limit(default_tier)
async def resolve_national_id(
    request: Request,
    response: Response,
    number: str = Query(
        ...,
        min_length=1,
        description="National company-registration number, e.g. a UK Companies House number.",
    ),
    country: str = Query(
        "",
        description="ISO 3166-1 alpha-2 country code (e.g. 'GB'); resolved to a GLEIF RA code.",
    ),
    ra_code: str = Query(
        "",
        description="GLEIF Registration Authority code (e.g. 'RA000585'); overrides 'country' when set.",
    ),
) -> ResolveNationalIdResponse:
    """Resolve a local company-registration number to its LEI(s) via GLEIF.

    The inverse of OpenCheck's normal LEI-first flow: a caller who has a
    national registry number (but not the LEI) obtains it here, then feeds the
    LEI to ``/lookup``. Queries GLEIF's three local-id filter fields and
    de-duplicates by LEI. The RA code — resolved from ``country`` when not given
    explicitly — scopes the search to one registry, avoiding false matches from
    coincidental number collisions across jurisdictions.
    """
    return await _resolve_national_id_impl(number=number, country=country, ra_code=ra_code)


async def _resolve_national_id_impl(
    number: str, country: str = "", ra_code: str = ""
) -> ResolveNationalIdResponse:
    """Body of ``/resolve-national-id``, callable in-process (MCP tool)
    without going through the rate-limited route."""
    num = number.strip()
    code = (ra_code or _RA_BY_COUNTRY.get(country.strip().upper(), "")).strip()

    # Advisory check-digit validation (never blocks the query — the registry
    # is the authority): only for countries whose GLEIF registry number is
    # unambiguously a python-stdnum scheme (see identifiers.py).
    checksum_warning = identifiers.national_id_checksum_warning(country, num)

    adapter = REGISTRY.get("gleif")
    if adapter is None or not hasattr(adapter, "search_by_local_id"):
        raise HTTPException(status_code=503, detail="GLEIF adapter unavailable")

    hits = await adapter.search_by_local_id(num, code)
    matches: list[NationalIdMatch] = []
    for h in hits:
        if not h.hit_id:
            continue
        jurisdiction = None
        if isinstance(h.raw, dict):
            entity = ((h.raw.get("attributes") or {}).get("entity") or {})
            jurisdiction = entity.get("jurisdiction")
        matches.append(
            NationalIdMatch(lei=h.hit_id, name=h.name, jurisdiction=jurisdiction)
        )

    return ResolveNationalIdResponse(
        number=num,
        country=(country.strip().upper() or None),
        ra_code=(code or None),
        matches=matches,
        checksum_warning=checksum_warning,
    )


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


def _graph_shape(
    bods: list[dict[str, Any]], signals: list[dict[str, Any]]
) -> dict[str, int | None]:
    """How big the ownership-and-control graph on this page actually is.

    The report's third verdict column invites the reader into FullCheck, and
    an invitation with no numbers on it is a button. These are the numbers the
    check has *already earned*: statements OpenCheck mapped from the sources
    that answered, deduplicated by ``statementId`` because several sources
    describe the same party and the merged list keeps each of them.

    It deliberately does **not** reach for the GLEIF subsidiary total or
    anything FullCheck would go on to discover. Those are a different scope,
    and a sentence that mixes "what we have" with "what we might find" is the
    same overclaim as a progress bar that runs ahead of its stream.

    ``depth`` is the longest ownership chain the risk layer actually measured
    (``COMPLEX_OWNERSHIP_LAYERS`` carries it as ``evidence.longest_path``), or
    ``None`` when the signal did not fire — never a guess, and never 0, which
    would render as a flat graph.
    """
    seen: set[str] = set()
    counts = {"companies": 0, "people": 0, "relationships": 0}
    key = {"entity": "companies", "person": "people", "relationship": "relationships"}
    for statement in bods:
        sid = statement.get("statementId")
        if isinstance(sid, str):
            if sid in seen:
                continue
            seen.add(sid)
        bucket = key.get(str(statement.get("recordType")))
        if bucket:
            counts[bucket] += 1

    depth: int | None = None
    for signal in signals:
        if signal.get("code") != "COMPLEX_OWNERSHIP_LAYERS":
            continue
        path = (signal.get("evidence") or {}).get("longest_path")
        if isinstance(path, list) and path:
            depth = max(depth or 0, len(path))
    return {**counts, "depth": depth}


async def _count_only(source_id: str, hit_id: str) -> dict[str, int] | None:
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
    else:
        try:
            raw, prov = await _fetch_with_provenance(adapter, hit_id)
        except Exception:  # noqa: BLE001
            return None
        mapper = _mapper_for(source_id)
        if mapper is None or raw.get("is_stub"):
            return None
        with _provenance.mapping_provenance(prov):
            bods = list(mapper(raw))
    return {
        "total": len(bods),
        "entities": sum(1 for s in bods if s.get("recordType") == "entity"),
        "persons": sum(1 for s in bods if s.get("recordType") == "person"),
        "relationships": sum(1 for s in bods if s.get("recordType") == "relationship"),
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
        bods = override
        issues = validate_shape(bods)
        prov = _stored_bundle_provenance(source_id, hit_id)
    else:
        mapper = _mapper_for(source_id)
        if mapper and not raw.get("is_stub"):
            with _provenance.mapping_provenance(prov):
                bundle: BODSBundle = mapper(raw)
            bods = list(bundle)
            issues = validate_shape(bods)

    license_notice = _license_notice_for(adapter.info, raw)
    signals = [s.to_dict() for s in assess_bundle(source_id, raw, bods, hit_id=hit_id)]
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
