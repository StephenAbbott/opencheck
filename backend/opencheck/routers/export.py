"""Export endpoint — /export."""

from __future__ import annotations

import asyncio
import io
import json
import re
import zipfile
from datetime import datetime, timezone

# 3.10-compatible alias for datetime.UTC (identical object on 3.11+).
UTC = timezone.utc
from typing import Any, Literal

from bods_xml.canonical import convert as _bods_xml_convert
from bods_xml.canonical import to_string as _bods_xml_str
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from .. import __version__
from ..bods import (
    aml_ai_counts,
    build_aml_ai_files,
    build_gql_files,
    gql_counts,
    map_to_ftm,
    map_to_senzing,
    to_cypher,
    to_ftm_jsonl,
    to_rdf,
    to_senzing_jsonl,
    validate_shape,
)
from ..bods.senzing import _desc_to_source_id
from ..dispositions import load_dispositions
from ..licensing import assess as assess_licensing
from ..licensing import full_matrix
from ..ratelimit import default_tier, heavy_tier, limiter, lookup_tier
from ..reporting import PdfUnavailable, build_report_markdown, build_report_pdf
from ..sources import REGISTRY, SearchKind
from .lookup import ReportResponse, _build_report, _lookup_impl

router = APIRouter()

_EXPORT_FORMATS = {"json", "jsonl", "zip", "xml", "senzing", "ftm", "gql", "amlai", "rdf"}
# One regex for the /export ?format= validation, derived from the set above so
# the two can't drift (ExportNetworkRequest.format is the third place — a
# typing.Literal, which must stay a literal; a test pins it to this set).
_EXPORT_FORMAT_PATTERN = f"^({'|'.join(sorted(_EXPORT_FORMATS))})$"

_TRAFFIC = {"green": "🟢", "amber": "🟡", "red": "🔴"}


@router.get("/license-matrix")
@limiter.limit(default_tier)
async def license_matrix(
    request: Request,
    response: Response,
    sources: str | None = Query(
        None,
        description=(
            "Optional comma-separated source ids. When given, the response also "
            "includes an `assessment` of the combined licensing of those sources."
        ),
    ),
) -> dict:
    """The full licensing compatibility matrix: every source's licence terms
    (commercial use, attribution, share-alike) plus the distinct licence
    catalogue. With `?sources=`, also returns the combined assessment for the
    contributing sources. Backs the Export panel's licensing assistant."""
    matrix = full_matrix()
    if sources:
        ids = [s.strip() for s in sources.split(",") if s.strip()]
        matrix["assessment"] = assess_licensing(ids).model_dump()
    return matrix


@router.get("/export")
@limiter.limit(lookup_tier)
async def export(
    request: Request,
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
        pattern=_EXPORT_FORMAT_PATTERN,
        description=(
            "json (pretty array) | jsonl (newline-delimited) | zip (bundle) | "
            "xml (canonical BODS XML) | senzing (newline-delimited Senzing JSON "
            "entity records, ready to load into Senzing) | ftm (newline-delimited "
            "FollowTheMoney entities, ready for OpenSanctions / OpenAleph / "
            "alephclient workflows) | gql (zip: BigQuery property-graph CSV "
            "tables + CREATE PROPERTY GRAPH DDL + 14 GQL queries, via bods-gql) | "
            "amlai (zip: Google AML AI input tables — party / "
            "party_supplementary_data / account_party_link NDJSON, via "
            "bods-aml-ai) | rdf (BODS RDF as TriG: one named graph per "
            "statement per the Open Ownership conversion pattern, canonical "
            "licence URI on every statement, and OpenCheck's risk signals / "
            "entity-resolution links as bods:Annotation overlays in a "
            "separate analysis graph)"
        ),
    ),
    subsidiaries: bool = Query(
        False,
        description=(
            "Opt-in: also fold the GLEIF subsidiary network (direct + ultimate "
            "children) into the BODS bundle. Off by default because a large group "
            "can add hundreds of statements. LEI exports only; requires live mode."
        ),
    ),
) -> Response:
    """Download a BODS v0.4 bundle for a subject."""
    if format not in _EXPORT_FORMATS:
        raise HTTPException(status_code=400, detail=f"Unknown format {format!r}")
    if lei is None and (q is None or not q.strip()):
        raise HTTPException(
            status_code=400,
            detail="Provide either ?lei=<LEI> or ?q=<free-text query>.",
        )

    sub_count = 0
    if lei is not None:
        payload = await _lookup_impl(lei=lei, deepen_top=deepen_top)
        slug = _filename_slug(payload.lei)
        export_query = payload.lei
        if subsidiaries:
            payload, sub_count = await _merge_subsidiaries(payload)
    else:
        assert q is not None
        payload = await _build_report(q, kind, deepen_top)
        slug = _filename_slug(q)
        export_query = q
    stamp = datetime.now(UTC).strftime("%Y%m%d")

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

    if format == "xml":
        xml_root = _bods_xml_convert(payload.bods)
        body = _bods_xml_str(xml_root).encode("utf-8")
        return Response(
            content=body,
            media_type="application/xml",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}.xml"'
                ),
            },
        )

    if format == "senzing":
        body = to_senzing_jsonl(payload.bods).encode("utf-8")
        return Response(
            content=body,
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}-senzing.jsonl"'
                ),
            },
        )

    if format == "ftm":
        body = to_ftm_jsonl(payload.bods).encode("utf-8")
        return Response(
            content=body,
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}-ftm.jsonl"'
                ),
            },
        )

    if format == "rdf":
        body = to_rdf(
            payload.bods,
            fmt="trig",
            anchor_lei=getattr(payload, "lei", None),
            run_date=datetime.now(UTC).strftime("%Y-%m-%d"),
            risk_signals=payload.risk_signals,
            possibly_same_entities=payload.possibly_same_entities,
            degraded_sources=payload.degraded_sources,
        ).encode("utf-8")
        return Response(
            content=body,
            media_type="application/trig",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}.trig"'
                ),
            },
        )

    if format == "gql":
        contributing_ids = sorted({h.source_id for h in payload.hits if not h.is_stub})
        licenses_md = _build_licenses_md(
            contributing_ids=contributing_ids,
            license_notices=payload.license_notices,
            licensing=assess_licensing(contributing_ids),
            query=export_query,
            kind=kind,
        )
        body = _build_gql_zip(payload.bods, slug=slug, stamp=stamp, licenses_md=licenses_md)
        return Response(
            content=body,
            media_type="application/zip",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}-bigquery.zip"'
                ),
            },
        )

    if format == "amlai":
        contributing_ids = sorted({h.source_id for h in payload.hits if not h.is_stub})
        licenses_md = _build_licenses_md(
            contributing_ids=contributing_ids,
            license_notices=payload.license_notices,
            licensing=assess_licensing(contributing_ids),
            query=export_query,
            kind=kind,
        )
        body = _build_aml_ai_zip(payload.bods, slug=slug, stamp=stamp, licenses_md=licenses_md)
        return Response(
            content=body,
            media_type="application/zip",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="opencheck-{slug}-{stamp}-aml-ai.zip"'
                ),
            },
        )

    # format == "zip"
    body = _build_export_zip(
        payload, q=export_query, kind=kind, slug=slug, stamp=stamp,
        subsidiary_statement_count=sub_count,
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


class ExportNetworkRequest(BaseModel):
    """Body for ``POST /export-network`` — export a client-assembled FullCheck
    network (a BODS bundle) without re-running the lookup."""

    bods: list[dict[str, Any]]
    format: Literal[
        "json", "jsonl", "xml", "senzing", "ftm", "cypher", "gql", "amlai", "rdf", "zip"
    ] = "zip"
    slug: str | None = None


@router.post("/export-network")
@limiter.limit(heavy_tier)
async def export_network(request: Request, req: ExportNetworkRequest) -> Response:
    """Format a FullCheck network (posted BODS) for download.

    A FullCheck network is assembled in the browser by progressive expansion, so
    the server can't reproduce it from a lookup — the client posts the BODS bundle
    and this returns it in the requested format, reusing the same Senzing / XML /
    Cypher / licensing machinery as ``/export``.
    """
    bods = req.bods or []
    slug = _filename_slug(req.slug or "fullcheck-network")
    stamp = datetime.now(UTC).strftime("%Y%m%d")

    def _file(body: bytes, media: str, ext: str) -> Response:
        return Response(
            content=body,
            media_type=media,
            headers={
                "Content-Disposition": f'attachment; filename="opencheck-{slug}-{stamp}.{ext}"'
            },
        )

    if req.format == "json":
        return _file(json.dumps(bods, indent=2).encode("utf-8"), "application/json", "json")
    if req.format == "jsonl":
        body = ("\n".join(json.dumps(s) for s in bods) + "\n").encode("utf-8")
        return _file(body, "application/x-ndjson", "jsonl")
    if req.format == "xml":
        body = _bods_xml_str(_bods_xml_convert(bods)).encode("utf-8")
        return _file(body, "application/xml", "xml")
    if req.format == "senzing":
        return _file(to_senzing_jsonl(bods).encode("utf-8"), "application/x-ndjson", "senzing.jsonl")
    if req.format == "ftm":
        return _file(to_ftm_jsonl(bods).encode("utf-8"), "application/x-ndjson", "ftm.jsonl")
    if req.format == "cypher":
        return _file(to_cypher(bods).encode("utf-8"), "text/plain; charset=utf-8", "cypher")
    if req.format == "rdf":
        # Client-assembled network: data statements only — the analytical
        # overlay needs a lookup run, so it ships via GET /export?format=rdf.
        return _file(to_rdf(bods, fmt="trig").encode("utf-8"), "application/trig", "trig")
    if req.format == "gql":
        contributing_ids = _network_source_ids(bods)
        licenses_md = _build_licenses_md(
            contributing_ids=contributing_ids,
            license_notices=[],
            licensing=assess_licensing(contributing_ids),
            query=slug,
            kind=SearchKind.ENTITY,
        )
        return _file(
            _build_gql_zip(bods, slug=slug, stamp=stamp, licenses_md=licenses_md),
            "application/zip",
            "bigquery.zip",
        )
    if req.format == "amlai":
        contributing_ids = _network_source_ids(bods)
        licenses_md = _build_licenses_md(
            contributing_ids=contributing_ids,
            license_notices=[],
            licensing=assess_licensing(contributing_ids),
            query=slug,
            kind=SearchKind.ENTITY,
        )
        return _file(
            _build_aml_ai_zip(bods, slug=slug, stamp=stamp, licenses_md=licenses_md),
            "application/zip",
            "aml-ai.zip",
        )
    # zip
    body = _build_network_zip(bods, slug=slug, stamp=stamp)
    return Response(
        content=body,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="opencheck-{slug}-{stamp}.zip"'},
    )


def _network_source_ids(bods: list[dict[str, Any]]) -> list[str]:
    """Registered source ids that contributed to a network, from BODS source blocks."""
    rev = _desc_to_source_id()
    ids: set[str] = set()
    for s in bods:
        desc = ((s.get("source") or {}).get("description") or "").strip()
        sid = rev.get(desc)
        if sid:
            ids.add(sid)
    return sorted(ids)


def _build_gql_zip(
    bods: list[dict[str, Any]], *, slug: str, stamp: str, licenses_md: str
) -> bytes:
    """The BigQuery GQL package: node/edge CSVs + property-graph DDL + the 14
    GQL queries + README (from ``bods/gql.py``), plus the licence notes."""
    buf = io.BytesIO()
    base = f"opencheck-{slug}-{stamp}-bigquery"
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in build_gql_files(bods).items():
            zf.writestr(f"{base}/{name}", content)
        zf.writestr(f"{base}/LICENSES.md", licenses_md)
    return buf.getvalue()


def _build_aml_ai_zip(
    bods: list[dict[str, Any]], *, slug: str, stamp: str, licenses_md: str
) -> bytes:
    """The Google AML AI package: the three input-table NDJSON files + README
    (from ``bods/aml_ai.py``), plus the licence notes."""
    buf = io.BytesIO()
    base = f"opencheck-{slug}-{stamp}-aml-ai"
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in build_aml_ai_files(bods).items():
            zf.writestr(f"{base}/{name}", content)
        zf.writestr(f"{base}/LICENSES.md", licenses_md)
    return buf.getvalue()


def _build_network_zip(bods: list[dict[str, Any]], *, slug: str, stamp: str) -> bytes:
    """Bundle a FullCheck network: BODS (json/jsonl/xml) + Senzing + Cypher +
    manifest + LICENSES.md (most-restrictive licence across contributing sources)."""
    contributing_ids = _network_source_ids(bods)
    licensing = assess_licensing(contributing_ids)
    counts = {"entity": 0, "person": 0, "relationship": 0}
    for s in bods:
        rt = s.get("recordType")
        if rt in counts:
            counts[rt] += 1

    manifest = {
        "opencheck_version": __version__,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "kind": "fullcheck-network",
        "bods_statement_count": len(bods),
        "node_counts": counts,
        "contributing_source_ids": contributing_ids,
        "senzing_record_count": len(map_to_senzing(bods)),
        "ftm_entity_count": len(map_to_ftm(bods)),
        **gql_counts(bods),
        **aml_ai_counts(bods),
        "bods_validation_issues": validate_shape(bods),
        "licensing": licensing.model_dump(),
    }
    licenses_md = _build_licenses_md(
        contributing_ids=contributing_ids,
        license_notices=[],
        licensing=licensing,
        query=slug,
        kind=SearchKind.ENTITY,
    )

    buf = io.BytesIO()
    base = f"opencheck-{slug}-{stamp}"
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{base}/bods.json", json.dumps(bods, indent=2))
        zf.writestr(f"{base}/bods.jsonl", "\n".join(json.dumps(s) for s in bods) + "\n")
        zf.writestr(f"{base}/bods.xml", _bods_xml_str(_bods_xml_convert(bods)))
        zf.writestr(f"{base}/senzing.jsonl", to_senzing_jsonl(bods))
        zf.writestr(f"{base}/ftm.jsonl", to_ftm_jsonl(bods))
        zf.writestr(f"{base}/network.cypher", to_cypher(bods))
        zf.writestr(f"{base}/manifest.json", json.dumps(manifest, indent=2, default=str))
        zf.writestr(f"{base}/LICENSES.md", licenses_md)
    return buf.getvalue()


class PdfExportRequest(BaseModel):
    """Body for ``POST /export/pdf``.

    ``narrative`` is the result the client already received from ``/narrative``;
    it is embedded verbatim so the PDF needs no fresh model call (per the product
    decision — the summary appears only when the user generated it on the page).
    The diagrams are rendered server-side from the BODS data, so no graph images
    are uploaded.
    """

    lei: str = Field(..., description="ISO 17442 Legal Entity Identifier.")
    deepen_top: int = Field(5, ge=0, le=10)
    narrative: dict[str, Any] | None = None
    # Analyst claim dispositions (a DispositionRecord dict) rendered next to
    # each claim so the PDF is the analyst's defensible record. When omitted,
    # the stored record for narrative.run_id is used if one exists.
    dispositions: dict[str, Any] | None = None


async def _resolve_dispositions(
    req: PdfExportRequest, norm_lei: str
) -> dict[str, Any] | None:
    """The dispositions to render: the posted record, else the stored sheet for
    this narrative run — so a report downloaded in a later session still
    carries the analyst's decisions."""
    if req.dispositions is not None:
        return req.dispositions
    run_id = (req.narrative or {}).get("run_id") or ""
    if not run_id:
        return None
    try:
        stored = await asyncio.to_thread(load_dispositions, norm_lei, run_id)
    except ValueError:
        return None  # malformed run_id — render without dispositions
    return stored.model_dump(mode="json") if stored is not None else None


@router.post("/export/pdf")
@limiter.limit(heavy_tier)
async def export_pdf(request: Request, req: PdfExportRequest) -> Response:
    """Build an accessible (tagged PDF/UA-1) due-diligence report for an LEI.

    Reuses the same cached lookup pipeline as ``/lookup`` (so the PDF can't
    diverge from the page), renders the report off the event loop, and streams
    it back. Returns 503 when the PDF toolchain (WeasyPrint) is unavailable.
    """
    norm_lei = req.lei.strip().upper()
    payload = await _lookup_impl(lei=norm_lei, deepen_top=req.deepen_top)  # raises 400/404 on bad LEI
    report = payload.model_dump()
    dispositions = await _resolve_dispositions(req, norm_lei)

    try:
        pdf_bytes = await asyncio.to_thread(
            build_report_pdf, report, narrative=req.narrative, dispositions=dispositions
        )
    except PdfUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    slug = _filename_slug(payload.legal_name or payload.lei or norm_lei)
    stamp = datetime.now(UTC).strftime("%Y%m%d")
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="opencheck-{slug}-{stamp}.pdf"',
        },
    )


@router.post("/export/markdown")
@limiter.limit(heavy_tier)
async def export_markdown(request: Request, req: PdfExportRequest) -> Response:
    """Build the due-diligence report as portable Markdown for an LEI.

    Same request body and lookup pipeline as ``/export/pdf`` — the narrative
    and analyst dispositions are embedded identically — but rendered as plain
    Markdown with no WeasyPrint dependency, so this route is always available
    (including on deployments where ``/export/pdf`` returns 503).
    """
    norm_lei = req.lei.strip().upper()
    payload = await _lookup_impl(lei=norm_lei, deepen_top=req.deepen_top)  # raises 400/404 on bad LEI
    report = payload.model_dump()
    dispositions = await _resolve_dispositions(req, norm_lei)

    markdown = await asyncio.to_thread(
        build_report_markdown, report, narrative=req.narrative, dispositions=dispositions
    )

    slug = _filename_slug(payload.legal_name or payload.lei or norm_lei)
    stamp = datetime.now(UTC).strftime("%Y%m%d")
    return Response(
        content=markdown.encode("utf-8"),
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="opencheck-{slug}-{stamp}.md"',
        },
    )


def _filename_slug(query: str) -> str:
    """Make a query safe to embed in a download filename."""
    slug = re.sub(r"[^a-z0-9]+", "-", query.lower()).strip("-")
    return slug or "export"


async def _merge_subsidiaries(payload: ReportResponse) -> tuple[ReportResponse, int]:
    """Opt-in: fold the GLEIF subsidiary-network BODS into the export bundle.

    Deduplicated by ``statementId`` — the subsidiary subject shares the GLEIF
    subject's statementId (both derive from the LEI), so it collapses cleanly and
    the child relationships resolve against the existing subject. Best-effort: any
    failure, live-mode-off, or an entity with no children leaves the bundle
    unchanged. Returns the (possibly new) payload and the number of statements
    added. Never mutates the lookup payload in place — a fresh copy is returned so
    the shared lookup cache is untouched.
    """
    from ..bods import validate_shape
    from ..subsidiaries import assemble_subsidiaries

    try:
        data = await assemble_subsidiaries(payload.lei, include_bods=True)
    except Exception:  # noqa: BLE001
        return payload, 0
    sub = (data or {}).get("bods") or []
    if not sub:
        return payload, 0

    existing = {s.get("statementId") for s in payload.bods}
    added = [s for s in sub if s.get("statementId") not in existing]
    if not added:
        return payload, 0

    merged = payload.bods + added
    return (
        payload.model_copy(update={"bods": merged, "bods_issues": validate_shape(merged)}),
        len(added),
    )


def _build_export_zip(
    payload: ReportResponse,
    *,
    q: str,
    kind: SearchKind,
    slug: str,
    stamp: str,
    subsidiary_statement_count: int = 0,
) -> bytes:
    """Assemble the canonical export bundle: BODS + manifest + licenses."""
    sources_consulted = [
        adapter.info.model_dump()
        for adapter in REGISTRY.values()
    ]
    contributing_ids = sorted({h.source_id for h in payload.hits if not h.is_stub})
    licensing = assess_licensing(contributing_ids)

    manifest = {
        "opencheck_version": __version__,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
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
        "senzing_record_count": len(map_to_senzing(payload.bods)),
        "ftm_entity_count": len(map_to_ftm(payload.bods)),
        **gql_counts(payload.bods),
        **aml_ai_counts(payload.bods),
        "subsidiary_network_included": subsidiary_statement_count > 0,
        "subsidiary_statement_count": subsidiary_statement_count,
        "bods_validation_issues": payload.bods_issues,
        "license_notices": payload.license_notices,
        "licensing": licensing.model_dump(),
        "errors": payload.errors,
    }

    licenses_md = _build_licenses_md(
        contributing_ids=contributing_ids,
        license_notices=payload.license_notices,
        licensing=licensing,
        query=q,
        kind=kind,
    )

    bods_json = json.dumps(payload.bods, indent=2)
    bods_jsonl = "\n".join(json.dumps(s) for s in payload.bods) + "\n"
    bods_xml = _bods_xml_str(_bods_xml_convert(payload.bods))
    senzing_jsonl = to_senzing_jsonl(payload.bods)
    ftm_jsonl = to_ftm_jsonl(payload.bods)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.json", bods_json)
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.jsonl", bods_jsonl)
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.xml", bods_xml)
        zf.writestr(f"opencheck-{slug}-{stamp}/senzing.jsonl", senzing_jsonl)
        zf.writestr(f"opencheck-{slug}-{stamp}/ftm.jsonl", ftm_jsonl)
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
    licensing,
    query: str,
    kind: SearchKind,
) -> str:
    """Markdown licence notes: a compatibility verdict + traffic-light matrix +
    per-source attribution + any NC notices."""
    lines: list[str] = []
    lines.append("# OpenCheck export — licence notes")
    lines.append("")
    lines.append(
        f"This bundle was generated for query `{query}` (kind: {kind.value}). "
        "It combines data from multiple open-data sources, each with its own "
        "licence. The **most restrictive** licence applies to the combined "
        "dataset for re-use."
    )
    lines.append("")

    # Compatibility verdict (the licensing assistant, in print form).
    lines.append("## Compatibility")
    lines.append("")
    lines.append(f"{_TRAFFIC.get(licensing.color, '')} **{licensing.headline}**")
    lines.append("")
    lines.append(f"- Commercial use: **{licensing.commercial_use}**")
    lines.append(f"- Attribution required: **{'yes' if licensing.attribution_required else 'no'}**")
    lines.append(f"- Share-alike obligations: **{'yes' if licensing.share_alike else 'no'}**")
    lines.append("")
    if licensing.warnings:
        for w in licensing.warnings:
            lines.append(f"> ⚠️ {w}")
            lines.append("")

    # Per-source traffic-light matrix.
    if licensing.per_source:
        lines.append("## Source licence matrix")
        lines.append("")
        lines.append("| | Source | Licence | Commercial | Attribution | Share-alike |")
        lines.append("|---|---|---|---|---|---|")
        for s in licensing.per_source:
            t = s.terms
            lines.append(
                f"| {_TRAFFIC.get(t.color, '')} | {s.name} (`{s.source_id}`) | {t.license} "
                f"| {t.commercial_use} | {'yes' if t.attribution_required else 'no'} "
                f"| {'yes' if t.share_alike else 'no'} |"
            )
        lines.append("")

    # Attribution + homepage detail per source.
    lines.append("## Attribution")
    lines.append("")
    for source_id in contributing_ids:
        adapter = REGISTRY.get(source_id)
        if adapter is None:
            continue
        info = adapter.info
        lines.append(f"### {info.name} (`{info.id}`)")
        lines.append("")
        lines.append(f"- **Licence**: {info.license}")
        lines.append(f"- **Homepage**: {info.homepage}")
        lines.append(f"- **Attribution**: {info.attribution}")
        lines.append("")

    if license_notices:
        lines.append("## Specific notices")
        lines.append("")
        for n in license_notices:
            lines.append(f"- **{n['source_id']}/{n['hit_id']}** — {n['notice']}")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"_{licensing.disclaimer}_")
    return "\n".join(lines) + "\n"
