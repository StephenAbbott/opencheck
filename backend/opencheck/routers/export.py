"""Export endpoint — /export."""

from __future__ import annotations

import asyncio
import io
import json
import re
import zipfile
from datetime import UTC, datetime
from typing import Any

from bods_xml.canonical import convert as _bods_xml_convert
from bods_xml.canonical import to_string as _bods_xml_str
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from .. import __version__
from ..licensing import assess as assess_licensing
from ..licensing import full_matrix
from ..reporting import PdfUnavailable, build_report_pdf
from ..sources import REGISTRY, SearchKind
from .lookup import ReportResponse, _build_report, lookup

router = APIRouter()

_EXPORT_FORMATS = {"json", "jsonl", "zip", "xml"}

_TRAFFIC = {"green": "🟢", "amber": "🟡", "red": "🔴"}


@router.get("/license-matrix")
async def license_matrix(
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
        pattern="^(json|jsonl|zip|xml)$",
        description="json (pretty array) | jsonl (newline-delimited) | zip (bundle) | xml (canonical BODS XML)",
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
        payload = await lookup(lei, deepen_top)
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


@router.post("/export/pdf")
async def export_pdf(req: PdfExportRequest) -> Response:
    """Build an accessible (tagged PDF/UA-1) due-diligence report for an LEI.

    Reuses the same cached lookup pipeline as ``/lookup`` (so the PDF can't
    diverge from the page), renders the report off the event loop, and streams
    it back. Returns 503 when the PDF toolchain (WeasyPrint) is unavailable.
    """
    norm_lei = req.lei.strip().upper()
    payload = await lookup(norm_lei, req.deepen_top)  # raises 400/404 on bad LEI
    report = payload.model_dump()
    try:
        pdf_bytes = await asyncio.to_thread(
            build_report_pdf, report, narrative=req.narrative
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

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.json", bods_json)
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.jsonl", bods_jsonl)
        zf.writestr(f"opencheck-{slug}-{stamp}/bods.xml", bods_xml)
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
