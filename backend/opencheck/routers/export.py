"""Export endpoint — /export."""

from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from .. import __version__
from ..sources import REGISTRY, SearchKind
from .lookup import LookupResponse, ReportResponse, _build_report, lookup

router = APIRouter()

_EXPORT_FORMATS = {"json", "jsonl", "zip"}


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
        pattern="^(json|jsonl|zip)$",
        description="json (pretty array) | jsonl (newline-delimited) | zip (bundle)",
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

    if lei is not None:
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
    """Markdown summary of every source's license + any NC notices."""
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
