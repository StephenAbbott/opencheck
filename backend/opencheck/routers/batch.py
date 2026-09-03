"""``GET /batch-stream`` — screen up to twenty LEIs as one SSE stream —
and ``GET /batch-export`` (Phase 167), the same batch as one zip.

Phase 164. The route is the HTTP skin on :mod:`opencheck.batch`: it parses
the pasted list, refuses declared bots exactly as ``/lookup-stream`` does
(same classifier, same setting, same message shape — a batch is the single
most expensive request the site can receive, so it inherits the Phase 144
gate rather than growing a second one), sits on the **heavy** rate tier
because one request fans out into up to twenty full lookups, and streams
each row as it completes.

Events, in order::

    batch_start  {accepted: [LEI…], rejected: [{token, reason}…],
                  overflow: n, cap: 20, concurrency: 2}
    row_done     {lei, legal_name, jurisdiction, register_status, verdict,
                  risk_count, context_count, coverage, degraded, …}
    row_failed   {lei, status, reason, retryable, degraded: true}
    batch_done   {requested, done, failed}

``row_done`` / ``row_failed`` arrive in completion order, not paste order.

``/batch-export`` runs the same loop to completion (free inside the
15-minute replay window, which is the case when the page has just shown
the table) and returns ``opencheck-batch-<stamp>.zip``::

    bundle.json    every row's BODS statements, de-duplicated by statementId
    rows.csv       the table — the same columns as the page's CSV button,
                   failed rows included with their reason
    manifest.json  totals, contributing sources, the licence verdict
    LICENSES.md    the composite licence over the union of sources

The most restrictive licence in the union applies to the whole bundle:
one CC-BY-NC source in one row makes the zip non-commercial, and the
manifest and LICENSES.md say so.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request, Response
from sse_starlette.sse import EventSourceResponse

from .. import __version__
from .. import batch as _batch
from ..bods import validate_shape
from ..config import get_settings
from ..licensing import assess as assess_licensing
from ..memwatch import is_bot
from ..ratelimit import heavy_tier, limiter
from ..sources import SearchKind

router = APIRouter()

# 3.10-compatible alias for datetime.UTC (identical object on 3.11+).
UTC = timezone.utc


def _refuse_bots(request: Request, route: str = "/batch-stream") -> None:
    if get_settings().bot_gate_lookup_stream and is_bot(
        request.headers.get("user-agent")
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                f"{route} serves the interactive OpenCheck app and is "
                "disallowed for automated clients (see /robots.txt). Use the "
                "JSON API at /lookup?lei=<LEI> (rate-limited) one LEI at a "
                "time, the MCP tool opencheck_batch_lookup, or the crawlable "
                "per-entity pages at /entity/<LEI>."
            ),
        )


async def _batch_sse_events(
    parsed: _batch.ParsedList, deepen_top: int, refresh: bool
) -> AsyncIterator[dict[str, Any]]:
    yield {
        "event": "batch_start",
        "data": json.dumps(
            {
                "accepted": parsed.leis,
                "rejected": [r.to_dict() for r in parsed.rejected],
                "overflow": parsed.overflow,
                "cap": parsed.cap,
                "concurrency": _batch.batch_concurrency(),
            }
        ),
    }
    async for event, payload in _batch.run_batch(
        parsed.leis, deepen_top=deepen_top, refresh=refresh
    ):
        yield {"event": event, "data": json.dumps(payload)}


@router.get("/batch-stream")
@limiter.limit(heavy_tier)
async def batch_stream(
    request: Request,
    leis: str = Query(
        ...,
        description=(
            "LEIs to screen, separated by commas, semicolons, spaces or "
            f"newlines. At most {_batch.MAX_ROWS} are taken; the rest are "
            "reported in batch_start.overflow."
        ),
    ),
    deepen_top: int = Query(5, ge=0, le=10),
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
) -> Response:
    """Screen a list of LEIs; one SSE ``row_done``/``row_failed`` per LEI."""
    _refuse_bots(request)
    parsed = _parse_or_422(leis)
    return EventSourceResponse(_batch_sse_events(parsed, deepen_top, refresh))


def _parse_or_422(leis: str) -> _batch.ParsedList:
    parsed = _batch.parse_lei_list(leis)
    if not parsed.leis:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "No valid LEIs to screen.",
                "rejected": [r.to_dict() for r in parsed.rejected],
            },
        )
    return parsed


def build_batch_zip(
    result: _batch.BatchResult, *, parsed: _batch.ParsedList, stamp: str
) -> bytes:
    """The combined export: BODS bundle + rows.csv + manifest + LICENSES.md."""
    from .export import _build_licenses_md

    licensing = assess_licensing(result.contributing_ids)
    counts = {"entity": 0, "person": 0, "relationship": 0}
    for st in result.statements:
        rt = st.get("recordType")
        if rt in counts:
            counts[rt] += 1
    manifest = {
        "opencheck_version": __version__,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "kind": "batch-screening",
        "leis": parsed.leis,
        "rejected": [r.to_dict() for r in parsed.rejected],
        "overflow": parsed.overflow,
        "cap": parsed.cap,
        "counts": result.totals,
        "failed": result.failed,
        "bods_statement_count": len(result.statements),
        "duplicate_statements_collapsed": result.duplicate_statement_count,
        "node_counts": counts,
        "contributing_source_ids": result.contributing_ids,
        "bods_validation_issues": validate_shape(result.statements),
        "licensing": licensing.model_dump(),
        "note": (
            "A row in `failed` was NOT screened and contributes no statements; "
            "a row with degraded=true in rows.csv had a screening check that did "
            "not fully run. Neither is a clean result."
        ),
    }
    licenses_md = _build_licenses_md(
        contributing_ids=result.contributing_ids,
        license_notices=result.license_notices,
        licensing=licensing,
        query=f"batch of {len(parsed.leis)} LEIs",
        kind=SearchKind.ENTITY,
    )
    base = f"opencheck-batch-{stamp}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{base}/bundle.json", json.dumps(result.statements, indent=2))
        zf.writestr(f"{base}/rows.csv", _batch.rows_csv(result))
        zf.writestr(f"{base}/manifest.json", json.dumps(manifest, indent=2, default=str))
        zf.writestr(f"{base}/LICENSES.md", licenses_md)
    return buf.getvalue()


@router.get("/batch-export")
@limiter.limit(heavy_tier)
async def batch_export(
    request: Request,
    leis: str = Query(..., description="LEIs to export, separated as for /batch-stream."),
    deepen_top: int = Query(5, ge=0, le=10),
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
) -> Response:
    """Screen a list and return one zip: merged BODS, rows.csv, manifest, LICENSES.md."""
    _refuse_bots(request, "/batch-export")
    parsed = _parse_or_422(leis)
    result = await _batch.collect_batch(
        parsed.leis, deepen_top=deepen_top, refresh=refresh
    )
    stamp = datetime.now(UTC).strftime("%Y%m%d")
    body = build_batch_zip(result, parsed=parsed, stamp=stamp)
    return Response(
        content=body,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="opencheck-batch-{stamp}.zip"',
            # The zip carries a non-empty `failed` list in manifest.json when a
            # row could not be screened — surfaced as a header too, so a
            # script can notice without opening the archive.
            "X-OpenCheck-Batch-Failed": str(len(result.failed)),
        },
    )
