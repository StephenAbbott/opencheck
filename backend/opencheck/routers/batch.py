"""``GET /batch-stream`` — screen up to twenty LEIs as one SSE stream.

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
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request, Response
from sse_starlette.sse import EventSourceResponse

from .. import batch as _batch
from ..config import get_settings
from ..memwatch import is_bot
from ..ratelimit import heavy_tier, limiter

router = APIRouter()


def _refuse_bots(request: Request) -> None:
    if get_settings().bot_gate_lookup_stream and is_bot(
        request.headers.get("user-agent")
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "/batch-stream serves the interactive OpenCheck app and is "
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
    parsed = _batch.parse_lei_list(leis)
    if not parsed.leis:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "No valid LEIs to screen.",
                "rejected": [r.to_dict() for r in parsed.rejected],
            },
        )
    return EventSourceResponse(_batch_sse_events(parsed, deepen_top, refresh))
