"""Share endpoints — per-entity og:image card + crawler-readable share page.

Social crawlers (Slack, LinkedIn, WhatsApp, X…) don't execute JavaScript,
so the static SPA serves them identical meta tags for every ``?lei=`` URL.
These endpoints close that gap:

* ``GET /og/{lei}.png`` — the shareable summary card (see ``og_image``).
  Rendered from the lookup replay cache when a completed run for the LEI
  is available (the sharer normally just ran it), else a "teaser" variant
  with the entity name resolved from GLEIF. Cached in memory.
* ``GET /share/{lei}`` — a minimal HTML page carrying the per-entity
  Open Graph / Twitter meta tags, which immediately redirects human
  visitors to the frontend. The frontend "Copy share link" button hands
  out THIS url so link previews show the live card.
"""

from __future__ import annotations

import asyncio
import html
import re
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import HTMLResponse

from ..config import get_settings
from ..og_image import render_share_card
from . import lookup as lookup_router

router = APIRouter(tags=["share"])

_LEI_RE = re.compile(r"^[0-9A-Z]{18}[0-9]{2}$")

# In-memory PNG cache: LEI -> (monotonic ts, png bytes, is_full_card).
# Full cards live longer than teasers, which should upgrade as soon as a
# completed lookup lands in the replay cache.
_OG_TTL_FULL = 60 * 60.0
_OG_TTL_TEASER = 120.0
_OG_MAX_ENTRIES = 128
_OG_CACHE: dict[str, tuple[float, bytes, bool]] = {}


def _clean_lei(lei: str) -> str:
    lei = (lei or "").strip().upper()
    if not _LEI_RE.match(lei):
        raise HTTPException(status_code=404, detail="Not a valid LEI.")
    return lei


def _summary_from_replay(lei: str) -> tuple[str | None, list[dict[str, Any]]] | None:
    """(legal_name, risk_signals) from a completed cached lookup, else None.

    Reads the lookup router's replay cache — any deepen_top variant will
    do, the freshest wins. Signals come from the ``risk_signals`` event;
    the name from ``gleif_done``.
    """
    prefix = f"{lei}:"
    best: tuple[float, list] | None = None
    now = time.monotonic()
    for key, entry in lookup_router._REPLAY_CACHE.items():
        if not key.startswith(prefix):
            continue
        if now - entry.stored >= lookup_router._REPLAY_TTL_SECONDS:
            continue
        if best is None or entry.stored > best[0]:
            best = (entry.stored, entry.events)
    if best is None:
        return None
    name: str | None = None
    signals: list[dict[str, Any]] = []
    for event, payload in best[1]:
        if event == "gleif_done":
            name = payload.get("legal_name")
        elif event == "risk_signals":
            signals = list(payload.get("signals") or [])
    return name, signals


async def _teaser_name(lei: str) -> str | None:
    """Entity name via the GLEIF anchor, for cards without cached results."""
    try:
        ctx, _ = await lookup_router._resolve_ctx(lei)
        return ctx.legal_name or None
    except lookup_router._LookupAbort as abort:
        if abort.status == 404:
            raise HTTPException(status_code=404, detail="LEI not found.")
        return None
    except Exception:  # noqa: BLE001 — a teaser without a name still works
        return None


async def _card_for(lei: str) -> tuple[bytes, bool]:
    """PNG card bytes for a LEI (cached). Returns (png, is_full_card)."""
    now = time.monotonic()
    cached = _OG_CACHE.get(lei)
    if cached is not None:
        ts, png, full = cached
        ttl = _OG_TTL_FULL if full else _OG_TTL_TEASER
        if now - ts < ttl:
            return png, full
    summary = _summary_from_replay(lei)
    if summary is not None:
        name, signals = summary
        png = await asyncio.to_thread(render_share_card, name, lei, signals)
        full = True
    else:
        name = await _teaser_name(lei)
        png = await asyncio.to_thread(render_share_card, name, lei, None)
        full = False
    while len(_OG_CACHE) >= _OG_MAX_ENTRIES:
        _OG_CACHE.pop(next(iter(_OG_CACHE)), None)
    _OG_CACHE[lei] = (now, png, full)
    return png, full


def invalidate_og_cache(lei: str) -> None:
    """Drop a cached card (used by tests; refresh happens via TTL)."""
    _OG_CACHE.pop(lei.strip().upper(), None)


@router.get("/og/{lei}.png")
async def og_image(lei: str) -> Response:
    """The shareable summary card for a LEI, as a 1200×630 PNG."""
    lei = _clean_lei(lei)
    png, full = await _card_for(lei)
    # Teasers cache briefly so a crawler retry after the sharer's lookup
    # completes picks up the full card; full cards can cache for longer.
    max_age = 3600 if full else 60
    return Response(
        content=png,
        media_type="image/png",
        headers={"Cache-Control": f"public, max-age={max_age}"},
    )


@router.get("/share/{lei}", response_class=HTMLResponse)
async def share_page(lei: str) -> HTMLResponse:
    """Crawler-readable share page: per-entity OG tags + instant redirect."""
    lei = _clean_lei(lei)
    settings = get_settings()
    # Never derive this from cors_origin: that is a CORS policy value and is
    # "*" on Render — not a URL. Guard against misconfiguration regardless.
    frontend = (settings.frontend_origin or "").rstrip("/")
    if not frontend.startswith("http"):
        frontend = "https://opencheck.world"
    api_base = (settings.public_api_base or "https://api.opencheck.world").rstrip("/")

    summary = _summary_from_replay(lei)
    if summary is not None:
        name, signals = summary
        description = (
            f"{len(signals)} risk signal{'s' if len(signals) != 1 else ''} · "
            "open corporate data from 34 sources · BODS v0.4"
        )
    else:
        name = await _teaser_name(lei)
        description = (
            "Live due diligence from 34 open data sources · BODS v0.4"
        )

    title = html.escape(f"{name or f'LEI {lei}'} — OpenCheck")
    target = f"{frontend}/?lei={lei}"
    image = f"{api_base}/og/{lei}.png"
    page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<meta property="og:type" content="website">
<meta property="og:site_name" content="OpenCheck">
<meta property="og:title" content="{title}">
<meta property="og:description" content="{html.escape(description)}">
<meta property="og:url" content="{html.escape(target)}">
<meta property="og:image" content="{html.escape(image)}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{title}">
<meta name="twitter:description" content="{html.escape(description)}">
<meta name="twitter:image" content="{html.escape(image)}">
<meta http-equiv="refresh" content="0;url={html.escape(target)}">
<meta name="robots" content="noindex">
</head>
<body>
<p>Redirecting to <a href="{html.escape(target)}">OpenCheck</a>…</p>
</body>
</html>"""
    return HTMLResponse(content=page, headers={"Cache-Control": "public, max-age=300"})
