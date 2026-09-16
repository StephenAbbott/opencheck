"""Routes for the watchlist (Phase 215): ``/watch``.

A list is a capability: whoever holds the token can read, extend and
re-check it. There is no account behind it, so there is nothing to log in
to, reset or leak — the token is shown once, kept by the browser, and stored
here only as its hash (see ``opencheck/watchlist.py``).

- ``POST /watch/items``            add an LEI (creates a list when no token is sent)
- ``GET /watch/{token}``           the list: watched entities, baselines, the log
- ``DELETE /watch/{token}/items/{lei}``
- ``POST /watch/{token}/recheck``  re-run one entity now — a deliberate human
                                   action, on the heavy tier; not a schedule
- ``GET /watch/{token}.atom``      the Atom feed a reader subscribes to
- ``GET /watchstats``              aggregate state of the watcher, no LEIs

``/watch`` is disallowed in robots.txt: a crawler that found a token in the
wild would otherwise re-check on every visit.
"""

from __future__ import annotations

from typing import Any
from xml.sax.saxutils import escape

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .. import watchlist as wl
from ..config import get_settings
from ..ratelimit import default_tier, heavy_tier, limiter

router = APIRouter()


class AddItem(BaseModel):
    lei: str = Field(..., min_length=20, max_length=40)
    token: str | None = Field(default=None, max_length=128)


class RecheckItem(BaseModel):
    lei: str = Field(..., min_length=20, max_length=40)


def _store() -> wl.WatchlistStore:
    store = wl.get_store()
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="The watchlist is not enabled on this instance (OPENCHECK_WATCHLIST_DB_FILE is unset).",
        )
    return store


def _list_or_404(store: wl.WatchlistStore, token: str) -> str:
    th = wl.token_hash(token)
    if not store.list_exists(th):
        raise HTTPException(status_code=404, detail="No watchlist with that token.")
    store.touch(th)
    return th


def _lei_or_400(raw: str) -> str:
    lei = wl.valid_lei(raw)
    if lei is None:
        raise HTTPException(status_code=400, detail="That is not a valid LEI (20 characters, ISO 17442 check digits).")
    return lei


def _public_api_base(request: Request) -> str:
    base = (get_settings().public_api_base or "").rstrip("/")
    return base or str(request.base_url).rstrip("/")


def _tiers() -> dict[str, Any]:
    """What the two tiers are doing — for the page's honesty line."""
    from .. import entity_pages as ep
    from .. import mirror_refresh

    mirror = ep.get_store()
    watermark = mirror.watermark() if mirror is not None else None
    mirror_state = mirror_refresh.state()
    settings = get_settings()
    return {
        "gleif": {
            "available": mirror is not None and bool(getattr(mirror, "is_mirror", False)),
            "watermark": watermark.strftime("%Y-%m-%d %H:%M:%S") if watermark else None,
            "refresh_enabled": mirror_state.get("enabled", False),
            "last_applied_at": mirror_state.get("last_applied_at"),
            "last_delta": mirror_state.get("last_delta"),
            "rows_applied": mirror_state.get("rows_applied") or {},
            "record_count": mirror.count() if mirror is not None else None,
        },
        "opensanctions": {
            "available": settings.watchlist_opensanctions_interval_s > 0,
            "last_version": wl.state().get("os_last_version"),
            "last_checked_at": wl.state().get("os_last_checked_at"),
        },
        "worker": {
            "enabled": wl.state().get("enabled", False),
            "interval_s": settings.watchlist_interval_s,
        },
    }


def _list_payload(store: wl.WatchlistStore, th: str, token: str, request: Request) -> dict[str, Any]:
    watches = store.watches(th)
    for w in watches:
        # The page shows the baseline's coarse facts, not the whole snapshot.
        snap = w.pop("snapshot", None) or {}
        w["baseline"] = {
            "register_status": snap.get("register_status"),
            "risk_codes": [s["code"] for s in snap.get("signals") or [] if s.get("kind") == "risk"],
            "context_codes": [s["code"] for s in snap.get("signals") or [] if s.get("kind") == "context"],
            "coverage": snap.get("coverage"),
            "verdict": snap.get("verdict"),
            "checked": snap.get("checked") or [],
            "degraded_sources": [d.get("source_id") for d in snap.get("degraded_sources") or []],
        }
    counts = store.counts(th)
    return {
        "watches": watches,
        "entries": store.entries(th),
        "caps": {
            "per_list": store.caps.max_per_list,
            "total": store.caps.max_total,
            "in_list": counts["in_list"],
            "total_watched": counts["total"],
        },
        "feed_url": f"{_public_api_base(request)}/watch/{token}.atom",
        "tiers": _tiers(),
    }


# Every handler under a @limiter.limit that returns a dict MUST take
# ``response: Response``: slowapi writes the X-RateLimit-* headers into it and
# raises ("parameter `response` must be an instance of Response") without
# it. The test suite runs with the limiter off, so only a limiter-on test
# (test_watchlist.py) or production sees the 500 — which is how the first
# deploy found it on /recheck and DELETE.
@router.post("/watch/items", status_code=201)
@limiter.limit(default_tier)
async def add_item(request: Request, response: Response, body: AddItem) -> dict[str, Any]:
    """Watch an LEI. With no token, a new list is created and its token
    returned — the one time it is. The baseline is the lookup as it stands
    (replayed when the reader has just run it, so usually free) plus the
    mirror's material GLEIF fields."""
    store = _store()
    lei = _lei_or_400(body.lei)
    if body.token:
        th = _list_or_404(store, body.token)
        token = body.token
    else:
        token = store.create_list()
        th = wl.token_hash(token)
    resp, facts, watermark = await wl.baseline(lei)
    snapshot = wl.snapshot_from_response(resp)
    try:
        watch = store.add_watch(
            th,
            lei,
            legal_name=resp.legal_name,
            jurisdiction=resp.jurisdiction,
            gleif_facts=facts,
            gleif_watermark=watermark,
            snapshot=snapshot,
        )
    except wl.CapExceededError as exc:
        which = "this list" if exc.which == "per_list" else "this instance"
        raise HTTPException(
            status_code=409,
            detail=f"The cap of {exc.cap} watched companies for {which} has been reached.",
        ) from exc
    watch.pop("snapshot", None)
    return {"token": token, "watch": watch, **_list_payload(store, th, token, request)}


@router.get("/watch/{token}.atom")
@limiter.limit(default_tier)
async def feed(request: Request, token: str) -> Response:
    store = _store()
    th = _list_or_404(store, token)
    entries = store.entries(th, limit=50)
    watches = store.watches(th)
    xml = render_atom(
        feed_id=f"urn:opencheck:watchlist:{th[:24]}",
        self_url=f"{_public_api_base(request)}/watch/{token}.atom",
        page_url=f"{_frontend_origin()}/watchlist?token={token}",
        title="OpenCheck watchlist",
        watches=watches,
        entries=entries,
    )
    return Response(
        content=xml,
        media_type="application/atom+xml; charset=utf-8",
        headers={"Cache-Control": "private, no-store", "X-Robots-Tag": "noindex"},
    )


@router.get("/watch/{token}")
@limiter.limit(default_tier)
async def get_list(request: Request, token: str) -> JSONResponse:
    store = _store()
    th = _list_or_404(store, token)
    return JSONResponse(
        _list_payload(store, th, token, request), headers={"Cache-Control": "private, no-store"}
    )


@router.delete("/watch/{token}/items/{lei}")
@limiter.limit(default_tier)
async def remove_item(request: Request, response: Response, token: str, lei: str) -> dict[str, Any]:
    store = _store()
    th = _list_or_404(store, token)
    norm = _lei_or_400(lei)
    if not store.remove_watch(th, norm):
        raise HTTPException(status_code=404, detail="That LEI is not on this watchlist.")
    return _list_payload(store, th, token, request)


@router.post("/watch/{token}/recheck")
@limiter.limit(heavy_tier)
async def recheck(request: Request, response: Response, token: str, body: RecheckItem) -> dict[str, Any]:
    """Re-check one watched entity now. A deliberate human action, so it
    bypasses the replay cache; the heavy tier bounds it."""
    store = _store()
    th = _list_or_404(store, token)
    lei = _lei_or_400(body.lei)
    if store.get_watch(th, lei) is None:
        raise HTTPException(status_code=404, detail="That LEI is not on this watchlist.")
    result = await wl.rerun(lei, wl.TIER_MANUAL, {"tier": wl.TIER_MANUAL}, only_token_hash=th)
    if result.get("error"):
        raise HTTPException(status_code=503, detail="The re-check could not run; the baseline is unchanged.")
    result.pop("snapshot", None)
    return {"result": result, **_list_payload(store, th, token, request)}


@router.get("/watchstats")
async def watch_stats() -> JSONResponse:
    """The watcher's aggregate state — deltas seen, watched LEIs a delta
    named, renewal churn filtered, re-runs, entries. Same contract as
    ``/mirror``: public, no LEI or name can appear."""
    counts = {"total": 0, "distinct_leis": 0, "pending": 0}
    store = wl.get_store()
    if store is not None:
        c = store.counts()
        counts = {"total": c["total"], "distinct_leis": c["distinct_leis"], "pending": store.pending_count()}
    return JSONResponse(
        {"enabled": store is not None, "watched": counts, "watcher": wl.state()},
        headers={"Cache-Control": "no-store"},
    )


# ---------------------------------------------------------------------------
# Atom
# ---------------------------------------------------------------------------


def _frontend_origin() -> str:
    frontend = (get_settings().frontend_origin or "").rstrip("/")
    return frontend if frontend.startswith("http") else "https://opencheck.world"


def _atom_time(iso: str | None) -> str:
    return iso or "1970-01-01T00:00:00Z"


TIER_SENTENCE = {
    wl.TIER_GLEIF: "GLEIF published a change to this record",
    wl.TIER_OPENSANCTIONS: "OpenSanctions published a change naming this entity",
    wl.TIER_MANUAL: "Re-checked on request",
}


def _describe(change: dict[str, Any]) -> str:
    """One sentence per change, in the feed's own words. The web page has
    its own wording (``lib/watchlist.ts``); both read the same kinds."""
    k = change.get("kind")
    old, new = change.get("old"), change.get("new")
    if k == "gleif_field":
        return f"GLEIF {change.get('field', '').replace('_', ' ')}: {old or '—'} → {new or '—'}."
    if k == "register_status":
        o = (old or {}).get("liveness") if isinstance(old, dict) else old
        n = (new or {}).get("liveness") if isinstance(new, dict) else new
        src = (new or {}).get("source_id") if isinstance(new, dict) else None
        return f"Register status: {o or '—'} → {n or '—'}" + (f" ({src})." if src else ".")
    if k in ("legal_name", "jurisdiction", "founding_date", "legal_form", "dissolution_date"):
        return f"{k.replace('_', ' ').capitalize()}: {old or '—'} → {new or '—'}."
    if k == "identifier":
        return f"Identifier {change.get('scheme')}: {old} → {new}."
    if k in ("signal_new", "context_new"):
        return f"New {'signal' if k == 'signal_new' else 'context'}: {change.get('code')} ({', '.join(change.get('sources') or [])})."
    if k in ("signal_retired", "context_retired"):
        return f"No longer reported: {change.get('code')} — the source that reported it answered."
    if k in ("signal_unchecked", "context_unchecked"):
        return f"Could not re-check: {change.get('code')} — {', '.join(change.get('degraded') or change.get('sources') or [])} did not answer."
    if k == "coverage_changed":
        return f"Coverage: {(old or {}).get('answered')} of {(old or {}).get('applicable')} → {(new or {}).get('answered')} of {(new or {}).get('applicable')} sources answered."
    if k == "coverage_unchecked":
        return f"Coverage fell to {(new or {}).get('answered')} of {(new or {}).get('applicable')} because sources could not be reached — not a fact about the company."
    if k == "verdict":
        return f"Verdict: {new}"
    return f"{k}."


def _entry_title(entry: dict[str, Any]) -> str:
    name = entry.get("legal_name") or entry["lei"]
    changes = entry.get("changes") or []
    if not changes:
        return f"{name}: {TIER_SENTENCE.get(entry['tier'], entry['tier'])}; the re-run found no difference"
    kinds = [c.get("kind") for c in changes]
    if any(k in ("register_status", "dissolution_date") for k in kinds):
        head = "register status changed"
    elif any(k == "signal_new" for k in kinds):
        head = "new risk signal"
    elif any(k == "gleif_field" for k in kinds):
        head = "GLEIF record changed"
    else:
        head = f"{len(changes)} change{'s' if len(changes) != 1 else ''}"
    return f"{name}: {head}"


def _entry_content(entry: dict[str, Any]) -> str:
    lines = [TIER_SENTENCE.get(entry["tier"], entry["tier"]) + "."]
    trig = entry.get("trigger") or {}
    if entry["tier"] == wl.TIER_GLEIF and trig.get("publish"):
        lines[0] = f"GLEIF published a change to this record in the {trig['publish']} Golden Copy delta."
        if trig.get("fields"):
            lines.append("Fields that changed: " + ", ".join(f.replace("_", " ") for f in trig["fields"]) + ".")
    if entry["tier"] == wl.TIER_OPENSANCTIONS:
        lines[0] = (
            f"OpenSanctions version {trig.get('version')} {str(trig.get('op') or 'changed').lower()}ed an entity "
            f"({trig.get('caption') or trig.get('entity_id')}) matching this company by {trig.get('matched_on')}"
            + (f"; datasets: {', '.join(trig.get('datasets') or [])}" if trig.get("datasets") else "")
            + "."
        )
    changes = entry.get("changes") or []
    if changes:
        lines.append("What a re-run found:")
        lines += [" - " + _describe(c) for c in changes]
    else:
        lines.append("A full re-run found no difference from the last check.")
    checked = entry.get("checked") or []
    if checked:
        reached = [c for c in checked if c.get("liveness") in ("live", "cached", "snapshot", "curated")]
        dates = sorted({(c.get("retrieved_at") or "")[:10] for c in reached if c.get("retrieved_at")})
        lines.append(
            f"{len(reached)} source{'s' if len(reached) != 1 else ''} checked as a result"
            + (f" (retrieved {', '.join(d for d in dates if d)})" if dates else "")
            + "."
        )
    degraded = entry.get("degraded") or []
    if degraded:
        lines.append(
            "Could not check: "
            + ", ".join(sorted({d.get("source_id") for d in degraded if d.get("source_id")}))
            + " — absence of a finding there is not a clean result."
        )
    return "\n".join(lines)


def render_atom(
    *,
    feed_id: str,
    self_url: str,
    page_url: str,
    title: str,
    watches: list[dict[str, Any]],
    entries: list[dict[str, Any]],
) -> str:
    updated = max([e["created_at"] for e in entries] + [w["added_at"] for w in watches], default=None)
    parts = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        f"  <title>{escape(title)}</title>",
        f"  <id>{escape(feed_id)}</id>",
        f'  <link rel="self" type="application/atom+xml" href="{escape(self_url, {chr(34): "&quot;"})}"/>',
        f'  <link rel="alternate" type="text/html" href="{escape(page_url, {chr(34): "&quot;"})}"/>',
        f"  <updated>{_atom_time(updated)}</updated>",
        "  <author><name>OpenCheck</name></author>",
        f"  <subtitle>{escape(_subtitle(watches))}</subtitle>",
        "  <generator>OpenCheck watchlist</generator>",
    ]
    frontend = _frontend_origin()
    for e in entries:
        link = f"{frontend}/?lei={e['lei']}"
        parts += [
            "  <entry>",
            f"    <id>urn:opencheck:watch-entry:{e['id']}</id>",
            f"    <title>{escape(_entry_title(e))}</title>",
            f'    <link rel="alternate" type="text/html" href="{escape(link, {chr(34): "&quot;"})}"/>',
            f"    <updated>{_atom_time(e['created_at'])}</updated>",
            f"    <published>{_atom_time(e['created_at'])}</published>",
            f'    <category term="{escape(e["tier"], {chr(34): "&quot;"})}"/>',
            f'    <content type="text">{escape(_entry_content(e))}</content>',
            "  </entry>",
        ]
    parts.append("</feed>")
    return "\n".join(parts) + "\n"


def _subtitle(watches: list[dict[str, Any]]) -> str:
    n = len(watches)
    return (
        f"{n} watched {'company' if n == 1 else 'companies'}. Re-checked only when GLEIF or "
        "OpenSanctions publish a change to them — OpenCheck does not poll the registers."
    )
