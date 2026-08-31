"""GLEIF subsidiary-network reveal (lazy, never on the main lookup).

Pulls a subject's **direct + ultimate** children from GLEIF Level 2, merges them
by child LEI (tagging ``direct`` / ``ultimate`` / ``both``), and returns a
count-first summary plus — when requested — the BODS statements to render the
network. Counts are exact (from GLEIF's pagination ``total``) even when the child
fetch is capped; large networks degrade to a table + BODS export rather than a
hairball.

A child that is both a direct and an ultimate child carries two distinct
relationshipStatements (``directOrIndirect`` ``direct`` and ``indirect``); the
graph merges them into one annotated edge, but the statements stay distinct.

**GLEIF saying no must not read as "this entity has no subsidiaries."** Before
Phase 146 a 429 (the Phase 143 transport hands the last one back to its caller)
was treated as end-of-data: ``_children`` broke out of the page loop, the
summary reported ``direct_total: 0``, and the result was *cached* — so an empty
network recorded during a saturation wave was served as truth long after GLEIF
recovered. For Shell plc that is 105 direct children rendering as none, with no
notice anywhere. This module now:

* separates *refusal* from *absence* — a 404 still means "no children of this
  kind", every other failure sets ``direct_available`` / ``ultimate_available``
  to false and the response says so;
* falls back to the entity-pages Golden Copy for **direct** children (the
  snapshot the ``/entity`` pages and the anchor's ``_snapshot_bundle`` already
  use), honestly badged ``snapshot_fallback`` with the extract date — the store
  has no ultimate-children rows, so that relation stays declared-unavailable;
* **never caches a degraded result**, and refuses to read back cache entries
  written before this marker existed (they may be exactly the poisoned empties
  the 2026-08-29 wave wrote).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from . import provenance
from .bods import map_gleif_subsidiaries
from .cache import Cache
from .config import get_settings
from .http import build_client

_LOG = logging.getLogger(__name__)

_RECORD_URL = "https://api.gleif.org/api/v1/lei-records/{lei}"
_CHILDREN_URL = "https://api.gleif.org/api/v1/lei-records/{lei}/{kind}-children"

_CACHE_NS = "subsidiaries"
_PAGE_SIZE = 100
_PAGE_CAP = 10            # ≤ 1000 children fetched per relation
GRAPH_THRESHOLD = 150     # node count above which we switch graph → table

#: Cached networks expire. GLEIF Level 2 moves slowly, but an entry that never
#: expires is an entry that can be wrong forever — and this cache had no age
#: bound at all until Phase 146.
_CACHE_MAX_AGE_DAYS = 7.0

#: Written into every cached payload. Entries lacking it predate the
#: degradation marker, so their emptiness cannot be trusted to mean anything;
#: they are re-fetched rather than served.
_COMPLETE_KEY = "complete"

_cache = Cache()


async def _children(client, lei: str, kind: str) -> tuple[list[dict], int, bool]:
    """``(records, total, answered)`` of GLEIF {direct|ultimate}-children records.

    ``answered`` is the honesty bit. **404 is an answer** — GLEIF has no
    children of this kind for this LEI — so it returns ``True`` with an empty
    list. Anything else (a 429 handed back by the Phase 143 transport, the
    throttle refusing to send, a 5xx, a timeout, unparseable JSON) is GLEIF
    declining to say, and returns ``False`` along with whatever pages did
    arrive: a partial list is still worth showing, as long as the caller is
    told it is partial rather than complete.
    """
    url = _CHILDREN_URL.format(lei=quote(lei), kind=kind)
    records: list[dict] = []
    total = 0
    for page in range(1, _PAGE_CAP + 1):
        try:
            resp = await client.get(
                url, params={"page[size]": _PAGE_SIZE, "page[number]": page}
            )
        except Exception as exc:  # noqa: BLE001 — incl. GleifRateLimitedError
            _LOG.warning("subsidiaries: %s-children HTTP error: %s", kind, exc)
            return records, total, False
        if resp.status_code == 404:
            break  # a real answer: no children of this kind
        if not resp.is_success:
            _LOG.warning(
                "subsidiaries: %s-children refused with HTTP %s", kind, resp.status_code
            )
            return records, total, False
        try:
            payload = resp.json()
        except ValueError:
            _LOG.warning("subsidiaries: %s-children returned unparseable JSON", kind)
            return records, total, False
        data = payload.get("data") or []
        records.extend(d for d in data if isinstance(d, dict))
        pagination = (payload.get("meta") or {}).get("pagination") or {}
        if page == 1:
            total = int(pagination.get("total") or len(data))
        last = pagination.get("lastPage")
        if not data or (last and page >= last):
            break
    return records, total, True


async def _subject_attrs(client, lei: str) -> tuple[dict[str, Any], bool]:
    """``(attributes, answered)`` for the subject's own Level-1 record."""
    try:
        resp = await client.get(_RECORD_URL.format(lei=quote(lei)))
    except Exception:  # noqa: BLE001 — incl. GleifRateLimitedError
        return {}, False
    if resp.status_code == 404:
        return {}, True
    if not resp.is_success:
        return {}, False
    try:
        return ((((resp.json() or {}).get("data") or {}).get("attributes")) or {}), True
    except ValueError:
        return {}, False


def _snapshot_children(lei: str) -> tuple[list[dict], int, str | None] | None:
    """Direct children from the entity-pages Golden Copy, or ``None``.

    The same local snapshot the ``/entity`` pages render from and the anchor
    falls back to (Phase 143). It holds **direct** parent/child edges only, so
    this can stand in for the direct relation and never for the ultimate one —
    and its ``total`` is the snapshot's own count, not GLEIF's live total.
    """
    from .entity_pages import get_store, gleif_record_from_row

    store = get_store()
    if store is None:
        return None
    rows, total = store.children(lei, limit=_PAGE_SIZE)
    if not rows:
        # No rows is not evidence of no children here: the LEI may simply be
        # absent from the store (a trimmed build, or issued after the last
        # refresh). Declining to answer keeps `direct_available` false.
        return None
    publish = (store.meta().get("source_publish_date") or "")[:10] or None
    return [gleif_record_from_row(r) for r in rows], total, publish


async def _build(lei: str) -> dict[str, Any]:
    """Fetch the subject + merge direct/ultimate children.

    Cached per LEI **only when GLEIF answered every call**. A degraded result
    is returned to this one caller and then thrown away: caching it is how an
    empty network survives the outage that caused it.
    """
    cache_key = f"{_CACHE_NS}/{lei}"
    cached = _cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
    if cached is not None and cached[0].get(_COMPLETE_KEY) is True:
        return cached[0]

    async with build_client() as client:
        (
            (subj_attrs, subj_ok),
            (direct_recs, direct_total, direct_ok),
            (ultimate_recs, ultimate_total, ultimate_ok),
        ) = await asyncio.gather(
            _subject_attrs(client, lei),
            _children(client, lei, "direct"),
            _children(client, lei, "ultimate"),
        )

    # GLEIF would not give us the direct children — try the local Golden Copy
    # before reporting none. Only when nothing at all arrived live: a partial
    # live page is closer to the truth than a month-old snapshot.
    snapshot_date: str | None = None
    if not direct_ok and not direct_recs:
        snap = _snapshot_children(lei)
        if snap is not None:
            direct_recs, direct_total, snapshot_date = snap

    merged: dict[str, dict[str, Any]] = {}

    def add(records: list[dict], kind: str) -> None:
        for r in records:
            attrs = r.get("attributes") or r
            clei = attrs.get("lei") or r.get("id")
            if not clei:
                continue
            m = merged.get(clei)
            if m is None:
                merged[clei] = {"record": r, "relations": {kind}}
            else:
                m["relations"].add(kind)

    add(direct_recs, "direct")
    add(ultimate_recs, "ultimate")
    children = [
        {"record": m["record"], "relations": sorted(m["relations"])}
        for m in merged.values()
    ]
    complete = subj_ok and direct_ok and ultimate_ok
    result = {
        "lei": lei,
        "subject_attrs": subj_attrs,
        "direct_total": direct_total,
        "ultimate_total": ultimate_total,
        "children": children,
        # Honesty flags — the whole point of Phase 146. `direct_available` is
        # true when the snapshot stood in, because the rows are real; what they
        # are not is live, which `snapshot_date` says.
        "direct_available": direct_ok or snapshot_date is not None,
        "ultimate_available": ultimate_ok,
        "subject_available": subj_ok,
        "snapshot_date": snapshot_date,
        _COMPLETE_KEY: complete,
    }
    if complete:
        _cache.put(cache_key, result)
    return result


def _row(m: dict[str, Any]) -> dict[str, Any]:
    attrs = m["record"].get("attributes") or m["record"]
    e = attrs.get("entity") or {}
    clei = attrs.get("lei") or m["record"].get("id")
    relations = m["relations"]
    relation = "both" if set(relations) >= {"direct", "ultimate"} else relations[0]
    return {
        "lei": clei,
        "name": (e.get("legalName") or {}).get("name"),
        "jurisdiction": e.get("jurisdiction"),
        "status": e.get("status"),
        "relation": relation,
        "link": f"https://search.gleif.org/#/record/{clei}",
    }


_EMPTY = {
    "available": False, "direct_total": 0, "ultimate_total": 0,
    "distinct_fetched": 0, "indirect_only": 0, "node_estimate": 0,
    "render_mode": "graph", "truncated": False, "jurisdictions": [],
    "children": [], "bods": None,
    # Offline/demo mode is not a GLEIF refusal — the network was never asked
    # for, and `reason` says so. Declaring these available keeps the degraded
    # notice for the case it describes.
    "children_available": True, "direct_available": True,
    "ultimate_available": True, "snapshot_fallback": False,
    "snapshot_date": None, "degraded_detail": None,
}


async def assemble_subsidiaries(lei: str, *, include_bods: bool = False) -> dict[str, Any]:
    """Summary + tagged children for a subject's subsidiary network.

    ``include_bods`` additionally returns the BODS statements (subject + children
    + direct/ultimate relationships) for the graph / export.
    """
    settings = get_settings()
    if not settings.allow_live:
        return {"lei": lei, "reason": "live mode disabled", **_EMPTY}

    data = await _build(lei)
    children = data["children"]
    direct_total = data["direct_total"]
    ultimate_total = data["ultimate_total"]
    node_estimate = max(direct_total, ultimate_total, len(children))
    # Legacy cache entries (written before the flags existed) default to
    # available; `_build` refuses to serve those, so this only ever covers a
    # payload built by an older code path in the same process.
    direct_available = bool(data.get("direct_available", True))
    ultimate_available = bool(data.get("ultimate_available", True))
    snapshot_date = data.get("snapshot_date")
    snapshot_fallback = snapshot_date is not None

    rows = [_row(m) for m in children]
    jmap: dict[str, int] = {}
    for r in rows:
        jmap[r["jurisdiction"] or "—"] = jmap.get(r["jurisdiction"] or "—", 0) + 1
    jurisdictions = sorted(jmap.items(), key=lambda kv: -kv[1])[:30]

    result: dict[str, Any] = {
        "lei": lei,
        "available": bool(children) or (direct_total + ultimate_total > 0),
        "reason": None,
        # False = GLEIF refused BOTH relations and no snapshot stood in, so
        # this response is not evidence about the entity at all. True with one
        # of the two relation flags false = partial, and named as such.
        "children_available": direct_available or ultimate_available,
        "direct_available": direct_available,
        "ultimate_available": ultimate_available,
        "snapshot_fallback": snapshot_fallback,
        "snapshot_date": snapshot_date,
        "degraded_detail": _degraded_detail(
            direct_available, ultimate_available, snapshot_fallback, snapshot_date
        ),
        "direct_total": direct_total,
        "ultimate_total": ultimate_total,
        "distinct_fetched": len(children),
        "indirect_only": sum(1 for m in children if m["relations"] == ["ultimate"]),
        "node_estimate": node_estimate,
        "render_mode": "graph" if node_estimate <= GRAPH_THRESHOLD else "table",
        "truncated": len(children) < node_estimate,
        "jurisdictions": [{"code": k, "count": v} for k, v in jurisdictions],
        "children": rows,
        "bods": None,
    }
    if include_bods:
        # Snapshot rows must not be exported as if they had come off the live
        # API — the mapper reads the active mapping provenance for every
        # statement's source block.
        if snapshot_fallback:
            with provenance.mapping_provenance(
                provenance.Provenance(
                    liveness="snapshot",
                    retrieved_at=_snapshot_datetime(snapshot_date),
                    detail="GLEIF Golden Copy snapshot (live API rate-limited)",
                )
            ):
                result["bods"] = map_gleif_subsidiaries(
                    lei, data["subject_attrs"], children
                )
        else:
            result["bods"] = map_gleif_subsidiaries(lei, data["subject_attrs"], children)
    return result


def _snapshot_datetime(publish: str | None) -> datetime | None:
    if not publish:
        return None
    try:
        return datetime.strptime(publish, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _degraded_detail(
    direct_available: bool,
    ultimate_available: bool,
    snapshot_fallback: bool,
    snapshot_date: str | None,
) -> str | None:
    """One sentence naming what GLEIF did not answer. ``None`` when it did.

    Written here rather than in the frontend because the backend is the only
    layer that knows *which* of the two relation calls was refused, and
    "we could not check" has to be specific to be worth more than silence.
    """
    if snapshot_fallback:
        dated = f" (extract of {snapshot_date})" if snapshot_date else ""
        if not ultimate_available:
            return (
                "GLEIF is rate-limiting or unreachable. Direct children are "
                f"shown from OpenCheck's Golden Copy snapshot{dated}; the "
                "ultimate (indirect) children could not be checked at all."
            )
        return (
            "GLEIF did not return the direct children, so they are shown from "
            f"OpenCheck's Golden Copy snapshot{dated} rather than live."
        )
    if not direct_available and not ultimate_available:
        return (
            "GLEIF is rate-limiting or unreachable, so the subsidiary network "
            "could not be checked. This is not a finding that the entity has none."
        )
    if not direct_available:
        return (
            "GLEIF did not return the direct children — only the ultimate "
            "(indirect) ones are shown, so this network is incomplete."
        )
    if not ultimate_available:
        return (
            "GLEIF did not return the ultimate (indirect) children — only the "
            "direct ones are shown, so this network is incomplete."
        )
    return None


__all__ = ["assemble_subsidiaries", "GRAPH_THRESHOLD"]
