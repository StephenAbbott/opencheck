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
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import quote

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

_cache = Cache()


async def _children(client, lei: str, kind: str) -> tuple[list[dict], int]:
    """Return ``(records, total)`` of GLEIF {direct|ultimate}-children L1 records."""
    url = _CHILDREN_URL.format(lei=quote(lei), kind=kind)
    records: list[dict] = []
    total = 0
    for page in range(1, _PAGE_CAP + 1):
        try:
            resp = await client.get(
                url, params={"page[size]": _PAGE_SIZE, "page[number]": page}
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("subsidiaries: %s-children HTTP error: %s", kind, exc)
            break
        if resp.status_code == 404 or not resp.is_success:
            break
        try:
            payload = resp.json()
        except ValueError:
            break
        data = payload.get("data") or []
        records.extend(d for d in data if isinstance(d, dict))
        pagination = (payload.get("meta") or {}).get("pagination") or {}
        if page == 1:
            total = int(pagination.get("total") or len(data))
        last = pagination.get("lastPage")
        if not data or (last and page >= last):
            break
    return records, total


async def _subject_attrs(client, lei: str) -> dict[str, Any]:
    try:
        resp = await client.get(_RECORD_URL.format(lei=quote(lei)))
    except Exception:  # noqa: BLE001
        return {}
    if not resp.is_success:
        return {}
    try:
        return (((resp.json() or {}).get("data") or {}).get("attributes")) or {}
    except ValueError:
        return {}


async def _build(lei: str) -> dict[str, Any]:
    """Fetch the subject + merge direct/ultimate children. Cached per LEI."""
    cache_key = f"{_CACHE_NS}/{lei}"
    cached = _cache.get_payload(cache_key)
    if cached is not None:
        return cached[0]

    async with build_client() as client:
        subj_attrs, (direct_recs, direct_total), (ultimate_recs, ultimate_total) = (
            await asyncio.gather(
                _subject_attrs(client, lei),
                _children(client, lei, "direct"),
                _children(client, lei, "ultimate"),
            )
        )

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
    result = {
        "lei": lei,
        "subject_attrs": subj_attrs,
        "direct_total": direct_total,
        "ultimate_total": ultimate_total,
        "children": children,
    }
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

    rows = [_row(m) for m in children]
    jmap: dict[str, int] = {}
    for r in rows:
        jmap[r["jurisdiction"] or "—"] = jmap.get(r["jurisdiction"] or "—", 0) + 1
    jurisdictions = sorted(jmap.items(), key=lambda kv: -kv[1])[:30]

    result: dict[str, Any] = {
        "lei": lei,
        "available": bool(children) or (direct_total + ultimate_total > 0),
        "reason": None,
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
        result["bods"] = map_gleif_subsidiaries(lei, data["subject_attrs"], children)
    return result


__all__ = ["assemble_subsidiaries", "GRAPH_THRESHOLD"]
