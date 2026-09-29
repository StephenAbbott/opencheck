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

Phase 179 adds the other order: with ``OPENCHECK_GLEIF_MIRROR_FIRST`` on and a
Phase 178 mirror that holds the subject, the whole network — subject record,
direct **and** ultimate children, up to the same cap the live path applies —
is read from the mirror and GLEIF is not called. It is badged ``snapshot``
with the mirror's watermark and ``snapshot_source: "mirror"`` so the notice
says the snapshot was chosen, not forced. A subject the mirror lacks takes the
live path above unchanged. Mirror-served networks are not written to the
response cache: the mirror *is* the cache, and a 7-day copy would outlive the
mirror's own refresh.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from . import lei_registration, mirrorstats, provenance
from .bods import map_gleif_subsidiaries
from .bods.mapper import _gleif_rr_period
from .cache import Cache
from .config import get_settings
from .gleif_throttle import UNAVAILABLE_UNREACHABLE, unavailable_reason
from .http import build_client
from .sources.gleif import SNAPSHOT_DETAIL

_LOG = logging.getLogger(__name__)

_RECORD_URL = "https://api.gleif.org/api/v1/lei-records/{lei}"
_CHILDREN_URL = "https://api.gleif.org/api/v1/lei-records/{lei}/{kind}-children"
#: Phase 255: the Level 2 relationship records behind the children — the only
#: place GLEIF publishes a relationship's period and its own update date.
_CHILD_RELS_URL = (
    "https://api.gleif.org/api/v1/lei-records/{lei}/{kind}-child-relationships"
)
_PARENT_REL_URL = (
    "https://api.gleif.org/api/v1/lei-records/{lei}/direct-parent-relationship"
)

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

#: Phase 255: the payload shape. Entries written before relationship records
#: and direct parents were read carry none of them, so they are rebuilt
#: rather than served — they would export the old flat, undated network.
_SHAPE_KEY = "shape"
_SHAPE = 2

#: Phase 255: whether the relationship records and the direct-parent lookups
#: all answered. A complete but unenriched network is cached (the children
#: are the network), but only trusted for :data:`_UNENRICHED_MAX_AGE_DAYS`.
_ENRICHED_KEY = "enriched"
_UNENRICHED_MAX_AGE_DAYS = 1 / 24

#: Phase 255: at most this many live ``/direct-parent-relationship`` calls per
#: network, for ultimate-only children the Golden Copy could not place. The
#: calls are discretionary (the caller's ``gleif_discretionary()`` scope), and
#: a child left unplaced keeps its indirect edge to the head — nothing is lost
#: but the intermediate hop.
_PARENT_LOOKUP_CAP = 25
_PARENT_LOOKUP_CONCURRENCY = 4

_cache = Cache()


async def _children(
    client, lei: str, kind: str
) -> tuple[list[dict], int, bool, str | None]:
    """``(records, total, answered, reason)`` of GLEIF {direct|ultimate}-children
    records. ``reason`` (Phase 258) says why ``answered`` is false —
    ``held_for_lookups`` / ``rate_limited`` / ``unreachable``; ``None`` when
    GLEIF answered.

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
            return records, total, False, unavailable_reason(exc)
        if resp.status_code == 404:
            break  # a real answer: no children of this kind
        if not resp.is_success:
            _LOG.warning(
                "subsidiaries: %s-children refused with HTTP %s", kind, resp.status_code
            )
            return records, total, False, unavailable_reason(status=resp.status_code)
        try:
            payload = resp.json()
        except ValueError:
            _LOG.warning("subsidiaries: %s-children returned unparseable JSON", kind)
            return records, total, False, UNAVAILABLE_UNREACHABLE
        data = payload.get("data") or []
        records.extend(d for d in data if isinstance(d, dict))
        pagination = (payload.get("meta") or {}).get("pagination") or {}
        if page == 1:
            total = int(pagination.get("total") or len(data))
        last = pagination.get("lastPage")
        if not data or (last and page >= last):
            break
    return records, total, True, None


async def _child_relationships(
    client, lei: str, kind: str
) -> tuple[dict[str, dict], bool]:
    """``({child_lei: relationship record}, answered)`` for one relation kind.

    Phase 255. The children endpoints return each child's Level 1 record,
    which says nothing about the relationship itself; these return the RR
    records — ``RELATIONSHIP_PERIOD`` and the relationship's own
    ``registration.lastUpdateDate``. Paged exactly as :func:`_children`. A
    refusal costs the dates, never the children: callers leave the
    ``*_available`` flags alone and only decline to cache.
    """
    url = _CHILD_RELS_URL.format(lei=quote(lei), kind=kind)
    out: dict[str, dict] = {}
    for page in range(1, _PAGE_CAP + 1):
        try:
            resp = await client.get(
                url, params={"page[size]": _PAGE_SIZE, "page[number]": page}
            )
        except Exception as exc:  # noqa: BLE001 — incl. GleifRateLimitedError
            _LOG.warning("subsidiaries: %s-child-relationships error: %s", kind, exc)
            return out, False
        if resp.status_code == 404:
            break
        if not resp.is_success:
            return out, False
        try:
            payload = resp.json() or {}
        except ValueError:
            return out, False
        data = payload.get("data") or []
        for d in data:
            child = _rr_start_lei(d)
            if child:
                out[child] = d
        last = ((payload.get("meta") or {}).get("pagination") or {}).get("lastPage")
        if not data or not last or page >= last:
            break
    return out, True


def _rr_start_lei(rr: dict | None) -> str | None:
    """The child (start node) LEI of a GLEIF relationship record."""
    if not isinstance(rr, dict):
        return None
    rel = (rr.get("attributes") or {}).get("relationship") or {}
    return ((rel.get("startNode") or {}).get("id")) or None


def _rr_end_lei(rr: dict | None) -> str | None:
    """The parent (end node) LEI of a GLEIF relationship record."""
    if not isinstance(rr, dict):
        return None
    rel = (rr.get("attributes") or {}).get("relationship") or {}
    return ((rel.get("endNode") or {}).get("id")) or None


def _store_direct_parents(leis: list[str]) -> dict[str, dict]:
    """``{child_lei: direct-parent relationship record}`` from the Golden Copy.

    The store keeps ``direct_parent_lei`` on every entity row and, from Phase
    178, the RR row with its period. Where only the parent LEI is held, a bare
    relationship record naming it is returned — the edge is known, the dates
    are not.
    """
    if not leis:
        return {}
    from .entity_pages import get_store, gleif_relationship_record

    store = get_store()
    if store is None:
        return {}
    try:
        rows = store.get_many(leis)
    except Exception:  # noqa: BLE001 — a store read never fails the network
        return {}
    out: dict[str, dict] = {}
    for lei, row in rows.items():
        parent = row.direct_parent_lei
        if not parent:
            continue
        rr = None
        try:
            held = store.relationship(lei, "direct")
        except Exception:  # noqa: BLE001
            held = None
        if held is not None and held.parent_lei == parent:
            rr = gleif_relationship_record(held)
        out[lei] = rr or {
            "type": "relationship-records",
            "attributes": {"relationship": {
                "startNode": {"id": lei, "type": "LEI"},
                "endNode": {"id": parent, "type": "LEI"},
                "type": "IS_DIRECTLY_CONSOLIDATED_BY",
            }},
        }
    return out


async def _live_direct_parents(client, leis: list[str]) -> tuple[dict[str, dict], bool]:
    """``({child_lei: RR record}, answered)`` from GLEIF's
    ``/direct-parent-relationship``, for at most :data:`_PARENT_LOOKUP_CAP`
    children. A 404 is an answer (the child files a reporting exception
    instead of a parent); anything else is a refusal, reported so the result
    is not cached."""
    sem = asyncio.Semaphore(_PARENT_LOOKUP_CONCURRENCY)
    answered = True

    async def one(child: str) -> tuple[str, dict | None]:
        nonlocal answered
        async with sem:
            # Once GLEIF (or the discretionary budget) has said no, stop
            # asking: every further call would be refused the same way.
            if not answered:
                return child, None
            try:
                resp = await client.get(_PARENT_REL_URL.format(lei=quote(child)))
            except Exception:  # noqa: BLE001 — incl. GleifRateLimitedError
                answered = False
                return child, None
        if resp.status_code == 404:
            return child, None
        if not resp.is_success:
            answered = False
            return child, None
        try:
            data = (resp.json() or {}).get("data")
        except ValueError:
            answered = False
            return child, None
        return child, data if isinstance(data, dict) and _rr_end_lei(data) else None

    wanted = leis[:_PARENT_LOOKUP_CAP]
    results = await asyncio.gather(*(one(c) for c in wanted))
    # The rest were never asked: the network is not enriched, so it is
    # rebuilt after an hour rather than kept a week.
    return {c: rr for c, rr in results if rr}, answered and len(leis) <= len(wanted)


def _attach_relationships(
    children: list[dict[str, Any]],
    direct_rels: dict[str, dict],
    ultimate_rels: dict[str, dict],
) -> None:
    """Put each child's relationship records on it (in place)."""
    for m in children:
        clei = _child_lei(m)
        rels: dict[str, dict] = {}
        if clei in direct_rels and "direct" in m["relations"]:
            rels["direct"] = direct_rels[clei]
        if clei in ultimate_rels and "ultimate" in m["relations"]:
            rels["ultimate"] = ultimate_rels[clei]
        m["rels"] = rels


def _child_lei(m: dict[str, Any]) -> str:
    rec = m.get("record") or {}
    attrs = rec.get("attributes") or rec
    return attrs.get("lei") or rec.get("id") or ""


def _ultimate_only(children: list[dict[str, Any]]) -> list[str]:
    return [_child_lei(m) for m in children if m["relations"] == ["ultimate"]]


def _attach_direct_parents(
    children: list[dict[str, Any]], parents: dict[str, dict]
) -> None:
    for m in children:
        rr = parents.get(_child_lei(m))
        if rr is not None and m["relations"] == ["ultimate"]:
            m["direct_parent"] = {"lei": _rr_end_lei(rr), "rel": rr}


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


def _snapshot_children(
    lei: str, kind: str = "direct", *, limit: int = _PAGE_SIZE
) -> tuple[list[dict], int, str | None] | None:
    """Children of one relation kind from the entity-pages Golden Copy, or ``None``.

    The same local snapshot the ``/entity`` pages render from and the anchor
    falls back to (Phase 143). Phase 146 could only stand in for the direct
    relation; Phase 178 indexed ``ultimate_parent_lei`` too, so either kind can
    be served — its ``total`` is the snapshot's own count, not GLEIF's live
    total, and the extract date says how old it is.
    """
    from .entity_pages import get_store, gleif_record_from_row

    store = get_store()
    if store is None:
        return None
    rows, total = store.children(lei, limit=limit, kind=kind)
    if not rows:
        # No rows is not evidence of no children here: the LEI may simply be
        # absent from the store (a trimmed build, or issued after the last
        # refresh). Declining to answer keeps `{kind}_available` false.
        return None
    publish = (store.meta().get("source_publish_date") or "")[:10] or None
    return [gleif_record_from_row(r) for r in rows], total, publish


async def _build(lei: str) -> dict[str, Any]:
    """Fetch the subject + merge direct/ultimate children.

    Cached per LEI **only when GLEIF answered every call**. A degraded result
    is returned to this one caller and then thrown away: caching it is how an
    empty network survives the outage that caused it.
    """
    if get_settings().gleif_mirror_first:
        mirrored = _mirror_network(lei)
        if mirrored is not None:
            return mirrored

    cache_key = f"{_CACHE_NS}/{lei}"
    cached = _cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
    if (
        cached is not None
        and cached[0].get(_COMPLETE_KEY) is True
        and cached[0].get(_SHAPE_KEY) == _SHAPE
        and (
            cached[0].get(_ENRICHED_KEY) is True
            # An unenriched network (relationship records or direct parents
            # refused) is served for an hour, then rebuilt — long enough not
            # to re-spend the calls on every request, short enough that the
            # dates arrive once GLEIF has room.
            or _cache.get_payload(cache_key, max_age_days=_UNENRICHED_MAX_AGE_DAYS)
            is not None
        )
    ):
        return cached[0]

    async with build_client() as client:
        # The children come first and alone: they are the network, and a
        # Phase 234 discretionary budget spent on dates first could leave
        # nothing for them.
        (
            (subj_attrs, subj_ok),
            (direct_recs, direct_total, direct_ok, direct_reason),
            (ultimate_recs, ultimate_total, ultimate_ok, ultimate_reason),
        ) = await asyncio.gather(
            _subject_attrs(client, lei),
            _children(client, lei, "direct"),
            _children(client, lei, "ultimate"),
        )
        # Phase 255: the relationship records behind them (dates), then the
        # direct parents of the ultimate-only children (the path). Both are
        # enrichment: refusal leaves the network whole and undated or flat.
        (direct_rels, direct_rels_ok), (ultimate_rels, ultimate_rels_ok) = (
            await asyncio.gather(
                _child_relationships(client, lei, "direct"),
                _child_relationships(client, lei, "ultimate"),
            )
            if (direct_recs or ultimate_recs)
            else (({}, True), ({}, True))
        )
        merged = _merge_children(direct_recs, ultimate_recs)
        unplaced = _ultimate_only(merged)
        parents = _store_direct_parents(unplaced)
        remaining = [c for c in unplaced if c not in parents]
        parents_ok = True
        # Only when the direct list is whole: with it refused, every "both"
        # child reads as ultimate-only and the calls would all be wasted.
        if remaining and direct_ok:
            live_parents, parents_ok = await _live_direct_parents(client, remaining)
            parents.update(live_parents)

    # GLEIF would not give us the direct children — try the local Golden Copy
    # before reporting none. Only when nothing at all arrived live: a partial
    # live page is closer to the truth than a month-old snapshot.
    snapshot_date: str | None = None
    direct_from_snapshot = False
    if not direct_ok and not direct_recs:
        snap = _snapshot_children(lei, "direct")
        if snap is not None:
            direct_recs, direct_total, snapshot_date = snap
            direct_from_snapshot = True
    # Phase 178: the same stand-in for the ultimate relation, which Phase 146
    # had to declare unavailable because the store had no ultimate index.
    ultimate_from_snapshot = False
    if not ultimate_ok and not ultimate_recs:
        snap = _snapshot_children(lei, "ultimate")
        if snap is not None:
            ultimate_recs, ultimate_total, ultimate_snapshot_date = snap
            ultimate_from_snapshot = True
            snapshot_date = snapshot_date or ultimate_snapshot_date

    children = _merge_children(direct_recs, ultimate_recs)
    _attach_relationships(children, direct_rels, ultimate_rels)
    # A snapshot stand-in can change which children are ultimate-only; place
    # any the live pass did not ask about from the store as well.
    late = [c for c in _ultimate_only(children) if c not in parents]
    if late:
        parents.update(_store_direct_parents(late))
    _attach_direct_parents(children, parents)
    complete = subj_ok and direct_ok and ultimate_ok
    enriched = direct_rels_ok and ultimate_rels_ok and parents_ok
    result = {
        "lei": lei,
        "subject_attrs": subj_attrs,
        "direct_total": direct_total,
        "ultimate_total": ultimate_total,
        "children": children,
        # Honesty flags — the whole point of Phase 146. `direct_available` is
        # true when the snapshot stood in, because the rows are real; what they
        # are not is live, which `snapshot_date` says.
        "direct_available": direct_ok or direct_from_snapshot,
        "ultimate_available": ultimate_ok or ultimate_from_snapshot,
        "subject_available": subj_ok,
        "snapshot_date": snapshot_date,
        "snapshot_source": "fallback" if snapshot_date else None,
        # Phase 258: why a relation was not answered, for the sentence.
        "unavailable_reason": direct_reason or ultimate_reason,
        _COMPLETE_KEY: complete,
        _SHAPE_KEY: _SHAPE,
        _ENRICHED_KEY: enriched,
    }
    if complete:
        _cache.put(cache_key, result)
    return result


def _mirror_network(lei: str) -> dict[str, Any] | None:
    """Phase 179: the whole network from the mirror, or ``None`` when the
    configured file is not a mirror or does not hold the subject (both
    counted, so the miss rate is a number on ``/mirror``)."""
    from .entity_pages import get_store, gleif_record_from_row

    store = get_store()
    if store is None or not store.is_mirror:
        mirrorstats.record("subsidiaries.no_mirror")
        return None
    row = store.get(lei)
    if row is None:
        mirrorstats.record("subsidiaries.miss_live")
        return None
    mirrorstats.record("subsidiaries.mirror")
    cap = _PAGE_SIZE * _PAGE_CAP
    direct_rows, direct_total = store.children(lei, limit=cap, kind="direct")
    ultimate_rows, ultimate_total = store.children(lei, limit=cap, kind="ultimate")
    watermark = store.watermark()
    subject = gleif_record_from_row(row)
    children = _merge_children(
        [gleif_record_from_row(r) for r in direct_rows],
        [gleif_record_from_row(r) for r in ultimate_rows],
    )
    # Phase 255: the RR rows the mirror holds, and the direct parents of the
    # ultimate-only children, both read locally.
    direct_rels: dict[str, dict] = {}
    ultimate_rels: dict[str, dict] = {}
    if store.has_relationships:
        from .entity_pages import gleif_relationship_record

        for m in children:
            clei = _child_lei(m)
            for kind, bucket in (("direct", direct_rels), ("ultimate", ultimate_rels)):
                if kind not in m["relations"]:
                    continue
                held = store.relationship(clei, kind)
                if held is not None and held.parent_lei == lei:
                    bucket[clei] = gleif_relationship_record(held)
    _attach_relationships(children, direct_rels, ultimate_rels)
    _attach_direct_parents(children, _store_direct_parents(_ultimate_only(children)))
    return {
        "lei": lei,
        "subject_attrs": subject.get("attributes") or {},
        "direct_total": direct_total,
        "ultimate_total": ultimate_total,
        "children": children,
        "direct_available": True,
        "ultimate_available": True,
        "subject_available": True,
        "snapshot_date": watermark.strftime("%Y-%m-%d") if watermark else None,
        "snapshot_source": "mirror",
        _COMPLETE_KEY: True,
        _SHAPE_KEY: _SHAPE,
        # Phase 261: a mirror built without relationship rows holds no dates,
        # and the tab says so rather than showing undated rows as if dated.
        _ENRICHED_KEY: bool(store.has_relationships),
    }


def _merge_children(direct_recs: list[dict], ultimate_recs: list[dict]) -> list[dict[str, Any]]:
    """Merge the two relation lists by child LEI, tagging each child with the
    relations it appears under — the same merge the live path does inline."""
    merged: dict[str, dict[str, Any]] = {}
    for records, kind in ((direct_recs, "direct"), (ultimate_recs, "ultimate")):
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
    return [
        {"record": m["record"], "relations": sorted(m["relations"])}
        for m in merged.values()
    ]


#: Phase 261: the fields of ``lei_registration.from_gleif_record`` a row
#: carries. The sentence, the LOU and the other dates stay on the subject's
#: own profile — a 154-row network would otherwise repeat them 154 times.
_ROW_REGISTRATION_KEYS = ("status", "label", "flag", "since", "next_renewal_date")


def _row_registration(record: dict[str, Any]) -> dict[str, Any] | None:
    """The child LEI's own registration status (Phase 261).

    ``status`` on the row is the *entity* status (ACTIVE), so without this a
    list in which 42 of Shell's 154 LEIs are LAPSED read as all current.
    """
    reg = lei_registration.from_gleif_record(record)
    if reg is None:
        return None
    return {k: reg.get(k) for k in _ROW_REGISTRATION_KEYS}


def _row(
    m: dict[str, Any],
    network: set[str] | frozenset[str] = frozenset(),
    subject_lei: str | None = None,
) -> dict[str, Any]:
    attrs = m["record"].get("attributes") or m["record"]
    e = attrs.get("entity") or {}
    clei = attrs.get("lei") or m["record"].get("id")
    relations = m["relations"]
    relation = "both" if set(relations) >= {"direct", "ultimate"} else relations[0]
    # Phase 261: the relationship's own dates. The direct record is the one
    # the graph draws for a "both" child (Phase 255: one edge per pair), so
    # its period is the one the row states.
    rels = m.get("rels") or {}
    start, end = _gleif_rr_period(rels.get("direct") or rels.get("ultimate"))
    # Phase 261: the direct parent of an ultimate-only child — the hop the
    # graph draws and the list did not. A parent GLEIF names that is the
    # subject itself says nothing the relation does not.
    parent = (m.get("direct_parent") or {}).get("lei") or None
    if parent in (clei, subject_lei):
        parent = None
    return {
        "lei": clei,
        "name": (e.get("legalName") or {}).get("name"),
        "jurisdiction": e.get("jurisdiction"),
        "status": e.get("status"),
        "relation": relation,
        "link": f"https://search.gleif.org/#/record/{clei}",
        "lei_registration": _row_registration(m["record"]),
        "direct_parent_lei": parent,
        "direct_parent_in_network": (parent in network) if parent else None,
        "relationship_start": start,
        "relationship_end": end,
    }


_EMPTY = {
    "available": False, "direct_total": 0, "ultimate_total": 0,
    "distinct_fetched": 0, "indirect_only": 0, "node_estimate": 0,
    "render_mode": "graph", "truncated": False, "jurisdictions": [], "countries": [],
    "enriched": True, "children": [], "bods": None,
    # Offline/demo mode is not a GLEIF refusal — the network was never asked
    # for, and `reason` says so. Declaring these available keeps the degraded
    # notice for the case it describes.
    "children_available": True, "direct_available": True,
    "ultimate_available": True, "snapshot_fallback": False,
    "snapshot_date": None, "snapshot_source": None, "degraded_detail": None,
    "unavailable_reason": None,
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
    snapshot_source = data.get("snapshot_source") or ("fallback" if snapshot_date else None)
    snapshot_fallback = snapshot_date is not None

    network = {_child_lei(m) for m in children}
    rows = [_row(m, network, lei) for m in children]
    jmap: dict[str, int] = {}
    for r in rows:
        jmap[r["jurisdiction"] or "—"] = jmap.get(r["jurisdiction"] or "—", 0) + 1
    jurisdictions = sorted(jmap.items(), key=lambda kv: -kv[1])[:30]
    # Phase 255: the same counts rolled up to the country. GLEIF files a US or
    # Canadian company under its state or province (``US-DE``, ``CA-AB``), so
    # ``jurisdictions`` lists Shell's Canadian subsidiaries as CA 5 and CA-AB 6.
    cmap: dict[str, int] = {}
    for r in rows:
        country = (r["jurisdiction"] or "—").split("-", 1)[0] or "—"
        cmap[country] = cmap.get(country, 0) + 1
    countries = sorted(cmap.items(), key=lambda kv: (-kv[1], kv[0]))

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
        # Phase 179: why the snapshot answered — "mirror" (chosen: the
        # mirror-first order) or "fallback" (forced: GLEIF refused). None when
        # the network came live.
        "snapshot_source": snapshot_source,
        "degraded_detail": _degraded_detail(
            direct_available, ultimate_available, snapshot_fallback, snapshot_date,
            snapshot_source, data.get("unavailable_reason"),
        ),
        # Phase 258: held_for_lookups / rate_limited / unreachable, or None.
        "unavailable_reason": (
            None if snapshot_source == "mirror" else data.get("unavailable_reason")
        ),
        "direct_total": direct_total,
        "ultimate_total": ultimate_total,
        "distinct_fetched": len(children),
        "indirect_only": sum(1 for m in children if m["relations"] == ["ultimate"]),
        "node_estimate": node_estimate,
        "render_mode": "graph" if node_estimate <= GRAPH_THRESHOLD else "table",
        "truncated": len(children) < node_estimate,
        "jurisdictions": [{"code": k, "count": v} for k, v in jurisdictions],
        "countries": [{"code": k, "count": v} for k, v in countries],
        # Phase 261: False = GLEIF would not give the relationship records or
        # the direct parents, so a row without a date or a "via" line is not
        # evidence that there is none. Payloads from before Phase 255 carry
        # no key; ``_build`` refuses to serve those.
        "enriched": bool(data.get(_ENRICHED_KEY, True)),
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
                    detail=SNAPSHOT_DETAIL[snapshot_source or "fallback"],
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


#: Phase 258: the cause, when known, in the reader's words. Before, every
#: refusal read "GLEIF is rate-limiting or unreachable" — including the
#: Phase 234 reserve, when OpenCheck had not asked GLEIF at all.
_CAUSE = {
    "held_for_lookups": "OpenCheck kept its last GLEIF requests for lookups, so GLEIF was not asked",
    "rate_limited": "GLEIF is rate-limiting OpenCheck's requests",
    "unreachable": "GLEIF did not answer",
}
_UNKNOWN_CAUSE = "GLEIF is rate-limiting or unreachable"


def _degraded_detail(
    direct_available: bool,
    ultimate_available: bool,
    snapshot_fallback: bool,
    snapshot_date: str | None,
    snapshot_source: str | None = None,
    reason: str | None = None,
) -> str | None:
    """One sentence naming what GLEIF did not answer, and why. ``None`` when it
    did — and ``None`` for a mirror-served network too (Phase 179): nothing was
    refused, the snapshot was the chosen source, and the badge and
    ``snapshot_date`` already say so.

    Written here rather than in the frontend because the backend is the only
    layer that knows *which* of the two relation calls was refused, and
    "we could not check" has to be specific to be worth more than silence.
    ``reason`` (Phase 258) names the cause; without one the sentence keeps its
    older wording rather than guess.
    """
    if snapshot_source == "mirror":
        return None
    cause = _CAUSE.get(reason or "", _UNKNOWN_CAUSE)
    if snapshot_fallback:
        dated = f" (extract of {snapshot_date})" if snapshot_date else ""
        if not ultimate_available:
            return (
                f"{cause}. Direct children are "
                f"shown from OpenCheck's Golden Copy snapshot{dated}; the "
                "ultimate (indirect) children could not be checked at all."
            )
        if not direct_available:
            return (
                f"{cause}. The ultimate (indirect) "
                f"children are shown from OpenCheck's Golden Copy snapshot{dated}; "
                "the direct children could not be checked at all."
            )
        if reason in _CAUSE:
            return (
                f"{cause}, so this network is shown from OpenCheck's Golden Copy "
                f"snapshot{dated} rather than live."
            )
        return (
            "GLEIF did not answer for this network, so it is shown from "
            f"OpenCheck's Golden Copy snapshot{dated} rather than live."
        )
    if not direct_available and not ultimate_available:
        return (
            f"{cause}, so the subsidiary network "
            "could not be checked. This is not a finding that the entity has none."
        )
    if not direct_available:
        if reason in _CAUSE:
            return (
                f"The direct children could not be fetched ({_CAUSE[reason]}) — only "
                "the ultimate (indirect) ones are shown, so this network is incomplete."
            )
        return (
            "GLEIF did not return the direct children — only the ultimate "
            "(indirect) ones are shown, so this network is incomplete."
        )
    if not ultimate_available:
        if reason in _CAUSE:
            return (
                f"The ultimate (indirect) children could not be fetched ({_CAUSE[reason]}) "
                "— only the direct ones are shown, so this network is incomplete."
            )
        return (
            "GLEIF did not return the ultimate (indirect) children — only the "
            "direct ones are shown, so this network is incomplete."
        )
    return None


__all__ = ["assemble_subsidiaries", "GRAPH_THRESHOLD"]
