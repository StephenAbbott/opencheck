"""FullCheck network expansion: ``/expand``, ``/expand-layer``, ``/expand-schemes``.

Split out of ``routers/lookup.py`` in Phase 246, unchanged except that the
expansion reaches ``_lookup_impl`` through the ``lookup`` module, so a test
(or a caller) that replaces ``lookup._lookup_impl`` still replaces it here.
A hop is a full lookup of a neighbour (LEI-keyed) or a one-register read
(register-scoped, Phase 182); both are charged to the caller's lookup budget
at the same point every other lookup is (Phase 234).

Phase 283: a hop no longer hides what it could not do. Every hop returns its
degradation records alongside its BODS and signals, and ``/expand-layer``
merges them into ``degraded_sources``; frontier nodes past the per-call cap
are named in ``capped`` instead of vanishing from the response.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, model_validator

from .. import (
    degradation as _degradation,
    outbound_rate as _outbound_rate,
    provenance as _provenance,
    register_hops,
    signalstats,
)
from ..bods import unique_statements
from ..cross_check import CHECK_NAME as _CROSS_CHECK
from ..icij_check import CHECK_NAME as _ICIJ_CHECK
from ..openaleph_check import CHECK_NAME as _OPENALEPH_CHECK
from ..bods.state_bodies import classify_government_entities
from ..cross_check import assess_cross_source_names
from ..ratelimit import default_tier, limiter, lookup_tier
from ..risk import DegradedSource, assess_bundle
from ..secret_scrub import describe_exception
from ..sources import REGISTRY
from . import lookup as _lookup
from ..pep_merge import merge_derived_pep
from .lookup import _LEI_SHAPE, _fetch_with_provenance, _mapper_for, _merge_signals

router = APIRouter()

#: A hop's BODS, its risk signals, and its degradation records (dicts in the
#: ``DegradedSource`` shape) — all with ids remapped onto the anchor.
_HopResult = tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]

#: Phase 283: the derived screening checks whose degradation an LEI hop
#: reports. An LEI hop is a full forty-source lookup, so its record also names
#: every source that did not answer; FullCheck reports only the screens —
#: whether the companies it reached were actually screened — and leaves the
#: per-source picture to each company's own lookup.
_SCREEN_CHECKS = frozenset({_CROSS_CHECK, _ICIJ_CHECK, _OPENALEPH_CHECK})

#: The check a subsidiaries hop's GLEIF degradation is filed under.
CHECK_SUBSIDIARIES = "gleif_subsidiaries"


def _entity_idents(stmt: dict[str, Any]) -> set[str]:
    """Upper-cased identifier values carried by a BODS entity statement."""
    ids = (stmt.get("recordDetails") or {}).get("identifiers") or []
    return {(i.get("id") or "").strip().upper() for i in ids if (i.get("id") or "").strip()}


def _anchor_replacements(bods: list[dict[str, Any]], key: str, anchor: str) -> dict[str, str]:
    """The statementId → ``anchor`` rewrites that collapse every representation of
    the entity identified by ``key`` — an LEI, or since Phase 182 a register
    number such as a Companies House company number — onto the existing graph
    node.

    Fix for the spike's cross-source finding: a national register keys its entity
    statement on the company number, not the LEI, so matching on the LEI alone
    left a floating duplicate. We seed the identifier set from every statement
    that asserts the key (GLEIF ties the LEI to the company number), then mark any
    entity statement sharing one of those identifier values for rewrite.
    """
    from ..bods.mapper import _stable_id

    norm = key.strip().upper()
    subj_idents: set[str] = {norm}
    for s in bods:
        if s.get("recordType") == "entity" and norm in _entity_idents(s):
            subj_idents |= _entity_idents(s)

    # The GLEIF subject statement is keyed on the LEI itself; a register hop's
    # subject is found through its identifiers like every other statement.
    subject_ids = {_stable_id("gleif", "entity", norm)} if _LEI_SHAPE.match(norm) else set()
    for s in bods:
        if s.get("recordType") == "entity" and (_entity_idents(s) & subj_idents):
            subject_ids.add(s["statementId"])
    subject_ids.discard(anchor)
    return {sid: anchor for sid in subject_ids}


def _apply_id_remap(items: list[dict[str, Any]], repl: dict[str, str]) -> list[dict[str, Any]]:
    """Rewrite statement ids over a serialised list (BODS *or* risk signals). A
    blunt string replace is safe — opencheck statement ids are unique 24-hex
    tokens with no collision risk — and it catches every reference field uniformly
    (including the ``evidence.statement_id`` fields risk signals carry)."""
    if not repl:
        return items
    raw = json.dumps(items)
    for old, new in repl.items():
        raw = raw.replace(old, new)
    return json.loads(raw)


def _collapse_onto_anchor(bods: list[dict[str, Any]], lei: str, anchor: str) -> list[dict[str, Any]]:
    """Collapse every representation of the LEI-identified entity onto ``anchor``."""
    return _apply_id_remap(bods, _anchor_replacements(bods, lei, anchor))


async def _expand_one_layer(
    lei: str, anchor: str, *, deepen_top: int = 3
) -> _HopResult:
    """Owner-ward hop: re-anchor a standard ``lookup`` (the entity's owners) and
    stitch it onto ``anchor`` (reusing the replay cache). Returns the new layer's
    BODS **and** the risk signals the sub-lookup already screened for the expanded
    entity — both with ids remapped onto the anchor — so FullCheck accumulates
    network-wide risk as it expands.

    Phase 283: also the sub-lookup's degradation records for the screening
    checks (``_SCREEN_CHECKS``). They used to be dropped, so a hop whose
    sanctions screen timed out read exactly like a clean one."""
    norm = lei.strip().upper()
    resp = await _lookup._lookup_impl(lei=norm, deepen_top=deepen_top)  # raises 400/404
    repl = _anchor_replacements(resp.bods, norm, anchor)
    degraded = [
        d for d in (getattr(resp, "degraded_sources", None) or [])
        if d.get("check") in _SCREEN_CHECKS
    ]
    return (
        _apply_id_remap(resp.bods, repl),
        _apply_id_remap(resp.risk_signals, repl),
        degraded,
    )


async def _subsidiaries_one_layer(lei: str, anchor: str) -> _HopResult:
    """Subsidiary-ward hop: fetch the entity's GLEIF Level-2 children and stitch
    them under ``anchor``. GLEIF L2 children aren't risk-screened, so no signals.

    Phase 283: what GLEIF did not answer (``degraded_detail``) and a children
    list cut short (``truncated``) come back as degradation records, so a
    partial subsidiary hop no longer reads as a complete one."""
    from ..subsidiaries import assemble_subsidiaries

    norm = lei.strip().upper()
    data = await assemble_subsidiaries(norm, include_bods=True)
    bods = (data or {}).get("bods") or []
    return _collapse_onto_anchor(bods, norm, anchor), [], _subsidiary_degradation(data)


def _subsidiary_degradation(data: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The degradation records a subsidiaries hop reports. Fixed sentences —
    ``degraded_detail`` is GLEIF's wording and is not copied, because the layer
    merge counts hops rather than repeating one hop's sentence."""
    if not data:
        return []
    out: list[dict[str, Any]] = []
    if data.get("degraded_detail"):
        unavailable = data.get("unavailable_reason")
        out.append({
            "source_id": "gleif",
            "check": CHECK_SUBSIDIARIES,
            "affected_signals": [],
            "detail": "GLEIF did not answer every subsidiary call",
            "reason": "rate_limited" if unavailable == "rate_limited" else "upstream_error",
        })
    if data.get("truncated"):
        out.append({
            "source_id": "gleif",
            "check": CHECK_SUBSIDIARIES,
            "affected_signals": [],
            "detail": "OpenCheck's limit on subsidiaries per company was reached",
            "reason": "truncated",
        })
    return out


@router.get("/expand")
@limiter.limit(lookup_tier)
async def expand(
    request: Request,
    response: Response,
    lei: str = Query(..., description="LEI of the corporate node to expand."),
    anchor: str = Query(
        ...,
        description=(
            "statementId of the existing graph node being expanded. The "
            "looked-up entity's identity statements are remapped onto it so the "
            "new owners layer stitches onto the existing node, not a duplicate."
        ),
    ),
    deepen_top: int = Query(3, ge=0, le=10),
) -> dict[str, Any]:
    """Progressive discovery: resolve one corporate node a hop deeper.

    Live-only and corporate-hops only (person nodes are terminal and the caller
    never expands them); not part of the main lookup synthesis. The owner-ward
    traversal foundation that FullCheck's network exploration builds on. See
    ``/expand-layer`` for the batch (whole-frontier) variant.
    """
    bods, _signals, _degraded = await _expand_one_layer(lei, anchor, deepen_top=deepen_top)
    return {"lei": lei.strip().upper(), "anchor": anchor, "bods": bods}


async def _register_one_layer(
    scheme: str, ident: str, anchor: str, *, name: str | None = None
) -> _HopResult:
    """Owner-ward hop on a register-scoped identifier (Phase 182): dispatch only
    the register that owns the scheme — Companies House for ``GB-COH`` — map its
    bundle to BODS, screen the parties it returns against OpenSanctions and
    EveryPolitician, and stitch the lot onto ``anchor``.

    This is the *cheap* hop: one register (four calls for Companies House —
    profile, officers, PSCs, PSC statements — plus its own PSC walk) and the
    name screen, not the forty-source fan-out ``_expand_one_layer`` pays for an
    LEI. It runs inside its own degradation and outbound-budget scopes, as the
    pipeline does, so a rate-capped register degrades instead of timing out.
    An unknown scheme, a value that is not a number of that register, or a
    register that is not live for this deployment yields nothing — never a
    guess.

    Phase 283: returns the scope's degradation records too — the register's
    own (it is the only source this hop reads, so a register that refused is
    the whole hop) and the name screen's, ``truncated`` included. They were
    collected and then dropped, so a failed screen in a hop read as clean.
    """
    hop = register_hops.hop_for(scheme)
    if hop is None:
        return [], [], []
    try:
        local_id = hop.normalise(ident)
    except ValueError:
        return [], [], []
    adapter = REGISTRY.get(hop.source_id)
    mapper = _mapper_for(hop.source_id)
    if adapter is None or mapper is None:
        return [], [], []

    _degradation.begin()
    _outbound_rate.begin()
    # Phase 184: the register's walk counters file this under "hop", not
    # "lookup" — the two are different questions for the PSC-graph ticket.
    origin_token = signalstats.walk_origin.set("hop")
    stub = False
    bods: list[dict[str, Any]] = []
    bundle_signals: list[dict[str, Any]] = []
    try:
        kwargs = {"legal_name": name} if hop.pass_legal_name and name else {}
        raw, prov = await _fetch_with_provenance(adapter, local_id, **kwargs)
        if not isinstance(raw, dict) or raw.get("is_stub"):
            stub = True
        else:
            with _provenance.mapping_provenance(prov):
                bods = classify_government_entities(
                    unique_statements(mapper(raw)), source_id=hop.source_id
                )
            bundle_signals = [
                s.to_dict() for s in assess_bundle(hop.source_id, raw, bods, hit_id=local_id)
            ]
    finally:
        signalstats.walk_origin.reset(origin_token)
        degraded: list[DegradedSource] = _degradation.collect()
        _outbound_rate.end()
    if stub:
        # Nothing to screen, but a register that refused said so in the scope.
        return [], [], [d.to_dict() for d in degraded]
    # Sanctions screening of the new node and everything it brought with it —
    # the subject entity is a target of the name screen like any other party.
    cross = await assess_cross_source_names(bods, degraded=degraded)
    # Phase 247: one PEP chip per upstream record here too. The own-role label
    # is left to the lookup, which knows its subject.
    signals = _merge_signals(
        bundle_signals, merge_derived_pep([s.to_dict() for s in cross])
    )
    repl = _anchor_replacements(bods, local_id, anchor)
    return (
        _apply_id_remap(bods, repl),
        _apply_id_remap(signals, repl),
        [d.to_dict() for d in degraded],
    )


@router.get("/expand-schemes")
@limiter.limit(default_tier)
async def expand_schemes(request: Request, response: Response) -> dict[str, Any]:
    """The identifier schemes ``/expand-layer`` can hop on without an LEI
    (Phase 182): scheme → the register that answers it. The frontier reads this
    so a node is offered for expansion only when a hop exists for it."""
    return {"schemes": register_hops.describe()}


_MAX_LAYER_ITEMS = 25  # cap concurrent hops per "add layer" so it can't fan out the register

#: What each check is, as the merged detail sentence names it.
_CHECK_PHRASES: dict[str, str] = {
    _CROSS_CHECK: "Sanctions and PEP screening",
    _ICIJ_CHECK: "Offshore-leaks screening",
    _OPENALEPH_CHECK: "OpenAleph screening",
    _degradation.CHECK_SOURCE_FETCH: "The register",
    _degradation.CHECK_SOURCE_READ: "The register's full record",
    CHECK_SUBSIDIARIES: "The subsidiary list",
}


def merge_layer_degradations(
    per_hop: list[list[dict[str, Any]]], hops_run: int
) -> list[dict[str, Any]]:
    """Phase 283: one ``degraded_sources`` list for a whole layer.

    Records are grouped by (source, check, reason); a group's
    ``affected_signals`` is the union of its members', and ``hops`` counts the
    expanded companies it hit (a company counts once per group however many
    records it filed). The detail is rewritten as a count over the layer —
    "… did not fully run for 3 of 12 companies expanded in this layer" — so no
    one hop's sentence stands for the rest, and, as for every
    ``DegradedSource``, it carries counts only, never names. Order is stable:
    first appearance wins.
    """
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for records in per_hop:
        hit: set[tuple[str, str, str]] = set()
        for d in records:
            key = (d.get("source_id") or "opencheck", d.get("check") or "", d.get("reason") or "")
            g = groups.get(key)
            if g is None:
                g = groups[key] = {
                    "source_id": key[0], "check": key[1], "reason": key[2],
                    "affected_signals": [], "hops": 0,
                }
            for code in d.get("affected_signals") or []:
                if code not in g["affected_signals"]:
                    g["affected_signals"].append(code)
            if key not in hit:
                hit.add(key)
                g["hops"] += 1
    out: list[dict[str, Any]] = []
    total = max(hops_run, 1)
    for g in groups.values():
        n = min(g["hops"], total)
        what = _CHECK_PHRASES.get(g["check"], "A check")
        noun = "company" if total == 1 else "companies"
        g["detail"] = f"{what} did not fully run for {n} of {total} {noun} expanded in this layer"
        g["hops"] = n
        out.append(g)
    return out


class _ExpandItem(BaseModel):
    """One frontier node. An LEI re-anchors a full lookup; a register-scoped
    identifier (``scheme`` + ``id``, e.g. ``GB-COH`` + ``02999029``) takes the
    cheap register hop (Phase 182). A node carrying both is expanded on its
    LEI — the client prefers it, and so does the server."""

    anchor: str
    lei: str | None = None
    scheme: str | None = None
    id: str | None = None
    #: The node's name as shown, for registers whose fetch takes ``legal_name``.
    name: str | None = None

    @model_validator(mode="after")
    def _keyed(self) -> _ExpandItem:
        if not self.lei and not (self.scheme and self.id):
            raise ValueError("an item needs an lei, or a scheme and an id")
        return self


class ExpandLayerRequest(BaseModel):
    items: list[_ExpandItem]
    # Context-aware direction: an ownership graph digs up (owners); a subsidiary
    # tree digs down (GLEIF Level-2 children). The view tells us which.
    direction: Literal["owners", "subsidiaries"] = "owners"


@router.post("/expand-layer")
@limiter.limit(lookup_tier)
async def expand_layer(
    request: Request, response: Response, req: ExpandLayerRequest
) -> dict[str, Any]:
    """Progressive discovery (batch): take the whole current frontier and go one
    layer deeper on every node at once, in the graph's existing direction.

    Each item names a frontier node by LEI, or (Phase 182) by a register-scoped
    identifier, plus its ``anchor`` (the caller selects the frontier).
    ``direction`` picks the hop: ``owners`` re-anchors a standard lookup on an
    LEI, or dispatches just the owning register for a register-scoped id (up
    the ownership chain); ``subsidiaries`` fetches GLEIF Level-2 children (down
    the subsidiary tree) and needs an LEI — a register-only node is skipped
    there. Hops run concurrently (bounded), each stitched onto its anchor, and
    the results are merged + de-duplicated by ``statementId``. Capped at
    ``_MAX_LAYER_ITEMS`` so a click can't fan out the whole register.

    Phase 283: the items past the cap are named in ``capped`` — in the order
    sent, which the client ranks — so the caller can send them again; they
    used to be in neither ``expanded`` nor ``deferred``. And every hop's
    degradation records come back merged in ``degraded_sources``: an empty
    ``risk_signals`` beside a non-empty ``degraded_sources`` is not a clean
    layer.
    """
    items = req.items[:_MAX_LAYER_ITEMS]
    capped = [i.anchor for i in req.items[_MAX_LAYER_ITEMS:]]
    sem = asyncio.Semaphore(5)
    hops = {"lei": 0, "register": 0, "skipped": 0}
    # Phase 234: an LEI hop is a full lookup and is charged to the caller's
    # lookup budget like any other. When the budget runs out part-way the hop
    # is *deferred* — named, with the seconds until budget frees — so the
    # client can come back for it; a hop that failed is *failed*, with its
    # reason. Both used to be ``except Exception: return [], []``, which drew
    # a node whose owners could not be fetched exactly like a node with none.
    deferred: list[str] = []
    failed: list[dict[str, Any]] = []
    retry_after: list[int] = []

    async def _one(item: _ExpandItem) -> _HopResult:
        async with sem:
            try:
                if item.lei:
                    hops["lei"] += 1
                    if req.direction == "subsidiaries":
                        return await _subsidiaries_one_layer(item.lei, item.anchor)
                    return await _expand_one_layer(item.lei, item.anchor)
                if req.direction == "subsidiaries" or not register_hops.hop_for(item.scheme):
                    hops["skipped"] += 1
                    return [], [], []
                hops["register"] += 1
                return await _register_one_layer(
                    item.scheme or "", item.id or "", item.anchor, name=item.name
                )
            except HTTPException as exc:
                if exc.status_code == 429:
                    hops["lei"] -= 1
                    deferred.append(item.anchor)
                    try:
                        retry_after.append(int((exc.headers or {}).get("Retry-After", "60")))
                    except ValueError:
                        retry_after.append(60)
                else:
                    failed.append({
                        "anchor": item.anchor,
                        "lei": item.lei,
                        "status": exc.status_code,
                        "reason": str(exc.detail),
                    })
                return [], [], []
            except Exception as exc:  # noqa: BLE001 — a bad node must not sink the batch
                failed.append({
                    "anchor": item.anchor,
                    "lei": item.lei,
                    "status": 500,
                    "reason": describe_exception(exc),
                })
                return [], [], []

    chunks = await asyncio.gather(*[_one(i) for i in items])

    seen: set[str] = set()
    merged: list[dict[str, Any]] = []
    seen_sig: set[str] = set()
    merged_sig: list[dict[str, Any]] = []
    for bods_chunk, sig_chunk, _deg_chunk in chunks:
        for s in bods_chunk:
            sid = s.get("statementId")
            if sid and sid not in seen:
                seen.add(sid)
                merged.append(s)
        for sig in sig_chunk:
            key = json.dumps(sig, sort_keys=True, default=str)
            if key not in seen_sig:
                seen_sig.add(key)
                merged_sig.append(sig)

    deferred_set = set(deferred)
    degraded_sources = merge_layer_degradations(
        [deg for _b, _s, deg in chunks], hops["lei"] + hops["register"]
    )
    return {
        "bods": merged,
        "risk_signals": merged_sig,
        # Every anchor this call answered for — including failed ones, which
        # are named in ``failed`` — but never a deferred one: the client keeps
        # a deferred node on its frontier and asks again after
        # ``retry_after_s``.
        "expanded": [i.anchor for i in items if i.anchor not in deferred_set],
        "deferred": [i.anchor for i in items if i.anchor in deferred_set],
        "retry_after_s": max(retry_after) if retry_after else None,
        "failed": failed,
        "count": len(items),
        "truncated": bool(capped),
        # Phase 283: the anchors past the cap, not run — send them again.
        "capped": capped,
        # Phase 283: what the layer's hops could not fully check (issue #50).
        "degraded_sources": degraded_sources,
        # Phase 182: what the layer cost, by hop kind.
        "hops": hops,
    }
