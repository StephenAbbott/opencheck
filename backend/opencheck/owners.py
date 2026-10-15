"""Who owns this company above X%? — the ``opencheck_owners`` and
``opencheck_ownership_map`` MCP tools (Phase 340).

Item D of the T-Rank research ticket: T-Rank's MCP server answers one question
per tool; OpenCheck's ``opencheck_lookup`` answered everything at once and left
an agent to dig the owners out. These two tools answer the ownership question
directly, from what OpenCheck already computes:

* **Calculated owners** — every party whose integrated ownership of the subject
  (Phase 339, ``integrated_ownership``) meets the threshold, one figure per
  source, never added across sources. A figure whose band straddles the
  threshold, or whose upper end is unknown, is ``may_be_above`` — never
  ``above``.
* **Declared beneficial owners** — the people the registers themselves flag
  (``beneficialOwnershipOrControl: true``) on a relationship with the subject.
* **In both** — the same person in both lists, matched on the same statement,
  a reconciled entity, or a shared register identifier; never on a name alone.

Stephen's decisions on the Phase 340 ticket (15 Oct 2026):

1. Three lists: calculated, declared, and the people in both.
2. The threshold is the subject's jurisdiction rule from
   ``bods/bo_regimes.py`` (a ``bo_register`` regime: Estonia "more than 25%",
   Ukraine "25% or more"); where none is recorded, **25% or more** (the
   AMLR's definition). The caller may override with ``threshold`` and
   ``rule``; the answer always says which rule it applied.
3. ``depth`` defaults to the lookup's own reach. Up to ``depth=3`` re-runs a
   lookup on each corporate owner at or above the threshold (FullCheck's
   ``/expand-layer`` hop), at most ``MAX_HOPS`` per call; where a chain stops,
   the answer says why.
4. The map is an SVG image plus a text table and the FullCheck link.
5. One map, every source's edges, each labelled with its source.
6. Names are included — they are already in ``opencheck_export_bods``. A
   calculated figure is arithmetic on what was filed, never a determination
   that someone is a beneficial owner; nothing is stored.

Context, never a signal: nothing here is read by the risk engine, the verdict
or ``/signalstats`` (the Phase 339 isolation test covers this module too).
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from . import integrated_ownership as io
from .bods.lifecycle import record_closed, statement_lifecycle, today_iso
from .bods.refs import resolver

#: Most extra lookups one call may start when ``depth`` > 1.
MAX_HOPS = 10
MAX_DEPTH = 3
#: Edges the map draws; every edge is in the text table regardless.
MAP_MAX_EDGES = 16

FULLCHECK_URL = "https://opencheck.world/?lei={lei}&mode=full"

_EPS = 1e-9
_LEI = re.compile(r"^[0-9A-Z]{18}[0-9]{2}$")

DETERMINATION_NOTE = (
    "Calculated figures are arithmetic on what the sources filed — shares "
    "multiplied along each path and added across paths, one figure per source, "
    "never added across sources — not a determination that anyone is a "
    "beneficial owner. Declared beneficial owners are as each register "
    "publishes them."
)
HINT = (
    "Context, not a risk finding. 'may_be_above' means the figure could meet the "
    "threshold (a band that straddles it, or a path whose share is not stated); "
    "read it as unknown, never as above. Two figures for one party are two "
    "sources' readings: never add them. Call opencheck_ownership_map to see the "
    "structure, or opencheck_lookup for risk signals."
)


# ----------------------------------------------------------------------
# The rule
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class Rule:
    operator: str  # ">" or ">="
    threshold: float
    basis: str  # "national" | "amlr" | "caller"
    text: str

    def passes(self, value: float | None) -> bool:
        if value is None:
            return False
        if self.operator == ">":
            return value > self.threshold + _EPS
        return value >= self.threshold - _EPS

    def as_dict(self) -> dict[str, Any]:
        return {
            "operator": self.operator,
            "threshold": self.threshold,
            "basis": self.basis,
            "text": self.text,
        }


def _pct(v: float) -> str:
    return f"{round(v, 2):g}%"


def national_regime(jurisdiction: str | None) -> Any | None:
    """The ``bo_register`` regime for a jurisdiction code, or None. An exact
    subdivision match (``US-DC``) wins over the country."""
    from .bods.bo_regimes import REGIMES

    code = (jurisdiction or "").strip().upper()
    if not code:
        return None
    candidates = [
        r for r in REGIMES.values()
        if r.regime_kind == "bo_register" and r.threshold_value is not None
        and r.threshold_operator in (">", ">=")
    ]
    for r in candidates:
        if (r.jurisdiction_code or "").upper() == code:
            return r
    country = code.split("-", 1)[0]
    for r in candidates:
        if (r.jurisdiction_code or "").upper() == country:
            return r
    return None


def _rule_words(op: str, value: float) -> str:
    return f"more than {_pct(value)}" if op == ">" else f"{_pct(value)} or more"


def resolve_rule(
    jurisdiction: str | None, threshold: float | None = None, rule: str | None = None
) -> Rule:
    """The rule an answer applies (decision 2): the subject's national rule
    where ``bo_regimes`` records one, else 25% or more (the AMLR reading).
    ``rule="amlr"`` forces "or more"; ``threshold`` replaces the number."""
    rule = (rule or "").strip().lower() or None
    if rule not in (None, "national", "amlr"):
        raise ValueError("rule must be 'national' or 'amlr'")
    if threshold is not None and not (0 <= float(threshold) <= 100):
        raise ValueError("threshold must be between 0 and 100")
    caller = " — threshold set by the caller" if threshold is not None else ""
    if rule == "amlr":
        value = float(threshold) if threshold is not None else 25.0
        return Rule(">=", value, "amlr",
                    f"{_rule_words('>=', value)} — EU AML Regulation (2024/1624) reading{caller}")
    regime = national_regime(jurisdiction)
    if regime is not None:
        op = regime.threshold_operator
        value = float(threshold) if threshold is not None else float(regime.threshold_value)
        return Rule(op, value, "national",
                    f"{_rule_words(op, value)} — {regime.jurisdiction} "
                    f"({regime.legal_basis[0]}){caller}")
    if rule == "national":
        raise ValueError(
            f"no national rule is recorded for jurisdiction {jurisdiction!r}; "
            "use rule='amlr' or leave rule unset"
        )
    value = float(threshold) if threshold is not None else 25.0
    return Rule(">=", value, "amlr",
                f"{_rule_words('>=', value)} — no national rule recorded for this "
                f"jurisdiction, so the EU AML Regulation's reading{caller}")


# ----------------------------------------------------------------------
# Identity: which rows are the same party
# ----------------------------------------------------------------------
def party_canon(bods: list[dict[str, Any]]) -> dict[str, str]:
    """statementId → canonical id. Entities as ``io.lookup_canon`` (a shared
    strong identifier); people when they carry the same register identifier
    (scheme and id). Never a name."""
    out = dict(io.lookup_canon(bods))
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    owner: dict[tuple[str, str], str] = {}
    for s in bods:
        if s.get("recordType") != "person" or not isinstance(s.get("statementId"), str):
            continue
        sid = s["statementId"]
        parent.setdefault(sid, sid)
        for i in (s.get("recordDetails") or {}).get("identifiers") or []:
            if not isinstance(i, dict):
                continue
            scheme = str(i.get("scheme") or i.get("schemeName") or "").strip().upper()
            ident = str(i.get("id") or "").strip()
            if not scheme or not ident:
                continue
            key = (scheme, ident)
            if key in owner:
                a, b = find(owner[key]), find(sid)
                if a != b:
                    parent[max(a, b)] = min(a, b)
            else:
                owner[key] = sid
    for sid in parent:
        root = find(sid)
        if root != sid:
            out[sid] = root
    return out


def lei_of(stmt: Mapping[str, Any] | None) -> str | None:
    for i in ((stmt or {}).get("recordDetails") or {}).get("identifiers") or []:
        if not isinstance(i, dict):
            continue
        val = str(i.get("id") or "").strip().upper()
        scheme = f"{i.get('scheme') or ''} {i.get('schemeName') or ''}".upper()
        if "LEI" in scheme and _LEI.match(val):
            return val
    return None


# ----------------------------------------------------------------------
# The three lists
# ----------------------------------------------------------------------
def _position(fig: Mapping[str, Any], rule: Rule) -> str | None:
    lo, hi = fig.get("minimum"), fig.get("maximum")
    if lo is None:
        # Shares held, size unknown (or a cycle that would not settle).
        return "may_be_above"
    if rule.passes(lo):
        return "above"
    if hi is None or rule.passes(hi):
        return "may_be_above"
    return None


def calculated_owners(
    payload: Mapping[str, Any] | None, rule: Rule
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(owners at or possibly above the threshold, holders listed without a
    figure — voting rights only, or formerly)."""
    owners: list[dict[str, Any]] = []
    unmeasured: list[dict[str, Any]] = []
    for h in io.with_figures(payload):
        if h.get("status") != io.STATUS_CURRENT:
            unmeasured.append({
                "party": h.get("party"),
                "name": h.get("name"),
                "kind": h.get("kind"),
                "status": h.get("status"),
                "note": h.get("note"),
                "sources": list(h.get("sources") or []),
            })
            continue
        figs = []
        for f in h.get("figures") or []:
            pos = _position(f, rule)
            if pos is None:
                continue
            figs.append({
                "source": f.get("source"),
                "source_id": f.get("source_id"),
                "text": f.get("text"),
                "minimum": f.get("minimum"),
                "maximum": f.get("maximum"),
                "position": pos,
                "route": io.route_words(f) or None,
            })
        if not figs:
            continue
        position = "above" if any(f["position"] == "above" for f in figs) else "may_be_above"
        owners.append({
            "party": h.get("party"),
            "name": h.get("name"),
            "kind": h.get("kind"),
            "position": position,
            "figures": figs,
        })
    owners.sort(key=lambda o: (o["position"] != "above",
                               -max((f.get("minimum") or 0.0) for f in o["figures"]),
                               str(o["name"]).casefold()))
    return owners, unmeasured


def _interest_words(interest: Mapping[str, Any]) -> str:
    from .reporting.diagram import interest_label

    try:
        return interest_label(dict(interest))
    except Exception:  # noqa: BLE001 — a label is never worth failing the answer
        return str(interest.get("type") or "interest")


def declared_owners(
    bods: list[dict[str, Any]], subject_ids: Iterable[str], as_of: str
) -> list[dict[str, Any]]:
    """Every party a register flags as a beneficial owner of the subject."""
    from .integrated_ownership import party_name, source_key

    resolve = resolver(bods)
    subjects = {s for s in subject_ids if s}
    by_id = {s.get("statementId"): s for s in bods if s.get("recordType") in ("entity", "person")}
    out: list[dict[str, Any]] = []
    for s in bods:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        if resolve(rd.get("subject")) not in subjects:
            continue
        flagged = [
            i for i in rd.get("interests") or []
            if isinstance(i, dict) and i.get("beneficialOwnershipOrControl") is True
        ]
        if not flagged:
            continue
        party = resolve(rd.get("interestedParty"))
        stmt = by_id.get(party)
        _, label = source_key(s)
        life = statement_lifecycle(s, as_of)
        out.append({
            "party": party or None,
            "name": party_name(stmt) or party or "Unspecified party",
            "kind": "person" if (stmt or {}).get("recordType") == "person" else "entity",
            "source": label,
            "interests": [_interest_words(i) for i in flagged],
            "current": not life.ended,
            **({"ended_on": life.ended_on} if life.ended and life.ended_on else {}),
            "closed_record": record_closed(s),
        })
    out.sort(key=lambda d: (not d["current"], str(d["name"]).casefold(), d["source"]))
    return out


def in_both(
    calculated: list[dict[str, Any]],
    declared: list[dict[str, Any]],
    canon: Mapping[str, str],
) -> list[dict[str, Any]]:
    """People (and entities) in both lists — same canonical node only."""
    def c(x: str) -> str:
        return canon.get(x, x)

    declared_by: dict[str, list[dict[str, Any]]] = {}
    for d in declared:
        if d.get("party"):
            declared_by.setdefault(c(d["party"]), []).append(d)
    out = []
    for o in calculated:
        hits = declared_by.get(c(o["party"])) or []
        if not hits:
            continue
        out.append({
            "name": o["name"],
            "kind": o["kind"],
            "position": o["position"],
            "calculated": [f"{f['text']} ({f['source']})" for f in o["figures"]],
            "declared_by": sorted({d["source"] for d in hits}),
        })
    return out


def chain_stops(
    calculated: list[dict[str, Any]],
    bods: list[dict[str, Any]],
    canon: Mapping[str, str],
    expanded: set[str],
    refused: Mapping[str, str],
    capped: bool,
) -> list[dict[str, Any]]:
    """Companies at or possibly above the threshold whose own owners the
    answer does not reach, and why."""
    def c(x: str) -> str:
        return canon.get(x, x)

    resolve = resolver(bods)
    owned = set()
    for s in bods:
        if s.get("recordType") == "relationship":
            rd = s.get("recordDetails") or {}
            sid = resolve(rd.get("subject"))
            if sid:
                owned.add(c(sid))
    by_canon: dict[str, dict[str, Any]] = {}
    for s in bods:
        if s.get("recordType") == "entity" and isinstance(s.get("statementId"), str):
            cid = c(s["statementId"])
            if lei_of(s) or cid not in by_canon:
                by_canon.setdefault(cid, s)
                if lei_of(s) and not lei_of(by_canon[cid]):
                    by_canon[cid] = s
    out = []
    for o in calculated:
        if o["kind"] != "entity":
            continue
        cid = c(o["party"])
        if cid in owned:
            continue
        etype = (((by_canon.get(cid) or {}).get("recordDetails") or {}).get("entityType") or {})
        if isinstance(etype, dict) and etype.get("type") in ("state", "stateBody"):
            continue  # a state or state body is the top of its chain
        lei = lei_of(by_canon.get(cid))
        if not lei:
            reason = "no LEI to follow; its owners are not in the sources OpenCheck reached"
        elif lei in refused:
            reason = f"its own lookup did not complete ({refused[lei]})"
        elif lei in expanded:
            reason = "its own lookup found no owners in any source"
        elif capped:
            reason = f"not followed: the {MAX_HOPS}-lookup limit for one call was reached"
        else:
            reason = "not followed: raise depth (up to 3) to look up its owners"
        out.append({"name": o["name"], "lei": lei, "figures": [f["text"] for f in o["figures"]],
                    "reason": reason})
    return out


# ----------------------------------------------------------------------
# Depth: follow corporate owners above the threshold
# ----------------------------------------------------------------------
HopFn = Callable[[str, str], Awaitable[list[dict[str, Any]]]]


@dataclass
class Network:
    bods: list[dict[str, Any]]
    subject_ids: list[str]
    payload: dict[str, Any]
    expanded: set[str]
    refused: dict[str, str]
    hops: int
    capped: bool
    depth_reached: int


def _recompute(bods: list[dict[str, Any]], subject_ids: list[str], as_of: str) -> dict[str, Any]:
    payload = io.compute(bods, subject_ids, as_of, io.lookup_canon(bods))
    payload["subject_ids"] = subject_ids
    return payload


async def grow(
    bods: list[dict[str, Any]],
    subject_ids: list[str],
    payload: dict[str, Any] | None,
    rule: Rule,
    depth: int,
    hop: HopFn,
    as_of: str,
    subject_lei: str | None = None,
) -> Network:
    """Follow corporate owners at or above the threshold up to ``depth`` - 1
    extra layers, at most ``MAX_HOPS`` lookups. ``hop(lei, anchor)`` returns
    that company's lookup BODS with its ids remapped onto ``anchor``."""
    from .bods import unique_statements

    bods = list(bods)
    payload = dict(payload) if payload else _recompute(bods, subject_ids, as_of)
    expanded: set[str] = {subject_lei} if subject_lei else set()
    refused: dict[str, str] = {}
    hops, capped, reached = 0, False, 1
    for _layer in range(max(0, min(depth, MAX_DEPTH) - 1)):
        owners, _ = calculated_owners(payload, rule)
        canon = io.lookup_canon(bods)
        by_canon: dict[str, dict[str, Any]] = {}
        for s in bods:
            sid = s.get("statementId")
            if s.get("recordType") == "entity" and isinstance(sid, str) and lei_of(s):
                by_canon.setdefault(canon.get(sid, sid), s)
        todo: list[tuple[str, str]] = []
        for o in owners:
            if o["kind"] != "entity":
                continue
            stmt = by_canon.get(o["party"])
            lei = lei_of(stmt)
            if lei and lei not in expanded and lei not in refused:
                todo.append((lei, o["party"]))
        if not todo:
            break
        added = False
        for lei, anchor in todo:
            if hops >= MAX_HOPS:
                capped = True
                break
            hops += 1
            try:
                layer = await hop(lei, anchor)
            except Exception as exc:  # noqa: BLE001 — one owner's lookup never fails the answer
                detail = getattr(exc, "detail", None) or exc.__class__.__name__
                refused[lei] = str(detail)[:120]
                continue
            expanded.add(lei)
            if layer:
                bods = unique_statements(bods + layer)
                added = True
        if added:
            reached += 1
            payload = _recompute(bods, subject_ids, as_of)
        if capped:
            break
    return Network(bods, subject_ids, payload, expanded - ({subject_lei} if subject_lei else set()),
                   refused, hops, capped, reached)


# ----------------------------------------------------------------------
# The answers
# ----------------------------------------------------------------------
def owners_answer(
    *,
    lei: str | None,
    legal_name: str | None,
    jurisdiction: str | None,
    net: Network,
    rule: Rule,
    depth: int,
    as_of: str,
    license_notices: list[Any] | None = None,
    risk_signals: list[Any] | None = None,
) -> dict[str, Any]:
    calculated, unmeasured = calculated_owners(net.payload, rule)
    declared = declared_owners(net.bods, net.subject_ids, as_of)
    canon = party_canon(net.bods)
    both = in_both(calculated, declared, canon)
    stops = chain_stops(calculated, net.bods, canon, net.expanded, net.refused, net.capped)
    return {
        "lei": lei,
        "legal_name": legal_name,
        "jurisdiction": jurisdiction,
        "rule": rule.as_dict(),
        "calculated_owners": calculated,
        "declared_beneficial_owners": declared,
        "in_both": both,
        "listed_without_a_figure": unmeasured,
        "chain_stops": stops,
        "depth": {
            "requested": depth,
            "layers_reached": net.depth_reached,
            "extra_lookups": net.hops,
            "hop_limit_reached": net.capped,
            "max_extra_lookups": MAX_HOPS,
        },
        "as_of": as_of,
        "method": {
            "name": "Integrated ownership",
            "description": io.METHOD_DESCRIPTION,
            "credit": io.METHOD_CREDIT,
        },
        "note": DETERMINATION_NOTE,
        "fullcheck_url": FULLCHECK_URL.format(lei=lei) if lei else None,
        "license_notices": license_notices or [],
        "hint": HINT,
        "summary": _summary(legal_name, rule, calculated, declared, both, stops)
        + _sanctions_sentence(risk_signals),
        "sanctions_ownership_test": sanctions_ownership_block(risk_signals),
    }


def _sanctions_sentence(signals: list[Any] | None) -> str:
    from . import sanctions_ownership as so

    sig = so.signal_of(signals)
    if sig is None:
        return ""
    regimes = (sig.get("evidence") or {}).get("regimes") or []
    return " Sanctions ownership test (computed, ownership only): " + so.regimes_sentence(regimes)


_HOLDER_KEYS = ("name", "stake", "designated", "basis", "held_by")


def sanctions_ownership_block(signals: list[Any] | None) -> dict[str, Any] | None:
    """Phase 342: the lookup's sanctions 50% rule, per regime — the one
    *finding* in this answer, carried over from the lookup's own
    ``SANCTIONS_OWNERSHIP_TEST`` / ``_POSSIBLE`` signal (never recomputed
    from the integrated figure above, which multiplies; the rule counts a
    caught company's holding in full). None when the lookup has neither."""
    from . import sanctions_ownership as so

    sig = so.signal_of(signals)
    if sig is None:
        return None
    ev = sig.get("evidence") or {}
    return {
        "finding": sig.get("code"),
        "kind": sig.get("kind"),
        "confidence": sig.get("confidence"),
        "summary": sig.get("summary"),
        "regimes": [
            {
                "regime": r.get("regime"),
                "rule": r.get("rule"),
                "result": r.get("result"),
                "if_confirmed": r.get("if_confirmed", False),
                "sources": [
                    {
                        "source": row.get("source"),
                        "result": row.get("result"),
                        "holding": (row.get("figure") or {}).get("text"),
                        "holders": [
                            {k: h.get(k) for k in _HOLDER_KEYS if h.get(k) is not None}
                            for h in row.get("holders") or []
                        ],
                        **({"name_only": row["name_only"]} if row.get("name_only") else {}),
                    }
                    for row in r.get("sources") or []
                ],
            }
            for r in ev.get("regimes") or []
        ],
        "designated": ev.get("designated") or [],
        "method": ev.get("description"),
        "not_computed": ev.get("not_computed"),
        "as_of": ev.get("as_of"),
        "credit": ev.get("credit"),
    }


def _summary(name, rule, calculated, declared, both, stops) -> str:
    above = [o for o in calculated if o["position"] == "above"]
    maybe = [o for o in calculated if o["position"] == "may_be_above"]
    current = [d for d in declared if d["current"]]
    bits = [f"{name or 'The company'} — rule applied: {rule.text}."]
    if above:
        bits.append(
            "Calculated at or above the threshold: "
            + "; ".join(
                o["name"] + " (" + ", ".join(
                    f"{f['text']} {f['source']}" for f in o["figures"] if f["position"] == "above"
                ) + ")"
                for o in above[:8]
            )
            + ("; …" if len(above) > 8 else "") + "."
        )
    else:
        bits.append("No party is calculated at or above the threshold.")
    if maybe:
        bits.append(f"{len(maybe)} more may be above it (a band or an unstated share).")
    bits.append(
        f"{len(current)} current declared beneficial owner(s)"
        + (f"; {len(both)} in both lists." if both else ".")
    )
    if stops:
        bits.append(f"{len(stops)} chain(s) stop at a company whose owners were not reached.")
    return " ".join(bits)


# ----------------------------------------------------------------------
# The map
# ----------------------------------------------------------------------
def ownership_map(
    *,
    lei: str | None,
    legal_name: str | None,
    net: Network,
    source: str | None = None,
    as_of: str,
) -> tuple[dict[str, Any], str | None]:
    """(the JSON answer, the SVG). One map of the subject and every party above
    it, every source's edges labelled with the source; role edges (officers,
    board) are left out — this is the ownership and control structure."""
    from .bods.mapper import gleif_reporting_exception
    from .integrated_ownership import _excluded, party_name, source_key  # noqa: F401
    from .reporting.diagram import _edge_label, _node_kind, _node_label, _render

    bods = net.bods
    canon = party_canon(bods)
    def c(x: str) -> str:
        return canon.get(x, x)

    resolve = resolver(bods)
    subj = {c(s) for s in net.subject_ids}
    by_canon: dict[str, dict[str, Any]] = {}
    for s in bods:
        if s.get("recordType") in ("entity", "person") and isinstance(s.get("statementId"), str):
            cid = c(s["statementId"])
            prev = by_canon.get(cid)
            if prev is None or (not party_name(prev) and party_name(s)):
                by_canon[cid] = s

    want = (source or "").strip().casefold() or None
    owners_of: dict[str, list[tuple[str, dict[str, Any], str]]] = {}
    # GLEIF reporting exceptions are filings, not parties: said in words, per
    # company, both levels folded into one entry when the reason is the same.
    # NON_PUBLIC (a parent that exists and is withheld) stays a drawn node.
    exceptions: dict[tuple[str, str], dict[str, Any]] = {}
    for s in bods:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        sub, party = resolve(rd.get("subject")), resolve(rd.get("interestedParty"))
        if not sub or not party:
            continue
        sid, label = source_key(s)
        if want and want not in (sid.casefold(), label.casefold()):
            continue
        exc = gleif_reporting_exception(s)
        if exc and not exc["undisclosed"]:
            entry = exceptions.setdefault((c(sub), exc["reason"]), {
                "company": c(sub), "reason": exc["reason"],
                "meaning": exc["meaning"], "levels": [],
            })
            if exc["level"] not in entry["levels"]:
                entry["levels"].append(exc["level"])
            continue
        lines, cat = _edge_label(s)
        if cat == "role":
            continue
        owners_of.setdefault(c(sub), []).append((c(party), s, label))

    # Breadth-first upward from the subject: nearest edges first. The layer is
    # the node's distance from the subject — the map's columns.
    seen = set(subj)
    frontier = list(subj)
    level: dict[str, int] = {n: 0 for n in subj}
    ordered: list[tuple[str, str, dict[str, Any], str]] = []
    while frontier:
        nxt = []
        for node in frontier:
            for party, stmt, label in owners_of.get(node, []):
                ordered.append((party, node, stmt, label))
                if party not in seen:
                    seen.add(party)
                    level[party] = level[node] + 1
                    nxt.append(party)
        frontier = nxt

    figures: dict[str, list[str]] = {}
    for h in io.with_figures(net.payload):
        if h.get("status") == io.STATUS_CURRENT:
            figures[c(str(h.get("party")))] = [
                f"{f.get('text')} ({f.get('source')})"
                for f in h.get("figures") or []
                if f.get("minimum") is not None
            ]

    rows = []
    for party, owned, stmt, label in ordered:
        life = statement_lifecycle(stmt, as_of)
        lines, _cat = _edge_label(stmt)
        rows.append({
            "owner": party_name(by_canon.get(party)) or party,
            "owner_kind": (
                "person" if (by_canon.get(party) or {}).get("recordType") == "person" else "entity"
            ),
            "interest": " · ".join(x for line in lines for x in line),
            "owned": party_name(by_canon.get(owned)) or owned,
            "source": label,
            "status": "ended" if life.ended else "current",
        })

    # The image draws current edges only (an ended one is in the table, marked
    # "ended"), one per owner–owned–source–wording, nearest layers first.
    drawable: list[tuple[str, str, dict[str, Any], str]] = []
    seen_edges: set[tuple[str, str, str, str]] = set()
    for party, owned, stmt, label in ordered:
        if statement_lifecycle(stmt, as_of).ended:
            continue
        lines, _cat = _edge_label(stmt)
        key = (party, owned, label, repr(lines))
        if key in seen_edges:
            continue
        seen_edges.add(key)
        drawable.append((party, owned, stmt, label))
    shown = drawable[:MAP_MAX_EDGES]
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    for party, owned, stmt, label in shown:
        for n in (party, owned):
            if n not in nodes:
                st = by_canon.get(n)
                sub = "; ".join(figures.get(n, [])[:2])
                nodes[n] = {"kind": _node_kind(st), "label": _node_label(st) if st else n,
                            "sublabel": sub[:60]}
        lines, cat = _edge_label(stmt)
        lines = [*lines, [label]]
        edges.append({
            "from": party, "to": owned, "lines": lines,
            "label": " · ".join(x for line in lines for x in line),
            "cat": cat, "ended": statement_lifecycle(stmt, as_of).ended,
        })

    svg: str | None = None
    if edges:
        title = f"{legal_name or lei or 'Company'} — who owns it"
        layer = {n: level.get(n, 0) for n in nodes}
        # Subjects named by two sources are one node already (canon); any
        # remaining layer-0 node that points at another is pushed left.
        depth_max = max(layer.values())
        svg, _summ = _render(
            nodes, edges, title, layer=layer, view_w=max(760, 270 + 250 * depth_max)
        )
    reporting_exceptions = []
    for entry in exceptions.values():
        levels = sorted(entry["levels"], key=["direct", "ultimate"].index)
        reporting_exceptions.append({
            "company": party_name(by_canon.get(entry["company"])) or entry["company"],
            "parent_levels": levels,
            "reason": entry["reason"],
            "meaning": entry["meaning"],
        })
    answer = {
        "lei": lei,
        "legal_name": legal_name,
        "source_filter": source,
        "summary": _map_summary(legal_name or lei, len(drawable), reporting_exceptions),
        "edges": rows,
        "edges_total": len(rows),
        "edges_drawn": len(shown),
        "current_edges": len(drawable),
        "omitted_from_image": max(0, len(drawable) - len(shown)),
        "integrated_figures": {
            (party_name(by_canon.get(k)) or k): v for k, v in figures.items() if v
        },
        "depth": {
            "layers_reached": net.depth_reached,
            "extra_lookups": net.hops,
            "hop_limit_reached": net.capped,
        },
        "reporting_exceptions": reporting_exceptions,
        "fullcheck_url": FULLCHECK_URL.format(lei=lei) if lei else None,
        "as_of": as_of,
        "note": (
            "Each edge is one source's relationship, labelled with that source; a "
            "party two sources name appears once with both edges. The image draws "
            "current edges only (at most 16, nearest the company first); every "
            "edge, ended ones included, is in 'edges'. Officers and board roles "
            "— company secretaries included — are left out. A GLEIF reporting "
            "exception names no party, so it is listed under "
            "'reporting_exceptions' rather than drawn; an undisclosed parent "
            "(NON_PUBLIC) is drawn. Holder sublabels are integrated figures, "
            "one per source, never added."
        ),
        "image": "attached as SVG" if svg else "no ownership or control edges to draw",
    }
    return answer, svg


def _map_summary(
    name: str | None, current_edges: int, exceptions: list[dict[str, Any]]
) -> str:
    """One plain sentence on what the map shows — so an empty picture is never
    the whole answer."""
    subject = (name or "the company").rstrip(".")
    if current_edges:
        head = (
            f"{current_edges} current ownership or control "
            f"{'edge' if current_edges == 1 else 'edges'} above {subject}."
        )
    else:
        head = f"No source names a current owner or controller of {subject}."
    parts = [head]
    for e in exceptions:
        levels = " and ".join(e["parent_levels"])
        noun = "parents" if len(e["parent_levels"]) > 1 else "parent"
        meaning = f" — {e['meaning']}" if e.get("meaning") else ""
        parts.append(
            f"GLEIF records a reporting exception for the {levels} {noun} of "
            f"{str(e['company']).rstrip('.')}: {e['reason']}{meaning}. It names no party."
        )
    return " ".join(parts)


def default_as_of() -> str:
    return today_iso()
