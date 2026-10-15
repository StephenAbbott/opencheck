"""Integrated ownership of a group of owners in one company (Phase 338).

The arithmetic is T-Rank's, as its white papers publish it (the GROW and
integrated-ownership papers, docs.trank.no/white_papers): multiply along a
path, add across paths, and give a company's own shares (held by itself
directly or round a cycle) back to its other holders pro rata. GROW measures
what a *group* owns without counting a share twice: the group is collapsed
into one node before the sum is taken, so a ministry's holding in a state
holding company is internal to the state, not a second stake.

Pure: edges in, an interval out. It never reads a statement — the caller
decides which relationships count (current only, one source's bundle) — so
the FullCheck path figure can reuse it later.

Rules that keep the figure honest, each one a decision (Stephen, 15 Oct 2026):

* **An interval, never a point it does not have.** A banded share (UK PSC's
  ``exclusiveMinimum: 75, maximum: 100``) carries both ends through the sum;
  integrated ownership rises with every edge weight, so the low ends give a
  lower bound and the high ends an upper bound.
* **A missing share is not a stand-in.** An edge with no stated share adds
  nothing to the lower bound and leaves the upper bound unknown, so the
  figure reads "at least X%" (``incomplete``). T-Rank's own table of
  category stand-ins ("wholly owned" = 98%) is deliberately not copied.
* **No figure rather than an approximation.** A cycle the iteration cannot
  settle, or a group whose direct holdings in one company add up to more than
  100% (the same stake named twice by one source), gives no figure and says
  why in ``reason``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

#: Iterations for the fixed point. Ownership graphs above one company are
#: small; a cycle whose weights keep it from settling in this many steps is
#: reported, not approximated.
MAX_ITERATIONS = 500
TOLERANCE = 1e-12
#: Floating-point slack when testing "more than 100%".
_OVER_100 = 1.0 + 1e-6

REASON_NO_PATH = "no_path"
REASON_NO_SHARES = "no_shares"
REASON_OVER_100 = "shares_exceed_100"
REASON_NOT_CONVERGED = "cycle_not_settled"
REASON_SELF_OWNED = "self_owned"

#: Words for each reason, for an evidence sentence.
REASON_WORDS = {
    REASON_NO_PATH: "no current ownership path reaches the company",
    REASON_NO_SHARES: "no share is stated on any path",
    REASON_OVER_100: "the stated holdings add up to more than 100%",
    REASON_NOT_CONVERGED: "a circular holding could not be resolved",
    REASON_SELF_OWNED: "a company on the path holds all of its own shares",
}


@dataclass(frozen=True)
class Edge:
    """``owner`` holds ``low``–``high`` (fractions, 0–1) of ``owned``.

    ``high is None`` means the share is not stated (or stated with no upper
    end); ``low`` is then 0 or the stated lower end."""

    owner: str
    owned: str
    low: float
    high: float | None


@dataclass
class Integrated:
    """The group's integrated ownership of the target, as percentages."""

    minimum: float | None = None
    maximum: float | None = None
    incomplete: bool = False
    reason: str | None = None
    #: Statement-free description of the route: every node a counted path
    #: passes through, in no particular order (the caller maps ids to names).
    via: list[str] = field(default_factory=list)

    @property
    def has_figure(self) -> bool:
        return self.minimum is not None and (self.minimum > 0 or self.maximum is not None)

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"method": "grow"}
        if self.minimum is not None:
            out["minimum"] = self.minimum
        if self.maximum is not None:
            out["maximum"] = self.maximum
        if (
            self.minimum is not None
            and self.maximum is not None
            and abs(self.minimum - self.maximum) < 1e-9
        ):
            out["exact"] = self.minimum
        if self.incomplete:
            out["incomplete"] = True
        if self.reason:
            out["reason"] = self.reason
        if self.via:
            out["via"] = list(self.via)
        return out


def share_bounds(share: dict[str, Any] | None) -> tuple[float, float | None] | None:
    """A BODS ``share`` object as (low, high) fractions, or None when it
    states nothing. ``exact`` is both ends; ``minimum`` / ``exclusiveMinimum``
    the low end; ``maximum`` / ``exclusiveMaximum`` the high end. An
    exclusive bound is used as its value — the figure is a reading of a band,
    never sharper than the band."""
    if not isinstance(share, dict):
        return None

    def num(*keys: str) -> float | None:
        for k in keys:
            v = share.get(k)
            if v is None or isinstance(v, bool):
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            return max(0.0, min(f, 100.0)) / 100.0
        return None

    exact = num("exact")
    if exact is not None:
        return exact, exact
    lo = num("minimum", "exclusiveMinimum")
    hi = num("maximum", "exclusiveMaximum")
    if lo is None and hi is None:
        return None
    return (lo or 0.0), hi


def _solve(
    group: frozenset[str],
    target: str,
    weights: dict[tuple[str, str], float],
) -> tuple[float | None, set[str], str | None]:
    """Integrated ownership of the collapsed group in ``target`` for one set
    of edge weights. Returns (fraction, nodes on counted paths, reason)."""
    group_node = "\x00group"

    # Collapse: an edge out of any member is an edge out of the group; edges
    # into members (their own owners) and between members are internal.
    w: dict[tuple[str, str], float] = {}
    for (a, b), v in weights.items():
        if b in group:
            continue
        src = group_node if a in group else a
        if src == b and src == group_node:
            continue
        w[(src, b)] = w.get((src, b), 0.0) + v

    # The group's direct holdings in one company cannot pass 100%: when they
    # do, one source has named the same stake twice.
    direct: dict[str, float] = {}
    for (a, b), v in w.items():
        if a == group_node:
            direct[b] = direct.get(b, 0.0) + v
    if any(v > _OVER_100 for v in direct.values()):
        return None, set(), REASON_OVER_100

    # Self-ownership: a company's holding in itself is given back to its
    # other holders pro rata (T-Rank, integrated-ownership paper §3).
    self_share = {b: v for (a, b), v in w.items() if a == b}
    for s in self_share.values():
        if s >= 1.0 - 1e-12:
            return None, set(), REASON_SELF_OWNED
    adj: dict[tuple[str, str], float] = {}
    for (a, b), v in w.items():
        if a == b:
            continue
        s = self_share.get(b, 0.0)
        adj[(a, b)] = v / (1.0 - s) if s else v

    # Only nodes on a path from the group to the target matter.
    out_edges: dict[str, list[tuple[str, float]]] = {}
    in_edges: dict[str, list[tuple[str, float]]] = {}
    for (a, b), v in adj.items():
        out_edges.setdefault(a, []).append((b, v))
        in_edges.setdefault(b, []).append((a, v))
    down: set[str] = set()
    stack = [group_node]
    while stack:
        n = stack.pop()
        for b, _ in out_edges.get(n, ()):
            if b not in down:
                down.add(b)
                stack.append(b)
    up: set[str] = set()
    stack = [target]
    while stack:
        n = stack.pop()
        for a, _ in in_edges.get(n, ()):
            if a not in up:
                up.add(a)
                stack.append(a)
    if target not in down:
        return None, set(), REASON_NO_PATH
    nodes = (down & up) | {target}

    # Fixed point: I(x) = Σ_y I(y)·w(y,x), with I(group) = 1. It converges
    # whenever every company on a cycle has some holder outside the cycle.
    value = {n: 0.0 for n in nodes}
    for _ in range(MAX_ITERATIONS):
        delta = 0.0
        for n in nodes:
            total = 0.0
            for a, v in in_edges.get(n, ()):
                if a == group_node:
                    total += v
                elif a in value:
                    total += value[a] * v
            delta = max(delta, abs(total - value[n]))
            value[n] = total
        if delta < TOLERANCE:
            break
    else:
        return None, set(), REASON_NOT_CONVERGED
    result = value[target]
    if result > _OVER_100:
        return None, set(), REASON_OVER_100
    return min(result, 1.0), nodes - {target}, None


def integrated_ownership(
    edges: Iterable[Edge], group: Iterable[str], target: str
) -> Integrated:
    """How much of ``target`` the ``group`` holds, directly and through
    companies it holds, as a percentage interval.

    One edge per (owner, owned) pair is used: when a source repeats a pair,
    the widest reading is kept (the higher of each end), never the sum —
    two statements of one pair are one holding reported twice."""
    members = frozenset(group)
    pairs: dict[tuple[str, str], Edge] = {}
    for e in edges:
        key = (e.owner, e.owned)
        prev = pairs.get(key)
        if prev is None:
            pairs[key] = e
            continue
        widest: float | None = (
            None if prev.high is None or e.high is None else max(prev.high, e.high)
        )
        pairs[key] = Edge(e.owner, e.owned, max(prev.low, e.low), widest)
    if not members or target in members:
        return Integrated(reason=REASON_NO_PATH)

    low_w = {k: e.low for k, e in pairs.items()}
    lo, lo_nodes, lo_reason = _solve(members, target, low_w)
    if lo_reason == REASON_NO_PATH:
        return Integrated(reason=REASON_NO_PATH)
    if lo is None:
        return Integrated(reason=lo_reason)

    # The high end needs every edge on a counted path to state one. Paths
    # are read from the topology (any edge, stated or not), so an unstated
    # edge on a path makes the figure "at least".
    topo_w = {k: 1.0 for k in pairs}
    _, path_nodes, topo_reason = _solve_topology(members, target, topo_w)
    on_path = path_nodes | members | {target}
    unstated = any(
        e.high is None
        for (a, b), e in pairs.items()
        if a in on_path and b in on_path and b not in members
    )
    hi: float | None = None
    if not unstated:
        high_w = {k: (e.high if e.high is not None else 0.0) for k, e in pairs.items()}
        hi_val, _, hi_reason = _solve(members, target, high_w)
        if hi_val is not None:
            hi = hi_val
        elif hi_reason == REASON_OVER_100:
            hi = 1.0  # a band's high ends can pass 100% together; cap it
        else:
            unstated = True

    if lo <= 0.0 and hi is None:
        return Integrated(reason=REASON_NO_SHARES, incomplete=True, via=sorted(path_nodes))
    return Integrated(
        minimum=round(lo * 100.0, 6),
        maximum=None if hi is None else round(max(hi, lo) * 100.0, 6),
        incomplete=hi is None,
        via=sorted(path_nodes),
    )


def _solve_topology(
    group: frozenset[str], target: str, weights: dict[tuple[str, str], float]
) -> tuple[None, set[str], str | None]:
    """Nodes on any path from the group to the target, whatever the shares."""
    group_node = "\x00group"
    out_edges: dict[str, set[str]] = {}
    in_edges: dict[str, set[str]] = {}
    for (a, b) in weights:
        if b in group or a == b:
            continue
        src = group_node if a in group else a
        out_edges.setdefault(src, set()).add(b)
        in_edges.setdefault(b, set()).add(src)
    down: set[str] = set()
    stack = [group_node]
    while stack:
        n = stack.pop()
        for b in out_edges.get(n, ()):
            if b not in down:
                down.add(b)
                stack.append(b)
    up: set[str] = set()
    stack = [target]
    while stack:
        n = stack.pop()
        for a in in_edges.get(n, ()):
            if a not in up:
                up.add(a)
                stack.append(a)
    if target not in down:
        return None, set(), REASON_NO_PATH
    return None, (down & up) - {target, group_node}, None


def percent_text(value: float) -> str:
    """67 → "67%", 16.666… → "16.67%"."""
    return f"{round(value, 2):g}%"


def figure_text(figure: dict[str, Any] | None) -> str | None:
    """An accumulated figure in words: "67%", "50–75%", "at least 24%",
    "up to 10%"; None when it states nothing."""
    if not figure:
        return None
    lo, hi = figure.get("minimum"), figure.get("maximum")
    if lo is None and hi is None:
        return None
    if lo is not None and hi is not None:
        if round(lo, 2) == round(hi, 2):
            return percent_text(lo)
        if lo <= 0:
            return f"up to {percent_text(hi)}"
        return f"{percent_text(lo)[:-1]}–{percent_text(hi)}"
    if lo is not None and lo > 0:
        return f"at least {percent_text(lo)}"
    return None
