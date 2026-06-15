"""Render a source's BODS relationships as a BOVS-styled print SVG.

This is the print analogue of the interactive Cytoscape graph. Rather than
screenshotting the canvas (whose BOVS icons / flags / risk badges live in a
separate HTML overlay and are missed by ``cy.png()``), we render a clean,
self-contained SVG straight from the BODS statements — crisp at any size and
natively accessible via ``<title>``/``<desc>``.

Visual language (matches the on-screen BOVS styling):

- person node  → green disc with a person glyph
- entity node  → navy disc with a building glyph
- unspecified  → grey disc with "?"
- ownership interest      → blue edge
- control / management role → purple edge
- each edge is labelled with the interest (type, share band, dates)
"""

# This module builds long inline-SVG strings; wrapping them to 100 cols would
# hurt readability more than it helps.
# ruff: noqa: E501

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from xml.sax.saxutils import escape

# Palette — mirrors frontend BODSGraph edge categories + BOVS node colours.
_OWN = "#1565c0"     # ownership interest (blue)
_CTRL = "#6a1b9a"    # control / management role (purple)
_PERSON = "#1d9e75"  # person node (green)
_ENTITY = "#0d1b3e"  # entity node (navy)
_UNSPEC = "#888888"  # unspecified party (grey)
_INK = "#1a1a1a"
_MUTE = "#595959"

_R = 26              # node radius
_VIEW_W = 760

# A diagram shows at most this many relationships to stay readable; the
# text-equivalent table always lists the full set.
MAX_DIAGRAM_RELATIONSHIPS = 10


@dataclass
class SourceDiagram:
    """A rendered diagram plus the rows for its text-equivalent table."""

    source_name: str
    svg: str
    rows: list[tuple[str, str, str]] = field(default_factory=list)  # (party, interest, subject)
    summary: str = ""  # plain-text description (used as the figure alt / desc)
    omitted: int = 0   # relationships present in `rows` but not drawn (cap overflow)

    @property
    def has_relationships(self) -> bool:
        return bool(self.rows)

    @property
    def shown(self) -> int:
        """How many relationships the diagram actually draws."""
        return max(len(self.rows) - self.omitted, 0)


# --- label / classification helpers -----------------------------------------


def _entity_name(stmt: dict[str, Any]) -> str:
    return (stmt.get("recordDetails") or {}).get("name") or "an entity"


def _person_name(stmt: dict[str, Any]) -> str:
    names = (stmt.get("recordDetails") or {}).get("names") or []
    if names and names[0].get("fullName"):
        return names[0]["fullName"]
    return "an unnamed person"


def _node_kind(stmt: dict[str, Any] | None) -> str:
    if stmt is None:
        return "unspecified"
    return "person" if stmt.get("recordType") == "person" else "entity"


def _node_label(stmt: dict[str, Any] | None) -> str:
    if stmt is None:
        return "Unspecified party"
    return _person_name(stmt) if stmt.get("recordType") == "person" else _entity_name(stmt)


def _party_label(party: Any, by_id: dict[str, dict[str, Any]]) -> str:
    """Resolve a relationship party (statementId, or an unspecified record) to a
    display label — used for the full text-equivalent table."""
    if isinstance(party, dict):
        reason = party.get("reason") or "unspecified"
        return f"Unspecified party ({reason})"
    return _node_label(by_id.get(party))


_OWNERSHIP_TYPES = {"shareholding", "ownership", "ownership-of-shares", "ownershipOfShares"}


def _classify(interests: list[dict[str, Any]]) -> str:
    """Ownership (blue) if any interest is a shareholding/ownership; else control."""
    for i in interests:
        t = (i.get("type") or "").lower()
        d = (i.get("details") or "").lower()
        if any(o.lower() in t for o in _OWNERSHIP_TYPES) or "ownership" in d or "share" in d:
            return "ownership"
    return "control"


def _interest_label(interests: list[dict[str, Any]]) -> str:
    """Compact edge label: detail + share band + start year."""
    if not interests:
        return "interest"
    i = interests[0]
    detail = i.get("details") or i.get("type") or "interest"
    share = i.get("share") or {}
    band = ""
    smin = share.get("exclusiveMinimum", share.get("minimum"))
    smax = share.get("maximum")
    if smin is not None and smax is not None and smin == smax:
        band = f" — {smin}%"
    elif smin is not None and smax == 100:
        band = f" — {smin}%+"
    elif smin is not None or smax is not None:
        band = f" — {smin or 0}–{smax or 100}%"
    year = ""
    start = i.get("startDate")
    if start:
        year = f" · from {str(start)[:4]}"
    return f"{detail}{band}{year}"


# --- SVG primitives ----------------------------------------------------------


def _person_glyph(cx: float, cy: float) -> str:
    return (
        f'<circle cx="{cx}" cy="{cy}" r="{_R}" fill="{_PERSON}"/>'
        f'<circle cx="{cx}" cy="{cy - 8}" r="8" fill="#fff"/>'
        f'<path d="M{cx - 17} {cy + 20} a17 13 0 0 1 34 0 z" fill="#fff"/>'
    )


def _entity_glyph(cx: float, cy: float) -> str:
    x0 = cx - 12
    parts = [
        f'<circle cx="{cx}" cy="{cy}" r="{_R}" fill="{_ENTITY}"/>',
        f'<rect x="{x0}" y="{cy - 14}" width="13" height="28" fill="#fff"/>',
        f'<rect x="{cx + 2}" y="{cy - 8}" width="10" height="22" fill="#fff"/>',
    ]
    for wy in (cy - 10, cy - 4):
        parts.append(f'<rect x="{x0 + 3}" y="{wy}" width="3" height="3" fill="{_ENTITY}"/>')
        parts.append(f'<rect x="{x0 + 8}" y="{wy}" width="3" height="3" fill="{_ENTITY}"/>')
    return "".join(parts)


def _unspec_glyph(cx: float, cy: float) -> str:
    return (
        f'<circle cx="{cx}" cy="{cy}" r="{_R}" fill="{_UNSPEC}"/>'
        f'<text x="{cx}" y="{cy + 5}" text-anchor="middle" font-size="16" fill="#fff">?</text>'
    )


def _node_svg(cx: float, cy: float, kind: str, label: str, sublabel: str = "") -> str:
    glyph = {"person": _person_glyph, "entity": _entity_glyph}.get(kind, _unspec_glyph)(cx, cy)
    lab = f'<text x="{cx}" y="{cy + _R + 16}" text-anchor="middle" font-size="11" fill="{_INK}">{escape(label)}</text>'
    sub = ""
    if sublabel:
        sub = f'<text x="{cx}" y="{cy + _R + 30}" text-anchor="middle" font-size="9" fill="{_MUTE}">{escape(sublabel)}</text>'
    return glyph + lab + sub


# --- layout + render ---------------------------------------------------------


def source_diagram(
    rel_statements: list[dict[str, Any]],
    by_id: dict[str, dict[str, Any]],
    *,
    source_name: str,
) -> SourceDiagram:
    """Build a diagram from one source's relationship statements.

    ``rel_statements`` are the BODS ``relationship`` records attributed to this
    source; ``by_id`` maps every statementId in the bundle to its statement so
    party/subject references resolve to labels and node kinds.
    """
    # The text-equivalent table lists every relationship, regardless of the cap.
    rows: list[tuple[str, str, str]] = [
        (
            _party_label((s.get("recordDetails") or {}).get("interestedParty"), by_id),
            _row_interest((s.get("recordDetails") or {}).get("interests") or []),
            _party_label((s.get("recordDetails") or {}).get("subject"), by_id),
        )
        for s in rel_statements
    ]

    # The diagram draws at most MAX_DIAGRAM_RELATIONSHIPS to stay readable.
    shown = rel_statements[:MAX_DIAGRAM_RELATIONSHIPS]
    omitted = len(rel_statements) - len(shown)

    # Collect nodes and edges (from the capped subset only).
    nodes: dict[str, dict[str, Any]] = {}   # id -> {kind, label, sublabel}
    edges: list[dict[str, Any]] = []
    unspec_seq = 0

    def node_for(party: Any) -> str:
        nonlocal unspec_seq
        if isinstance(party, dict):  # unspecified {reason}
            unspec_seq += 1
            nid = f"_unspec{unspec_seq}"
            reason = party.get("reason") or "unspecified"
            nodes[nid] = {"kind": "unspecified", "label": "Unspecified party", "sublabel": reason}
            return nid
        stmt = by_id.get(party)
        if party not in nodes:
            sub = ""
            if stmt and stmt.get("recordType") == "entity":
                idents = (stmt.get("recordDetails") or {}).get("identifiers") or []
                if idents:
                    first = idents[0]
                    sub = f"{first.get('scheme', '')} {first.get('id', '')}".strip()
            nodes[party] = {"kind": _node_kind(stmt), "label": _node_label(stmt), "sublabel": sub}
        return party

    for s in shown:
        rd = s.get("recordDetails") or {}
        pid = node_for(rd.get("interestedParty"))
        sid = node_for(rd.get("subject"))
        interests = rd.get("interests") or []
        edges.append({
            "from": pid,
            "to": sid,
            "label": _interest_label(interests),
            "cat": _classify(interests),
        })

    if not edges:
        # Entity-only (e.g. GLEIF with no parent): draw the subject alone.
        diagram = _entity_only_diagram(by_id, source_name)
        diagram.rows = rows  # keep any rows (normally empty here)
        return diagram

    svg, summary = _render(nodes, edges, source_name)
    return SourceDiagram(
        source_name=source_name, svg=svg, rows=rows, summary=summary, omitted=omitted
    )


def _row_interest(interests: list[dict[str, Any]]) -> str:
    """A fuller interest description for the text-equivalent table."""
    if not interests:
        return "Relationship"
    parts = []
    for i in interests:
        detail = i.get("details") or i.get("type") or "interest"
        share = i.get("share") or {}
        smin = share.get("exclusiveMinimum", share.get("minimum"))
        smax = share.get("maximum")
        band = ""
        if smin is not None and smax is not None and smin == smax:
            band = f", {smin}%"
        elif smin is not None and smax == 100:
            band = f", {smin}% or more"
        elif smin is not None or smax is not None:
            band = f", {smin or 0}–{smax or 100}%"
        dates = []
        if i.get("startDate"):
            dates.append(f"from {i['startDate']}")
        if i.get("endDate"):
            dates.append(f"to {i['endDate']}")
        d = f", {', '.join(dates)}" if dates else ""
        parts.append(f"{detail}{band}{d}")
    return "; ".join(parts)


def _layer_nodes(nodes: dict, edges: list) -> dict[str, int]:
    """Longest-path layering: sinks (subjects pointed at, never pointing) = 0."""
    succ: dict[str, list[str]] = {n: [] for n in nodes}
    for e in edges:
        succ[e["from"]].append(e["to"])
    layer: dict[str, int] = {}

    def depth(n: str, seen: frozenset[str]) -> int:
        if n in layer:
            return layer[n]
        outs = [m for m in succ[n] if m not in seen]
        layer[n] = 0 if not outs else 1 + max(depth(m, seen | {n}) for m in outs)
        return layer[n]

    for n in nodes:
        depth(n, frozenset())
    return layer


def _render(nodes: dict, edges: list, source_name: str) -> tuple[str, str]:
    layer = _layer_nodes(nodes, edges)
    max_layer = max(layer.values())
    # Columns: layer 0 (subjects) rightmost.
    right_x, left_x = 620, 130
    span = right_x - left_x
    col_x = {lyr: right_x - (span * lyr / max_layer if max_layer else 0) for lyr in range(max_layer + 1)}

    # Stack nodes within each layer.
    by_layer: dict[int, list[str]] = {}
    for n, lyr in layer.items():
        by_layer.setdefault(lyr, []).append(n)
    rows_max = max(len(v) for v in by_layer.values())
    # Cap the total height so even a full 10-node column fits within one A4 page
    # (at the figure's rendered width, ~980 units ≈ a page's usable height).
    # With few nodes the spacing stays at the comfortable 150.
    max_h = 980
    row_h = min(150, (max_h - 24) / rows_max) if rows_max else 150
    height = max(rows_max * row_h, 150) + 24
    pos: dict[str, tuple[float, float]] = {}
    for lyr, ns in by_layer.items():
        ns.sort(key=lambda n: nodes[n]["label"])
        n_ct = len(ns)
        for i, n in enumerate(ns):
            y = height * (i + 1) / (n_ct + 1)
            pos[n] = (col_x[lyr], y)

    parts: list[str] = [
        f'<svg viewBox="0 0 {_VIEW_W} {int(height)}" role="img" '
        f'aria-labelledby="dt ds" xmlns="http://www.w3.org/2000/svg">',
        f'<title id="dt">{escape(source_name)} — ownership and control diagram</title>',
    ]
    # Build accessible description.
    summary = _summary(nodes, edges)
    parts.append(f'<desc id="ds">{escape(summary)}</desc>')
    parts.append(
        '<defs>'
        f'<marker id="aro" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="{_OWN}"/></marker>'
        f'<marker id="arc" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="{_CTRL}"/></marker>'
        '</defs>'
    )
    # Edges first (under nodes).
    for e in edges:
        (px, py), (sx, sy) = pos[e["from"]], pos[e["to"]]
        colour = _OWN if e["cat"] == "ownership" else _CTRL
        marker = "aro" if e["cat"] == "ownership" else "arc"
        x1, x2 = px + _R, sx - _R
        parts.append(
            f'<line x1="{x1:.0f}" y1="{py:.0f}" x2="{x2:.0f}" y2="{sy:.0f}" '
            f'stroke="{colour}" stroke-width="3" marker-end="url(#{marker})"/>'
        )
        mx, my = (x1 + x2) / 2, (py + sy) / 2
        # Nudge the label off the line: above when the edge rises, below when it falls.
        dy = -8 if sy <= py else 18
        parts.append(
            f'<text x="{mx:.0f}" y="{my + dy:.0f}" text-anchor="middle" font-size="11" '
            f'fill="{colour}">{escape(e["label"])}</text>'
        )
    # Nodes.
    for n, (cx, cy) in pos.items():
        nd = nodes[n]
        parts.append(_node_svg(cx, cy, nd["kind"], nd["label"], nd.get("sublabel", "")))
    # Legend.
    cats = {e["cat"] for e in edges}
    ly = height - 6
    leg = []
    if "ownership" in cats:
        leg.append((_OWN, "ownership interest", 40))
    if "control" in cats:
        leg.append((_CTRL, "control / management role", 220))
    for colour, text, lx in leg:
        parts.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 24}" y2="{ly}" stroke="{colour}" stroke-width="3"/>')
        parts.append(f'<text x="{lx + 30}" y="{ly + 4}" font-size="9" fill="{_MUTE}">{escape(text)}</text>')
    parts.append("</svg>")
    return "".join(parts), summary


def _entity_only_diagram(by_id: dict, source_name: str) -> SourceDiagram:
    # Pick the first entity statement as the lone node (best-effort).
    subj = next((s for s in by_id.values() if s.get("recordType") == "entity"), None)
    label = _node_label(subj)
    svg = (
        '<svg viewBox="0 0 760 150" role="img" aria-labelledby="dt ds" xmlns="http://www.w3.org/2000/svg">'
        f'<title id="dt">{escape(source_name)} — {escape(label)}</title>'
        f'<desc id="ds">{escape(source_name)} reports the entity with no ownership or control relationships.</desc>'
        + _node_svg(150, 70, "entity", label)
        + f'<text x="248" y="66" font-size="11" fill="{_MUTE}">No ownership or control relationships reported</text>'
        + f'<text x="248" y="82" font-size="11" fill="{_MUTE}">by this source; entity record only.</text>'
        "</svg>"
    )
    return SourceDiagram(source_name=source_name, svg=svg, rows=[], summary=(
        f"{source_name} reports {label} with no ownership or control relationships."
    ))


def _summary(nodes: dict, edges: list) -> str:
    bits = []
    for e in edges:
        bits.append(f"{nodes[e['from']]['label']} — {e['label']} — {nodes[e['to']]['label']}.")
    return " ".join(bits)
