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
- edges use the canvas's four kinds, colours and dash patterns (Phase 221):
  ownership blue solid, control orange dotted, role purple dashed,
  unclassified grey — ``EDGE_STYLE`` below, pinned to graphStyle.ts
- each edge is labelled in the canvas's words ("Owns 75–100%", "Controls"),
  wrapped to the room the edge has, with its start year; the register's own
  wording and full dates are in the text-equivalent table
- an ended relationship (closed record, or every interest past its endDate)
  keeps its kind, is drawn in a lighter tint (still 3:1) with a hollow
  arrowhead and says "ended <date>" (Phases 219, 243)
"""

# This module builds long inline-SVG strings; wrapping them to 100 cols would
# hurt readability more than it helps.
# ruff: noqa: E501

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from xml.sax.saxutils import escape

from ..bods.lifecycle import ended_phrase, interest_ended, statement_lifecycle

# Node palette — BOVS node colours.
_PERSON = "#1d9e75"  # person node (green)
_ENTITY = "#0d1b3e"  # entity node (navy)
_UNSPEC = "#888888"  # unspecified party (grey)
_INK = "#1a1a1a"
_MUTE = "#595959"


# --- the on-screen graph's edge vocabulary, mirrored (Phase 221) -------------
#
# The PDF diagram draws the same four edge kinds as the canvas, in the same
# colours and dash patterns, labelled with the same words. Until Phase 221 this
# module knew only "ownership" and "everything else", and drew everything else
# purple and solid — so a PSC's significant influence or control was purple in
# the PDF and orange and dotted on screen.
#
# The values below are copies, not imports: the frontend is TypeScript. They
# are pinned to their originals by ``tests/test_reporting_diagram_parity.py``,
# which parses ``frontend/src/lib/graphStyle.ts`` (``EDGE_STYLE``,
# ``ENDED_EDGE``) and ``frontend/src/lib/bodsGraph.ts`` (``INTEREST_LABELS``,
# ``categorise``, ``buildEdgeLabel``) and fails when either side moves alone.


@dataclass(frozen=True)
class EdgeStyle:
    color: str       # line colour, as drawn
    text_color: str  # label colour, darkened to reach 4.5:1 (WCAG 1.4.3)
    dash: str        # "solid" | "dotted" | "dashed" — the non-colour cue (WCAG 1.4.1)
    name: str        # legend name
    ended_color: str = ""  # an ended edge's line colour, ≥3:1 on white (Phase 243)


#: Mirrors ``EDGE_STYLE`` in ``frontend/src/lib/graphStyle.ts`` (the four kinds a
#: relationship can be; ``possiblySame`` is a canvas-only suggestion edge).
EDGE_STYLE: dict[str, EdgeStyle] = {
    "ownership": EdgeStyle("#3b82f6", "#1d4ed8", "solid", "Ownership", "#5391f7"),
    "control": EdgeStyle("#e65100", "#9a3412", "dotted", "Control", "#ea6d29"),
    "role": EdgeStyle("#7c3aed", "#6d28d9", "dashed", "Role", "#a77bf3"),
    "unknown": EdgeStyle("#888888", "#595959", "solid", "Unclassified", "#929292"),
}

#: The SVG for each dash pattern. A 3-unit round-capped stroke with a near-zero
#: dash reads as dots, as Cytoscape's ``line-style: dotted`` does.
_DASH_ATTRS: dict[str, str] = {
    "solid": "",
    "dotted": ' stroke-dasharray="0.5 6" stroke-linecap="round"',
    "dashed": ' stroke-dasharray="9 6"',
}

#: Mirrors ``categorise()`` in ``frontend/src/lib/bodsGraph.ts``. Precedence is
#: the tuple order: ownership → control → role → unknown.
EDGE_CATEGORY_TYPES: tuple[tuple[str, frozenset[str]], ...] = (
    ("ownership", frozenset({"shareholding", "votingRights"})),
    ("control", frozenset({
        "appointmentOfBoard",
        "otherInfluenceOrControl",
        "controlViaCompanyRulesOrArticles",
        "controlByLegalFramework",
    })),
    ("role", frozenset({"seniorManagingOfficial", "boardMember", "boardChair"})),
)

#: Mirrors ``INTEREST_LABELS`` in ``frontend/src/lib/bodsGraph.ts``.
INTEREST_LABELS: dict[str, str] = {
    "shareholding": "Owns",
    "votingRights": "Controls (votes)",
    "appointmentOfBoard": "Controls (board)",
    "otherInfluenceOrControl": "Controls",
    "controlViaCompanyRulesOrArticles": "Controls (articles)",
    "controlByLegalFramework": "Controls (law)",
    "seniorManagingOfficial": "Director",
    "boardMember": "Board member",
    "boardChair": "Chair",
    "unknownInterest": "Interest (unknown)",
    "unpublishedInterest": "Interest (unpublished)",
    "enjoymentAndUseOfAssets": "Enjoys assets",
    "rightToProfitOrIncomeFromAssets": "Profits from assets",
}

#: Mirrors ``buildEdgeLabel``'s ``labels.slice(0, 2)``.
MAX_INTEREST_LINES = 2

# Phase 219 / 243 — an ended relationship is drawn in its kind's
# ``ended_color`` (a lighter tint held at ≥3:1 on white, WCAG 1.4.11) with a
# hollow arrowhead. Mirrors ``EdgeStyle.endedColor`` and ``ENDED_EDGE.arrowFill``
# in graphStyle.ts (pinned by the parity test). Phase 219's 0.5 stroke-opacity
# measured under 2.3:1. Labels stay at full text contrast (WCAG 1.4.3).
_ENDED_ARROW_FILL = "hollow"
_ENDED_MIN_CONTRAST = 3.0

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


def categorise(interests: list[dict[str, Any]]) -> str:
    """The edge kind for a set of interests — ``categorise()`` in bodsGraph.ts.

    Read from each interest's BODS ``type`` only, never from its free-text
    ``details``: the canvas does not read details, and a PDF that did would
    colour the same edge differently from the screen again."""
    types = {i.get("type") for i in interests}
    for category, members in EDGE_CATEGORY_TYPES:
        if types & members:
            return category
    return "unknown"


def _num(value: Any) -> str:
    """A share bound as JavaScript prints it: ``75.0`` → ``75``."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _first_set(share: dict[str, Any], *keys: str) -> Any:
    """``a ?? b`` — the first key whose value is not None."""
    for k in keys:
        if share.get(k) is not None:
            return share[k]
    return None


def interest_label(interest: dict[str, Any]) -> str:
    """One interest in the canvas's words — ``interestLabel()`` in bodsGraph.ts
    ("Owns 75–100%", "Controls", "Director"). The register's own wording
    (``details``) stays in the text-equivalent table."""
    base = INTEREST_LABELS.get(interest.get("type") or "") or interest.get("type") or "Interest"
    share = interest.get("share")
    if not share or not isinstance(share, dict):
        return base
    owns = base.startswith("Owns")
    verb = "Owns" if owns else "Controls"
    if share.get("exact") is not None:
        rest = (base[4:] if owns else base[8:]).strip()
        return f"{verb} {_num(share['exact'])}%{' ' + rest if rest else ''}".strip()
    lo = _first_set(share, "minimum", "exclusiveMinimum")
    hi = _first_set(share, "maximum", "exclusiveMaximum")
    if lo is not None and hi is not None:
        return f"{verb} {_num(lo)}–{_num(hi)}%"
    return base


def edge_label_lines(interests: list[dict[str, Any]]) -> list[str]:
    """``buildEdgeLabel()``: beneficial interests first, identical labels once,
    at most :data:`MAX_INTEREST_LINES` lines."""
    ordered = sorted(interests, key=lambda i: 0 if i.get("beneficialOwnershipOrControl") else 1)
    lines: list[str] = []
    for i in ordered:
        label = interest_label(i)
        if label not in lines:
            lines.append(label)
    return lines[:MAX_INTEREST_LINES]


def _edge_label(stmt: dict[str, Any]) -> tuple[list[list[str]], str]:
    """The label for one relationship statement, as logical lines of clauses,
    plus its edge kind.

    As on the canvas (``toGraphEdge``), a current relationship is labelled and
    classified from its *current* interests — an ended one pooled onto the same
    record is history, and the table carries it — and an ended relationship
    from all of them, with "ended <date>" as a line of its own. The start year
    stays, as a clause the wrapper can move to its own line; the full dates
    are in the table."""
    interests = list((stmt.get("recordDetails") or {}).get("interests") or [])
    life = statement_lifecycle(stmt)
    shown = interests
    if not life.ended:
        current = [i for i in interests if not interest_ended(i, False)]
        shown = current or interests
    lines: list[list[str]] = [[text] for text in edge_label_lines(shown)] or [["Relationship"]]
    starts = sorted(str(i["startDate"])[:4] for i in shown if i.get("startDate"))
    dates: list[str] = []
    if starts:
        dates.append(f"from {starts[0]}")
    if life.ended:
        dates.append(ended_phrase(life.ended_on))
    if dates:
        lines.append(dates)
    return lines, categorise(shown)


# --- label wrapping ------------------------------------------------------------

#: Average advance of the diagram's 11-unit sans label face, in viewBox units.
#: Deliberately generous so an estimate never undershoots the drawn text.
_LABEL_CHAR_W = 6.3
_LABEL_FONT = 11
_LABEL_LINE_H = 13
#: The narrowest and widest a label may wrap to, in characters.
_LABEL_MIN_CHARS = 14
_LABEL_MAX_CHARS = 30


def wrap_label(lines: list[list[str]], max_chars: int) -> list[str]:
    """Lay out a label's logical lines within ``max_chars`` characters.

    Each logical line is a list of clauses ("from 2022", "ended 4 October
    2024"). Clauses share a line, joined by " · ", while they fit; a clause
    that does not fit starts a new line; a clause longer than the width on its
    own is word-wrapped. A word longer than the width is never cut."""
    out: list[str] = []
    for clauses in lines:
        current = ""
        for clause in clauses:
            joined = f"{current} · {clause}" if current else clause
            if len(joined) <= max_chars:
                current = joined
                continue
            if current:
                out.append(current)
                current = ""
            if len(clause) <= max_chars:
                current = clause
                continue
            for word in clause.split():
                candidate = f"{current} {word}" if current else word
                if len(candidate) <= max_chars or not current:
                    current = candidate
                else:
                    out.append(current)
                    current = word
        if current:
            out.append(current)
    return out


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
            _row_interest_for(s),
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
        life = statement_lifecycle(s)
        lines, category = _edge_label(s)
        edges.append({
            "from": pid,
            "to": sid,
            "lines": lines,
            "label": " · ".join(c for line in lines for c in line),
            "cat": category,
            "ended": life.ended,
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


def _row_interest_for(stmt: dict[str, Any]) -> str:
    """The table row for one relationship statement. The interest text already
    carries each ``endDate`` ("to 2024-11-30"); a closed record that published
    none would otherwise read as current, so it says "ended" (Phase 219)."""
    interests = (stmt.get("recordDetails") or {}).get("interests") or []
    text = _row_interest(interests)
    life = statement_lifecycle(stmt)
    if life.ended and not life.ended_on:
        text = f"{text} (ended)"
    return text


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


def _marker_id(category: str, ended: bool) -> str:
    return f"ar-{category}{'-ended' if ended else ''}"


Box = tuple[float, float, float, float]  # x0, y0, x1, y1


def _overlaps(a: Box, b: Box, pad: float = 2) -> bool:
    return a[0] < b[2] + pad and b[0] < a[2] + pad and a[1] < b[3] + pad and b[1] < a[3] + pad


def _node_boxes(pos: dict[str, tuple[float, float]], nodes: dict) -> list[Box]:
    """The area each node occupies: its disc plus the name (and sublabel) under it."""
    boxes: list[Box] = []
    for n, (cx, cy) in pos.items():
        nd = nodes[n]
        text_w = max(len(nd["label"]) * 6.2, len(nd.get("sublabel") or "") * 5.0, 2 * _R)
        bottom = cy + _R + (34 if nd.get("sublabel") else 20)
        boxes.append((cx - text_w / 2, cy - _R, cx + text_w / 2, bottom))
    return boxes


#: Where along an edge a label may sit, as a fraction of the way from its
#: *fanned* end (the end with fewer edges): tried in order, first clear wins.
_LABEL_POSITIONS = (0.35, 0.5, 0.25, 0.65, 0.2, 0.75)


def _place_label(
    lines: list[list[str]],
    x1: float, y1: float, x2: float, y2: float,
    *,
    from_fanned_start: bool | None,
    avoid: list[Box],
    view: Box,
) -> tuple[list[str], float, float, Box]:
    """Choose where an edge's label goes, and wrap it to the room it has there.

    Phase 221. The label used to be one unwrapped line at the edge's midpoint,
    nudged above or below. Every edge into a subject converges on it, so the
    midpoints of neighbouring edges sit close together, and a long label ran
    under the subject node (BANK SADERAT PLC, Companies House) or over its
    neighbour's label. Now the label is centred *on* the edge, on a white
    backing, nearer the end where edges fan apart; it wraps to the horizontal
    room it has at that point, so it cannot reach either node; and it moves
    along the edge until it clears the nodes and every label already placed.
    If nothing is clear it takes the first position — overlap is then the
    graph's density, and the table under the figure still says everything.
    """
    if from_fanned_start is None:
        order = (0.5, 0.35, 0.65, 0.25, 0.75)
    else:
        order = _LABEL_POSITIONS
    first_choice: tuple[list[str], float, float, Box] | None = None
    for f in order:
        t = f if from_fanned_start in (True, None) else 1 - f
        cx, cy = x1 + t * (x2 - x1), y1 + t * (y2 - y1)
        room = 2 * min(t, 1 - t) * abs(x2 - x1) - 24
        max_chars = max(_LABEL_MIN_CHARS, min(_LABEL_MAX_CHARS, int(room / _LABEL_CHAR_W)))
        wrapped = wrap_label(lines, max_chars)
        w = max(len(line) for line in wrapped) * _LABEL_CHAR_W + 8
        h = (len(wrapped) - 1) * _LABEL_LINE_H + _LABEL_FONT + 5
        box = (cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)
        placed = (wrapped, cx, cy, box)
        if first_choice is None:
            first_choice = placed
        inside = box[0] >= view[0] and box[1] >= view[1] and box[2] <= view[2] and box[3] <= view[3]
        if inside and not any(_overlaps(box, other) for other in avoid):
            return placed
    assert first_choice is not None
    return first_choice


def _label_svg(wrapped: list[str], style: EdgeStyle, box: Box) -> str:
    """A placed label: a white backing and one ``<tspan>`` per line, each with
    its own ``y`` (WeasyPrint lays those out without relying on ``dy``)."""
    x0, y0, x1, y1 = box
    cx = (x0 + x1) / 2
    first = y0 + _LABEL_FONT + 1
    spans = "".join(
        f'<tspan x="{cx:.0f}" y="{first + k * _LABEL_LINE_H:.0f}">{escape(t)}</tspan>'
        for k, t in enumerate(wrapped)
    )
    return (
        f'<rect x="{x0:.0f}" y="{y0:.0f}" width="{x1 - x0:.0f}" height="{y1 - y0:.0f}" rx="3" '
        f'fill="#fff" fill-opacity="0.85"/>'
        f'<text x="{cx:.0f}" y="{first:.0f}" text-anchor="middle" font-size="{_LABEL_FONT}" '
        f'fill="{style.text_color}">{spans}</text>'
    )


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
    # Arrowheads: one per edge kind drawn, and a hollow twin in the lighter
    # ended tint for an ended edge (Phase 219 / 243).
    cats = [c for c in EDGE_STYLE if any(e["cat"] == c for e in edges)]
    defs = ["<defs>"]
    for c in cats:
        for ended in (False, True):
            st = EDGE_STYLE[c]
            head = (
                f'fill="#fff" stroke="{st.ended_color}" stroke-width="1.5"'
                if ended
                else f'fill="{st.color}"'
            )
            defs.append(
                f'<marker id="{_marker_id(c, ended)}" viewBox="-1 -1 12 12" refX="9" refY="5" markerWidth="7" markerHeight="7" '
                f'orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" {head}/></marker>'
            )
    defs.append("</defs>")
    parts.append("".join(defs))
    # Edges first (under nodes).
    degree: dict[str, int] = {}
    for e in edges:
        degree[e["from"]] = degree.get(e["from"], 0) + 1
        degree[e["to"]] = degree.get(e["to"], 0) + 1
    avoid = _node_boxes(pos, nodes)
    view: Box = (0, 0, _VIEW_W, height - 16)  # clear of the legend row
    labels: list[str] = []
    for e in edges:
        (px, py), (sx, sy) = pos[e["from"]], pos[e["to"]]
        style = EDGE_STYLE[e["cat"]]
        stroke = style.ended_color if e.get("ended") else style.color
        x1, x2 = px + _R, sx - _R
        parts.append(
            f'<line x1="{x1:.0f}" y1="{py:.0f}" x2="{x2:.0f}" y2="{sy:.0f}" '
            f'stroke="{stroke}" stroke-width="3"{_DASH_ATTRS[style.dash]} '
            f'marker-end="url(#{_marker_id(e["cat"], bool(e.get("ended")))})"/>'
        )
        dp, ds = degree[e["from"]], degree[e["to"]]
        fanned_start = None if dp == ds else dp < ds
        wrapped, _cx, _cy, box = _place_label(
            e["lines"], x1, py, x2, sy, from_fanned_start=fanned_start, avoid=avoid, view=view
        )
        avoid.append(box)
        labels.append(_label_svg(wrapped, style, box))
    # Nodes, then labels on top: a label is never painted over by a node name,
    # and its backing keeps it legible where it crosses another edge.
    for n, (cx, cy) in pos.items():
        nd = nodes[n]
        parts.append(_node_svg(cx, cy, nd["kind"], nd["label"], nd.get("sublabel", "")))
    parts.extend(labels)
    # Legend — only the kinds this diagram draws, in the canvas legend's names
    # and line styles, laid out left to right.
    ly = height - 6
    lx = 40.0
    entries = [(EDGE_STYLE[c], EDGE_STYLE[c].name, False) for c in cats]
    if any(e.get("ended") for e in edges):
        entries.append((EDGE_STYLE["unknown"], "Ended relationship (lighter, hollow arrowhead)", True))
    for style, text, ended in entries:
        stroke = style.ended_color if ended else style.color
        parts.append(
            f'<line x1="{lx:.0f}" y1="{ly}" x2="{lx + 24:.0f}" y2="{ly}" stroke="{stroke}" '
            f'stroke-width="3"{_DASH_ATTRS[style.dash]}/>'
        )
        if ended:  # the hollow head, drawn inline (a legend sample is not an edge)
            parts.append(
                f'<path d="M{lx + 24:.0f} {ly - 3.5} L{lx + 31:.0f} {ly} L{lx + 24:.0f} {ly + 3.5} z" '
                f'fill="#fff" stroke="{stroke}" stroke-width="1.5"/>'
            )
        tx = lx + (36 if ended else 30)
        parts.append(f'<text x="{tx:.0f}" y="{ly + 4}" font-size="9" fill="{_MUTE}">{escape(text)}</text>')
        lx = tx + len(text) * 5.2 + 24
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
