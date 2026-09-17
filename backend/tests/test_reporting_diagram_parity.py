"""The PDF diagram and the on-screen graph draw edges the same way (Phase 221).

``opencheck/reporting/diagram.py`` renders the exported PDF / HTML diagram in
Python; the canvas is Cytoscape, styled from ``frontend/src/lib/graphStyle.ts``
and labelled by ``frontend/src/lib/bodsGraph.ts``. They cannot share a file, so
from Phase 124 to Phase 220 each carried a comment saying it must move with the
other and nothing pinned them. They drifted: the canvas moved control to orange
and dotted, and the PDF kept drawing every non-ownership edge purple and solid
(seen on production, BANK SADERAT PLC, UK Companies House figure).

This file parses the TypeScript, the way ``test_ra_codes.py`` parses
``raCodes.ts``, and fails when either side changes alone.
"""

from __future__ import annotations

import re
from pathlib import Path

from opencheck.reporting import diagram

_LIB = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib"


def _read(name: str) -> str:
    # bodsGraph.ts carries a stray NUL byte, which makes grep call it binary.
    return (_LIB / name).read_bytes().decode("utf-8", errors="replace").replace("\x00", "")


def _block(source: str, start: str) -> str:
    """The text from ``start`` to the first line that is just ``};`` or ``}``."""
    i = source.index(start)
    m = re.compile(r"^\}(;)?\s*$", re.M).search(source, i)
    assert m, f"no end for {start!r}"
    return source[i:m.end()]


def _ts_edge_styles() -> dict[str, dict[str, str]]:
    body = _block(_read("graphStyle.ts"), "export const EDGE_STYLE")
    out: dict[str, dict[str, str]] = {}
    for kind, entry in re.findall(r"^\s{2}(\w+): \{(.*?)^\s{2}\},", body, re.M | re.S):
        out[kind] = dict(re.findall(r'(\w+): "([^"]*)"', entry))
    return out


def test_every_relationship_edge_kind_matches_graph_style():
    ts = _ts_edge_styles()
    # possiblySame is a canvas-only suggestion edge; every other kind is drawn.
    assert set(ts) - {"possiblySame"} == set(diagram.EDGE_STYLE)
    for kind, py in diagram.EDGE_STYLE.items():
        assert (py.color, py.text_color, py.dash, py.name) == (
            ts[kind]["color"], ts[kind]["textColor"], ts[kind]["dash"], ts[kind]["name"],
        ), f"{kind} differs between diagram.py and graphStyle.ts"


def test_every_dash_pattern_has_an_svg_form():
    assert {s.dash for s in diagram.EDGE_STYLE.values()} <= set(diagram._DASH_ATTRS)


def test_ended_opacity_matches_graph_style():
    body = _block(_read("graphStyle.ts"), "export const ENDED_EDGE")
    m = re.search(r"lineOpacity:\s*([\d.]+)", body)
    assert m and float(m.group(1)) == diagram._ENDED_OPACITY


def test_interest_labels_match_bods_graph():
    body = _block(_read("bodsGraph.ts"), "const INTEREST_LABELS")
    ts = dict(re.findall(r'^\s+(\w+): "([^"]*)",', body, re.M))
    assert ts == diagram.INTEREST_LABELS


def test_edge_categories_match_bods_graph_categorise():
    body = _block(_read("bodsGraph.ts"), "function categorise(")
    # Each `return "<kind>"` is decided by the `i.type === "..."` tests above it.
    pieces = re.split(r'return "(\w+)";', body)  # [seg, kind, seg, kind, …, tail]
    ts = [
        (kind, frozenset(re.findall(r'i\.type === "(\w+)"', segment)))
        for segment, kind in zip(pieces[0:-1:2], pieces[1::2], strict=True)
    ]
    assert ts[-1] == ("unknown", frozenset()), "categorise() should fall through to unknown"
    assert tuple(ts[:-1]) == diagram.EDGE_CATEGORY_TYPES


def test_label_line_cap_matches_build_edge_label():
    body = _block(_read("bodsGraph.ts"), "export function buildEdgeLabel(")
    m = re.search(r"labels\.slice\(0, (\d+)\)", body)
    assert m and int(m.group(1)) == diagram.MAX_INTEREST_LINES
