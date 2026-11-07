"""The MCP ownership map's layout (MCP testing, 7 Nov 2026, issue 5).

Rosneft's map drew fifteen owners in a 980-unit column: names overprinted the
next node's disc, the Arabic label sat on the legend, and the two edges where
Rosneft owns one of its own owners were drawn straight back into the owner
column, on top of the owners' edges. The map's canvas now grows with its
column; a return edge is a curve; a figure sublabel gets a line per source.
The PDF figure keeps its A4 cap.
"""

from __future__ import annotations

import asyncio
import re

from opencheck import owners as O
from opencheck.reporting.diagram import _R, _node_extent, _render

AS_OF = "2026-11-07"


def src(s):
    return {"description": {"opensanctions": "OpenSanctions", "climatetrace": "Global Energy Monitor / Climate TRACE"}[s],
            "opencheckSourceId": s}


def ent(i, n, s="opensanctions"):
    return {"statementId": i, "recordId": i, "recordType": "entity",
            "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": n}, "source": src(s)}


def rel(i, subj, party, share, s="opensanctions"):
    return {"statementId": i, "recordId": i, "recordType": "relationship",
            "recordDetails": {"isComponent": False, "subject": subj, "interestedParty": party,
                              "interests": [{"type": "shareholding", "share": {"exact": share}}]},
            "source": src(s)}


def the_map(bods):
    async def no_hop(lei, anchor):
        raise AssertionError

    net = asyncio.run(O.grow(list(bods), ["T"], None, O.resolve_rule("RU"), 1, no_hop, AS_OF, None))
    return O.ownership_map(lei="LEI", legal_name="Target", net=net, as_of=AS_OF)


def _fifteen_owners():
    bods = [ent("T", "Target")]
    for k in range(15):
        bods += [ent(f"O{k}", f"Owner number {k} with a long registered name"), rel(f"r{k}", "T", f"O{k}", 5.0)]
    return bods


def _circles(svg):
    return [(float(x), float(y)) for x, y in re.findall(r'<circle cx="([\d.]+)" cy="([\d.]+)"', svg)]


def test_a_full_column_is_spaced_one_whole_node_apart():
    _ans, svg = the_map(_fifteen_owners())
    owners_col = sorted(y for x, y in _circles(svg) if x < 300)
    assert len(owners_col) == 15
    node = {"label": "Owner number 0 with a long registered name", "sublabel": "5% (OpenSanctions)"}
    gaps = [b - a for a, b in zip(owners_col, owners_col[1:])]
    assert min(gaps) >= _R + _node_extent(node)


def test_the_legend_sits_below_the_last_node():
    _ans, svg = the_map(_fifteen_owners())
    height = float(re.search(r'viewBox="0 0 \d+ ([\d.]+)"', svg).group(1))
    last = max(y for _x, y in _circles(svg))
    node = {"label": "Owner number 14 with a long registered name", "sublabel": "5% (OpenSanctions)"}
    legend_y = float(re.findall(r'<line x1="40" y1="([\d.]+)"', svg)[-1])
    assert last + _node_extent(node) < legend_y - 4 <= height


def test_a_cross_holding_is_a_curve_and_is_said():
    # Rosneft owns 100% of RN-NeftKapitalInvest, which owns 9.6% of Rosneft.
    bods = [ent("T", "Target"), ent("N", "RN-NeftKapitalInvest LLC"),
            rel("up", "T", "N", 9.6), rel("down", "N", "T", 100.0)]
    ans, svg = the_map(bods)
    curves = re.findall(r'<path d="M[\d.]+ [\d.]+ Q', svg)
    assert len(curves) == 1
    assert "1 edge where the company holds part of one of its own owners" in ans["summary"]
    assert "is drawn as a curved arrow" in ans["summary"]


def test_two_sources_figures_get_a_line_each():
    bods = [ent("T", "Target"), ent("G", "Government of Estonia"),
            rel("a", "T", "G", 100.0, "opensanctions"), rel("b", "T", "G", 100.0, "climatetrace")]
    _ans, svg = the_map(bods)
    assert svg.count(">100% (OpenSanctions)<") == 1
    assert svg.count(">100% (Global Energy Monitor / Climate TRACE)<") == 1


def test_the_pdf_figure_keeps_its_a4_cap():
    nodes = {f"n{k}": {"kind": "entity", "label": f"Party {k}", "sublabel": ""} for k in range(30)}
    nodes["s"] = {"kind": "entity", "label": "Subject", "sublabel": ""}
    edges = [{"from": f"n{k}", "to": "s", "lines": [["Owns"]], "label": "Owns", "cat": "ownership", "ended": False}
             for k in range(30)]
    svg, _ = _render(nodes, edges, "PDF")
    height = float(re.search(r'viewBox="0 0 \d+ ([\d.]+)"', svg).group(1))
    assert height <= 980 + 24


# --- the post-deploy re-test, 7 Nov 2026 -------------------------------------

_LABEL = re.compile(
    r'<rect x="(-?\d+)" y="(-?\d+)" width="(\d+)" height="(\d+)" rx="3"[^>]*/><text[^>]*>(.*?)</text>'
)


def _label_boxes(svg):
    return [(int(x), int(y), int(x) + int(w), int(y) + int(h), body)
            for x, y, w, h, body in _LABEL.findall(svg)]


def _rosneft_shape():
    """Rosneft's shape: owners from two sources, two of them held back by the
    company (RAO Rosneftegazstroy with no share stated, RN-NeftKapitalInvest
    at 100%), the second also named — as a separate record — by GEM."""
    bods = [ent("T", "Target")]
    names = ["BP Russian Investments Ltd", "National Settlement Depository JSC",
             "QH Oil Investments LLC", "Rosneftegaz JSC"]
    for k, n in enumerate(names):
        bods += [ent(f"G{k}", n, "climatetrace"), rel(f"g{k}", "T", f"G{k}", 10.0 + k, "climatetrace")]
    bods += [ent("RAO", "RAO Rosneftegazstroy"), ent("RN", "RN-NeftKapitalInvest LLC"),
             ent("RNG", "RN-NeftKapitalInvest LLC", "climatetrace"),
             {**rel("raoup", "T", "RAO", 0), "recordDetails": {
                 **rel("raoup", "T", "RAO", 0)["recordDetails"], "interests": [{"type": "shareholding"}]}},
             rel("rnup", "T", "RN", 9.6), rel("rngup", "T", "RNG", 9.6, "climatetrace"),
             {**rel("raodown", "RAO", "T", 0), "recordDetails": {
                 **rel("raodown", "RAO", "T", 0)["recordDetails"], "interests": [{"type": "shareholding"}]}},
             rel("rndown", "RN", "T", 100.0)]
    return bods


def test_a_cross_holding_label_clears_every_other_label():
    _ans, svg = the_map(_rosneft_shape())
    assert len(re.findall(r'<path d="M[\d.]+ [\d.]+ Q', svg)) == 2
    boxes = _label_boxes(svg)
    for i, a in enumerate(boxes):
        for b in boxes[i + 1:]:
            clear = a[2] <= b[0] or b[2] <= a[0] or a[3] <= b[1] or b[3] <= a[1]
            assert clear, (a[4][:60], b[4][:60])


def test_a_wrapped_label_steps_one_whole_line_height():
    # A bundled two-source label wraps to several lines; each must sit 13
    # units below the last (12 and 14 alternately let an 11-unit face touch).
    bods = [ent("T", "Target"), ent("P", "Parent"),
            rel("a", "T", "P", 100.0, "opensanctions"), rel("b", "T", "P", 100.0, "climatetrace")]
    _ans, svg = the_map(bods)
    for _x0, _y0, _x1, _y1, body in _label_boxes(svg):
        ys = [int(y) for y in re.findall(r'<tspan x="\d+" y="(\d+)"', body)]
        assert all(b - a == 13 for a, b in zip(ys, ys[1:])), ys
