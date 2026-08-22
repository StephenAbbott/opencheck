import { describe, expect, it } from "vitest";
import {
  EDGE_STYLE,
  NODE_MARK,
  SIGNAL_STYLE,
  buildGraphLegend,
  signalStyle,
} from "./graphStyle";
import type { RiskSignal } from "./api";

function sig(code: string, statementId: string): RiskSignal {
  return {
    code,
    summary: `${code} summary`,
    confidence: "high",
    source_id: "opensanctions",
    evidence: { statement_id: statementId },
  } as unknown as RiskSignal;
}

const NAME: Record<string, string> = {
  SANCTIONED: "Sanctioned",
  PEP: "PEP",
  COMPLEX_OWNERSHIP_LAYERS: "Complex ownership layers",
};
const name = (code: string) => NAME[code] ?? code.replace(/_/g, " ");

describe("buildGraphLegend", () => {
  it("describes only what this graph draws", () => {
    // A legend listing all 30 badges above a four-node diagram explains
    // nothing. It is scoped to the edge kinds present and the codes actually
    // badged on a node.
    const legend = buildGraphLegend({
      edgeCategories: ["ownership"],
      signalsByNode: new Map([["s1", [sig("SANCTIONED", "s1")]]]),
      hasPeople: false,
      hasCollapsed: false,
      signalName: name,
    });
    expect(legend.edges.map((e) => e.key)).toEqual(["ownership"]);
    expect(legend.nodes).toEqual([]);
    expect(legend.signals.map((s) => s.key)).toEqual(["SANCTIONED"]);
  });

  it("names the three node marks v1 drew but never labelled", () => {
    // A dashed outline for a person and a blue ring for a collapsed branch were
    // both on screen with nothing anywhere saying so.
    const legend = buildGraphLegend({
      edgeCategories: [],
      signalsByNode: new Map(),
      hasPeople: true,
      hasCollapsed: true,
      signalName: name,
    });
    expect(legend.nodes.map((n) => n.key)).toEqual(["person", "collapsed"]);
    for (const n of legend.nodes) expect(n.meaning.length).toBeGreaterThan(10);
  });

  it("orders signal marks worst first, matching how the badges stack", () => {
    // The canvas draws the worst-severity badge when a node carries several,
    // so the legend must not contradict that ranking.
    const legend = buildGraphLegend({
      edgeCategories: [],
      signalsByNode: new Map([
        ["a", [sig("COMPLEX_OWNERSHIP_LAYERS", "a"), sig("SANCTIONED", "a")]],
        ["b", [sig("PEP", "b")]],
      ]),
      hasPeople: false,
      hasCollapsed: false,
      signalName: name,
    });
    expect(legend.signals.map((s) => s.key)).toEqual([
      "SANCTIONED",
      "PEP",
      "COMPLEX_OWNERSHIP_LAYERS",
    ]);
  });

  it("lists a code once however many nodes carry it", () => {
    const legend = buildGraphLegend({
      edgeCategories: [],
      signalsByNode: new Map([
        ["a", [sig("PEP", "a")]],
        ["b", [sig("PEP", "b")]],
        ["c", [sig("PEP", "c")]],
      ]),
      hasPeople: false,
      hasCollapsed: false,
      signalName: name,
    });
    expect(legend.signals).toHaveLength(1);
  });

  it("still names a badge whose code has no presentation entry", () => {
    // An unmapped code falls back to the de-underscored code — worse than a
    // written label, better than the two-letter badge alone, which is all v1
    // showed.
    const legend = buildGraphLegend({
      edgeCategories: [],
      signalsByNode: new Map([["a", [sig("SOME_NEW_CODE", "a")]]]),
      hasPeople: false,
      hasCollapsed: false,
      signalName: name,
    });
    expect(legend.signals[0].name).toBe("SOME NEW CODE");
    expect(legend.signals[0].badge).toBe("!");
  });

  it("renders nothing at all for an empty graph", () => {
    const legend = buildGraphLegend({
      edgeCategories: [],
      signalsByNode: new Map(),
      hasPeople: false,
      hasCollapsed: false,
      signalName: name,
    });
    expect(legend.edges.length + legend.nodes.length + legend.signals.length).toBe(0);
  });
});

describe("the vocabulary the legend is generated from", () => {
  it("can name every badge SIGNAL_STYLE is able to draw", () => {
    // This is the guarantee the hand-written legend could not make: v1 covered
    // 3 of 30. If a new signal code lands with no name, the legend would show a
    // bare mark, so the failure belongs here rather than on screen.
    const legend = buildGraphLegend({
      edgeCategories: [],
      signalsByNode: new Map(
        Object.keys(SIGNAL_STYLE).map((code, i) => [`n${i}`, [sig(code, `n${i}`)]])
      ),
      hasPeople: false,
      hasCollapsed: false,
      signalName: name,
    });
    expect(legend.signals).toHaveLength(Object.keys(SIGNAL_STYLE).length);
    for (const s of legend.signals) {
      expect(s.name, `no name for ${s.key}`).toBeTruthy();
      expect(s.badge, `no badge for ${s.key}`).toBeTruthy();
    }
  });

  it("gives every edge kind a non-colour cue as well as a colour", () => {
    // Colour alone would fail WCAG 1.4.1 on the legend explaining a diagram
    // that already distinguishes control and role by dash pattern.
    const dashes = new Set(Object.values(EDGE_STYLE).map((e) => e.dash));
    expect(dashes.size).toBeGreaterThan(1);
    for (const [key, e] of Object.entries(EDGE_STYLE)) {
      expect(e.dash, `${key} has no dash cue`).toBeTruthy();
      expect(e.meaning.length, `${key} has no meaning`).toBeGreaterThan(10);
      expect(e.name, `${key} has no name`).toBeTruthy();
    }
  });

  it("keeps the severity ranking the graph stacks badges by", () => {
    // Duplicated from RiskChip.test.ts on purpose — that suite pins the chip
    // palette, this one pins the value the legend sorts on. They must not be
    // free to drift apart.
    expect(signalStyle("SANCTIONED").severity).toBeGreaterThan(
      signalStyle("SANCTIONS_LINKED").severity
    );
    expect(signalStyle("SANCTIONS_LINKED").severity).toBeGreaterThan(
      signalStyle("COUNTER_SANCTIONED").severity
    );
    // Context codes must never outrank a signal with a compliance consequence.
    expect(signalStyle("NON_EU_JURISDICTION").severity).toBe(0);
    expect(signalStyle("GLEIF_REPORTING_EXCEPTION").severity).toBe(0);
  });

  it("gives an unknown code a mark rather than crashing", () => {
    expect(signalStyle("NOT_A_CODE").label).toBe("!");
    expect(signalStyle("NOT_A_CODE").severity).toBe(0);
  });

  it("describes both node marks", () => {
    for (const [key, m] of Object.entries(NODE_MARK)) {
      expect(m.name, key).toBeTruthy();
      expect(m.meaning.length, key).toBeGreaterThan(10);
    }
  });
});
