/**
 * The graph's visual vocabulary, in one place — and the function that turns it
 * into a legend (Phase 124).
 *
 * `SIGNAL_STYLE` and the edge/node entries below used to live inside
 * `BODSGraph.tsx`, next to the Cytoscape stylesheet that consumes them. The
 * legend beside the canvas was a separate, hand-written list of six chips:
 * three edge kinds and three signal families. `SIGNAL_STYLE` defines **thirty**
 * badges — `RS`, `RSC`, `Db`, `REr`, `F!`, `EU!`, `≥3`, `Nm`, `Ex` and the rest
 * — so a node could be drawn with a two-letter mark whose only explanation was
 * a `title` containing the raw code. Node shape carried three more unlabelled
 * distinctions: a dashed border for a person, a blue ring for a collapsed
 * branch, a dashed amber line for "likely same".
 *
 * So the legend is now **generated from the same values the canvas draws**.
 * These constants live here rather than in the component for two reasons: it
 * breaks the import cycle a legend component would otherwise create, and the
 * frontend suite is logic-only (no jsdom), so anything reachable without
 * rendering can be tested — `graphStyle.test.ts` pins that every badge
 * `SIGNAL_STYLE` can draw has a human-readable name and that the legend
 * describes exactly what is on screen.
 *
 * The legend is **scoped to the graph in front of you**: it lists the edge
 * kinds present in this model and the signal codes present on these nodes, not
 * the whole vocabulary. A legend of thirty entries above a four-node diagram
 * explains nothing.
 */

import type { RiskSignal } from "./api";

export interface SignalStyle {
  bg: string;
  border: string;
  text: string;
  /** The 1–3 character mark drawn on the node. Not a label — see `name`. */
  label: string;
  severity: number;
}

export const SIGNAL_STYLE: Record<string, SignalStyle> = {
  SANCTIONED:               { bg:"#ffe4e6", border:"#be123c", text:"#be123c", label:"S",  severity:7 },
  RELATED_SANCTIONED:       { bg:"#ffe4e6", border:"#be123c", text:"#be123c", label:"RS", severity:7 },
  // Counter-sanctions (OpenSanctions `sanction.counter`): a direct listing,
  // but by a regime with weak democratic institutions. Severity 2 and slate
  // — below plain adjacency, and outside the rose/amber ramp entirely.
  COUNTER_SANCTIONED:         { bg:"#f1f5f9", border:"#475569", text:"#334155", label:"CS",  severity:2 },
  RELATED_COUNTER_SANCTIONED: { bg:"#f1f5f9", border:"#475569", text:"#334155", label:"RCS", severity:2 },
  SANCTIONED_SECURITY:      { bg:"#fff1f2", border:"#be123c", text:"#be123c", label:"SS", severity:6 },
  // Owned by a designated party (OpenSanctions `sanction.control`) — its own
  // tier between a direct listing and plain adjacency.
  SANCTIONS_CONTROLLED:         { bg:"#ffe4e6", border:"#9f1239", text:"#9f1239", label:"SC",  severity:6 },
  RELATED_SANCTIONS_CONTROLLED: { bg:"#ffe4e6", border:"#9f1239", text:"#9f1239", label:"RSC", severity:6 },
  SANCTIONS_LINKED:         { bg:"#fef3c7", border:"#b45309", text:"#b45309", label:"SL", severity:3 },
  RELATED_SANCTIONS_LINKED: { bg:"#fef3c7", border:"#b45309", text:"#b45309", label:"RSL", severity:3 },
  DEBARMENT:                { bg:"#ffedd5", border:"#c2410c", text:"#9a3412", label:"Db", severity:4 },
  RELATED_DEBARMENT:        { bg:"#ffedd5", border:"#c2410c", text:"#9a3412", label:"RDb", severity:4 },
  // Export-control family (Phase 118): a listing of the party itself, one
  // tier above debarment and below sanction control; adjacency sits with the
  // sanctions-linked tier, and "Trade risk" (export.risk) one below that.
  EXPORT_CONTROLLED:            { bg:"#ffe4e6", border:"#9f1239", text:"#9f1239", label:"E",   severity:5 },
  RELATED_EXPORT_CONTROLLED:    { bg:"#ffe4e6", border:"#9f1239", text:"#9f1239", label:"RE",  severity:5 },
  EXPORT_CONTROL_LINKED:        { bg:"#fef3c7", border:"#b45309", text:"#b45309", label:"EL",  severity:3 },
  RELATED_EXPORT_CONTROL_LINKED:{ bg:"#fef3c7", border:"#b45309", text:"#b45309", label:"REL", severity:3 },
  EXPORT_RISK:                  { bg:"#fff7ed", border:"#c2410c", text:"#c2410c", label:"Er",  severity:2 },
  RELATED_EXPORT_RISK:          { bg:"#fff7ed", border:"#c2410c", text:"#c2410c", label:"REr", severity:2 },
  FATF_BLACK_LIST:          { bg:"#fee2e2", border:"#991b1b", text:"#991b1b", label:"F!",  severity:5 },
  EU_HIGH_RISK_THIRD_COUNTRY: { bg:"#fee2e2", border:"#b91c1c", text:"#b91c1c", label:"EU!", severity:4 },
  PEP:                      { bg:"#f5f3ff", border:"#6d28d9", text:"#6d28d9", label:"P",  severity:4 },
  RELATED_PEP:              { bg:"#f5f3ff", border:"#6d28d9", text:"#6d28d9", label:"RP", severity:4 },
  COMPLEX_CORPORATE_STRUCTURE: { bg:"#fef2f2", border:"#b91c1c", text:"#b91c1c", label:"CC", severity:3 },
  FATF_GREY_LIST:           { bg:"#fff7ed", border:"#9a3412", text:"#9a3412", label:"Fg", severity:2 },
  // Context, not risk — slate, and the LOWEST severity, so a node is
  // never ranked by the graph on the strength of being non-EU alone.
  NON_EU_JURISDICTION:      { bg:"#f8fafc", border:"#64748b", text:"#475569", label:"N",  severity:0 },
  STATE_CONTROLLED:         { bg:"#fff7ed", border:"#c2410c", text:"#c2410c", label:"St", severity:2 },
  OFFSHORE_LEAKS:           { bg:"#fef3c7", border:"#92400e", text:"#92400e", label:"OL", severity:2 },
  TRUST_OR_ARRANGEMENT:     { bg:"#eef2ff", border:"#4338ca", text:"#4338ca", label:"T",  severity:1 },
  COMPLEX_OWNERSHIP_LAYERS: { bg:"#f0f9ff", border:"#0369a1", text:"#0369a1", label:"≥3", severity:1 },
  POSSIBLE_OBFUSCATION:     { bg:"#fefce8", border:"#854d0e", text:"#854d0e", label:"?",  severity:1 },
  NOMINEE:                  { bg:"#fdf4ff", border:"#7e22ce", text:"#7e22ce", label:"Nm", severity:1 },
  OPAQUE_OWNERSHIP:         { bg:"#f8fafc", border:"#475569", text:"#475569", label:"O",  severity:1 },
  // Context, not risk — a permitted GLEIF reporting exception (no parent to
  // report / parent without an LEI). Lowest severity, like NON_EU_JURISDICTION,
  // so the exception bridge node is never ranked as a warning.
  GLEIF_REPORTING_EXCEPTION: { bg:"#f8fafc", border:"#64748b", text:"#475569", label:"Ex", severity:0 },
};

export const DEFAULT_SIGNAL_STYLE: SignalStyle =
  { bg:"#f1f5f9", border:"#64748b", text:"#64748b", label:"!", severity:0 };

export function signalStyle(code: string): SignalStyle {
  return SIGNAL_STYLE[code] ?? DEFAULT_SIGNAL_STYLE;
}

// ---------------------------------------------------------------------------
// Edge kinds — the same values the Cytoscape stylesheet applies
//
// The exported PDF/HTML diagram (backend/opencheck/reporting/diagram.py) draws
// with copies of EDGE_STYLE (endedColor included) and ENDED_EDGE.arrowFill.
// backend/tests/test_reporting_diagram_parity.py parses this file and fails
// when the two differ, so change both together (Phase 221).
// ---------------------------------------------------------------------------

export type EdgeLegendKind = "ownership" | "control" | "role" | "unknown" | "possiblySame";

export interface EdgeStyle {
  /** Line colour, as drawn. */
  color: string;
  /** Label colour, darkened where the line colour cannot carry text at 4.5:1. */
  textColor: string;
  /** Tint behind the legend chip. */
  tint: string;
  /** The non-colour cue — so the legend is not colour-only (WCAG 1.4.1). */
  dash: "solid" | "dotted" | "dashed";
  /** The line colour of an ENDED relationship of this kind (Phase 243): the
   *  kind's colour mixed towards white, but never below 3:1 on white
   *  (WCAG 1.4.11). `graphStyle.test.ts` and the backend parity test both
   *  measure it. */
  endedColor: string;
  name: string;
  /** What this kind of line asserts, in one clause. */
  meaning: string;
}

export const EDGE_STYLE: Record<EdgeLegendKind, EdgeStyle> = {
  ownership: {
    color: "#3b82f6", endedColor: "#5391f7", textColor: "#1d4ed8", tint: "#eff6ff", dash: "solid",
    name: "Ownership",
    meaning: "a reported holding in the company it points to",
  },
  control: {
    color: "#e65100", endedColor: "#ea6d29", textColor: "#9a3412", tint: "#fdf0e8", dash: "dotted",
    name: "Control",
    meaning: "influence or control reported without a shareholding",
  },
  role: {
    color: "#7c3aed", endedColor: "#a77bf3", textColor: "#6d28d9", tint: "#f5f3ff", dash: "dashed",
    name: "Role",
    meaning: "a directorship or other officer appointment",
  },
  unknown: {
    color: "#888888", endedColor: "#929292", textColor: "#595959", tint: "#f8fafc", dash: "solid",
    name: "Unclassified",
    meaning: "a relationship the source did not categorise",
  },
  possiblySame: {
    color: "#b45309", endedColor: "#b45309", textColor: "#b45309", tint: "#fffbeb", dash: "dashed",
    name: "Likely same entity",
    meaning: "same name and jurisdiction, no shared identifier — review, not a merge",
  },
};

/**
 * An ended relationship (Phase 219) is not a sixth edge kind: an ended
 * shareholding is still ownership. It is a modifier on whichever kind it is —
 * the same dash, a lighter tint of the same colour, a hollow arrowhead, and
 * "ended <date>" as a second label line.
 *
 * Why lighter and not a line style: BOVS has no rule for historical
 * relationships, but its relevance rule lets less relevant parts be drawn "with
 * reduced prominence through tinting or transparency", and its completeness
 * rule forbids leaving them out. Every dash pattern is already spoken for
 * (dotted control, dashed role, dashed amber "likely same").
 *
 * Phase 219 drew it at `line-opacity: 0.5`, which measured 1.75–2.26:1 on
 * white — under the 3:1 WCAG 1.4.11 asks of a line a reader has to see
 * (Opus 5.5 check, A-M2). Phase 243 replaces the opacity with a per-kind
 * `EdgeStyle.endedColor` tint held at ≥ `minContrast`, so the reduction in
 * prominence is now small, and the hollow arrowhead is what says "ended" at a
 * glance (WCAG 1.4.1: not colour alone). The label's date line says it in
 * words, at full text contrast.
 */
export const ENDED_EDGE = {
  /** Cytoscape `target-arrow-fill`; the PDF diagram draws the same. */
  arrowFill: "hollow",
  /** The floor every `endedColor` is held to, against white. */
  minContrast: 3,
  name: "Ended relationship",
  meaning: "drawn lighter with a hollow arrowhead, and the date it ended — the source records that it has ceased",
} as const;

/** WCAG relative luminance of a `#rrggbb` colour. */
function relativeLuminance(hex: string): number {
  const h = hex.replace("#", "");
  const [r, g, b] = [0, 2, 4].map((i) => {
    const c = parseInt(h.slice(i, i + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/** The canvas background every line is measured against. */
export const CANVAS_BACKGROUND = "#ffffff";

/** WCAG contrast ratio of two `#rrggbb` colours (≥ 1); against the canvas
 *  background when `b` is omitted. */
export function contrastRatio(a: string, b: string = CANVAS_BACKGROUND): number {
  const [la, lb] = [relativeLuminance(a), relativeLuminance(b)].sort((x, y) => y - x);
  return (la + 0.05) / (lb + 0.05);
}

// ---------------------------------------------------------------------------
// Node marks — the three distinctions the old legend never named
// ---------------------------------------------------------------------------

export type NodeMark = "person" | "collapsed" | "identityVerified" | "cluster";

export const NODE_MARK: Record<NodeMark, { name: string; meaning: string }> = {
  person: { name: "Person", meaning: "drawn with a dashed outline; companies are solid" },
  collapsed: { name: "Collapsed branch", meaning: "blue ring — select it to open what is underneath" },
  // Phase 203. The name is the whole claim, so the legend chip reads the same
  // words a screen reader hears on the tree row.
  identityVerified: {
    name: "Identity verified with Companies House",
    meaning: "green tick — the register records an identity verification statement for this person",
  },
  // Phase 243. Says what is inside and what is never inside, so a count is
  // not read as "nothing to see here".
  cluster: {
    name: "Grouped siblings",
    meaning:
      "a box counting the leaf companies one source reports under the same parent — press its + to draw them; a node carrying a signal is never grouped, and the text version lists every one",
  },
};

/** The grouped-siblings node (Phase 243) — Cytoscape takes colour strings. */
export const CLUSTER_NODE = {
  width: 150,
  height: 56,
  fill: "#eef1fb",   // oo.soft
  border: "#3d30d4", // oo.blue
  text: "#1a1a2e",   // the node-label ink
} as const;

// ---------------------------------------------------------------------------
// Building the legend
// ---------------------------------------------------------------------------

export interface LegendEntry {
  key: string;
  name: string;
  meaning: string;
}

export interface SignalLegendEntry extends LegendEntry {
  /** The mark drawn on the node, so the legend can be matched to the picture. */
  badge: string;
  style: SignalStyle;
}

export interface EdgeLegendEntry extends LegendEntry {
  style: EdgeStyle;
}

export interface GraphLegendModel {
  edges: EdgeLegendEntry[];
  /** Modifiers that apply across edge kinds — today only "Ended relationship"
   *  (Phase 219), listed only when the graph draws one. */
  edgeModifiers: LegendEntry[];
  nodes: LegendEntry[];
  signals: SignalLegendEntry[];
}

/**
 * The legend for one graph: only what that graph draws.
 *
 * `signalName` is injected rather than imported so this module stays free of
 * component imports — the caller passes `RISK_PRESENTATION`'s lookup. An
 * unmapped code falls back to the de-underscored code, which is still better
 * than the two-letter badge alone.
 */
export function buildGraphLegend({
  edgeCategories,
  signalsByNode,
  hasPeople,
  hasCollapsed,
  hasIdentityVerified = false,
  hasEnded = false,
  hasClusters = false,
  signalName,
}: {
  edgeCategories: Iterable<string>;
  signalsByNode: Map<string, RiskSignal[]>;
  hasPeople: boolean;
  hasCollapsed: boolean;
  /** Any node carries the identity-verification tick (Phase 203). */
  hasIdentityVerified?: boolean;
  /** Any edge has ended (Phase 219). */
  hasEnded?: boolean;
  /** Any sibling group is drawn as one node (Phase 243). */
  hasClusters?: boolean;
  signalName: (code: string) => string;
}): GraphLegendModel {
  const present = new Set(edgeCategories);
  const edges = (Object.keys(EDGE_STYLE) as EdgeLegendKind[])
    .filter((k) => present.has(k))
    .map((k) => ({ key: k, name: EDGE_STYLE[k].name, meaning: EDGE_STYLE[k].meaning, style: EDGE_STYLE[k] }));

  const edgeModifiers: LegendEntry[] = hasEnded
    ? [{ key: "ended", name: ENDED_EDGE.name, meaning: ENDED_EDGE.meaning }]
    : [];

  const nodes: LegendEntry[] = [];
  if (hasPeople) nodes.push({ key: "person", ...NODE_MARK.person });
  if (hasCollapsed) nodes.push({ key: "collapsed", ...NODE_MARK.collapsed });
  if (hasIdentityVerified) nodes.push({ key: "identityVerified", ...NODE_MARK.identityVerified });
  if (hasClusters) nodes.push({ key: "cluster", ...NODE_MARK.cluster });

  // Distinct codes actually badged on a node in this graph, worst first, so the
  // legend reads in the same order as the eye ranks the badges.
  const codes = new Set<string>();
  for (const list of signalsByNode.values()) for (const s of list) codes.add(s.code);
  const signals = [...codes]
    .sort((a, b) => signalStyle(b).severity - signalStyle(a).severity || a.localeCompare(b))
    .map((code) => ({
      key: code,
      badge: signalStyle(code).label,
      name: signalName(code),
      meaning: "",
      style: signalStyle(code),
    }));

  return { edges, edgeModifiers, nodes, signals };
}
