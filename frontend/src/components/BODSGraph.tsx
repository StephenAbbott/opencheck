/**
 * BODSGraph — renders a BODS v0.4 statement bundle as an interactive
 * ownership/control graph using Cytoscape.js + dagre hierarchical layout.
 *
 * The pure BODS → graph transform + hierarchy helpers live in lib/bodsGraph.ts
 * (framework-free, unit-tested); this component owns the Cytoscape instance,
 * the HTML overlay layer (BOVS icons / jurisdiction flags / risk badges /
 * collapse toggles), and the interactive viewport tools:
 *   - zoom, pan, fit — native to Cytoscape;
 *   - search within the graph (highlight + step through matches);
 *   - collapsible parents/subsidiaries (DAG-aware; deep graphs auto-collapse).
 *
 * Node icons and jurisdiction flag overlays are rendered as an HTML layer
 * that sits above the Cytoscape canvas — Cytoscape's canvas background-image
 * system has sub-pixel drift at non-integer zoom levels.
 *
 * BOVS Metadata Overlays spec: jurisdiction flag at the 45° (NE) circumference
 * point; risk badge at 315° (NW); collapse toggle at due-south (270°).
 */

import { useEffect, useMemo, useRef, useState } from "react";
import cytoscape, { type Core, type ElementDefinition, type StylesheetStyle } from "cytoscape";
import dagre from "cytoscape-dagre";
import {
  searchNodes,
  computeVisibility,
  type GraphModel,
  type Visibility,
} from "../lib/bodsGraph";
import type { RiskSignal } from "../lib/api";
// buildSignalMap lives in lib/signalScope.ts (Phase 109) — the badge machinery
// here and the per-source scoping filter must read `evidence` identically, so
// they share one implementation rather than two that can drift.
import { buildSignalMap } from "../lib/signalScope";
import { CLUSTER_NODE, EDGE_STYLE, ENDED_EDGE, signalStyle } from "../lib/graphStyle";
import GraphLegend from "./GraphLegend";
import { IdentityTick } from "./ui/IdentityTick";
import { RISK_PRESENTATION } from "./risk/RiskChip";
import type { SameAsCandidate } from "../lib/reconcile";
import type { LayerControl } from "../lib/fullCheckHeader";
import { Button } from "./ui";
import {
  CANVAS_MAX_HEIGHT,
  canvasHeightFor,
  collapsibleNodes,
  findSiblingClusters,
  hiddenByCluster,
  hitBox,
  resolveMarkCollisions,
  labelFontFor,
  labelsUnreadable,
  LABEL_FONT_PX,
  LABEL_MAX_WIDTH,
  WRAP_MAX_COLS,
  signalPillSize,
  togglePillSize,
  type MarkBox,
  type Rect,
  readingOrder,
  rovingTarget,
  wrapWideRanks,
  type SiblingCluster,
} from "../lib/graphScale";
import { animationMs } from "../lib/motion";

cytoscape.use(dagre);

// ---------------------------------------------------------------------------
// Risk signal → BOVS badge colour (Option C)
// ---------------------------------------------------------------------------

interface NodeOverlay {
  id:      string;
  label:   string;   // node label — disambiguates overlay-button accessible names
  cx:      number;   // screen-space x of node centre
  cy:      number;   // screen-space y of node centre
  r:       number;   // screen-space node radius
  icon:    string;   // base64 data-URI for BOVS entity/person icon
  flagUrl?: string;  // URL for jurisdiction flag SVG (null if no jurisdiction)
  signals?: RiskSignal[];  // risk signals scoped to this node
  hasChildren?: boolean;   // node has downstream subsidiaries (can collapse)
  collapsed?: boolean;     // node is currently collapsed
  hiddenCount?: number;    // descendants hidden because this node is collapsed
  identityVerified?: boolean; // Companies House verified identity → tick at SE (Phase 203)
  clusterId?: string;         // a grouped-siblings node (Phase 243)
  halfW?: number;             // screen half-width when wider than 2r (a cluster box)
}

// The graph's visual vocabulary moved to lib/graphStyle.ts in Phase 124, so
// the legend can be generated from it without importing this component (which
// pulls in Cytoscape). Re-exported here because RiskChip.test.ts and other
// call sites have always imported SIGNAL_STYLE from this module.
export { SIGNAL_STYLE } from "../lib/graphStyle";

/** What a node's collapse / group toggle says. */
function toggleLabel(item: NodeOverlay): string {
  return item.collapsed ? (item.hiddenCount ? `+${item.hiddenCount}` : "+") : "−";
}

/**
 * Every overlay mark's hit box and every node's footprint, for the collision
 * pass (Phase 250). Uses the same size functions the render does, so what is
 * checked is what is drawn.
 */
function overlayGeometry(
  overlays: NodeOverlay[],
  signalMap: Map<string, RiskSignal[]>,
): { marks: MarkBox[]; nodes: (Rect & { owner: string })[] } {
  const marks: MarkBox[] = [];
  const nodes: (Rect & { owner: string })[] = [];
  for (const item of overlays) {
    const hw = item.halfW ?? item.r;
    nodes.push({ owner: item.id, left: item.cx - hw, top: item.cy - item.r, width: hw * 2, height: item.r * 2 });
    if (item.flagUrl && !item.clusterId) {
      const bw = item.r * BADGE_W_FACTOR;
      const bh = item.r * BADGE_H_FACTOR;
      nodes.push({
        owner: item.id,
        left: item.cx + item.r * Math.cos(OVERLAY_ANGLE) - bw / 2,
        top: item.cy - item.r * Math.sin(OVERLAY_ANGLE) - bh / 2,
        width: bw,
        height: bh,
      });
    }
    const sigs = item.signals ?? signalMap.get(item.id);
    if (sigs && sigs.length > 0) {
      const worst = sigs.reduce(
        (best, sg) => (signalStyle(sg.code).severity > signalStyle(best.code).severity ? sg : best),
        sigs[0],
      );
      const text = sigs.length === 1 ? signalStyle(worst.code).label : `${sigs.length} ⚠`;
      const { w, h } = signalPillSize(item.r, text, sigs.length > 1);
      const box = hitBox(item.cx - item.r * Math.cos(OVERLAY_ANGLE), item.cy - item.r * Math.sin(OVERLAY_ANGLE), w, h);
      marks.push({ key: `${item.id}::signal`, owner: item.id, move: "up", ...box });
    }
    if (item.hasChildren) {
      const { w, h } = togglePillSize(item.r, toggleLabel(item));
      marks.push({ key: `${item.id}::toggle`, owner: item.id, move: "down", ...hitBox(item.cx, item.cy + item.r, w, h) });
    }
  }
  return { marks, nodes };
}

// ---------------------------------------------------------------------------
// BODS GraphModel → Cytoscape elements
// ---------------------------------------------------------------------------

function modelToElements(model: GraphModel, sameAs: SameAsCandidate[] = []): ElementDefinition[] {
  const elements: ElementDefinition[] = [];
  const nodeIds = new Set(model.nodes.map((n) => n.id));
  for (const n of model.nodes) {
    elements.push({
      // Highlighting reads provenance, so a matched source lights its node too.
      data: { id: n.id, label: n.label, recordType: n.recordType, icon: n.icon, flagUrl: n.flagUrl, identityVerified: n.identityVerified === true, sources: [...n.sources, ...(n.matchedSources ?? [])] },
    });
  }
  for (const e of model.edges) {
    elements.push({
      data: {
        id: e.id, source: e.source, target: e.target,
        label: e.label, category: e.category, details: e.details, sources: e.sources,
        ended: e.ended === true,
      },
    });
  }
  // POSSIBLY_SAME_AS — dashed, undirected "likely same" suggestion edges. Added
  // here (not in the GraphModel) so they never enter the ownership hierarchy
  // used by collapse/tree/frontier — they are a human-review overlay only.
  for (const c of sameAs) {
    if (!nodeIds.has(c.a) || !nodeIds.has(c.b)) continue;
    elements.push({
      data: {
        id: `sameas~${c.a}~${c.b}`, source: c.a, target: c.b,
        label: "likely same", category: "possiblySame", sources: [],
        details: `Likely the same entity (${c.reason}, no shared identifier) — review before treating as one.`,
      },
    });
  }
  return elements;
}

const DAGRE_LAYOUT = {
  name: "dagre",
  rankDir: "TB", nodeSep: 60, rankSep: 100, edgeSep: 20, animate: false,
} as const;

// ---------------------------------------------------------------------------
// Cytoscape stylesheet — nodes are plain white circles (icons/flags in HTML overlay)
// ---------------------------------------------------------------------------

const STYLESHEET: StylesheetStyle[] = [
  {
    selector: "node",
    style: {
      shape: "ellipse",
      width: 80,
      height: 80,
      "background-color": "#ffffff",
      "border-width": 2,
      "border-color": "#1a1a2e",
      label: "data(label)",
      "text-valign": "bottom",
      "text-halign": "center",
      // Clear the due-south collapse pill (which hangs ~10px below the node)
      // so the entity name underneath stays readable.
      "text-margin-y": 16,
      "font-family": "DM Sans, system-ui, sans-serif",
      "font-size": 11,
      color: "#1a1a2e",
      "text-wrap": "wrap",
      "text-max-width": "120px",
    } as cytoscape.Css.Node,
  },
  {
    selector: "node[recordType = 'person'], node[recordType = 'personStatement']",
    style: { "border-style": "dashed" } as cytoscape.Css.Node,
  },
  // Phase 243 — a group of leaf siblings drawn as one node: a box, not a
  // circle, so it cannot be mistaken for a party, with its count inside.
  {
    selector: "node[recordType = 'cluster']",
    style: {
      shape: "round-rectangle",
      width: CLUSTER_NODE.width,
      height: CLUSTER_NODE.height,
      "background-color": CLUSTER_NODE.fill,
      "border-color": CLUSTER_NODE.border,
      "border-width": 4,
      "border-style": "double",
      "text-valign": "center",
      "text-margin-y": 0,
      "text-max-width": `${CLUSTER_NODE.width - 16}px`,
      "font-size": 12,
      "font-weight": 600,
      color: CLUSTER_NODE.text,
    } as cytoscape.Css.Node,
  },
  {
    selector: "node:selected",
    style: { "border-color": "#3d30d4", "border-width": 3 } as cytoscape.Css.Node,
  },
  // Collapsed node — solid blue ring so it reads as "expandable".
  { selector: "node.collapsed", style: { "border-color": "#3d30d4", "border-width": 3 } as cytoscape.Css.Node },
  // Search highlight / dim
  { selector: "node.search-match", style: { "border-color": "#3d30d4", "border-width": 5 } as cytoscape.Css.Node },
  { selector: "node.search-dim", style: { opacity: 0.3 } as cytoscape.Css.Node },
  { selector: "edge.search-dim", style: { opacity: 0.12 } as cytoscape.Css.Edge },
  // Source highlight / dim (FullCheck provenance legend toggles)
  { selector: "node.src-match", style: { "border-color": "#3d30d4", "border-width": 5 } as cytoscape.Css.Node },
  { selector: "node.src-dim", style: { opacity: 0.2 } as cytoscape.Css.Node },
  { selector: "edge.src-dim", style: { opacity: 0.07 } as cytoscape.Css.Edge },
  // ── Edges ────────────────────────────────────────────────────────────────
  {
    selector: "edge",
    style: {
      width: 1.5,
      "line-color": "#333333",
      "target-arrow-color": "#333333",
      "target-arrow-shape": "triangle",
      "arrow-scale": 1.2,
      "curve-style": "bezier",
      label: "data(label)",
      "text-wrap": "wrap",
      "font-family": "DM Sans, system-ui, sans-serif",
      "font-size": 10,
      color: "#444",
      "text-background-color": "#ffffff",
      "text-background-opacity": 0.85,
      "text-background-padding": "2px",
      "text-border-opacity": 0,
      "edge-text-rotation": "autorotate",
    } as cytoscape.Css.Edge,
  },
  // Ownership takes oo.node.blue -- the FullCheck accent. The network mode's
  // colour and the ownership edge are the same thing said twice, so they are
  // now literally the same value. Label is oo.graph.ownershipText (#1d4ed8):
  // #3b82f6 is 3.0:1 on white, below the 4.5:1 text minimum (WCAG 1.4.3).
  // Phase 124: these read from EDGE_STYLE rather than restating it. The legend
  // beside the canvas is generated from the same object, so a colour change
  // moves both — writing the values twice is exactly the drift the
  // design-system lint exists to stop, and this file had just acquired it.
  ...(["ownership", "control", "role", "unknown"] as const).map((k) => ({
    selector: `edge[category = '${k}']`,
    style: {
      "line-color": EDGE_STYLE[k].color,
      "target-arrow-color": EDGE_STYLE[k].color,
      color: EDGE_STYLE[k].textColor,
      ...(EDGE_STYLE[k].dash === "solid" ? {} : { "line-style": EDGE_STYLE[k].dash }),
    } as cytoscape.Css.Edge,
  })),
  // POSSIBLY_SAME_AS — dashed, undirected; a "likely same entity" suggestion
  // for review, never a merge. Its extra geometry is why it is not in the map
  // above, but its colour still comes from the one place.
  {
    selector: "edge[category = 'possiblySame']",
    style: {
      "line-color": EDGE_STYLE.possiblySame.color, color: EDGE_STYLE.possiblySame.textColor,
      "line-style": "dashed", "curve-style": "bezier",
      "target-arrow-shape": "none", "source-arrow-shape": "none",
      width: 1.5, "font-style": "italic",
    } as cytoscape.Css.Edge,
  },
  // Phase 219 / 243 — an ended relationship keeps its kind's dash, takes the
  // kind's lighter `endedColor` (held at ≥3:1 on white, WCAG 1.4.11 — the
  // 0.5 line-opacity it replaced measured under 2.3:1) and a hollow
  // arrowhead, the non-colour cue. The label (whose second line reads
  // "ended <date>") stays at full text contrast. Placed after the category
  // rules so it composes with their dash.
  ...(["ownership", "control", "role", "unknown"] as const).map((k) => ({
    selector: `edge[?ended][category = '${k}']`,
    style: {
      "line-color": EDGE_STYLE[k].endedColor,
      "target-arrow-color": EDGE_STYLE[k].endedColor,
    } as cytoscape.Css.Edge,
  })),
  {
    selector: "edge[?ended]",
    // The label's white background is dropped too: an autorotated two- or
    // three-line label on a short edge covers most of its length, and over a
    // lighter line that hid the line altogether (seen on the Bank Saderat PLC
    // demo graph).
    style: {
      "target-arrow-fill": ENDED_EDGE.arrowFill,
      "text-background-opacity": 0,
    } as cytoscape.Css.Edge,
  },
  { selector: "edge.hovered",                style: { width: 3, "z-index": 999 } as cytoscape.Css.Edge },
];

// BOVS overlay geometry (fractions of the node radius).
const BADGE_W_FACTOR = 0.75;
const BADGE_H_FACTOR = 0.50;
const OVERLAY_ANGLE = Math.PI / 4;   // 45° diagonal compass point
const ICON_FRACTION = 0.6;           // BOVS icon = 60% of node diameter
const TICK_ANGLE = Math.PI / 6;       // identity tick: 30° below east (screen y grows downward)

/** Phase 243 — the synthetic nodes and edges a sibling cluster draws. */
function clusterElements(clusters: SiblingCluster[]): ElementDefinition[] {
  const out: ElementDefinition[] = [];
  for (const c of clusters) {
    out.push({ data: { id: c.id, label: c.label, recordType: "cluster", sources: [], cluster: true } });
    const [source, target] = c.direction === "below" ? [c.hub, c.id] : [c.id, c.hub];
    out.push({
      data: {
        id: `${c.id}~edge`, source, target,
        label: c.edgeLabel, category: c.category, sources: [],
        details: `${c.label}. Press the node's + to draw them one by one; the text version lists every one.`,
        ended: c.ended,
      },
    });
  }
  return out;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function BODSGraph({
  model,
  signals = [],
  entityName,
  collapsed,
  onCollapsedChange,
  selectedId = null,
  onSelect,
  highlightSource = null,
  sameAs = [],
  layer,
  onAddLayer,
  onSkipToText,
  textVersionId,
}: {
  model: GraphModel;
  signals?: RiskSignal[];
  entityName?: string;
  /** Collapsed node ids (controlled — shared with the tree pane). */
  collapsed: Set<string>;
  onCollapsedChange: (next: Set<string>) => void;
  /** Selected node id (controlled — shared with the tree pane). */
  selectedId?: string | null;
  onSelect?: (id: string | null) => void;
  /** FullCheck provenance: when set, nodes/edges asserted by this source are
   *  highlighted and the rest dimmed (highlight, don't hide). */
  highlightSource?: string | null;
  /** FullCheck: name-only "likely same" candidates → dashed review edges. */
  sameAs?: SameAsCandidate[];
  /** FullCheck progressive discovery: what the single-layer control says, from
   *  `lib/fullCheckHeader`. Omitted (with `onAddLayer`) on a view-only graph —
   *  QuickCheck's panels don't expand. */
  layer?: LayerControl;
  /** Resolve one more layer of the frontier. The control lives here, with the
   *  other canvas controls, rather than above the graph competing with the
   *  mode's primary action. */
  onAddLayer?: () => void;
  /** Phase 243 — open the text equivalent and move focus to it. When given, a
   *  "Skip to text version" link is the first stop in the graph, ahead of
   *  the canvas controls. */
  onSkipToText?: () => void;
  /** The id of the text equivalent, for the skip link's href. */
  textVersionId?: string;
}) {
  const containerRef  = useRef<HTMLDivElement | null>(null);
  const overlayRef    = useRef<HTMLDivElement | null>(null);
  const cyRef         = useRef<Core | null>(null);
  const [overlays, setOverlays] = useState<NodeOverlay[]>([]);
  const [edgeTooltip, setEdgeTooltip] = useState<{ x: number; y: number; text: string } | null>(null);
  // Risk-badge popover — badges are buttons (keyboard-reachable), click toggles
  // the full signal text; Escape dismisses (see effect below).
  const [signalTooltip, setSignalTooltip] =
    useState<{ id: string; x: number; y: number; text: string } | null>(null);
  // Phase 243 — which sibling clusters the reader has opened, and which
  // overlay control holds the one tab stop.
  const [expandedClusters, setExpandedClusters] = useState<Set<string>>(() => new Set());
  const [rovingKey, setRovingKey] = useState<string | null>(null);

  // ── Search state ───────────────────────────────────────────────────────────
  const [query, setQuery] = useState("");
  const [matchIds, setMatchIds] = useState<string[]>([]);
  const [matchIdx, setMatchIdx] = useState(0);
  const [matchSet, setMatchSet] = useState<Set<string> | null>(null);

  // Refs that overlay/effect closures read for current values.
  const collapsedRef = useRef(collapsed);
  collapsedRef.current = collapsed;
  const visRef = useRef<Visibility | null>(null);
  const childrenRef = useRef<Set<string>>(new Set());
  const updateOverlaysRef = useRef<(() => void) | null>(null);
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;

  // statementId → RiskSignal[] — shared by the overlay badges and the table view.
  const signalMap = useMemo(() => buildSignalMap(signals), [signals]);

  // Phase 243 — sibling clusters on wide ranks. A node the badges would mark
  // is never grouped: a finding inside a count is a finding nobody sees.
  // Phase 250 — and on any rank that would need wrapping, once a fit has
  // shown the labels cannot be read at the default threshold ("dense").
  // Sticky while the network grows (FullCheck expansion), cleared when it
  // shrinks — a new subject starts from the default again.
  const [dense, setDense] = useState(false);
  const nodeCountRef = useRef(model.nodes.length);
  useEffect(() => {
    if (model.nodes.length < nodeCountRef.current) setDense(false);
    nodeCountRef.current = model.nodes.length;
  }, [model]);
  const flaggedIds = useMemo(
    () => new Set(model.nodes.filter((n) => signalMap.has(n.id)).map((n) => n.id)),
    [model, signalMap],
  );
  const denseClusters = useMemo(
    () => findSiblingClusters(model, { flagged: flaggedIds, rankThreshold: WRAP_MAX_COLS }),
    [model, flaggedIds],
  );
  const clusters = useMemo(
    () => (dense ? denseClusters : findSiblingClusters(model, { flagged: flaggedIds })),
    [dense, denseClusters, model, flaggedIds],
  );
  const denseRef = useRef({ dense, more: denseClusters.length > clusters.length });
  denseRef.current = { dense, more: denseClusters.length > clusters.length };
  const clusterById = useMemo(() => new Map(clusters.map((c) => [c.id, c])), [clusters]);
  const clusterRef = useRef(clusterById);
  clusterRef.current = clusterById;
  const nodeLabelById = useMemo(() => new Map(model.nodes.map((n) => [n.id, n.label])), [model]);
  const openClusterCount = clusters.filter((c) => expandedClusters.has(c.id)).length;
  const closedClusterCount = clusters.length - openClusterCount;

  function toggleCollapse(id: string) {
    const next = new Set(collapsed);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onCollapsedChange(next);
  }

  function expandCluster(id: string) {
    setExpandedClusters((prev) => new Set(prev).add(id));
  }

  /** Open every cluster that hides one of `ids`; a no-op when none does. */
  function revealInClusters(ids: Iterable<string>) {
    const hidden = hiddenByCluster(clusters, expandedClusters);
    const open = new Set<string>();
    for (const id of ids) {
      const c = hidden.get(id);
      if (c) open.add(c);
    }
    if (open.size === 0) return;
    setExpandedClusters((prev) => new Set([...prev, ...open]));
  }

  // ── Build the Cytoscape instance (rebuilds only when data changes) ─────────
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    if (cyRef.current) { cyRef.current.destroy(); cyRef.current = null; }
    setOverlays([]);

    if (model.nodes.length === 0) {
      el.innerHTML = '<p class="text-xs text-oo-muted p-2 italic">No nodes to visualise.</p>';
      return;
    }

    childrenRef.current = collapsibleNodes(model, collapsedRef.current);

    const cy = cytoscape({
      container: el,
      elements: [...modelToElements(model, sameAs), ...clusterElements(clusters)],
      style: STYLESHEET,
      layout: DAGRE_LAYOUT,
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
      minZoom: 0.2,
      maxZoom: 4,
    });
    cyRef.current = cy;

    function updateOverlays() {
      const pan  = cy.pan();
      const zoom = cy.zoom();
      const collapsedNow = collapsedRef.current;
      const vis = visRef.current;
      const hasChildren = childrenRef.current;
      const next: NodeOverlay[] = [];

      cy.nodes().forEach(node => {
        if (node.style("display") === "none") return; // collapsed away or grouped
        const id = node.id();
        const pos = node.position();
        const cluster = clusterRef.current.get(id);
        next.push({
          id,
          label:   node.data("label") as string,
          cx:      pos.x * zoom + pan.x,
          cy:      pos.y * zoom + pan.y,
          r:       cluster ? (CLUSTER_NODE.height * zoom) / 2 : (node.width() * zoom) / 2,
          halfW:   cluster ? (CLUSTER_NODE.width * zoom) / 2 : undefined,
          icon:    node.data("icon")    as string,
          flagUrl: node.data("flagUrl") as string | undefined,
          signals: signalMap.get(id),
          hasChildren: cluster ? true : hasChildren.has(id),
          collapsed: cluster ? true : collapsedNow.has(id),
          hiddenCount: cluster ? cluster.members.length : vis?.hiddenCount.get(id) ?? 0,
          identityVerified: node.data("identityVerified") === true,
          clusterId: cluster?.id,
        });
      });
      setOverlays(next);
    }
    updateOverlaysRef.current = updateOverlays;

    cy.on("viewport", updateOverlays);

    cy.on("mousemove", "edge", (evt) => {
      const details = evt.target.data("details") as string | undefined;
      if (!details) return;
      evt.target.addClass("hovered");
      el.style.cursor = "pointer";
      const rp = evt.renderedPosition;
      setEdgeTooltip({ x: rp.x, y: rp.y, text: details });
    });
    cy.on("mouseout", "edge", (evt) => {
      evt.target.removeClass("hovered");
      el.style.cursor = "";
      setEdgeTooltip(null);
    });
    cy.on("tap", "edge", (evt) => {
      const details = evt.target.data("details") as string | undefined;
      if (!details) return;
      const rp = evt.renderedPosition;
      setEdgeTooltip(prev => prev?.text === details ? null : { x: rp.x, y: rp.y, text: details });
    });
    cy.on("tap", "node", (evt) => {
      const id = evt.target.id() as string;
      // A cluster is not a party: tapping it draws its members.
      if (clusterRef.current.has(id)) { expandCluster(id); return; }
      onSelectRef.current?.(id);
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) { setEdgeTooltip(null); onSelectRef.current?.(null); }
    });
    cy.on("viewport", () => { setEdgeTooltip(null); setSignalTooltip(null); });

    return () => { cy.destroy(); cyRef.current = null; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, signalMap, sameAs, clusters]);

  /** Fit the drawn graph — only what is displayed, never the hidden members. */
  function fitVisible(cy: Core) {
    cy.fit(cy.elements().filter((e) => e.style("display") !== "none"), 32);
  }

  // ── Apply collapse + clusters: show/hide, re-layout, wrap, size, fit ───────
  useEffect(() => {
    const cy = cyRef.current;
    const el = containerRef.current;
    if (!cy || !el || model.nodes.length === 0) return;

    const vis = computeVisibility(model, collapsed);
    visRef.current = vis;
    childrenRef.current = collapsibleNodes(model, collapsed);
    const grouped = hiddenByCluster(clusters, expandedClusters);

    const shown = new Set<string>();
    for (const id of vis.visible) if (!grouped.has(id)) shown.add(id);
    for (const c of clusters) {
      if (!expandedClusters.has(c.id) && c.members.some((m) => vis.visible.has(m))) shown.add(c.id);
    }

    cy.batch(() => {
      cy.nodes().forEach(n => {
        n.style("display", shown.has(n.id()) ? "element" : "none");
        n.toggleClass("collapsed", collapsed.has(n.id()) && (vis.hiddenCount.get(n.id()) ?? 0) > 0);
      });
      cy.edges().forEach(e => {
        const show =
          shown.has(e.source().id()) &&
          shown.has(e.target().id()) &&
          !collapsed.has(e.source().id()); // a collapsed node hides its downstream edges
        e.style("display", show ? "element" : "none");
      });
    });

    // Labels go back to their stylesheet size before every layout, so a size
    // grown for one fit is never carried into the next (Phase 250).
    cy.nodes().removeStyle("font-size text-max-width");
    const visEles = cy.elements().filter((e) => e.style("display") !== "none");
    visEles.layout(DAGRE_LAYOUT).run();

    // Fold ranks that are all leaves (or all roots) into rows, so a wide rank
    // cannot stretch the canvas back into a strip.
    const visNodes = visEles.nodes();
    const visEdges = visEles.edges();
    const hasOut = new Set(visEdges.map((e) => e.source().id()));
    const hasIn = new Set(visEdges.map((e) => e.target().id()));
    const placed = wrapWideRanks(
      visNodes.map((n) => ({
        id: n.id(), x: n.position("x"), y: n.position("y"),
        leaf: !hasOut.has(n.id()), root: !hasIn.has(n.id()), w: n.width(),
      })),
      {
        aspect: el.clientWidth / CANVAS_MAX_HEIGHT,
        // Phase 250: fold for the canvas the graph will actually be fitted to.
        viewport: { width: el.clientWidth, height: CANVAS_MAX_HEIGHT },
      },
    );
    cy.batch(() => {
      visNodes.forEach((n) => {
        const p = placed.get(n.id());
        if (p) n.position(p);
      });
    });

    // Fit to content: the canvas takes the drawn graph's proportions (within
    // limits) instead of leaving most of a fixed-height box empty.
    const bb = visEles.boundingBox();
    el.style.height = `${canvasHeightFor({ w: bb.w, h: bb.h }, el.clientWidth)}px`;
    cy.resize();
    fitVisible(cy);

    // Phase 250 — readable at Fit. First lever: group the groupable on every
    // wide rank (a second pass, with `dense` set). Second: draw the labels
    // larger in graph units so they render at reading size, up to a cap.
    const { dense: isDense, more } = denseRef.current;
    if (!isDense && more && labelsUnreadable(cy.zoom())) {
      setDense(true);
      return;
    }
    const people = visNodes.filter((n) => n.data("recordType") !== "cluster");
    for (let pass = 0; pass < 2; pass += 1) {
      const font = labelFontFor(cy.zoom());
      if (font <= LABEL_FONT_PX && pass === 0) break;
      people.style({
        "font-size": font,
        "text-max-width": `${Math.round(Math.min(LABEL_MAX_WIDTH * (font / LABEL_FONT_PX), 132))}px`,
      });
      fitVisible(cy);
    }
    updateOverlaysRef.current?.();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, signals, collapsed, clusters, expandedClusters]);

  // ── Apply search over the currently-visible nodes ──────────────────────────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    const vis = visRef.current;
    const visibleNodes = vis ? model.nodes.filter(n => vis.visible.has(n.id)) : model.nodes;
    const ids = searchNodes(visibleNodes, query);
    const active = query.trim().length > 0;
    const set = active ? new Set(ids) : null;

    // A match inside a group is drawn, not counted behind a box.
    if (active && ids.length > 0) revealInClusters(ids);

    cy.batch(() => {
      cy.nodes().forEach(n => {
        n.removeClass("search-match search-dim");
        if (!active) return;
        n.addClass(set!.has(n.id()) ? "search-match" : "search-dim");
      });
      cy.edges().forEach(e => { e.toggleClass("search-dim", active); });
    });

    setMatchIds(ids);
    setMatchIdx(0);
    setMatchSet(set);

    if (active && ids.length > 0) {
      const node = cy.getElementById(ids[0]);
      if (node.nonempty()) cy.animate({ center: { eles: node } }, { duration: animationMs(250) });
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, model, collapsed, expandedClusters, clusters]);

  // ── Provenance: highlight one source, dim the rest (FullCheck legend) ──────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const active = !!highlightSource;
    cy.batch(() => {
      cy.nodes().forEach((n) => {
        n.removeClass("src-match src-dim");
        if (!active) return;
        const srcs = (n.data("sources") as string[] | undefined) ?? [];
        n.addClass(srcs.includes(highlightSource!) ? "src-match" : "src-dim");
      });
      cy.edges().forEach((e) => {
        e.removeClass("src-dim");
        if (!active) return;
        const srcs = (e.data("sources") as string[] | undefined) ?? [];
        if (!srcs.includes(highlightSource!)) e.addClass("src-dim");
      });
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [highlightSource, model, clusters]);

  // ── Reflect the shared selection into the graph (highlight + centre) ───────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.nodes(":selected").unselect();
    if (selectedId) {
      // Selected from the text version while grouped: draw its group first.
      revealInClusters([selectedId]);
      const node = cy.getElementById(selectedId);
      const visible = node.nonempty() && node.style("display") !== "none";
      if (visible) {
        node.select();
        cy.animate({ center: { eles: node } }, { duration: animationMs(250) });
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId, model, expandedClusters, clusters]);

  // ── Dismiss the risk-badge popover / edge tooltip with Escape while open ───
  useEffect(() => {
    if (!signalTooltip && !edgeTooltip) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") { setSignalTooltip(null); setEdgeTooltip(null); }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [signalTooltip, edgeTooltip]);

  function focusMatch(idx: number) {
    const cy = cyRef.current;
    if (!cy || matchIds.length === 0) return;
    const wrapped = ((idx % matchIds.length) + matchIds.length) % matchIds.length;
    setMatchIdx(wrapped);
    const node = cy.getElementById(matchIds[wrapped]);
    if (node.nonempty()) cy.animate({ center: { eles: node } }, { duration: animationMs(250) });
  }

  function zoomBy(factor: number) {
    const cy = cyRef.current;
    if (!cy) return;
    cy.zoom({
      level: cy.zoom() * factor,
      renderedPosition: { x: (containerRef.current?.clientWidth ?? 0) / 2, y: (containerRef.current?.clientHeight ?? 0) / 2 },
    });
  }

  const searching = query.trim().length > 0;
  const resultLabel = !searching
    ? ""
    : matchIds.length === 0
    ? "No matches"
    : `${matchIdx + 1} of ${matchIds.length}`;
  const collapsedCount = collapsed.size;

  // The edge kinds this graph actually draws — the legend lists these and no
  // others, so a four-node diagram is not captioned with the whole vocabulary.
  const edgeCategories = useMemo(() => {
    const cats = new Set(model.edges.map((e) => e.category as string));
    // `possiblySame` edges are synthesised in modelToElements from the
    // `sameAs` prop, so they are drawn on the canvas without ever appearing
    // in model.edges. Reading only model.edges made the legend omit the one
    // edge kind a reader is least likely to guess — and it is the kind that
    // must not be mistaken for a merge.
    if (sameAs.length > 0) cats.add("possiblySame");
    return cats;
  }, [model, sameAs]);

  // Counts for the canvas's accessible name (role="img" — the name is all a
  // screen-reader user perceives of it; the table view is the full equivalent).
  const personCount = model.nodes.filter(
    (n) => n.recordType === "person" || n.recordType === "personStatement"
  ).length;
  const entityCount = model.nodes.length - personCount;
  const endedCount = model.edges.filter((e) => e.ended).length;
  const graphAriaLabel =
    `Ownership structure graph${entityName ? ` for ${entityName}` : ""} — ` +
    `${entityCount} ${entityCount === 1 ? "entity" : "entities"}, ` +
    `${personCount} ${personCount === 1 ? "person" : "people"}, ` +
    `${model.edges.length} ${model.edges.length === 1 ? "relationship" : "relationships"}` +
    // Phase 219 — the lighter line is invisible to a screen reader; say how many.
    (endedCount > 0 ? `, ${endedCount} of them ended` : "") +
    ". " +
    // Phase 243 — nor can a screen reader see that a box stands for many.
    (closedClusterCount > 0
      ? `${closedClusterCount} ${closedClusterCount === 1 ? "group" : "groups"} of sibling companies drawn as one box each. `
      : "") +
    "Open “Read as text” below the diagram for a text equivalent.";

  // ── The overlay's controls, in reading order, as one roving tab stop ───────
  const rovingKeys = useMemo(() => {
    const keys: string[] = [];
    for (const o of readingOrder(overlays)) {
      if (o.signals && o.signals.length > 0) keys.push(`${o.id}::signal`);
      if (o.hasChildren) keys.push(`${o.id}::toggle`);
    }
    return keys;
  }, [overlays]);
  const activeKey = rovingKey && rovingKeys.includes(rovingKey) ? rovingKey : rovingKeys[0] ?? null;

  // Phase 250 — marks keep a pixel floor while nodes shrink with the zoom, so
  // on a wide network they piled into each other. Lift/drop them clear.
  const markOffsets = useMemo(() => {
    const { marks, nodes } = overlayGeometry(overlays, signalMap);
    return resolveMarkCollisions(marks, nodes);
  }, [overlays, signalMap]);

  function onOverlayKeyDown(e: React.KeyboardEvent<HTMLDivElement>) {
    const current = (document.activeElement as HTMLElement | null)?.dataset?.rovingKey;
    if (!current) return;
    const next = rovingTarget(rovingKeys.length, rovingKeys.indexOf(current), e.key);
    if (next === null) return;
    e.preventDefault();
    const key = rovingKeys[next];
    setRovingKey(key);
    const target = overlayRef.current?.querySelector<HTMLElement>(`[data-roving-key="${CSS.escape(key)}"]`);
    target?.focus();
  }

  /** Bring a focused mark's node into view: a control that is focused
   *  off-canvas is a control the reader cannot see (WCAG 2.4.11). */
  function onRovingFocus(key: string, item: NodeOverlay) {
    setRovingKey(key);
    const el = containerRef.current;
    const cy = cyRef.current;
    if (!el || !cy) return;
    const outside = item.cx < 0 || item.cy < 0 || item.cx > el.clientWidth || item.cy > el.clientHeight;
    if (outside) cy.animate({ center: { eles: cy.getElementById(item.id) } }, { duration: animationMs(200) });
  }

  if (model.nodes.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  const toolButton =
    "hover:text-oo-blue min-h-[24px] min-w-[24px] px-2 inline-flex items-center justify-center rounded " +
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-oo-blue";

  return (
    <div className="bg-white border border-oo-rule rounded-oo">
      {/* Toolbar */}
      <div className="border-b border-oo-rule">
        <div className="flex items-center flex-wrap gap-1 px-2 py-1 text-oo-meta text-oo-muted">
          {/* Phase 243 — past the canvas controls to the text equivalent. A
              link, not a button: it goes somewhere. Visible when focused. */}
          {onSkipToText && (
            <a
              href={textVersionId ? `#${textVersionId}` : undefined}
              onClick={(e) => { e.preventDefault(); onSkipToText(); }}
              className="sr-only focus:not-sr-only focus:px-2 focus:py-1 focus:rounded focus:bg-oo-soft focus:text-oo-blue focus:underline"
            >
              Skip to text version
            </a>
          )}
          <button type="button" className={`${toolButton} font-mono`} aria-label="Zoom in" onClick={() => zoomBy(1.3)}>
            +
          </button>
          <button type="button" className={`${toolButton} font-mono`} aria-label="Zoom out" onClick={() => zoomBy(1 / 1.3)}>
            −
          </button>
          <button type="button" className={toolButton}
            onClick={() => { if (cyRef.current) fitVisible(cyRef.current); }}>
            Fit
          </button>
          {collapsedCount > 0 && (
            <button type="button" className={toolButton}
              aria-label={`Expand all ${collapsedCount} collapsed ${collapsedCount === 1 ? "branch" : "branches"}`}
              onClick={() => onCollapsedChange(new Set())}>
              Expand all
            </button>
          )}
          {/* Phase 243 — a group the reader opened can be closed again. */}
          {openClusterCount > 0 && (
            <Button variant="ghost" size="sm" onClick={() => setExpandedClusters(new Set())}>
              Regroup {openClusterCount === 1 ? "siblings" : `${openClusterCount} groups`}
            </Button>
          )}

          {/* Progressive discovery, one hop at a time. This was a full-width
              button above the graph, beside "Run FullCheck" — two blue buttons
              driving the same expansion at different budgets, with nothing on
              screen to say which was the bigger one. It manipulates the canvas,
              so it belongs with the canvas controls; the frontier count comes
              with it as the badge, because it is the one thing this control
              says that "Run FullCheck" cannot. */}
          {layer && onAddLayer && (
            <Button
              // Secondary, never primary (Phase 245): "Run FullCheck" is the
              // panel's one primary action, and the check found three solid
              // blue buttons on one phone screen of FullCheck.
              variant={layer.disabled ? "ghost" : "secondary"}
              size="sm"
              className="ml-1"
              onClick={onAddLayer}
              disabled={layer.disabled}
              aria-label={layer.ariaLabel}
            >
              <span>{layer.label}</span>
              {layer.count !== null && (
                <span className="font-mono opacity-80">{layer.count}</span>
              )}
            </Button>
          )}

          {/* Search-within-graph */}
          <div className="flex items-center gap-1 ml-auto">
              <label htmlFor="bods-graph-search" className="sr-only">Search nodes in the graph</label>
              <input
                id="bods-graph-search"
                type="search"
                value={query}
                placeholder="Search nodes…"
                autoComplete="off"
                className="px-2 py-0.5 min-h-[24px] text-xs border border-oo-rule rounded w-32 sm:w-44"
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") { e.preventDefault(); focusMatch(matchIdx + (e.shiftKey ? -1 : 1)); }
                  else if (e.key === "Escape") { e.preventDefault(); setQuery(""); }
                }}
              />
              <button type="button" className={`${toolButton} font-mono disabled:opacity-30`}
                aria-label="Previous match"
                disabled={matchIds.length === 0} onClick={() => focusMatch(matchIdx - 1)}>
                ‹
              </button>
              <button type="button" className={`${toolButton} font-mono disabled:opacity-30`}
                aria-label="Next match"
                disabled={matchIds.length === 0} onClick={() => focusMatch(matchIdx + 1)}>
                ›
              </button>
                <span role="status" aria-live="polite" className="min-w-[64px] tabular-nums text-oo-meta">
                {resultLabel}
              </span>
          </div>
        </div>
        {/* Legend — generated from lib/graphStyle.ts, scoped to this graph */}
        <GraphLegend
          edgeCategories={edgeCategories}
          signalsByNode={signalMap}
          hasPeople={personCount > 0}
          hasCollapsed={collapsedCount > 0}
          hasIdentityVerified={model.nodes.some((n) => n.identityVerified)}
          hasEnded={endedCount > 0}
          hasClusters={closedClusterCount > 0}
        />
      </div>

      {/* Graph container + HTML overlay. The text equivalent is BodsTree,
          rendered by BodsGraphExplorer beside this canvas — see the WCAG note
          in that component. */}
      <div style={{ position: "relative" }}>
        <div
          ref={containerRef}
          className="overflow-hidden"
          style={{ width: "100%", height: 420 }}
          role="img"
          aria-label={graphAriaLabel}
        />

        {/* Pixel-perfect icon + flag + risk + collapse overlay. Its controls
            are ONE tab stop (roving tabindex, arrow keys between them): on
            Shell's FullCheck there were 36 of them in the tab order ahead of
            the text version (Opus 5.5 check, A-M1). */}
        <div
          ref={overlayRef}
          role={rovingKeys.length > 0 ? "toolbar" : undefined}
          aria-label={rovingKeys.length > 0
            ? `Marks on the diagram — ${rovingKeys.length} ${rovingKeys.length === 1 ? "control" : "controls"}; arrow keys move between them`
            : undefined}
          onKeyDown={onOverlayKeyDown}
          style={{ position: "absolute", inset: 0, overflow: "hidden", pointerEvents: "none" }}
        >
          {overlays.map(item => {
            const iconSize = item.r * 2 * ICON_FRACTION;
            const bw = item.r * BADGE_W_FACTOR;
            const bh = item.r * BADGE_H_FACTOR;
            const flagCx = item.cx + item.r * Math.cos(OVERLAY_ANGLE);
            const flagCy = item.cy - item.r * Math.sin(OVERLAY_ANGLE);
            const sigCx = item.cx - item.r * Math.cos(OVERLAY_ANGLE);
            const sigCy = item.cy - item.r * Math.sin(OVERLAY_ANGLE);
            const dim = matchSet != null && !matchSet.has(item.id);
            const tickPx = Math.max(12, item.r * 0.45);
            const isCluster = item.clusterId !== undefined;

            let sigBadge: React.ReactNode = null;
            if (item.signals && item.signals.length > 0) {
              const sigs = item.signals;
              const worst = sigs.reduce(
                (best, s) => signalStyle(s.code).severity > signalStyle(best.code).severity ? s : best,
                sigs[0]
              );
              const st = signalStyle(worst.code);
              const text = sigs.length === 1 ? st.label : `${sigs.length} ⚠`;
              const { fontPx, w: pillW, h: pillH } = signalPillSize(item.r, text, sigs.length > 1);
              // The badge's own mark is 1-3 characters; its accessible name was
              // the RAW CODE ("RELATED_SANCTIONS_CONTROLLED: ..."), which is a
              // backend constant, not a label. RISK_PRESENTATION already holds
              // the display name every chip on the page uses.
              const tooltip = sigs
                .map(s => `${RISK_PRESENTATION[s.code]?.label ?? s.code.replace(/_/g, " ")}: ${s.summary}`)
                .join("\n");
              const key = `${item.id}::signal`;
              // One button whatever the count (Phase 243 merged the single-
              // and stacked-badge buttons). Its box is at least 24px square
              // (WCAG 2.5.8) and transparent; the pill inside keeps its size.
              // Phase 250: lifted clear of any mark or node it would cover.
              const lift = markOffsets.get(key) ?? 0;
              const box = hitBox(sigCx, sigCy + lift, pillW, pillH);
              sigBadge = (
                <>
                {lift !== 0 && (
                  // A lifted badge keeps a thread to the node it belongs to.
                  <span aria-hidden="true" style={{
                    position: "absolute", left: sigCx - 0.75, width: 1.5,
                    top: sigCy + lift, height: -lift, background: st.border, opacity: 0.7,
                  }}/>
                )}
                <button
                  type="button"
                  aria-label={tooltip}
                  data-roving-key={key}
                  tabIndex={key === activeKey ? 0 : -1}
                  onFocus={() => onRovingFocus(key, item)}
                  onClick={() =>
                    setSignalTooltip((prev) =>
                      prev?.id === item.id ? null : { id: item.id, x: sigCx, y: box.top + box.height / 2, text: tooltip }
                    )
                  }
                  className="rounded-full focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-oo-blue"
                  style={{
                    position: "absolute", ...box, padding: 0, background: "transparent", border: 0,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    pointerEvents: "auto", cursor: "pointer",
                  }}
                >
                  {sigs.length > 1 && (
                    <span aria-hidden="true" style={{
                      position: "absolute", left: (box.width - pillW) / 2 + 3, top: (box.height - pillH) / 2 + 3,
                      width: pillW, height: pillH, background: st.bg,
                      border: `1.5px solid ${st.border}`, borderRadius: pillH, opacity: 0.5,
                    }}/>
                  )}
                  <span aria-hidden="true" style={{
                    position: "relative", minWidth: pillW, height: pillH, background: st.bg,
                    border: `1.5px solid ${st.border}`, borderRadius: pillH,
                    display: "flex", alignItems: "center", justifyContent: "center", gap: 2,
                    fontSize: fontPx, fontWeight: 700, color: st.text,
                    boxShadow: "0 1px 3px rgba(0,0,0,0.2)", whiteSpace: "nowrap", padding: `0 ${fontPx * 0.5}px`,
                  }}>
                    {text}
                  </span>
                </button>
                </>
              );
            }

            // Collapse toggle — due south of the node (a cluster's sits on its
            // lower edge). Its box is at least 24px square; the pill inside
            // keeps the drawn size.
            let toggle: React.ReactNode = null;
            if (item.hasChildren) {
              const label = toggleLabel(item);
              const { fontPx, w: pillW, h: tp } = togglePillSize(item.r, label);
              const cluster = item.clusterId ? clusterById.get(item.clusterId) : undefined;
              const key = `${item.id}::toggle`;
              const box = hitBox(item.cx, item.cy + item.r + (markOffsets.get(key) ?? 0), pillW, tp);
              toggle = (
                <button
                  type="button"
                  data-roving-key={key}
                  tabIndex={key === activeKey ? 0 : -1}
                  onFocus={() => onRovingFocus(key, item)}
                  aria-label={cluster
                    ? `Draw ${cluster.label} — the group ${cluster.direction === "below" ? "under" : "above"} ${nodeLabelById.get(cluster.hub) ?? "this company"}`
                    : item.collapsed
                    ? `Expand ${item.hiddenCount ?? 0} hidden subsidiaries of ${item.label}`
                    : `Collapse subsidiaries of ${item.label}`}
                  onClick={() => (item.clusterId ? expandCluster(item.clusterId) : toggleCollapse(item.id))}
                  className="rounded-full focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-oo-blue"
                  style={{
                    position: "absolute", ...box, padding: 0, background: "transparent", border: 0,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    pointerEvents: "auto", cursor: "pointer",
                  }}
                >
                  <span aria-hidden="true" style={{
                    minWidth: pillW, height: tp,
                    background: item.collapsed ? CLUSTER_NODE.border : "white",
                    color: item.collapsed ? "white" : CLUSTER_NODE.border,
                    border: `1.5px solid ${CLUSTER_NODE.border}`, borderRadius: tp,
                    fontSize: fontPx, fontWeight: 700, lineHeight: 1,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    boxShadow: "0 1px 3px rgba(0,0,0,0.2)", padding: `0 ${tp * 0.3}px`,
                  }}>
                    {label}
                  </span>
                </button>
              );
            }

            return (
              <div key={item.id} style={{ opacity: dim ? 0.25 : 1, transition: "opacity 0.15s" }}>
                {!isCluster && (
                  <img src={item.icon} alt="" style={{
                    position: "absolute", width: iconSize, height: iconSize,
                    left: item.cx - iconSize / 2, top: item.cy - iconSize / 2, objectFit: "contain",
                  }}/>
                )}
                {!isCluster && item.flagUrl && (
                  <div style={{
                    position: "absolute", width: bw, height: bh,
                    left: flagCx - bw / 2, top: flagCy - bh / 2,
                    border: "1.5px solid rgba(0,0,0,0.25)", borderRadius: 2, overflow: "hidden",
                    backgroundColor: "white", boxShadow: "0 1px 3px rgba(0,0,0,0.18)",
                  }}>
                    <img src={item.flagUrl} alt="" style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }}/>
                  </div>
                )}
                {sigBadge}
                {item.identityVerified && (
                  // Phase 203 — flag NE, risk NW, collapse toggle due south,
                  // so the tick sits in the lower right. Not at SE (135°): that
                  // point overlaps the collapse toggle a person node carries, so
                  // it sits 30° below east (TICK_ANGLE), clear of the toggle.
                  // Decorative on a role="img" canvas; the legend names it and
                  // the tree row says it in words.
                  <div style={{
                    position: "absolute",
                    left: item.cx + item.r * Math.cos(TICK_ANGLE) - tickPx / 2,
                    top:  item.cy + item.r * Math.sin(TICK_ANGLE) - tickPx / 2,
                    boxShadow: "0 1px 3px rgba(0,0,0,0.2)", borderRadius: tickPx,
                    border: "1.5px solid white", lineHeight: 0,
                  }}>
                    <IdentityTick size={tickPx} />
                  </div>
                )}
                {toggle}
              </div>
            );
          })}
        </div>

        {/* Edge details tooltip */}
        {edgeTooltip && (
          <div style={{
            position: "absolute",
            left: Math.min(edgeTooltip.x + 12, (containerRef.current?.clientWidth ?? 400) - 220),
            top:  Math.max(edgeTooltip.y - 48, 8),
            zIndex: 20, pointerEvents: "none", background: "white",
            border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px",
            fontSize: 12, lineHeight: 1.5, maxWidth: 210,
            boxShadow: "0 2px 8px rgba(0,0,0,0.12)", color: "#1a1a2e", whiteSpace: "pre-wrap",
          }}>
            {edgeTooltip.text}
          </div>
        )}

        {/* Risk-badge signal popover (opened by the badge buttons; Escape closes) */}
        {signalTooltip && (
          <div style={{
            position: "absolute",
            left: Math.min(signalTooltip.x + 12, (containerRef.current?.clientWidth ?? 400) - 220),
            top:  Math.max(signalTooltip.y - 48, 8),
            zIndex: 20, pointerEvents: "none", background: "white",
            border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px",
            fontSize: 12, lineHeight: 1.5, maxWidth: 210,
            boxShadow: "0 2px 8px rgba(0,0,0,0.12)", color: "#1a1a2e", whiteSpace: "pre-wrap",
          }}>
            {signalTooltip.text}
          </div>
        )}
      </div>
    </div>
  );
}
