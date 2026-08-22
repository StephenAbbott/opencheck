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
  nodesWithChildren,
  type GraphModel,
  type Visibility,
} from "../lib/bodsGraph";
import type { RiskSignal } from "../lib/api";
// buildSignalMap lives in lib/signalScope.ts (Phase 109) — the badge machinery
// here and the per-source scoping filter must read `evidence` identically, so
// they share one implementation rather than two that can drift.
import { buildSignalMap } from "../lib/signalScope";
import { EDGE_STYLE, signalStyle } from "../lib/graphStyle";
import GraphLegend from "./GraphLegend";
import { RISK_PRESENTATION } from "./risk/RiskChip";
import type { SameAsCandidate } from "../lib/reconcile";

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
}

// The graph's visual vocabulary moved to lib/graphStyle.ts in Phase 124, so
// the legend can be generated from it without importing this component (which
// pulls in Cytoscape). Re-exported here because RiskChip.test.ts and other
// call sites have always imported SIGNAL_STYLE from this module.
export { SIGNAL_STYLE } from "../lib/graphStyle";

// ---------------------------------------------------------------------------
// BODS GraphModel → Cytoscape elements
// ---------------------------------------------------------------------------

function modelToElements(model: GraphModel, sameAs: SameAsCandidate[] = []): ElementDefinition[] {
  const elements: ElementDefinition[] = [];
  const nodeIds = new Set(model.nodes.map((n) => n.id));
  for (const n of model.nodes) {
    elements.push({
      data: { id: n.id, label: n.label, recordType: n.recordType, icon: n.icon, flagUrl: n.flagUrl, sources: n.sources },
    });
  }
  for (const e of model.edges) {
    elements.push({
      data: {
        id: e.id, source: e.source, target: e.target,
        label: e.label, category: e.category, details: e.details, sources: e.sources,
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
  { selector: "edge.hovered",                style: { width: 3, "z-index": 999 } as cytoscape.Css.Edge },
];

// BOVS overlay geometry (fractions of the node radius).
const BADGE_W_FACTOR = 0.75;
const BADGE_H_FACTOR = 0.50;
const OVERLAY_ANGLE = Math.PI / 4;   // 45° diagonal compass point
const ICON_FRACTION = 0.6;           // BOVS icon = 60% of node diameter

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
}) {
  const containerRef  = useRef<HTMLDivElement | null>(null);
  const cyRef         = useRef<Core | null>(null);
  const [overlays, setOverlays] = useState<NodeOverlay[]>([]);
  const [edgeTooltip, setEdgeTooltip] = useState<{ x: number; y: number; text: string } | null>(null);
  // Risk-badge popover — badges are buttons (keyboard-reachable), click toggles
  // the full signal text; Escape dismisses (see effect below).
  const [signalTooltip, setSignalTooltip] =
    useState<{ id: string; x: number; y: number; text: string } | null>(null);

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

  function toggleCollapse(id: string) {
    const next = new Set(collapsed);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onCollapsedChange(next);
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

    childrenRef.current = nodesWithChildren(model);

    const cy = cytoscape({
      container: el,
      elements: modelToElements(model, sameAs),
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
      const vis = visRef.current;
      const collapsedNow = collapsedRef.current;
      const hasChildren = childrenRef.current;
      const next: NodeOverlay[] = [];

      cy.nodes().forEach(node => {
        const id = node.id();
        if (vis && !vis.visible.has(id)) return; // skip collapsed-away nodes
        const pos = node.position();
        next.push({
          id,
          label:   node.data("label") as string,
          cx:      pos.x * zoom + pan.x,
          cy:      pos.y * zoom + pan.y,
          r:       (node.width() * zoom) / 2,
          icon:    node.data("icon")    as string,
          flagUrl: node.data("flagUrl") as string | undefined,
          signals: signalMap.get(id),
          hasChildren: hasChildren.has(id),
          collapsed: collapsedNow.has(id),
          hiddenCount: vis?.hiddenCount.get(id) ?? 0,
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
    cy.on("tap", "node", (evt) => { onSelectRef.current?.(evt.target.id()); });
    cy.on("tap", (evt) => {
      if (evt.target === cy) { setEdgeTooltip(null); onSelectRef.current?.(null); }
    });
    cy.on("viewport", () => { setEdgeTooltip(null); setSignalTooltip(null); });

    return () => { cy.destroy(); cyRef.current = null; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, signalMap, sameAs]);

  // ── Apply collapse: hide/show elements, re-layout the visible subset ───────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy || model.nodes.length === 0) return;

    const vis = computeVisibility(model, collapsed);
    visRef.current = vis;

    cy.batch(() => {
      cy.nodes().forEach(n => {
        n.style("display", vis.visible.has(n.id()) ? "element" : "none");
        n.toggleClass("collapsed", collapsed.has(n.id()) && (vis.hiddenCount.get(n.id()) ?? 0) > 0);
      });
      cy.edges().forEach(e => {
        const show =
          vis.visible.has(e.source().id()) &&
          vis.visible.has(e.target().id()) &&
          !collapsed.has(e.source().id()); // a collapsed node hides its downstream edges
        e.style("display", show ? "element" : "none");
      });
    });

    const visEles = cy.elements(":visible");
    visEles.layout(DAGRE_LAYOUT).run();
    cy.fit(visEles, 32);
    updateOverlaysRef.current?.();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, signals, collapsed]);

  // ── Apply search over the currently-visible nodes ──────────────────────────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    const vis = visRef.current;
    const visibleNodes = vis ? model.nodes.filter(n => vis.visible.has(n.id)) : model.nodes;
    const ids = searchNodes(visibleNodes, query);
    const active = query.trim().length > 0;
    const set = active ? new Set(ids) : null;

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
      if (node.nonempty()) cy.animate({ center: { eles: node } }, { duration: 250 });
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, model, collapsed]);

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
  }, [highlightSource, model]);

  // ── Reflect the shared selection into the graph (highlight + centre) ───────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.nodes(":selected").unselect();
    if (selectedId) {
      const node = cy.getElementById(selectedId);
      const visible = visRef.current?.visible.has(selectedId) ?? true;
      if (node.nonempty() && visible) {
        node.select();
        cy.animate({ center: { eles: node } }, { duration: 250 });
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId, model]);

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
    if (node.nonempty()) cy.animate({ center: { eles: node } }, { duration: 250 });
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
  const edgeCategories = useMemo(
    () => new Set(model.edges.map((e) => e.category as string)),
    [model]
  );

  // Counts for the canvas's accessible name (role="img" — the name is all a
  // screen-reader user perceives of it; the table view is the full equivalent).
  const personCount = model.nodes.filter(
    (n) => n.recordType === "person" || n.recordType === "personStatement"
  ).length;
  const entityCount = model.nodes.length - personCount;
  const graphAriaLabel =
    `Ownership structure graph${entityName ? ` for ${entityName}` : ""} — ` +
    `${entityCount} ${entityCount === 1 ? "entity" : "entities"}, ` +
    `${personCount} ${personCount === 1 ? "person" : "people"}, ` +
    `${model.edges.length} ${model.edges.length === 1 ? "relationship" : "relationships"}. ` +
    "Use the table view for a text equivalent.";

  if (model.nodes.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  return (
    <div className="bg-white border border-oo-rule rounded-oo">
      {/* Toolbar */}
      <div className="border-b border-oo-rule">
        <div className="flex items-center flex-wrap gap-1 px-2 py-1 text-oo-meta text-oo-muted">
          <button type="button" className="hover:text-oo-blue font-mono px-2" aria-label="Zoom in"
              onClick={() => cyRef.current?.zoom({ level: (cyRef.current?.zoom() ?? 1) * 1.3,
                renderedPosition: { x: (containerRef.current?.clientWidth ?? 0) / 2, y: (containerRef.current?.clientHeight ?? 0) / 2 } })}>
              +
            </button>
            <button type="button" className="hover:text-oo-blue font-mono px-2" aria-label="Zoom out"
              onClick={() => cyRef.current?.zoom({ level: (cyRef.current?.zoom() ?? 1) / 1.3,
                renderedPosition: { x: (containerRef.current?.clientWidth ?? 0) / 2, y: (containerRef.current?.clientHeight ?? 0) / 2 } })}>
              −
            </button>
            <button type="button" className="hover:text-oo-blue px-2"
              onClick={() => cyRef.current?.fit(undefined, 32)}>
              Fit
            </button>
          {collapsedCount > 0 && (
            <button type="button" className="hover:text-oo-blue px-2"
              aria-label={`Expand all ${collapsedCount} collapsed ${collapsedCount === 1 ? "branch" : "branches"}`}
              onClick={() => onCollapsedChange(new Set())}>
              Expand all
            </button>
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
                className="px-2 py-0.5 text-xs border border-oo-rule rounded w-32 sm:w-44"
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") { e.preventDefault(); focusMatch(matchIdx + (e.shiftKey ? -1 : 1)); }
                  else if (e.key === "Escape") { e.preventDefault(); setQuery(""); }
                }}
              />
              <button type="button" className="hover:text-oo-blue font-mono px-1 disabled:opacity-30"
                aria-label="Previous match"
                disabled={matchIds.length === 0} onClick={() => focusMatch(matchIdx - 1)}>
                ‹
              </button>
              <button type="button" className="hover:text-oo-blue font-mono px-1 disabled:opacity-30"
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

        {/* Pixel-perfect icon + flag + risk + collapse overlay */}
        <div style={{ position: "absolute", inset: 0, overflow: "hidden", pointerEvents: "none" }}>
          {overlays.map(item => {
            const iconSize = item.r * 2 * ICON_FRACTION;
            const bw = item.r * BADGE_W_FACTOR;
            const bh = item.r * BADGE_H_FACTOR;
            const flagCx = item.cx + item.r * Math.cos(OVERLAY_ANGLE);
            const flagCy = item.cy - item.r * Math.sin(OVERLAY_ANGLE);
            const sigCx = item.cx - item.r * Math.cos(OVERLAY_ANGLE);
            const sigCy = item.cy - item.r * Math.sin(OVERLAY_ANGLE);
            const dim = matchSet != null && !matchSet.has(item.id);

            let sigBadge: React.ReactNode = null;
            if (item.signals && item.signals.length > 0) {
              const sigs = item.signals;
              const worst = sigs.reduce(
                (best, s) => signalStyle(s.code).severity > signalStyle(best.code).severity ? s : best,
                sigs[0]
              );
              const st = signalStyle(worst.code);
              const badgePx = Math.max(18, item.r * 0.55);
              // The badge's own mark is 1-3 characters; its accessible name was
              // the RAW CODE ("RELATED_SANCTIONS_CONTROLLED: ..."), which is a
              // backend constant, not a label. RISK_PRESENTATION already holds
              // the display name every chip on the page uses.
              const tooltip = sigs
                .map(s => `${RISK_PRESENTATION[s.code]?.label ?? s.code.replace(/_/g, " ")}: ${s.summary}`)
                .join("\n");

              // The badge is a real button (overlay is pointer-events:none, so
              // it re-enables pointer events like the collapse toggle below):
              // click/Enter toggles a popover with the full signal text —
              // the old non-focusable div's `title` could never show.
              const toggleSignalTooltip = () =>
                setSignalTooltip((prev) =>
                  prev?.id === item.id ? null : { id: item.id, x: sigCx, y: sigCy, text: tooltip }
                );

              if (sigs.length === 1) {
                sigBadge = (
                  <button type="button" aria-label={tooltip}
                    onClick={toggleSignalTooltip} style={{
                    position: "absolute", left: sigCx - badgePx * 0.9, top: sigCy - badgePx * 0.45,
                    minWidth: badgePx * 1.8, height: badgePx * 0.9, background: st.bg,
                    border: `1.5px solid ${st.border}`, borderRadius: badgePx,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: Math.max(8, badgePx * 0.42), fontWeight: 700, color: st.text,
                    boxShadow: "0 1px 3px rgba(0,0,0,0.2)", whiteSpace: "nowrap", padding: `0 ${badgePx * 0.3}px`,
                    pointerEvents: "auto", cursor: "pointer",
                  }}>
                    {st.label}
                  </button>
                );
              } else {
                sigBadge = (
                  <div style={{ position: "absolute", left: sigCx - badgePx * 0.75, top: sigCy - badgePx * 0.45 }}>
                    <div style={{ position: "absolute", left: 3, top: 3, width: badgePx * 1.5, height: badgePx * 0.9,
                      background: st.bg, border: `1.5px solid ${st.border}`, borderRadius: badgePx, opacity: 0.5 }}/>
                    <button type="button" aria-label={tooltip}
                      onClick={toggleSignalTooltip} style={{
                      position: "relative", minWidth: badgePx * 1.5, height: badgePx * 0.9, background: st.bg,
                      border: `1.5px solid ${st.border}`, borderRadius: badgePx,
                      display: "flex", alignItems: "center", justifyContent: "center",
                      fontSize: Math.max(8, badgePx * 0.42), fontWeight: 700, color: st.text,
                      boxShadow: "0 1px 4px rgba(0,0,0,0.25)", whiteSpace: "nowrap", padding: `0 ${badgePx * 0.3}px`, gap: 2,
                      pointerEvents: "auto", cursor: "pointer",
                    }}>
                      {sigs.length} ⚠
                    </button>
                  </div>
                );
              }
            }

            // Collapse toggle — due south of the node, clickable (overlay is
            // pointer-events:none, so the button re-enables pointer events).
            let toggle: React.ReactNode = null;
            if (item.hasChildren) {
              const tp = Math.max(13, item.r * 0.42);
              const label = item.collapsed ? (item.hiddenCount ? `+${item.hiddenCount}` : "+") : "−";
              toggle = (
                <button
                  type="button"
                  aria-label={item.collapsed
                    ? `Expand ${item.hiddenCount ?? 0} hidden subsidiaries of ${item.label}`
                    : `Collapse subsidiaries of ${item.label}`}
                  onClick={() => toggleCollapse(item.id)}
                  style={{
                    position: "absolute",
                    left: item.cx - tp, top: item.cy + item.r - tp * 0.5,
                    minWidth: tp * 2, height: tp,
                    pointerEvents: "auto", cursor: "pointer",
                    background: item.collapsed ? "#3d30d4" : "#ffffff",
                    color: item.collapsed ? "#ffffff" : "#3d30d4",
                    border: "1.5px solid #3d30d4", borderRadius: tp,
                    fontSize: Math.max(9, tp * 0.55), fontWeight: 700, lineHeight: 1,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    boxShadow: "0 1px 3px rgba(0,0,0,0.2)", padding: `0 ${tp * 0.3}px`,
                  }}
                >
                  {label}
                </button>
              );
            }

            return (
              <div key={item.id} style={{ opacity: dim ? 0.25 : 1, transition: "opacity 0.15s" }}>
                <img src={item.icon} alt="" style={{
                  position: "absolute", width: iconSize, height: iconSize,
                  left: item.cx - iconSize / 2, top: item.cy - iconSize / 2, objectFit: "contain",
                }}/>
                {item.flagUrl && (
                  <div style={{
                    position: "absolute", width: bw, height: bh,
                    left: flagCx - bw / 2, top: flagCy - bh / 2,
                    border: "1.5px solid rgba(0,0,0,0.25)", borderRadius: 2, overflow: "hidden",
                    backgroundColor: "#fff", boxShadow: "0 1px 3px rgba(0,0,0,0.18)",
                  }}>
                    <img src={item.flagUrl} alt="" style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }}/>
                  </div>
                )}
                {sigBadge}
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
            zIndex: 20, pointerEvents: "none", background: "#fff",
            border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px",
            fontSize: 11, lineHeight: 1.5, maxWidth: 210,
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
            zIndex: 20, pointerEvents: "none", background: "#fff",
            border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px",
            fontSize: 11, lineHeight: 1.5, maxWidth: 210,
            boxShadow: "0 2px 8px rgba(0,0,0,0.12)", color: "#1a1a2e", whiteSpace: "pre-wrap",
          }}>
            {signalTooltip.text}
          </div>
        )}
      </div>
    </div>
  );
}
