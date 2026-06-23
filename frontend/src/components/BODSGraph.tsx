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

import { useEffect, useRef, useState } from "react";
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

cytoscape.use(dagre);

// ---------------------------------------------------------------------------
// Risk signal → BOVS badge colour (Option C)
// ---------------------------------------------------------------------------

interface NodeOverlay {
  id:      string;
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

interface SignalStyle { bg: string; border: string; text: string; label: string; severity: number }

const SIGNAL_STYLE: Record<string, SignalStyle> = {
  SANCTIONED:               { bg:"#ffe4e6", border:"#be123c", text:"#be123c", label:"S",  severity:6 },
  RELATED_SANCTIONED:       { bg:"#ffe4e6", border:"#be123c", text:"#be123c", label:"RS", severity:6 },
  SANCTIONS_LINKED:         { bg:"#fef3c7", border:"#b45309", text:"#b45309", label:"SL", severity:3 },
  RELATED_SANCTIONS_LINKED: { bg:"#fef3c7", border:"#b45309", text:"#b45309", label:"RSL", severity:3 },
  DEBARMENT:                { bg:"#ffedd5", border:"#c2410c", text:"#9a3412", label:"Db", severity:4 },
  RELATED_DEBARMENT:        { bg:"#ffedd5", border:"#c2410c", text:"#9a3412", label:"RDb", severity:4 },
  FATF_BLACK_LIST:          { bg:"#fee2e2", border:"#991b1b", text:"#991b1b", label:"F!",  severity:5 },
  PEP:                      { bg:"#f5f3ff", border:"#6d28d9", text:"#6d28d9", label:"P",  severity:4 },
  RELATED_PEP:              { bg:"#f5f3ff", border:"#6d28d9", text:"#6d28d9", label:"RP", severity:4 },
  COMPLEX_CORPORATE_STRUCTURE: { bg:"#fef2f2", border:"#b91c1c", text:"#b91c1c", label:"CC", severity:3 },
  FATF_GREY_LIST:           { bg:"#fff7ed", border:"#9a3412", text:"#9a3412", label:"Fg", severity:2 },
  NON_EU_JURISDICTION:      { bg:"#fff7ed", border:"#c2410c", text:"#c2410c", label:"N",  severity:2 },
  STATE_CONTROLLED:         { bg:"#fff7ed", border:"#c2410c", text:"#c2410c", label:"St", severity:2 },
  OFFSHORE_LEAKS:           { bg:"#fef3c7", border:"#92400e", text:"#92400e", label:"OL", severity:2 },
  TRUST_OR_ARRANGEMENT:     { bg:"#eef2ff", border:"#4338ca", text:"#4338ca", label:"T",  severity:1 },
  COMPLEX_OWNERSHIP_LAYERS: { bg:"#f0f9ff", border:"#0369a1", text:"#0369a1", label:"≥3", severity:1 },
  POSSIBLE_OBFUSCATION:     { bg:"#fefce8", border:"#854d0e", text:"#854d0e", label:"?",  severity:1 },
  NOMINEE:                  { bg:"#fdf4ff", border:"#7e22ce", text:"#7e22ce", label:"Nm", severity:1 },
  OPAQUE_OWNERSHIP:         { bg:"#f8fafc", border:"#475569", text:"#475569", label:"O",  severity:1 },
};

const DEFAULT_SIGNAL_STYLE: SignalStyle =
  { bg:"#f1f5f9", border:"#64748b", text:"#64748b", label:"!", severity:0 };

function signalStyle(code: string): SignalStyle {
  return SIGNAL_STYLE[code] ?? DEFAULT_SIGNAL_STYLE;
}

/** Build a map from BODS statementId → RiskSignal[] from each signal's evidence. */
function buildSignalMap(signals: RiskSignal[]): Map<string, RiskSignal[]> {
  const map = new Map<string, RiskSignal[]>();
  const add = (id: string, sig: RiskSignal) => {
    if (!id) return;
    if (!map.has(id)) map.set(id, []);
    map.get(id)!.push(sig);
  };

  for (const sig of signals) {
    const ev = (sig.evidence ?? {}) as Record<string, unknown>;
    if (typeof ev.statement_id === "string")        add(ev.statement_id, sig);
    if (typeof ev.subject_statement_id === "string") add(ev.subject_statement_id, sig);
    for (const key of ["matches", "jurisdictions"] as const) {
      const arr = ev[key];
      if (Array.isArray(arr)) {
        for (const item of arr) {
          if (item && typeof item === "object" && typeof (item as Record<string,unknown>).statement_id === "string") {
            add((item as Record<string,unknown>).statement_id as string, sig);
          }
        }
      }
    }
    if (Array.isArray(ev.longest_path)) {
      for (const id of ev.longest_path) {
        if (typeof id === "string") add(id, sig);
      }
    }
  }
  return map;
}

// ---------------------------------------------------------------------------
// BODS GraphModel → Cytoscape elements
// ---------------------------------------------------------------------------

function modelToElements(model: GraphModel): ElementDefinition[] {
  const elements: ElementDefinition[] = [];
  for (const n of model.nodes) {
    elements.push({
      data: { id: n.id, label: n.label, recordType: n.recordType, icon: n.icon, flagUrl: n.flagUrl },
    });
  }
  for (const e of model.edges) {
    elements.push({
      data: {
        id: e.id, source: e.source, target: e.target,
        label: e.label, category: e.category, details: e.details,
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
    style: { "border-color": "#1565c0", "border-width": 3 } as cytoscape.Css.Node,
  },
  // Collapsed node — solid blue ring so it reads as "expandable".
  { selector: "node.collapsed", style: { "border-color": "#1565c0", "border-width": 3 } as cytoscape.Css.Node },
  // Search highlight / dim
  { selector: "node.search-match", style: { "border-color": "#1565c0", "border-width": 5 } as cytoscape.Css.Node },
  { selector: "node.search-dim", style: { opacity: 0.3 } as cytoscape.Css.Node },
  { selector: "edge.search-dim", style: { opacity: 0.12 } as cytoscape.Css.Edge },
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
  { selector: "edge[category = 'ownership']", style: { "line-color": "#1565c0", "target-arrow-color": "#1565c0", color: "#1565c0" } as cytoscape.Css.Edge },
  { selector: "edge[category = 'control']",  style: { "line-color": "#e65100", "target-arrow-color": "#e65100", color: "#e65100" } as cytoscape.Css.Edge },
  { selector: "edge[category = 'role']",     style: { "line-color": "#6a1b9a", "target-arrow-color": "#6a1b9a", color: "#6a1b9a", "line-style": "dashed" } as cytoscape.Css.Edge },
  { selector: "edge[category = 'unknown']",  style: { "line-color": "#888",    "target-arrow-color": "#888",    color: "#888" } as cytoscape.Css.Edge },
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
}) {
  const containerRef  = useRef<HTMLDivElement | null>(null);
  const cyRef         = useRef<Core | null>(null);
  const [overlays, setOverlays] = useState<NodeOverlay[]>([]);
  const [edgeTooltip, setEdgeTooltip] = useState<{ x: number; y: number; text: string } | null>(null);

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
      elements: modelToElements(model),
      style: STYLESHEET,
      layout: DAGRE_LAYOUT,
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
      minZoom: 0.2,
      maxZoom: 4,
    });
    cyRef.current = cy;

    const signalMap = buildSignalMap(signals);

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
    cy.on("viewport", () => setEdgeTooltip(null));

    return () => { cy.destroy(); cyRef.current = null; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, signals]);

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

  if (model.nodes.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  return (
    <div className="bg-white border border-oo-rule rounded-oo">
      {/* Toolbar */}
      <div className="border-b border-oo-rule">
        <div className="flex items-center flex-wrap gap-1 px-2 py-1 text-xs text-oo-muted">
          <button type="button" className="hover:text-oo-blue font-mono px-2" title="Zoom in"
            onClick={() => cyRef.current?.zoom({ level: (cyRef.current?.zoom() ?? 1) * 1.3,
              renderedPosition: { x: (containerRef.current?.clientWidth ?? 0) / 2, y: (containerRef.current?.clientHeight ?? 0) / 2 } })}>
            +
          </button>
          <button type="button" className="hover:text-oo-blue font-mono px-2" title="Zoom out"
            onClick={() => cyRef.current?.zoom({ level: (cyRef.current?.zoom() ?? 1) / 1.3,
              renderedPosition: { x: (containerRef.current?.clientWidth ?? 0) / 2, y: (containerRef.current?.clientHeight ?? 0) / 2 } })}>
            −
          </button>
          <button type="button" className="hover:text-oo-blue px-2" title="Fit"
            onClick={() => cyRef.current?.fit(undefined, 32)}>
            Fit
          </button>
          {collapsedCount > 0 && (
            <button type="button" className="hover:text-oo-blue px-2" title="Expand all collapsed nodes"
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
              title="Previous match" aria-label="Previous match"
              disabled={matchIds.length === 0} onClick={() => focusMatch(matchIdx - 1)}>
              ‹
            </button>
            <button type="button" className="hover:text-oo-blue font-mono px-1 disabled:opacity-30"
              title="Next match" aria-label="Next match"
              disabled={matchIds.length === 0} onClick={() => focusMatch(matchIdx + 1)}>
              ›
            </button>
            <span role="status" aria-live="polite" className="min-w-[64px] tabular-nums text-[11px]">
              {resultLabel}
            </span>
          </div>
        </div>
        {/* Legend */}
        <div className="flex flex-wrap gap-1.5 px-3 pb-2">
          <span className="flex items-center gap-1.5 text-[11px] font-medium px-2 py-0.5 rounded-full border bg-[#e8f0fb] border-[#1565c0] text-[#1565c0]">
            <span className="inline-block w-3.5 h-0.5 bg-[#1565c0] rounded-full flex-shrink-0"/>Ownership
          </span>
          <span className="flex items-center gap-1.5 text-[11px] font-medium px-2 py-0.5 rounded-full border bg-[#fdf0e8] border-[#e65100] text-[#e65100]">
            <span className="inline-block w-3.5 h-0.5 bg-[#e65100] rounded-full flex-shrink-0"/>Control
          </span>
          <span className="flex items-center gap-1.5 text-[11px] font-medium px-2 py-0.5 rounded-full border bg-[#f3eef8] border-[#6a1b9a] text-[#6a1b9a]">
            <span className="inline-block w-3.5 flex-shrink-0" style={{borderTop:"1.5px dashed #6a1b9a"}}/>Role
          </span>
          {signals.length > 0 && <>
            <span className="flex items-center gap-1.5 text-[11px] font-medium px-2 py-0.5 rounded-full border bg-[#ffe4e6] border-[#be123c] text-[#be123c]">Sanction</span>
            <span className="flex items-center gap-1.5 text-[11px] font-medium px-2 py-0.5 rounded-full border bg-[#f5f3ff] border-[#6d28d9] text-[#6d28d9]">PEP</span>
            <span className="flex items-center gap-1.5 text-[11px] font-medium px-2 py-0.5 rounded-full border bg-[#fff7ed] border-[#c2410c] text-[#c2410c]">Jurisdiction</span>
          </>}
        </div>
      </div>

      {/* Graph container + HTML overlay */}
      <div style={{ position: "relative" }}>
        <div
          ref={containerRef}
          className="overflow-hidden"
          style={{ width: "100%", height: 420 }}
          role="img"
          aria-label={entityName
            ? `Ownership structure graph for ${entityName}`
            : "Ownership structure graph"}
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
              const tooltip = sigs.map(s => `${s.code}: ${s.summary}`).join("\n");

              if (sigs.length === 1) {
                sigBadge = (
                  <div title={tooltip} style={{
                    position: "absolute", left: sigCx - badgePx * 0.9, top: sigCy - badgePx * 0.45,
                    minWidth: badgePx * 1.8, height: badgePx * 0.9, background: st.bg,
                    border: `1.5px solid ${st.border}`, borderRadius: badgePx,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: Math.max(8, badgePx * 0.42), fontWeight: 700, color: st.text,
                    boxShadow: "0 1px 3px rgba(0,0,0,0.2)", whiteSpace: "nowrap", padding: `0 ${badgePx * 0.3}px`,
                  }}>
                    {st.label}
                  </div>
                );
              } else {
                sigBadge = (
                  <div style={{ position: "absolute", left: sigCx - badgePx * 0.75, top: sigCy - badgePx * 0.45 }}>
                    <div style={{ position: "absolute", left: 3, top: 3, width: badgePx * 1.5, height: badgePx * 0.9,
                      background: st.bg, border: `1.5px solid ${st.border}`, borderRadius: badgePx, opacity: 0.5 }}/>
                    <div title={tooltip} style={{
                      position: "relative", minWidth: badgePx * 1.5, height: badgePx * 0.9, background: st.bg,
                      border: `1.5px solid ${st.border}`, borderRadius: badgePx,
                      display: "flex", alignItems: "center", justifyContent: "center",
                      fontSize: Math.max(8, badgePx * 0.42), fontWeight: 700, color: st.text,
                      boxShadow: "0 1px 4px rgba(0,0,0,0.25)", whiteSpace: "nowrap", padding: `0 ${badgePx * 0.3}px`, gap: 2,
                    }}>
                      {sigs.length} ⚠
                    </div>
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
                  title={item.collapsed ? `Expand ${item.hiddenCount ?? 0} hidden` : "Collapse subsidiaries"}
                  aria-label={item.collapsed ? `Expand ${item.hiddenCount ?? 0} hidden subsidiaries` : "Collapse subsidiaries"}
                  onClick={() => toggleCollapse(item.id)}
                  style={{
                    position: "absolute",
                    left: item.cx - tp, top: item.cy + item.r - tp * 0.5,
                    minWidth: tp * 2, height: tp,
                    pointerEvents: "auto", cursor: "pointer",
                    background: item.collapsed ? "#1565c0" : "#ffffff",
                    color: item.collapsed ? "#ffffff" : "#1565c0",
                    border: "1.5px solid #1565c0", borderRadius: tp,
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
      </div>
    </div>
  );
}
