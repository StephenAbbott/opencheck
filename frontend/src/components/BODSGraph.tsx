/**
 * BODSGraph — renders a BODS v0.4 statement bundle as an interactive
 * ownership/control graph using Cytoscape.js + dagre hierarchical layout.
 *
 * Node icons and jurisdiction flag overlays are rendered as an HTML layer
 * that sits above the Cytoscape canvas. This gives pixel-perfect centering
 * and sizing at all zoom levels — Cytoscape's canvas background-image system
 * has sub-pixel drift at non-integer zoom levels which made icons appear to
 * shift and flags fail to fill their container.
 *
 * BOVS Metadata Overlays spec:
 *   "Jurisdiction: overlaying icons around the circumference of the Node.
 *    Prefer positions at 45°, 135°, 225°, 315° (diagonal compass points)."
 * → Flag badge centred exactly at the 45° (NE) circumference point.
 */

import { useEffect, useRef, useState } from "react";
import cytoscape, { type Core, type ElementDefinition, type StylesheetStyle } from "cytoscape";
import dagre from "cytoscape-dagre";
import { BOVS_ICONS } from "../lib/bovsIcons";
import type { RiskSignal } from "../lib/api";

cytoscape.use(dagre);

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Stmt = Record<string, unknown>;
type RD   = Record<string, unknown>;
type Interest = {
  type?: string;
  share?: { exact?: number; minimum?: number; maximum?: number;
            exclusiveMinimum?: number; exclusiveMaximum?: number };
  directOrIndirect?: string;
  beneficialOwnershipOrControl?: boolean;
  details?: string;
};

interface NodeOverlay {
  id:      string;
  cx:      number;   // screen-space x of node centre
  cy:      number;   // screen-space y of node centre
  r:       number;   // screen-space node radius
  icon:    string;   // base64 data-URI for BOVS entity/person icon
  flagUrl?: string;  // URL for jurisdiction flag SVG (null if no jurisdiction)
  signals?: RiskSignal[];  // risk signals scoped to this node
}

// ---------------------------------------------------------------------------
// Risk signal → BOVS badge colour (Option C)
//
// Colours match the existing RiskChip palette in RiskChip.tsx.
// BOVS position: 315° (NW) compass point on the node circumference.
// Multiple signals collapse into a "N ⚠" stack badge in the worst colour.
// RELATED_* signals also annotate the connecting edge with a ⚠ circle.
// ---------------------------------------------------------------------------

interface SignalStyle { bg: string; border: string; text: string; label: string; severity: number }

const SIGNAL_STYLE: Record<string, SignalStyle> = {
  SANCTIONED:               { bg:"#ffe4e6", border:"#be123c", text:"#be123c", label:"S",  severity:6 },
  RELATED_SANCTIONED:       { bg:"#ffe4e6", border:"#be123c", text:"#be123c", label:"RS", severity:6 },
  FATF_BLACK_LIST:          { bg:"#fee2e2", border:"#991b1b", text:"#991b1b", label:"F!",  severity:5 },
  PEP:                      { bg:"#f5f3ff", border:"#6d28d9", text:"#6d28d9", label:"P",  severity:4 },
  RELATED_PEP:              { bg:"#f5f3ff", border:"#6d28d9", text:"#6d28d9", label:"RP", severity:4 },
  COMPLEX_CORPORATE_STRUCTURE: { bg:"#fef2f2", border:"#b91c1c", text:"#b91c1c", label:"CC", severity:3 },
  FATF_GREY_LIST:           { bg:"#fff7ed", border:"#9a3412", text:"#9a3412", label:"Fg", severity:2 },
  NON_EU_JURISDICTION:      { bg:"#fff7ed", border:"#c2410c", text:"#c2410c", label:"N",  severity:2 },
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

/**
 * Build a map from BODS statementId → RiskSignal[] by extracting statement
 * IDs from each signal's evidence block.
 *
 * Evidence fields (per signal type):
 *  - SANCTIONED, PEP                → evidence.statement_id
 *  - RELATED_SANCTIONED, RELATED_PEP → evidence.subject_statement_id
 *  - TRUST, NOMINEE, COMPLEX, AMLA   → evidence.matches[].statement_id
 *  - NON_EU, FATF                    → evidence.jurisdictions[].statement_id
 *  - COMPLEX_OWNERSHIP_LAYERS        → evidence.longest_path[]  (array of ids)
 */
function buildSignalMap(signals: RiskSignal[]): Map<string, RiskSignal[]> {
  const map = new Map<string, RiskSignal[]>();
  const add = (id: string, sig: RiskSignal) => {
    if (!id) return;
    if (!map.has(id)) map.set(id, []);
    map.get(id)!.push(sig);
  };

  for (const sig of signals) {
    const ev = (sig.evidence ?? {}) as Record<string, unknown>;

    // Direct single-statement signals
    if (typeof ev.statement_id === "string")        add(ev.statement_id, sig);
    if (typeof ev.subject_statement_id === "string") add(ev.subject_statement_id, sig);

    // Multi-statement array signals
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

    // COMPLEX_OWNERSHIP_LAYERS: longest_path is a string[]
    if (Array.isArray(ev.longest_path)) {
      for (const id of ev.longest_path) {
        if (typeof id === "string") add(id, sig);
      }
    }
  }
  return map;
}

// ---------------------------------------------------------------------------
// BOVS interest-type → annotation label
// ---------------------------------------------------------------------------

const INTEREST_LABELS: Record<string, string> = {
  shareholding:                    "Owns",
  votingRights:                    "Controls (votes)",
  appointmentOfBoard:              "Controls (board)",
  otherInfluenceOrControl:         "Controls",
  controlViaCompanyRulesOrArticles:"Controls (articles)",
  controlByLegalFramework:         "Controls (law)",
  seniorManagingOfficial:          "Director",
  boardMember:                     "Board member",
  boardChair:                      "Chair",
  unknownInterest:                 "Interest (unknown)",
  unpublishedInterest:             "Interest (unpublished)",
  enjoymentAndUseOfAssets:         "Enjoys assets",
  rightToProfitOrIncomeFromAssets: "Profits from assets",
};

function interestLabel(i: Interest): string {
  const base = INTEREST_LABELS[i.type ?? ""] ?? i.type ?? "Interest";
  const s = i.share;
  if (!s) return base;
  if (s.exact != null) {
    const verb = base.startsWith("Owns") ? "Owns" : "Controls";
    const rest = base.startsWith("Owns") ? base.slice(4).trim() : base.slice(8).trim();
    return `${verb} ${s.exact}%${rest ? ` ${rest}` : ""}`.trim();
  }
  const lo = s.minimum ?? s.exclusiveMinimum;
  const hi = s.maximum ?? s.exclusiveMaximum;
  if (lo != null && hi != null) {
    return `${base.startsWith("Owns") ? "Owns" : "Controls"} ${lo}–${hi}%`;
  }
  return base;
}

function buildEdgeLabel(interests: Interest[]): string {
  if (!interests.length) return "";
  const sorted = [...interests].sort((a, b) =>
    (b.beneficialOwnershipOrControl ? 1 : 0) - (a.beneficialOwnershipOrControl ? 1 : 0)
  );
  return sorted.slice(0, 2).map(interestLabel).join("\n");
}

// ---------------------------------------------------------------------------
// BOVS icons (base64 data URIs) — immune to canvas taint issues from xlink SVGs
// ---------------------------------------------------------------------------

const FLAGS_BASE = "/bods-dagre-images/flags";

const ENTITY_ICON: Record<string, string> = {
  registeredEntity:       BOVS_ICONS["registeredEntity"],
  registeredEntityListed: BOVS_ICONS["registeredEntityListed"],
  legalEntity:            BOVS_ICONS["registeredEntity"],
  arrangement:            BOVS_ICONS["arrangement"],
  anonymousEntity:        BOVS_ICONS["anonymousEntity"],
  unknownEntity:          BOVS_ICONS["unknownEntity"],
  state:                  BOVS_ICONS["state"],
  stateBody:              BOVS_ICONS["stateBody"],
};

const PERSON_ICON: Record<string, string> = {
  knownPerson:     BOVS_ICONS["knownPerson"],
  anonymousPerson: BOVS_ICONS["anonymousPerson"],
  unknownPerson:   BOVS_ICONS["anonymousPerson"],
};

function nodeIcon(stmt: Stmt): string {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const rt = (stmt.recordType ?? stmt.statementType) as string;
  if (rt === "person" || rt === "personStatement") {
    return PERSON_ICON[(rd.personType as string) ?? "knownPerson"] ?? BOVS_ICONS["knownPerson"];
  }
  return ENTITY_ICON[((rd.entityType as RD)?.type as string) ?? "registeredEntity"] ?? BOVS_ICONS["registeredEntity"];
}

function flagUrl(stmt: Stmt): string | undefined {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const jur = (rd.jurisdiction ?? rd.incorporatedInJurisdiction) as RD | undefined;
  const code = (jur?.code as string | undefined)?.toLowerCase().split("-")[0];
  return code ? `${FLAGS_BASE}/${code}.svg` : undefined;
}

// ---------------------------------------------------------------------------
// BODS → Cytoscape elements (nodes + edges only — no badge phantom nodes)
// ---------------------------------------------------------------------------

function bodsToElements(statements: Stmt[]): ElementDefinition[] {
  const elements: ElementDefinition[] = [];
  const nodeIds = new Set<string>();
  // BODS v0.4: relationship endpoints reference declarationSubject (e.g.
  // "XI-LEI-…") rather than the statementId UUID.  Build a lookup so edge
  // resolution works for both v0.3 and v0.4 data.
  const declSubjToNodeId = new Map<string, string>();

  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (rt !== "entity" && rt !== "person" && rt !== "entityStatement" && rt !== "personStatement") continue;
    const id = (stmt.statementId ?? stmt.statementID) as string;
    if (!id || nodeIds.has(id)) continue;
    nodeIds.add(id);
    // Register declarationSubject alias (v0.4) so relationship endpoints
    // can resolve to the Cytoscape node id (= statementId UUID).
    const declSubj = stmt.declarationSubject as string | undefined;
    if (declSubj && declSubj !== id) declSubjToNodeId.set(declSubj, id);

    const rd = (stmt.recordDetails ?? {}) as RD;
    const name = (rd.name as string)
      ?? ((rd.names as RD[] | undefined)?.[0]?.fullName as string)
      ?? id.slice(-8);

    elements.push({
      data: {
        id,
        label: name,
        recordType: rt,
        icon:    nodeIcon(stmt),
        flagUrl: flagUrl(stmt),
      },
    });
  }

  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (rt !== "relationship" && rt !== "ownershipOrControlStatement") continue;

    const rd = (stmt.recordDetails ?? {}) as RD;
    const rawIP   = rd.interestedParty;
    const rawSubj = rd.subject;

    const resolveRef = (raw: unknown): string | undefined => {
      if (typeof raw === "string") {
        // v0.4: plain string — may be a statementId UUID or a declarationSubject alias
        return nodeIds.has(raw) ? raw : declSubjToNodeId.get(raw);
      }
      // v0.3: object with describedBy* reference pointing at the statementId
      const obj = raw as RD | undefined;
      return (obj?.describedByEntityStatement as string | undefined)
          ?? (obj?.describedByPersonStatement as string | undefined);
    };

    const sourceId = resolveRef(rawIP);
    const targetId = resolveRef(rawSubj);

    if (!sourceId || !targetId || !nodeIds.has(sourceId) || !nodeIds.has(targetId)) continue;

    const interests = (rd.interests ?? []) as Interest[];
    const hasOwnership = interests.some(i => i.type === "shareholding" || i.type === "votingRights");
    const hasControl   = !hasOwnership && interests.some(i =>
      i.type === "appointmentOfBoard" || i.type === "otherInfluenceOrControl" ||
      i.type === "controlViaCompanyRulesOrArticles" || i.type === "controlByLegalFramework");
    const isRole = interests.some(i =>
      i.type === "seniorManagingOfficial" || i.type === "boardMember" || i.type === "boardChair");

    // Collect details text from all interests for the hover tooltip.
    const detailsText = interests
      .map(i => i.details)
      .filter((d): d is string => !!d)
      .join(" · ") || undefined;

    elements.push({
      data: {
        id: (stmt.statementId ?? stmt.statementID) as string ?? `${sourceId}-${targetId}`,
        source: sourceId,
        target: targetId,
        label:    buildEdgeLabel(interests),
        category: hasOwnership ? "ownership" : hasControl ? "control" : isRole ? "role" : "unknown",
        details:  detailsText,
      },
    });
  }

  return elements;
}

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
      "text-margin-y": 6,
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
  // Edges with details: thicker on hover class, cursor handled via JS
  { selector: "edge.hovered",                style: { width: 3, "z-index": 999 } as cytoscape.Css.Edge },
];

// BOVS flag badge dimensions — proportional to the node radius so they scale
// consistently at all zoom levels.  At zoom=1 (node Ø=80px, radius=40px):
//   badge width  = 40 * 0.75 = 30px
//   badge height = 40 * 0.50 = 20px  → 3:2 aspect ratio
const BADGE_W_FACTOR = 0.75;   // fraction of node radius
const BADGE_H_FACTOR = 0.50;
// BOVS: flag at 45° (NE) compass point on the circumference
const OVERLAY_ANGLE = Math.PI / 4;
// BOVS icon: 60% of node diameter
const ICON_FRACTION = 0.6;

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function BODSGraph({
  statements,
  signals = [],
  entityName,
}: {
  statements: unknown[];
  signals?: RiskSignal[];
  entityName?: string;
}) {
  const containerRef  = useRef<HTMLDivElement | null>(null);
  const cyRef         = useRef<Core | null>(null);
  const [overlays, setOverlays] = useState<NodeOverlay[]>([]);
  const [edgeTooltip, setEdgeTooltip] = useState<{ x: number; y: number; text: string } | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    if (cyRef.current) { cyRef.current.destroy(); cyRef.current = null; }
    setOverlays([]);

    if (!statements.length) return;

    const elements = bodsToElements(statements as Stmt[]);
    if (elements.filter(e => !e.data.source).length === 0) {
      el.innerHTML = '<p class="text-xs text-oo-muted p-2 italic">No nodes to visualise.</p>';
      return;
    }

    const cy = cytoscape({
      container: el,
      elements,
      style: STYLESHEET,
      layout: {
        name: "dagre",
        // @ts-expect-error — dagre-specific options
        rankDir: "TB", nodeSep: 60, rankSep: 100, edgeSep: 20, animate: false,
      },
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
      minZoom: 0.2,
      maxZoom: 4,
    });

    cyRef.current = cy;

    // Build signal map once per render (signals prop changes trigger re-mount).
    const signalMap = buildSignalMap(signals);

    // Recompute HTML overlay positions whenever the viewport changes.
    // All values are in SCREEN pixels so that icons/flags render at pixel-perfect
    // size and position regardless of the current zoom level.
    function updateOverlays() {
      const pan  = cy.pan();
      const zoom = cy.zoom();
      const next: NodeOverlay[] = [];

      cy.nodes().forEach(node => {
        const pos = node.position();
        const id  = node.id();
        next.push({
          id,
          cx:      pos.x * zoom + pan.x,
          cy:      pos.y * zoom + pan.y,
          r:       (node.width() * zoom) / 2,
          icon:    node.data("icon")    as string,
          flagUrl: node.data("flagUrl") as string | undefined,
          signals: signalMap.get(id),
        });
      });

      setOverlays(next);
    }

    cy.on("viewport", updateOverlays);

    // ── Edge tooltip — hover (desktop) and tap (mobile) ──────────────────
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

    // Tap: toggle tooltip for touch / click (dismiss on second tap or background tap)
    cy.on("tap", "edge", (evt) => {
      const details = evt.target.data("details") as string | undefined;
      if (!details) return;
      const rp = evt.renderedPosition;
      setEdgeTooltip(prev =>
        prev?.text === details ? null : { x: rp.x, y: rp.y, text: details }
      );
    });

    cy.on("tap", (evt) => {
      if (evt.target === cy) setEdgeTooltip(null);
    });

    // Clear tooltip on pan/zoom so it doesn't float in a stale position.
    cy.on("viewport", () => setEdgeTooltip(null));

    cy.ready(() => {
      cy.fit(undefined, 32);
      updateOverlays();
    });

    return () => { cy.destroy(); cyRef.current = null; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statements, signals]);

  if (statements.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  return (
    <div className="bg-white border border-oo-rule rounded-oo">
      {/* Toolbar */}
      <div className="border-b border-oo-rule">
        {/* Zoom controls row */}
        <div className="flex items-center gap-1 px-2 py-1 text-xs text-oo-muted">
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
        </div>
        {/* Legend — coloured pills, wrapping rows */}
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

        {/* Pixel-perfect icon + flag + risk signal overlay */}
        <div
          style={{
            position: "absolute",
            inset: 0,
            overflow: "hidden",
            pointerEvents: "none",
          }}
        >
          {overlays.map(item => {
            const iconSize = item.r * 2 * ICON_FRACTION;
            const bw = item.r * BADGE_W_FACTOR;
            const bh = item.r * BADGE_H_FACTOR;
            // Flag: 45° NE (top-right)
            const flagCx = item.cx + item.r * Math.cos(OVERLAY_ANGLE);
            const flagCy = item.cy - item.r * Math.sin(OVERLAY_ANGLE);
            // Risk signal badge: 315° NW (top-left) per BOVS Metadata Overlays spec
            const sigCx = item.cx - item.r * Math.cos(OVERLAY_ANGLE);
            const sigCy = item.cy - item.r * Math.sin(OVERLAY_ANGLE);

            // Build risk badge
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
                // Single signal: labelled pill
                sigBadge = (
                  <div
                    title={tooltip}
                    style={{
                      position: "absolute",
                      left: sigCx - badgePx * 0.9,
                      top:  sigCy - badgePx * 0.45,
                      minWidth: badgePx * 1.8,
                      height: badgePx * 0.9,
                      background: st.bg,
                      border: `1.5px solid ${st.border}`,
                      borderRadius: badgePx,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontSize: Math.max(8, badgePx * 0.42),
                      fontWeight: 700,
                      color: st.text,
                      boxShadow: "0 1px 3px rgba(0,0,0,0.2)",
                      whiteSpace: "nowrap",
                      padding: `0 ${badgePx * 0.3}px`,
                    }}
                  >
                    {st.label}
                  </div>
                );
              } else {
                // Stack badge: "N ⚠" in worst colour, shadow circles for depth
                sigBadge = (
                  <div style={{ position: "absolute", left: sigCx - badgePx * 0.75, top: sigCy - badgePx * 0.45 }}>
                    {/* shadow circle for depth */}
                    <div style={{
                      position: "absolute", left: 3, top: 3,
                      width: badgePx * 1.5, height: badgePx * 0.9,
                      background: st.bg, border: `1.5px solid ${st.border}`,
                      borderRadius: badgePx, opacity: 0.5,
                    }}/>
                    <div
                      title={tooltip}
                      style={{
                        position: "relative",
                        minWidth: badgePx * 1.5,
                        height: badgePx * 0.9,
                        background: st.bg,
                        border: `1.5px solid ${st.border}`,
                        borderRadius: badgePx,
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        fontSize: Math.max(8, badgePx * 0.42),
                        fontWeight: 700,
                        color: st.text,
                        boxShadow: "0 1px 4px rgba(0,0,0,0.25)",
                        whiteSpace: "nowrap",
                        padding: `0 ${badgePx * 0.3}px`,
                        gap: 2,
                      }}
                    >
                      {sigs.length} ⚠
                    </div>
                  </div>
                );
              }
            }

            return (
              <div key={item.id}>
                {/* BOVS entity/person icon — centred, 60% of node */}
                <img src={item.icon} alt="" style={{
                  position: "absolute",
                  width: iconSize, height: iconSize,
                  left: item.cx - iconSize / 2, top: item.cy - iconSize / 2,
                  objectFit: "contain",
                }}/>

                {/* BOVS jurisdiction flag — 45° NE circumference */}
                {item.flagUrl && (
                  <div style={{
                    position: "absolute",
                    width: bw, height: bh,
                    left: flagCx - bw / 2, top: flagCy - bh / 2,
                    border: "1.5px solid rgba(0,0,0,0.25)",
                    borderRadius: 2, overflow: "hidden",
                    backgroundColor: "#fff",
                    boxShadow: "0 1px 3px rgba(0,0,0,0.18)",
                  }}>
                    <img src={item.flagUrl} alt=""
                      style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }}/>
                  </div>
                )}

                {/* BOVS risk signal badge — 315° NW circumference */}
                {sigBadge}
              </div>
            );
          })}
        </div>

        {/* Edge details tooltip — appears on hover/tap for edges that carry an interests.details string */}
        {edgeTooltip && (
          <div
            style={{
              position:     "absolute",
              left:         Math.min(edgeTooltip.x + 12, (containerRef.current?.clientWidth ?? 400) - 220),
              top:          Math.max(edgeTooltip.y - 48, 8),
              zIndex:       20,
              pointerEvents:"none",
              background:   "#fff",
              border:       "1px solid #d1d5db",
              borderRadius: 6,
              padding:      "6px 10px",
              fontSize:     11,
              lineHeight:   1.5,
              maxWidth:     210,
              boxShadow:    "0 2px 8px rgba(0,0,0,0.12)",
              color:        "#1a1a2e",
              whiteSpace:   "pre-wrap",
            }}
          >
            {edgeTooltip.text}
          </div>
        )}
      </div>
    </div>
  );
}
