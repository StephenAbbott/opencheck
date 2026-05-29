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
};

interface NodeOverlay {
  id:      string;
  cx:      number;   // screen-space x of node centre
  cy:      number;   // screen-space y of node centre
  r:       number;   // screen-space node radius
  icon:    string;   // base64 data-URI for BOVS entity/person icon
  flagUrl?: string;  // URL for jurisdiction flag SVG (null if no jurisdiction)
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

  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (rt !== "entity" && rt !== "person" && rt !== "entityStatement" && rt !== "personStatement") continue;
    const id = (stmt.statementId ?? stmt.statementID) as string;
    if (!id || nodeIds.has(id)) continue;
    nodeIds.add(id);

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

    const sourceId: string | undefined =
      typeof rawIP === "string"   ? rawIP
      : (rawIP as RD | undefined)?.describedByEntityStatement as string
      ?? (rawIP as RD | undefined)?.describedByPersonStatement as string;

    const targetId: string | undefined =
      typeof rawSubj === "string"  ? rawSubj
      : (rawSubj as RD | undefined)?.describedByEntityStatement as string
      ?? (rawSubj as RD | undefined)?.describedByPersonStatement as string;

    if (!sourceId || !targetId || !nodeIds.has(sourceId) || !nodeIds.has(targetId)) continue;

    const interests = (rd.interests ?? []) as Interest[];
    const hasOwnership = interests.some(i => i.type === "shareholding" || i.type === "votingRights");
    const hasControl   = !hasOwnership && interests.some(i =>
      i.type === "appointmentOfBoard" || i.type === "otherInfluenceOrControl" ||
      i.type === "controlViaCompanyRulesOrArticles" || i.type === "controlByLegalFramework");
    const isRole = interests.some(i =>
      i.type === "seniorManagingOfficial" || i.type === "boardMember" || i.type === "boardChair");

    elements.push({
      data: {
        id: (stmt.statementId ?? stmt.statementID) as string ?? `${sourceId}-${targetId}`,
        source: sourceId,
        target: targetId,
        label:  buildEdgeLabel(interests),
        category: hasOwnership ? "ownership" : hasControl ? "control" : isRole ? "role" : "unknown",
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

export default function BODSGraph({ statements }: { statements: unknown[] }) {
  const containerRef  = useRef<HTMLDivElement | null>(null);
  const cyRef         = useRef<Core | null>(null);
  const [overlays, setOverlays] = useState<NodeOverlay[]>([]);

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

    // Recompute HTML overlay positions whenever the viewport changes.
    // All values are in SCREEN pixels so that icons/flags render at pixel-perfect
    // size and position regardless of the current zoom level.
    function updateOverlays() {
      const pan  = cy.pan();
      const zoom = cy.zoom();
      const next: NodeOverlay[] = [];

      cy.nodes().forEach(node => {
        const pos = node.position();
        next.push({
          id:      node.id(),
          cx:      pos.x * zoom + pan.x,
          cy:      pos.y * zoom + pan.y,
          r:       (node.width() * zoom) / 2,
          icon:    node.data("icon")    as string,
          flagUrl: node.data("flagUrl") as string | undefined,
        });
      });

      setOverlays(next);
    }

    cy.on("viewport", updateOverlays);

    cy.ready(() => {
      cy.fit(undefined, 32);
      updateOverlays();
    });

    return () => { cy.destroy(); cyRef.current = null; };
  }, [statements]);

  if (statements.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  return (
    <div className="bg-white border border-oo-rule rounded-oo">
      {/* Toolbar */}
      <div className="flex items-center gap-1 px-2 py-1 border-b border-oo-rule text-xs text-oo-muted">
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
        <span className="ml-auto flex items-center gap-3">
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#1565c0]"/>Ownership</span>
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#e65100]"/>Control</span>
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#6a1b9a]" style={{borderTop:"1.5px dashed #6a1b9a",background:"none"}}/>Role</span>
        </span>
      </div>

      {/* Graph container + HTML overlay */}
      <div style={{ position: "relative" }}>
        <div
          ref={containerRef}
          className="overflow-hidden"
          style={{ width: "100%", height: 420 }}
        />

        {/* Pixel-perfect icon + flag overlay — never painted by Cytoscape canvas */}
        <div
          style={{
            position: "absolute",
            inset: 0,
            overflow: "hidden",
            pointerEvents: "none",   // clicks pass through to Cytoscape
          }}
        >
          {overlays.map(item => {
            // Icon: 60% of rendered node diameter, centred on node
            const iconSize = item.r * 2 * ICON_FRACTION;
            // Flag badge: proportional to node radius
            const bw = item.r * BADGE_W_FACTOR;
            const bh = item.r * BADGE_H_FACTOR;
            // 45° NE compass → screen: x+, y- (y increases downward in screen space)
            const flagCx = item.cx + item.r * Math.cos(OVERLAY_ANGLE);
            const flagCy = item.cy - item.r * Math.sin(OVERLAY_ANGLE);

            return (
              <div key={item.id}>
                {/* BOVS entity/person icon — always centred, always 60% of node */}
                <img
                  src={item.icon}
                  alt=""
                  style={{
                    position: "absolute",
                    width:  iconSize,
                    height: iconSize,
                    left:   item.cx - iconSize / 2,
                    top:    item.cy - iconSize / 2,
                    objectFit: "contain",
                  }}
                />

                {/* BOVS jurisdiction flag overlay — centred at 45° (NE) circumference */}
                {item.flagUrl && (
                  <div
                    style={{
                      position:        "absolute",
                      width:           bw,
                      height:          bh,
                      left:            flagCx - bw / 2,
                      top:             flagCy - bh / 2,
                      border:          "1.5px solid rgba(0,0,0,0.25)",
                      borderRadius:    2,
                      overflow:        "hidden",
                      backgroundColor: "#fff",
                      boxShadow:       "0 1px 3px rgba(0,0,0,0.18)",
                    }}
                  >
                    <img
                      src={item.flagUrl}
                      alt=""
                      style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }}
                    />
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
