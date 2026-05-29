/**
 * BODSGraph — renders a BODS v0.4 statement bundle as an interactive
 * ownership/control graph using Cytoscape.js with the dagre hierarchical
 * layout (top-to-bottom, matching BOVS vertical directionality).
 *
 * Replaces the previous @openownership/bods-dagre implementation.
 */

import { useEffect, useRef } from "react";
import cytoscape, {
  type Core,
  type ElementDefinition,
  type StylesheetStyle,
} from "cytoscape";
import dagre from "cytoscape-dagre";
import { BOVS_ICONS } from "../lib/bovsIcons";

// Register the dagre layout once at module load.
cytoscape.use(dagre);

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Stmt = Record<string, unknown>;
type RD = Record<string, unknown>;
type Interest = {
  type?: string;
  share?: { exact?: number; minimum?: number; maximum?: number; exclusiveMinimum?: number; exclusiveMaximum?: number };
  directOrIndirect?: string;
  beneficialOwnershipOrControl?: boolean;
  details?: string;
};

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

function interestLabel(interest: Interest): string {
  const base = INTEREST_LABELS[interest.type ?? ""] ?? interest.type ?? "Interest";
  const share = interest.share;
  if (!share) return base;
  if (share.exact != null) {
    const verb = base.startsWith("Owns") ? "Owns" : "Controls";
    const rest = base.startsWith("Owns") ? base.slice(4).trim() : base.slice(8).trim();
    return `${verb} ${share.exact}%${rest ? ` ${rest}` : ""}`.trim();
  }
  const lo = share.minimum ?? share.exclusiveMinimum;
  const hi = share.maximum ?? share.exclusiveMaximum;
  if (lo != null && hi != null) {
    const verb = base.startsWith("Owns") ? "Owns" : "Controls";
    return `${verb} ${lo}–${hi}%`;
  }
  return base;
}

function buildEdgeLabel(interests: Interest[]): string {
  if (!interests.length) return "";
  const sorted = [...interests].sort((a, b) => {
    if (a.beneficialOwnershipOrControl && !b.beneficialOwnershipOrControl) return -1;
    if (!a.beneficialOwnershipOrControl && b.beneficialOwnershipOrControl) return 1;
    return 0;
  });
  return sorted.slice(0, 2).map(interestLabel).join("\n");
}

// ---------------------------------------------------------------------------
// BOVS node icons (base64 data URIs) and country flag URLs
//
// WHY DATA URIS FOR ICONS:
// The BOVS SVGs (some exported from Adobe Illustrator) include xmlns:xlink
// namespace declarations. When Cytoscape's Canvas renderer calls
//   new Image().src = "/bods-dagre-images/bovs-organisation.svg"
// some browsers silently treat the SVG as potentially referencing external
// resources via xlink and refuse to draw it on a tainted canvas.
// Base64 data URIs bypass this entirely — no HTTP request, no CORS check,
// no external resource concern. They load immediately and reliably.
//
// FLAGS stay as URL paths because:
//  a) 265 SVGs × ~1KB = too large to inline
//  b) Flag SVGs are simple (no xlink) and load fine as canvas images
//  c) Flags are applied via node.style() after layout, not via stylesheet
//     data() mapper, which avoids any mapper-timing issues.
// ---------------------------------------------------------------------------

const FLAGS_BASE = "/bods-dagre-images/flags";

const ENTITY_TYPE_ICON: Record<string, string> = {
  registeredEntity:       BOVS_ICONS["registeredEntity"],
  registeredEntityListed: BOVS_ICONS["registeredEntityListed"],
  legalEntity:            BOVS_ICONS["registeredEntity"],
  arrangement:            BOVS_ICONS["arrangement"],
  anonymousEntity:        BOVS_ICONS["anonymousEntity"],
  unknownEntity:          BOVS_ICONS["unknownEntity"],
  state:                  BOVS_ICONS["state"],
  stateBody:              BOVS_ICONS["stateBody"],
};

const PERSON_TYPE_ICON: Record<string, string> = {
  knownPerson:     BOVS_ICONS["knownPerson"],
  anonymousPerson: BOVS_ICONS["anonymousPerson"],
  unknownPerson:   BOVS_ICONS["anonymousPerson"],
};

function nodeIcon(stmt: Stmt): string {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const rt = (stmt.recordType ?? stmt.statementType) as string;
  if (rt === "person" || rt === "personStatement") {
    const personType = (rd.personType as string) ?? "knownPerson";
    return PERSON_TYPE_ICON[personType] ?? BOVS_ICONS["knownPerson"];
  }
  const entityType = ((rd.entityType as RD)?.type as string) ?? "registeredEntity";
  return ENTITY_TYPE_ICON[entityType] ?? BOVS_ICONS["registeredEntity"];
}

function flagUrl(stmt: Stmt): string | null {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const jur = (rd.jurisdiction ?? rd.incorporatedInJurisdiction) as RD | undefined;
  const code = (jur?.code as string | undefined)?.toLowerCase().split("-")[0];
  if (!code) return null;
  return `${FLAGS_BASE}/${code}.svg`;
}

// ---------------------------------------------------------------------------
// BODS statement → Cytoscape element definitions
// ---------------------------------------------------------------------------

function bodsToElements(statements: Stmt[]): ElementDefinition[] {
  const elements: ElementDefinition[] = [];
  const nodeIds = new Set<string>();

  // First pass: entity and person statements → nodes
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

    const flag = flagUrl(stmt);
    const nodeData: Record<string, unknown> = {
      id,
      label: name,
      recordType: rt,
      // Data URI — loads in canvas with no HTTP or CORS step.
      icon: nodeIcon(stmt),
    };
    // flagUrl stored separately; applied via node.style() after layout.
    if (flag) nodeData.flagUrl = flag;

    elements.push({ data: nodeData });
  }

  // Second pass: relationship statements → edges
  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (rt !== "relationship" && rt !== "ownershipOrControlStatement") continue;

    const rd = (stmt.recordDetails ?? {}) as RD;
    const rawSubject = rd.subject;
    const rawIP = rd.interestedParty;

    const sourceId: string | undefined =
      typeof rawIP === "string" ? rawIP
        : (rawIP as RD | undefined)?.describedByEntityStatement as string
        ?? (rawIP as RD | undefined)?.describedByPersonStatement as string;

    const targetId: string | undefined =
      typeof rawSubject === "string" ? rawSubject
        : (rawSubject as RD | undefined)?.describedByEntityStatement as string
        ?? (rawSubject as RD | undefined)?.describedByPersonStatement as string;

    if (!sourceId || !targetId) continue;
    if (!nodeIds.has(sourceId) || !nodeIds.has(targetId)) continue;

    const interests = (rd.interests ?? []) as Interest[];
    const edgeLabel = buildEdgeLabel(interests);

    const hasOwnership = interests.some(i => i.type === "shareholding" || i.type === "votingRights");
    const hasControl = !hasOwnership && interests.some(i =>
      i.type === "appointmentOfBoard" ||
      i.type === "otherInfluenceOrControl" ||
      i.type === "controlViaCompanyRulesOrArticles" ||
      i.type === "controlByLegalFramework"
    );
    const isRole = interests.some(i =>
      i.type === "seniorManagingOfficial" || i.type === "boardMember" || i.type === "boardChair"
    );
    const category = hasOwnership ? "ownership" : hasControl ? "control" : isRole ? "role" : "unknown";

    elements.push({
      data: {
        id: (stmt.statementId ?? stmt.statementID) as string ?? `${sourceId}-${targetId}`,
        source: sourceId,
        target: targetId,
        label: edgeLabel,
        category,
      },
    });
  }

  return elements;
}

// ---------------------------------------------------------------------------
// Cytoscape stylesheet
// ---------------------------------------------------------------------------

const STYLESHEET: StylesheetStyle[] = [
  // ── Nodes ────────────────────────────────────────────────────────────────
  {
    selector: "node",
    style: {
      shape: "ellipse",
      width: 80,
      height: 80,
      "background-color": "#ffffff",
      "border-width": 2,
      "border-color": "#1a1a2e",
      // BOVS icon: data URI so canvas loading is immediate and reliable.
      // Explicit 60% size; Cytoscape defaults background-position to
      // 50% 50% (CSS semantics = centred), stable at all zoom levels.
      "background-image": "data(icon)",
      "background-width": "60%",
      "background-height": "60%",
      "background-repeat": "no-repeat",
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
    style: {
      "border-style": "dashed",
    } as cytoscape.Css.Node,
  },
  {
    selector: "node:selected",
    style: {
      "border-color": "#1565c0",
      "border-width": 3,
    } as cytoscape.Css.Node,
  },
  // Badge overlay nodes (jurisdiction flags) — suppress all inherited text/label styles.
  {
    selector: "node[isBadge]",
    style: {
      label: "",
      "text-opacity": 0,
      events: "no",
    } as cytoscape.Css.Node,
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
  {
    selector: "edge[category = 'ownership']",
    style: {
      "line-color": "#1565c0",
      "target-arrow-color": "#1565c0",
      color: "#1565c0",
    } as cytoscape.Css.Edge,
  },
  {
    selector: "edge[category = 'control']",
    style: {
      "line-color": "#e65100",
      "target-arrow-color": "#e65100",
      color: "#e65100",
    } as cytoscape.Css.Edge,
  },
  {
    selector: "edge[category = 'role']",
    style: {
      "line-color": "#6a1b9a",
      "target-arrow-color": "#6a1b9a",
      color: "#6a1b9a",
      "line-style": "dashed",
    } as cytoscape.Css.Edge,
  },
  {
    selector: "edge[category = 'unknown']",
    style: {
      "line-color": "#888",
      "target-arrow-color": "#888",
      color: "#888",
    } as cytoscape.Css.Edge,
  },
];

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function BODSGraph({ statements }: { statements: unknown[] }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    if (cyRef.current) {
      cyRef.current.destroy();
      cyRef.current = null;
    }

    if (!statements.length) return;

    const stmts = statements as Stmt[];
    const elements = bodsToElements(stmts);

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
        // @ts-expect-error — dagre-specific options not in base type
        rankDir: "TB",
        nodeSep: 60,
        rankSep: 100,
        edgeSep: 20,
        animate: false,
      },
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
      minZoom: 0.2,
      maxZoom: 4,
    });

    cyRef.current = cy;

    // After layout: add BOVS-compliant jurisdiction flag overlay badges.
    //
    // BOVS "Metadata Overlays" spec:
    //   "Metadata values are shown by overlaying icons around the circumference
    //    of the related Party's Node. Prefer positions at 45°, 135°, 225°, 315°."
    //   "Jurisdiction: identify the State using the same icon you would use for
    //    that State as a Party." → country flag at the 45° (NE) position.
    //
    // Implementation: add small locked phantom nodes positioned so their centre
    // is exactly at the 45° circumference point of each entity/person node.
    // They extend equally inside and outside the circle — this is the correct
    // BOVS overlay appearance. They are non-selectable and non-movable.
    cy.ready(() => {
      const NODE_RADIUS = 40; // half of the 80px node width
      const BADGE_W = 30;     // flag badge width  (px, model space)
      const BADGE_H = 20;     // flag badge height — 3:2 flag aspect ratio
      const ANGLE = Math.PI / 4; // 45° NE compass point

      const badgeDefs: cytoscape.ElementDefinition[] = [];

      cy.nodes().forEach((node) => {
        const flag = node.data("flagUrl") as string | undefined;
        if (!flag) return;

        const pos = node.position();
        // Centre the badge on the 45° circumference point.
        // Screen y increases downward, so NE = (cos45, -sin45).
        badgeDefs.push({
          data: {
            id: `badge-${node.id()}`,
            flagUrl: flag,
            isBadge: true,
          },
          position: {
            x: pos.x + NODE_RADIUS * Math.cos(ANGLE),
            y: pos.y - NODE_RADIUS * Math.sin(ANGLE),
          },
        });
      });

      if (badgeDefs.length > 0) {
        const badges = cy.add(badgeDefs);
        badges.forEach((badge) => {
          badge.style({
            shape: "rectangle",
            width: BADGE_W,
            height: BADGE_H,
            "background-color": "#ffffff",
            "background-image": badge.data("flagUrl") as string,
            "background-fit": "cover",
            "border-width": 1.5,
            "border-color": "#888888",
            "border-opacity": 0.7,
            label: "",
            // Badges render on top of main nodes.
            "z-index": 999,
          });
          // Prevent user interaction — badges are decorative only.
          badge.lock();
          badge.unselectify();
        });
      }

      // Fit only the main (non-badge) nodes so badges at circumference edges
      // don't cause unnecessary whitespace.
      cy.fit(cy.nodes().filter("[!isBadge]"), 32);
    });

    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [statements]);

  if (statements.length === 0) {
    return (
      <p className="text-xs text-oo-muted italic">
        No BODS statements to visualise.
      </p>
    );
  }

  return (
    <div className="bg-white border border-oo-rule rounded-oo">
      <div className="flex items-center gap-1 px-2 py-1 border-b border-oo-rule text-xs text-oo-muted">
        <button
          type="button"
          className="hover:text-oo-blue font-mono px-2"
          title="Zoom in"
          onClick={() => cyRef.current?.zoom({ level: (cyRef.current?.zoom() ?? 1) * 1.3, renderedPosition: { x: (containerRef.current?.clientWidth ?? 0) / 2, y: (containerRef.current?.clientHeight ?? 0) / 2 } })}
        >
          +
        </button>
        <button
          type="button"
          className="hover:text-oo-blue font-mono px-2"
          title="Zoom out"
          onClick={() => cyRef.current?.zoom({ level: (cyRef.current?.zoom() ?? 1) / 1.3, renderedPosition: { x: (containerRef.current?.clientWidth ?? 0) / 2, y: (containerRef.current?.clientHeight ?? 0) / 2 } })}
        >
          −
        </button>
        <button
          type="button"
          className="hover:text-oo-blue px-2"
          title="Fit to view"
          onClick={() => cyRef.current?.fit(undefined, 32)}
        >
          Fit
        </button>
        <span className="ml-auto flex items-center gap-3">
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#1565c0]"/>Ownership</span>
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#e65100]"/>Control</span>
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#6a1b9a]" style={{borderTop:"1.5px dashed #6a1b9a", background:"none"}}/>Role</span>
        </span>
      </div>
      <div
        ref={containerRef}
        className="overflow-hidden"
        style={{ width: "100%", height: 420 }}
      />
    </div>
  );
}
