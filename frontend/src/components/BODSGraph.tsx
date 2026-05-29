/**
 * BODSGraph — renders a BODS v0.4 statement bundle as an interactive
 * ownership/control graph using Cytoscape.js with the dagre hierarchical
 * layout (top-to-bottom, matching BOVS vertical directionality).
 *
 * Replaces the previous @openownership/bods-dagre implementation, which
 * required loading a UMD bundle via a classic <script> tag, fought a
 * BezierJS edge-offset bug, and couldn't produce BOVS-compliant annotations
 * without extensive post-render DOM surgery.
 *
 * Cytoscape.js advantages:
 *  - 7.9M weekly downloads; academically published; MIT licence.
 *  - Full CSS-style stylesheet: node icons, edge labels, arrowheads
 *    all declared declaratively — no DOM hacks.
 *  - cytoscape-dagre plugin re-uses the same dagre layout algorithm,
 *    so the visual hierarchy is unchanged.
 *  - BOVS icons served from /bods-dagre-images/ via background-image.
 *  - Edge labels for BOVS annotation are a first-class feature.
 */

import { useEffect, useRef } from "react";
import cytoscape, {
  type Core,
  type ElementDefinition,
  type StylesheetStyle,
} from "cytoscape";
import dagre from "cytoscape-dagre";

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

/** Build a short BOVS-compliant annotation string for one interest. */
function interestLabel(interest: Interest): string {
  const base = INTEREST_LABELS[interest.type ?? ""] ?? interest.type ?? "Interest";
  const share = interest.share;
  if (!share) return base;

  // Exact percentage
  if (share.exact != null) {
    // Replace "Owns"/"Controls…" with quantified form
    const verb = base.startsWith("Owns") ? "Owns" : "Controls";
    const rest = base.startsWith("Owns") ? base.slice(4).trim() : base.slice(8).trim();
    return `${verb} ${share.exact}%${rest ? ` ${rest}` : ""}`.trim();
  }
  // Range
  const lo = share.minimum ?? share.exclusiveMinimum;
  const hi = share.maximum ?? share.exclusiveMaximum;
  if (lo != null && hi != null) {
    const verb = base.startsWith("Owns") ? "Owns" : "Controls";
    return `${verb} ${lo}–${hi}%`;
  }
  return base;
}

/** Build a combined label from all interests on one relationship statement. */
function buildEdgeLabel(interests: Interest[]): string {
  if (!interests.length) return "";
  // Prioritise: beneficial ownership first, then others
  const sorted = [...interests].sort((a, b) => {
    if (a.beneficialOwnershipOrControl && !b.beneficialOwnershipOrControl) return -1;
    if (!a.beneficialOwnershipOrControl && b.beneficialOwnershipOrControl) return 1;
    return 0;
  });
  // Show at most 2 interests to avoid label overflow
  return sorted.slice(0, 2).map(interestLabel).join("\n");
}

// ---------------------------------------------------------------------------
// BOVS node-type → background image URL (served from /bods-dagre-images/)
// ---------------------------------------------------------------------------

const IMAGES_BASE = "/bods-dagre-images";

const ENTITY_TYPE_IMAGE: Record<string, string> = {
  registeredEntity:       `${IMAGES_BASE}/bovs-organisation.svg`,
  registeredEntityListed: `${IMAGES_BASE}/bovs-listed.svg`,
  legalEntity:            `${IMAGES_BASE}/bovs-organisation.svg`,
  arrangement:            `${IMAGES_BASE}/bovs-arrangement.svg`,
  anonymousEntity:        `${IMAGES_BASE}/bovs-entity-unknown.svg`,
  unknownEntity:          `${IMAGES_BASE}/bovs-entity-unknown.svg`,
  state:                  `${IMAGES_BASE}/bovs-state.svg`,
  stateBody:              `${IMAGES_BASE}/bovs-statebody.svg`,
};

const PERSON_TYPE_IMAGE: Record<string, string> = {
  knownPerson:     `${IMAGES_BASE}/bovs-person.svg`,
  anonymousPerson: `${IMAGES_BASE}/bovs-person-unknown.svg`,
  unknownPerson:   `${IMAGES_BASE}/bovs-person-unknown.svg`,
};

function nodeImage(stmt: Stmt): string {
  const rd = (stmt.recordDetails ?? {}) as RD;
  if (stmt.recordType === "person") {
    const personType = (rd.personType as string) ?? "knownPerson";
    return PERSON_TYPE_IMAGE[personType] ?? `${IMAGES_BASE}/bovs-person.svg`;
  }
  const entityType = ((rd.entityType as RD)?.type as string) ?? "registeredEntity";
  return ENTITY_TYPE_IMAGE[entityType] ?? `${IMAGES_BASE}/bovs-organisation.svg`;
}

function flagImage(stmt: Stmt): string | null {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const jur = (rd.jurisdiction ?? rd.incorporatedInJurisdiction) as RD | undefined;
  const code = (jur?.code as string | undefined)?.toLowerCase().split("-")[0];
  if (!code) return null;
  return `${IMAGES_BASE}/flags/${code}.svg`;
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

    elements.push({
      data: {
        id,
        label: name,
        recordType: rt,
        image: nodeImage(stmt),
        flagImage: flagImage(stmt),
        stmt,
      },
    });
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

    // Skip dangling references or unidentified BO (inline dict with "reason")
    if (!sourceId || !targetId) continue;
    if (!nodeIds.has(sourceId) || !nodeIds.has(targetId)) continue;

    const interests = (rd.interests ?? []) as Interest[];
    const edgeLabel = buildEdgeLabel(interests);

    // Determine primary interest category for edge colour
    const hasOwnership = interests.some(i =>
      i.type === "shareholding" || i.type === "votingRights");
    const hasControl = interests.some(i =>
      !hasOwnership && (
        i.type === "appointmentOfBoard" ||
        i.type === "otherInfluenceOrControl" ||
        i.type === "controlViaCompanyRulesOrArticles" ||
        i.type === "controlByLegalFramework"
      )
    );
    const isRole = interests.some(i =>
      i.type === "seniorManagingOfficial" ||
      i.type === "boardMember" ||
      i.type === "boardChair"
    );

    const category = hasOwnership ? "ownership" : hasControl ? "control" : isRole ? "role" : "unknown";

    elements.push({
      data: {
        id: (stmt.statementId ?? stmt.statementID) as string ?? `${sourceId}-${targetId}`,
        source: sourceId,
        target: targetId,
        label: edgeLabel,
        category,
        interests,
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
      // BOVS entity/person icon centred in the node
      "background-image": "data(image)",
      "background-fit": "contain",
      "background-clip": "node",
      "background-width": "60%",
      "background-height": "60%",
      "background-position-x": "50%",
      "background-position-y": "50%",
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
  // Ownership edges (shareholding / voting) — dark blue
  {
    selector: "edge[category = 'ownership']",
    style: {
      "line-color": "#1565c0",
      "target-arrow-color": "#1565c0",
      color: "#1565c0",
    } as cytoscape.Css.Edge,
  },
  // Control edges (board, influence) — orange
  {
    selector: "edge[category = 'control']",
    style: {
      "line-color": "#e65100",
      "target-arrow-color": "#e65100",
      color: "#e65100",
    } as cytoscape.Css.Edge,
  },
  // Role edges (director, board member) — purple, dashed
  {
    selector: "edge[category = 'role']",
    style: {
      "line-color": "#6a1b9a",
      "target-arrow-color": "#6a1b9a",
      color: "#6a1b9a",
      "line-style": "dashed",
    } as cytoscape.Css.Edge,
  },
  // Unknown edges — grey
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

    // Destroy any existing Cytoscape instance before re-rendering.
    if (cyRef.current) {
      cyRef.current.destroy();
      cyRef.current = null;
    }

    if (!statements.length) return;

    const stmts = statements as Stmt[];
    const elements = bodsToElements(stmts);

    if (elements.filter(e => !e.data.source).length === 0) {
      // No renderable nodes
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
        rankDir: "TB",       // top-to-bottom per BOVS vertical rule
        nodeSep: 60,
        rankSep: 100,
        edgeSep: 20,
        animate: false,
      },
      // Interaction
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
      minZoom: 0.2,
      maxZoom: 4,
    });

    cyRef.current = cy;

    // Fit the graph with padding after layout completes.
    cy.ready(() => {
      cy.fit(undefined, 32);
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
      {/* Toolbar */}
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
        {/* Legend */}
        <span className="ml-auto flex items-center gap-3">
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#1565c0]"/>Ownership</span>
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#e65100]"/>Control</span>
          <span className="flex items-center gap-1"><span className="inline-block w-4 h-0.5 bg-[#6a1b9a] border-dashed" style={{borderTop:'1.5px dashed #6a1b9a', background:'none'}}/>Role</span>
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
