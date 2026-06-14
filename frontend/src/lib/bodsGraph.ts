/**
 * bodsGraph — pure BODS v0.4 → graph-model transform.
 *
 * This module holds the *framework-agnostic* core that the interactive
 * visualisation builds on: it turns a BODS statement bundle into a neutral
 * `GraphModel` ({ nodes, edges }) with no Cytoscape or React dependency.
 *
 * Keeping this pure means the same model feeds:
 *   - the Cytoscape graph (BODSGraph.tsx maps GraphModel → ElementDefinition[]),
 *   - search-within-graph (searchNodes),
 *   - and, in later phases, the collapsible state + the accessible tree pane.
 *
 * It is also unit-testable without a DOM (see bodsGraph.test.ts).
 */

import { BOVS_ICONS } from "./bovsIcons";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Stmt = Record<string, unknown>;
type RD = Record<string, unknown>;

export interface Interest {
  type?: string;
  share?: {
    exact?: number;
    minimum?: number;
    maximum?: number;
    exclusiveMinimum?: number;
    exclusiveMaximum?: number;
  };
  directOrIndirect?: string;
  beneficialOwnershipOrControl?: boolean;
  details?: string;
}

export type EdgeCategory = "ownership" | "control" | "role" | "unknown";

export interface GraphNode {
  id: string;
  label: string;
  recordType: string;
  /** base64 data-URI for the BOVS entity/person icon. */
  icon: string;
  /** URL for the jurisdiction flag SVG (undefined if no jurisdiction). */
  flagUrl?: string;
  /** Identifier values (e.g. LEI, company number) — used for search. */
  identifiers: string[];
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  label: string;
  category: EdgeCategory;
  details?: string;
}

export interface GraphModel {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

// ---------------------------------------------------------------------------
// BOVS interest-type → annotation label
// ---------------------------------------------------------------------------

const INTEREST_LABELS: Record<string, string> = {
  shareholding: "Owns",
  votingRights: "Controls (votes)",
  appointmentOfBoard: "Controls (board)",
  otherInfluenceOrControl: "Controls",
  controlViaCompanyRulesOrArticles: "Controls (articles)",
  controlByLegalFramework: "Controls (law)",
  seniorManagingOfficial: "Director",
  boardMember: "Board member",
  boardChair: "Chair",
  unknownInterest: "Interest (unknown)",
  unpublishedInterest: "Interest (unpublished)",
  enjoymentAndUseOfAssets: "Enjoys assets",
  rightToProfitOrIncomeFromAssets: "Profits from assets",
};

export function interestLabel(i: Interest): string {
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

export function buildEdgeLabel(interests: Interest[]): string {
  if (!interests.length) return "";
  const sorted = [...interests].sort(
    (a, b) =>
      (b.beneficialOwnershipOrControl ? 1 : 0) - (a.beneficialOwnershipOrControl ? 1 : 0)
  );
  return sorted.slice(0, 2).map(interestLabel).join("\n");
}

// ---------------------------------------------------------------------------
// BOVS icons + jurisdiction flags
// ---------------------------------------------------------------------------

export const FLAGS_BASE = "/bods-dagre-images/flags";

const ENTITY_ICON: Record<string, string> = {
  registeredEntity: BOVS_ICONS["registeredEntity"],
  registeredEntityListed: BOVS_ICONS["registeredEntityListed"],
  legalEntity: BOVS_ICONS["registeredEntity"],
  arrangement: BOVS_ICONS["arrangement"],
  anonymousEntity: BOVS_ICONS["anonymousEntity"],
  unknownEntity: BOVS_ICONS["unknownEntity"],
  state: BOVS_ICONS["state"],
  stateBody: BOVS_ICONS["stateBody"],
};

const PERSON_ICON: Record<string, string> = {
  knownPerson: BOVS_ICONS["knownPerson"],
  anonymousPerson: BOVS_ICONS["anonymousPerson"],
  unknownPerson: BOVS_ICONS["anonymousPerson"],
};

function nodeIcon(stmt: Stmt): string {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const rt = (stmt.recordType ?? stmt.statementType) as string;
  if (rt === "person" || rt === "personStatement") {
    return PERSON_ICON[(rd.personType as string) ?? "knownPerson"] ?? BOVS_ICONS["knownPerson"];
  }
  return (
    ENTITY_ICON[((rd.entityType as RD)?.type as string) ?? "registeredEntity"] ??
    BOVS_ICONS["registeredEntity"]
  );
}

function flagUrl(stmt: Stmt): string | undefined {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const jur = (rd.jurisdiction ?? rd.incorporatedInJurisdiction) as RD | undefined;
  const code = (jur?.code as string | undefined)?.toLowerCase().split("-")[0];
  return code ? `${FLAGS_BASE}/${code}.svg` : undefined;
}

function nodeIdentifiers(stmt: Stmt): string[] {
  const rd = (stmt.recordDetails ?? {}) as RD;
  const ids = (rd.identifiers as RD[] | undefined) ?? [];
  return ids
    .map((i) => (i?.id as string | undefined) ?? "")
    .filter((s): s is string => s.length > 0);
}

// ---------------------------------------------------------------------------
// BODS → GraphModel
// ---------------------------------------------------------------------------

const NODE_TYPES = new Set(["entity", "person", "entityStatement", "personStatement"]);
const REL_TYPES = new Set(["relationship", "ownershipOrControlStatement"]);

/**
 * Transform a BODS statement bundle into a neutral graph model.
 *
 * Handles both v0.3 (object refs via `describedBy*`) and v0.4 (string refs,
 * which may be a `statementId` UUID or a `declarationSubject` alias).
 */
export function bodsToGraph(statements: Stmt[]): GraphModel {
  const nodes: GraphNode[] = [];
  const nodeIds = new Set<string>();
  // v0.4 relationship endpoints reference declarationSubject (e.g. "XI-LEI-…")
  // rather than the statementId UUID. Build a lookup so edges resolve either.
  const declSubjToNodeId = new Map<string, string>();

  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (!NODE_TYPES.has(rt)) continue;
    const id = (stmt.statementId ?? stmt.statementID) as string;
    if (!id || nodeIds.has(id)) continue;
    nodeIds.add(id);

    const declSubj = stmt.declarationSubject as string | undefined;
    if (declSubj && declSubj !== id) declSubjToNodeId.set(declSubj, id);

    const rd = (stmt.recordDetails ?? {}) as RD;
    const name =
      (rd.name as string) ??
      ((rd.names as RD[] | undefined)?.[0]?.fullName as string) ??
      id.slice(-8);

    nodes.push({
      id,
      label: name,
      recordType: rt,
      icon: nodeIcon(stmt),
      flagUrl: flagUrl(stmt),
      identifiers: nodeIdentifiers(stmt),
    });
  }

  const edges: GraphEdge[] = [];

  const resolveRef = (raw: unknown): string | undefined => {
    if (typeof raw === "string") {
      return nodeIds.has(raw) ? raw : declSubjToNodeId.get(raw);
    }
    const obj = raw as RD | undefined;
    return (
      (obj?.describedByEntityStatement as string | undefined) ??
      (obj?.describedByPersonStatement as string | undefined)
    );
  };

  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (!REL_TYPES.has(rt)) continue;

    const rd = (stmt.recordDetails ?? {}) as RD;
    const sourceId = resolveRef(rd.interestedParty);
    const targetId = resolveRef(rd.subject);
    if (!sourceId || !targetId || !nodeIds.has(sourceId) || !nodeIds.has(targetId)) continue;

    const interests = (rd.interests ?? []) as Interest[];
    const hasOwnership = interests.some(
      (i) => i.type === "shareholding" || i.type === "votingRights"
    );
    const hasControl =
      !hasOwnership &&
      interests.some(
        (i) =>
          i.type === "appointmentOfBoard" ||
          i.type === "otherInfluenceOrControl" ||
          i.type === "controlViaCompanyRulesOrArticles" ||
          i.type === "controlByLegalFramework"
      );
    const isRole = interests.some(
      (i) =>
        i.type === "seniorManagingOfficial" ||
        i.type === "boardMember" ||
        i.type === "boardChair"
    );

    const detailsText =
      interests
        .map((i) => i.details)
        .filter((d): d is string => !!d)
        .join(" · ") || undefined;

    edges.push({
      id: ((stmt.statementId ?? stmt.statementID) as string) ?? `${sourceId}-${targetId}`,
      source: sourceId,
      target: targetId,
      label: buildEdgeLabel(interests),
      category: hasOwnership ? "ownership" : hasControl ? "control" : isRole ? "role" : "unknown",
      details: detailsText,
    });
  }

  return { nodes, edges };
}

// ---------------------------------------------------------------------------
// Search within graph
// ---------------------------------------------------------------------------

/**
 * Return the ids of nodes whose name or any identifier contains `query`
 * (case-insensitive substring). An empty/blank query returns no matches.
 */
export function searchNodes(nodes: GraphNode[], query: string): string[] {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  return nodes
    .filter(
      (n) =>
        n.label.toLowerCase().includes(q) ||
        n.identifiers.some((id) => id.toLowerCase().includes(q))
    )
    .map((n) => n.id);
}
