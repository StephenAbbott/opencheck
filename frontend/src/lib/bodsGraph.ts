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

// ---------------------------------------------------------------------------
// Hierarchy helpers — collapsible parents/subsidiaries
//
// The graph is a DAG (a node can have several parents — e.g. a shared
// subsidiary), not a simple tree, so "hide the subtree below X" is a
// reachability question, not a delete. A node stays visible if it is still
// reachable from a root without passing through a collapsed node.
// ---------------------------------------------------------------------------

/** source id → list of distinct downstream (subsidiary) node ids. */
export function childAdjacency(model: GraphModel): Map<string, string[]> {
  const map = new Map<string, string[]>();
  for (const e of model.edges) {
    const arr = map.get(e.source) ?? [];
    if (!arr.includes(e.target)) arr.push(e.target);
    map.set(e.source, arr);
  }
  return map;
}

/** Ids of nodes that have at least one downstream child (can be collapsed). */
export function nodesWithChildren(model: GraphModel): Set<string> {
  const set = new Set<string>();
  for (const e of model.edges) set.add(e.source);
  return set;
}

/** Longest-path depth (0 = root) per node, via Kahn topological order.
 *  Nodes left in a cycle keep depth 0. */
export function computeLevels(model: GraphModel): Map<string, number> {
  const indeg = new Map<string, number>();
  const children = childAdjacency(model);
  for (const n of model.nodes) indeg.set(n.id, 0);
  for (const e of model.edges) indeg.set(e.target, (indeg.get(e.target) ?? 0) + 1);

  const level = new Map<string, number>();
  for (const n of model.nodes) level.set(n.id, 0);

  const queue = model.nodes.filter((n) => (indeg.get(n.id) ?? 0) === 0).map((n) => n.id);
  while (queue.length) {
    const u = queue.shift()!;
    for (const v of children.get(u) ?? []) {
      level.set(v, Math.max(level.get(v) ?? 0, (level.get(u) ?? 0) + 1));
      indeg.set(v, (indeg.get(v) ?? 0) - 1);
      if ((indeg.get(v) ?? 0) === 0) queue.push(v);
    }
  }
  return level;
}

/** All downstream descendants of `id` in the full graph. */
export function descendants(model: GraphModel, id: string): Set<string> {
  const children = childAdjacency(model);
  const out = new Set<string>();
  const stack = [...(children.get(id) ?? [])];
  while (stack.length) {
    const cur = stack.pop()!;
    if (out.has(cur)) continue;
    out.add(cur);
    for (const c of children.get(cur) ?? []) stack.push(c);
  }
  return out;
}

export interface Visibility {
  visible: Set<string>;
  hidden: Set<string>;
  /** Per collapsed node: how many of its descendants are currently hidden. */
  hiddenCount: Map<string, number>;
}

/**
 * Given a set of collapsed node ids, compute which nodes remain visible.
 *
 * A node is visible if it is reachable from a root (indegree-0 node) without
 * traversing *out of* a collapsed node. A node reachable via a non-collapsed
 * path stays visible even if one of its parents is collapsed (DAG-correct).
 * If the graph has no roots (a pure cycle), everything is shown.
 */
export function computeVisibility(model: GraphModel, collapsed: Set<string>): Visibility {
  const children = childAdjacency(model);
  const indeg = new Map<string, number>();
  for (const n of model.nodes) indeg.set(n.id, 0);
  for (const e of model.edges) indeg.set(e.target, (indeg.get(e.target) ?? 0) + 1);

  let roots = model.nodes.filter((n) => (indeg.get(n.id) ?? 0) === 0).map((n) => n.id);
  if (roots.length === 0) roots = model.nodes.map((n) => n.id); // pure cycle → show all

  const visible = new Set<string>();
  const queue = [...roots];
  while (queue.length) {
    const u = queue.shift()!;
    if (visible.has(u)) continue;
    visible.add(u);
    if (collapsed.has(u)) continue; // don't descend into a collapsed node
    for (const v of children.get(u) ?? []) queue.push(v);
  }

  const hidden = new Set<string>();
  for (const n of model.nodes) if (!visible.has(n.id)) hidden.add(n.id);

  const hiddenCount = new Map<string, number>();
  for (const c of collapsed) {
    let n = 0;
    for (const d of descendants(model, c)) if (hidden.has(d)) n += 1;
    hiddenCount.set(c, n);
  }

  return { visible, hidden, hiddenCount };
}

export interface AutoCollapseOptions {
  /** Collapse nodes at this depth or deeper (default 2 → keep top 3 levels). */
  collapseDepth?: number;
  /** Only auto-collapse when the graph is at least this deep (default 3). */
  triggerDepth?: number;
}

/**
 * Pick an initial collapsed set for a complex graph: when the structure is
 * deep (≥ triggerDepth levels), collapse every node at `collapseDepth` or
 * deeper that has children, so the first view stays readable and the user
 * expands the paths they care about. Shallow graphs collapse nothing.
 */
export function autoCollapse(model: GraphModel, opts: AutoCollapseOptions = {}): Set<string> {
  const collapseDepth = opts.collapseDepth ?? 2;
  const triggerDepth = opts.triggerDepth ?? 3;

  const levels = computeLevels(model);
  let maxLevel = 0;
  for (const v of levels.values()) maxLevel = Math.max(maxLevel, v);
  if (maxLevel < triggerDepth) return new Set();

  const hasChildren = nodesWithChildren(model);
  const collapsed = new Set<string>();
  for (const n of model.nodes) {
    if ((levels.get(n.id) ?? 0) >= collapseDepth && hasChildren.has(n.id)) collapsed.add(n.id);
  }
  return collapsed;
}

// ---------------------------------------------------------------------------
// Tree view — accessible tabular tree mirroring the graph
// ---------------------------------------------------------------------------

export interface TreeRow {
  /** Node id (matches the graph node id). */
  id: string;
  /** Unique per *occurrence* — a shared subsidiary appears under each parent. */
  rowKey: string;
  /** Nesting depth (0 = root); drives indentation and aria-level. */
  depth: number;
  label: string;
  recordType: string;
  flagUrl?: string;
  identifiers: string[];
  /** Interest label on the edge from this row's parent (undefined for roots). */
  interestLabel?: string;
  interestCategory?: EdgeCategory;
  /** Has downstream children in the full graph (so it can expand/collapse). */
  hasChildren: boolean;
  /** Number of direct children. */
  childCount: number;
  collapsed: boolean;
  /** Already shown in full above (DAG duplicate) — not expanded again here. */
  isRepeat: boolean;
}

/**
 * Flatten the DAG into an ordered list of tree rows for the accessible tree
 * pane. A node with multiple parents is shown in full under its first parent
 * and as a non-expandable "repeat" under the others (keeps the tree finite and
 * cycle-safe). Collapsed nodes are listed but their children are not.
 */
export function buildTree(model: GraphModel, collapsed: Set<string>): TreeRow[] {
  const byId = new Map(model.nodes.map((n) => [n.id, n] as const));
  const outEdges = new Map<string, { target: string; label: string; category: EdgeCategory }[]>();
  for (const e of model.edges) {
    const arr = outEdges.get(e.source) ?? [];
    arr.push({ target: e.target, label: e.label, category: e.category });
    outEdges.set(e.source, arr);
  }

  const indeg = new Map<string, number>();
  for (const n of model.nodes) indeg.set(n.id, 0);
  for (const e of model.edges) indeg.set(e.target, (indeg.get(e.target) ?? 0) + 1);
  let roots = model.nodes.filter((n) => (indeg.get(n.id) ?? 0) === 0).map((n) => n.id);
  if (roots.length === 0) roots = model.nodes.map((n) => n.id);

  const rows: TreeRow[] = [];
  const seen = new Set<string>();

  const visit = (
    id: string,
    depth: number,
    parentEdge?: { label: string; category: EdgeCategory }
  ) => {
    const node = byId.get(id);
    if (!node) return;
    const children = outEdges.get(id) ?? [];
    const isRepeat = seen.has(id);
    const isCollapsed = collapsed.has(id);

    rows.push({
      id,
      rowKey: `${rows.length}:${id}`,
      depth,
      label: node.label,
      recordType: node.recordType,
      flagUrl: node.flagUrl,
      identifiers: node.identifiers,
      interestLabel: parentEdge?.label || undefined,
      interestCategory: parentEdge?.category,
      hasChildren: children.length > 0,
      childCount: children.length,
      collapsed: isCollapsed,
      isRepeat,
    });

    seen.add(id);
    if (isRepeat || isCollapsed) return; // shown in full elsewhere, or collapsed
    for (const c of children) visit(c.target, depth + 1, { label: c.label, category: c.category });
  };

  for (const r of roots) visit(r, 0);
  return rows;
}
