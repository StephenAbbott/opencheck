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

import { refIndex, resolveRef as resolveRefById } from "./bodsRefs";
import { BOVS_ICONS } from "./bovsIcons";
import { isIdentityVerified } from "./identityVerification";
import {
  endedPhrase,
  interestEnded,
  recordClosed,
  relationshipLifecycle,
  todayIso,
} from "./relationshipStatus";

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
  startDate?: string;
  endDate?: string;
}

export type EdgeCategory = "ownership" | "control" | "role" | "unknown" | "possiblySame";

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
  /** Distinct sources that asserted this node (provenance / corroboration).
   *  After FullCheck reconciliation a merged node carries every contributing
   *  source; otherwise the single source that emitted it. */
  sources: string[];
  /** Sources joined to this node by OpenCheck's match rather than a shared
   *  identifier (the EITI Company Assessment's LEI match). Shown and
   *  highlightable as provenance; never counted as corroboration. */
  matchedSources?: string[];
  /** Companies House has verified this person's identity (Phase 203), read
   *  from the person statement's BODS annotation. Absent means no such
   *  annotation — never "unverified". */
  identityVerified?: true;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  label: string;
  category: EdgeCategory;
  details?: string;
  /** Distinct sources that asserted this relationship (provenance). */
  sources: string[];
  /** Phase 219 — the relationship has ended: its record is closed, or every
   *  interest on it has an `endDate` on or before today. Drawn less prominently
   *  (BOVS relevance), never omitted (BOVS completeness). Absent = current. */
  ended?: true;
  /** The latest published `endDate` of an ended edge. Absent on an ended edge
   *  means the source closed the record without publishing a date. */
  endedOn?: string;
}

export interface GraphModel {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

// ---------------------------------------------------------------------------
// BOVS interest-type → annotation label
// ---------------------------------------------------------------------------

// Mirrored by the PDF diagram (backend/opencheck/reporting/diagram.py) along
// with categorise() and buildEdgeLabel's two-line cap; pinned by
// backend/tests/test_reporting_diagram_parity.py (Phase 221).
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
  // De-duplicate identical labels so a merged edge that pooled, e.g., a direct
  // and an ultimate consolidation interest (both → "Controls") shows one line.
  const seen = new Set<string>();
  const labels: string[] = [];
  for (const i of sorted) {
    const l = interestLabel(i);
    if (!seen.has(l)) {
      seen.add(l);
      labels.push(l);
    }
  }
  return labels.slice(0, 2).join("\n");
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

/** Sources the reconciler joined by match, not identifier (`_matchedSources`). */
function stmtMatchedSources(stmt: Stmt): string[] {
  const tagged = (stmt as RD)._matchedSources as string[] | undefined;
  return Array.isArray(tagged) ? tagged : [];
}

/** Provenance for a statement: the reconciler stamps `_sources` (the distinct
 *  sources that asserted it); otherwise fall back to the single source block. */
function stmtSources(stmt: Stmt): string[] {
  const tagged = (stmt as RD)._sources as string[] | undefined;
  if (Array.isArray(tagged) && tagged.length) return tagged;
  const desc = ((stmt.source as RD | undefined)?.description as string | undefined) ?? "";
  return desc ? [desc] : [];
}

// ---------------------------------------------------------------------------
// BODS → GraphModel
// ---------------------------------------------------------------------------

const NODE_TYPES = new Set(["entity", "person", "entityStatement", "personStatement"]);
const REL_TYPES = new Set(["relationship", "ownershipOrControlStatement"]);

// GLEIF Level-2 relationship records map to BODS with the accounting-
// consolidation type carried in the interest's free-text `details`.
const ULTIMATE_CONSOLIDATION = "IS_ULTIMATELY_CONSOLIDATED_BY";
const DIRECT_CONSOLIDATION = "IS_DIRECTLY_CONSOLIDATED_BY";

type ConsolidationKind = "direct" | "ultimate" | null;

/** Classify a relationship's consolidation flavour from its interest details.
 *  Recognises both the OO-bundle form (``IS_…_CONSOLIDATED_BY``) and the live
 *  GLEIF mapper form (``…direct-child`` / ``…ultimate-child``). */
function consolidationKind(interests: Interest[]): ConsolidationKind {
  let direct = false;
  let ultimate = false;
  for (const i of interests) {
    const d = i.details ?? "";
    const dl = d.toLowerCase();
    if (d.includes(ULTIMATE_CONSOLIDATION) || dl.includes("ultimate-child")) ultimate = true;
    else if (d.includes(DIRECT_CONSOLIDATION) || dl.includes("direct-child")) direct = true;
  }
  if (ultimate && !direct) return "ultimate";
  if (direct && !ultimate) return "direct";
  return null;
}

/** When a merged edge pooled both a direct and an ultimate consolidation
 *  statement (the same entity is a direct *and* ultimate child), annotate the
 *  single edge to reflect both — instead of drawing two edges or hiding one. */
function consolidationFlavour(interests: Interest[]): string | null {
  let direct = false;
  let ultimate = false;
  for (const i of interests) {
    const k = consolidationKind([i]);
    if (k === "direct") direct = true;
    else if (k === "ultimate") ultimate = true;
  }
  return direct && ultimate ? "Controls (direct + ultimate)" : null;
}

/** Edge colour-category from a (possibly pooled) interest set. Precedence:
 *  ownership → control → role → unknown. */
function categorise(interests: Interest[]): EdgeCategory {
  if (interests.some((i) => i.type === "shareholding" || i.type === "votingRights")) {
    return "ownership";
  }
  if (
    interests.some(
      (i) =>
        i.type === "appointmentOfBoard" ||
        i.type === "otherInfluenceOrControl" ||
        i.type === "controlViaCompanyRulesOrArticles" ||
        i.type === "controlByLegalFramework"
    )
  ) {
    return "control";
  }
  if (
    interests.some(
      (i) =>
        i.type === "seniorManagingOfficial" ||
        i.type === "boardMember" ||
        i.type === "boardChair"
    )
  ) {
    return "role";
  }
  return "unknown";
}

/** Distinct interest `details` strings, joined for the edge tooltip. */
function combineDetails(interests: Interest[]): string | undefined {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const i of interests) {
    const d = i.details;
    if (d && !seen.has(d)) {
      seen.add(d);
      out.push(d);
    }
  }
  return out.length ? out.join(" · ") : undefined;
}

/** Is `goal` reachable from `start` over the direct-consolidation adjacency? */
function reachableViaDirect(
  adj: Map<string, string[]>,
  start: string,
  goal: string
): boolean {
  const stack = [...(adj.get(start) ?? [])];
  const seen = new Set<string>();
  while (stack.length) {
    const cur = stack.pop()!;
    if (cur === goal) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    for (const next of adj.get(cur) ?? []) stack.push(next);
  }
  return false;
}

interface RawEdge {
  id: string;
  source: string;
  target: string;
  interests: Interest[];
  kind: ConsolidationKind;
  sources: string[];
  /** The statement's record is closed (`recordStatus: "closed"`). A merged edge
   *  is closed only when every record pooled into it is. */
  closed: boolean;
  /** BODS `recordDetails.componentRecords` on a primary indirect relationship:
   *  the recordIds of the intermediary entities and hop relationships it is
   *  assembled from (Phase 183, UK corporate-PSC chains). */
  componentRecords?: string[];
}

export interface BuildGraphOptions {
  /** B — collapse parallel same-direction edges between a pair into a single
   *  edge that pools their interests. Default true. */
  mergeParallelEdges?: boolean;
  /** C — hide an ultimate-consolidation edge when a chain of direct-
   *  consolidation edges already connects the same pair. Default true. */
  suppressRedundantUltimateConsolidation?: boolean;
  /** D — hide a primary indirect relationship (one carrying
   *  `componentRecords`) when every hop relationship it lists is itself drawn:
   *  the chain already shows it. Default true. */
  suppressRedundantComponentPrimary?: boolean;
  /** The day "ended" is judged against, as `YYYY-MM-DD` (Phase 219). Defaults
   *  to today; tests pin it so a fixture's `endDate` cannot drift into the past. */
  asOf?: string;
}

/**
 * Transform a BODS statement bundle into a neutral graph model.
 *
 * Handles both v0.3 (object refs via `describedBy*`) and v0.4 (string refs,
 * which may be a `statementId` UUID or a `declarationSubject` alias).
 *
 * By default two view-layer clean-ups run (both leave the underlying BODS
 * untouched — they only shape the rendered graph/tree, never the export):
 *   - B (`mergeParallelEdges`): one edge per entity pair, pooling interests.
 *   - C (`suppressRedundantUltimateConsolidation`): drop GLEIF ultimate-
 *     consolidation edges already implied by the direct-consolidation tree.
 *   - D (`suppressRedundantComponentPrimary`): drop a primary indirect
 *     relationship whose component hops are all drawn — the same shape as C
 *     for the BODS primary-plus-components structure (Phase 183).
 */
export function bodsToGraph(statements: Stmt[], opts: BuildGraphOptions = {}): GraphModel {
  const mergeParallelEdges = opts.mergeParallelEdges ?? true;
  const suppressRedundant = opts.suppressRedundantUltimateConsolidation ?? true;
  const suppressComponentPrimary = opts.suppressRedundantComponentPrimary ?? true;
  const asOf = opts.asOf ?? todayIso();
  const nodes: GraphNode[] = [];
  const nodeIds = new Set<string>();
  // v0.4 relationship endpoints reference the party's *recordId* (or a
  // declarationSubject alias such as "XI-LEI-…"), not its statementId, and a
  // publisher's own statements (MEIP, Phase 208) do not share the two. Resolve
  // every spelling to the node id (Phase 210).
  const refs = refIndex(statements);

  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (!NODE_TYPES.has(rt)) continue;
    const id = (stmt.statementId ?? stmt.statementID) as string;
    if (!id || nodeIds.has(id)) continue;
    nodeIds.add(id);

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
      sources: stmtSources(stmt),
      ...(stmtMatchedSources(stmt).length ? { matchedSources: stmtMatchedSources(stmt) } : {}),
      ...(isIdentityVerified(stmt) ? { identityVerified: true as const } : {}),
    });
  }

  const resolveRef = (raw: unknown): string | undefined => resolveRefById(raw, refs);

  // One raw edge per ownership-or-control statement.
  const raw: RawEdge[] = [];
  // recordIds of the entity/person statements that became nodes, and of the
  // relationship statements that became edges — what D reads.
  const nodeRecordIds = new Set<string>();
  const drawnRelationshipRecords = new Set<string>();
  for (const stmt of statements) {
    const rt = (stmt.recordType ?? stmt.statementType) as string;
    if (NODE_TYPES.has(rt) && typeof stmt.recordId === "string") nodeRecordIds.add(stmt.recordId);
    if (!REL_TYPES.has(rt)) continue;

    const rd = (stmt.recordDetails ?? {}) as RD;
    const sourceId = resolveRef(rd.interestedParty);
    const targetId = resolveRef(rd.subject);
    if (!sourceId || !targetId || !nodeIds.has(sourceId) || !nodeIds.has(targetId)) continue;

    const interests = (rd.interests ?? []) as Interest[];
    const componentRecords = Array.isArray(rd.componentRecords)
      ? (rd.componentRecords as unknown[]).filter((c): c is string => typeof c === "string")
      : undefined;
    raw.push({
      id: ((stmt.statementId ?? stmt.statementID) as string) ?? `${sourceId}-${targetId}`,
      source: sourceId,
      target: targetId,
      interests,
      kind: consolidationKind(interests),
      sources: stmtSources(stmt),
      closed: recordClosed(stmt),
      componentRecords: componentRecords?.length ? componentRecords : undefined,
    });
    if (typeof stmt.recordId === "string") drawnRelationshipRecords.add(stmt.recordId);
  }

  // C — drop an ultimate-consolidation edge when a chain of direct-consolidation
  // edges already connects the same pair. GLEIF publishes both a direct and an
  // ultimate parent record: for a group head these collapse onto the same pair
  // (a duplicate edge); for deeper members the ultimate edge just skips levels
  // already covered by the direct tree (the "star"). Only redundant *ultimate*
  // edges are removed, so a member whose direct parent is absent from the
  // subgraph keeps its ultimate link and stays connected.
  let kept = raw;
  if (suppressRedundant) {
    // Phase 219: a *current* ultimate edge is redundant only through a chain of
    // *current* direct edges. A GLEIF direct parent that has since lapsed does
    // not imply today's ultimate parent, so hiding the ultimate edge behind it
    // would draw a company's only current link as history. An ended ultimate
    // edge may still be hidden by any direct chain, as before.
    const endedRaw = (e: RawEdge) => relationshipLifecycle(e.interests, e.closed, asOf).ended;
    const buildAdj = (includeEnded: boolean) => {
      const adj = new Map<string, string[]>();
      for (const e of raw) {
        if (e.kind !== "direct" || (!includeEnded && endedRaw(e))) continue;
        const arr = adj.get(e.source) ?? [];
        if (!arr.includes(e.target)) arr.push(e.target);
        adj.set(e.source, arr);
      }
      return adj;
    };
    const directAdj = buildAdj(true);
    const currentDirectAdj = buildAdj(false);
    const directPair = new Set<string>();
    for (const e of raw) if (e.kind === "direct") directPair.add(`${e.source} ${e.target}`);
    // Keep an ultimate edge whose pair ALSO has a direct edge — that is the same
    // entity being both a direct and ultimate child, which B merges into one
    // edge annotated "direct + ultimate" (decision: merge visually, keep both
    // statements). Only drop ultimate edges that skip levels already covered by
    // the direct tree through *intermediates* (the redundant "star").
    kept = raw.filter(
      (e) =>
        !(
          e.kind === "ultimate" &&
          !directPair.has(`${e.source} ${e.target}`) &&
          reachableViaDirect(endedRaw(e) ? directAdj : currentDirectAdj, e.source, e.target)
        )
    );
  }

  // D — drop a primary indirect relationship when the hops it is assembled
  // from are all drawn. The BODS primary-plus-components structure publishes
  // both the chain (person → holding → subject, isComponent true) and one
  // primary person → subject edge listing the chain in componentRecords; on
  // the canvas the primary is the same triangle-closing "star" C removes for
  // GLEIF, so it is hidden on the same terms — the statement stays in the
  // data and the export. A primary whose hops are NOT all here (a bundle
  // trimmed elsewhere) keeps its edge, so the person stays connected.
  // componentRecords also lists the intermediary *entity* records; those
  // count as present when their node is, and a record that is in neither set
  // is missing from this bundle, which keeps the primary.
  if (suppressComponentPrimary) {
    kept = kept.filter(
      (e) =>
        !(
          e.componentRecords &&
          e.componentRecords.some((c) => drawnRelationshipRecords.has(c)) &&
          e.componentRecords.every(
            (c) => drawnRelationshipRecords.has(c) || nodeRecordIds.has(c)
          )
        )
    );
  }

  // B — merge parallel same-direction edges into one, pooling their interests
  // (so a pair carrying several statements renders as a single edge whose label
  // and tooltip list every interest — mirroring how a multi-nature Companies
  // House relationship already renders as one edge).
  let finalEdges = kept;
  if (mergeParallelEdges) {
    const groups = new Map<string, RawEdge[]>();
    for (const e of kept) {
      const key = `${e.source} ${e.target}`;
      const g = groups.get(key);
      if (g) g.push(e);
      else groups.set(key, [e]);
    }
    finalEdges = [...groups.values()].map((g) =>
      g.length === 1
        ? g[0]
        : {
            id: `${g[0].source}~${g[0].target}`,
            source: g[0].source,
            target: g[0].target,
            kind: null,
            sources: [...new Set(g.flatMap((x) => x.sources))],
            // Pooled interests lose track of which record they came from, so a
            // closed record's interests are stamped ended here (an interest
            // on a closed record has ended whatever its own endDate says).
            interests: g.flatMap((x) =>
              x.closed ? x.interests.map((i) => ({ ...i, [CLOSED_RECORD]: true })) : x.interests
            ),
            closed: g.every((x) => x.closed),
          }
    );
  }

  const edges: GraphEdge[] = finalEdges.map((e) => toGraphEdge(e, asOf));

  return { nodes, edges };
}

// ---------------------------------------------------------------------------
// Ended relationships (Phase 219)
// ---------------------------------------------------------------------------

/** Internal marker: this interest came from a closed record (set when parallel
 *  edges are pooled, so the record's status survives the merge). */
const CLOSED_RECORD = "__closedRecord";

type MarkedInterest = Interest & { [CLOSED_RECORD]?: true };

/** "ended 30 November 2024" → "Ended 30 November 2024". */
function capitalise(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * A raw (possibly pooled) edge → the GraphEdge the canvas and tree draw.
 *
 * - **Current edge** (some interest is still current): labelled, categorised
 *   and described from its *current* interests. Any ended interests pooled
 *   onto the same pair move out of the label into the details, each with its
 *   end — so a PSC whose holding moved from 25–50% to 75–100% reads as the
 *   holding it has now, not as both.
 * - **Ended edge** (the record is closed, or every interest has ended): the
 *   usual label with an "ended <date>" line under it, and `ended: true` for
 *   the stylesheet. The date line is the non-colour cue; the fade is not
 *   enough on its own (WCAG 1.4.1).
 */
function toGraphEdge(e: RawEdge, asOf: string): GraphEdge {
  const marked = e.interests as MarkedInterest[];
  const isEnded = (i: MarkedInterest) =>
    interestEnded(i, e.closed || i[CLOSED_RECORD] === true, asOf);
  // Strip the internal marker before anything is labelled or exposed.
  const strip = (i: MarkedInterest): Interest => {
    if (!(CLOSED_RECORD in i)) return i;
    const copy: MarkedInterest = { ...i };
    delete copy[CLOSED_RECORD];
    return copy;
  };
  const current = marked.filter((i) => !isEnded(i)).map(strip);
  const endedInterests = marked.filter(isEnded).map(strip);
  const all = marked.map(strip);

  // The same rule relationshipLifecycle states, applied to a pooled edge: a
  // relationship with no interests has ended only if its record is closed.
  const { ended, endedOn } = relationshipLifecycle(
    endedInterests.length === all.length ? all : [],
    e.closed || (all.length > 0 && current.length === 0),
    asOf
  );

  if (!ended) {
    const endedLines = endedInterests.map((i) => {
      const end = i.endDate && i.endDate.slice(0, 10) <= asOf ? i.endDate.slice(0, 10) : undefined;
      return `${capitalise(endedPhrase(end))}: ${interestLabel(i)}`;
    });
    const details = [combineDetails(current), ...new Set(endedLines)].filter(Boolean).join(" · ");
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      label: consolidationFlavour(current) ?? buildEdgeLabel(current),
      category: current.length ? categorise(current) : categorise(all),
      details: details || undefined,
      sources: e.sources,
    };
  }

  const phrase = endedPhrase(endedOn);
  const base = consolidationFlavour(all) ?? buildEdgeLabel(all);
  const details = [`${capitalise(phrase)}.`, combineDetails(all)].filter(Boolean).join(" ");
  return {
    id: e.id,
    source: e.source,
    target: e.target,
    label: base ? `${base}\n${phrase}` : phrase,
    category: categorise(all),
    details,
    sources: e.sources,
    ended: true,
    ...(endedOn ? { endedOn } : {}),
  };
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
  /** Phase 219 — the edge from this row's parent has ended. The tree says so in
   *  words ("ended 30 November 2024"): the canvas fade has no text equivalent
   *  otherwise, and the interest cell shows only the label's first line. */
  interestEnded: boolean;
  /** The published end date of that edge, when there is one. */
  interestEndedOn?: string;
  /** Has downstream children in the full graph (so it can expand/collapse). */
  hasChildren: boolean;
  /** Number of direct children. */
  childCount: number;
  collapsed: boolean;
  /** Already shown in full above (DAG duplicate) — not expanded again here. */
  isRepeat: boolean;
  /** No relationship statement names this party at either end. A depth-0 row
   *  otherwise looks exactly like a genuine ultimate parent, so the tree has
   *  to say which it is: the relationship table this replaced (Phase 124) named
   *  these explicitly under "Parties with no reported relationships". */
  isolated: boolean;
  /** The graph node's Companies House identity verification tick (Phase 203). */
  identityVerified: boolean;
}

/**
 * Flatten the DAG into an ordered list of tree rows for the accessible tree
 * pane. A node with multiple parents is shown in full under its first parent
 * and as a non-expandable "repeat" under the others (keeps the tree finite and
 * cycle-safe). Collapsed nodes are listed but their children are not.
 */
export function buildTree(model: GraphModel, collapsed: Set<string>): TreeRow[] {
  const byId = new Map(model.nodes.map((n) => [n.id, n] as const));
  type ParentEdge = { label: string; category: EdgeCategory; ended?: true; endedOn?: string };
  const outEdges = new Map<string, (ParentEdge & { target: string })[]>();
  for (const e of model.edges) {
    const arr = outEdges.get(e.source) ?? [];
    arr.push({ target: e.target, label: e.label, category: e.category, ended: e.ended, endedOn: e.endedOn });
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
    parentEdge?: ParentEdge
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
      interestEnded: parentEdge?.ended === true,
      ...(parentEdge?.endedOn ? { interestEndedOn: parentEdge.endedOn } : {}),
      hasChildren: children.length > 0,
      childCount: children.length,
      collapsed: isCollapsed,
      isRepeat,
      isolated: depth === 0 && children.length === 0,
      identityVerified: node.identityVerified === true,
    });

    seen.add(id);
    if (isRepeat || isCollapsed) return; // shown in full elsewhere, or collapsed
    for (const c of children) visit(c.target, depth + 1, c);
  };

  for (const r of roots) visit(r, 0);
  return rows;
}
