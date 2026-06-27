/**
 * expand — pure helpers for progressive-discovery (corporate-hop) expansion.
 *
 * These back the "Add next layer" action in BodsGraphExplorer. They are
 * framework-agnostic and unit-tested without a DOM. Person nodes are terminal,
 * so only entity statements are ever expandable, and only when they carry an
 * LEI we can re-anchor a live lookup on.
 */

type Stmt = Record<string, unknown>;

const LEI_RE = /^[0-9A-Z]{18}[0-9]{2}$/;

/** A BODS entity statement is the only expandable node type (people terminate). */
export function isEntityStatement(stmt: Stmt | undefined): boolean {
  return !!stmt && stmt.recordType === "entity";
}

/** The LEI an entity statement carries, if any — the key we expand on.
 * Prefers an identifier whose scheme names "LEI"; falls back to any identifier
 * value shaped like an LEI. Returns null when there is nothing to expand on. */
export function subjectLei(stmt: Stmt | undefined): string | null {
  const rd = (stmt?.recordDetails ?? {}) as Stmt;
  const ids = (rd.identifiers ?? []) as Stmt[];
  for (const i of ids) {
    const val = String(i.id ?? "").toUpperCase();
    const scheme = `${i.scheme ?? ""} ${i.schemeName ?? ""}`.toUpperCase();
    if (scheme.includes("LEI") && LEI_RE.test(val)) return val;
  }
  for (const i of ids) {
    const val = String(i.id ?? "").toUpperCase();
    if (LEI_RE.test(val)) return val;
  }
  return null;
}

/** A graph edge, minimally — enough to find the ownership frontier. */
export interface EdgeLite {
  source: string;
  target: string;
  category: string;
}

export interface FrontierAnchor {
  lei: string;
  anchor: string;
}

export type ExpandDirection = "owners" | "subsidiaries";

/** The current expansion frontier: entity nodes we can dig one layer past, in
 * the graph's existing direction (edges run owner → owned).
 *
 * - `owners` (ownership graph, digs *up*): a node is on the frontier when nobody
 *   shown owns it yet — it is never the *target* of an ownership/control edge.
 *   Expanding reveals its owners, one rank further up.
 * - `subsidiaries` (subsidiary tree, digs *down*): a node is on the frontier
 *   when it doesn't yet own anything shown — it is never the *source* of an
 *   ownership/control edge (a leaf). Expanding reveals its children, one rank
 *   further down.
 *
 * People are terminal and no-LEI nodes can't be resolved live, so both are
 * excluded; already-expanded anchors are skipped so each click walks outward.
 */
export function frontierAnchors(
  statements: Stmt[],
  edges: EdgeLite[],
  expandedIds: Set<string>,
  direction: ExpandDirection = "owners"
): FrontierAnchor[] {
  const oc = edges.filter((e) => e.category === "ownership" || e.category === "control");
  // owners: exclude the owned (targets). subsidiaries: exclude nodes that
  // already own something shown (sources) — i.e. keep only the leaves.
  const exclude = new Set(
    direction === "owners" ? oc.map((e) => e.target) : oc.map((e) => e.source)
  );
  const out: FrontierAnchor[] = [];
  const seen = new Set<string>();
  for (const s of statements) {
    if (!isEntityStatement(s)) continue;
    const id = s.statementId as string | undefined;
    if (!id || seen.has(id) || expandedIds.has(id) || exclude.has(id)) continue;
    const lei = subjectLei(s);
    if (!lei) continue;
    seen.add(id);
    out.push({ lei, anchor: id });
  }
  return out;
}

/** Merge two BODS bundles, de-duplicating by statementId (base wins). */
export function mergeStatements(base: Stmt[], extra: Stmt[]): Stmt[] {
  const seen = new Set(base.map((s) => s.statementId as string));
  const out = [...base];
  for (const s of extra) {
    const id = s.statementId as string | undefined;
    if (id && !seen.has(id)) {
      seen.add(id);
      out.push(s);
    }
  }
  return out;
}
