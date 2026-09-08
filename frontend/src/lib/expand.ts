/**
 * expand — pure helpers for progressive-discovery (corporate-hop) expansion.
 *
 * These back the "Add next layer" action in BodsGraphExplorer. They are
 * framework-agnostic and unit-tested without a DOM. Person nodes are terminal,
 * so only entity statements are ever expandable, and only when they carry a key
 * a hop can follow: an LEI we can re-anchor a live lookup on, or (Phase 182) a
 * register-scoped identifier — a `GB-COH` company number from a Companies
 * House PSC filing, the common case in UK chains — whose register the server
 * can dispatch on its own. Which schemes those are comes from `/expand-schemes`;
 * the frontier never assumes.
 */

import type { RiskSignal } from "./api";

type Stmt = Record<string, unknown>;

const LEI_RE = /^[0-9A-Z]{18}[0-9]{2}$/;

/** Merge two risk-signal lists, dropping exact duplicates. */
export function mergeSignals(a: RiskSignal[], b: RiskSignal[]): RiskSignal[] {
  const seen = new Set<string>();
  const out: RiskSignal[] = [];
  for (const s of [...a, ...b]) {
    const key = JSON.stringify(s);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(s);
    }
  }
  return out;
}

/** Signals in `extra` that aren't already in `base` — the QuickCheck-vs-FullCheck
 *  diff: what expanding the network surfaced beyond the subject's own screening. */
export function signalsBeyond(base: RiskSignal[], extra: RiskSignal[]): RiskSignal[] {
  const baseKeys = new Set(base.map((s) => JSON.stringify(s)));
  const seen = new Set<string>();
  const out: RiskSignal[] = [];
  for (const s of extra) {
    const key = JSON.stringify(s);
    if (baseKeys.has(key) || seen.has(key)) continue;
    seen.add(key);
    out.push(s);
  }
  return out;
}

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

/** A register-scoped identifier on an entity statement (`GB-COH` + `02999029`). */
export interface RegisterId {
  scheme: string;
  id: string;
}

/** The first identifier whose scheme the server can hop on, if any. The
 * schemes come from `/expand-schemes`; an empty set means "LEI only", which is
 * what the frontier was before Phase 182. Scheme matching is case-insensitive,
 * the id is passed as filed — the server normalises (a dropped leading zero is
 * its job, per Phase 177). */
export function subjectRegisterId(
  stmt: Stmt | undefined,
  hopSchemes: ReadonlySet<string>
): RegisterId | null {
  if (hopSchemes.size === 0) return null;
  const rd = (stmt?.recordDetails ?? {}) as Stmt;
  const ids = (rd.identifiers ?? []) as Stmt[];
  for (const i of ids) {
    const scheme = String(i.scheme ?? "").toUpperCase();
    const id = String(i.id ?? "").trim();
    if (scheme && id && hopSchemes.has(scheme)) return { scheme, id };
  }
  return null;
}

/** A graph edge, minimally — enough to find the ownership frontier. */
export interface EdgeLite {
  source: string;
  target: string;
  category: string;
}

/** A frontier node and the key its hop uses. Exactly one of `lei` or
 * (`scheme`, `id`) is set; a node carrying both is keyed on its LEI, which is
 * the fuller hop. `name` rides along for registers that fetch by name. */
export interface FrontierAnchor {
  anchor: string;
  lei?: string;
  scheme?: string;
  id?: string;
  name?: string;
}

const NO_SCHEMES: ReadonlySet<string> = new Set();

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
 * People are terminal, so they are excluded; already-expanded anchors are
 * skipped so each click walks outward. A node is keyed on its LEI when it has
 * one; otherwise (Phase 182) on the first identifier whose scheme is in
 * `hopSchemes`, but only when digging up — the subsidiary hop is GLEIF
 * Level-2 children and needs an LEI. A node with neither is a dead end.
 */
export function frontierAnchors(
  statements: Stmt[],
  edges: EdgeLite[],
  expandedIds: Set<string>,
  direction: ExpandDirection = "owners",
  hopSchemes: ReadonlySet<string> = NO_SCHEMES
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
    if (lei) {
      seen.add(id);
      out.push({ lei, anchor: id });
      continue;
    }
    if (direction !== "owners") continue;
    const reg = subjectRegisterId(s, hopSchemes);
    if (!reg) continue;
    seen.add(id);
    const name = ((s.recordDetails as Stmt | undefined)?.name as string | undefined) || undefined;
    out.push({ scheme: reg.scheme, id: reg.id, anchor: id, name });
  }
  return out;
}

/** Dedupe a raw frontier by canonical node id (the reconcile remap).
 *
 * The frontier is computed on RAW statements so live traversal keys stay
 * valid, but the FullCheck display is reconciled — several per-source
 * duplicates of one entity can sit on the raw frontier. Deduping by
 * canonical id keeps the "Add next layer — N" count equal to what the user
 * sees and expands each real-world entity once. Per canonical id the
 * LEI-keyed anchor wins over a register-keyed one — GLEIF's statement for a
 * UK company carries both the LEI and the company number, Companies House's
 * only the number, and the two reconcile to one node that should be expanded
 * once, on its LEI (Phase 182); otherwise the first raw anchor survives (its
 * statementId is what expansion bookkeeping tracks). */
export function dedupeFrontier(
  anchors: FrontierAnchor[],
  remap: Record<string, string>
): FrontierAnchor[] {
  const byCanonical = new Map<string, FrontierAnchor>();
  const order: string[] = [];
  for (const f of anchors) {
    const cid = remap[f.anchor] ?? f.anchor;
    const incumbent = byCanonical.get(cid);
    if (!incumbent) {
      byCanonical.set(cid, f);
      order.push(cid);
    } else if (!incumbent.lei && f.lei) {
      byCanonical.set(cid, f);
    }
  }
  return order.map((cid) => byCanonical.get(cid)!);
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
