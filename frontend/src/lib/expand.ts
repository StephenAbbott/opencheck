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
  /** Phase 219: the relationship has ended. Read by `rankFrontier` (Phase 283). */
  ended?: boolean;
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

/** Rank a frontier before it is sent (Phase 283).
 *
 * `/expand-layer` runs at most 25 hops per call and names the rest in
 * `capped`, so the order the frontier is sent in decides which companies are
 * expanded first. Bundle order is arbitrary; this puts first:
 *
 * 1. the nodes nearest the subject — fewest ownership/control steps back
 *    towards it (down the owned edges when digging up for owners, up them
 *    when digging down for subsidiaries) to a node with nothing further that
 *    way, which is where the subject sits;
 * 2. then a node that reaches the subject along current links before one
 *    that reaches it only through an ended one;
 * 3. then a node keyed on an LEI (the fuller hop) before a register-only one;
 * 4. then the order it arrived in, so the ranking is deterministic.
 *
 * Distances are read from the raw edges the frontier was computed on. A node
 * the walk cannot place (no edge back towards the subject) sorts after every
 * placed node. Pure: the logic-only suite pins it.
 */
export function rankFrontier(
  anchors: FrontierAnchor[],
  edges: EdgeLite[],
  direction: ExpandDirection = "owners"
): FrontierAnchor[] {
  if (anchors.length < 2) return anchors;
  const oc = edges.filter((e) => e.category === "ownership" || e.category === "control");
  // The step towards the subject: owners → the company it owns; subsidiaries
  // → the company that owns it.
  const towards = new Map<string, { next: string; ended: boolean }[]>();
  const nodes = new Set<string>();
  for (const e of oc) {
    const [from, next] = direction === "owners" ? [e.source, e.target] : [e.target, e.source];
    nodes.add(from);
    nodes.add(next);
    const list = towards.get(from) ?? [];
    list.push({ next, ended: Boolean(e.ended) });
    towards.set(from, list);
  }
  // Multi-source BFS from the nodes with nothing further towards the subject.
  // `all` counts every edge; `current` only edges that have not ended.
  const distances = (useEnded: boolean): Map<string, number> => {
    const back = new Map<string, string[]>();
    for (const [from, list] of towards) {
      for (const { next, ended } of list) {
        if (ended && !useEnded) continue;
        const l = back.get(next) ?? [];
        l.push(from);
        back.set(next, l);
      }
    }
    const dist = new Map<string, number>();
    let rank: string[] = [];
    for (const n of nodes) {
      if (!towards.has(n)) {
        dist.set(n, 0);
        rank.push(n);
      }
    }
    while (rank.length) {
      const next: string[] = [];
      for (const n of rank) {
        for (const from of back.get(n) ?? []) {
          if (!dist.has(from)) {
            dist.set(from, (dist.get(n) ?? 0) + 1);
            next.push(from);
          }
        }
      }
      rank = next;
    }
    return dist;
  };
  const all = distances(true);
  const current = distances(false);
  const key = (f: FrontierAnchor, i: number): number[] => [
    all.get(f.anchor) ?? Number.MAX_SAFE_INTEGER,
    current.has(f.anchor) ? 0 : 1,
    f.lei ? 0 : 1,
    i,
  ];
  return anchors
    .map((f, i) => ({ f, k: key(f, i) }))
    .sort((a, b) => {
      for (let j = 0; j < a.k.length; j++) if (a.k[j] !== b.k[j]) return a.k[j] - b.k[j];
      return 0;
    })
    .map(({ f }) => f);
}
