/**
 * knowabilityChain — the jurisdictions on the ownership path (Phase 226).
 *
 * The backend's `knowability_chain` event gives the chain to the depth the
 * lookup itself reached. FullCheck goes further: each `/expand-layer` adds a
 * rank of owners, and the reader should see what *those* jurisdictions
 * publish too. So this module walks the graph the explorer already holds —
 * upward from the subject, along ownership/control edges, ended ones included
 * (Phase 220: kept, and said) — and returns the codes in path order. The
 * component then asks `GET /knowability?jurisdictions=…` for the codes it does
 * not yet hold a statement for. The sentences are always the server's: this
 * file composes no prose (the rule at the top of `lib/knowability.ts`).
 *
 * Mirrors `opencheck.knowability.chain_jurisdictions_from` on the backend:
 * subject statements first (there may be one per source before
 * reconciliation), then each rank of owners before the rank above it,
 * persons skipped (they carry no jurisdiction), subdivisions folded to the
 * country (`US-DE` → `US`, as `statement_for` does).
 */

import type { KnowabilityChain, KnowabilityStatement } from "./api";
import type { EdgeLite } from "./expand";
import { isEntityStatement, subjectLei } from "./expand";

type Stmt = Record<string, unknown>;

/** ISO 3166-1 alpha-2 of an entity statement, or null for a person / a
 *  statement with no jurisdiction. Reads `jurisdiction.code`, falling back to
 *  `incorporatedInJurisdiction.code` (older publishers), and folds `US-DE` to
 *  `US` so the chain never asks the server for a code the table keys by
 *  country. */
export function statementJurisdiction(stmt: Stmt | undefined): string | null {
  if (!stmt || !isEntityStatement(stmt)) return null;
  const rd = (stmt.recordDetails ?? {}) as Stmt;
  const jur = (rd.jurisdiction ?? rd.incorporatedInJurisdiction) as Stmt | undefined;
  const raw = typeof jur?.code === "string" ? jur.code : null;
  if (!raw) return null;
  const code = raw.trim().toUpperCase();
  if (!/^[A-Z]{2}(-[A-Z0-9]{1,3})?$/.test(code)) return null;
  return code.slice(0, 2);
}

/** The jurisdictions on the upward path from `subjectLeiCode`, subject first
 *  then path order, deduped. Empty when no statement carries the LEI. Edges
 *  run owner → owned (`source` owns `target`), so walking up means following
 *  edges whose *target* is the current node. */
export function chainCodes(statements: Stmt[], edges: EdgeLite[], subjectLeiCode: string | null): string[] {
  if (!subjectLeiCode) return [];
  const want = subjectLeiCode.trim().toUpperCase();
  const byId = new Map<string, Stmt>();
  for (const s of statements) {
    const id = typeof s.statementId === "string" ? s.statementId : null;
    if (id && isEntityStatement(s)) byId.set(id, s);
  }
  const starts = Array.from(byId.entries())
    .filter(([, s]) => subjectLei(s)?.toUpperCase() === want)
    .map(([id]) => id);
  if (starts.length === 0) return [];

  const owners = new Map<string, string[]>();
  for (const e of edges) {
    if (e.category !== "ownership" && e.category !== "control") continue;
    const list = owners.get(e.target) ?? [];
    if (!list.includes(e.source)) list.push(e.source);
    owners.set(e.target, list);
  }

  const out: string[] = [];
  const add = (id: string) => {
    const code = statementJurisdiction(byId.get(id));
    if (code && !out.includes(code)) out.push(code);
  };
  const seen = new Set(starts);
  let rank = starts;
  for (const id of rank) add(id);
  while (rank.length) {
    const next: string[] = [];
    for (const node of rank) {
      for (const owner of owners.get(node) ?? []) {
        if (!seen.has(owner)) {
          seen.add(owner);
          next.push(owner);
        }
      }
    }
    for (const id of next) add(id);
    rank = next;
  }
  return out;
}

/** Merge the run's frozen chain with statements fetched as the network grew:
 *  `codes` is the order to show (the run's chain first, then new codes in
 *  the order the graph produced them), and the statement for each code is the
 *  first one held — the frozen one wins over a later fetch, so a sentence
 *  never changes under the reader. Codes with no statement yet are returned
 *  in `pending` for the component to fetch. */
export function mergeChain(
  frozen: KnowabilityChain | null,
  graphCodes: string[],
  fetched: ReadonlyMap<string, KnowabilityStatement>,
): { codes: string[]; statements: KnowabilityStatement[]; pending: string[] } {
  const held = new Map<string, KnowabilityStatement>();
  for (const st of frozen?.statements ?? []) if (!held.has(st.code)) held.set(st.code, st);
  for (const [code, st] of fetched) if (!held.has(code)) held.set(code, st);
  const codes: string[] = [];
  for (const c of [...(frozen?.codes ?? []), ...graphCodes]) if (!codes.includes(c)) codes.push(c);
  const statements: KnowabilityStatement[] = [];
  const pending: string[] = [];
  for (const c of codes) {
    const st = held.get(c);
    if (st) statements.push(st);
    else pending.push(c);
  }
  return { codes, statements, pending };
}

/** The sentence the list opens with. Counts jurisdictions, not companies:
 *  three Cayman holding companies are one register. */
export function chainSummary(codes: string[], subject: string | null, expanding: boolean): string {
  const others = codes.filter((c) => c !== subject);
  if (codes.length === 0) return expanding ? "Reading the ownership path." : "No ownership path mapped yet.";
  if (others.length === 0) {
    return "Every company on the mapped path is in the subject's own jurisdiction.";
  }
  return `The mapped ownership path runs through ${others.length} other ${
    others.length === 1 ? "jurisdiction" : "jurisdictions"
  }.`;
}
