/**
 * expand — pure helpers for progressive-discovery (corporate-hop) expansion.
 *
 * SPIKE: these back the "reveal owners" action in BodsGraphExplorer. They are
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
