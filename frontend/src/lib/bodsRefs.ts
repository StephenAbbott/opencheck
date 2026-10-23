/**
 * Resolving a relationship's parties to the statements they name (Phase 210).
 *
 * BODS v0.4 says a relationship's `subject` and `interestedParty` hold the
 * **recordId** of the party's entity or person statement. Every OpenCheck
 * mapper sets `statementId === recordId` for entities and people, so nothing
 * distinguished the two and every consumer — the graph, the tree, the
 * reconciler, the BackgroundCheck people extractor, the statement cards —
 * keyed its lookups on `statementId` and happened to work.
 *
 * Phase 208 brought the first statements OpenCheck did not write: the OECD's
 * MEIP release, where `statementId` is a hash, `recordId` is `meip-entity-N`
 * and the relationships reference the latter, as the standard says. The edge
 * then resolved to nothing and A/S Norske Shell and SHELL PLC drew as two
 * unlinked nodes.
 *
 * `refIndex` maps every spelling — statementId, recordId — to the statementId every index in the app is keyed on; `resolveRef`
 * reads a reference out of either the v0.4 bare string or the legacy wrapped
 * object and canonicalises it. Unknown references come back unchanged so a
 * dangling edge still reads as dangling. Pure; the mirror of
 * `backend/opencheck/bods/refs.py`, pinned to it by
 * `backend/tests/test_frontend_parity.py` (Phase 266).
 */

type Stmt = Record<string, unknown>;

const PARTY_TYPES = new Set(["entity", "person", "entityStatement", "personStatement"]);

/** Every spelling of a party's id → its statementId.
 *
 *  `declarationSubject` is deliberately not a key: it names the subject of
 *  the network a statement belongs to (the OECD stamps the group head on
 *  every MEIP statement; OpenCheck stamps the looked-up company on every
 *  statement of a source bundle), so it names *another* statement. Phase 210
 *  indexed it as a third tier below recordId, which in any bundle without the
 *  subject's own statement resolved the subject's id to whichever party came
 *  first. Nothing references a party by declarationSubject. */
export function refIndex(statements: Stmt[]): Map<string, string> {
  // Two tiers, in strict order: a statementId is never shadowed by another
  // statement's recordId.
  const bySid = new Map<string, string>();
  const byRid = new Map<string, string>();
  for (const s of statements) {
    const kind = (s.recordType ?? s.statementType) as string | undefined;
    if (!kind || !PARTY_TYPES.has(kind)) continue;
    const sid = (s.statementId ?? s.statementID) as string | undefined;
    if (!sid) continue;
    if (!bySid.has(sid)) bySid.set(sid, sid);
    const rid = s.recordId;
    if (typeof rid === "string" && rid && !byRid.has(rid)) byRid.set(rid, sid);
  }
  return new Map([...byRid, ...bySid]);
}

/** The reference a `subject` / `interestedParty` value carries — the v0.4
 *  bare string, or the id inside a legacy `describedBy*Statement` wrapper —
 *  or `undefined` for an unspecified record (`{reason}`) and for nothing. */
export function partyRef(raw: unknown): string | undefined {
  if (typeof raw === "string") return raw || undefined;
  if (raw && typeof raw === "object") {
    const o = raw as Stmt;
    for (const k of ["describedByEntityStatement", "describedByPersonStatement", "describedByAnonymousEntityStatement"]) {
      const v = o[k];
      if (typeof v === "string" && v) return v;
    }
  }
  return undefined;
}

/** The statementId of the statement a party reference names, in any
 *  spelling; the reference itself when the bundle holds no such statement. */
export function resolveRef(raw: unknown, index: Map<string, string>): string | undefined {
  const ref = partyRef(raw);
  if (ref === undefined) return undefined;
  return index.get(ref) ?? ref;
}
