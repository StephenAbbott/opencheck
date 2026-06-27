/**
 * reconcile — entity resolution for the FullCheck network (display transform).
 *
 * FullCheck overlays every source's BODS on one canvas, so the same real-world
 * company appears as several nodes (GLEIF's Shell, Companies House's Shell, …),
 * each with different `statementId`s. This merges entity statements that share a
 * **strong identifier** (an LEI, or a scheme-scoped registration number) into one
 * canonical node, keyed by a stable identifier-derived id, remapping every
 * reference and de-duplicating relationships. Each surviving statement carries
 * `_sources` — the distinct sources that asserted it — so provenance becomes
 * corroboration (a node 3 sources agree on reads as confirmed).
 *
 * Safe by construction: it only merges on real identifiers (LEI / company
 * number), never on names — a name-only match is a `POSSIBLY_SAME_AS` confidence
 * edge, deferred. Pure and unit-tested; applied only to the *display* model, so
 * the live expansion bookkeeping is untouched.
 */

import type { RiskSignal } from "./api";

type Stmt = Record<string, unknown>;

const LEI_RE = /^[0-9A-Z]{18}[0-9]{2}$/;

export interface ReconcileResult {
  statements: Stmt[];
  /** Original entity statementId → canonical node id. Apply to risk-signal
   *  evidence so overlays still land on the merged node. */
  remap: Record<string, string>;
}

const rd = (s: Stmt): Stmt => (s.recordDetails ?? {}) as Stmt;
const sourceOf = (s: Stmt): string =>
  String(((s.source ?? {}) as Stmt).description ?? "").trim();

/** Normalised identifier keys for an entity statement. LEIs are global (scheme
 *  ignored); everything else is scoped by its scheme so company numbers from
 *  different registers never collide. */
function identKeys(s: Stmt): string[] {
  const ids = (rd(s).identifiers ?? []) as Stmt[];
  const keys: string[] = [];
  for (const i of ids) {
    const val = String(i.id ?? "").trim().toUpperCase();
    if (!val) continue;
    if (LEI_RE.test(val)) keys.push(`LEI:${val}`);
    else keys.push(`${String(i.scheme ?? "?").trim().toUpperCase()}:${val}`);
  }
  return keys;
}

export function reconcileBods(statements: Stmt[]): ReconcileResult {
  const stmts = statements ?? [];
  const entities = stmts.filter((s) => s.recordType === "entity" && s.statementId);

  // Union-find over entity statementIds, joined by any shared identifier key.
  const parent = new Map<string, string>();
  const find = (x: string): string => {
    let r = x;
    while (parent.get(r) !== r) r = parent.get(r)!;
    let c = x;
    while (parent.get(c) !== r) {
      const n = parent.get(c)!;
      parent.set(c, r);
      c = n;
    }
    return r;
  };
  const union = (a: string, b: string) => parent.set(find(a), find(b));

  for (const s of entities) parent.set(s.statementId as string, s.statementId as string);
  const keyTo = new Map<string, string>();
  for (const s of entities) {
    const sid = s.statementId as string;
    for (const k of identKeys(s)) {
      const prev = keyTo.get(k);
      if (prev) union(prev, sid);
      else keyTo.set(k, sid);
    }
  }

  const byId = new Map<string, Stmt>(entities.map((s) => [s.statementId as string, s]));
  const groups = new Map<string, string[]>();
  for (const s of entities) {
    const root = find(s.statementId as string);
    const g = groups.get(root) ?? [];
    g.push(s.statementId as string);
    groups.set(root, g);
  }

  const remap: Record<string, string> = {};
  const canonStmt = new Map<string, Stmt>();

  for (const [root, members] of groups) {
    const allKeys = new Set<string>();
    for (const m of members) for (const k of identKeys(byId.get(m)!)) allKeys.add(k);
    // Stable canonical id: prefer the LEI (an entity's LEI never changes); else
    // the lexically smallest identifier key; else the group root.
    const lei = [...allKeys].find((k) => k.startsWith("LEI:"));
    const primary = lei ?? [...allKeys].sort()[0] ?? root;
    const canonicalId = `recon:${primary}`;

    const mergedIdents: Stmt[] = [];
    const seenIdent = new Set<string>();
    const sources = new Set<string>();
    let name = "";
    let jurisdiction: unknown;
    let entityType: unknown;
    for (const m of members) {
      remap[m] = canonicalId;
      const d = rd(byId.get(m)!);
      if (!name && d.name) name = d.name as string;
      if (!jurisdiction && d.jurisdiction) jurisdiction = d.jurisdiction;
      if (!entityType && d.entityType) entityType = d.entityType;
      for (const i of (d.identifiers ?? []) as Stmt[]) {
        const k = `${i.scheme}|${i.id}`;
        if (!seenIdent.has(k)) {
          seenIdent.add(k);
          mergedIdents.push(i);
        }
      }
      const src = sourceOf(byId.get(m)!);
      if (src) sources.add(src);
    }
    canonStmt.set(canonicalId, {
      statementId: canonicalId,
      recordId: canonicalId,
      declarationSubject: canonicalId,
      recordType: "entity",
      recordDetails: {
        entityType: entityType ?? { type: "registeredEntity" },
        name: name || canonicalId,
        identifiers: mergedIdents,
        ...(jurisdiction ? { jurisdiction } : {}),
      },
      source: byId.get(members[0])!.source ?? {},
      _sources: [...sources],
    });
  }

  const ref = (id: unknown): unknown =>
    typeof id === "string" && remap[id] ? remap[id] : id;

  const out: Stmt[] = [];
  const emittedCanon = new Set<string>();
  const relIndex = new Map<string, number>();

  for (const s of stmts) {
    if (s.recordType === "entity") {
      const cid = remap[s.statementId as string];
      if (!cid) {
        out.push({ ...s, _sources: sourceOf(s) ? [sourceOf(s)] : [] });
        continue;
      }
      if (emittedCanon.has(cid)) continue;
      emittedCanon.add(cid);
      out.push(canonStmt.get(cid)!);
    } else if (s.recordType === "relationship") {
      const d = rd(s);
      const subject = ref(d.subject);
      const party = typeof d.interestedParty === "string" ? ref(d.interestedParty) : d.interestedParty;
      const itypes = ((d.interests ?? []) as Stmt[]).map((i) => i.type).join(",");
      const key = `${subject}|${JSON.stringify(party)}|${itypes}`;
      const src = sourceOf(s);
      const existing = relIndex.get(key);
      if (existing !== undefined) {
        const prev = out[existing];
        const prevSrc = (prev._sources as string[]) ?? [];
        if (src && !prevSrc.includes(src)) prev._sources = [...prevSrc, src];
        continue;
      }
      relIndex.set(key, out.length);
      out.push({
        ...s,
        declarationSubject: ref(s.declarationSubject),
        recordDetails: { ...d, subject, interestedParty: party },
        _sources: src ? [src] : [],
      });
    } else {
      out.push({ ...s, _sources: sourceOf(s) ? [sourceOf(s)] : [] });
    }
  }

  return { statements: out, remap };
}

/** Apply an id remap to risk signals so their evidence statement-ids follow the
 *  merged node. Blunt string rewrite — opencheck ids are unique tokens. */
export function remapSignals(signals: RiskSignal[], remap: Record<string, string>): RiskSignal[] {
  if (!signals.length || !Object.keys(remap).length) return signals;
  let raw = JSON.stringify(signals);
  for (const [oldId, newId] of Object.entries(remap)) raw = raw.split(oldId).join(newId);
  return JSON.parse(raw) as RiskSignal[];
}
