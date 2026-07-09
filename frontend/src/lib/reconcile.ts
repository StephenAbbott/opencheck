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

/** Scheme-name segments that mark a NON-REGISTER identifier type — tax/VAT
 *  numbers, securities/regulator ids, classification codes. These are barred
 *  from the jurisdiction+bare-value bridge in identKeys: a tax or CIK number
 *  that coincidentally equals a different entity's company number in the same
 *  jurisdiction must never merge them. Known collision classes across our
 *  sources: PL-NIP (tax) vs PL-KRS (register) are both 10 bare digits; a
 *  US-SEC-CIK can equal an unrelated state registration number; AT-UID is a
 *  VAT number that doesn't contain the string "VAT".
 *
 *  Matching is by exact scheme SEGMENT (split on "-"/"_"), not substring —
 *  "BN" must catch CA-BN (tax business number) without catching NZ-NZBN
 *  (the register number). A denylist rather than a register-scheme whitelist
 *  because scheme labels are open-ended in practice — OpenCorporates
 *  passes through org-id-style codes (CA-CC, US-DE, …) that legitimately
 *  bridge to national-adapter labels for the same number (verified live:
 *  Canada Basketball's 0343587 arrives as both CA-CORP and CA-CC); a
 *  whitelist silently drops those real merges. Non-register identifiers
 *  still merge scheme-scoped (`XI-VAT:value` etc.) — an identical
 *  scheme+value means the same entity. When adding an adapter that emits a
 *  new tax/securities/classification scheme, extend this set.
 */
const NON_REGISTER_SEGMENTS = new Set([
  "VAT", // generic VAT (DK-VAT, XI-VAT, …)
  "UID", // AT-UID — Austrian VAT
  "TVA", // French VAT
  "MOMS", // Nordic VAT
  "MWST", // Swiss/German VAT
  "KMKR", // EE-KMKR — Estonian VAT
  "DIC", // CZ-DIC — Czech tax id
  "NIP", // PL-NIP — Polish tax id
  "OIB", // HR-OIB — Croatian tax/personal id
  "BN", // CA-BN — Canadian business (tax) number
  "EIN", // US federal EIN
  "FEIN", // US federal EIN (alt label)
  "UTR", // GB unique taxpayer reference
  "TAX", // generic tax markers
  "CIK", // US-SEC-CIK — SEC filer id
  "ISIN", // securities
  "CUSIP", // securities
  "NACE", // activity classification
  "SIC", // activity classification
  "TOL", // FI-TOL — Finnish activity classification
]);

/** True when a labelled scheme may join the jurisdiction bridge: none of its
 *  segments (after the jurisdiction prefix) marks a non-register type. */
function isRegisterLikeScheme(scheme: string): boolean {
  return scheme
    .split(/[-_]/)
    .slice(1)
    .every((seg) => !NON_REGISTER_SEGMENTS.has(seg));
}

/** Normalised identifier keys for an entity statement. LEIs are global (scheme
 *  ignored); everything else is scoped by its scheme so company numbers from
 *  different registers never collide.
 *
 *  Sources disagree on the *scheme label* for the same national register —
 *  Novo Nordisk's Danish company number 24256790 arrives as scheme "" from
 *  GLEIF, DK-COA from OpenCorporates and DK-CVR from CVR — so a bare
 *  registration number ALSO keys by jurisdiction+value. That extra key is
 *  scoped to national-register schemes (empty, or "<JUR>-…") so QCC / S&P /
 *  BIC / OpenCorporates ids never cross-merge, and composite values like
 *  "dk/24256790" are skipped. */
function identKeys(s: Stmt): string[] {
  const d = rd(s);
  const ids = (d.identifiers ?? []) as Stmt[];
  const jur = String(((d.jurisdiction as Stmt | undefined)?.code as string | undefined) ?? "")
    .trim()
    .toUpperCase()
    .split("-")[0];
  const keys: string[] = [];
  for (const i of ids) {
    const val = String(i.id ?? "").trim().toUpperCase();
    if (!val) continue;
    if (LEI_RE.test(val)) {
      keys.push(`LEI:${val}`);
      continue;
    }
    const scheme = String(i.scheme ?? "?").trim().toUpperCase();
    keys.push(`${scheme}:${val}`);
    // Jurisdiction+bare-value key bridges the same registration number under
    // different scheme labels (CVR / COA / CC / empty). Unschemed identifiers
    // (GLEIF registeredAs and friends) always join; labelled schemes join only
    // when jurisdiction-prefixed AND register-like — tax/VAT/securities/
    // classification types are barred (see NON_REGISTER_SEGMENTS), so a
    // PL-NIP, US-SEC-CIK or AT-UID that coincidentally equals a different
    // entity's company number in the same jurisdiction never merges them.
    // Non-register identifiers still merge scheme-scoped (`XI-VAT:value`
    // etc.), since an identical scheme+value means the same entity.
    if (
      jur &&
      (scheme === "" || (scheme.startsWith(`${jur}-`) && isRegisterLikeScheme(scheme))) &&
      !val.includes("/")
    ) {
      keys.push(`JUR:${jur}:${val}`);
    }
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
    let foundingDate = "";
    for (const m of members) {
      remap[m] = canonicalId;
      const d = rd(byId.get(m)!);
      if (!name && d.name) name = d.name as string;
      if (!jurisdiction && d.jurisdiction) jurisdiction = d.jurisdiction;
      if (!entityType && d.entityType) entityType = d.entityType;
      if (!foundingDate && d.foundingDate) foundingDate = d.foundingDate as string;
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
        ...(foundingDate ? { foundingDate } : {}),
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

// ---------------------------------------------------------------------------
// POSSIBLY_SAME_AS — name-only "likely same" candidates (human-reviewed)
//
// Run AFTER reconcileBods on the reconciled nodes. Identifier-based merging has
// already collapsed the certain matches; this surfaces the residual: distinct
// nodes that share an exact normalised name + jurisdiction but no shared
// identifier. The Splink spike (see Notion) showed this rule beats both fuzzy
// matching and a trained probabilistic model on OpenCheck's data (F1 0.95).
//
// These are **suggestions for a human**, rendered as a dashed "likely same"
// edge — never a silent merge (a false merge is a compliance liability). A
// founding-date tiebreaker rejects the same-name/different-entity case (e.g.
// distinct same-named subsidiaries incorporated in different years); address is
// deliberately NOT used — its cross-source formatting is too noisy to require.
// ---------------------------------------------------------------------------

export interface SameAsCandidate {
  /** statementIds (canonical node ids after reconcileBods) of the two nodes. */
  a: string;
  b: string;
  /** Why they're flagged — drives the edge tooltip. */
  reason: string;
}

function normName(s: string): string {
  return s
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function jurOf(s: Stmt): string {
  return String(((rd(s).jurisdiction as Stmt | undefined)?.code as string | undefined) ?? "")
    .trim()
    .toUpperCase()
    .split("-")[0];
}

function foundingYear(s: Stmt): string | null {
  const m = String(rd(s).foundingDate ?? "").trim().match(/^(\d{4})/);
  return m ? m[1] : null;
}

/** Compatible unless BOTH founding years are present and differ. */
function dateCompatible(a: Stmt, b: Stmt): boolean {
  const ya = foundingYear(a);
  const yb = foundingYear(b);
  return !(ya && yb && ya !== yb);
}

/** Candidate "likely same" pairs among the reconciled entity nodes: exact
 *  normalised name + same jurisdiction, no shared identifier (already merged if
 *  they did), passing the founding-date tiebreaker. */
export function possiblySameAs(statements: Stmt[]): SameAsCandidate[] {
  const ents = (statements ?? []).filter((s) => s.recordType === "entity" && s.statementId);
  const groups = new Map<string, Stmt[]>();
  for (const s of ents) {
    const nm = normName(String(rd(s).name ?? ""));
    const jur = jurOf(s);
    if (!nm || !jur) continue; // both required — name alone over-merges
    const key = `${nm}|${jur}`;
    const g = groups.get(key) ?? [];
    g.push(s);
    groups.set(key, g);
  }
  const out: SameAsCandidate[] = [];
  for (const group of groups.values()) {
    if (group.length < 2) continue;
    for (let i = 0; i < group.length; i++) {
      for (let j = i + 1; j < group.length; j++) {
        const a = group[i];
        const b = group[j];
        if (a.statementId === b.statementId) continue;
        if (!dateCompatible(a, b)) continue; // different incorporation year → different entity
        out.push({
          a: a.statementId as string,
          b: b.statementId as string,
          reason: "same name + jurisdiction",
        });
      }
    }
  }
  return out;
}

/** Apply an id remap to risk signals so their evidence statement-ids follow the
 *  merged node. Blunt string rewrite — opencheck ids are unique tokens. */
export function remapSignals(signals: RiskSignal[], remap: Record<string, string>): RiskSignal[] {
  if (!signals.length || !Object.keys(remap).length) return signals;
  let raw = JSON.stringify(signals);
  for (const [oldId, newId] of Object.entries(remap)) raw = raw.split(oldId).join(newId);
  return JSON.parse(raw) as RiskSignal[];
}
