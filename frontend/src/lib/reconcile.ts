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
 * Safe by construction: entities merge only on real identifiers (LEI / company
 * number), never on names — a name-only match is a `POSSIBLY_SAME_AS` confidence
 * edge, deferred. Pure and unit-tested; applied only to the *display* model, so
 * the live expansion bookkeeping is untouched.
 *
 * **People merge too, on name AND date of birth together — never either alone.**
 * Added when OpenCorporates officers started arriving (Phase 195): it mirrors
 * Companies House, so every UK board came through twice and one human was two
 * nodes. Measured on Lloyds Bank PLC's 16 serving officers, matching each
 * OpenCorporates person against the Companies House side: name tokens alone
 * matched 16 of 16, date of birth alone 15 of 16, **both together 16 of 16** —
 * a mirror, not a coincidence. The pair is the point: a name alone is the merge
 * this file has always refused, and a birth month alone is shared by thousands.
 * A person the register dates only to a year is not merged at all.
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

/** Titles a register prints in front of a name and which say nothing about who
 *  the person is. Companies House files "BENNETT, Kelly Brian" and
 *  OpenCorporates "KELLY BRIAN BENNETT"; order is handled by comparing token
 *  sets rather than strings, so only the noise words need removing. */
const NAME_TITLES = new Set([
  "mr", "mrs", "ms", "miss", "dr", "prof", "professor", "sir", "dame", "lord",
  "lady", "rev", "hon", "the", "baron", "baroness", "earl", "count", "countess",
]);

/**
 * The merge key for a person: their name tokens and their date of birth, or
 * `null` where either is missing.
 *
 * Tokens are sorted, so "BENNETT, Kelly Brian" and "KELLY BRIAN BENNETT" — the
 * same person as Companies House and OpenCorporates each write them — produce
 * one key. The birth date must carry a month: BODS legitimately publishes
 * `YYYY` where a register only dates a person to a year, and a name plus a
 * year is a weaker claim than this file is willing to merge on.
 */
export function personKey(s: Stmt): string | null {
  const d = rd(s);
  const names = (d.names ?? []) as Stmt[];
  const legal = names.find((n) => n.type === "legal") ?? names[0];
  const tokens = normName(String(legal?.fullName ?? ""))
    .split(" ")
    .filter((t) => t && !NAME_TITLES.has(t))
    .sort();
  const birth = String(d.birthDate ?? "").trim();
  if (tokens.length < 2 || !/^\d{4}-\d{2}/.test(birth)) return null;
  return `PERSON:${tokens.join("|")}|${birth.slice(0, 7)}`;
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

  // --- people ------------------------------------------------------------
  // Same mechanism, stricter key, one difference: a person nobody else
  // describes keeps their own statementId. Entities are always given a
  // canonical id because an identifier is what names them; a person is named
  // by a statement, and Phase 193 made that id mean something (the register's
  // officer id). Renaming a solo person would churn ids for no merge.
  const persons = stmts.filter((s) => s.recordType === "person" && s.statementId);
  const personGroups = new Map<string, Stmt[]>();
  for (const s of persons) {
    const k = personKey(s);
    if (!k) continue; // not enough to merge on — left as its own node
    personGroups.set(k, [...(personGroups.get(k) ?? []), s]);
  }

  for (const [key, members] of personGroups) {
    if (members.length < 2) continue;
    const canonicalId = `recon:${key}`;

    const sources = new Set<string>();
    const names: Stmt[] = [];
    const seenName = new Set<string>();
    const nationalities: Stmt[] = [];
    const seenNat = new Set<string>();
    const addresses: Stmt[] = [];
    const seenAddr = new Set<string>();
    const identifiers: Stmt[] = [];
    const seenIdent = new Set<string>();
    const annotations: Stmt[] = [];
    let birthDate = "";
    let personType: unknown;
    let politicalExposure: unknown;

    for (const m of members) {
      remap[m.statementId as string] = canonicalId;
      const d = rd(m);
      if (!birthDate && d.birthDate) birthDate = d.birthDate as string;
      if (!personType && d.personType) personType = d.personType;
      if (!politicalExposure && d.politicalExposure) politicalExposure = d.politicalExposure;
      for (const n of (d.names ?? []) as Stmt[]) {
        const k = `${n.type}|${normName(String(n.fullName ?? ""))}`;
        if (!seenName.has(k)) {
          seenName.add(k);
          names.push(n);
        }
      }
      for (const n of (d.nationalities ?? []) as Stmt[]) {
        const k = String(n.code ?? n.name ?? "");
        if (k && !seenNat.has(k)) {
          seenNat.add(k);
          nationalities.push(n);
        }
      }
      for (const a of (d.addresses ?? []) as Stmt[]) {
        const k = normName(String(a.address ?? ""));
        if (k && !seenAddr.has(k)) {
          seenAddr.add(k);
          addresses.push(a);
        }
      }
      for (const i of (d.identifiers ?? []) as Stmt[]) {
        const k = `${i.scheme}|${i.id}`;
        if (!seenIdent.has(k)) {
          seenIdent.add(k);
          identifiers.push(i);
        }
      }
      // Each source's annotations are kept, not merged: they say what THAT
      // register asserted — including the Companies House note recording
      // which officer id the person was grouped on.
      for (const a of (m.annotations ?? []) as Stmt[]) annotations.push(a);
      const src = sourceOf(m);
      if (src) sources.add(src);
    }

    canonStmt.set(canonicalId, {
      statementId: canonicalId,
      recordId: canonicalId,
      declarationSubject: canonicalId,
      recordType: "person",
      recordDetails: {
        personType: personType ?? "knownPerson",
        names,
        ...(birthDate ? { birthDate } : {}),
        ...(nationalities.length ? { nationalities } : {}),
        ...(addresses.length ? { addresses } : {}),
        ...(identifiers.length ? { identifiers } : {}),
        ...(politicalExposure ? { politicalExposure } : {}),
      },
      ...(annotations.length ? { annotations } : {}),
      source: members[0].source ?? {},
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
    } else if (s.recordType === "person") {
      const cid = remap[s.statementId as string];
      if (!cid) {
        out.push({ ...s, _sources: sourceOf(s) ? [sourceOf(s)] : [] });
        continue;
      }
      if (emittedCanon.has(cid)) continue;
      emittedCanon.add(cid);
      out.push(canonStmt.get(cid)!);
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
