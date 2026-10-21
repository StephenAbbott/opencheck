/**
 * BackgroundCheck — extract the people connected to an entity from an
 * assembled BODS bundle (SPIKE, feat/background-check).
 *
 * Walks the flat statement list from /lookup and builds one row per
 * natural person: name, birth date, nationalities, and every role the
 * bundle records for them (directorship, PSC interest, beneficial
 * ownership, shareholding…), each with the source that asserted it and
 * the company it is held in.
 *
 * People are merged on a shared identifier first (the Estonian register's
 * person key, a Wikidata item, a Companies House officer id…), then on
 * normalised name + birth year — the same person typically appears in both
 * the live Companies House bundle and the Open Ownership UK PSC bulk dataset.
 * A name alone never merges records from different sources, and two
 * different values of a one-person-per-value scheme are never merged (see
 * `groupPersonRecords`). Merging is presentational only: every underlying
 * statementId is kept so claims remain traceable to their statements.
 */

import { personIdentifierNotes } from "./annotations";
import { refIndex, resolveRef } from "./bodsRefs";
import {
  ROUTE_ACSP,
  readIdentityVerification,
  type IdentityVerification,
} from "./identityVerification";

export type Stmt = Record<string, unknown>;

export interface ConnectedPersonRole {
  /** Human label, e.g. "Director", "Ownership of shares — 25–50%". */
  label: string;
  /** Name of the entity the interest is held in, when resolvable. */
  subjectName?: string;
  /** Source description that asserted this role. */
  source?: string;
  startDate?: string;
  endDate?: string;
}

export interface ConnectedPerson {
  /** Merge key — normalised name + birth year, made unique with the first
   *  statementId when two people share both. */
  key: string;
  name: string;
  birthDate?: string;
  birthYear?: number;
  nationalities: string[];
  statementIds: string[];
  sources: string[];
  /** Normalised "scheme:value" identifiers (e.g. wikidata Q-id, CH officer
   * id) collected across the person's statements — feeds cluster matching. */
  identifiers: string[];
  roles: ConnectedPersonRole[];
  /** Companies House identity verification carried by any of this person's
   *  statements (Phase 203). Absent is "no such record", never "unverified". */
  identityVerification?: IdentityVerification;
  /** Where the New Zealand register files this person as a director or
   *  shareholder: the company's number and the name as filed. What the
   *  "Companies on the New Zealand register" control asks about — the
   *  register keys a person on nothing but that name, so the filing (whose
   *  address grades every match) is the anchor. */
  nzRoleHolder?: NzRoleHolder;
}

export interface NzRoleHolder {
  /** NZ company number, else the NZBN, of the company the role is held in. */
  companyNumber: string;
  /** The person's name as the NZ filing gives it. */
  name: string;
}

const NZ_SOURCE_ID = "nz_companies";
const NZ_SOURCE_DESCRIPTION = "New Zealand Companies Register (NZBN)";

function isNzStatement(s: Stmt): boolean {
  const source = rec(s.source);
  const id = str(source.opencheckSourceId);
  return id ? id === NZ_SOURCE_ID : str(source.description) === NZ_SOURCE_DESCRIPTION;
}

/** The NZ company number an entity statement carries (NZ-COH), else its
 *  NZBN — the two keys `/nz-associations/person` accepts. */
export function nzCompanyNumberOf(rd: Record<string, unknown>): string | undefined {
  const ids = arr(rd.identifiers).map(rec);
  const pick = (scheme: string) =>
    ids.map((i) => (str(i.scheme) === scheme ? str(i.id) : undefined)).find((v) => v && /^\d{1,13}$/.test(v));
  return pick("NZ-COH") ?? pick("NZ-NZBN");
}

function str(v: unknown): string | undefined {
  return typeof v === "string" && v ? v : undefined;
}

function rec(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : {};
}

function arr(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function personName(rd: Record<string, unknown>): string | undefined {
  const names = arr(rd.names).map(rec);
  const individual = names.find((n) => str(n.type) === "individual");
  const chosen = individual ?? names.find((n) => str(n.fullName));
  if (chosen) {
    const full = str(chosen.fullName);
    if (full) return full;
    const given = str(chosen.givenName) ?? "";
    const family = str(chosen.familyName) ?? "";
    const joined = `${given} ${family}`.trim();
    if (joined) return joined;
  }
  return undefined;
}

export function normaliseName(name: string): string {
  return name
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Courtesy titles / honorifics that carry no identity. Companies House officer
 * names embed them ("FAIRBAIRN, Carolyn Julie, Dame"); OpenCorporates does not,
 * so they are stripped before name matching to avoid blocking a merge.
 */
const HONORIFICS: ReadonlySet<string> = new Set([
  "mr", "mrs", "ms", "miss", "mx", "dr", "prof", "professor", "sir", "dame",
  "lord", "lady", "rev", "reverend", "hon", "honourable", "honorable",
]);

/**
 * Normalised name tokens with honorific titles removed (falls back to the full
 * token list if stripping would leave nothing).
 */
export function nameTokens(name: string): string[] {
  const toks = normaliseName(name).split(" ").filter(Boolean);
  const stripped = toks.filter((t) => !HONORIFICS.has(t));
  return stripped.length > 0 ? stripped : toks;
}

/**
 * Order-insensitive match key: sort the (title-stripped) normalised name tokens
 * so "NELSON PELTZ" and "PELTZ, Nelson" — and "Carolyn Julie Fairbairn" and
 * "FAIRBAIRN, Carolyn Julie, Dame" — collapse to the same key and merge. The
 * displayed name stays the first-seen original.
 */
export function nameMatchKey(name: string): string {
  return [...nameTokens(name)].sort().join(" ");
}

export function birthYearOf(birthDate?: string): number | undefined {
  const m = birthDate?.match(/^(\d{4})/);
  return m ? Number(m[1]) : undefined;
}

/** Compact human label for a BODS interest object. */
export function describeRoleInterest(interest: Record<string, unknown>): string {
  const details = str(interest.details);
  if (details) return details;
  const type = str(interest.type);
  if (!type) return "Interest (unspecified)";
  // camelCase → words, e.g. seniorManagingOfficial → "senior managing official"
  const words = type.replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase();
  const share = rec(interest.share);
  const exact = share.exact;
  if (typeof exact === "number") return `${words} — ${exact}%`;
  const min = share.minimum;
  const max = share.maximum;
  if (typeof min === "number" || typeof max === "number") {
    return `${words} — ${min ?? "?"}–${max ?? "?"}%`;
  }
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/**
 * Extract the BODS subgraph for one person: their person statements, every
 * relationship statement naming them as interested party, and the entity
 * statements those relationships point at. The result is a valid flat BODS
 * statement array — each statement keeps its `source` block, so provenance
 * and attribution travel with the download.
 */
export function extractPersonSubgraph(
  statements: Stmt[],
  personStatementIds: string[]
): Stmt[] {
  const wanted = new Set(personStatementIds);
  const subjectIds = new Set<string>();
  const persons: Stmt[] = [];
  const relationships: Stmt[] = [];

  const refs = refIndex(statements);
  for (const s of statements) {
    const id = str(s.statementId);
    const recordType = str(s.recordType) ?? str(s.statementType);
    if (id && wanted.has(id)) {
      persons.push(s);
      continue;
    }
    if (recordType === "relationship" || recordType === "ownershipOrControlStatement") {
      const rd = rec(s.recordDetails);
      const party = resolveRef(rd.interestedParty, refs);
      if (party && wanted.has(party)) {
        relationships.push(s);
        const subject = resolveRef(rd.subject, refs);
        if (subject) subjectIds.add(subject);
      }
    }
  }

  const entities = statements.filter((s) => {
    const id = str(s.statementId);
    return id !== undefined && subjectIds.has(id);
  });

  // BODS ordering convention: referenced statements before the statements
  // that reference them.
  return [...entities, ...persons, ...relationships];
}

/**
 * Identifier schemes whose values name exactly one person each: two different
 * values in one of these schemes are two different people, whatever their
 * names. The Estonian register's person key is one per person by
 * construction; a Wikidata item is one per human (Unilever's "Fernando
 * Fernández" card merged a Mexican actor's item with another person's).
 *
 * Companies House officer ids are deliberately NOT here: Companies House can
 * give one person several officer ids, and a veto on them would split people
 * the name + birth-year rule has always merged correctly.
 */
export const ONE_PERSON_PER_VALUE_SCHEMES: ReadonlySet<string> = new Set([
  "ee-ariregister-hash",
  "wikidata",
]);

interface PersonRecord {
  stmt: Stmt;
  id: string;
  name: string;
  nameKey: string;
  birthDate?: string;
  birthYear?: number;
  /** opencheckSourceId, else the source description. */
  source?: string;
  /** Normalised "scheme:value" keys. */
  identifiers: string[];
}

function personRecord(s: Stmt): PersonRecord | undefined {
  const id = str(s.statementId);
  if (!id) return undefined;
  const rd = rec(s.recordDetails);
  const personType = str(rd.personType);
  // anonymousPerson / unknownPerson carry no screenable identity.
  if (personType && personType !== "knownPerson") return undefined;
  const name = personName(rd);
  if (!name) return undefined;
  const birthDate = str(rd.birthDate);
  const identifiers: string[] = [];
  // Phase 255: a Wikidata Q-id or record id now rides in an `identifying`
  // annotation; read it back so people still group on the same key.
  for (const idObj of [...arr(rd.identifiers).map(rec), ...personIdentifierNotes(s).map(rec)]) {
    const scheme = str(idObj.scheme) ?? str(idObj.schemeName);
    const value = str(idObj.id);
    const idKey = (scheme && value ? `${scheme}:${value}` : value)?.toLowerCase();
    if (idKey && !identifiers.includes(idKey)) identifiers.push(idKey);
  }
  const source = rec(s.source);
  return {
    stmt: s,
    id,
    name,
    nameKey: nameMatchKey(name),
    birthDate,
    birthYear: birthYearOf(birthDate),
    source: str(source.opencheckSourceId) ?? str(source.description),
    identifiers,
  };
}

function schemeOf(idKey: string): string | undefined {
  const at = idKey.indexOf(":");
  return at > 0 ? idKey.slice(0, at) : undefined;
}

interface PersonGroup {
  years: Set<number>;
  sources: Set<string>;
  /** scheme → values, for ONE_PERSON_PER_VALUE_SCHEMES only. */
  strict: Map<string, Set<string>>;
}

function strictConflict(a: PersonGroup, b: PersonGroup): boolean {
  for (const [scheme, values] of a.strict) {
    const other = b.strict.get(scheme);
    if (!other) continue;
    for (const v of other) if (!values.has(v)) return true;
    for (const v of values) if (!other.has(v)) return true;
  }
  return false;
}

function intersects<T>(a: Set<T>, b: Set<T>): boolean {
  for (const v of a) if (b.has(v)) return true;
  return false;
}

/**
 * Decide which person statements are one person. Returns, for each record,
 * the index of its group's root.
 *
 * 1. Records sharing an identifier (any scheme) are one person.
 * 2. Records with the same order-insensitive name are one person when
 *    - both carry a birth year and the years agree, or
 *    - neither group carries a birth year and both come from the same source
 *      (one register naming one person on several lists of one company);
 *    never on a name alone across sources — those stay separate candidates
 *    for the clustering step to surface for review.
 * 3. Nothing is merged across two different values of a scheme that names one
 *    person per value (ONE_PERSON_PER_VALUE_SCHEMES).
 *
 * A birth year on one side and none on the other was never a merge and still
 * is not: clustering offers it as a possible match.
 */
export function groupPersonRecords(
  records: ReadonlyArray<{ nameKey: string; birthYear?: number; source?: string; identifiers: string[] }>
): number[] {
  const parent = records.map((_, i) => i);
  const groups: PersonGroup[] = records.map((r) => {
    const strict = new Map<string, Set<string>>();
    for (const idKey of r.identifiers) {
      const scheme = schemeOf(idKey);
      if (!scheme || !ONE_PERSON_PER_VALUE_SCHEMES.has(scheme)) continue;
      const set = strict.get(scheme) ?? new Set<string>();
      set.add(idKey);
      strict.set(scheme, set);
    }
    return {
      years: new Set(r.birthYear !== undefined ? [r.birthYear] : []),
      sources: new Set(r.source ? [r.source] : []),
      strict,
    };
  });
  const find = (i: number): number => {
    while (parent[i] !== i) {
      parent[i] = parent[parent[i]];
      i = parent[i];
    }
    return i;
  };
  const union = (i: number, j: number): boolean => {
    const a = find(i);
    const b = find(j);
    if (a === b) return true;
    if (strictConflict(groups[a], groups[b])) return false;
    const [keep, drop] = a < b ? [a, b] : [b, a];
    parent[drop] = keep;
    const g = groups[keep];
    const d = groups[drop];
    d.years.forEach((y) => g.years.add(y));
    d.sources.forEach((v) => g.sources.add(v));
    for (const [scheme, values] of d.strict) {
      const set = g.strict.get(scheme) ?? new Set<string>();
      values.forEach((v) => set.add(v));
      g.strict.set(scheme, set);
    }
    return true;
  };

  // 1. Shared identifiers.
  const firstWithId = new Map<string, number>();
  records.forEach((r, i) => {
    for (const idKey of r.identifiers) {
      const seen = firstWithId.get(idKey);
      if (seen === undefined) firstWithId.set(idKey, i);
      else union(seen, i);
    }
  });

  // 2. Same name, with a birth year to agree on — or one source, no years.
  const byName = new Map<string, number[]>();
  records.forEach((r, i) => {
    const list = byName.get(r.nameKey) ?? [];
    list.push(i);
    byName.set(r.nameKey, list);
  });
  for (const members of byName.values()) {
    for (let x = 1; x < members.length; x++) {
      for (let y = 0; y < x; y++) {
        const a = groups[find(members[x])];
        const b = groups[find(members[y])];
        if (find(members[x]) === find(members[y])) continue;
        const agree =
          (a.years.size > 0 && b.years.size > 0 && intersects(a.years, b.years)) ||
          (a.years.size === 0 && b.years.size === 0 && intersects(a.sources, b.sources));
        if (agree) union(members[x], members[y]);
      }
    }
  }

  return records.map((_, i) => find(i));
}

/**
 * A person is a *current* connection to the entity if they hold at least one
 * role with no end date. People whose every recorded role has ended are
 * "former" connections; people with no recorded roles are treated as current
 * (we cannot prove the connection has ended, so we do not hide them).
 */
export function isCurrentConnection(person: ConnectedPerson): boolean {
  if (person.roles.length === 0) return true;
  return person.roles.some((role) => !role.endDate);
}

/**
 * Extract natural persons + their roles from a BODS bundle.
 * Order: bundle insertion order of first appearance (subject-first for
 * a typical lookup bundle, mirroring cross_check target ordering).
 */
export function extractConnectedPeople(statements: Stmt[]): ConnectedPerson[] {
  const byStatementId = new Map<string, Stmt>();
  const refs = refIndex(statements);
  const entityNames = new Map<string, string>();
  const nzNumbers = new Map<string, string>();

  for (const s of statements) {
    const id = str(s.statementId);
    if (!id) continue;
    byStatementId.set(id, s);
    const recordType = str(s.recordType) ?? str(s.statementType);
    const rd = rec(s.recordDetails);
    if (recordType === "entity" || recordType === "entityStatement") {
      const name = str(rd.name);
      if (name) entityNames.set(id, name);
      const nz = isNzStatement(s) ? nzCompanyNumberOf(rd) : undefined;
      if (nz) nzNumbers.set(id, nz);
    }
  }

  const people = new Map<string, ConnectedPerson>();
  const keyByStatementId = new Map<string, string>();

  // ---- 1. One record per screenable person statement ----------------------
  const records: PersonRecord[] = [];
  for (const s of statements) {
    const recordType = str(s.recordType) ?? str(s.statementType);
    if (recordType !== "person" && recordType !== "personStatement") continue;
    const r = personRecord(s);
    if (r) records.push(r);
  }

  // ---- 2. Group the records: identifiers first, then name (+ birth year) --
  const groupOf = groupPersonRecords(records);

  // ---- 3. One ConnectedPerson per group, in order of first appearance -----
  const keyByGroup = new Map<number, string>();
  for (let i = 0; i < records.length; i++) {
    const r = records[i];
    const root = groupOf[i];
    let key = keyByGroup.get(root);
    let person = key ? people.get(key) : undefined;
    if (!person) {
      const first = records.find((x, j) => groupOf[j] === root && x.birthYear !== undefined);
      const birthDate = first?.birthDate;
      const birthYear = first?.birthYear;
      key = `${r.nameKey}|${birthYear ?? ""}`;
      // Two people can share a name and a (missing) birth year now that a
      // name alone no longer merges them: keep their keys apart.
      if (people.has(key)) key = `${key}|${r.id}`;
      keyByGroup.set(root, key);
      person = {
        key,
        name: r.name,
        birthDate,
        birthYear,
        nationalities: [],
        statementIds: [],
        sources: [],
        identifiers: [],
        roles: [],
      };
      people.set(key, person);
    }
    const s = r.stmt;
    const rd = rec(s.recordDetails);
    for (const n of arr(rd.nationalities).map(rec)) {
      const label = str(n.name) ?? str(n.code);
      if (label && !person.nationalities.includes(label)) {
        person.nationalities.push(label);
      }
    }
    for (const idKey of r.identifiers) {
      if (!person.identifiers.includes(idKey)) person.identifiers.push(idKey);
    }
    if (!person.statementIds.includes(r.id)) person.statementIds.push(r.id);
    // Merged rows can hold one person from two filings (a PSC notification and
    // a directorship). Keep the record that says more: an ACSP verification
    // names who verified and when; the direct route publishes neither.
    const verification = readIdentityVerification(s);
    if (
      verification &&
      (!person.identityVerification ||
        (person.identityVerification.route !== ROUTE_ACSP && verification.route === ROUTE_ACSP))
    ) {
      person.identityVerification = verification;
    }
    const source = str(rec(s.source).description);
    if (source && !person.sources.includes(source)) person.sources.push(source);
    keyByStatementId.set(r.id, key as string);
  }

  for (const s of statements) {
    const recordType = str(s.recordType) ?? str(s.statementType);
    if (recordType !== "relationship" && recordType !== "ownershipOrControlStatement") {
      continue;
    }
    const rd = rec(s.recordDetails);
    const partyId = resolveRef(rd.interestedParty, refs);
    if (!partyId) continue; // unspecified / unknown party
    const key = keyByStatementId.get(partyId);
    if (!key) continue; // party is an entity, not a person
    const person = people.get(key);
    if (!person) continue;
    const subjectId = resolveRef(rd.subject, refs) ?? "";
    const subjectName = entityNames.get(subjectId);
    if (!person.nzRoleHolder && isNzStatement(s) && nzNumbers.has(subjectId)) {
      const filed = personName(rec(byStatementId.get(partyId)?.recordDetails));
      if (filed) person.nzRoleHolder = { companyNumber: nzNumbers.get(subjectId) as string, name: filed };
    }
    const source = str(rec(s.source).description);
    const interests = arr(rd.interests).map(rec);
    const roleInterests = interests.length > 0 ? interests : [{}];
    for (const interest of roleInterests) {
      const label = describeRoleInterest(interest);
      const startDate = str(interest.startDate);
      const endDate = str(interest.endDate);
      const dup = person.roles.some(
        (r) =>
          r.label === label &&
          r.subjectName === subjectName &&
          r.endDate === endDate
      );
      if (!dup) {
        person.roles.push({ label, subjectName, source, startDate, endDate });
      }
    }
  }

  return Array.from(people.values());
}
