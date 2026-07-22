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
 * People are merged across sources by normalised name + birth year —
 * the same person typically appears in both the live Companies House
 * bundle and the Open Ownership UK PSC bulk dataset (and, for Estonia,
 * as officer + beneficial owner + shareholder). Merging is
 * presentational only: every underlying statementId is kept so claims
 * remain traceable to their statements.
 */

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
  /** Merge key — normalised name + birth year. */
  key: string;
  name: string;
  birthDate?: string;
  birthYear?: number;
  nationalities: string[];
  statementIds: string[];
  sources: string[];
  roles: ConnectedPersonRole[];
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

export interface PossiblySamePerson {
  /** keys of the two ConnectedPerson entries */
  a: string;
  b: string;
  name: string;
  reason: string;
}

/**
 * Same-name pairs that may describe one individual — HUMAN-REVIEW
 * suggestions only, never auto-merged (mirrors the entity report's
 * "possibly the same entity" pattern).
 *
 * Suggested only when the birth years don't positively disagree: two
 * records with the same name and *different* birth years are treated as
 * genuinely different people, but when either side lacks a birth year
 * the extraction keeps them separate and this flags the pair for review.
 */
export function possiblySamePeople(
  people: ConnectedPerson[]
): PossiblySamePerson[] {
  const pairs: PossiblySamePerson[] = [];
  for (let i = 0; i < people.length; i++) {
    for (let j = i + 1; j < people.length; j++) {
      const a = people[i];
      const b = people[j];
      if (normaliseName(a.name) !== normaliseName(b.name)) continue;
      // Same name + same birth year would already have merged; same
      // name + two conflicting birth years is a real distinction.
      if (a.birthYear !== undefined && b.birthYear !== undefined) continue;
      pairs.push({
        a: a.key,
        b: b.key,
        name: a.name,
        reason:
          a.birthYear === undefined && b.birthYear === undefined
            ? "same name, no birth year on either record"
            : "same name, birth year missing on one record",
      });
    }
  }
  return pairs;
}

/**
 * Extract natural persons + their roles from a BODS bundle.
 * Order: bundle insertion order of first appearance (subject-first for
 * a typical lookup bundle, mirroring cross_check target ordering).
 */
export function extractConnectedPeople(statements: Stmt[]): ConnectedPerson[] {
  const byStatementId = new Map<string, Stmt>();
  const entityNames = new Map<string, string>();

  for (const s of statements) {
    const id = str(s.statementId);
    if (!id) continue;
    byStatementId.set(id, s);
    const recordType = str(s.recordType) ?? str(s.statementType);
    const rd = rec(s.recordDetails);
    if (recordType === "entity" || recordType === "entityStatement") {
      const name = str(rd.name);
      if (name) entityNames.set(id, name);
    }
  }

  const people = new Map<string, ConnectedPerson>();
  const keyByStatementId = new Map<string, string>();

  const addPerson = (s: Stmt): ConnectedPerson | undefined => {
    const id = str(s.statementId);
    if (!id) return undefined;
    const rd = rec(s.recordDetails);
    const personType = str(rd.personType);
    // anonymousPerson / unknownPerson carry no screenable identity.
    if (personType && personType !== "knownPerson") return undefined;
    const name = personName(rd);
    if (!name) return undefined;
    const birthDate = str(rd.birthDate);
    const birthYear = birthYearOf(birthDate);
    const key = `${normaliseName(name)}|${birthYear ?? ""}`;
    let person = people.get(key);
    if (!person) {
      person = {
        key,
        name,
        birthDate,
        birthYear,
        nationalities: [],
        statementIds: [],
        sources: [],
        roles: [],
      };
      people.set(key, person);
    }
    if (!person.birthDate && birthDate) {
      person.birthDate = birthDate;
      person.birthYear = birthYear;
    }
    for (const n of arr(rd.nationalities).map(rec)) {
      const label = str(n.name) ?? str(n.code);
      if (label && !person.nationalities.includes(label)) {
        person.nationalities.push(label);
      }
    }
    if (!person.statementIds.includes(id)) person.statementIds.push(id);
    const source = str(rec(s.source).description);
    if (source && !person.sources.includes(source)) person.sources.push(source);
    keyByStatementId.set(id, key);
    return person;
  };

  for (const s of statements) {
    const recordType = str(s.recordType) ?? str(s.statementType);
    if (recordType === "person" || recordType === "personStatement") {
      addPerson(s);
    }
  }

  for (const s of statements) {
    const recordType = str(s.recordType) ?? str(s.statementType);
    if (recordType !== "relationship" && recordType !== "ownershipOrControlStatement") {
      continue;
    }
    const rd = rec(s.recordDetails);
    const party = rd.interestedParty;
    const partyId = str(party);
    if (!partyId) continue; // unspecified / unknown party
    const key = keyByStatementId.get(partyId);
    if (!key) continue; // party is an entity, not a person
    const person = people.get(key);
    if (!person) continue;
    const subjectName = entityNames.get(str(rd.subject) ?? "");
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
