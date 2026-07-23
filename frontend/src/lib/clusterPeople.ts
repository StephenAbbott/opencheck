/**
 * BackgroundCheck person clustering — Cluster UI ticket, Phase 1.
 *
 * Groups the connected people that MAY be one individual into candidate
 * clusters, scored, with per-pair evidence. Nothing is merged: a cluster is a
 * review affordance layered over the untouched `ConnectedPerson` records
 * (every statementId is still theirs, every card still screens independently).
 *
 * This supersedes `possiblySamePeople`, whose flag only caught identical
 * normalised names with a missing birth year — blind to name variants and to
 * conflicting birth years. Here:
 *   - name matching is GRADED (order swaps, middle names/initials, spelling drift);
 *   - a birth-year conflict is a SIGNAL that lowers confidence and is surfaced,
 *     never a silent "different people" veto;
 *   - a shared hard identifier (Companies House officer id, Wikidata Q-id)
 *     DOMINATES and can carry a pair to High even across a birth-year conflict;
 *   - name similarity ALONE never reaches High — that needs an identifier, or a
 *     near-exact name plus corroboration (matching birth year AND shared
 *     nationality or company).
 *
 * Pure and deterministic — unit-tested in clusterPeople.test.ts.
 */

import { nameTokens, type ConnectedPerson } from "./backgroundCheck";

/** A ConnectedPerson optionally carrying normalised "scheme:value" identifiers. */
export type ClusterablePerson = ConnectedPerson & { identifiers?: string[] };

export type PairConfidence = "high" | "medium";
export type BirthYearRelation = "match" | "conflict" | "one-missing" | "both-missing";

export interface PersonClusterPair {
  aKey: string;
  bKey: string;
  /** 0..1 blended similarity. */
  score: number;
  confidence: PairConfidence;
  nameNote: string;
  birthYearRelation: BirthYearRelation;
  sharedIdentifiers: string[];
  /** One-line human-readable justification for the grouping. */
  evidence: string;
}

export interface PersonCluster {
  /** Keys of the ConnectedPerson members (>= 2). */
  keys: string[];
  size: number;
  confidence: PairConfidence;
  pairs: PersonClusterPair[];
}

export interface ClusterResult {
  clusters: PersonCluster[];
  /** Keys of people that stand alone. */
  singletons: string[];
}

// ---- name similarity -------------------------------------------------------

function tokens(name: string): string[] {
  return nameTokens(name);
}

/** Dice coefficient over character bigrams — robust to spelling/transliteration drift. */
function diceCoefficient(a: string, b: string): number {
  if (a === b) return 1;
  if (a.length < 2 || b.length < 2) return 0;
  const bigrams = (s: string): Map<string, number> => {
    const m = new Map<string, number>();
    for (let i = 0; i < s.length - 1; i++) {
      const bg = s.slice(i, i + 2);
      m.set(bg, (m.get(bg) ?? 0) + 1);
    }
    return m;
  };
  const A = bigrams(a);
  const B = bigrams(b);
  let inter = 0;
  let sizeA = 0;
  let sizeB = 0;
  for (const v of A.values()) sizeA += v;
  for (const v of B.values()) sizeB += v;
  for (const [bg, ca] of A) {
    const cb = B.get(bg);
    if (cb) inter += Math.min(ca, cb);
  }
  return (2 * inter) / (sizeA + sizeB);
}

/** Every token of `few` matched by a token or initial of `many`. */
function tokensCoveredByInitials(few: string[], many: string[]): boolean {
  const used = new Array<boolean>(many.length).fill(false);
  return few.every((t) => {
    for (let i = 0; i < many.length; i++) {
      if (used[i]) continue;
      const m = many[i];
      if (m === t || (t.length === 1 && m[0] === t) || (m.length === 1 && t[0] === m)) {
        used[i] = true;
        return true;
      }
    }
    return false;
  });
}

export interface NameSimilarity {
  score: number;
  note: string;
}

export function nameSimilarity(aName: string, bName: string): NameSimilarity {
  const a = tokens(aName).join(" ");
  const b = tokens(bName).join(" ");
  if (a === b) return { score: 1, note: "identical name" };

  const ta = tokens(aName);
  const tb = tokens(bName);

  if ([...ta].sort().join(" ") === [...tb].sort().join(" ")) {
    return { score: 0.97, note: "same name, order differs" };
  }

  const shorter = ta.length <= tb.length ? ta : tb;
  const longer = ta.length <= tb.length ? tb : ta;

  const sameFirstLast =
    shorter.length >= 2 &&
    (shorter[0] === longer[0] || shorter[0][0] === longer[0][0]) &&
    shorter[shorter.length - 1] === longer[longer.length - 1];
  if (sameFirstLast && tokensCoveredByInitials(shorter, longer)) {
    return { score: 0.9, note: "differs only by a middle name or initial" };
  }

  const dice = diceCoefficient(a, b);
  if (dice >= 0.8) return { score: 0.8, note: "names very similar" };
  if (dice >= 0.6) return { score: 0.6, note: "names loosely similar" };
  return { score: dice, note: "names differ" };
}

// ---- other pairwise signals ------------------------------------------------

export function birthYearRelation(
  a: ClusterablePerson,
  b: ClusterablePerson
): BirthYearRelation {
  if (a.birthYear === undefined && b.birthYear === undefined) return "both-missing";
  if (a.birthYear === undefined || b.birthYear === undefined) return "one-missing";
  return a.birthYear === b.birthYear ? "match" : "conflict";
}

function overlap(a: string[] = [], b: string[] = []): string[] {
  const B = new Set(b.map((x) => x.toLowerCase()));
  return a.filter((x) => B.has(x.toLowerCase()));
}

function sharedIdentifiers(a: ClusterablePerson, b: ClusterablePerson): string[] {
  return overlap(a.identifiers ?? [], b.identifiers ?? []);
}

function companiesOf(p: ClusterablePerson): string[] {
  return (p.roles ?? []).map((r) => r.subjectName).filter((s): s is string => !!s);
}

// ---- pair scoring ----------------------------------------------------------

const MEDIUM = 0.62;

export function scorePair(
  a: ClusterablePerson,
  b: ClusterablePerson
): PersonClusterPair | null {
  const ids = sharedIdentifiers(a, b);
  const name = nameSimilarity(a.name, b.name);
  const byr = birthYearRelation(a, b);
  const nats = overlap(a.nationalities, b.nationalities);
  const cos = overlap(companiesOf(a), companiesOf(b));

  let score = name.score;
  if (byr === "match") score += 0.1;
  if (nats.length) score += 0.05;
  if (cos.length) score += 0.05;
  if (byr === "conflict") score = Math.min(score, 0.7);
  score = Math.min(score, 1);

  const identifierDominant = ids.length > 0;
  if (identifierDominant) score = Math.max(score, 0.95);

  if (score < MEDIUM && !identifierDominant) return null;

  const highOnMerit =
    name.score >= 0.9 && byr === "match" && (nats.length > 0 || cos.length > 0);
  const confidence: PairConfidence =
    identifierDominant || highOnMerit ? "high" : "medium";

  const byrNote =
    byr === "match"
      ? `both born ${a.birthYear}`
      : byr === "conflict"
      ? `birth years differ (${a.birthYear} vs ${b.birthYear})`
      : byr === "one-missing"
      ? "birth year missing on one record"
      : "no birth year on either record";

  const parts: string[] = [];
  if (ids.length) parts.push(`shared identifier ${ids.join(", ")}`);
  parts.push(name.note);
  parts.push(byrNote);
  if (nats.length) parts.push(`both ${nats.join("/")}`);
  if (cos.length) parts.push(`shared role in ${cos.join(", ")}`);

  return {
    aKey: a.key,
    bKey: b.key,
    score: Number(score.toFixed(3)),
    confidence,
    nameNote: name.note,
    birthYearRelation: byr,
    sharedIdentifiers: ids,
    evidence: parts.join("; "),
  };
}

// ---- clustering (union-find over candidate pairs) --------------------------

export function clusterConnectedPeople(people: ClusterablePerson[]): ClusterResult {
  const parent = people.map((_, i) => i);
  const find = (i: number): number => {
    while (parent[i] !== i) {
      parent[i] = parent[parent[i]];
      i = parent[i];
    }
    return i;
  };
  const union = (i: number, j: number): void => {
    parent[find(i)] = find(j);
  };

  const pairs: PersonClusterPair[] = [];
  for (let i = 0; i < people.length; i++) {
    for (let j = i + 1; j < people.length; j++) {
      const pair = scorePair(people[i], people[j]);
      if (!pair) continue;
      pairs.push(pair);
      union(i, j);
    }
  }

  const groups = new Map<number, string[]>();
  for (let i = 0; i < people.length; i++) {
    const root = find(i);
    const g = groups.get(root) ?? [];
    g.push(people[i].key);
    groups.set(root, g);
  }

  const clusters: PersonCluster[] = [];
  const clustered = new Set<string>();
  for (const keys of groups.values()) {
    if (keys.length < 2) continue;
    keys.forEach((k) => clustered.add(k));
    const memberSet = new Set(keys);
    const clusterPairs = pairs.filter(
      (p) => memberSet.has(p.aKey) && memberSet.has(p.bKey)
    );
    const confidence: PairConfidence = clusterPairs.some(
      (p) => p.confidence === "high"
    )
      ? "high"
      : "medium";
    clusters.push({ keys, size: keys.length, confidence, pairs: clusterPairs });
  }

  const singletons = people
    .map((p) => p.key)
    .filter((k) => !clustered.has(k));
  return { clusters, singletons };
}
