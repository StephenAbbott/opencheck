#!/usr/bin/env node
/**
 * Phase 3 evaluation harness for the Cluster UI ticket — SELF-CONTAINED.
 *
 * Runs the real clustering logic over one or more /lookup bundles and prints,
 * per bundle: the connected people, the clusters formed (confidence + evidence),
 * and summary metrics. High-confidence clusters are called out for scrutiny (a
 * false High pairing two different people is the failure we most care about).
 *
 * The extraction + clustering below mirror frontend/src/lib/backgroundCheck.ts
 * (order-insensitive nameMatchKey) and clusterPeople.ts verbatim, so verdicts
 * match the live UI. No imports — drop it anywhere and run.
 *
 * Usage:
 *   node cluster-eval.mjs <bundle1.json> [bundle2.json ...]
 *   curl 'http://127.0.0.1:8000/lookup?lei=<LEI>&refresh=true' > company.json
 */

import { readFileSync } from "node:fs";

// ---- name matching (mirror of backgroundCheck.ts + clusterPeople.ts) -------
function normaliseName(name) {
  return name
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}
const HONORIFICS = new Set([
  "mr", "mrs", "ms", "miss", "mx", "dr", "prof", "professor", "sir", "dame",
  "lord", "lady", "rev", "reverend", "hon", "honourable", "honorable",
]);
function nameTokens(name) {
  const toks = normaliseName(name).split(" ").filter(Boolean);
  const stripped = toks.filter((t) => !HONORIFICS.has(t));
  return stripped.length > 0 ? stripped : toks;
}
function nameMatchKey(name) {
  return [...nameTokens(name)].sort().join(" ");
}
function tokens(name) {
  return nameTokens(name);
}
function diceCoefficient(a, b) {
  if (a === b) return 1;
  if (a.length < 2 || b.length < 2) return 0;
  const bigrams = (s) => {
    const m = new Map();
    for (let i = 0; i < s.length - 1; i++) {
      const bg = s.slice(i, i + 2);
      m.set(bg, (m.get(bg) ?? 0) + 1);
    }
    return m;
  };
  const A = bigrams(a), B = bigrams(b);
  let inter = 0, sizeA = 0, sizeB = 0;
  for (const v of A.values()) sizeA += v;
  for (const v of B.values()) sizeB += v;
  for (const [bg, ca] of A) { const cb = B.get(bg); if (cb) inter += Math.min(ca, cb); }
  return (2 * inter) / (sizeA + sizeB);
}
function tokensCoveredByInitials(few, many) {
  const used = new Array(many.length).fill(false);
  return few.every((t) => {
    for (let i = 0; i < many.length; i++) {
      if (used[i]) continue;
      const m = many[i];
      if (m === t || (t.length === 1 && m[0] === t) || (m.length === 1 && t[0] === m)) {
        used[i] = true; return true;
      }
    }
    return false;
  });
}
function nameSimilarity(aName, bName) {
  const a = tokens(aName).join(" "), b = tokens(bName).join(" ");
  if (a === b) return { score: 1, note: "identical name" };
  const ta = tokens(aName), tb = tokens(bName);
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

// ---- pairwise signals + scoring --------------------------------------------
function birthYearRelation(a, b) {
  if (a.birthYear === undefined && b.birthYear === undefined) return "both-missing";
  if (a.birthYear === undefined || b.birthYear === undefined) return "one-missing";
  return a.birthYear === b.birthYear ? "match" : "conflict";
}
function overlap(a = [], b = []) {
  const B = new Set(b.map((x) => String(x).toLowerCase()));
  return a.filter((x) => B.has(String(x).toLowerCase()));
}
function companiesOf(p) {
  return (p.roles ?? []).map((r) => r.subjectName).filter(Boolean);
}
const MEDIUM = 0.62;
function scorePair(a, b) {
  const ids = overlap(a.identifiers ?? [], b.identifiers ?? []);
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
  const highOnMerit = name.score >= 0.9 && byr === "match" && (nats.length > 0 || cos.length > 0);
  const confidence = identifierDominant || highOnMerit ? "high" : "medium";
  const byrNote =
    byr === "match" ? `both born ${a.birthYear}`
    : byr === "conflict" ? `birth years differ (${a.birthYear} vs ${b.birthYear})`
    : byr === "one-missing" ? "birth year missing on one record"
    : "no birth year on either record";
  const parts = [];
  if (ids.length) parts.push(`shared identifier ${ids.join(", ")}`);
  parts.push(name.note);
  parts.push(byrNote);
  if (nats.length) parts.push(`both ${nats.join("/")}`);
  if (cos.length) parts.push(`shared role in ${cos.join(", ")}`);
  return { aKey: a.key, bKey: b.key, score: Number(score.toFixed(3)), confidence, evidence: parts.join("; ") };
}
function clusterConnectedPeople(people) {
  const parent = people.map((_, i) => i);
  const find = (i) => { while (parent[i] !== i) { parent[i] = parent[parent[i]]; i = parent[i]; } return i; };
  const union = (i, j) => { parent[find(i)] = find(j); };
  const pairs = [];
  for (let i = 0; i < people.length; i++)
    for (let j = i + 1; j < people.length; j++) {
      const pair = scorePair(people[i], people[j]);
      if (!pair) continue;
      pairs.push(pair); union(i, j);
    }
  const groups = new Map();
  for (let i = 0; i < people.length; i++) {
    const root = find(i);
    (groups.get(root) ?? groups.set(root, []).get(root)).push(people[i].key);
  }
  const clusters = [], clustered = new Set();
  for (const keys of groups.values()) {
    if (keys.length < 2) continue;
    keys.forEach((k) => clustered.add(k));
    const memberSet = new Set(keys);
    const clusterPairs = pairs.filter((p) => memberSet.has(p.aKey) && memberSet.has(p.bKey));
    const confidence = clusterPairs.some((p) => p.confidence === "high") ? "high" : "medium";
    clusters.push({ keys, size: keys.length, confidence, pairs: clusterPairs });
  }
  const singletons = people.map((p) => p.key).filter((k) => !clustered.has(k));
  return { clusters, singletons };
}

// ---- person extraction (mirror of extractConnectedPeople merge) ------------
const str = (v) => (typeof v === "string" && v ? v : undefined);
const rec = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v : {});
const arr = (v) => (Array.isArray(v) ? v : []);
function personName(rd) {
  const names = arr(rd.names).map(rec);
  const chosen = names.find((n) => str(n.type) === "individual") ?? names.find((n) => str(n.fullName));
  if (!chosen) return undefined;
  const full = str(chosen.fullName);
  if (full) return full;
  const joined = `${str(chosen.givenName) ?? ""} ${str(chosen.familyName) ?? ""}`.trim();
  return joined || undefined;
}
function birthYearOf(bd) { const m = bd?.match(/^(\d{4})/); return m ? Number(m[1]) : undefined; }
function extractPersonIdentifiers(rd) {
  const out = [];
  for (const id of arr(rd.identifiers).map(rec)) {
    const scheme = str(id.scheme) ?? str(id.schemeName);
    const value = str(id.id);
    if (scheme && value) out.push(`${scheme}:${value}`.toLowerCase());
    else if (value) out.push(String(value).toLowerCase());
  }
  return out;
}
function extractPeople(statements) {
  const people = new Map();
  for (const s of statements) {
    const t = str(s.recordType) ?? str(s.statementType);
    if (t !== "person" && t !== "personStatement") continue;
    const rd = rec(s.recordDetails);
    const pt = str(rd.personType);
    if (pt && pt !== "knownPerson") continue;
    const name = personName(rd);
    if (!name) continue;
    const birthDate = str(rd.birthDate);
    const birthYear = birthYearOf(birthDate);
    const key = `${nameMatchKey(name)}|${birthYear ?? ""}`;
    let p = people.get(key);
    if (!p) { p = { key, name, birthDate, birthYear, nationalities: [], identifiers: [], roles: [], sources: [], statementIds: [] }; people.set(key, p); }
    for (const n of arr(rd.nationalities).map(rec)) { const l = str(n.name) ?? str(n.code); if (l && !p.nationalities.includes(l)) p.nationalities.push(l); }
    for (const id of extractPersonIdentifiers(rd)) if (!p.identifiers.includes(id)) p.identifiers.push(id);
    const src = str(rec(s.source).description);
    if (src && !p.sources.includes(src)) p.sources.push(src);
    const sid = str(s.statementId);
    if (sid && !p.statementIds.includes(sid)) p.statementIds.push(sid);
  }
  return Array.from(people.values());
}

function evalBundle(path) {
  const raw = JSON.parse(readFileSync(path, "utf8"));
  const statements = Array.isArray(raw) ? raw : arr(raw.bods);
  const people = extractPeople(statements);
  const { clusters, singletons } = clusterConnectedPeople(people);
  console.log(`\n########## ${path} ##########`);
  console.log(`people=${people.length}  clusters=${clusters.length}  singletons=${singletons.length}`);
  const byKey = Object.fromEntries(people.map((p) => [p.key, p]));
  clusters.forEach((c, i) => {
    console.log(`\n  Cluster ${i + 1} [${c.confidence.toUpperCase()}] — ${c.size} records`);
    for (const k of c.keys) {
      const p = byKey[k];
      console.log(`    · ${p.name}  (born ${p.birthYear ?? "—"}; ${p.sources.join("/") || "—"}${p.identifiers.length ? "; ids " + p.identifiers.join(",") : ""})`);
    }
    for (const pair of c.pairs) console.log(`      → ${pair.evidence}  [score ${pair.score}]`);
  });
  const high = clusters.filter((c) => c.confidence === "high").length;
  const med = clusters.filter((c) => c.confidence === "medium").length;
  console.log(`\n  METRICS: highClusters=${high} mediumClusters=${med} clusteredPeople=${people.length - singletons.length}`);
  console.log(`  ↳ eyeball every HIGH cluster: a High grouping of two DIFFERENT people is the key failure to catch.`);
  return { path, people: people.length, clusters: clusters.length, high, med };
}

function main() {
  const paths = process.argv.slice(2);
  if (!paths.length) { console.error("usage: node cluster-eval.mjs <bundle.json> [more.json ...]"); process.exit(1); }
  const rows = paths.map(evalBundle);
  console.log("\n===== ROLLUP =====");
  for (const r of rows) console.log(`${r.path}: people=${r.people} clusters=${r.clusters} (high=${r.high}, medium=${r.med})`);
}
main();
