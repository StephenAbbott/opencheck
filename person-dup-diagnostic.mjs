#!/usr/bin/env node
/**
 * Phase 0 diagnostic for the "Cluster UI for BackgroundCheck" ticket.
 *
 * Given a /lookup response (or a raw BODS statement array), prints the
 * connected people exactly as BackgroundCheck derives them, then reports:
 *   (A) which pairs the CURRENT `possiblySamePeople` logic flags, and
 *   (B) which likely-duplicate pairs it SILENTLY MISSES, with the reason
 *       classified as a name-variant or a birth-year conflict.
 *
 * The name normalisation and flag logic below mirror
 * frontend/src/lib/backgroundCheck.ts. The merge key uses the NEW
 * order-insensitive `nameMatchKey` (sorted tokens) — matching the folded-in
 * fix — so register-format duplicates ("NELSON PELTZ" vs "PELTZ, Nelson")
 * merge here as they will in the patched UI. Until that patch ships, the live
 * UI still shows the pre-fix (order-sensitive) split. The "missed candidate"
 * detector in section (B) is a conservative PREVIEW heuristic for the cluster
 * work — not the final scorer, it just shows what the current flag can't see.
 *
 * Usage:
 *   node person-dup-diagnostic.mjs <bundle.json>
 *   curl 'http://127.0.0.1:8000/lookup?lei=549300MKFYEKVRWML317' > unilever.json
 *   node person-dup-diagnostic.mjs unilever.json
 */

import { readFileSync } from "node:fs";

// ---- verbatim from backgroundCheck.ts -------------------------------------

function str(v) {
  return typeof v === "string" && v ? v : undefined;
}
function rec(v) {
  return v && typeof v === "object" && !Array.isArray(v) ? v : {};
}
function arr(v) {
  return Array.isArray(v) ? v : [];
}
function personName(rd) {
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
function normaliseName(name) {
  return name
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}
// Order-insensitive merge key — mirrors the nameMatchKey change in
// backgroundCheck.ts, so "NELSON PELTZ" and "PELTZ, Nelson" merge.
function nameMatchKey(name) {
  return normaliseName(name).split(" ").filter(Boolean).sort().join(" ");
}
function birthYearOf(birthDate) {
  const m = birthDate?.match(/^(\d{4})/);
  return m ? Number(m[1]) : undefined;
}

/** Person extraction — the merge subset of extractConnectedPeople. */
function extractConnectedPeople(statements) {
  const people = new Map();
  const addPerson = (s) => {
    const id = str(s.statementId);
    if (!id) return;
    const recordType = str(s.recordType) ?? str(s.statementType);
    if (recordType !== "person" && recordType !== "personStatement") return;
    const rd = rec(s.recordDetails);
    const personType = str(rd.personType);
    if (personType && personType !== "knownPerson") return; // anon/unknown
    const name = personName(rd);
    if (!name) return;
    const birthDate = str(rd.birthDate);
    const birthYear = birthYearOf(birthDate);
    const key = `${nameMatchKey(name)}|${birthYear ?? ""}`;
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
      };
      people.set(key, person);
    }
    if (!person.birthDate && birthDate) {
      person.birthDate = birthDate;
      person.birthYear = birthYear;
    }
    for (const n of arr(rd.nationalities).map(rec)) {
      const label = str(n.name) ?? str(n.code);
      if (label && !person.nationalities.includes(label)) person.nationalities.push(label);
    }
    if (!person.statementIds.includes(id)) person.statementIds.push(id);
    const source = str(rec(s.source).description);
    if (source && !person.sources.includes(source)) person.sources.push(source);
  };
  for (const s of statements) addPerson(s);
  return Array.from(people.values());
}

/** verbatim from backgroundCheck.ts */
function possiblySamePeople(people) {
  const pairs = [];
  for (let i = 0; i < people.length; i++) {
    for (let j = i + 1; j < people.length; j++) {
      const a = people[i];
      const b = people[j];
      if (normaliseName(a.name) !== normaliseName(b.name)) continue;
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

// ---- PREVIEW heuristic (not final): missed likely-duplicate pairs ---------

function tokens(name) {
  return normaliseName(name).split(" ").filter(Boolean);
}
function isSubset(a, b) {
  const B = new Set(b);
  return a.every((t) => B.has(t));
}
function jaccard(a, b) {
  const A = new Set(a), B = new Set(b);
  const inter = [...A].filter((t) => B.has(t)).length;
  const uni = new Set([...A, ...B]).size;
  return uni ? inter / uni : 0;
}
/** Would a reasonable reviewer suspect these two cards are one person? */
function looksLikeSamePerson(a, b) {
  const ta = tokens(a.name), tb = tokens(b.name);
  if (!ta.length || !tb.length) return false;
  const sameSet = [...ta].sort().join(" ") === [...tb].sort().join(" "); // ordering swap
  const subset = isSubset(ta, tb) || isSubset(tb, ta);                    // middle name/initial added
  const sameFamily = ta[ta.length - 1] === tb[tb.length - 1];
  const sameGivenInit = ta[0][0] === tb[0][0];
  const overlap = jaccard(ta, tb);
  return sameSet || subset || (sameFamily && sameGivenInit) || overlap >= 0.5;
}

function main() {
  const path = process.argv[2];
  if (!path) {
    console.error("usage: node person-dup-diagnostic.mjs <bundle.json>");
    process.exit(1);
  }
  const raw = JSON.parse(readFileSync(path, "utf8"));
  const statements = Array.isArray(raw) ? raw : arr(raw.bods);
  if (!statements.length) {
    console.error("No statements found (expected an array or an object with a .bods array).");
    process.exit(1);
  }

  const people = extractConnectedPeople(statements);
  const flagged = possiblySamePeople(people);
  const flaggedKeys = new Set(flagged.map((p) => [p.a, p.b].sort().join("::")));

  console.log(`\n=== Connected people (${people.length}) ===`);
  console.log("name | normalised | birthYear | #stmts | sources");
  for (const p of people) {
    console.log(
      `${p.name} | ${normaliseName(p.name)} | ${p.birthYear ?? "—"} | ${p.statementIds.length} | ${p.sources.join(", ") || "—"}`
    );
  }

  console.log(`\n=== (A) Currently FLAGGED by possiblySamePeople (${flagged.length}) ===`);
  if (!flagged.length) console.log("(none)");
  for (const f of flagged) console.log(`FLAGGED: ${f.name} — ${f.reason}`);

  // (B) pairs that look like the same person but are NOT merged and NOT flagged
  const missed = [];
  for (let i = 0; i < people.length; i++) {
    for (let j = i + 1; j < people.length; j++) {
      const a = people[i], b = people[j];
      const pairId = [a.key, b.key].sort().join("::");
      if (flaggedKeys.has(pairId)) continue;          // already flagged
      if (!looksLikeSamePerson(a, b)) continue;
      const nA = normaliseName(a.name), nB = normaliseName(b.name);
      let reason;
      if (nA === nB && a.birthYear !== undefined && b.birthYear !== undefined && a.birthYear !== b.birthYear) {
        reason = `BIRTH-YEAR CONFLICT — same name, years differ (${a.birthYear} vs ${b.birthYear})`;
      } else if (nA !== nB) {
        const yrs =
          a.birthYear !== undefined && b.birthYear !== undefined
            ? a.birthYear === b.birthYear ? `, both born ${a.birthYear}` : `, years differ ${a.birthYear} vs ${b.birthYear}`
            : "";
        reason = `NAME VARIANT — '${a.name}' vs '${b.name}'${yrs}`;
      } else {
        reason = "same normalised name (would normally have merged — inspect)";
      }
      missed.push({ a, b, reason });
    }
  }

  console.log(`\n=== (B) MISSED likely-duplicate pairs — invisible to the current flag (${missed.length}) ===`);
  if (!missed.length) console.log("(none)");
  for (const m of missed) {
    console.log(`MISSED: ${m.a.name} [${m.a.sources.join("/")}]  <->  ${m.b.name} [${m.b.sources.join("/")}]`);
    console.log(`        ${m.reason}`);
  }

  console.log(
    `\n=== Summary ===\npeople=${people.length}  flagged=${flagged.length}  missed=${missed.length}\n`
  );
}

main();
