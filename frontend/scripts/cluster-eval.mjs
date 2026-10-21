#!/usr/bin/env node
/**
 * Phase 3 evaluation harness for the Cluster UI ticket.
 *
 * Runs the real extraction and clustering over one or more /lookup bundles and
 * prints, per bundle: the clusters formed (confidence + evidence) and summary
 * metrics. High-confidence clusters are called out for scrutiny (a false High
 * pairing two different people is the failure we most care about).
 *
 * It imports frontend/src/lib/backgroundCheck.ts and clusterPeople.ts
 * directly, so it cannot drift from the UI. (Until the person-merge fix of
 * 20 Oct 2026 it carried a verbatim copy, which had already fallen behind:
 * it read no identifiers from annotations.) Run it with vite-node, which
 * vitest ships:
 *
 *   curl 'http://127.0.0.1:8000/lookup?lei=<LEI>&refresh=true' > company.json
 *   cd frontend && npx vite-node scripts/cluster-eval.mjs ../company.json [more.json ...]
 *
 * A bundle is a captured /lookup response ({bods: [...]}), an export
 * ({statements: [...]}) or a bare statement array — never committed.
 */

import { readFileSync } from "node:fs";
import { extractConnectedPeople } from "../src/lib/backgroundCheck.ts";
import { clusterConnectedPeople } from "../src/lib/clusterPeople.ts";

function evalBundle(path) {
  const raw = JSON.parse(readFileSync(path, "utf8"));
  const statements = Array.isArray(raw) ? raw : (raw.bods ?? raw.statements ?? []);
  const people = extractConnectedPeople(statements);
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
