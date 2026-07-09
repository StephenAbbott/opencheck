/**
 * verify-reconciliation — real-data verification matrix for the FullCheck
 * reconciliation transform (issue #25, item 4).
 *
 * Bundles the REAL src/lib/reconcile.ts with esbuild, fetches live BODS
 * exports from the production API for a matrix of entity shapes, and checks
 * structural invariants that unit fixtures can't capture (data-shape
 * failures were the recurring surprise in past graph work):
 *
 *   I1  no merged node carries two different LEIs (cross-entity merge)
 *   I2  canonical ids are stable under input reordering
 *   I3  reconciliation is idempotent (re-reconciling merges nothing more)
 *   I4  every remap target exists exactly once in the output
 *   I5  POSSIBLY_SAME_AS pairs share no identifier (else they'd have merged)
 *
 * Matrix: a sanctioned entity (risk + reconciliation interacting), a deep
 * multi-jurisdiction group (canonical-id stability at scale), a small
 * multi-source entity, and whatever single-source bundles the API returns.
 *
 * Usage (from frontend/):  node scripts/verify-reconciliation.mjs
 * Requires network access to https://api.opencheck.world (Render free tier —
 * the first request after idle can be slow; the script retries once).
 */

import { buildSync } from "esbuild";
import { createRequire } from "node:module";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const API = process.env.OPENCHECK_API ?? "https://api.opencheck.world";

// The matrix: name → LEI. Swap entries freely; the invariants are generic.
const MATRIX = {
  "sanctioned (Rosneft)": "253400JT3MQWNDKMJE44",
  "deep group (Shell)": "21380068P1DRHMJ8KU70",
  "small multi-source (Canada Basketball)": "254900I2XU5S46WX4821",
};

// ---------------------------------------------------------------------------

const out = join(mkdtempSync(join(tmpdir(), "recon-verify-")), "reconcile.cjs");
buildSync({
  entryPoints: [join(here, "../src/lib/reconcile.ts")],
  bundle: true,
  format: "cjs",
  platform: "node",
  outfile: out,
});
const { reconcileBods, possiblySameAs } = createRequire(import.meta.url)(out);

const LEI_RE = /^[0-9A-Z]{18}[0-9]{2}$/;
const rd = (s) => s.recordDetails || {};
const leisOf = (s) =>
  (rd(s).identifiers || [])
    .map((i) => String(i.id || "").toUpperCase())
    .filter((v) => LEI_RE.test(v));
const identSet = (s) =>
  new Set((rd(s).identifiers || []).map((i) => `${i.scheme || ""}|${String(i.id || "").toUpperCase()}`));

function shuffle(arr, seed) {
  const a = [...arr];
  let s = seed;
  for (let i = a.length - 1; i > 0; i--) {
    s = (s * 1103515245 + 12345) % 2147483648;
    const j = s % (i + 1);
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function fetchBods(lei) {
  const url = `${API}/export?lei=${lei}&format=json&deepen_top=3`;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const r = await fetch(url, { signal: AbortSignal.timeout(90_000) });
      if (r.ok) return await r.json();
    } catch {
      /* retry once — Render cold start */
    }
  }
  return null;
}

function check(name, bods) {
  const ents = bods.filter((s) => s.recordType === "entity");
  const srcs = [...new Set(bods.map((s) => (s.source || {}).description).filter(Boolean))];
  const { statements, remap } = reconcileBods(bods);
  const outEnts = statements.filter((s) => s.recordType === "entity");
  const fails = [];

  for (const e of outEnts) {
    const leis = [...new Set(leisOf(e))];
    if (leis.length > 1) fails.push(`I1 cross-LEI merge on ${e.statementId}: ${leis.join(",")}`);
  }

  const canon = new Set(outEnts.map((e) => e.statementId));
  for (const seed of [7, 42, 1337]) {
    const r2 = reconcileBods(shuffle(bods, seed));
    const c2 = new Set(r2.statements.filter((s) => s.recordType === "entity").map((e) => e.statementId));
    if (c2.size !== canon.size || [...canon].some((c) => !c2.has(c)))
      fails.push(`I2 canonical ids unstable under shuffle(seed=${seed})`);
  }

  const againCount = reconcileBods(statements).statements.filter((s) => s.recordType === "entity").length;
  if (againCount !== outEnts.length) fails.push(`I3 not idempotent: ${outEnts.length} -> ${againCount}`);

  for (const cid of new Set(Object.values(remap)))
    if (!canon.has(cid)) fails.push(`I4 remap target missing from output: ${cid}`);

  const byId = new Map(outEnts.map((e) => [e.statementId, e]));
  const pairs = possiblySameAs(outEnts);
  for (const p of pairs) {
    const a = identSet(byId.get(p.a) || {});
    const b = identSet(byId.get(p.b) || {});
    for (const k of a) if (b.has(k)) fails.push(`I5 possibly-same pair shares identifier ${k}`);
  }

  const multi = outEnts.filter((e) => (e._sources || []).length > 1).length;
  console.log(
    `${name}: ${bods.length} stmts / ${ents.length} raw entities -> ${outEnts.length} reconciled ` +
      `(${multi} multi-source, ${srcs.length} sources, ${pairs.length} possibly-same) · ` +
      (fails.length ? "FAIL" : "PASS")
  );
  for (const f of fails) console.log(`  ✗ ${f}`);
  return fails.length === 0;
}

let ok = true;
for (const [name, lei] of Object.entries(MATRIX)) {
  const bods = await fetchBods(lei);
  if (!bods) {
    console.log(`${name}: SKIP (export unavailable)`);
    continue;
  }
  ok = check(name, bods) && ok;
  // Degenerate single-source case: re-check on just one source's statements.
  const bySrc = new Map();
  for (const s of bods) {
    const src = (s.source || {}).description || "";
    bySrc.set(src, [...(bySrc.get(src) ?? []), s]);
  }
  const [firstSrc, firstStmts] = [...bySrc.entries()].sort((a, b) => b[1].length - a[1].length)[0] ?? [];
  if (firstSrc && bySrc.size > 1) ok = check(`  └ single-source slice (${firstSrc})`, firstStmts) && ok;
}
process.exit(ok ? 0 : 1);
