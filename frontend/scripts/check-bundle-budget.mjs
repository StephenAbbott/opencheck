#!/usr/bin/env node
/**
 * Fails the build when what a first visit downloads before the homepage can
 * draw grows past its budget (`bundle-budget.json`).
 *
 *   npm run build && npm run check:bundle
 *   npm run check:bundle -- --update                    # lock in a gain
 *   npm run check:bundle -- --update --allow-increase   # accept a growth, on purpose
 *
 * The measure is brotli bytes of the module entry, its modulepreloads and the
 * stylesheet, read from `dist/index.html`. A failure names each file and its
 * size, so the commit that grew it can see what to move behind a lazy import.
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { brotliSize, budgetFor, criticalAssets, judge, kb, updateAllowed } from "./bundleBudget.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dist = join(root, "dist");
const budgetPath = join(root, "bundle-budget.json");
const args = new Set(process.argv.slice(2));

if (!existsSync(join(dist, "index.html"))) {
  console.error("check:bundle — no dist/index.html; run `npm run build` first.");
  process.exit(2);
}

const assets = criticalAssets(readFileSync(join(dist, "index.html"), "utf8"));
const sizes = assets.map((path) => ({ path, bytes: brotliSize(readFileSync(join(dist, path))) }));
const measured = sizes.reduce((sum, s) => sum + s.bytes, 0);

const recorded = existsSync(budgetPath) ? JSON.parse(readFileSync(budgetPath, "utf8")) : {};
const current = recorded.criticalBrotliBytes;

const table = sizes
  .sort((a, b) => b.bytes - a.bytes)
  .map((s) => `  ${kb(s.bytes).padStart(9)}  ${s.path}`)
  .join("\n");

if (args.has("--update")) {
  const next = budgetFor(measured);
  if (!updateAllowed(current, next, args.has("--allow-increase"))) {
    console.error(
      `check:bundle — refusing to raise the budget from ${kb(current)} to ${kb(next)} ` +
        "without --allow-increase.\n" +
        table,
    );
    process.exit(1);
  }
  writeFileSync(
    budgetPath,
    JSON.stringify(
      {
        criticalBrotliBytes: next,
        measuredBrotliBytes: measured,
        note: "Brotli bytes the homepage downloads before first paint: the module entry, its modulepreloads and the stylesheet. Written by `npm run check:bundle -- --update`.",
      },
      null,
      2,
    ) + "\n",
  );
  console.log(`check:bundle — budget ${kb(next)} (measured ${kb(measured)}).`);
  process.exit(0);
}

if (current === undefined) {
  console.error("check:bundle — no budget recorded; run with --update.");
  process.exit(1);
}

const verdict = judge(measured, current);
if (verdict.status === "over") {
  console.error(
    `check:bundle — first-load assets are ${kb(measured)}, ${kb(verdict.by)} over the ` +
      `${kb(current)} budget. Move what the homepage does not show behind a lazy import ` +
      "(lazyWithRetry / lazyNamedWithRetry), or, if the growth is deliberate, run " +
      "`npm run check:bundle -- --update --allow-increase`.\n" +
      table,
  );
  process.exit(1);
}
if (verdict.status === "stale") {
  console.error(
    `check:bundle — first-load assets are ${kb(measured)}, well under the ${kb(current)} ` +
      "budget. Lock the gain in with `npm run check:bundle -- --update`.\n" +
      table,
  );
  process.exit(1);
}
console.log(`check:bundle — ok: ${kb(measured)} of ${kb(current)}.\n${table}`);
