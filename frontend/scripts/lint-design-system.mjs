#!/usr/bin/env node
/**
 * Design-system lint (Phase 124, delivery phase H).
 *
 * Two rules, both from the Phase 122 audit:
 *
 *   1. **No raw hex outside the token files.** 61 hardcoded colours were found
 *      across the app, including `#3d30d4` written out seven times although it
 *      *is* `oo-blue`, and four separate "soft selected blue" palettes that
 *      existed because nobody could see the other three.
 *   2. **No `text-[NNpx]` outside the named scale.** 15 arbitrary sizes,
 *      `text-[11px]` 122 times, `text-[10px]` 77, `text-[9px]` 5 — a body text
 *      of effectively 11–13px arrived at by accident rather than decision.
 *
 * ## Why this is a ratchet and not a ban
 *
 * A lint that fails on every existing violation is a lint that gets disabled.
 * There were 477 arbitrary sizes across 22 files when this was written;
 * migrating them in one commit would be a diff nobody can review and a
 * regression risk out of all proportion to the problem. So the baseline records
 * what each file currently carries, and the rule is **a file may never carry
 * more than it did**. New code cannot add a violation, a new file cannot
 * introduce one at all, and the migration is a monotonic decrease that can land
 * component by component — which is how Phase 122 already did BodsTree,
 * SourceLegend and the chips it touched.
 *
 * `--update` rewrites the baseline, and refuses to raise a count unless
 * `--allow-increase` is passed as well: the ratchet must not be loosened by the
 * same command that tightens it.
 *
 * ## What is deliberately allowed
 *
 * `ALLOWED_HEX_FILES` are the places a literal colour is the correct thing to
 * write: the Tailwind config and the CSS variables (the definitions
 * themselves), `lib/graphStyle.ts` (Cytoscape takes colours as strings, not
 * class names — this is the graph's token file, and Phase 124 moved the values
 * there precisely so they had one), and `lib/bovsIcons.ts` (base64 data URIs
 * that happen to match a hex pattern).
 */

import { readFileSync, writeFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(fileURLToPath(new URL(".", import.meta.url)), "..");
const SRC = join(ROOT, "src");
const BASELINE = join(ROOT, "design-system-baseline.json");

/** Files where a literal colour is the definition, not a drift. */
const ALLOWED_HEX_FILES = new Set([
  "src/lib/graphStyle.ts",
  "src/lib/bovsIcons.ts",
]);

/** `#fff`, `#ffffff`, `#ffffffff` — but not a fragment of a longer token. */
const HEX = /#[0-9a-fA-F]{3,8}\b/g;
/** `text-[11px]`, `text-[10.5px]`. */
const ARBITRARY_TEXT = /text-\[[0-9.]+px\]/g;

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) out.push(...walk(full));
    else if (/\.(tsx?|jsx?)$/.test(name)) out.push(full);
  }
  return out;
}

export function scan() {
  const counts = {};
  for (const file of walk(SRC)) {
    const rel = relative(ROOT, file);
    const src = readFileSync(file, "utf8");
    const hex = ALLOWED_HEX_FILES.has(rel) ? 0 : (src.match(HEX) ?? []).length;
    const text = (src.match(ARBITRARY_TEXT) ?? []).length;
    if (hex || text) counts[rel] = { hex, text };
  }
  return counts;
}

function loadBaseline() {
  try {
    return JSON.parse(readFileSync(BASELINE, "utf8"));
  } catch {
    return {};
  }
}

function totals(counts) {
  return Object.values(counts).reduce(
    (a, c) => ({ hex: a.hex + c.hex, text: a.text + c.text }),
    { hex: 0, text: 0 }
  );
}

const args = process.argv.slice(2);
const counts = scan();
const baseline = loadBaseline();

if (args.includes("--update")) {
  const allowIncrease = args.includes("--allow-increase");
  if (!allowIncrease) {
    const raised = Object.entries(counts).filter(([f, c]) => {
      const b = baseline[f];
      return b && (c.hex > b.hex || c.text > b.text);
    });
    if (raised.length) {
      console.error(
        "Refusing to raise the baseline. The ratchet only turns one way.\n" +
          raised.map(([f, c]) => `  ${f}  hex ${baseline[f].hex}→${c.hex}  text ${baseline[f].text}→${c.text}`).join("\n") +
          "\n\nFix the file, or pass --allow-increase if you genuinely mean it."
      );
      process.exit(1);
    }
  }
  writeFileSync(BASELINE, JSON.stringify(counts, null, 2) + "\n");
  const t = totals(counts);
  console.log(`design-system: baseline written — ${t.hex} hex, ${t.text} arbitrary text sizes`);
  process.exit(0);
}

const failures = [];
for (const [file, c] of Object.entries(counts)) {
  const b = baseline[file];
  if (!b) {
    failures.push(
      `${file}: new file with ${c.hex} raw hex and ${c.text} text-[NNpx] — new code uses tokens and the named scale`
    );
    continue;
  }
  if (c.hex > b.hex) failures.push(`${file}: raw hex ${b.hex} → ${c.hex}`);
  if (c.text > b.text) failures.push(`${file}: text-[NNpx] ${b.text} → ${c.text}`);
}

// A file that improved should update the baseline, so the gain is locked in.
const improved = Object.entries(counts).filter(([f, c]) => {
  const b = baseline[f];
  return b && (c.hex < b.hex || c.text < b.text);
});
const removed = Object.keys(baseline).filter((f) => !(f in counts));

if (failures.length) {
  console.error("Design-system lint failed:\n" + failures.map((f) => `  ${f}`).join("\n"));
  console.error(
    "\nUse the tokens in tailwind.config.js (oo-*) and the named type scale " +
      "(text-oo-meta … text-oo-display) rather than literals.\n" +
      "If you have reduced a count elsewhere, run: npm run lint:design -- --update"
  );
  process.exit(1);
}

if (improved.length || removed.length) {
  console.error(
    "Design-system lint: counts went DOWN and the baseline is stale.\n" +
      [...improved.map(([f, c]) => `  ${f}  hex ${baseline[f].hex}→${c.hex}  text ${baseline[f].text}→${c.text}`),
       ...removed.map((f) => `  ${f}  now clean`)].join("\n") +
      "\n\nRun: npm run lint:design -- --update  (this locks the improvement in)"
  );
  process.exit(1);
}

const t = totals(counts);
console.log(
  `design-system: ok — ${t.hex} raw hex, ${t.text} arbitrary text sizes remaining ` +
    `across ${Object.keys(counts).length} files (ratcheting down)`
);
