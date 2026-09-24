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
 *   4. **No raw Tailwind palette colour** (Phase 241). `bg-emerald-50`,
 *      `text-rose-700`: 505 of them against ~138 semantic-token uses when the
 *      Opus 5.5 check counted, which is how the ESG tab came to wear the
 *      "ok / corroborated" green and the licence panel the risk red. Every
 *      Tailwind hue is counted, not only the ones in use, so moving a colour
 *      from emerald to teal cannot dodge the count; the `oo-*` tokens name
 *      what a colour means.
 *   5. **No raw `<button>` outside `components/ui/`** (Phase 241). 104 of
 *      them against 15 `<Button>`s. Counted as JSX elements by the
 *      TypeScript parser, so a `<button>` in a comment or a string is not one.
 *   3. **No banned synonym.** `lib/vocab.ts` names one word per concept;
 *      `BANNED_SYNONYMS` lists what must not come back. Until Phase 125 that
 *      list was documentation with nothing enforcing it, which is how four
 *      verbs for two actions survived a vocabulary pass in the first place.
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
import ts from "typescript";

const ROOT = join(fileURLToPath(new URL(".", import.meta.url)), "..");
const SRC = join(ROOT, "src");
const BASELINE = join(ROOT, "design-system-baseline.json");

/** Files where a literal colour is the definition, not a drift. */
const ALLOWED_HEX_FILES = new Set([
  "src/lib/graphStyle.ts",
  "src/lib/bovsIcons.ts",
  // The badge tier's token file (Phase 175), on the same grounds as
  // graphStyle.ts: `FeatureMark` paints the ring with an inline style, so the
  // value has to be a string and there is no class name to write instead. It
  // also has to hold the exact hexes that `outputs/mode-badges/*.svg` carry,
  // which is a correspondence a Tailwind class cannot express.
  "src/lib/features.ts",
  // The mode accents (Phase 185), on the same grounds again: the tab strip
  // paints the active bar with an inline style. They lived as literals in
  // App.tsx until the fifth mode would have pushed that file over its
  // ratchet; `MODE_ACCENT` is now the one place the five values are written,
  // and `checkMode.test.ts` reads each back out of tailwind.config.js.
  "src/lib/checkMode.ts",
]);

/** `#fff`, `#ffffff`, `#ffffffff` — but not a fragment of a longer token. */
const HEX = /#[0-9a-fA-F]{3,8}\b/g;
/** `text-[11px]`, `text-[10.5px]`. */
const ARBITRARY_TEXT = /text-\[[0-9.]+px\]/g;

/**
 * A Tailwind palette utility — `bg-rose-50`, `hover:text-amber-800`,
 * `border-emerald-200/60`. Every default hue, so a migration cannot move a
 * colour to an unlisted one and read as progress.
 */
const HUES =
  "slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose";
const PALETTE = new RegExp(
  `(?<![\\w-])(?:bg|text|border|ring|divide|outline|fill|stroke|from|via|to|decoration|placeholder|accent)-(?:${HUES})-\\d{2,3}\\b`,
  "g"
);

/** Where a raw `<button>` is the definition, not a drift. */
const BUTTON_EXEMPT_DIR = "src/components/ui/";

/** The metrics each file is ratcheted on, in report order. */
const METRICS = ["hex", "text", "vocab", "palette", "button"];
const METRIC_LABEL = {
  hex: "raw hex",
  text: "text-[NNpx]",
  vocab: "banned terms",
  palette: "raw palette classes",
  button: "raw <button>",
};

/** Raw `<button>` JSX elements, by the TypeScript parser. */
function countButtons(src, fileName) {
  const sf = ts.createSourceFile(fileName, src, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let n = 0;
  const visit = (node) => {
    if (
      (ts.isJsxOpeningElement(node) || ts.isJsxSelfClosingElement(node)) &&
      node.tagName.getText(sf) === "button"
    ) {
      n += 1;
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return n;
}

/**
 * Terms that must not reappear in user-facing strings, read from
 * `lib/vocab.ts` so the lint and the module cannot disagree — a second copy of
 * the list is the same failure the list exists to prevent.
 *
 * Matched inside JSX text and string literals only, and word-bounded, so
 * `hit.source_id`, `bucket.hits.length` and `hitCount` are untouched: the field
 * names are not the problem, the prose is. `vocab.ts` itself is exempt because
 * it has to name the words it bans.
 */
function bannedTerms() {
  const src = readFileSync(join(SRC, "lib/vocab.ts"), "utf8");
  const block = src.match(/BANNED_SYNONYMS[^=]*=\s*\{([\s\S]*?)\n\};/);
  if (!block) throw new Error("BANNED_SYNONYMS not found in lib/vocab.ts");
  return [...block[1].matchAll(/^\s*"([^"]+)":/gm)].map((m) => m[1]);
}

const VOCAB_EXEMPT = new Set(["src/lib/vocab.ts", "src/lib/vocab.test.ts"]);

/**
 * Count banned terms in **prose only**, using TypeScript's own parser.
 *
 * Regex cannot do this. A first attempt matched word boundaries over whole
 * files and reported 26 violations in App.tsx, all false — `bucket.hits.length`,
 * a `"hit_id"` key, a `Liveness = "stub"` union member. Restricting it to
 * quoted strings made it worse (90), because an apostrophe in JSX text
 * ("doesn't") pairs with the next one and swallows the code between them, and
 * because `min-h-screen` contains the word "screen" at a word boundary.
 *
 * So this walks the AST and looks only at string literals and JSX text, then
 * drops anything inside a `className`. Those are the two places a user-facing
 * word can actually be, and the className exclusion is what keeps Tailwind out.
 */
function countBanned(src, terms, fileName) {
  const sf = ts.createSourceFile(fileName, src, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const prose = [];

  const inClassName = (node) => {
    for (let p = node.parent; p; p = p.parent) {
      if (ts.isJsxAttribute(p) && p.name.getText() === "className") return true;
      if (ts.isJsxElement(p) || ts.isJsxSelfClosingElement(p)) return false;
    }
    return false;
  };

  const visit = (node) => {
    if (ts.isJsxText(node)) {
      prose.push(node.text);
    } else if (
      (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) &&
      !inClassName(node) &&
      !ts.isImportDeclaration(node.parent) &&
      !ts.isPropertyAssignment(node.parent)
    ) {
      prose.push(node.text);
    } else if (ts.isTemplateExpression(node) && !inClassName(node)) {
      prose.push(node.head.text, ...node.templateSpans.map((sp) => sp.literal.text));
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);

  const text = prose.join("\n");
  let n = 0;
  for (const term of terms) {
    // Exact phrases, case-sensitive: these are labels, and "Look up" inside
    // "Look up ROSNEFT OIL COMPANY" is the same regression.
    const re = new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "g");
    n += (text.match(re) ?? []).length;
  }
  return n;
}

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
  const terms = bannedTerms();
  for (const file of walk(SRC)) {
    const rel = relative(ROOT, file);
    const src = readFileSync(file, "utf8");
    const hex = ALLOWED_HEX_FILES.has(rel) ? 0 : (src.match(HEX) ?? []).length;
    const text = (src.match(ARBITRARY_TEXT) ?? []).length;
    const vocab = VOCAB_EXEMPT.has(rel) ? 0 : countBanned(src, terms, rel);
    const palette = (src.match(PALETTE) ?? []).length;
    const button =
      rel.startsWith(BUTTON_EXEMPT_DIR) || !/\.[jt]sx$/.test(rel) || /\.test\.[jt]sx$/.test(rel)
        ? 0
        : countButtons(src, rel);
    if (hex || text || vocab || palette || button) counts[rel] = { hex, text, vocab, palette, button };
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
  const t = Object.fromEntries(METRICS.map((m) => [m, 0]));
  for (const c of Object.values(counts)) for (const m of METRICS) t[m] += c[m] ?? 0;
  return t;
}

const describe = (t) => METRICS.map((m) => `${t[m]} ${METRIC_LABEL[m]}`).join(", ");
const delta = (b, c) => METRICS.map((m) => `${m} ${b[m] ?? 0}→${c[m] ?? 0}`).join("  ");

const args = process.argv.slice(2);
const counts = scan();
const baseline = loadBaseline();

if (args.includes("--update")) {
  const allowIncrease = args.includes("--allow-increase");
  if (!allowIncrease) {
    const raised = Object.entries(counts).filter(([f, c]) => {
      const b = baseline[f];
      return b && METRICS.some((m) => (c[m] ?? 0) > (b[m] ?? 0));
    });
    if (raised.length) {
      console.error(
        "Refusing to raise the baseline. The ratchet only turns one way.\n" +
          raised.map(([f, c]) => `  ${f}  ${delta(baseline[f], c)}`).join("\n") +
          "\n\nFix the file, or pass --allow-increase if you genuinely mean it."
      );
      process.exit(1);
    }
  }
  writeFileSync(BASELINE, JSON.stringify(counts, null, 2) + "\n");
  console.log(`design-system: baseline written — ${describe(totals(counts))}`);
  process.exit(0);
}

const failures = [];
for (const [file, c] of Object.entries(counts)) {
  const b = baseline[file];
  if (!b) {
    const parts = METRICS.filter((m) => c[m]).map((m) => `${c[m]} ${METRIC_LABEL[m]}`);
    failures.push(
      `${file}: new file with ${parts.join(", ")} — new code uses the tokens, the named scale, ` +
        "lib/vocab.ts and ui/Button"
    );
    continue;
  }
  for (const m of METRICS) {
    if ((c[m] ?? 0) > (b[m] ?? 0)) {
      failures.push(
        `${file}: ${METRIC_LABEL[m]} ${b[m] ?? 0} → ${c[m]}` + (m === "vocab" ? " (see lib/vocab.ts)" : "")
      );
    }
  }
}

// A file that improved should update the baseline, so the gain is locked in.
const improved = Object.entries(counts).filter(([f, c]) => {
  const b = baseline[f];
  return b && METRICS.some((m) => (c[m] ?? 0) < (b[m] ?? 0));
});
const removed = Object.keys(baseline).filter((f) => !(f in counts));

if (failures.length) {
  console.error("Design-system lint failed:\n" + failures.map((f) => `  ${f}`).join("\n"));
  console.error(
    "\nUse the tokens in tailwind.config.js (oo-*), the named type scale " +
      "(text-oo-meta … text-oo-display) and ui/Button rather than literals.\n" +
      "If you have reduced a count elsewhere, run: npm run lint:design -- --update"
  );
  process.exit(1);
}

if (improved.length || removed.length) {
  console.error(
    "Design-system lint: counts went DOWN and the baseline is stale.\n" +
      [...improved.map(([f, c]) => `  ${f}  ${delta(baseline[f], c)}`),
       ...removed.map((f) => `  ${f}  now clean`)].join("\n") +
      "\n\nRun: npm run lint:design -- --update  (this locks the improvement in)"
  );
  process.exit(1);
}

console.log(
  `design-system: ok — ${describe(totals(counts))} remaining across ` +
    `${Object.keys(counts).length} files (ratcheting down)`
);
