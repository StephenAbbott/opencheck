/**
 * What the /features page promises — pinned where a wrong value fails the
 * build rather than sitting on a marketing page nobody re-reads.
 *
 * The three rules in `lib/features.ts`'s docstring are the tests: two
 * sentences, one call to action, real anchors.
 *
 * The badge-colour test reads `tailwind.config.js` rather than restating the
 * hexes. Restating them proves only that two files agree with each other; what
 * needs proving is that five of the six accents are the *shipped tokens* — the
 * logo's own node colours and the graph's warm value — and that the sixth,
 * batch screening's cyan, is knowingly not one. It is also why this file
 * carries no literal colour and does not need the lint's hex allowlist.
 */

import { describe, expect, it } from "vitest";

// Vite's `?raw` rather than `node:fs`: the frontend tsconfig has no
// `@types/node` (nothing in `src/` runs outside a browser or a bundler), and
// adding it so one test can read a file would widen the type surface of the
// whole app to make a single assertion.
import TAILWIND from "../../tailwind.config.js?raw";

import { FEATURES, featureById, ledeSentence, sentenceCount } from "./features";
import { ICON_NAMES } from "../components/ui/Icon";

/** Every colour literal the Tailwind theme defines. */
const TOKEN_VALUES = new Set(
  [...TAILWIND.matchAll(/"(#[0-9a-f]{6})"/gi)].map((m) => m[1].toLowerCase()),
);

/** The value of one named token, e.g. `node.green`. */
function token(group: string, name: string): string {
  const block = TAILWIND.match(new RegExp(`\\b${group}:\\s*\\{([\\s\\S]*?)\\n\\s*\\},`));
  if (!block) throw new Error(`token group ${group} not found in tailwind.config.js`);
  const hex = block[1].match(new RegExp(`\\b${name}:\\s*"(#[0-9a-f]{6})"`, "i"));
  if (!hex) throw new Error(`token ${group}.${name} not found in tailwind.config.js`);
  return hex[1].toLowerCase();
}

describe("FEATURES", () => {
  it("lists the six features the /features ticket names", () => {
    expect(FEATURES.map((f) => f.id)).toEqual([
      "quickcheck",
      "fullcheck",
      "backgroundcheck",
      "batch-screening",
      "time-machine",
      "network-visualisations",
    ]);
  });

  it("gives every feature a unique, anchor-safe id", () => {
    const ids = FEATURES.map((f) => f.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const id of ids) expect(id).toMatch(/^[a-z][a-z0-9-]*$/);
  });

  it("keeps every description to at most two sentences", () => {
    for (const f of FEATURES) {
      expect(sentenceCount(f.description), f.id).toBeLessThanOrEqual(2);
      expect(f.description.trim().endsWith("."), f.id).toBe(true);
    }
  });

  it("uses no full stop that is not a sentence end", () => {
    // `sentenceCount` is a full-stop counter; an abbreviation, decimal or
    // ellipsis in the copy would make it lie. "BP p.l.c." lives in a CTA
    // label, not a description, which is why this only guards descriptions.
    for (const f of FEATURES) {
      expect(f.description, f.id).not.toMatch(/\.\.\./);
      expect(f.description, f.id).not.toMatch(/\b[a-z]\.[a-z]\./i);
      expect(f.description, f.id).not.toMatch(/\d\.\d/);
    }
  });

  it("gives every feature exactly one call to action, as a real path", () => {
    for (const f of FEATURES) {
      expect(f.cta.label.length, f.id).toBeGreaterThan(0);
      expect(f.cta.href, f.id).toMatch(/^\/(\?|batch|features|sources|about|api)/);
    }
  });

  it("draws every glyph from the shipped icon set", () => {
    for (const f of FEATURES) {
      expect(ICON_NAMES, f.id).toContain(f.icon);
    }
  });

  it("takes the three mode accents from the logo's own node colours", () => {
    // These are the values `outputs/mode-badges/*.svg` carry, and they are in
    // tailwind.config.js because Phase 122 formalised them there.
    expect(featureById("quickcheck")?.accent).toBe(token("node", "green"));
    expect(featureById("fullcheck")?.accent).toBe(token("node", "blue"));
    expect(featureById("backgroundcheck")?.accent).toBe(token("node", "purple"));
  });

  it("takes the two capability accents from existing tokens", () => {
    expect(featureById("time-machine")?.accent).toBe(token("graph", "same"));
    // The logo's network-EDGE colour, for the feature that draws them.
    expect(featureById("network-visualisations")?.accent).toBe(token("mark", "line"));
  });

  it("knows batch screening's accent is the one invented value", () => {
    // Deliberate, and recorded rather than hidden: nothing in the shipped
    // palette was both unclaimed and legible as a ring on the badge navy.
    // If a future token pass adds a cyan, this test is where it gets adopted.
    const batch = featureById("batch-screening")?.accent;
    expect(batch).toBeDefined();
    expect(TOKEN_VALUES.has(batch as string)).toBe(false);
  });

  it("gives every accent a distinct value, so a row of marks is readable", () => {
    const accents = FEATURES.map((f) => f.accent);
    expect(new Set(accents).size).toBe(accents.length);
    for (const a of accents) expect(a).toMatch(/^#[0-9a-f]{6}$/);
  });

  it("gives every image a path under /features/ and real alt text", () => {
    for (const f of FEATURES) {
      expect(f.image.src, f.id).toMatch(/^\/features\/[a-z-]+\.png$/);
      // Long enough to describe the picture rather than name it: the Phase 124
      // rule that a sentence hidden from sighted users must still be a
      // sentence.
      expect(f.image.alt.length, f.id).toBeGreaterThan(60);
      expect(f.image.width, f.id).toBeGreaterThan(0);
      expect(f.image.height, f.id).toBeGreaterThan(0);
    }
  });

  it("labels each feature as a mode, a workflow or a capability", () => {
    for (const f of FEATURES) {
      expect(["Check mode", "Workflow", "Capability"], f.id).toContain(f.kind);
    }
    // The three check modes are the three with mode tabs; batch and the two
    // capabilities are deliberately not called modes, because the mode
    // strip's ceiling is four (Phase 157/166).
    expect(FEATURES.filter((f) => f.kind === "Check mode").map((f) => f.id)).toEqual([
      "quickcheck",
      "fullcheck",
      "backgroundcheck",
    ]);
  });

  it("never writes a source count into the copy", () => {
    // Phase 175 shipped "40 open sources" in the QuickCheck description and was
    // wrong two days later, when eiti_assessment took the registry to 41. The
    // count now comes from `/sources` through `ledeSentence` and may not come
    // back: a number beside the word "sources" in any description fails here.
    for (const f of FEATURES) {
      expect(f.description, f.id).not.toMatch(/\b\d[\d,]*\s+(open\s+)?(sources?|registers?|adapters?)\b/i);
      expect(f.cta.label, f.id).not.toMatch(/\b\d[\d,]*\s+(open\s+)?(sources?|registers?|adapters?)\b/i);
    }
  });

  it("asserts no certainty the risk layer refuses to", () => {
    // The findings.py discipline, applied to marketing copy: nothing here may
    // read as a guarantee, and "clean" may only appear in a sentence that
    // denies it.
    for (const f of FEATURES) {
      const d = f.description.toLowerCase();
      expect(d, f.id).not.toMatch(/\bguarantee/);
      expect(d, f.id).not.toMatch(/\b(complete|comprehensive) (screen|check|coverage)\b/);
      if (d.includes("clean")) expect(d, f.id).toMatch(/never|rather than|not\b/);
    }
  });
});

describe("ledeSentence", () => {
  it("names the live count when it has arrived", () => {
    expect(ledeSentence(41)).toContain("one lookup over 41 open sources");
  });

  it("loses the figure rather than guessing while the count is unknown", () => {
    const lede = ledeSentence(null);
    expect(lede).toContain("every open source that can answer");
    expect(lede).not.toMatch(/\d/);
  });

  it("says the same thing either way", () => {
    for (const lede of [ledeSentence(41), ledeSentence(null)]) {
      expect(lede).toContain("Legal Entity Identifier");
      expect(lede).toContain("Beneficial Ownership Data Standard");
      expect(sentenceCount(lede)).toBe(2);
    }
  });
});

describe("sentenceCount", () => {
  it("counts sentence-ending punctuation", () => {
    expect(sentenceCount("One.")).toBe(1);
    expect(sentenceCount("One. Two.")).toBe(2);
    expect(sentenceCount("One — a clause — still one.")).toBe(1);
    expect(sentenceCount("")).toBe(0);
  });
});

describe("featureById", () => {
  it("finds a feature by its anchor", () => {
    expect(featureById("time-machine")?.name).toBe("Time Machine");
  });

  it("returns undefined for an anchor no feature owns", () => {
    expect(featureById("esg")).toBeUndefined();
  });
});
