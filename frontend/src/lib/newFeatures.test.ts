import { describe, expect, it } from "vitest";

import tailwindConfig from "../../tailwind.config.js?raw";
import { NEW_FEATURES, NEW_FEATURE_TONES } from "./newFeatures";

const SVGS = import.meta.glob("../../public/new-features/*.svg", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

describe("NEW_FEATURES", () => {
  it("holds the three features the band was briefed with, in order", () => {
    expect(NEW_FEATURES.map((f) => f.title)).toEqual([
      "Sanctions 50% test",
      "Ownership map via MCP",
      "Watchlist",
    ]);
  });

  it("gives every feature a one-sentence description", () => {
    for (const f of NEW_FEATURES) {
      expect(f.description, f.id).toMatch(/^[A-Z].*\.$/);
      // One sentence: no full stop before the last character.
      expect(f.description.slice(0, -1), f.id).not.toMatch(/\.\s/);
      expect(f.description.length, f.id).toBeLessThanOrEqual(140);
    }
  });

  it("states no count that moves with the registry or the engine", () => {
    for (const f of NEW_FEATURES) {
      const text = `${f.description} ${f.cta} ${f.imageAlt}`;
      expect(text, f.id).not.toMatch(/\d+\s*(sources|registers|adapters|datasets|companies)/i);
    }
  });

  it("links every CTA to a same-site path", () => {
    for (const f of NEW_FEATURES) {
      expect(f.href, f.id).toMatch(/^\/[^/]/);
      expect(f.cta.trim(), f.id).not.toBe("");
    }
  });

  it("ships an SVG for every image, with nothing executable in it", () => {
    for (const f of NEW_FEATURES) {
      const key = `../../public${f.image}`;
      const svg = SVGS[key];
      expect(svg, f.image).toBeDefined();
      expect(svg).toMatch(/viewBox="0 0 320 168"/);
      expect(svg).not.toMatch(/<script|<foreignObject|\son[a-z]+\s*=\s*"/i);
      expect(f.imageAlt.trim(), f.id).not.toBe("");
    }
  });

  it("uses only tones that exist, and every tone class names a defined token", () => {
    for (const f of NEW_FEATURES) {
      expect(NEW_FEATURE_TONES[f.tone], f.id).toBeDefined();
    }
    // The risk panel's border is the one token Phase 344 added, inside the
    // risk tier (the value is named in tailwind.config.js, not here).
    expect(tailwindConfig).toMatch(/risk:\s*\{[^}]*tintBorder:/);
    for (const tone of Object.values(NEW_FEATURE_TONES)) {
      for (const cls of `${tone.panel} ${tone.eyebrow}`.split(" ")) {
        expect(cls).toMatch(/^(bg|border|text)-oo-/);
      }
    }
  });
});
