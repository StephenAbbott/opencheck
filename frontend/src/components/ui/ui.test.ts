import { describe, expect, it } from "vitest";
import { buttonClasses } from "./Button";
import { chipClasses, CONFIDENCE_GLYPH, CONFIDENCE_LABEL } from "./Chip";
import { sectionLabelClasses } from "./SectionLabel";
import { ICON_NAMES, ICON_PATHS } from "./Icon";

/**
 * These are the guards that stop the primitives drifting back into the
 * per-component variants they replaced. The suite is logic-only (there is
 * no jsdom in this project), so it asserts on the class builders rather
 * than on rendered output.
 */

describe("buttonClasses", () => {
  it("always carries the focus ring and the radius", () => {
    for (const v of ["primary", "secondary", "ghost", "warn", "danger"] as const) {
      const cls = buttonClasses(v);
      expect(cls).toContain("focus-visible:ring-oo-blue");
      expect(cls).toContain("rounded-oo");
    }
  });

  it("meets the 44px touch target at the default size", () => {
    expect(buttonClasses("primary")).toContain("min-h-[44px]");
    expect(buttonClasses("primary", "sm")).toContain("min-h-[36px]");
  });

  it("hovers within its own hue rather than turning grey", () => {
    // v1's primary hovered to oo-burst, a grey-navy.
    expect(buttonClasses("primary")).not.toContain("oo-burst");
  });

  it("appends caller classes last so they win", () => {
    expect(buttonClasses("primary", "md", "w-full")).toMatch(/w-full$/);
  });

  it("uses no raw hex outside the token set", () => {
    // The two darkened primary hover steps are the only literals allowed,
    // because Tailwind has no darker step of oo-blue to reference.
    const allowed = new Set(["#3529b8", "#2e2399"]);
    for (const v of ["primary", "secondary", "ghost", "warn", "danger"] as const) {
      for (const hex of buttonClasses(v).match(/#[0-9a-f]{3,8}/gi) ?? []) {
        expect(allowed.has(hex.toLowerCase())).toBe(true);
      }
    }
  });
});

describe("chipClasses", () => {
  it("gives every tone a background, a border and a text colour", () => {
    for (const t of ["risk", "context", "warn", "ok", "neutral", "accent"] as const) {
      const cls = chipClasses(t);
      expect(cls).toMatch(/\bbg-/);
      expect(cls).toMatch(/\bborder-/);
      expect(cls).toMatch(/\btext-\w/);
    }
  });

  it("keeps one rounding across every tone and size", () => {
    for (const t of ["risk", "context", "warn", "ok", "neutral", "accent"] as const) {
      for (const s of ["sm", "md"] as const) {
        expect(chipClasses(t, s)).toContain("rounded-full");
      }
    }
  });

  it("uses only the named type steps", () => {
    expect(chipClasses("neutral", "sm")).toContain("text-oo-meta");
    expect(chipClasses("neutral", "md")).toContain("text-oo-small");
  });
});

describe("confidence", () => {
  it("names every level it draws a glyph for", () => {
    for (const level of Object.keys(CONFIDENCE_GLYPH)) {
      expect(CONFIDENCE_LABEL[level]).toBeTruthy();
    }
  });

  it("never leaves a level to colour or glyph alone", () => {
    // Each label has to read as a sentence fragment on its own, because it
    // is all a screen reader gets.
    for (const label of Object.values(CONFIDENCE_LABEL)) {
      expect(label.length).toBeGreaterThan(6);
    }
  });
});

describe("sectionLabelClasses", () => {
  it("uses the eyebrow tracking token, not an arbitrary value", () => {
    expect(sectionLabelClasses()).toContain("tracking-oo-eyebrow");
    expect(sectionLabelClasses()).not.toMatch(/tracking-\[/);
  });

  it("never drops below the 12px floor", () => {
    expect(sectionLabelClasses()).toContain("text-oo-meta");
  });
});

describe("Icon", () => {
  it("draws every icon from stroke paths only", () => {
    for (const name of ICON_NAMES) {
      expect(ICON_PATHS[name].length).toBeGreaterThan(0);
      for (const d of ICON_PATHS[name]) {
        expect(typeof d).toBe("string");
        expect(d.length).toBeGreaterThan(0);
      }
    }
  });

  it("carries a glyph for all four check modes", () => {
    for (const mode of ["quickcheck", "fullcheck", "backgroundcheck", "esg"]) {
      expect(ICON_NAMES).toContain(mode);
    }
  });

  it("replaces the text glyphs the audit found", () => {
    // ⬇ ↓ → ⚠️ ↗ ‹ › ▸ ▾ were all doing icon duty in v1.
    for (const name of ["download", "arrowRight", "warning", "chevronDown", "external"]) {
      expect(ICON_NAMES).toContain(name);
    }
  });
});
