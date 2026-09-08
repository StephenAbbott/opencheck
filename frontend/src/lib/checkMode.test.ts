import { describe, expect, it } from "vitest";
import tailwindConfig from "../../tailwind.config.js?raw";
import {
  CHECK_MODES,
  MODE_ACCENT,
  TOPIC_MODES,
  documentTitleFor,
  modeLabel,
  modeParam,
  parseMode,
} from "./checkMode";

describe("parseMode", () => {
  it("round-trips every mode", () => {
    for (const mode of CHECK_MODES) {
      expect(parseMode(mode)).toBe(mode);
    }
  });

  it("falls back to quick rather than failing on junk", () => {
    // A stale link, a hand-edited URL or a mode removed in a later phase
    // should open the fastest check, not an error or an empty report.
    for (const junk of ["", "  ", "QUICK", "fullcheck", "esg2", null, undefined]) {
      expect(parseMode(junk)).toBe("quick");
    }
  });
});

describe("modeParam", () => {
  it("keeps quick out of the URL", () => {
    // QuickCheck is the default, so a shared link keeps the short form it
    // has always had and nothing that is already indexed changes.
    expect(modeParam("quick")).toBeNull();
  });

  it("names every other mode", () => {
    expect(modeParam("full")).toBe("full");
    expect(modeParam("background")).toBe("background");
    expect(modeParam("subsidiaries")).toBe("subsidiaries");
    expect(modeParam("esg")).toBe("esg");
  });

  it("writes a value parseMode can read back", () => {
    for (const mode of CHECK_MODES) {
      expect(parseMode(modeParam(mode))).toBe(mode);
    }
  });
});

describe("documentTitleFor", () => {
  it("leaves the QuickCheck title byte-identical to the server-rendered page", () => {
    // The /entity pages render "NAME OF SUBJECT - OpenCheck" — hyphen, not
    // em-dash. The SPA title must match it exactly for the default view.
    expect(documentTitleFor("quick", "ROSNEFT OIL COMPANY")).toBe(
      "ROSNEFT OIL COMPANY - OpenCheck",
    );
  });

  it("distinguishes a row of restored tabs", () => {
    const titles = CHECK_MODES.map((m) => documentTitleFor(m, "BP P.L.C."));
    expect(new Set(titles).size).toBe(CHECK_MODES.length);
  });

  it("names the mode for every non-default check", () => {
    expect(documentTitleFor("full", "BP P.L.C.")).toContain("FullCheck");
    expect(documentTitleFor("background", "BP P.L.C.")).toContain("BackgroundCheck");
    expect(documentTitleFor("subsidiaries", "BP P.L.C.")).toContain("Subsidiaries");
    expect(documentTitleFor("esg", "BP P.L.C.")).toContain("Climate & ESG");
  });
});

describe("modeLabel", () => {
  it("gives every mode a label", () => {
    for (const mode of CHECK_MODES) {
      expect(modeLabel(mode).length).toBeGreaterThan(3);
    }
  });

  it("uses the product names as they are written everywhere else", () => {
    // One label per mode: v1 called the same thing "BackgroundCheck",
    // "Background check" and "Person report · BackgroundCheck".
    expect(modeLabel("quick")).toBe("QuickCheck");
    expect(modeLabel("full")).toBe("FullCheck");
    expect(modeLabel("background")).toBe("BackgroundCheck");
    expect(modeLabel("subsidiaries")).toBe("Subsidiaries");
  });
});

describe("mode order and topics", () => {
  it("keeps the three depths first, then the two topics with subsidiaries before ESG", () => {
    // Phase 185: a different question, but still about ownership, so it sits
    // next to the ownership tabs and ESG stays last.
    expect(CHECK_MODES).toEqual(["quick", "full", "background", "subsidiaries", "esg"]);
    expect([...TOPIC_MODES].sort()).toEqual(["esg", "subsidiaries"]);
    expect(CHECK_MODES.filter((m) => TOPIC_MODES.has(m))).toEqual(["subsidiaries", "esg"]);
  });
});

/** The `key: "#hex"` pairs under one nested group of tailwind.config.js. */
function tokenGroup(name: string): Record<string, string> {
  const m = tailwindConfig.match(new RegExp(`${name}:\\s*\\{([^}]*)\\}`));
  if (!m) throw new Error(`no ${name} group in tailwind.config.js`);
  const out: Record<string, string> = {};
  for (const [, key, hex] of m[1].matchAll(/(\w+):\s*"(#[0-9a-f]{6})"/g)) out[key] = hex;
  return out;
}

describe("MODE_ACCENT", () => {
  it("has one accent per mode, all distinct", () => {
    expect(Object.keys(MODE_ACCENT).sort()).toEqual([...CHECK_MODES].sort());
    expect(new Set(Object.values(MODE_ACCENT)).size).toBe(CHECK_MODES.length);
  });

  it("takes every value from a design-system token rather than inventing one", () => {
    // Restating the hexes here would only prove two files agree; read the
    // tokens out of the config so a re-brand there fails this test.
    const node = tokenGroup("node");
    const graph = tokenGroup("graph");
    expect(MODE_ACCENT.quick).toBe(node.green);
    expect(MODE_ACCENT.full).toBe(node.blue);
    expect(MODE_ACCENT.background).toBe(node.purple);
    expect(MODE_ACCENT.esg).toBe(node.teal);
    // Subsidiaries lists what the company controls, so it takes the colour
    // the graph already draws control edges in.
    expect(MODE_ACCENT.subsidiaries).toBe(graph.control);
  });
});
