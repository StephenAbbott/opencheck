import { describe, expect, it } from "vitest";
import {
  CHECK_MODES,
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
  });
});
