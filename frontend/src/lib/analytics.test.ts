/**
 * Phase 89 — the privacy contract for analytics paths.
 *
 * canonicalPath is the single choke-point between browser URLs and what
 * GoatCounter records. These tests pin the ticket's constraint: no LEIs,
 * no person names, no query strings — ever.
 */
import { describe, expect, it } from "vitest";

import { canonicalPath, qaOptOut } from "./analytics";

describe("canonicalPath", () => {
  it("rolls ?lei= lookups up to /lookup with no query string", () => {
    expect(canonicalPath("/", "?lei=549300PPXHEU2JF0AM85")).toBe("/lookup");
    expect(canonicalPath("/", "?lei=549300PPXHEU2JF0AM85&refresh=1")).toBe("/lookup");
  });

  it("rolls a saved report up to /report — its id is a capability", () => {
    expect(canonicalPath("/report/SU82_KMkQo2QbEv3Kcfm8A", "")).toBe("/report");
    expect(canonicalPath("/report/SU82_KMkQo2QbEv3Kcfm8A", "?mode=full")).toBe("/report");
  });

  it("rolls ?person= reports up to /person-check (wins over ?lei=)", () => {
    expect(canonicalPath("/", "?person=Jane%20Doe")).toBe("/person-check");
    expect(canonicalPath("/", "?lei=549300PPXHEU2JF0AM85&person=Jane")).toBe(
      "/person-check",
    );
  });

  it("rolls entity and browse pages up without the LEI or country", () => {
    expect(
      canonicalPath("/entity/549300PPXHEU2JF0AM85-unilever-plc", ""),
    ).toBe("/entity");
    expect(canonicalPath("/browse/GB", "?page=4")).toBe("/browse");
    expect(canonicalPath("/browse", "")).toBe("/browse");
  });

  it("passes known subject-free views through and buckets the unknown to /", () => {
    for (const view of ["/", "/sources", "/about", "/api", "/changelog"]) {
      expect(canonicalPath(view, "")).toBe(view);
    }
    expect(canonicalPath("/some/unexpected/path", "")).toBe("/");
  });

  it("never returns a query string or an LEI, whatever comes in", () => {
    const cases: Array<[string, string]> = [
      ["/", "?lei=2138001ZRU8TFM5YE456"],
      ["/entity/2138001ZRU8TFM5YE456-stanford-bay", "?utm_source=x"],
      ["/weird", "?lei=2138001ZRU8TFM5YE456&person=X&foo=bar"],
    ];
    for (const [p, s] of cases) {
      const out = canonicalPath(p, s);
      expect(out).not.toContain("?");
      expect(out).not.toMatch(/[0-9A-Z]{18}[0-9]{2}/);
    }
  });
});

describe("qaOptOut", () => {
  it("?qa=1 opts the browser out and remembers it", () => {
    expect(qaOptOut("?qa=1", null)).toEqual({ optedOut: true, store: "1" });
    expect(qaOptOut("?lei=213800LH1BZH3DI6G760&qa=1", null)).toEqual({ optedOut: true, store: "1" });
  });

  it("a remembered opt-out holds on pages without the parameter", () => {
    expect(qaOptOut("", "1")).toEqual({ optedOut: true, store: undefined });
    expect(qaOptOut("?lei=213800LH1BZH3DI6G760", "1")).toEqual({ optedOut: true, store: undefined });
  });

  it("?qa=0 clears it", () => {
    expect(qaOptOut("?qa=0", "1")).toEqual({ optedOut: false, store: null });
  });

  it("a reader with no mark is counted, and any other value changes nothing", () => {
    expect(qaOptOut("", null)).toEqual({ optedOut: false, store: undefined });
    expect(qaOptOut("?qa=yes", null)).toEqual({ optedOut: false, store: undefined });
    expect(qaOptOut("?qa=yes", "1")).toEqual({ optedOut: true, store: undefined });
  });
});
