import { describe, expect, it } from "vitest";

import {
  COUNTRY_OPTIONS,
  RA_CODES,
  raCodeFor,
  validateNationalId,
} from "./raCodes";

/**
 * The national-ID reverse lookup fails closed: a query scoped to the wrong
 * registration authority returns an empty result set, which is exactly what a
 * company that does not exist returns. Nothing about a wrong RA code is
 * visible from the outside, so the only defence is pinning it.
 *
 * backend/tests/test_ra_codes.py parses this module and asserts the codes and
 * sub-registry rules match the backend's. These tests cover what a parse
 * cannot: that the lookup logic itself behaves.
 */

describe("raCodeFor", () => {
  it("returns the country's authority when the number says nothing more", () => {
    expect(raCodeFor("GB", "00102498")).toBe("RA000585");
    expect(raCodeFor("NL", "34362985")).toBe("RA000463");
  });

  it("routes Scottish company numbers to Scotland's authority", () => {
    // Until Phase 141 these were scoped to RA000585, England & Wales — the one
    // registry a Scottish company is guaranteed not to be in.
    for (const n of ["SC651281", "SO301234", "SF001234"]) {
      expect(raCodeFor("GB", n)).toBe("RA000587");
    }
  });

  it("routes Northern Irish company numbers to Northern Ireland's authority", () => {
    for (const n of ["NI012345", "NC001234", "R0001234"]) {
      expect(raCodeFor("GB", n)).toBe("RA000586");
    }
  });

  it("never returns the Pensions Regulator's code", () => {
    // RA000591 is The Pensions Regulator, not a company registry. It sat in
    // the backend helper as Northern Ireland's code for months.
    const numbers = ["NI012345", "NC001234", "R0001234", "SC651281", "00102498"];
    for (const n of numbers) expect(raCodeFor("GB", n)).not.toBe("RA000591");
  });

  it("ignores case and surrounding whitespace", () => {
    expect(raCodeFor("GB", "  sc651281 ")).toBe("RA000587");
    expect(raCodeFor("GB", "ni012345")).toBe("RA000586");
  });

  it("falls back to the country default without a number", () => {
    expect(raCodeFor("GB")).toBe("RA000585");
    expect(raCodeFor("GB", "   ")).toBe("RA000585");
  });

  it("scopes nothing for a country it does not know", () => {
    // An unscoped GLEIF query is a wider search; a wrongly scoped one finds
    // nothing at all. Guessing is the worse failure.
    expect(raCodeFor("ZZ", "12345")).toBe("");
    expect(raCodeFor("", "12345")).toBe("");
  });
});

describe("COUNTRY_OPTIONS", () => {
  it("offers every country that has an entry", () => {
    // The hand-listed version silently omitted New Zealand: a correct code, a
    // working backend mapping, and no way to pick it.
    expect(COUNTRY_OPTIONS.map((o) => o.code).sort()).toEqual(
      Object.keys(RA_CODES).sort(),
    );
  });

  it("is sorted by country name", () => {
    const names = COUNTRY_OPTIONS.map((o) => o.entry.countryName);
    expect(names).toEqual([...names].sort((a, b) => a.localeCompare(b, "en")));
  });

  it("carries a real entry for every option", () => {
    for (const { code, entry } of COUNTRY_OPTIONS) {
      expect(entry, `${code} has no entry`).toBeTruthy();
      expect(entry.raCode).toMatch(/^RA\d{6}$/);
      expect(entry.idLabel.length).toBeGreaterThan(0);
    }
  });
});

describe("sub-registry declarations", () => {
  it("declares codes distinct from the country default", () => {
    // A sub-registry that repeats the default is a no-op rule, which reads as
    // coverage without being any.
    for (const [code, entry] of Object.entries(RA_CODES)) {
      for (const rule of entry.subRegistries ?? []) {
        expect(rule.raCode, `${code} sub-registry repeats the default`).not.toBe(
          entry.raCode,
        );
        expect(rule.prefixes.length).toBeGreaterThan(0);
        expect(rule.label.length).toBeGreaterThan(0);
      }
    }
  });

  it("does not let two rules claim the same prefix", () => {
    for (const [code, entry] of Object.entries(RA_CODES)) {
      const seen = new Set<string>();
      for (const rule of entry.subRegistries ?? []) {
        for (const prefix of rule.prefixes) {
          expect(seen.has(prefix), `${code} claims ${prefix} twice`).toBe(false);
          seen.add(prefix);
        }
      }
    }
  });
});

describe("validateNationalId", () => {
  it("accepts the prefixed forms its own hint invites", () => {
    // The GB format hint names OC, SC and NI explicitly; a validator that
    // warned on them would contradict the field's own instructions.
    for (const n of ["SC651281", "NI012345", "OC301234", "00102498"]) {
      expect(validateNationalId("GB", n)).toBe(true);
    }
  });

  it("stays advisory for empty input and unknown countries", () => {
    expect(validateNationalId("GB", "")).toBe(true);
    expect(validateNationalId("ZZ", "whatever")).toBe(true);
  });

  it("accepts a ΓΕΜΗ number in both the padded and unpadded forms", () => {
    // GLEIF stores some zero-padded to 12 and some not, and the ΓΕΜΗ API
    // accepts either, so neither may be rejected here.
    expect(validateNationalId("GR", "160228803000")).toBe(true);
    expect(validateNationalId("GR", "003324001000")).toBe(true);
  });
});
