import { describe, expect, it } from "vitest";
import {
  SECURITIES_SOURCE_NAMES,
  isinListSourceLine,
  isinListStaleLine,
  isinListUnavailableNotice,
} from "./securities";
import { sourceList } from "./vocab";

describe("isinListUnavailableNotice (Phase 253)", () => {
  it("does not blame GLEIF when OpenCheck held the request back", () => {
    const s = isinListUnavailableNotice("held_for_lookups", 0);
    expect(s).toMatch(/OpenCheck did not ask GLEIF/);
    expect(s).toMatch(/for lookups/);
    expect(s).not.toMatch(/rate-limiting/);
    expect(s).not.toMatch(/unreachable|did not answer/);
  });

  it("names GLEIF's rate limit only when that is the cause", () => {
    expect(isinListUnavailableNotice("rate_limited", 0)).toMatch(/GLEIF is rate-limiting/);
    expect(isinListUnavailableNotice("unreachable", 0)).toMatch(/GLEIF did not answer/);
  });

  it("guesses at no cause when the backend sends none", () => {
    const s = isinListUnavailableNotice(undefined, 0);
    expect(s).not.toMatch(/rate-limiting|unreachable|did not answer|for lookups/);
    expect(s).toMatch(/unknown for the moment/);
  });

  it("says the sanctions check ran only when it found nothing to show", () => {
    expect(isinListUnavailableNotice("held_for_lookups", 0)).toMatch(/sanctioned-securities check did run/);
    expect(isinListUnavailableNotice("held_for_lookups", 2)).not.toMatch(/sanctioned-securities check/);
  });
});

describe("isinListStaleLine (Phase 253)", () => {
  it("is null for a current list", () => {
    expect(isinListStaleLine(false, "2026-09-28T07:00:00+00:00", null)).toBeNull();
    expect(isinListStaleLine(undefined, null, null)).toBeNull();
  });

  it("dates a stand-in page by when GLEIF was asked, in UTC", () => {
    const s = isinListStaleLine(true, "2026-09-23T23:30:00+00:00", "held_for_lookups");
    expect(s).toContain("as of 23 Sept 2026");
    expect(s).toContain("OpenCheck kept its last GLEIF requests for lookups");
  });

  it("names the cause it was given", () => {
    expect(isinListStaleLine(true, "2026-09-20T00:00:00Z", "rate_limited")).toContain("rate-limiting");
    expect(isinListStaleLine(true, "2026-09-20T00:00:00Z", "unreachable")).toContain("did not answer");
  });
});

describe("SECURITIES_SOURCE_NAMES", () => {
  it("spells OpenFIGI the way OpenFIGI does", () => {
    expect(sourceList(["openfigi", "opensanctions"], { opensanctions: "OpenSanctions", ...SECURITIES_SOURCE_NAMES }))
      .toBe("OpenFIGI and OpenSanctions");
  });
});

describe("isinListSourceLine (Phase 258)", () => {
  it("dates a list read from GLEIF's file and says its order", () => {
    expect(isinListSourceLine("gleif_file", "2026-09-28T07:15:11+00:00", 1813)).toBe(
      "From GLEIF's ISIN-to-LEI file of 28 Sept 2026, listed in ISIN order.",
    );
  });

  it("does not mention order for a single ISIN", () => {
    expect(isinListSourceLine("gleif_file", "2026-09-28T07:15:11Z", 1)).toBe(
      "From GLEIF's ISIN-to-LEI file of 28 Sept 2026.",
    );
  });

  it("is silent for the live API and for no list", () => {
    expect(isinListSourceLine("gleif_api", "2026-09-28T07:15:11Z", 5)).toBeNull();
    expect(isinListSourceLine(null, null, 0)).toBeNull();
    expect(isinListSourceLine(undefined, undefined, 0)).toBeNull();
  });
});
