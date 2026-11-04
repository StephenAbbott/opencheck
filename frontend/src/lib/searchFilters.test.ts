import { describe, expect, it } from "vitest";
import {
  COUNTRY_JURISDICTIONS,
  activeFilterCount,
  clashingSubdivisions,
  countLine,
  filterChips,
  gleifFilterParams,
  jurisdictionMatches,
  matchLegalForms,
  needsExactJurisdiction,
  recordMatches,
  subdivisionsOf,
  withoutFilter,
  type LegalForm,
} from "./searchFilters";

const form = (code: string, name: string, abbreviation: string | null, abbreviations: string[] = abbreviation ? [abbreviation] : [], issued = 0): LegalForm => ({
  code,
  name,
  abbreviation,
  abbreviations,
  names: [name],
  subdivision: null,
  issued_leis: issued,
});

const GERMAN = [
  form("2HBR", "Gesellschaft mit beschränkter Haftung", "GmbH", ["GmbH"], 109_065),
  form("8Z6G", "Kommanditgesellschaft", "KG", ["KG", "GmbH & Co. KG"], 22_768),
  form("V2YH", "Stiftung des privaten Rechts", null, [], 15_198),
  form("63KS", "Unternehmergesellschaft", "UG", ["GmbH UG", "UG (haftungsbeschränkt)"], 3_778),
];

describe("jurisdictions", () => {
  it("knows the codes GLEIF's filter also matches inside a state or province", () => {
    expect(clashingSubdivisions("DE")).toEqual(["US-DE"]);
    expect(clashingSubdivisions("PA")).toEqual(["US-PA"]);
    expect(clashingSubdivisions("AZ")).toEqual(["AE-AZ", "US-AZ"]);
    expect(clashingSubdivisions("GB")).toEqual([]);
  });

  it("matches a country or its own sub-divisions, never a state of another country", () => {
    expect(jurisdictionMatches("US-DE", "US")).toBe(true);
    expect(jurisdictionMatches("US-DE", "DE")).toBe(false);
    expect(jurisdictionMatches("US-PA", "PA")).toBe(false);
    expect(jurisdictionMatches("PA", "PA")).toBe(true);
  });

  it("offers countries by name and US states separately, without GLEIF's unknown", () => {
    expect(COUNTRY_JURISDICTIONS.some((j) => j.code === "DE")).toBe(true);
    expect(COUNTRY_JURISDICTIONS.some((j) => j.code.includes("-") || j.code === "XX")).toBe(false);
    expect(subdivisionsOf("US").some((j) => j.code === "US-DE")).toBe(true);
    expect(subdivisionsOf("DE")).toEqual([]);
  });

  it("scans only when GLEIF would return records from elsewhere", () => {
    expect(needsExactJurisdiction({ jurisdiction: "PA" })).toBe(true);
    expect(needsExactJurisdiction({ jurisdiction: "DE", legalForm: ["2HBR"] })).toBe(false);
    expect(needsExactJurisdiction({ jurisdiction: "DE", legalForm: ["8888", "9999"] })).toBe(true);
    expect(needsExactJurisdiction({ jurisdiction: "GB" })).toBe(false);
  });
});

describe("the GLEIF query", () => {
  it("uses entity.legalForm, which GLEIF accepts, not entity.legalForm.id", () => {
    expect(
      gleifFilterParams({ jurisdiction: "DE", legalForm: ["2HBR"], entityStatus: "ACTIVE", registrationStatus: ["ISSUED"], category: ["FUND"] }),
    ).toEqual([
      ["filter[entity.jurisdiction]", "DE"],
      ["filter[entity.legalForm]", "2HBR"],
      ["filter[entity.status]", "ACTIVE"],
      ["filter[registration.status]", "ISSUED"],
      ["filter[entity.category]", "FUND"],
    ]);
  });

  it("checks every row against the filters", () => {
    const row = (jurisdiction: string, status = "ISSUED") => ({
      attributes: { entity: { jurisdiction, legalForm: { id: "8888" }, status: "ACTIVE", category: "GENERAL" }, registration: { status } },
    });
    expect(recordMatches(row("PA"), { jurisdiction: "PA" })).toBe(true);
    expect(recordMatches(row("US-PA"), { jurisdiction: "PA" })).toBe(false);
    expect(recordMatches(row("PA", "LAPSED"), { jurisdiction: "PA", registrationStatus: ["ISSUED"] })).toBe(false);
  });
});

describe("chips", () => {
  it("names each filter and what its remove control removes", () => {
    const chips = filterChips(
      { jurisdiction: "DE", legalForm: ["2HBR"], entityStatus: "ACTIVE", registrationStatus: ["LAPSED"], category: ["FUND"] },
      GERMAN,
    );
    expect(chips.map((c) => c.label)).toEqual([
      "Germany",
      "GmbH · Gesellschaft mit beschränkter Haftung",
      "Company active",
      "LEI lapsed",
      "Fund",
    ]);
    expect(chips[0].removeLabel).toBe("Remove jurisdiction filter: Germany");
    expect(filterChips({ jurisdiction: "DE", legalForm: ["8888", "9999"] })[1].label).toBe("No ELF code");
  });

  it("drops the legal form with its jurisdiction", () => {
    expect(withoutFilter({ jurisdiction: "DE", legalForm: ["2HBR"], entityStatus: "ACTIVE" }, "jurisdiction")).toEqual({
      entityStatus: "ACTIVE",
    });
    expect(activeFilterCount({ jurisdiction: "DE", legalForm: ["2HBR"] })).toBe(2);
  });
});

describe("matching a typed legal form", () => {
  it("puts an exact abbreviation first, then names, keeping the list's order", () => {
    expect(matchLegalForms(GERMAN, "gmbh").map((f) => f.code)).toEqual(["2HBR", "8Z6G", "63KS"]);
    expect(matchLegalForms(GERMAN, "stift").map((f) => f.code)).toEqual(["V2YH"]);
    expect(matchLegalForms(GERMAN, "63ks").map((f) => f.code)).toEqual(["63KS"]);
    expect(matchLegalForms(GERMAN, "").length).toBe(4);
  });
});

describe("the count line", () => {
  it("says how many matched, not how many are shown", () => {
    expect(countLine({ shown: 10, total: 48_503 })).toBe("First 10 of 48,503 GLEIF records");
    expect(countLine({ shown: 4, total: 4 })).toBe("4 GLEIF records");
    expect(countLine({ shown: 1, total: 1 })).toBe("1 GLEIF record");
  });

  it("says when the exact scan stopped early", () => {
    expect(countLine({ shown: 3, total: 75, scanCapped: true, recordsChecked: 1000, rawTotal: 3317 })).toBe(
      "Showing the 3 found in the first 1,000 of 3,317 GLEIF records checked",
    );
  });
});
