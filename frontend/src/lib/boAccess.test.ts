import { describe, expect, it } from "vitest";
import { boAccessSentence } from "./boAccess";
import type { BoAccessNotice } from "./api";

const IE: BoAccessNotice = {
  status: "restricted",
  country_code: "IE",
  country_name: "Ireland",
  effective_date: null,
  access_url: "https://rbo.gov.ie/faqs/access-to-the-search-facility-on-the-rbo-register/",
  register_name:
    "Central Register of Beneficial Ownership of Companies and Industrial and Provident Societies (RBO)",
  register_url: "https://rbo.gov.ie/",
  register_covers: ["companies"],
};

describe("boAccessSentence", () => {
  it("names the register that holds the owners, not the card's source", () => {
    expect(boAccessSentence(IE)).toBe(
      "Beneficial ownership information for most companies in Ireland is kept in the RBO, not in this source. It is not public: available to certain groups only.",
    );
  });

  it("keeps a name with no acronym, and the country when the register covers trusts too", () => {
    expect(
      boAccessSentence({
        ...IE,
        country_name: "Netherlands",
        register_name: "UBO register",
        register_covers: ["companies", "trusts_arrangements"],
      }),
    ).toBe(
      "Beneficial ownership information for Netherlands is kept in the UBO register, not in this source. It is not public: available to certain groups only.",
    );
  });

  it("keeps the country wording when no register is named", () => {
    expect(boAccessSentence({ ...IE, register_name: null })).toBe(
      "Beneficial ownership data from Ireland is not public: available to certain groups only.",
    );
  });

  it("dates an announced restriction", () => {
    const s = boAccessSentence({
      ...IE,
      status: "becoming_restricted",
      effective_date: "2030-01-01",
    });
    expect(s).toContain("kept in the RBO. It is currently public");
    expect(s).toContain("1 January 2030");
  });
});
