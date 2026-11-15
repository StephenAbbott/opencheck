import { describe, expect, it } from "vitest";
import { boAccessSentence } from "./boAccess";
import type { BoAccessNotice } from "./api";

const IE: BoAccessNotice = {
  status: "restricted",
  country_code: "IE",
  country_name: "Ireland",
  effective_date: null,
  access_url: "https://rbo.gov.ie/faqs/access-to-the-search-facility-on-the-rbo-register/",
  register_name: "Register of Beneficial Owners (RBO)",
  register_url: "https://rbo.gov.ie/",
};

describe("boAccessSentence", () => {
  it("names the register that holds the owners, not the card's source", () => {
    expect(boAccessSentence(IE)).toBe(
      "Beneficial ownership information for Ireland is kept in the Register of Beneficial Owners (RBO), not in this source. It is not public: available to certain groups only.",
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
    expect(s).toContain("kept in the Register of Beneficial Owners (RBO). It is currently public");
    expect(s).toContain("1 January 2030");
  });
});
