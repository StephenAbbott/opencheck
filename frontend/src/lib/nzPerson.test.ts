import { describe, expect, it } from "vitest";
import type { NzPersonAssociations, NzPersonAssociationsResponse } from "./api";
import { NZ_MATCH_CAVEAT, nzCompaniesSummary, nzCompanyRoles, nzTruncatedNote } from "./nzPerson";

const person = (over: Partial<NzPersonAssociations> = {}): NzPersonAssociations => ({
  name: "John Doe",
  role_here: ["director"],
  other_company_count: 3,
  high_confidence_count: 1,
  address_match_count: 2,
  name_only_count: 1,
  as_director: 3,
  as_shareholder: 1,
  total_records_under_name: 3,
  truncated: false,
  companies: [],
  ...over,
});

const resp = (p: NzPersonAssociations | null, over: Partial<NzPersonAssociationsResponse> = {}) => ({
  company_number: "1166320",
  name: "John Doe",
  available: p !== null,
  reason: null,
  subject_name: "MERIDIAN ENERGY LIMITED",
  person: p,
  ...over,
});

describe("nzCompaniesSummary", () => {
  it("splits address-matched from name-only, and never calls them the same person", () => {
    const s = nzCompaniesSummary(resp(person()));
    expect(s).toBe(
      "Under this name the New Zealand register lists 3 other companies: 2 with a matching address, 1 on the name alone.",
    );
    expect(s).not.toMatch(/same person|is a director of/);
  });

  it("says absence in the same voice", () => {
    expect(nzCompaniesSummary(resp(person({ other_company_count: 0, address_match_count: 0, name_only_count: 0 })))).toBe(
      "The New Zealand register names no other current company under this name.",
    );
  });

  it("names why the register could not be searched", () => {
    expect(nzCompaniesSummary(resp(null, { reason: "Companies Entity Role Search not configured" }))).toBe(
      "The New Zealand register could not be searched for this person (Companies Entity Role Search not configured).",
    );
  });
});

describe("rows and notes", () => {
  it("puts the share after the shareholder role only", () => {
    expect(
      nzCompanyRoles({
        number: "1", name: "A", nzbn: null, roles: ["director", "shareholder"], share_percentage: 50,
        confidence: "high", basis: "Same registered address", link: null,
      }),
    ).toBe("Director · Shareholder 50%");
  });

  it("says when only part of a common name was read", () => {
    expect(nzTruncatedNote(resp(person()))).toBeNull();
    expect(nzTruncatedNote(resp(person({ truncated: true, total_records_under_name: 412 })))).toMatch(/^412 role records/);
  });

  it("keeps the alternative-address warning", () => {
    expect(NZ_MATCH_CAVEAT).toMatch(/alternative address/);
    expect(NZ_MATCH_CAVEAT).toMatch(/not an identity/);
  });
});
