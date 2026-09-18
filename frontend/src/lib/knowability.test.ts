import { describe, expect, it } from "vitest";

import type { KnowabilityStatement } from "./api";
import {
  formatKnowabilityDate,
  knowabilityBadge,
  knowabilityRows,
  knowabilityView,
} from "./knowability";

const GB: KnowabilityStatement = {
  code: "GB",
  name: "United Kingdom",
  sentence:
    "Register of People with Significant Control (PSC), Companies House is open to the public. OpenCheck reads beneficial owners from Companies House.",
  sentences: [
    "Register of People with Significant Control (PSC), Companies House is open to the public.",
    "OpenCheck reads beneficial owners from Companies House.",
  ],
  stated_absence: false,
  review_status: "verified",
  last_verified: "2026-09-18",
  access: "public",
  access_since: null,
  next_change_expected: null,
  fields: {
    register: "Register of People with Significant Control (PSC), Companies House",
    register_url: "https://find-and-update.company-information.service.gov.uk/",
    threshold_wording: "more than 25 %",
    fields_published: ["names", "month_year_of_birth", "percentage_bands"],
    reporting_basis: "first_qualifying_link",
    covers: ["companies"],
    verification: "identity_verified",
    verification_note: null,
    amld6_lia: "Not applicable",
    amld6_details: null,
    boris: "Not applicable",
    amlr_alignment_watch: false,
    pending_changes: null,
    company_register: { name: "Companies House", url: null, public: "Yes", publishes: ["officers", "filings_history"] },
    fatf: { body: "FATF", onsite: "2027-03-01", plenary: null },
    eu_eea: false,
    groups: ["OpenCheck source"],
    notes: null,
  },
  opencheck_reads: [
    { source_id: "companies_house", name: "Companies House", requires_api_key: true, reads_beneficial_owners: true },
    { source_id: "bods_uk_psc", name: "UK PSC (BODS)", requires_api_key: false, reads_beneficial_owners: true },
  ],
  sources: [{ url: "https://www.legislation.gov.uk/ukpga/2006/46/part/21A", title: null }],
  as_of: "2026-09-18",
};

const ABSENT: KnowabilityStatement = {
  code: "AR",
  name: "Argentina",
  sentence: "OpenCheck holds no register notes for Argentina; an owner absent from this report says nothing about what Argentina publishes.",
  sentences: ["OpenCheck holds no register notes for Argentina; an owner absent from this report says nothing about what Argentina publishes."],
  stated_absence: true,
  review_status: "absent",
  fields: {},
  opencheck_reads: [],
  sources: [],
  as_of: "2026-09-18",
};

describe("formatKnowabilityDate", () => {
  it("formats an ISO day in UTC and tolerates junk", () => {
    expect(formatKnowabilityDate("2026-09-18")).toBe("18 Sept 2026");
    expect(formatKnowabilityDate("2026-09-18T13:30:46+00:00")).toBe("18 Sept 2026");
    expect(formatKnowabilityDate(null)).toBeNull();
    expect(formatKnowabilityDate("not a date")).toBeNull();
  });
});

describe("knowabilityBadge (Phase 224)", () => {
  it("says when Stephen checked a verified row, in a context tone", () => {
    expect(knowabilityBadge(GB)).toEqual({ label: "Checked 18 Sept 2026", tone: "context" });
  });
  it("calls an unverified row a draft — a fact about the row, never a warning", () => {
    expect(knowabilityBadge({ ...GB, review_status: "unverified", last_verified: null })).toEqual({
      label: "Unverified draft",
      tone: "neutral",
    });
  });
  it("names a stated absence as such", () => {
    expect(knowabilityBadge(ABSENT)).toEqual({ label: "No register notes held", tone: "neutral" });
  });
  it("never picks a risk or warn tone", () => {
    for (const st of [GB, ABSENT, { ...GB, access: "authorities_and_obliged_entities_only" }]) {
      expect(["context", "neutral"]).toContain(knowabilityBadge(st).tone);
    }
  });
});

describe("knowabilityRows", () => {
  it("lists the fields behind the sentence, dropping empty ones", () => {
    const rows = knowabilityRows(GB);
    const byLabel = Object.fromEntries(rows.map((r) => [r.label, r.value]));
    expect(byLabel["Beneficial ownership register"]).toBe(GB.fields.register);
    expect(byLabel["Who can see it"]).toBe("Public");
    expect(byLabel["Threshold wording"]).toBe("more than 25 %");
    expect(byLabel["Fields published"]).toBe("names, month year of birth, percentage bands");
    expect(byLabel["Reporting basis"]).toBe("first qualifying link (not the ultimate person)");
    expect(byLabel["Verification of filings"]).toBe("identities verified");
    expect(byLabel["Company register"]).toBe("Companies House; public: yes; publishes officers, filings history");
    expect(byLabel["OpenCheck reads"]).toBe(
      "Companies House (beneficial owners) (API key); UK PSC (BODS) (beneficial owners)",
    );
    expect(byLabel["Next FATF / FSRB assessment"]).toBe("FATF; onsite 1 Mar 2027");
    // Empty fields are absent, not dashes.
    expect(byLabel["Pending changes"]).toBeUndefined();
    expect(byLabel["Current arrangement since"]).toBeUndefined();
    expect(byLabel["Notes"]).toBeUndefined();
  });

  it("spells out the lower rungs of the access ladder with the AMLD floor", () => {
    const rows = knowabilityRows({ ...GB, access: "legitimate_interest", access_since: "2025-09-01" });
    const byLabel = Object.fromEntries(rows.map((r) => [r.label, r.value]));
    expect(byLabel["Who can see it"]).toBe("Authorities, obliged entities, and others on legitimate interest");
    expect(byLabel["Current arrangement since"]).toBe("1 Sept 2025");
  });

  it("says when OpenCheck reads no register there", () => {
    const rows = knowabilityRows({ ...GB, name: "Cayman Islands", opencheck_reads: [] });
    expect(rows.find((r) => r.label === "OpenCheck reads")?.value).toBe("no Cayman Islands register");
  });

  it("has nothing to list for a stated absence", () => {
    expect(knowabilityRows(ABSENT)).toEqual([]);
  });
});

describe("knowabilityView", () => {
  it("frames the server sentence verbatim, with sources and the as-of day", () => {
    const v = knowabilityView(GB);
    expect(v.heading).toBe("What can be known");
    expect(v.subheading).toBe("United Kingdom");
    expect(v.sentence).toBe(GB.sentence);
    expect(v.sources).toEqual([
      {
        url: "https://www.legislation.gov.uk/ukpga/2006/46/part/21A",
        title: "www.legislation.gov.uk/ukpga/2006/46/part/21A",
      },
    ]);
    expect(v.asOfLine).toBe("Rendered as of 18 Sept 2026");
  });

  it("reads no clock: no as-of line when the statement carries none", () => {
    expect(knowabilityView({ ...GB, as_of: undefined }).asOfLine).toBeNull();
  });
});
