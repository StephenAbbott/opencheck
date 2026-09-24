/**
 * subjectProfile — the status chip and the identity-band rows (Phase 154).
 *
 * The chip carries register status alone and never a risk tone; the rows say
 * what is known and name who said it, never a count.
 */
import { describe, expect, it } from "vitest";

import type { LeiRegistration, SubjectProfile } from "./api";
import {
  LEI_NOT_ENTITY_STATUS,
  formatProfileDate,
  leiRegistrationChip,
  leiRegistrationLine,
  profileRows,
  statusChip,
} from "./subjectProfile";

const NAMES = {
  companies_house: "UK Companies House",
  gleif: "GLEIF",
  opencorporates: "OpenCorporates",
};

const fact = (value: string, sources: string[]) => ({
  value,
  sources,
  independent_sources: sources.length,
  other_values: [],
});

const shell = (overrides: Partial<SubjectProfile> = {}): SubjectProfile => ({
  legal_form: fact("Public limited company", ["companies_house", "gleif"]),
  register_status: {
    liveness: "live",
    since: null,
    raw: "active",
    source_id: "companies_house",
    sources: ["companies_house", "gleif", "opencorporates"],
    independent_sources: 2,
    other_values: [],
  },
  founding_date: fact("2002-02-05", ["companies_house", "gleif"]),
  registered_address: { ...fact("Shell Centre, London, SE1 7NA", ["companies_house"]), country: "GB" },
  jurisdiction: "GB",
  statement_ids: ["a", "b"],
  ...overrides,
});

describe("statusChip", () => {
  it("names the register and keeps a live status neutral — a status is a fact with no valence", () => {
    const chip = statusChip(shell(), NAMES);
    expect(chip).toEqual({
      label: "Active · UK Companies House",
      tone: "neutral",
      detail: "UK Companies House records this company as active.",
    });
  });

  it("warns while a terminal process is under way, with its date", () => {
    const chip = statusChip(
      shell({
        register_status: {
          liveness: "pending",
          since: "2026-01-31",
          raw: "liquidation",
          source_id: "companies_house",
          sources: ["companies_house"],
          independent_sources: 1,
          other_values: [{ source_id: "gleif", value: "live" }],
        },
      }),
      NAMES,
    );
    expect(chip?.tone).toBe("warn");
    expect(chip?.label).toBe("Terminal process under way · UK Companies House");
    expect(chip?.detail).toContain("since 31 Jan 2026");
  });

  it("marks a dissolved company as terminal — never as a risk", () => {
    const chip = statusChip(
      shell({
        register_status: {
          liveness: "terminal",
          since: "2019-04-03",
          raw: "dissolved",
          source_id: "companies_house",
          sources: ["companies_house"],
          independent_sources: 1,
          other_values: [],
        },
      }),
      NAMES,
    );
    expect(chip?.tone).toBe("terminal");
    expect(chip?.label).toBe("Dissolved · UK Companies House");
    expect(chip?.tone).not.toBe("risk");
  });

  it("renders nothing when no register stated a status — absence is not active", () => {
    expect(statusChip(shell({ register_status: null }), NAMES)).toBeNull();
    expect(statusChip(null, NAMES)).toBeNull();
    expect(statusChip(undefined, NAMES)).toBeNull();
  });

  it("falls back to a prettified id when the registry names are not loaded", () => {
    expect(statusChip(shell())?.label).toBe("Active · Companies House");
  });
});

describe("profileRows", () => {
  it("lists the four facts in reading order, each naming its sources in English", () => {
    const rows = profileRows(shell(), NAMES);
    expect(rows.map((r) => r.label)).toEqual([
      "Legal form",
      "Register status",
      "Incorporated",
      "Registered address",
    ]);
    expect(rows[0]).toEqual({
      label: "Legal form",
      value: "Public limited company",
      sources: "Source: UK Companies House and GLEIF",
    });
    expect(rows[1].value).toBe("Active");
    expect(rows[1].sources).toBe("Source: UK Companies House, GLEIF and OpenCorporates");
    expect(rows[2].value).toBe("5 Feb 2002");
    expect(rows[3].value).toBe("Shell Centre, London, SE1 7NA");
    // Never a count: two sources that copy each other would read as two.
    for (const r of rows) expect(r.sources).not.toMatch(/\d+ sources/);
  });

  it("omits a fact no source stated rather than rendering a placeholder", () => {
    const rows = profileRows(shell({ legal_form: null, registered_address: null }), NAMES);
    expect(rows.map((r) => r.label)).toEqual(["Register status", "Incorporated"]);
    expect(profileRows(null)).toEqual([]);
  });

  it("carries the register's own wording on a terminal status when it differs", () => {
    const rows = profileRows(
      shell({
        register_status: {
          liveness: "terminal",
          since: "2019-04-03",
          raw: "struck off",
          source_id: "companies_house",
          sources: ["companies_house"],
          independent_sources: 1,
          other_values: [],
        },
      }),
      NAMES,
    );
    expect(rows[1].value).toBe("Dissolved since 3 Apr 2019 — register status: “struck off”");
  });
});

describe("formatProfileDate", () => {
  it("formats a full ISO date and leaves a bare year or year-month as written", () => {
    expect(formatProfileDate("2002-02-05")).toBe("5 Feb 2002");
    expect(formatProfileDate("2002")).toBe("2002");
    expect(formatProfileDate("2002-02")).toBe("2002-02");
  });
});

// Phase 242 — the LEI record's own status. AFPC's record as GLEIF published it
// on 24 Sept 2026: LAPSED, renewal due 19 Oct 2017, entity still ACTIVE.
const lapsed: LeiRegistration = {
  status: "LAPSED",
  label: "Lapsed",
  flag: true,
  since: "2017-10-19",
  next_renewal_date: "2017-10-19",
  last_update_date: "2026-04-08",
  initial_registration_date: "2016-10-21",
  managing_lou: "5493001KJTIIGC8Y1R12",
  source_id: "gleif",
  sentence:
    "GLEIF records this LEI as lapsed: its renewal was due on 19 Oct 2017 and has not been made, so no issuer has re-checked its reference data since then. GLEIF last updated the record on 8 Apr 2026. This is the status of the LEI record, not of the company.",
};
const issued: LeiRegistration = {
  ...lapsed,
  status: "ISSUED",
  label: "Issued",
  flag: false,
  since: null,
  next_renewal_date: "2027-03-21",
  sentence: "GLEIF records this LEI as issued.",
};

describe("leiRegistrationChip", () => {
  it("says a lapse with the date it took effect, in the context tone", () => {
    const chip = leiRegistrationChip(lapsed);
    expect(chip).toEqual({ label: "LEI lapsed since 19 Oct 2017", tone: "context", detail: lapsed.sentence });
  });

  it("renders nothing for an issued LEI, or when no status was recorded", () => {
    expect(leiRegistrationChip(issued)).toBeNull();
    expect(leiRegistrationChip(null)).toBeNull();
    expect(leiRegistrationChip(undefined)).toBeNull();
  });

  it("gives no date where GLEIF publishes none, and reads a batch row's two fields", () => {
    expect(leiRegistrationChip({ status: "RETIRED" })?.label).toBe("LEI retired");
    expect(leiRegistrationChip({ status: "PENDING_TRANSFER" })?.label).toBe("LEI pending transfer");
    expect(leiRegistrationChip({ status: "LAPSED", since: "2026-09-21" })).toEqual({
      label: "LEI lapsed since 21 Sept 2026",
      tone: "context",
      detail: `GLEIF records this LEI as lapsed since 21 Sept 2026. This is ${LEI_NOT_ENTITY_STATUS}.`,
    });
  });

  it("never borrows the register chip's tones or a risk tone", () => {
    for (const status of ["LAPSED", "RETIRED", "MERGED", "ANNULLED", "DUPLICATE"]) {
      expect(leiRegistrationChip({ status })?.tone).toBe("context");
    }
  });
});

describe("the LEI registration row", () => {
  it("states the status beside register status, and says it is not the company's", () => {
    const rows = profileRows(shell({ lei_registration: lapsed }), NAMES);
    const i = rows.findIndex((r) => r.label === "LEI registration");
    expect(rows[i - 1].label).toBe("Register status");
    expect(rows[i]).toEqual({
      label: "LEI registration",
      value: `Lapsed — renewal was due 19 Oct 2017 · ${LEI_NOT_ENTITY_STATUS}`,
      sources: "Source: GLEIF",
    });
    // The register status row is untouched: a lapsed LEI is not a dissolved company.
    expect(rows[i - 1].value).toBe("Active");
  });

  it("states an issued LEI briefly, and omits the row with no status", () => {
    expect(leiRegistrationLine(issued)).toBe("Issued — renews 21 Mar 2027");
    expect(profileRows(shell(), NAMES).some((r) => r.label === "LEI registration")).toBe(false);
    expect(profileRows(shell({ lei_registration: null }), NAMES).some((r) => r.label === "LEI registration")).toBe(false);
  });
});
