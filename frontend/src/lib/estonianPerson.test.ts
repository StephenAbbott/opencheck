import { describe, expect, it } from "vitest";

import type { AppointmentItem, PersonAppointmentsResponse } from "./api";
import {
  companiesSummary,
  estonianPersonHash,
  estonianPersonHashOf,
  groupByCompany,
  positionLine,
  registerCompanyUrl,
} from "./estonianPerson";

const HASH = "77a468f2-248a-5e41-8356-c975af9613a5";

function item(over: Partial<AppointmentItem>): AppointmentItem {
  return {
    company_name: "007 Agent & Partners OÜ",
    company_number: "16752073",
    company_status: "Entered in the register",
    role: "Shareholder (osanik)",
    appointed_on: null,
    resigned_on: null,
    ...over,
  };
}

function response(over: Partial<PersonAppointmentsResponse>): PersonAppointmentsResponse {
  return {
    officer_id: HASH,
    name: "Veronika Oškina",
    birth_date: null,
    is_stub: false,
    total_results: 2,
    active_count: 2,
    appointments: [],
    bods: [],
    attribution: "Estonian e-Business Register",
    caveat: "…",
    scheme: "EE-ARIREGISTER-HASH",
    source_id: "ariregister",
    as_of: "2026-10-16",
    company_count: 2,
    truncated: false,
    ...over,
  };
}

describe("the register's person key", () => {
  it("is read from a grouped person's identifier keys", () => {
    expect(estonianPersonHash(["wikidata:q1", `ee-ariregister-hash:${HASH}`])).toBe(HASH);
    expect(estonianPersonHash(["ee-ariregister-hash:not-a-uuid"])).toBeNull();
    expect(estonianPersonHash([])).toBeNull();
  });

  it("is read from a person statement's identifying annotation, never a name", () => {
    const stmt = {
      annotations: [{
        motivation: "identifying",
        description: `Identifier ${HASH} (scheme EE-ARIREGISTER-HASH; Estonian e-Business Register person hash). BODS keeps …`,
      }],
    };
    expect(estonianPersonHashOf(stmt)).toBe(HASH);
    expect(estonianPersonHashOf({ recordDetails: { names: [{ fullName: "Veronika Oškina" }] } })).toBeNull();
  });
});

describe("the list", () => {
  it("says role, since and share in the register's terms", () => {
    expect(positionLine(item({ appointed_on: "2010-03-10", share_percent: "60.00" })))
      .toBe("Shareholder (osanik) · since 10 March 2010 · 60.00%");
    expect(positionLine(item({ share_size: "2500.00", share_currency: "EUR" })))
      .toBe("Shareholder (osanik) · 2500.00 EUR");
    expect(positionLine(item({ role: null }))).toBe("Listed");
  });

  it("groups a person's rows by company, in the server's order", () => {
    const groups = groupByCompany([
      item({ register_list: "registry_card", role: "Board member (juhatuse liige)" }),
      item({ company_number: "10421629", company_name: "Eesti Energia AS", register_list: "off_card" }),
      item({ register_list: "shareholders" }),
    ]);
    expect(groups.map((g) => [g.code, g.positions.length])).toEqual([["16752073", 2], ["10421629", 1]]);
  });

  it("states absence and a missing index in the same voice as presence", () => {
    expect(companiesSummary(response({}))).toBe(
      "Named on the current lists of 2 companies in the e-Business Register as of 16 October 2026, this one included.",
    );
    expect(companiesSummary(response({ company_count: 0 }))).toMatch(/on no company as of 16 October 2026/);
    expect(companiesSummary(response({ is_stub: true }))).toMatch(/index is not loaded/);
    expect(companiesSummary(response({ truncated: true, total_results: 600, appointments: [item({})] })))
      .toMatch(/The first 1 of 600 entries are shown\./);
  });

  it("links only a registry code to the register", () => {
    expect(registerCompanyUrl("16752073")).toBe("https://ariregister.rik.ee/eng/company/16752073");
    expect(registerCompanyUrl("Unnamed")).toBeNull();
  });
});
