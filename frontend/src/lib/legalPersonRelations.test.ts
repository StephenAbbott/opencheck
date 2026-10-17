import { describe, expect, it } from "vitest";

import type { LegalPersonRelationsResponse } from "./api";
import {
  companyStatusNote,
  estonianRegistryCodeOf,
  relationLine,
  relationsSummary,
} from "./legalPersonRelations";

const base: LegalPersonRelationsResponse = {
  code: "10421629",
  country: "EST",
  status: "ok",
  total: 2,
  truncated: false,
  retrieved_at: "2026-10-17T10:00:00+00:00",
  attribution: "e-Business Register",
  caveat: "…",
  companies: [
    {
      name: "Enefit OÜ", code: "11184032", status_code: "R", status: "Entered in the register",
      current: true, relations: [],
    },
    {
      name: "Osaühing Pogi", code: "10078240", status_code: "K", status: "Deleted",
      current: false, relations: [],
    },
  ],
};

describe("legal-person relations", () => {
  it("reads the Estonian registry code off an entity statement's identifiers", () => {
    expect(estonianRegistryCodeOf({ recordDetails: { identifiers: [{ scheme: "EE-ARIREGISTER", id: "10421629" }] } })).toBe("10421629");
    expect(estonianRegistryCodeOf({ recordDetails: { identifiers: [{ scheme: "XI-LEI", id: "10421629" }] } })).toBeNull();
    expect(estonianRegistryCodeOf({ recordDetails: { identifiers: [{ scheme: "EE-ARIREGISTER", id: "123" }] } })).toBeNull();
  });

  it("words a relation with its dates and share", () => {
    expect(relationLine({ role: "Shareholder (osanik)", since: "2009-10-08", until: "2012-02-07", ended: true, founder: false, share_percent: "55" }))
      .toBe("Shareholder (osanik) · 8 October 2009 – 7 February 2012 · 55%");
    expect(relationLine({ role: "Founder (asutaja)", since: "2009-10-05", ended: false, founder: true, share_size: "22000", share_currency: "Estonian Kroon" }))
      .toBe("Founder (asutaja) · at founding, 5 October 2009 · 22000 Estonian Kroon");
  });

  it("says a deleted company is deleted and a live one nothing", () => {
    expect(companyStatusNote(base.companies[1])).toBe("Deleted");
    expect(companyStatusNote(base.companies[0])).toBeNull();
  });

  it("counts current and deleted companies, and never reads an unread list as none", () => {
    expect(relationsSummary(base)).toBe("Recorded in 2 companies (1 with a current role, 1 deleted), read on 17 October 2026.");
    expect(relationsSummary({ ...base, status: "unavailable", detail: "the register's query allowance for today is spent", companies: [], total: 0 }))
      .toContain("This is not a finding that there are none");
    expect(relationsSummary({ ...base, status: "not_configured", companies: [], total: 0 })).toContain("not configured");
    expect(relationsSummary({ ...base, total: 8562, truncated: true })).toContain("The first 2 are shown.");
  });
});
