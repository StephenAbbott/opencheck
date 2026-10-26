import { describe, expect, it } from "vitest";

import type { SourceHit } from "./api";
import {
  ACCOUNTS_SOURCE_NOTE,
  brregAccountsFacts,
  brregAccountsOf,
  brregAccountsPdf,
  type BrregAccounts,
} from "./brregAccounts";

// The summary the backend builds from Equinor ASA's real answer of
// 26 Oct 2026 (tests/data/brreg_accounts_live.json, summarise()).
const EQUINOR: BrregAccounts = {
  status: "filed",
  period_start: "2025-01-01",
  period_end: "2025-12-31",
  year: 2025,
  journal_number: "2026635024",
  group_accounts: true,
  group_parent: true,
  audit_opted_out: false,
  not_audited: false,
  liquidation_accounts: false,
  small_company: false,
  currency: "USD",
  rules: "forenkletAnvendelseIFRS",
  pdf_url: "https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/923609016/2025",
  source_url: "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
};

function hit(sourceId: string, accounts: unknown): SourceHit {
  return {
    source_id: sourceId,
    hit_id: "923609016",
    kind: "entity",
    name: "EQUINOR ASA",
    summary: "NO-ORGNR 923609016",
    identifiers: {},
    is_stub: false,
    raw: { navn: "EQUINOR ASA", accounts },
  } as SourceHit;
}

describe("brregAccounts", () => {
  it("reads the summary off a brreg hit only", () => {
    expect(brregAccountsOf(hit("brreg", EQUINOR))).toEqual(EQUINOR);
    expect(brregAccountsOf(hit("cvr_denmark", EQUINOR))).toBeNull();
    expect(brregAccountsOf(hit("brreg", null))).toBeNull();
  });

  it("lists the facts of the latest period, never a figure", () => {
    const facts = brregAccountsFacts(EQUINOR);
    expect(facts).toEqual([
      "Period 1 Jan 2025 – 31 Dec 2025",
      "Journal no. 2026635024",
      "Reported in USD",
      "simplified IFRS",
      "Filed as a parent company",
    ]);
    // Equinor's figures run to twelve digits (105828000000); none may appear.
    expect(facts.join(" ")).not.toMatch(/\d{11,}|revenue|turnover|result/i);
  });

  it("links the register's PDF copy when it lists one", () => {
    expect(brregAccountsPdf(EQUINOR)).toEqual({
      url: EQUINOR.pdf_url,
      label: "Filed annual accounts 2025 (PDF)",
    });
    expect(brregAccountsPdf({ ...EQUINOR, pdf_url: null })).toBeNull();
  });

  it("says nothing extra for the statuses the sentence already words", () => {
    for (const status of ["none", "unsupported", "unavailable"] as const) {
      const a: BrregAccounts = { status, format: "BANK", format_words: "bank" };
      expect(brregAccountsFacts(a)).toEqual([]);
      expect(brregAccountsPdf(a)).toBeNull();
    }
  });

  it("names its source and says no figures are shown", () => {
    expect(ACCOUNTS_SOURCE_NOTE).toMatch(/Regnskapsregisteret/);
    expect(ACCOUNTS_SOURCE_NOTE).toMatch(/not the figures/);
  });
});
