/**
 * leads — what the retired-LEI leads block says (Phase 308).
 *
 * Every sentence is a claim about what is NOT asserted, so each is pinned:
 * the block renders only for an ended company with no successor named, a
 * failure never reads as "nothing similar exists", and every candidate is a
 * name match only.
 */
import { describe, expect, it } from "vitest";

import type { LeadsResponse, SubjectProfile } from "./api";
import {
  LEADS_HEADING,
  NO_CANDIDATES,
  NO_PARENT,
  UNAVAILABLE_LINES,
  candidateLine,
  leadsEligible,
  leadsLines,
  parentLine,
  unavailableLine,
} from "./leads";

const BARRICK = "5493002CWGHR03YL8X75";
const MINING = "0O4KBQCJZX82UKGCBV73";

const ended = (overrides: Partial<SubjectProfile> = {}): SubjectProfile => ({
  legal_form: null,
  register_status: {
    liveness: "terminal",
    since: "2025-11-26",
    raw: "INACTIVE",
    source_id: "gleif",
    sources: ["gleif"],
    independent_sources: 1,
    other_values: [],
  },
  founding_date: null,
  registered_address: null,
  jurisdiction: "CA-ON",
  lei_registration: null,
  lei_successor: {
    relation: "none",
    named: [],
    event: { type: "DISSOLUTION", status: "COMPLETED", effective_day: "2025-11-26" },
    chain: [],
    chain_source: null,
    chain_complete: true,
    hops: 0,
    source_id: "gleif",
    sentence: "GLEIF names no successor on this LEI record.",
  },
  statement_ids: [],
  ...overrides,
});

const response = (overrides: Partial<LeadsResponse> = {}): LeadsResponse => ({
  lei: BARRICK,
  searched_name: "BARRICK GOLD INC.",
  parent: null,
  candidates: [
    {
      lei: MINING,
      name: "BARRICK MINING CORPORATION",
      jurisdiction: "CA-BC",
      registration_status: "ISSUED",
      match: "all_tokens",
      matched_name: "Barrick Gold Corporation",
      matched_name_type: "PREVIOUS_LEGAL_NAME",
      name_only: true,
    },
  ],
  gleif_unavailable_reason: null,
  note: "Leads, not a successor.",
  ...overrides,
});

describe("leadsEligible", () => {
  it("asks for leads only for an ended company GLEIF names no successor for", () => {
    expect(leadsEligible(ended())).toBe(true);
    expect(leadsEligible(null)).toBe(false);
    expect(leadsEligible(ended({ lei_successor: null }))).toBe(false);
    expect(leadsEligible(ended({ lei_successor: undefined }))).toBe(false);
    const live = ended();
    live.register_status = { ...live.register_status!, liveness: "live", raw: "ACTIVE" };
    expect(leadsEligible(live)).toBe(false);
    const withSuccessor = ended();
    withSuccessor.lei_successor = {
      ...withSuccessor.lei_successor!,
      relation: "successor",
      named: [{ lei: MINING, name: "X" }],
    };
    expect(leadsEligible(withSuccessor)).toBe(false);
    const duplicate = ended();
    duplicate.lei_successor = { ...duplicate.lei_successor!, relation: "duplicate", named: [{ lei: MINING, name: "X" }] };
    expect(leadsEligible(duplicate)).toBe(false);
  });
});

describe("the lines", () => {
  it("names the heading as leads, not a successor", () => {
    expect(LEADS_HEADING).toContain("not a successor");
  });

  it("says which name a candidate matched on, and links to it", () => {
    const lines = leadsLines(response());
    expect(lines.candidates).toEqual([
      {
        line: "BARRICK MINING CORPORATION · CA-BC · LEI issued — every word of the name, as its former legal name “Barrick Gold Corporation”",
        href: `/?lei=${MINING}`,
      },
    ]);
    expect(lines.candidatesNote).toBeNull();
    expect(lines.parent).toBe(NO_PARENT);
    expect(lines.parentHref).toBeNull();
  });

  it("says a failure in words — never an empty list", () => {
    for (const reason of ["held_for_lookups", "rate_limited", "unreachable", "something_new"]) {
      const lines = leadsLines(response({ candidates: [], gleif_unavailable_reason: reason }));
      expect(lines.candidatesNote).toBe(UNAVAILABLE_LINES[reason] ?? UNAVAILABLE_LINES.unreachable);
      expect(lines.candidatesNote).not.toBe(NO_CANDIDATES);
    }
    expect(unavailableLine(null)).toBeNull();
    expect(leadsLines(response({ candidates: [] })).candidatesNote).toBe(NO_CANDIDATES);
  });

  it("states the parent as last filed, with its standing", () => {
    const p = {
      lei: MINING,
      name: "BARRICK MINING CORPORATION",
      entity_status: "ACTIVE",
      registration_status: "ISSUED",
      relationship_status: "INACTIVE",
      standing: false,
    };
    expect(parentLine(p)).toBe(
      "BARRICK MINING CORPORATION (0O4KBQCJZX82UKGCBV73) · entity active · relationship no longer served by GLEIF",
    );
    expect(parentLine({ ...p, standing: true })).toBe("BARRICK MINING CORPORATION (0O4KBQCJZX82UKGCBV73) · entity active");
    expect(parentLine({ ...p, name: null, entity_status: null, standing: true })).toBe(MINING);
    expect(leadsLines(response({ parent: p })).parentHref).toBe(`/?lei=${MINING}`);
  });

  it("words every tier, and an unknown one as a similar name", () => {
    const base = response().candidates[0];
    expect(candidateLine({ ...base, match: "exact", matched_name: null, matched_name_type: null })).toContain("— same name");
    expect(candidateLine({ ...base, match: "same_name", matched_name: null, matched_name_type: null })).toContain(
      "— same name, different legal form",
    );
    expect(candidateLine({ ...base, match: "distinctive_tokens", matched_name: null, matched_name_type: null })).toContain(
      "— the distinctive words of the name",
    );
    expect(candidateLine({ ...base, match: "other", matched_name: null, matched_name_type: null })).toContain("— a similar name");
    expect(candidateLine({ ...base, matched_name: "X", matched_name_type: "TRADING_OR_OPERATING_NAME" })).toContain(
      "as its trading or operating name “X”",
    );
  });
});
