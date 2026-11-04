import { describe, expect, it } from "vitest";

import type { RiskSignal } from "./api";
import {
  DISCREPANCY_NOTE_BUSY,
  DISCREPANCY_NOTE_DESCRIPTION,
  DISCREPANCY_NOTE_LABEL,
  DISCREPANCY_NOTE_UNAVAILABLE,
  discrepancyNoteAvailable,
  discrepancyNoteItem,
} from "./discrepancyNote";

function consistency(items: unknown[]): RiskSignal {
  return {
    code: "RECORD_CONSISTENCY",
    kind: "context",
    confidence: "high",
    summary: "Two sources describe this entity.",
    source_id: "opencheck",
    hit_id: "X",
    evidence: { items },
  } as unknown as RiskSignal;
}

const DISAGREE = {
  field: "liveness",
  relation: "disagree",
  sources: ["companies_house", "gleif"],
  statement_ids: ["a", "b"],
  values: ["terminal", "live"],
  said: ["dissolved (since 3 Apr 2019)", "ACTIVE"],
};
const AGREE = { ...DISAGREE, field: "jurisdiction", relation: "agree", values: ["GB", "GB"], said: ["GB", "GB"] };

describe("discrepancyNoteAvailable", () => {
  it("is true only when a shown disagreement exists", () => {
    expect(discrepancyNoteAvailable([consistency([DISAGREE, AGREE])])).toBe(true);
    expect(discrepancyNoteAvailable([consistency([AGREE])])).toBe(false);
    expect(discrepancyNoteAvailable([consistency([])])).toBe(false);
    expect(discrepancyNoteAvailable([])).toBe(false);
  });

  it("reads only the Records compared signal", () => {
    const other = { ...consistency([DISAGREE]), code: "SANCTIONED" } as RiskSignal;
    expect(discrepancyNoteAvailable([other])).toBe(false);
  });
});

describe("discrepancyNoteItem", () => {
  it("is enabled with its description when a note can be made", () => {
    expect(discrepancyNoteItem([consistency([DISAGREE])], false)).toEqual({
      label: DISCREPANCY_NOTE_LABEL,
      description: DISCREPANCY_NOTE_DESCRIPTION,
      disabled: false,
    });
  });

  it("is disabled with its reason when nothing qualifies", () => {
    expect(discrepancyNoteItem([consistency([AGREE])], false)).toEqual({
      label: DISCREPANCY_NOTE_LABEL,
      description: DISCREPANCY_NOTE_UNAVAILABLE,
      disabled: true,
    });
  });

  it("says it is working while busy", () => {
    const item = discrepancyNoteItem([consistency([DISAGREE])], true);
    expect(item.label).toBe(DISCREPANCY_NOTE_BUSY);
    expect(item.disabled).toBe(true);
  });
});
