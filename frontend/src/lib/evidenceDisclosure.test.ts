import { describe, expect, it } from "vitest";
import {
  EVIDENCE_COLLAPSE_THRESHOLD,
  EVIDENCE_PREVIEW_COUNT,
  citedSourceLabels,
  shouldCollapseEvidence,
} from "./evidenceDisclosure";

describe("shouldCollapseEvidence", () => {
  it("collapses only above the threshold", () => {
    expect(shouldCollapseEvidence(EVIDENCE_COLLAPSE_THRESHOLD, false)).toBe(false);
    expect(shouldCollapseEvidence(EVIDENCE_COLLAPSE_THRESHOLD + 1, false)).toBe(true);
    expect(shouldCollapseEvidence(0, false)).toBe(false);
  });

  it("never collapses while sign-off is active — the disposition controls must stay visible", () => {
    expect(shouldCollapseEvidence(15, true)).toBe(false);
  });

  it("preview is smaller than the threshold, so collapsing always saves space", () => {
    expect(EVIDENCE_PREVIEW_COUNT).toBeLessThan(EVIDENCE_COLLAPSE_THRESHOLD);
  });
});

describe("citedSourceLabels", () => {
  const packet = {
    facts: [
      { id: "f1", source_name: "GLEIF" },
      { id: "f2", source_name: "OpenSanctions" },
      { id: "f3", source_name: "GLEIF" },
    ],
    risks: [{ id: "r1", source_name: "OpenSanctions" }, { id: "r2", source_name: "Wikidata" }],
  };

  it("deduplicates across facts and risks, preserving first-seen order", () => {
    const claims = [
      { fact_ids: ["f1", "r1"] },
      { fact_ids: ["f2", "f3"] },
      { fact_ids: ["r2"] },
    ];
    expect(citedSourceLabels(claims, packet)).toEqual(["GLEIF", "OpenSanctions", "Wikidata"]);
  });

  it("ignores gap ids and unknown ids — gaps are limitations, not sources", () => {
    const claims = [{ fact_ids: ["g1", "nope", "f1"] }];
    expect(citedSourceLabels(claims, packet)).toEqual(["GLEIF"]);
  });

  it("returns empty for no claims", () => {
    expect(citedSourceLabels([], packet)).toEqual([]);
  });
});
