import { describe, it, expect } from "vitest";
import {
  signalKind,
  isRiskFinding,
  isContextObservation,
  partitionByKind,
  riskFindingCount,
} from "./signalKind";
import type { RiskSignal } from "./api";

function sig(code: string, kind?: "risk" | "context"): RiskSignal {
  return {
    code,
    confidence: "high",
    summary: `${code} fired`,
    source_id: "gleif",
    hit_id: "h1",
    evidence: {},
    ...(kind ? { kind } : {}),
  };
}

describe("signalKind", () => {
  it("treats a missing kind as risk", () => {
    // Lookup responses cached before the field existed must not suddenly
    // become context and vanish from the risk chip strip.
    expect(signalKind(sig("SANCTIONED"))).toBe("risk");
    expect(isRiskFinding(sig("SANCTIONED"))).toBe(true);
  });

  it("reads an explicit context classification", () => {
    expect(signalKind(sig("NON_EU_JURISDICTION", "context"))).toBe("context");
    expect(isContextObservation(sig("NON_EU_JURISDICTION", "context"))).toBe(true);
    expect(isRiskFinding(sig("NON_EU_JURISDICTION", "context"))).toBe(false);
  });

  it("keeps the two list-based jurisdiction signals as risk findings", () => {
    // Jurisdiction RISK comes only from authoritative maintained lists.
    for (const code of [
      "FATF_BLACK_LIST",
      "FATF_GREY_LIST",
      "EU_HIGH_RISK_THIRD_COUNTRY",
    ]) {
      expect(isRiskFinding(sig(code, "risk"))).toBe(true);
    }
  });

  it("partitions while preserving order within each group", () => {
    const [risk, context] = partitionByKind([
      sig("SANCTIONED", "risk"),
      sig("NON_EU_JURISDICTION", "context"),
      sig("FATF_GREY_LIST", "risk"),
    ]);
    expect(risk.map((s) => s.code)).toEqual(["SANCTIONED", "FATF_GREY_LIST"]);
    expect(context.map((s) => s.code)).toEqual(["NON_EU_JURISDICTION"]);
  });

  it("returns empty groups rather than throwing on an empty input", () => {
    expect(partitionByKind([])).toEqual([[], []]);
  });
});

describe("riskFindingCount", () => {
  const one = (code: string, over: Partial<RiskSignal> = {}): RiskSignal => ({
    code,
    confidence: "high",
    summary: "",
    source_id: "opensanctions",
    hit_id: "h",
    evidence: {},
    ...over,
  });

  it("counts findings, not signals", () => {
    // The risk layer emits one signal per matching hit, so one finding can
    // arrive three times. FullCheck's network-risk line said "9 signals" one
    // screen under a verdict strip saying "4".
    expect(
      riskFindingCount([
        one("OFFSHORE_LEAKS", { hit_id: "a" }),
        one("OFFSHORE_LEAKS", { hit_id: "b" }),
        one("OFFSHORE_LEAKS", { hit_id: "c" }),
      ])
    ).toBe(1);
  });

  it("leaves structural context out of a risk count", () => {
    expect(
      riskFindingCount([
        one("SANCTIONED"),
        one("NON_EU_JURISDICTION", { kind: "context" }),
      ])
    ).toBe(1);
  });

  it("is zero for nothing", () => {
    expect(riskFindingCount([])).toBe(0);
  });
});
