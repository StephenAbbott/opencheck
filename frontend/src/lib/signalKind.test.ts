import { describe, it, expect } from "vitest";
import {
  signalKind,
  isRiskFinding,
  isContextObservation,
  partitionByKind,
  riskFindingCount,
  groupNetworkSignals,
  groupSignalsByCode,
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

describe("groupNetworkSignals (Phase 250)", () => {
  // Shell PLC's FullCheck after one layer, 25 Sept 2026: twelve instances
  // drawn as twelve chips under a sentence that said five.
  const sig = (code: string, confidence: "high" | "medium" | "low", kind?: "context") =>
    ({ code, confidence, summary: code, ...(kind ? { kind } : {}) }) as unknown as RiskSignal;
  const shell = [
    sig("GLEIF_REPORTING_EXCEPTION", "high", "context"),
    sig("NON_EU_JURISDICTION", "low", "context"),
    sig("NON_EU_JURISDICTION", "low", "context"),
    sig("FATF_GREY_LIST", "medium"),
    sig("RELATED_SANCTIONS_LINKED", "high"),
    sig("RELATED_DEBARMENT", "high"),
    sig("RELATED_DEBARMENT", "medium"),
    sig("RELATED_EXPORT_RISK", "medium"),
    sig("OFFSHORE_LEAKS", "medium"),
    sig("OFFSHORE_LEAKS", "medium"),
    sig("NON_EU_JURISDICTION", "medium", "context"),
    sig("OFFSHORE_LEAKS", "medium"),
  ];

  it("draws as many risk chips as the sentence counts", () => {
    const { risk, context } = groupNetworkSignals(shell);
    expect(risk).toHaveLength(riskFindingCount(shell));
    expect(risk.map((g) => g.code)).toEqual([
      "FATF_GREY_LIST",
      "RELATED_SANCTIONS_LINKED",
      "RELATED_DEBARMENT",
      "RELATED_EXPORT_RISK",
      "OFFSHORE_LEAKS",
    ]);
    expect(context.map((g) => [g.code, g.signals.length])).toEqual([
      ["GLEIF_REPORTING_EXCEPTION", 1],
      ["NON_EU_JURISDICTION", 3],
    ]);
  });

  it("leads a group with its most confident instance and keeps every one", () => {
    const [nonEu] = groupSignalsByCode(shell.filter((s) => s.code === "NON_EU_JURISDICTION"));
    expect(nonEu.lead.confidence).toBe("medium");
    expect(nonEu.signals).toHaveLength(3);
    const [debar] = groupSignalsByCode(shell.filter((s) => s.code === "RELATED_DEBARMENT"));
    expect(debar.lead.confidence).toBe("high");
  });
});
