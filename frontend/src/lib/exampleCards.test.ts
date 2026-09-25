import { describe, it, expect } from "vitest";

import { EXAMPLE_LEIS } from "../components/HomePanels";
import { RISK_PRESENTATION } from "../components/risk/RiskChip";
import { SIGNAL_STYLE } from "./graphStyle";
import { cardChips, moreFindingsLabel, rankCardSignals, MAX_CARD_CHIPS } from "./exampleCards";

describe("cardChips", () => {
  it("shows the worst three and counts the rest", () => {
    const rosneft = EXAMPLE_LEIS.find((e) => e.lei === "253400JT3MQWNDKMJE44");
    const { shown, more } = cardChips(rosneft?.signals);
    expect(shown.map((s) => s.code)).toEqual([
      "SANCTIONED",
      "RELATED_SANCTIONED",
      "SANCTIONED_SECURITY",
    ]);
    expect(more).toBe((rosneft?.signals?.length ?? 0) - MAX_CARD_CHIPS);
    expect(moreFindingsLabel(more)).toBe(`+${more} more findings`);
  });

  it("ranks by severity, then confidence, then subject before related", () => {
    const ranked = rankCardSignals([
      { code: "OFFSHORE_LEAKS", confidence: "high" },
      { code: "RELATED_PEP", confidence: "medium" },
      { code: "PEP", confidence: "medium" },
      { code: "RELATED_PEP", confidence: "high" },
    ]);
    expect(ranked.map((s) => `${s.code}:${s.confidence}`)).toEqual([
      "RELATED_PEP:high",
      "PEP:medium",
      "RELATED_PEP:medium",
      "OFFSHORE_LEAKS:high",
    ]);
  });

  it("says nothing when everything fits", () => {
    expect(cardChips([{ code: "OFFSHORE_LEAKS", confidence: "high" }]).more).toBe(0);
    expect(moreFindingsLabel(0)).toBe("");
    expect(moreFindingsLabel(1)).toBe("+1 more finding");
    expect(cardChips(undefined)).toEqual({ shown: [], more: 0 });
  });
});

describe("EXAMPLE_LEIS", () => {
  it("lists each risk code once, with a chip label and a graph severity", () => {
    for (const ex of EXAMPLE_LEIS) {
      const codes = (ex.signals ?? []).map((s) => s.code);
      expect(new Set(codes).size, ex.name).toBe(codes.length);
      for (const code of codes) {
        expect(RISK_PRESENTATION[code], `${ex.name}: ${code}`).toBeDefined();
        expect(SIGNAL_STYLE[code], `${ex.name}: ${code}`).toBeDefined();
      }
    }
  });

  it("carries no context codes — a card chip reads as a finding", () => {
    const context = new Set([
      "NON_EU_JURISDICTION",
      "GLEIF_REPORTING_EXCEPTION",
      "RELATED_PEP_SUBJECT_ROLE",
    ]);
    for (const ex of EXAMPLE_LEIS) {
      for (const s of ex.signals ?? []) expect(context.has(s.code), `${ex.name}: ${s.code}`).toBe(false);
    }
  });
});
