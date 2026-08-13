import { describe, it, expect } from "vitest";
import { RISK_PRESENTATION } from "./RiskChip";
import { SIGNAL_STYLE } from "../BODSGraph";

/**
 * The two presentation maps are hand-maintained mirrors of the backend's
 * signal codes, in two separate files. A backend code with no entry renders
 * as a bare, unstyled code string; a graph badge with no chip is worse
 * still — the node lights up with nothing to explain it.
 */
describe("risk signal presentation maps", () => {
  it("gives every graph badge a matching chip", () => {
    const missing = Object.keys(SIGNAL_STYLE).filter(
      (code) => !(code in RISK_PRESENTATION),
    );
    expect(missing).toEqual([]);
  });

  it("covers the whole sanction family", () => {
    // Emitted by risk.py (subject) and cross_check.py / openaleph_check.py
    // (related parties). SANCTIONS_CONTROLLED must outrank SANCTIONS_LINKED
    // in the graph's worst-severity-wins badge stacking: being owned by a
    // designated party is a stronger fact than standing next to one.
    for (const code of [
      "SANCTIONED",
      "SANCTIONS_CONTROLLED",
      "SANCTIONS_LINKED",
      "RELATED_SANCTIONED",
      "RELATED_SANCTIONS_CONTROLLED",
      "RELATED_SANCTIONS_LINKED",
    ]) {
      expect(RISK_PRESENTATION[code], `no chip for ${code}`).toBeDefined();
      expect(SIGNAL_STYLE[code], `no graph badge for ${code}`).toBeDefined();
    }

    expect(SIGNAL_STYLE.SANCTIONED.severity).toBeGreaterThan(
      SIGNAL_STYLE.SANCTIONS_CONTROLLED.severity,
    );
    expect(SIGNAL_STYLE.SANCTIONS_CONTROLLED.severity).toBeGreaterThan(
      SIGNAL_STYLE.DEBARMENT.severity,
    );
    expect(SIGNAL_STYLE.DEBARMENT.severity).toBeGreaterThan(
      SIGNAL_STYLE.SANCTIONS_LINKED.severity,
    );
  });
});
