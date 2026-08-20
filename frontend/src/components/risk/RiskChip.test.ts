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
      "COUNTER_SANCTIONED",
      "SANCTIONS_CONTROLLED",
      "SANCTIONS_LINKED",
      "RELATED_SANCTIONED",
      "RELATED_COUNTER_SANCTIONED",
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
    // A counter-designation ranks below plain adjacency. It is a direct
    // listing structurally, but by a regime the reader owes no obligation
    // to — so it must never win the graph's worst-severity-wins stacking
    // over a signal that carries an actual compliance consequence.
    expect(SIGNAL_STYLE.SANCTIONS_LINKED.severity).toBeGreaterThan(
      SIGNAL_STYLE.COUNTER_SANCTIONED.severity,
    );
    expect(SIGNAL_STYLE.COUNTER_SANCTIONED.severity).toEqual(
      SIGNAL_STYLE.RELATED_COUNTER_SANCTIONED.severity,
    );
  });

  it("covers the whole export-control family", () => {
    // Emitted by risk.py (subject) and cross_check.py / openaleph_check.py
    // (related parties) since Phase 118. An export-control listing is a
    // restriction on the party itself, so it must outrank debarment and
    // plain sanction adjacency in the graph's worst-severity-wins stacking,
    // while staying below sanction control (ownership by a designated
    // party). Adjacency ties the sanctions-linked tier; "Trade risk"
    // (export.risk) sits one below that.
    for (const code of [
      "EXPORT_CONTROLLED",
      "EXPORT_CONTROL_LINKED",
      "EXPORT_RISK",
      "RELATED_EXPORT_CONTROLLED",
      "RELATED_EXPORT_CONTROL_LINKED",
      "RELATED_EXPORT_RISK",
    ]) {
      expect(RISK_PRESENTATION[code], `no chip for ${code}`).toBeDefined();
      expect(SIGNAL_STYLE[code], `no graph badge for ${code}`).toBeDefined();
    }

    expect(SIGNAL_STYLE.SANCTIONS_CONTROLLED.severity).toBeGreaterThan(
      SIGNAL_STYLE.EXPORT_CONTROLLED.severity,
    );
    expect(SIGNAL_STYLE.EXPORT_CONTROLLED.severity).toBeGreaterThan(
      SIGNAL_STYLE.DEBARMENT.severity,
    );
    expect(SIGNAL_STYLE.EXPORT_CONTROL_LINKED.severity).toEqual(
      SIGNAL_STYLE.SANCTIONS_LINKED.severity,
    );
    expect(SIGNAL_STYLE.EXPORT_CONTROL_LINKED.severity).toBeGreaterThan(
      SIGNAL_STYLE.EXPORT_RISK.severity,
    );
    // Related-party variants rank identically to the subject codes.
    for (const code of [
      "EXPORT_CONTROLLED",
      "EXPORT_CONTROL_LINKED",
      "EXPORT_RISK",
    ]) {
      expect(SIGNAL_STYLE[code].severity).toEqual(
        SIGNAL_STYLE[`RELATED_${code}`].severity,
      );
    }
  });

  it("keeps counter-sanctions out of the sanctions colour ramp", () => {
    // The whole point of the split: "Counter-sanctioned" must not read as a
    // shade of "Sanctioned". Both chips are rose-family; these must not be.
    for (const code of ["COUNTER_SANCTIONED", "RELATED_COUNTER_SANCTIONED"]) {
      const classes = RISK_PRESENTATION[code].classes;
      expect(classes, `${code} must not be rose`).not.toMatch(/rose/);
      expect(classes, `${code} must not be amber`).not.toMatch(/amber/);
    }
    expect(RISK_PRESENTATION.COUNTER_SANCTIONED.label).toEqual(
      "Counter-sanctioned",
    );
  });
});
