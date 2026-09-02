/**
 * liveness — browser reader for the Phase 151 register-status grammar.
 * Cases mirror backend/tests/test_liveness.py so both sides read one grammar.
 */
import { describe, expect, it } from "vitest";

import { readRegisterStatus, registerStatusLabel } from "./liveness";

const stmt = (description: string, extra: Record<string, unknown> = {}) => ({
  statementId: "x",
  recordType: "entity",
  recordDetails: { name: "OLD SHELL LIMITED", ...extra },
  source: { description: "UK Companies House" },
  annotations: [
    {
      statementPointerTarget: "/recordDetails",
      motivation: "commenting",
      description,
      createdBy: { name: "OpenCheck" },
    },
  ],
});

describe("readRegisterStatus", () => {
  it("reads a terminal status with date and raw label", () => {
    const s = readRegisterStatus(
      stmt("UK Companies House records this entity as dissolved since 2019-04-03 — register status: “dissolved”.", {
        dissolutionDate: "2019-04-03",
      })
    );
    expect(s).toEqual({ source: "UK Companies House", liveness: "terminal", since: "2019-04-03", raw: "dissolved" });
    expect(registerStatusLabel(s!)).toBe("Dissolved · since 2019-04-03");
  });

  it("reads pending and live, and labels only non-live", () => {
    const pending = readRegisterStatus(
      stmt("UK Companies House records this entity as in a terminal process — register status: “liquidation”.")
    )!;
    expect(pending.liveness).toBe("pending");
    expect(registerStatusLabel(pending)).toBe("Terminal process under way · “liquidation”");
    const live = readRegisterStatus(stmt("GLEIF records this entity as active — register status: “ACTIVE”."))!;
    expect(live).toEqual({ source: "GLEIF", liveness: "live", since: null, raw: "ACTIVE" });
    expect(registerStatusLabel(live)).toBeNull();
  });

  it("handles a source label with dashes and Greek, and a raw label with quotes", () => {
    const s = readRegisterStatus(
      stmt(
        "ΓΕΜΗ — Greek General Commercial Registry (Γενικό Εμπορικό Μητρώο) records this entity as dissolved — register status: “Διαγραφή \"λόγω\" συγχώνευσης”."
      )
    )!;
    expect(s.source.startsWith("ΓΕΜΗ")).toBe(true);
    expect(s.raw).toBe('Διαγραφή "λόγω" συγχώνευσης');
  });

  it("ignores unrelated annotations and falls back to a bare dissolutionDate", () => {
    const other = stmt("Something else entirely.", { dissolutionDate: "2011-06-30" });
    expect(readRegisterStatus(other)).toEqual({
      source: "UK Companies House",
      liveness: "terminal",
      since: "2011-06-30",
      raw: null,
    });
    // The pre-151 sentinel is not a date and reads as nothing.
    expect(readRegisterStatus(stmt("Something else.", { dissolutionDate: "unknown" }))).toBeNull();
    expect(readRegisterStatus({ recordDetails: {} })).toBeNull();
    expect(readRegisterStatus(null)).toBeNull();
  });
});
