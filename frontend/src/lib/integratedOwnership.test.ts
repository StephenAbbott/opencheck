import { describe, expect, it } from "vitest";
import table from "./integratedOwnershipCases.json";
import { computeIntegratedOwnership, routeWords } from "./integratedOwnership";
import { figureText, integratedOwnership, pyRound, shareBounds } from "./ownershipMath";

type Case = {
  name: string;
  as_of: string;
  subject_ids: string[];
  canon?: Record<string, string>;
  statements: Record<string, unknown>[];
  expect: unknown;
};

describe("integrated ownership — the shared case table (Phase 339)", () => {
  for (const c of (table as { cases: Case[] }).cases) {
    it(c.name, () => {
      const got = computeIntegratedOwnership(c.statements, c.subject_ids, c.as_of, c.canon ?? {});
      const proj = got.holders.map((h) => ({
        party: h.party,
        status: h.status,
        kind: h.kind,
        figures: h.figures.map((f) => ({ source_id: f.source_id, text: f.text, route: routeWords(f) })),
      }));
      expect(proj).toEqual(c.expect);
    });
  }
});

describe("ownershipMath — the Phase 338 arithmetic, ported", () => {
  const e = (owner: string, owned: string, pct: number) => ({ owner, owned, low: pct / 100, high: pct / 100 });

  it("counts each share once for a group (GROW: 48%, not 61.5%)", () => {
    const edges = [
      e("S1", "C", 20), e("S2", "C", 20), e("S2", "S1", 60), e("S2", "N1", 50), e("N1", "C", 10),
      e("S3", "N2", 20), e("S4", "N2", 10), e("S3", "S4", 70), e("S4", "S3", 40), e("N2", "C", 10),
    ];
    const r = integratedOwnership(edges, ["S1", "S2", "S3", "S4"], "C");
    expect(r.minimum).toBeCloseTo(48, 6);
    expect(r.maximum).toBeCloseTo(48, 6);
    expect(new Set(r.via)).toEqual(new Set(["N1", "N2"]));
  });

  it("refuses one stake named twice rather than doubling it", () => {
    expect(integratedOwnership([e("Gov", "T", 67), e("Min", "T", 67)], ["Gov", "Min"], "T").reason).toBe(
      "shares_exceed_100",
    );
  });

  it("reads every v0.4 share shape", () => {
    expect(shareBounds({ exact: 67 })).toEqual([0.67, 0.67]);
    expect(shareBounds({ exclusiveMinimum: 75, maximum: 100 })).toEqual([0.75, 1]);
    expect(shareBounds({ minimum: 25, exclusiveMaximum: 50 })).toEqual([0.25, 0.5]);
    expect(shareBounds({ minimum: 10 })).toEqual([0.1, null]);
    expect(shareBounds({})).toBeNull();
    expect(shareBounds(null)).toBeNull();
  });

  it("words a figure as the backend does", () => {
    expect(figureText({ minimum: 67, maximum: 67 })).toBe("67%");
    expect(figureText({ minimum: 50.1, maximum: 67 })).toBe("50.1–67%");
    expect(figureText({ minimum: 24 })).toBe("at least 24%");
    expect(figureText({ minimum: 0, maximum: 10 })).toBe("up to 10%");
    expect(figureText({ minimum: 0 })).toBeNull();
    expect(figureText(null)).toBeNull();
  });

  it("rounds half to even, as Python's round does", () => {
    expect(pyRound(0.125, 2)).toBe(0.12);
    expect(pyRound(0.375, 2)).toBe(0.38);
    expect(pyRound(16.666666, 2)).toBe(16.67);
  });
});
