import { describe, expect, it } from "vitest";
import { citeGroupDescription, groupCitations, type CiteLike } from "./citations";

const cite = (over: Partial<CiteLike> = {}): CiteLike => ({
  id: "f1",
  kind: "fact",
  label: "GLEIF",
  sourceId: "gleif",
  statementId: null,
  confidence: "medium",
  ...over,
});

describe("groupCitations", () => {
  it("turns twenty-eight identical chips into one", () => {
    // Live on Shell: one claim cited 28 GLEIF facts, and the chips stacked one
    // per line down a phone screen. Every one of them scrolled to the same
    // card.
    const cites = Array.from({ length: 28 }, (_, i) => cite({ id: `f${i}` }));
    const groups = groupCitations(cites);
    expect(groups).toHaveLength(1);
    expect(groups[0].count).toBe(28);
    expect(groups[0].label).toBe("GLEIF");
    expect(groups[0].cites).toHaveLength(28);
  });

  it("keeps distinct sources distinct, in first-seen order", () => {
    const groups = groupCitations([
      cite({ id: "a", label: "OpenCorporates", sourceId: "opencorporates" }),
      cite({ id: "b", label: "UK Companies House", sourceId: "companies_house" }),
      cite({ id: "c", label: "OpenCorporates", sourceId: "opencorporates" }),
    ]);
    expect(groups.map((g) => g.label)).toEqual([
      "OpenCorporates",
      "UK Companies House",
    ]);
    expect(groups.map((g) => g.count)).toEqual([2, 1]);
  });

  it("does not merge a limitation into a source", () => {
    // Gaps carry no source id, and a "Limitation" chip is a different claim
    // from a citation — grouping on sourceId would have collapsed every gap
    // together with every other source-less citation.
    const groups = groupCitations([
      cite({ id: "a" }),
      cite({ id: "g1", kind: "gap", label: "Limitation", sourceId: null, confidence: null }),
      cite({ id: "g2", kind: "gap", label: "Limitation", sourceId: null, confidence: null }),
    ]);
    expect(groups.map((g) => [g.kind, g.count])).toEqual([
      ["fact", 1],
      ["gap", 2],
    ]);
  });

  it("does not merge a risk citation into a fact citation from the same source", () => {
    const groups = groupCitations([
      cite({ id: "f", kind: "fact", label: "OpenSanctions" }),
      cite({ id: "r", kind: "risk", label: "OpenSanctions" }),
    ]);
    expect(groups).toHaveLength(2);
  });

  it("takes the WEAKEST confidence in the group", () => {
    // The glyph is a claim about the whole group. One high-confidence record
    // among twenty-seven medium ones must not lend the group a ●.
    const groups = groupCitations([
      cite({ id: "a", confidence: "high" }),
      cite({ id: "b", confidence: "medium" }),
      cite({ id: "c", confidence: "high" }),
    ]);
    expect(groups[0].confidence).toBe("medium");
  });

  it("ignores a missing confidence rather than treating it as the weakest", () => {
    // Absent is not "low" — a citation that carries no level says nothing
    // about the level, and downgrading the group on its account would be an
    // inference.
    const groups = groupCitations([
      cite({ id: "a", confidence: "high" }),
      cite({ id: "b", confidence: null }),
    ]);
    expect(groups[0].confidence).toBe("high");
  });

  it("says nothing about nothing", () => {
    expect(groupCitations([])).toEqual([]);
  });
});

describe("citeGroupDescription", () => {
  it("names the count only when there is one to name", () => {
    const [one] = groupCitations([cite()]);
    expect(citeGroupDescription(one)).toBe("GLEIF");
    const [many] = groupCitations([cite({ id: "a" }), cite({ id: "b" })]);
    expect(citeGroupDescription(many)).toBe("GLEIF, 2 records");
  });
});
