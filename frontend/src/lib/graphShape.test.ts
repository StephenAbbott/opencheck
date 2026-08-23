import { describe, expect, it } from "vitest";
import { depthPhrase, networkSummary } from "./graphShape";

const shape = (over: Partial<Parameters<typeof networkSummary>[0] & object> = {}) => ({
  companies: 4,
  people: 2,
  relationships: 5,
  depth: 4,
  ...over,
});

describe("networkSummary", () => {
  it("says nothing before the event lands", () => {
    // The column is an invitation with numbers on it. No numbers, no column —
    // rendering it empty would promise a network the check has not found.
    expect(networkSummary(undefined)).toBeNull();
    expect(networkSummary(null)).toBeNull();
  });

  it("does not call the subject on its own a network", () => {
    // Every lookup produces at least the subject's entity statement, so a
    // bare count would put "1 company · Explore the full ownership network"
    // on every report, including the ones that found nothing else.
    expect(networkSummary(shape({ companies: 1, people: 0, relationships: 0 }))).toBeNull();
  });

  it("needs an edge, not just two records of the same party", () => {
    // Two sources describing the same company are two entity statements with
    // no relationship between them. The ownership view would draw them side
    // by side, unconnected — which is not what "network" promises.
    expect(networkSummary(shape({ companies: 3, relationships: 0 }))).toBeNull();
  });

  it("renders once there is a graph to render", () => {
    const s = networkSummary(shape());
    expect(s).toEqual({
      companies: 4,
      people: 2,
      relationships: 5,
      depthPhrase: "four layers deep",
    });
  });

  it("counts people even when only one company was mapped", () => {
    // A UK company with two PSCs and no parent is a real ownership graph.
    const s = networkSummary(shape({ companies: 1, people: 2, relationships: 2 }));
    expect(s?.people).toBe(2);
  });

  it("treats missing and negative counts as zero rather than NaN", () => {
    const s = networkSummary({
      companies: 3,
      people: undefined as unknown as number,
      relationships: 2,
      depth: null,
    });
    expect(s?.people).toBe(0);
    expect(s?.depthPhrase).toBeNull();
  });
});

describe("depthPhrase", () => {
  it("never says a depth that was not measured", () => {
    // COMPLEX_OWNERSHIP_LAYERS not firing means the chain was not walked.
    // "1 layer deep" would assert a flat graph the check never established.
    expect(depthPhrase(null)).toBeNull();
    expect(depthPhrase(undefined)).toBeNull();
    expect(depthPhrase(0)).toBeNull();
    expect(depthPhrase(-2)).toBeNull();
  });

  it("agrees in number", () => {
    expect(depthPhrase(1)).toBe("one layer deep");
    expect(depthPhrase(2)).toBe("two layers deep");
  });

  it("switches to digits past ten rather than inventing a word", () => {
    expect(depthPhrase(10)).toBe("ten layers deep");
    expect(depthPhrase(14)).toBe("14 layers deep");
  });
});
