import { describe, expect, it } from "vitest";
import { answeredCount, lookupProgress, progressLabel, coverageCopy, jurisdictionPhrase } from "./lookupProgress";

const none = new Set<string>();
const base = {
  anchored: false,
  applicable: [] as string[],
  started: none,
  completed: none,
  errored: none,
};

describe("lookupProgress", () => {
  it("shows no chips at all before the stream says which sources apply", () => {
    // The old grid rendered the entire registry — ESG adapters included —
    // during the pre-anchor window, when nothing is known. An empty list and
    // an honest phase label is the only truthful state here.
    const p = lookupProgress(base);
    expect(p.sources).toEqual([]);
    expect(p.total).toBeNull();
    expect(p.phase).toBe("connecting");
    expect(p.label).toMatch(/connect/i);
  });

  it("reports total as null rather than 0 while unknown", () => {
    // 0 would render as a complete progress bar.
    expect(lookupProgress(base).total).toBeNull();
    expect(lookupProgress({ ...base, anchored: true }).total).toBeNull();
  });

  it("puts every applicable source in waiting until its own event lands", () => {
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["gleif", "companies_house", "opensanctions"],
    });
    expect(p.sources.map((s) => s.state)).toEqual(["waiting", "waiting", "waiting"]);
    expect(p.phase).toBe("querying");
    expect(p.settled).toBe(0);
    expect(p.total).toBe(3);
  });

  it("moves a source only when the stream moves it", () => {
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a", "b", "c"],
      started: new Set(["a", "b"]),
      completed: new Set(["a"]),
    });
    expect(p.sources).toEqual([
      { sourceId: "a", state: "done" },
      { sourceId: "b", state: "querying" },
      { sourceId: "c", state: "waiting" },
    ]);
    expect(p.settled).toBe(1);
  });

  it("keeps a failed source distinct from a completed one", () => {
    // Folding a failure into the success count is the same untruth as the
    // simulated bar: "3 of 3" when one errored.
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a", "b"],
      completed: new Set(["a"]),
      errored: new Set(["b"]),
    });
    expect(p.sources[1].state).toBe("failed");
    expect(p.settled).toBe(2);
  });

  it("lets an error win over a completion for the same source", () => {
    // App.tsx adds an errored source to BOTH sets, so the two overlap by
    // design; the failure is the honest state to show.
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a"],
      completed: new Set(["a"]),
      errored: new Set(["a"]),
    });
    expect(p.sources[0].state).toBe("failed");
  });

  it("preserves dispatch order", () => {
    const applicable = ["z_source", "a_source", "m_source"];
    const p = lookupProgress({ ...base, anchored: true, applicable });
    expect(p.sources.map((s) => s.sourceId)).toEqual(applicable);
  });

  it("advances to finishing only when everything has settled", () => {
    const args = { ...base, anchored: true, applicable: ["a", "b"] };
    expect(lookupProgress({ ...args, completed: new Set(["a"]) }).phase).toBe("querying");
    expect(lookupProgress({ ...args, completed: new Set(["a", "b"]) }).phase).toBe("finishing");
  });

  it("handles a source that completes before its start event is processed", () => {
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a"],
      completed: new Set(["a"]),
    });
    expect(p.sources[0].state).toBe("done");
    expect(p.phase).toBe("finishing");
  });

  it("stops saying it is resolving the entity once the entity is resolved", () => {
    // anchored, but sources_applicable has not landed. Repeating "Resolving
    // the entity in GLEIF…" claims the lookup is doing something it has
    // finished — the same class of untruth as the simulated bar, smaller.
    // This is also the state an older backend, or an empty applicable list,
    // would sit in for a whole run.
    const p = lookupProgress({ ...base, anchored: true });
    expect(p.phase).toBe("dispatching");
    expect(p.label).not.toMatch(/GLEIF/);
    expect(p.total).toBeNull();
  });

  it("names the anchor step rather than pretending to query", () => {
    // Between stream-open and gleif_done the lookup is resolving one entity in
    // GLEIF. That is what it is doing, so that is what it should say.
    const p = lookupProgress({ ...base, started: new Set(["gleif"]) });
    expect(p.phase).toBe("anchoring");
    expect(p.label).toMatch(/GLEIF/);
  });

  it("gives every phase a present-tense label", () => {
    // The old grid's role="status" line flipped to the past tense on a timer.
    for (const args of [
      base,
      { ...base, started: new Set(["gleif"]) },
      { ...base, anchored: true },
      { ...base, anchored: true, applicable: ["a"] },
      { ...base, anchored: true, applicable: ["a"], completed: new Set(["a"]) },
    ]) {
      expect(lookupProgress(args).label).toMatch(/…$/);
    }
  });
});

describe("progressLabel", () => {
  it("stays in the present tense until everything has settled", () => {
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a", "b", "c"],
      completed: new Set(["a"]),
    });
    expect(progressLabel(p, 0)).toBe("Querying — 1 of 3 sources answered");
  });

  it("reaches the past tense only on real completion", () => {
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a", "b"],
      completed: new Set(["a", "b"]),
    });
    expect(progressLabel(p, 0)).toBe("Queried 2 of 2 sources");
  });

  it("says how many did not answer instead of counting them as queried", () => {
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a", "b", "c"],
      completed: new Set(["a", "b"]),
      errored: new Set(["c"]),
    });
    expect(progressLabel(p, 1)).toBe("Queried 3 of 3 sources, 1 did not answer");
  });

  it("falls back to the phase label while the total is unknown", () => {
    expect(progressLabel(lookupProgress(base), 0)).toMatch(/connect/i);
  });

  it("agrees in number for a single source", () => {
    const p = lookupProgress({
      ...base,
      anchored: true,
      applicable: ["a"],
      completed: new Set(["a"]),
    });
    expect(progressLabel(p, 0)).toBe("Queried 1 of 1 source");
  });
});

describe("answeredCount", () => {
  it("never exceeds the number of applicable sources", () => {
    // Production rendered "13 of 12 sources answered" above "Every applicable
    // source answered": the GLEIF anchor completes before sources_applicable
    // and is never in that list, so counting the completed set overshot.
    const applicable = ["companies_house", "opensanctions"];
    const completed = new Set(["gleif", "companies_house", "opensanctions", "sec_edgar"]);
    expect(answeredCount(applicable, completed)).toBe(2);
    expect(answeredCount(applicable, completed)).toBeLessThanOrEqual(applicable.length);
  });

  it("counts only what actually answered", () => {
    expect(answeredCount(["a", "b", "c"], new Set(["a"]))).toBe(1);
    expect(answeredCount(["a", "b"], new Set())).toBe(0);
  });

  it("is 0 before sources_applicable arrives, not the completed size", () => {
    expect(answeredCount([], new Set(["gleif"]))).toBe(0);
  });
});

describe("answeredCount and the sources that did not answer", () => {
  it("does not count an errored source as one that answered", () => {
    // The source card for an errored source reads "Did not answer". Counting
    // it as answered let the verdict strip say "3 of 3 sources answered ·
    // Every applicable source answered" directly above that card.
    const applicable = ["a", "b", "c"];
    // App adds an errored source to BOTH sets, by design.
    const completed = new Set(["a", "b", "c"]);
    const errored = new Set(["c"]);
    expect(answeredCount(applicable, completed, errored)).toBe(2);
  });

  it("is unchanged when nothing errored", () => {
    expect(answeredCount(["a", "b"], new Set(["a", "b"]), new Set())).toBe(2);
  });
});

describe("coverageCopy (Phase 156)", () => {
  it("names the registry and the company so ten of ten is never read as forty minus thirty", () => {
    const c = coverageCopy({ answered: 10, applicable: 10, total: 40, jurisdiction: "GB", screening: false });
    expect(c.answered).toBe(11); // the GLEIF anchor is one of the forty and it answered
    expect(c.applicable).toBe(11);
    expect(c.statNoun).toBe("sources answered");
    expect(c.detail).toBe("11 of OpenCheck's 40 sources apply to a GB company; every one answered.");
    expect(c.aside).toBe("11 of 11 sources answered");
  });

  it("says how many are still answering while the stream is open", () => {
    const c = coverageCopy({ answered: 3, applicable: 10, total: 40, jurisdiction: "GB", screening: true, pending: 7 });
    expect(c.detail).toBe("11 of OpenCheck's 40 sources apply to a GB company; 7 still answering.");
    expect(c.aside).toBe("4 of 11 sources answered · 7 still running…");
  });

  it("states a shortfall plainly rather than claiming every one answered", () => {
    const c = coverageCopy({ answered: 8, applicable: 10, total: 40, jurisdiction: "NL", screening: false });
    expect(c.detail).toBe("11 of OpenCheck's 40 sources apply to a NL company; 9 answered.");
  });

  it("drops the registry total until /sources has loaded, and never overshoots it", () => {
    expect(
      coverageCopy({ answered: 10, applicable: 10, total: null, jurisdiction: "GB", screening: false }).detail,
    ).toBe("11 sources apply to a GB company; every one answered.");
    // A total smaller than the applicable count is a stale or partial list —
    // do not print "11 of 9".
    expect(
      coverageCopy({ answered: 10, applicable: 10, total: 9, jurisdiction: "GB", screening: false }).detail,
    ).toBe("11 sources apply to a GB company; every one answered.");
  });

  it("names the country from a region-suffixed code and falls back to 'this company'", () => {
    expect(jurisdictionPhrase("US-DE")).toBe("a US company");
    expect(jurisdictionPhrase(" gb ")).toBe("a GB company");
    expect(jurisdictionPhrase(null)).toBe("this company");
    expect(jurisdictionPhrase("")).toBe("this company");
  });

  it("handles the singular and a report with no anchor", () => {
    const c = coverageCopy({ answered: 0, applicable: 0, total: 40, jurisdiction: null, screening: false });
    expect(c.answered).toBe(1);
    expect(c.statNoun).toBe("source answered");
    expect(c.detail).toBe("1 of OpenCheck's 40 sources applies to this company; every one answered.");
    const none = coverageCopy({ answered: 0, applicable: 1, total: 40, jurisdiction: "GB", screening: false, anchorAnswered: false });
    expect(none.detail).toBe("1 of OpenCheck's 40 sources applies to a GB company; 0 answered.");
  });
});
