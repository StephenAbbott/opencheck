import { describe, expect, it } from "vitest";
import {
  accumulateDegraded,
  cappedAnchors,
  cappedSentence,
  cappedStopSentence,
  deferredAnchors,
  deferredSentence,
  failedCountSentence,
  failedSentence,
  retryAfterSeconds,
  waitingSentence,
} from "./expandLayer";

describe("expandLayer (Phase 234)", () => {
  it("names nothing when the layer finished", () => {
    expect(deferredAnchors({})).toEqual([]);
    expect(deferredSentence({ deferred: [], count: 4 })).toBeNull();
    expect(failedSentence(undefined)).toBeNull();
    expect(failedCountSentence(0)).toBeNull();
  });

  it("says how many were deferred, why, and when to come back", () => {
    const s = deferredSentence({ deferred: ["a", "b"], retry_after_s: 39.2, count: 5 });
    expect(s).toContain("2 of 5 companies were not expanded yet");
    expect(s).toContain("full lookup");
    expect(s).toContain("40s");
  });

  it("never waits less than a second", () => {
    expect(retryAfterSeconds({ retry_after_s: null })).toBe(1);
    expect(retryAfterSeconds({ retry_after_s: 0 })).toBe(1);
    expect(retryAfterSeconds({ retry_after_s: 12 })).toBe(12);
  });

  it("reports failures as not the same as having no owners", () => {
    const one = failedSentence([{ anchor: "a", status: 404, reason: "LEI not found in GLEIF." }]);
    expect(one).toBe(
      "1 company could not be expanded: LEI not found in GLEIF. Their owners are not shown, which is not the same as having none."
    );
    const mixed = failedSentence([
      { anchor: "a", status: 404, reason: "x" },
      { anchor: "b", status: 500, reason: "y" },
    ]);
    expect(mixed).toMatch(/^2 companies could not be expanded\. /);
    expect(failedCountSentence(3)).toContain("not the same as having none");
  });

  it("words the wait in the progress line", () => {
    expect(waitingSentence(1, 30)).toBe("Waiting 30s for your lookup budget — 1 company left in this layer…");
  });
});

describe("expandLayer (Phase 283)", () => {
  it("names the capped nodes and asks for the layer again", () => {
    expect(cappedAnchors({})).toEqual([]);
    expect(cappedSentence({ capped: [], count: 25 })).toBeNull();
    expect(cappedAnchors({ capped: ["X", "Y"] })).toEqual(["X", "Y"]);
    expect(cappedSentence({ capped: ["X", "Y", "Z", "W", "V"], count: 25 })).toBe(
      "5 companies at the edge were not expanded in this step — OpenCheck expands 25 at a time, " +
        "nearest the subject first. Add the layer again to continue."
    );
    expect(cappedSentence({ capped: ["X"], count: 25 })).toMatch(/^1 company at the edge was not/);
    expect(cappedStopSentence(3)).toBe(
      "the expansion limit was reached with 3 companies at the edge of the network not expanded"
    );
  });

  const screen = (hops: number, signals: string[]) => ({
    source_id: "opensanctions",
    check: "cross_source_names",
    reason: "timeout" as const,
    affected_signals: signals,
    detail: "server sentence",
    hops,
  });

  it("accumulates layer degradations over the run, counting companies", () => {
    let acc = accumulateDegraded([], [screen(2, ["RELATED_SANCTIONED"])]);
    acc = accumulateDegraded(acc, [
      screen(1, ["RELATED_PEP"]),
      { ...screen(1, []), source_id: "gleif", check: "gleif_subsidiaries", reason: "truncated" },
    ]);
    expect(acc).toHaveLength(2);
    expect(acc[0]).toMatchObject({
      source_id: "opensanctions",
      affected_signals: ["RELATED_SANCTIONED", "RELATED_PEP"],
      detail: "Sanctions and PEP screening did not fully run for 3 companies expanded in this network",
    });
    expect(acc[1].detail).toBe(
      "The subsidiary list did not fully run for 1 company expanded in this network"
    );
  });

  it("leaves the run's list alone when a layer reports nothing", () => {
    const acc = accumulateDegraded([], [screen(1, [])]);
    expect(accumulateDegraded(acc, undefined)).toBe(acc);
    expect(accumulateDegraded(acc, [])).toBe(acc);
  });
});
