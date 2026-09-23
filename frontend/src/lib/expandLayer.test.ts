import { describe, expect, it } from "vitest";
import {
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
