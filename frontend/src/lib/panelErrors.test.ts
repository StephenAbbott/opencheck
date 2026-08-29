import { describe, expect, it } from "vitest";
import {
  clearPanelError,
  describeFetchFailure,
  mergePanelError,
  panelError,
  panelLabel,
  securitiesOverlayUnavailable,
  securitiesPageUnavailable,
} from "./panelErrors";

describe("describeFetchFailure", () => {
  it("never hands a reader a raw Error string", () => {
    // SubsidiaryNetwork rendered `String(e)`, so the user saw
    // "Error: 500 Internal Server Error" in the middle of a report.
    const d = describeFetchFailure(new Error("500 Internal Server Error"));
    expect(d).not.toMatch(/^Error:/);
    expect(d).toBe("the service errored (HTTP 500)");
  });

  it("distinguishes try-again from this-will-keep-failing", () => {
    expect(describeFetchFailure(new Error("429 Too Many Requests"))).toMatch(/rate-limited/);
    expect(describeFetchFailure(new Error("404 Not Found"))).toMatch(/no record/);
    expect(describeFetchFailure(new Error("503 Service Unavailable"))).toMatch(/HTTP 503/);
    expect(describeFetchFailure(new Error("403 Forbidden"))).toMatch(/refused/);
  });

  it("still says something for a network drop with no status", () => {
    // A dropped connection rejects with a TypeError and no status at all,
    // and silence is the failure mode being fixed.
    expect(describeFetchFailure(new TypeError("Failed to fetch"))).toBe(
      "the request could not be completed",
    );
    expect(describeFetchFailure("something odd")).toBeTruthy();
    expect(describeFetchFailure(undefined)).toBeTruthy();
  });
});

describe("panelError", () => {
  it("says what can no longer be relied on, not just that something broke", () => {
    // The point is not "an error happened" — it is that the reader must not
    // read the missing section as an absence of findings.
    const e = panelError("securities", new Error("500 x"));
    expect(e.missing).toMatch(/sanctions list/);
    expect(e.missing).toMatch(/did not run/);
    expect(e.detail).toMatch(/HTTP 500/);
  });

  it("covers both panels", () => {
    for (const panel of ["securities", "subsidiaries"] as const) {
      const e = panelError(panel, new Error("500 x"));
      expect(e.missing.length, panel).toBeGreaterThan(10);
      expect(panelLabel(panel), panel).toBeTruthy();
    }
  });
});

describe("degraded /securities responses (Phase 145)", () => {
  it("no-overlay degradation reports the check as not run, like the 500 did", () => {
    const e = securitiesOverlayUnavailable();
    expect(e.panel).toBe("securities");
    expect(e.missing).toMatch(/sanctions list/);
    expect(e.missing).toMatch(/did not run/);
    expect(e.detail).toMatch(/no sanctions index/);
  });

  it("a failed later page does NOT claim the sanctions check did not run", () => {
    // The banner is still on screen when a page-2 fetch degrades — saying
    // "the check did not run" beside it would be false.
    const e = securitiesPageUnavailable();
    expect(e.panel).toBe("securities");
    expect(e.missing).not.toMatch(/did not run/);
    expect(e.missing).toMatch(/ISIN list/);
    expect(e.detail).toMatch(/rate-limiting/);
  });

  it("merges into the panel list like any other securities failure", () => {
    let list = mergePanelError([], securitiesOverlayUnavailable());
    list = mergePanelError(list, securitiesPageUnavailable());
    expect(list).toHaveLength(1);
    expect(list[0].detail).toMatch(/rate-limiting/);
  });
});

describe("mergePanelError", () => {
  it("keeps one entry per panel so a retry replaces rather than stacks", () => {
    let list = mergePanelError([], panelError("securities", new Error("500 x")));
    list = mergePanelError(list, panelError("securities", new Error("429 y")));
    expect(list).toHaveLength(1);
    expect(list[0].detail).toMatch(/rate-limited/);
  });

  it("keeps different panels apart", () => {
    let list = mergePanelError([], panelError("securities", new Error("500 x")));
    list = mergePanelError(list, panelError("subsidiaries", new Error("500 x")));
    expect(list.map((p) => p.panel).sort()).toEqual(["securities", "subsidiaries"]);
  });
});

describe("clearPanelError", () => {
  it("removes only the panel that recovered", () => {
    const list = [
      panelError("securities", new Error("500 x")),
      panelError("subsidiaries", new Error("500 x")),
    ];
    expect(clearPanelError(list, "securities").map((p) => p.panel)).toEqual(["subsidiaries"]);
  });

  it("is a no-op when nothing is recorded", () => {
    expect(clearPanelError([], "securities")).toEqual([]);
  });
});
