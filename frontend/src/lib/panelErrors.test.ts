import { describe, expect, it } from "vitest";
import {
  clearPanelError,
  describeFetchFailure,
  mergePanelError,
  panelError,
  panelLabel,
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
