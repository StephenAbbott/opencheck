import { afterEach, describe, expect, it, vi } from "vitest";
import { LOOKUP_EVENT_HANDLERS, replayLookupEvents, type LookupStreamHandlers, type SavedReportEvent } from "./api";
import {
  HOLD_WINDOW_MS,
  bannerText,
  manageTokenFor,
  modeInSavedReport,
  notInSavedReport,
  openErrorMessage,
  rememberManageToken,
  reportIdFromPath,
  runCompletedAtFrom,
  saveEligibility,
  savedConfirmation,
  savedDeepen,
  savedReportLink,
  savedStatements,
  shortHash,
  utcDate,
  utcDateTime,
} from "./savedReport";

const ID = "SU82_KMkQo2QbEv3Kcfm8A";

describe("addressing", () => {
  it("reads a report id off /report/{id} and nothing else", () => {
    expect(reportIdFromPath(`/report/${ID}`)).toBe(ID);
    expect(reportIdFromPath(`/report/${ID}/`)).toBe(ID);
    expect(reportIdFromPath("/report")).toBeNull();
    expect(reportIdFromPath("/report/short")).toBeNull();
    expect(reportIdFromPath(`/reports/${ID}`)).toBeNull();
    expect(reportIdFromPath(`/report/${ID}/extra`)).toBeNull();
    expect(reportIdFromPath("/report/../../etc/passwdAAAAAA")).toBeNull();
  });

  it("shares this site's page, not the API", () => {
    expect(savedReportLink(ID, "https://opencheck.world/")).toBe(`https://opencheck.world/report/${ID}`);
  });
});

describe("dates are UTC and written the house way", () => {
  it("uses Sept, June and July, never the three-letter forms", () => {
    expect(utcDate("2026-09-16T15:18:42Z")).toBe("16 Sept 2026");
    expect(utcDate("2026-06-01T00:00:00Z")).toBe("1 June 2026");
    expect(utcDate("2026-07-31T23:59:59Z")).toBe("31 July 2026");
    expect(utcDateTime("2026-09-16T15:18:42+00:00")).toBe("16 Sept 2026, 15:18 UTC");
  });

  it("does not move with the offset a timestamp was written in", () => {
    expect(utcDateTime("2026-09-17T01:30:00+02:00")).toBe("16 Sept 2026, 23:30 UTC");
  });
});

describe("the banner", () => {
  const meta = { saved_at: "2026-09-16T15:18:42Z", expires_at: "2026-12-15T15:18:42Z" };

  it("names the fifth clock beside the check's own, and says nothing was re-checked", () => {
    const b = bannerText(meta, "2026-09-16T15:12:00+00:00");
    expect(b.heading).toBe("Saved report");
    expect(b.clocks).toBe("Saved 16 Sept 2026, 15:18 UTC, from a check that finished at 15:12 UTC.");
    expect(b.notLive).toMatch(/Nothing on this page has been re-checked/);
    expect(b.kept).toBe("Kept until 15 Dec 2026.");
  });

  it("dates the check when it finished on the day before the save", () => {
    const b = bannerText({ ...meta, saved_at: "2026-09-17T00:05:00Z" }, "2026-09-16T23:55:00+00:00");
    expect(b.clocks).toContain("finished at 16 Sept 2026, 23:55 UTC");
  });

  it("shortens a hash for display", () => {
    expect(shortHash("8b73a92b55d18b963f700550d467194c")).toBe("8b73a92b55d1");
  });
});

describe("what a saved report holds", () => {
  it("holds QuickCheck and FullCheck only", () => {
    expect(modeInSavedReport("quick")).toBe(true);
    expect(modeInSavedReport("full")).toBe(true);
    for (const m of ["background", "subsidiaries", "history", "esg"] as const) {
      expect(modeInSavedReport(m)).toBe(false);
    }
  });

  it("says a missing tab is missing, and why", () => {
    expect(notInSavedReport("History")).toMatch(/^History is not part of a saved report\./);
    expect(notInSavedReport("History")).toMatch(/Run a live check/);
  });
});

describe("when Save can be offered", () => {
  const now = new Date("2026-09-16T15:20:00Z");

  it("waits for the check to finish", () => {
    expect(saveEligibility({ streaming: true, runCompletedAt: null, retried: false, now })).toEqual({
      canSave: false,
      reason: "Available when the check finishes.",
    });
  });

  it("refuses after a per-source retry — the page is no longer one run", () => {
    const e = saveEligibility({ streaming: false, runCompletedAt: "2026-09-16T15:19:00Z", retried: true, now });
    expect(e.canSave).toBe(false);
    expect(e.reason).toMatch(/retried/);
  });

  it("needs the run's name", () => {
    expect(saveEligibility({ streaming: false, runCompletedAt: null, retried: false, now }).canSave).toBe(false);
  });

  it("allows it inside the hold window and not at its edge", () => {
    const fresh = new Date(now.getTime() - HOLD_WINDOW_MS + 1000).toISOString();
    const stale = new Date(now.getTime() - HOLD_WINDOW_MS).toISOString();
    expect(saveEligibility({ streaming: false, runCompletedAt: fresh, retried: false, now })).toEqual({
      canSave: true,
      reason: null,
    });
    const e = saveEligibility({ streaming: false, runCompletedAt: stale, retried: false, now });
    expect(e.canSave).toBe(false);
    expect(e.reason).toMatch(/15 minutes/);
  });

  it("reads the run's name off the done event", () => {
    expect(runCompletedAtFrom({ lei: "X", bods_issues: [], license_notices: [], run_completed_at: "2026-09-16T15:19:00+00:00" })).toBe(
      "2026-09-16T15:19:00+00:00",
    );
    expect(runCompletedAtFrom({ lei: "X", bods_issues: [], license_notices: [] })).toBeNull();
  });

  it("confirms a save in one line", () => {
    expect(savedConfirmation({ expires_at: "2026-12-15T15:18:42Z" }, true, true)).toBe(
      "Saved with its summary. The link is copied. Kept until 15 Dec 2026.",
    );
    expect(savedConfirmation({ expires_at: "2026-12-15T15:18:42Z" }, false, false)).toBe(
      "Saved. Kept until 15 Dec 2026.",
    );
  });
});

describe("opening a saved report that is not there", () => {
  it("words 404, 410 and 503 for a reader", () => {
    expect(openErrorMessage(404, "No saved report with that id.")).toMatch(/no saved report at this address/);
    expect(openErrorMessage(410, "This saved report expired on 2026-12-15 and has been deleted.")).toMatch(/expired/);
    expect(openErrorMessage(503, "")).toMatch(/not available/);
  });
});

// A fixture in the shape Phase 216 stores, with a signal code the engine no
// longer emits: the page must carry it, not drop it.
const RETIRED = "RETIRED_SIGNAL_CODE_FROM_2026";
const EVENTS: SavedReportEvent[] = [
  { event: "gleif_done", data: { lei: "213800LH1BZH3DI6G760", legal_name: "BP P.L.C.", jurisdiction: "GB", derived_identifiers: {} } },
  { event: "sources_applicable", data: { source_ids: ["companies_house"] } },
  { event: "hit", data: { source_id: "gleif", hit_id: "213800LH1BZH3DI6G760", kind: "entity", name: "BP P.L.C.", summary: "GB", is_stub: false } },
  { event: "deepen_result", data: { source_id: "gleif", hit_id: "213800LH1BZH3DI6G760", bods: [{ statementId: "a" }], bods_issues: [], risk_signals: [], license: "CC0-1.0", license_notice: null } },
  { event: "deepen_result", data: { source_id: "companies_house", hit_id: "00102498", bods: [{ statementId: "b" }, { statementId: "c" }] } },
  { event: "risk_signals", data: { signals: [{ code: RETIRED, kind: "risk", confidence: "high", summary: "retired", source_id: "gleif", hit_id: "x", evidence: {} }], degraded_sources: [], verdict: "One finding." } },
  { event: "done", data: { lei: "213800LH1BZH3DI6G760", bods_issues: [], license_notices: [], run_completed_at: "2026-09-16T15:18:42+00:00" } },
];

describe("replaying stored events", () => {
  it("feeds every stored event to the handler a live stream would, in order, and carries a retired code", () => {
    const calls: string[] = [];
    let signals: { code: string }[] = [];
    const handlers: LookupStreamHandlers = {
      onGleifDone: () => calls.push("gleif_done"),
      onSourcesApplicable: () => calls.push("sources_applicable"),
      onHit: () => calls.push("hit"),
      onRiskSignals: (e) => {
        calls.push("risk_signals");
        signals = e.signals;
      },
      onDone: (e) => calls.push(`done:${e.run_completed_at}`),
    };
    replayLookupEvents(EVENTS, handlers);
    expect(calls).toEqual(["gleif_done", "sources_applicable", "hit", "risk_signals", "done:2026-09-16T15:18:42+00:00"]);
    expect(signals.map((s) => s.code)).toEqual([RETIRED]);
  });

  it("maps every streamed event to a handler, and never the internal deepen events", () => {
    expect(Object.keys(LOOKUP_EVENT_HANDLERS)).not.toContain("deepen_result");
    expect(Object.keys(LOOKUP_EVENT_HANDLERS)).toEqual(
      expect.arrayContaining(["gleif_done", "hit", "risk_signals", "subject_profile", "bods_counts", "source_error"]),
    );
  });

  it("reports a stored error event rather than swallowing it", () => {
    const onError = vi.fn();
    replayLookupEvents([{ event: "error", data: { detail: "nope" } }], { onError });
    expect(onError).toHaveBeenCalledWith("nope");
  });
});

describe("source drawers and FullCheck read the saved mapping", () => {
  it("gives a drawer the saved mapping for its own result only", () => {
    const d = savedDeepen(EVENTS, "gleif", "213800LH1BZH3DI6G760");
    expect(d?.bods).toEqual([{ statementId: "a" }]);
    expect(d?.license).toBe("CC0-1.0");
    expect(d?.raw).toEqual({});
    expect(savedDeepen(EVENTS, "gleif", "someone-else")).toBeNull();
    expect(savedDeepen(EVENTS, "opensanctions", "x")).toBeNull();
  });

  it("fills what a report saved before Phase 217 did not carry", () => {
    const d = savedDeepen(EVENTS, "companies_house", "00102498");
    expect(d).toMatchObject({ bods_issues: [], risk_signals: [], license: "", license_notice: null });
  });

  it("draws FullCheck from every saved mapping, in order", () => {
    expect(savedStatements(EVENTS).map((s) => s.statementId)).toEqual(["a", "b", "c"]);
  });
});

describe("the manage token", () => {
  const store = new Map<string, string>();
  afterEach(() => {
    store.clear();
    vi.unstubAllGlobals();
  });

  it("is kept per report in this browser", () => {
    vi.stubGlobal("window", {
      localStorage: {
        getItem: (k: string) => store.get(k) ?? null,
        setItem: (k: string, v: string) => void store.set(k, v),
      },
    });
    expect(manageTokenFor(ID)).toBeNull();
    rememberManageToken(ID, "tok");
    rememberManageToken("AAAAAAAAAAAAAAAAAAAAAA", "other");
    expect(manageTokenFor(ID)).toBe("tok");
  });

  it("survives storage that throws", () => {
    vi.stubGlobal("window", {
      localStorage: {
        getItem: () => {
          throw new Error("blocked");
        },
        setItem: () => {
          throw new Error("blocked");
        },
      },
    });
    expect(() => rememberManageToken(ID, "tok")).not.toThrow();
    expect(manageTokenFor(ID)).toBeNull();
  });
});
