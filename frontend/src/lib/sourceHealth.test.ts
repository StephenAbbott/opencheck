/**
 * sourceHealth — how the last sweep's verdict is worded on a catalogue card
 * (Phase 161). Not tested is never healthy; degraded is never coloured as
 * failed; a card with something to explain opens unasked.
 */
import { describe, expect, it } from "vitest";

import type { SourceHealthReport, SourceHealthRow } from "./api";
import {
  cardHealth,
  formatSweepTime,
  freshnessPhrase,
  healthSummary,
  STATUS_TONE,
} from "./sourceHealth";

const row = (overrides: Partial<SourceHealthRow> = {}): SourceHealthRow => ({
  status: "ok",
  reason: "",
  known_gap: "",
  liveness: "live",
  retrieved_at: "2026-08-31T07:31:02Z",
  latency_ms: 412,
  attempts: 1,
  statement_total: 12,
  statement_collapse: null,
  history: ["ok", "ok", "ok"],
  ...overrides,
});

const report = (sources: Record<string, SourceHealthRow>, extra: Partial<Extract<SourceHealthReport, { available: true }>> = {}): SourceHealthReport => ({
  available: true,
  generated_at: "2026-08-31T07:31:04Z",
  compared_against: "2026-08-24T07:30:58Z",
  registry_size: 40,
  probed: 40,
  counts: { ok: 34, degraded: 2, fail: 0, skipped: 4 },
  sweeps: ["2026-08-17T07:30:00Z", "2026-08-24T07:30:58Z", "2026-08-31T07:31:04Z"],
  sources,
  ...extra,
});

describe("healthSummary", () => {
  it("names the sweep's time in UTC and carries the four counts", () => {
    const s = healthSummary(report({}));
    expect(s).toEqual({
      sweptAt: "Mon 31 Aug 2026, 07:31 UTC",
      counts: { ok: 34, degraded: 2, fail: 0, skipped: 4 },
      staleNote: null,
    });
  });

  it("says so when the API served its last good copy", () => {
    expect(healthSummary(report({}, { stale: true }))?.staleNote).toMatch(/could not be read just now/);
  });

  it("is null when no sweep has published — the page then renders as before", () => {
    expect(healthSummary({ available: false, reason: "no sweep report" })).toBeNull();
    expect(healthSummary(null)).toBeNull();
    expect(healthSummary(undefined)).toBeNull();
  });
});

describe("cardHealth", () => {
  it("reads a healthy source: ok tone, freshness, latency, statements, closed by default", () => {
    const h = cardHealth("gleif", report({ gleif: row() }), "GLEIF");
    expect(h?.word).toBe("Healthy");
    expect(h?.tone).toBe("ok");
    expect(h?.checked).toBe("checked Mon 31 Aug 2026, 07:31 UTC");
    expect(h?.rows).toEqual([
      { label: "Freshness", value: "Answered live" },
      { label: "Latency", value: "0.4 s" },
      { label: "Statements last sweep", value: "12 statements" },
    ]);
    expect(h?.notes).toEqual([]);
    expect(h?.openByDefault).toBe(false);
    expect(h?.summary).toBe("GLEIF: healthy in the sweep of Mon 31 Aug 2026, 07:31 UTC.");
  });

  it("colours a degradation as context, prints the reason and the known gap, and opens", () => {
    const h = cardHealth(
      "jar_lithuania",
      report({
        jar_lithuania: row({
          status: "degraded",
          reason: "register unreachable from CI (HTTP 403)",
          known_gap: "the register refuses datacentre IPs",
          liveness: null,
          retrieved_at: null,
          latency_ms: 1180,
          attempts: 2,
          statement_total: null,
          history: ["ok", "degraded", "degraded"],
        }),
      }),
    );
    expect(h?.word).toBe("Degraded");
    expect(h?.tone).toBe("context");
    expect(h?.tone).not.toBe("warn");
    expect(h?.notes).toEqual([
      "register unreachable from CI (HTTP 403)",
      "Known gap: the register refuses datacentre IPs",
    ]);
    // Rows the sweep could not fill are left out, not printed as dashes.
    expect(h?.rows).toEqual([{ label: "Latency", value: "1.2 s" }]);
    expect(h?.openByDefault).toBe(true);
    expect(h?.history).toEqual(["ok", "degraded", "degraded"]);
  });

  it("marks a failure as warn — something did not run — never risk", () => {
    const h = cardHealth("x", report({ x: row({ status: "fail", reason: "timed out after 60s" }) }));
    expect(h?.word).toBe("Failed");
    expect(h?.tone).toBe("warn");
    expect(Object.values(STATUS_TONE)).not.toContain("risk");
    expect(h?.openByDefault).toBe(true);
  });

  it("says not tested, in the neutral tone, of a source the sweep skipped — never healthy", () => {
    const h = cardHealth(
      "bolagsverket",
      report({ bolagsverket: row({ status: "skipped", reason: "not configured: BOLAGSVERKET_API_KEY", liveness: null, latency_ms: null, statement_total: null, history: ["skipped"] }) }),
    );
    expect(h?.word).toBe("Not tested");
    expect(h?.tone).toBe("neutral");
    expect(h?.rows).toEqual([]);
    expect(h?.notes).toEqual(["not configured: BOLAGSVERKET_API_KEY"]);
    expect(h?.openByDefault).toBe(true);
  });

  it("reports a statement collapse on an otherwise healthy source and opens the card", () => {
    const h = cardHealth(
      "cvr",
      report({ cvr: row({ statement_total: 1, statement_collapse: { relationship: { was: 9, now: 0 } } }) }),
    );
    expect(h?.rows[2].value).toBe("1 statement — fell since the previous sweep (relationship 9 → 0)");
    expect(h?.openByDefault).toBe(true);
  });

  it("notes a pass on retry", () => {
    const h = cardHealth("x", report({ x: row({ attempts: 2 }) }));
    expect(h?.rows.at(-1)).toEqual({ label: "Attempts", value: "passed on retry" });
  });

  it("is null for a source the sweep did not cover, and when no sweep has published", () => {
    expect(cardHealth("new_source", report({}))).toBeNull();
    expect(cardHealth("gleif", { available: false, reason: "x" })).toBeNull();
  });
});

describe("freshnessPhrase", () => {
  it("dates a snapshot but not a live answer", () => {
    expect(freshnessPhrase({ liveness: "snapshot", retrieved_at: "2026-08-01T00:00:00Z" })).toBe("Snapshot · 1 Aug 2026");
    expect(freshnessPhrase({ liveness: "live", retrieved_at: "2026-08-31T07:31:02Z" })).toBe("Answered live");
    expect(freshnessPhrase({ liveness: "curated", retrieved_at: null })).toBe("Curated fixture");
    expect(freshnessPhrase({ liveness: null, retrieved_at: null })).toBe("—");
  });
});

describe("formatSweepTime", () => {
  it("prints UTC with the weekday, and leaves an unparseable value as written", () => {
    expect(formatSweepTime("2026-08-31T07:31:04Z")).toBe("Mon 31 Aug 2026, 07:31 UTC");
    expect(formatSweepTime("last week")).toBe("last week");
    expect(formatSweepTime(null)).toBe("—");
  });
});
