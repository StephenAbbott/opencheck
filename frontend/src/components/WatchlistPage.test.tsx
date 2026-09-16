/**
 * The markup tier for /watchlist (Phase 215): one h1, the empty state when
 * no token is held, and — with a list — one row per watched company, the
 * feed address, the log newest first with the tier chip and the honesty
 * line, and a re-check that never fires without a click.
 */

import { render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import WatchlistPage from "./WatchlistPage";
import { TOKEN_KEY, type WatchlistPayload } from "../lib/watchlist";

const PAYLOAD: WatchlistPayload = {
  watches: [
    {
      lei: "213800LH1BZH3DI6G760",
      added_at: "2026-09-10T08:00:00Z",
      legal_name: "Vosper Ltd",
      jurisdiction: "GB",
      gleif_facts: { registration_status: "ISSUED" },
      gleif_watermark: "2026-09-16 00:00:00",
      snapshot_at: "2026-09-10T08:00:00Z",
      last_checked_at: "2026-09-16T08:00:00Z",
      baseline: {
        register_status: { liveness: "live", source_id: "companies_house" },
        risk_codes: ["SANCTIONED"],
        context_codes: [],
        coverage: { applicable: 10, answered: 10 },
        verdict: "x",
        checked: [],
        degraded_sources: [],
      },
    },
  ],
  entries: [
    {
      id: 1, lei: "213800LH1BZH3DI6G760", legal_name: "Vosper Ltd", created_at: "2026-09-15T00:00:00Z", tier: "manual",
      trigger: {}, changes: [{ kind: "verdict", new: "a" }], checked: [], degraded: [],
    },
    {
      id: 2, lei: "213800LH1BZH3DI6G760", legal_name: "Vosper Ltd", created_at: "2026-09-16T08:05:00Z", tier: "gleif",
      trigger: { publish: "2026-09-16 00:00:00", fields: ["registration_status"] },
      changes: [{ kind: "gleif_field", field: "registration_status", old: "ISSUED", new: "LAPSED" }],
      checked: [{ source_id: "gleif", liveness: "live", retrieved_at: "2026-09-16T08:05:00Z" }],
      degraded: [{ source_id: "kvk" }],
    },
  ],
  caps: { per_list: 10, total: 200, in_list: 1, total_watched: 3 },
  feed_url: "https://api.opencheck.world/watch/tok.atom",
  tiers: {
    gleif: { available: true, watermark: "2026-09-16 00:00:00", refresh_enabled: true, last_applied_at: null, last_delta: "IntraDay", rows_applied: { entities: 3903 }, record_count: 3424074 },
    opensanctions: { available: true, last_version: "v", last_checked_at: null },
    worker: { enabled: true, interval_s: 300 },
  },
};

function mockFetch(payload: WatchlistPayload) {
  const calls: { url: string; method: string }[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push({ url: String(url), method: init?.method ?? "GET" });
      return { ok: true, status: 200, json: async () => payload } as Response;
    }),
  );
  return calls;
}

describe("WatchlistPage", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.history.replaceState({}, "", "/watchlist");
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders the empty state, one h1 and no fetch when nothing is held", () => {
    const calls = mockFetch(PAYLOAD);
    render(<WatchlistPage />);
    expect(screen.getAllByRole("heading", { level: 1 })).toHaveLength(1);
    expect(screen.getByTestId("watchlist-empty")).toHaveTextContent("Watch for changes");
    expect(calls).toHaveLength(0);
  });

  it("opens the list from the URL token, keeps it, and renders rows, feed, log and the honesty line", async () => {
    window.history.replaceState({}, "", "/watchlist?token=tok");
    const calls = mockFetch(PAYLOAD);
    render(<WatchlistPage sourceNames={{ companies_house: "Companies House", kvk: "KvK" }} />);
    await waitFor(() => expect(screen.getByTestId("feed-url")).toHaveTextContent("/watch/tok.atom"));
    expect(calls[0].url).toMatch(/\/watch\/tok$/);
    expect(window.localStorage.getItem(TOKEN_KEY)).toBe("tok");

    const rows = within(screen.getByRole("list", { name: "Watched companies" })).getAllByRole("listitem");
    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveTextContent("Vosper Ltd");
    expect(rows[0]).toHaveTextContent("Active · Companies House");
    expect(rows[0]).toHaveTextContent("10 of 10 sources");
    expect(within(rows[0]).getByRole("button", { name: "Re-check now" })).toBeEnabled();
    expect(within(rows[0]).getByRole("button", { name: "Stop watching" })).toBeEnabled();

    // Newest first, tier chip, the trigger sentence and the degraded line.
    const log = Array.from(screen.getByRole("list", { name: "Change log, newest first" }).children);
    expect(log).toHaveLength(2);
    expect(log[0]).toHaveTextContent("GLEIF delta");
    expect(log[0]).toHaveTextContent("GLEIF published a change to this record in its 2026-09-16 00:00:00 delta");
    expect(log[0]).toHaveTextContent("GLEIF LEI registration status: ISSUED → LAPSED.");
    expect(log[0]).toHaveTextContent("Could not check: KvK");
    expect(log[1]).toHaveTextContent("By hand");

    expect(screen.getByTestId("tier-sentence")).toHaveTextContent("3,424,074 LEI records");
    expect(screen.getByTestId("tier-sentence")).toHaveTextContent("never polled");
    // Nothing but the GET ran: a re-check is a click, never a render.
    expect(calls.every((c) => c.method === "GET")).toBe(true);
  });

  it("renders the log's empty state with a list that has no entries", async () => {
    window.localStorage.setItem(TOKEN_KEY, "tok");
    mockFetch({ ...PAYLOAD, entries: [] });
    render(<WatchlistPage />);
    await waitFor(() => expect(screen.getByTestId("log-empty")).toBeInTheDocument());
    expect(screen.getByTestId("log-empty")).toHaveTextContent("Nothing yet");
  });
});
