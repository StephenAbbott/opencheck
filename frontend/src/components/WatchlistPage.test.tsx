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
      context: {
        expected_jurisdiction: "GB",
        expected_parent_lei: "A",
        rationale: "Key supplier since 2019.",
        onboarding_report_id: "AbCdEfGhIjKlMnOpQrStUv",
        set_at: "2026-09-11T08:00:00Z",
        expected_parent_name: "Alpha Holdings",
        baseline_disagrees: [{ field: "ultimate_parent_lei", expected: "A", actual: "B" }],
        onboarding_report: {
          report_id: "AbCdEfGhIjKlMnOpQrStUv", available: true, saved_at: "2026-09-10T08:00:00Z",
          report_path: "/report/AbCdEfGhIjKlMnOpQrStUv",
        },
      },
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
      trigger: {}, changes: [{ kind: "verdict", new: "a", materiality: "note" }], checked: [], degraded: [],
      materiality: "note", materiality_rules: 1,
    },
    {
      id: 2, lei: "213800LH1BZH3DI6G760", legal_name: "Vosper Ltd", created_at: "2026-09-16T08:05:00Z", tier: "gleif",
      trigger: { publish: "2026-09-16 00:00:00", fields: ["registration_status"] },
      changes: [{ kind: "gleif_field", field: "registration_status", old: "ISSUED", new: "LAPSED", materiality: "note" }],
      checked: [{ source_id: "gleif", liveness: "live", retrieved_at: "2026-09-16T08:05:00Z" }],
      degraded: [{ source_id: "kvk" }],
      materiality: "note", materiality_rules: 1,
    },
    {
      id: 3, lei: "213800LH1BZH3DI6G760", legal_name: "Vosper Ltd", created_at: "2026-09-17T08:05:00Z", tier: "gleif",
      trigger: { publish: "2026-09-17 00:00:00", fields: ["ultimate_parent_lei"] },
      changes: [{
        kind: "gleif_field", field: "ultimate_parent_lei", old: "A", new: "B", materiality: "escalate",
        // pKYC stage B: compared with what the watcher expected, at write time.
        expectation: { field: "ultimate_parent_lei", expected: "A", met: false },
      }],
      checked: [], degraded: [],
      // graded on read: an entry written before grading existed
      materiality: "escalate", materiality_rules: null,
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

    const watched = screen.getByRole("list", { name: "Watched companies" });
    // Direct rows only: a row's context block carries its own list (stage B).
    const rows = within(watched).getAllByRole("listitem").filter((li) => li.parentElement === watched);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveTextContent("Vosper Ltd");
    expect(rows[0]).toHaveTextContent("Active · Companies House");
    expect(rows[0]).toHaveTextContent("10 of 10 sources");
    expect(within(rows[0]).getByRole("button", { name: "Re-check now" })).toBeEnabled();
    expect(within(rows[0]).getByRole("button", { name: "Stop watching" })).toBeEnabled();

    // Newest first, tier chip, the trigger sentence and the degraded line.
    const log = Array.from(screen.getByRole("list", { name: "Change log, newest first" }).children);
    expect(log).toHaveLength(3);
    expect(log[0]).toHaveTextContent("Escalate");
    expect(log[1]).toHaveTextContent("GLEIF delta");
    expect(log[1]).toHaveTextContent("GLEIF published a change to this record in its 2026-09-16 00:00:00 delta");
    expect(log[1]).toHaveTextContent("GLEIF LEI registration status: ISSUED → LAPSED.");
    expect(log[1]).toHaveTextContent("Could not check: KvK");
    expect(log[2]).toHaveTextContent("By hand");

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

  it("forgets a held token the instance does not know, and says so in the empty state", async () => {
    window.localStorage.setItem(TOKEN_KEY, "stale");
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: false, status: 404, json: async () => ({ detail: "No watchlist with that token." }) }) as Response),
    );
    render(<WatchlistPage />);
    await waitFor(() => expect(screen.getByTestId("watchlist-empty")).toBeInTheDocument());
    expect(screen.getByTestId("watchlist-empty")).toHaveTextContent("no longer available");
    expect(screen.queryByRole("alert")).toBeNull();
    expect(window.localStorage.getItem(TOKEN_KEY)).toBeNull();
  });

  it("keeps a held token, with the error and a retry, when the list could not be reached", async () => {
    window.localStorage.setItem(TOKEN_KEY, "tok");
    vi.stubGlobal("fetch", vi.fn(async () => ({ ok: false, status: 503, json: async () => ({ detail: "down" }) }) as Response));
    render(<WatchlistPage />);
    await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("down"));
    expect(screen.getByRole("button", { name: "Try again" })).toBeInTheDocument();
    expect(window.localStorage.getItem(TOKEN_KEY)).toBe("tok");
  });

  it("treats an unknown URL token as an error, not as this browser's list gone", async () => {
    window.history.replaceState({}, "", "/watchlist?token=wrong");
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: false, status: 404, json: async () => ({ detail: "No watchlist with that token." }) }) as Response),
    );
    render(<WatchlistPage />);
    await waitFor(() => expect(screen.getByTestId("watchlist-empty")).toHaveTextContent("No watchlist with that token."));
    expect(window.localStorage.getItem(TOKEN_KEY)).toBeNull();
  });

  it("shows the materiality chip, the graded-today note, and the Material-only filter hides every note entry", async () => {
    window.localStorage.setItem(TOKEN_KEY, "tok");
    mockFetch(PAYLOAD);
    const user = (await import("@testing-library/user-event")).default.setup();
    render(<WatchlistPage />);
    const log = await screen.findByRole("list", { name: "Change log, newest first" });
    expect(within(log).getAllByRole("listitem").filter((li) => li.parentElement === log)).toHaveLength(3);
    expect(within(log).getAllByTestId("materiality-chip").map((c) => c.textContent)).toEqual([
      "Escalate",
      "Note only",
      "Note only",
    ]);
    expect(within(log).getByText(/Graded under today's table/)).toBeTruthy();
    const group = screen.getByRole("group", { name: "Show" });
    expect(within(group).getByRole("button", { name: "All" })).toHaveAttribute("aria-pressed", "true");

    await user.click(within(group).getByRole("button", { name: "Material only" }));
    const filtered = screen.getByRole("list", { name: "Change log, newest first" });
    expect(within(filtered).getAllByRole("listitem").filter((li) => li.parentElement === filtered)).toHaveLength(1);
    expect(within(filtered).getByTestId("materiality-chip").textContent).toBe("Escalate");
    expect(screen.getByTestId("log-hidden").textContent).toContain("2 note-only entries are hidden");
    expect(window.location.search).toBe("?show=material");
    expect(window.localStorage.getItem("opencheck.watchlist.filter")).toBe("material");

    await user.click(within(group).getByRole("button", { name: "All" }));
    expect(screen.queryByTestId("log-hidden")).toBeNull();
    expect(window.location.search).toBe("");
  });

  it("starts on Material only from the URL, and says so when every entry is note only", async () => {
    window.history.replaceState({}, "", "/watchlist?show=material");
    window.localStorage.setItem(TOKEN_KEY, "tok");
    mockFetch({ ...PAYLOAD, entries: PAYLOAD.entries.slice(0, 2) });
    render(<WatchlistPage />);
    expect(await screen.findByTestId("log-all-note")).toBeTruthy();
    expect(screen.queryByRole("list", { name: "Change log, newest first" })).toBeNull();
  });

  // ---- pKYC stage B: what the watcher expected ----------------------------

  it("shows the context, the baseline disagreement and the report link, and a contradicted change beside its chip", async () => {
    window.localStorage.setItem(TOKEN_KEY, "tok");
    mockFetch(PAYLOAD);
    render(<WatchlistPage />);
    const block = await screen.findByTestId("watch-context");
    expect(block).toHaveTextContent("Key supplier since 2019.");
    expect(block).toHaveTextContent("Expected jurisdiction: GB");
    expect(block).toHaveTextContent("Expected ultimate parent: Alpha Holdings · A");
    expect(within(block).getByRole("link", { name: /saved report of 2026-09-10/ })).toHaveAttribute(
      "href",
      "/report/AbCdEfGhIjKlMnOpQrStUv",
    );
    expect(screen.getByTestId("baseline-disagrees")).toHaveTextContent(
      "Baseline ultimate parent is B (Alpha Holdings); your watch expected A.",
    );
    const log = screen.getByRole("list", { name: "Change log, newest first" });
    const sentence = within(log).getByTestId("expectation-sentence");
    expect(sentence).toHaveTextContent("Your watch expected A.");
    expect(sentence.className).toContain("warn");
    // The materiality chip is untouched by the contradiction.
    expect(within(log.children[0] as HTMLElement).getByTestId("materiality-chip").textContent).toBe("Escalate");
  });

  it("saves the context with a PATCH, null for a cleared field, and shows a refusal where it was typed", async () => {
    window.localStorage.setItem(TOKEN_KEY, "tok");
    const calls: { url: string; method: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const body = init?.body ? JSON.parse(String(init.body)) : null;
        calls.push({ url: String(url), method: init?.method ?? "GET", body });
        if (init?.method === "PATCH" && body?.rationale === "mail a@b.co") {
          return {
            ok: false, status: 422,
            json: async () => ({ detail: { field: "rationale", message: "The rationale cannot carry an e-mail address." } }),
          } as Response;
        }
        return { ok: true, status: 200, json: async () => ({ ...PAYLOAD, watch: PAYLOAD.watches[0] }) } as Response;
      }),
    );
    const { default: userEvent } = await import("@testing-library/user-event");
    const user = userEvent.setup();
    render(<WatchlistPage />);
    const block = await screen.findByTestId("watch-context");
    await user.click(within(block).getByRole("button", { name: "Edit context" }));
    const form = screen.getByRole("form", { name: "Context for Vosper Ltd" });
    expect(form).toHaveTextContent("About the company, not a person");
    const rationale = within(form).getByLabelText(/Why this company is watched/);
    await user.clear(rationale);
    await user.type(rationale, "mail a@b.co");
    await user.click(within(form).getByRole("button", { name: "Save context" }));
    expect(await within(form).findByRole("alert")).toHaveTextContent("cannot carry an e-mail address");

    await user.clear(rationale);
    await user.clear(within(form).getByLabelText(/Expected jurisdiction/));
    await user.click(within(form).getByRole("button", { name: "Save context" }));
    await waitFor(() => expect(screen.queryByRole("form", { name: "Context for Vosper Ltd" })).toBeNull());
    const patches = calls.filter((c) => c.method === "PATCH");
    expect(patches).toHaveLength(2);
    expect(patches[1].url).toMatch(/\/watch\/tok\/items\/213800LH1BZH3DI6G760$/);
    expect(patches[1].body).toEqual({
      expected_jurisdiction: null,
      expected_parent_lei: "A",
      rationale: null,
      onboarding_report_id: "AbCdEfGhIjKlMnOpQrStUv",
    });
  });
});
