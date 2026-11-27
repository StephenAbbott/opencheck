/**
 * The /changes page (the daily-deltas ticket). `lib/dailyChanges.test.ts`
 * pins the words; this pins what reaches the page: the headline band is open
 * and cut at five, the other bands are folded, an OpenSanctions row wears the
 * risk chip, new LEIs are counted and not listed, and "Run a check" hands the
 * LEI back rather than fetching anything.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import ChangesPage from "./ChangesPage";
import type { DailyDay, DailyIndex, DailyRow } from "../lib/dailyChanges";

const fetchDailyChangesIndex = vi.hoisted(() => vi.fn());
const fetchDailyChangesDay = vi.hoisted(() => vi.fn());
vi.mock("../lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../lib/api")>()),
  fetchDailyChangesIndex,
  fetchDailyChangesDay,
}));

const DAY = "2026-11-12";

const row = (i: number, over: Partial<DailyRow> = {}): DailyRow => ({
  lei: `5493000000000000${String(i).padStart(4, "0")}`,
  name: `COMPANY ${i}`,
  jurisdiction: "GB",
  source: "gleif",
  band: "headline",
  reasons: ["parent_changed"],
  changes: [{ field: "direct_parent_lei", old: null, new: "PARENTLEI" }],
  as_of: `${DAY} 08:00:00`,
  ...over,
});

const index: DailyIndex = {
  available: true,
  days: [{ day: DAY, bands: {}, sources: {} }],
  files: [{ day: DAY, bods: "", csv: "", json: "", bytes: 2048, statements: 321, leis: 9 }],
};

const day: DailyDay = {
  day: DAY,
  complete: true,
  opensanctions_notice: "",
  counts: { headline: 7, ceased: 1, new: 1204 },
  sources: { gleif: { churn: 14310 } },
  rows: [
    row(0, { source: "opensanctions", reasons: ["os_topic_added"], changes: [{ topics_added: ["sanction"] }] }),
    ...Array.from({ length: 6 }, (_, i) => row(i + 1)),
    row(9, { band: "ceased", reasons: ["entity_inactive"], name: "CEASED CO" }),
  ],
};

function renderPage(onOpen = vi.fn()) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <ChangesPage onOpen={onOpen} onOpenWatchlist={vi.fn()} />
    </QueryClientProvider>,
  );
  return onOpen;
}

describe("ChangesPage", () => {
  beforeEach(() => {
    fetchDailyChangesIndex.mockResolvedValue(index);
    fetchDailyChangesDay.mockResolvedValue(day);
  });

  it("opens the headline band, cut at five, and folds the rest", async () => {
    renderPage();
    expect(await screen.findByRole("heading", { level: 2, name: "What changed on 12 November 2026" })).toBeTruthy();
    const headline = await screen.findByRole("region", { name: "Ownership and sanctions" });
    expect(within(headline).getAllByRole("listitem")).toHaveLength(5);
    expect(within(headline).getByText("Listing added").className).toMatch(/oo-risk/);
    await userEvent.click(within(headline).getByRole("button", { name: "Show the other 2" }));
    expect(within(headline).getAllByRole("listitem")).toHaveLength(7);
    // The ceased company is in a folded section, not on the open list.
    expect(screen.getByText("CEASED CO").closest("details")?.open).toBe(false);
    expect(screen.getByText("new LEIs. They are in the day's file.")).toBeTruthy();
    expect(screen.getByText(/321 statements/)).toBeTruthy();
  });

  it("hands the LEI back to run a check", async () => {
    const onOpen = renderPage();
    const headline = await screen.findByRole("region", { name: "Ownership and sanctions" });
    await userEvent.click(within(headline).getAllByRole("button", { name: "Run a check" })[0]);
    expect(onOpen).toHaveBeenCalledWith("54930000000000000000");
  });

  it("says when the server does not run the list", async () => {
    fetchDailyChangesIndex.mockRejectedValue(new Error("503"));
    renderPage();
    expect(await screen.findByRole("alert")).toHaveProperty(
      "textContent",
      "The daily changes list is not available on this server right now.",
    );
  });
});
