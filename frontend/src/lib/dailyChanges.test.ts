import { describe, expect, it } from "vitest";

import {
  changeLines,
  chooseDay,
  countedNotListed,
  dayFromSearch,
  formatBytes,
  longDay,
  neighbours,
  reasonBreakdown,
  recordLink,
  rowChip,
  sourceLine,
  type DailyDay,
  type DailyIndex,
  type DailyRow,
} from "./dailyChanges";

const row = (over: Partial<DailyRow> = {}): DailyRow => ({
  lei: "5493001KJTIIGC8Y1R12",
  name: "ACME LTD",
  jurisdiction: "GB",
  source: "gleif",
  band: "headline",
  reasons: ["parent_changed"],
  changes: [{ field: "direct_parent_lei", old: "AAA", new: "BBB" }],
  as_of: "2026-11-12 08:00:00",
  ...over,
});

const index: DailyIndex = {
  available: true,
  days: [
    { day: "2026-11-13", bands: {}, sources: {} },
    { day: "2026-11-12", bands: {}, sources: {} },
    { day: "2026-11-11", bands: {}, sources: {} },
  ],
};

describe("dailyChanges", () => {
  it("wears the risk tone only on an OpenSanctions listing", () => {
    expect(rowChip(row({ source: "opensanctions", reasons: ["os_topic_added"] })).tone).toBe("risk");
    expect(rowChip(row())).toEqual({ label: "Parent changed", tone: "context" });
    expect(rowChip(row({ band: "ceased", reasons: ["entity_inactive"] })).tone).toBe("neutral");
  });

  it("prints a change as what, before and now", () => {
    expect(changeLines(row())).toEqual([{ what: "Direct parent", before: "AAA", now: "BBB" }]);
    expect(changeLines(row({ changes: [{ field: "direct_parent_lei", old: null, new: "BBB" }] }))[0].before).toBe("none");
    const os = row({ source: "opensanctions", changes: [{ topics_added: ["sanction"] }] });
    expect(changeLines(os)).toEqual([{ what: "OpenSanctions topics added: sanctions listing" }]);
    const ev = row({ changes: [{ field: "corporate_events", new: [{ type: "CHANGE_LEGAL_FORM", status: "COMPLETED" }] }] });
    expect(changeLines(ev)[0].what).toBe("Legal entity event recorded: change legal form (completed)");
  });

  it("names an added dataset by its OpenSanctions title, else by its code", () => {
    const os = row({
      source: "opensanctions",
      changes: [{ datasets_added: ["ca_dfatd_sema_sanctions"], topics_added: ["reg.warn"] }],
    });
    const titles = { ca_dfatd_sema_sanctions: "Canadian Consolidated Autonomous Sanctions List" };
    expect(changeLines(os, titles)).toEqual([
      { what: "OpenSanctions topics added: regulatory warning" },
      { what: "Added to: Canadian Consolidated Autonomous Sanctions List" },
    ]);
    expect(changeLines(os)[1]).toEqual({ what: "Added to: ca_dfatd_sema_sanctions" });
    const two = row({ source: "opensanctions", changes: [{ datasets_added: ["a", "b"] }] });
    expect(changeLines(two, { a: "List A" })).toEqual([{ what: "Added to these lists: List A; b" }]);
  });

  it("links a row to the record its change is on", () => {
    expect(recordLink(row()).href).toContain("search.gleif.org");
    const os = row({ source: "opensanctions", ref: { opensanctions_id: "NK-a", url: "https://www.opensanctions.org/entities/NK-a/" } });
    expect(recordLink(os)).toEqual({ href: "https://www.opensanctions.org/entities/NK-a/", label: "OpenSanctions record" });
    expect(sourceLine(row())).toBe("GLEIF · 08:00 UTC delta");
  });

  it("chooses the asked day if held, else the newest complete one", () => {
    expect(chooseDay(index, "2026-11-11", "2026-11-13")).toBe("2026-11-11");
    expect(chooseDay(index, "2025-01-01", "2026-11-13")).toBe("2026-11-12");
    expect(chooseDay(index, null, "2026-11-14")).toBe("2026-11-13");
    expect(chooseDay({ available: true, days: [] }, null, "2026-11-14")).toBeNull();
    expect(neighbours(index, "2026-11-12")).toEqual({ older: "2026-11-11", newer: "2026-11-13" });
    expect(dayFromSearch("?day=2026-11-12")).toBe("2026-11-12");
    expect(dayFromSearch("?day=yesterday")).toBeNull();
    expect(longDay("2026-11-02")).toBe("2 November 2026");
  });

  it("counts what is not listed, from the day's counters", () => {
    const day: DailyDay = {
      day: "2026-11-12", complete: true, rows: [], opensanctions_notice: "",
      counts: { new: 1204, note: 1530 },
      sources: { gleif: { churn: 14310, sole_proprietor: 212 } },
    };
    expect(countedNotListed(day).map((c) => c.n)).toEqual([1204, 1530, 14310, 212]);
  });

  it("breaks a band down by reason, most frequent first", () => {
    const rows = [row(), row(), row({ reasons: ["successor_named"] })];
    expect(reasonBreakdown(rows).map((p) => `${p.label} ${p.n}`)).toEqual(["Parent changed 2", "Successor named 1"]);
    expect(formatBytes(2_500_000)).toBe("2.4 MB");
    expect(formatBytes(null)).toBeNull();
  });
});
