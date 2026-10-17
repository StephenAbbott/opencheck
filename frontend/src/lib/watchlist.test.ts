/**
 * What the watchlist says (Phase 215). The backend's diff vocabulary is a
 * closed list (`opencheck/watchlist.py::CHANGE_KINDS`); `describeChange` must
 * word every kind, and the two honesty rules — absence is a finding only when
 * the source answered; say what fired and when each source was reached — are
 * pinned as sentences.
 */

import { describe, expect, it, vi } from "vitest";

import {
  checkedSentence,
  describeChange,
  describeChildren,
  describeEvents,
  eventDay,
  fieldWords,
  gleifLogLines,
  logLineWords,
  entryHeadline,
  registerChangeLines,
  tierChip,
  tierSentence,
  tokenFromLocation,
  triggerSentence,
  type WatchEntry,
} from "./watchlist";

// Mirror of CHANGE_KINDS in backend/opencheck/watchlist.py.
const KINDS = [
  "gleif_field",
  "legal_name",
  "jurisdiction",
  "register_status",
  "founding_date",
  "legal_form",
  "dissolution_date",
  "identifier",
  "signal_new",
  "signal_retired",
  "signal_unchecked",
  "context_new",
  "context_retired",
  "context_unchecked",
  "coverage_changed",
  "coverage_unchecked",
  "verdict",
];

function entry(over: Partial<WatchEntry> = {}): WatchEntry {
  return {
    id: 1,
    lei: "213800LH1BZH3DI6G760",
    legal_name: "Vosper Ltd",
    created_at: "2026-09-16T10:00:00Z",
    tier: "gleif",
    trigger: { tier: "gleif", publish: "2026-09-16 00:00:00", fields: ["registration_status"] },
    changes: [],
    checked: [],
    degraded: [],
    ...over,
  };
}

describe("describeChange", () => {
  it("words every kind the backend can emit", () => {
    for (const kind of KINDS) {
      const s = describeChange({
        kind,
        field: "registration_status",
        scheme: "GB-COH",
        code: "SANCTIONED",
        sources: ["opensanctions"],
        degraded: ["opensanctions"],
        missing: ["kvk"],
        old: { liveness: "live", answered: 10, applicable: 10 },
        new: { liveness: "terminal", answered: 9, applicable: 10 },
      });
      expect(s, kind).not.toBe(`${kind.replace(/_/g, " ")}.`);
      expect(s.length, kind).toBeGreaterThan(10);
    }
  });

  it("never words an unchecked signal as the signal going away", () => {
    const s = describeChange({ kind: "signal_unchecked", code: "SANCTIONED", sources: ["opensanctions"], degraded: ["opensanctions"] });
    expect(s).toMatch(/Could not re-check/);
    expect(s).toMatch(/Not a clean result/);
    expect(s).not.toMatch(/no longer/);
    const r = describeChange({ kind: "signal_retired", code: "SANCTIONED", sources: ["opensanctions"] });
    expect(r).toMatch(/no longer reported/);
    expect(r).toMatch(/answered/);
  });

  it("says a coverage fall from degraded sources is about the check, not the company", () => {
    const s = describeChange({
      kind: "coverage_unchecked",
      old: { answered: 10, applicable: 10 },
      new: { answered: 9, applicable: 10 },
      missing: ["kvk"],
    });
    expect(s).toBe("Coverage fell to 9 of 10 because kvk could not be reached — a fact about the check, not the company.");
  });

  it("uses the source label it is given", () => {
    const s = describeChange({ kind: "signal_new", code: "PEP", sources: ["opensanctions"] }, () => "OpenSanctions");
    expect(s).toBe("New risk signal PEP (OpenSanctions).");
  });

  it("names GLEIF fields in words", () => {
    expect(describeChange({ kind: "gleif_field", field: "registration_status", old: "ISSUED", new: "LAPSED" })).toBe(
      "GLEIF LEI registration status: ISSUED → LAPSED.",
    );
    expect(describeChange({ kind: "register_status", old: { liveness: "live" }, new: { liveness: "terminal", source_id: "companies_house" } }, (id) => id)).toBe(
      "Register status: live → terminal (companies_house).",
    );
  });

  it("names the Phase 300 fields and joins list values", () => {
    expect(fieldWords("registered_as")).toBe("register number");
    expect(fieldWords("hq_address_country")).toBe("headquarters country");
    expect(fieldWords("conformity_flag")).toBe("policy conformity flag");
    expect(
      describeChange({ kind: "gleif_field", field: "successors", old: null, new: ["2138000000000000T178 — Mirror Top plc", "Other Ltd"] }),
    ).toBe("GLEIF successor entities: — → 2138000000000000T178 — Mirror Top plc; Other Ltd.");
    expect(describeChange({ kind: "gleif_field", field: "successors", old: [], new: null })).toBe("GLEIF successor entities: — → —.");
  });
});

describe("triggerSentence and headline", () => {
  it("opens a GLEIF entry with the delta that fired and the fields", () => {
    expect(triggerSentence(entry())).toBe(
      "GLEIF published a change to this record in its 2026-09-16 00:00:00 delta (LEI registration status).",
    );
  });

  it("opens an OpenSanctions entry with the op, the entity and how it matched", () => {
    const e = entry({
      tier: "opensanctions",
      trigger: { tier: "opensanctions", op: "ADD", caption: "Vosper Limited", matched_on: "name", datasets: ["eu_fsf"] },
    });
    expect(triggerSentence(e)).toBe("OpenSanctions added Vosper Limited, matching this company by name — eu_fsf.");
  });

  it("headlines the worst change first", () => {
    expect(entryHeadline(entry())).toBe("Vosper Ltd: re-run found no difference");
    expect(entryHeadline(entry({ changes: [{ kind: "gleif_field" }, { kind: "signal_new" }] }))).toBe("Vosper Ltd: new risk signal");
    expect(entryHeadline(entry({ changes: [{ kind: "gleif_field" }, { kind: "register_status" }] }))).toBe(
      "Vosper Ltd: register status changed",
    );
    expect(entryHeadline(entry({ legal_name: null, changes: [{ kind: "verdict" }, { kind: "legal_form" }] }))).toBe(
      "213800LH1BZH3DI6G760: 2 changes",
    );
  });

  it("headlines a legal entity event above everything else", () => {
    const changes = [{ kind: "register_status" }, { kind: "gleif_field", field: "corporate_events" }] as WatchEntry["changes"];
    expect(entryHeadline(entry({ changes }))).toBe("Vosper Ltd: legal entity event recorded");
  });

  it("opens a resync entry with the rebuild, not a delta", () => {
    const e = entry({ trigger: { tier: "gleif", publish: "2026-10-08 00:00:00", fields: ["corporate_events"], resync: true } });
    expect(triggerSentence(e)).toBe(
      "OpenCheck's GLEIF mirror was rebuilt from the 2026-10-08 00:00:00 Golden Copy and now reads legal entity events; GLEIF's record changed after this company was first watched (legal entity events).",
    );
  });
});

// Phase 305: the same table as EVENT_DAY_CASES in backend/tests/test_gleif_events.py.
describe("eventDay", () => {
  const cases: [string | null, string][] = [
    ["2024-10-24T22:00:00Z", "2024-10-25"],
    ["2026-10-05T23:00:00Z", "2026-10-06"],
    ["2026-10-06T00:00:00Z", "2026-10-06"],
    ["2023-06-30T18:30:00Z", "2023-07-01"],
    ["2023-06-30T16:00:00Z", "2023-07-01"],
    ["2024-01-31T10:59:19Z", "2024-01-31"],
    ["2024-01-31T11:59:59Z", "2024-01-31"],
    ["2024-01-31T12:00:00Z", "2024-02-01"],
    ["2024-12-31T23:00:00Z", "2025-01-01"],
    ["2024-02-28T22:00:00Z", "2024-02-29"],
    ["2026-10-05T23:00:00+00:00", "2026-10-06"],
    ["2026-10-05T23:00:00+02:00", "2026-10-05"],
    ["2026-10-06T01:00:00-05:00", "2026-10-06"],
    ["2026-10-06", "2026-10-06"],
    ["2026-13-01T00:00:00Z", ""],
    ["not a date", ""],
    ["", ""],
    [null, ""],
  ];
  it.each(cases)("%s → %s", (input, expected) => {
    expect(eventDay(input)).toBe(expected);
  });
});

describe("describeEvents", () => {
  const liq = { type: "LIQUIDATION", status: "IN_PROGRESS", effective: "2026-10-05T23:00:00Z", recorded: "2026-10-06T08:00:00Z" };
  const done = { ...liq, status: "COMPLETED" };
  const ma = { type: "MERGERS_AND_ACQUISITIONS", status: "COMPLETED", effective: "2026-10-01T00:00:00Z", recorded: "2026-10-01T00:00:00Z" };

  it("words the feed's sentences, word for word", () => {
    expect(describeEvents(null, [liq])).toBe("GLEIF recorded a legal entity event: liquidation (in progress), effective 2026-10-06.");
    expect(describeEvents([liq], [done])).toBe(
      "GLEIF recorded a legal entity event: liquidation: in progress → completed, effective 2026-10-06.",
    );
    expect(describeEvents([], [liq, ma])).toBe(
      "GLEIF recorded legal entity events: liquidation (in progress), effective 2026-10-06; merger or acquisition (completed), effective 2026-10-01.",
    );
    expect(describeEvents([liq], null)).toBe("GLEIF no longer lists: liquidation (in progress), effective 2026-10-06.");
  });

  it("is what describeChange says for a corporate_events change", () => {
    expect(describeChange({ kind: "gleif_field", field: "corporate_events", old: null, new: [liq] })).toBe(
      "GLEIF recorded a legal entity event: liquidation (in progress), effective 2026-10-06.",
    );
    expect(fieldWords("corporate_events")).toBe("legal entity events");
  });
});

describe("checkedSentence", () => {
  it("counts the sources actually reached and the dates they were", () => {
    expect(
      checkedSentence([
        { source_id: "gleif", liveness: "live", retrieved_at: "2026-09-16T10:00:00Z" },
        { source_id: "companies_house", liveness: "cached", retrieved_at: "2026-09-15T09:00:00Z" },
        { source_id: "kvk", liveness: "stub", retrieved_at: null },
      ]),
    ).toBe("2 sources checked as a result on 2026-09-15 and 2026-09-16.");
    expect(checkedSentence([])).toBeNull();
  });
});

describe("tierSentence", () => {
  it("states the mirror's size and the last delta, and that registers are never polled", () => {
    const s = tierSentence({
      gleif: { available: true, watermark: "2026-09-16 00:00:00", refresh_enabled: true, last_applied_at: null, last_delta: "IntraDay", rows_applied: { entities: 3903 }, record_count: 3424074 },
      opensanctions: { available: true, last_version: "20260916065435-fec", last_checked_at: null },
      worker: { enabled: true, interval_s: 300 },
    });
    expect(s).toContain("3,424,074 LEI records as of 2026-09-16 00:00:00 UTC");
    expect(s).toContain("changed 3,903 of them");
    expect(s).toContain("last version 20260916065435-fec");
    expect(s).toContain("never polled");
    expect(tierSentence(null)).toBe("");
  });

  it("says when a tier cannot fire on this instance", () => {
    const s = tierSentence({
      gleif: { available: false, watermark: null, refresh_enabled: false, last_applied_at: null, last_delta: null, rows_applied: {}, record_count: null },
      opensanctions: { available: false, last_version: null, last_checked_at: null },
      worker: { enabled: false, interval_s: 0 },
    });
    expect(s).toContain("no mirror on this instance");
    expect(s).toContain("sanctions tier is off");
  });

  it("names an OpenSanctions backlog and a gap (Phase 260)", () => {
    const base = {
      gleif: { available: false, watermark: null, refresh_enabled: false, last_applied_at: null, last_delta: null, rows_applied: {}, record_count: null },
      worker: { enabled: true, interval_s: 300 },
    };
    const s = tierSentence({
      ...base,
      opensanctions: {
        available: true, last_version: "v12", last_checked_at: null, backlog: 8,
        gaps: [{ reason: "aged_out", after: "v0", before: "v1", detected_at: "2026-09-28T10:00:00Z" }],
      },
    });
    expect(s).toContain("8 OpenSanctions versions are still to be read, oldest first.");
    expect(s).toContain("On 2026-09-28 some OpenSanctions versions could not be read");
    const quiet = tierSentence({ ...base, opensanctions: { available: true, last_version: "v12", last_checked_at: null, backlog: 0, gaps: [] } });
    expect(quiet).not.toContain("still to be read");
    expect(quiet).not.toContain("could not be read");
    const one = tierSentence({ ...base, opensanctions: { available: true, last_version: "v12", last_checked_at: null, backlog: 1 } });
    expect(one).toContain("1 OpenSanctions version is still to be read");
  });
});

describe("catch-up entries (Phase 260)", () => {
  const entry = (trigger: Record<string, unknown>) => ({
    id: 3, lei: "213800LH1BZH3DI6G760", legal_name: "Vosper Ltd", created_at: "2026-09-28T00:00:00Z",
    tier: "catch_up" as const, trigger, changes: [], checked: [], degraded: [],
  });

  it("says why every company was re-checked, in either case", () => {
    expect(triggerSentence(entry({ reason: "aged_out", after: "v0", before: "v9" }))).toBe(
      "OpenSanctions versions published after v0 were no longer listed when the watcher came to read them, so every watched company was re-checked.",
    );
    expect(triggerSentence(entry({ reason: "delta_missing", after: "v4", before: "v4" }))).toContain(
      "listed version v4 but its delta file could not be downloaded",
    );
  });

  it("has its own chip, never the sanctions tone", () => {
    expect(tierChip("catch_up")).toEqual({ label: "Catch-up re-check", tone: "context" });
    expect(tierChip("opensanctions").tone).toBe("risk");
    expect(tierChip("manual").label).toBe("By hand");
    expect(tierChip("gleif").label).toBe("GLEIF delta");
  });
});

describe("register tier entries (Estonia)", () => {
  const entry = (trigger: Record<string, unknown>, changes: WatchEntry["changes"] = []) =>
    ({
      id: 4, lei: "5493005044RTLQ5RZU70", legal_name: "Eesti Energia AS", created_at: "2026-10-17T12:00:00Z",
      tier: "register" as const, trigger, changes, checked: [], degraded: [],
    }) as WatchEntry;

  it("says which days of the register's open data and which company", () => {
    expect(triggerSentence(entry({ code: "10421629", cuts: ["2026-10-17"] }))).toBe(
      "The Estonian e-Business Register recorded a change to this company in its open data of 2026-10-17 (registry code 10421629).",
    );
    expect(triggerSentence(entry({ cuts: ["2026-10-16", "2026-10-17"] }))).toContain(
      "its open data of 2026-10-16 and 2026-10-17.",
    );
  });

  it("words the counts as the feed does, never a person", () => {
    expect(
      registerChangeLines({
        removed: true, officers: [2, 2], beneficial_owners: [0, 1], supervisory_board: [3, 0],
        name: ["A OÜ", "B OÜ"], legal_form: ["OÜ", "AS"], status: ["R", "Q"],
      }),
    ).toEqual([
      "The company is no longer in the register's open data — deleted companies are not published.",
      "Persons on the registry card: changed (2 entries before and after).",
      "Beneficial owners: none → 1 entry.",
      "Supervisory board: 3 → none entries.",
      "Name: A OÜ → B OÜ.",
      "Status: Entered in the register → Q.",
      "Legal form: OÜ → AS.",
    ]);
    expect(registerChangeLines(undefined)).toEqual([]);
  });

  it("is news without a snapshot difference, and has its own chip", () => {
    expect(entryHeadline(entry({}))).toBe("Eesti Energia AS: Estonian register record changed");
    expect(tierChip("register")).toEqual({ label: "Register change", tone: "accent" });
  });

  it("explains a register gap as a catch-up", () => {
    const gap = { ...entry({ reason: "register_gap", after: "2026-10-11", before: "2026-10-13" }), tier: "catch_up" as const };
    expect(triggerSentence(gap)).toBe(
      "The Estonian e-Business Register's changes between 2026-10-11 and 2026-10-13 could not be read, so every watched Estonian company was re-checked.",
    );
  });

  it("names the tier in the honesty line only where the index is held", () => {
    const base = {
      gleif: { available: false, watermark: null, refresh_enabled: false, last_applied_at: null, last_delta: null, rows_applied: {}, record_count: null },
      opensanctions: { available: false, last_version: null, last_checked_at: null },
      worker: { enabled: true, interval_s: 300 },
    };
    const s = tierSentence({
      ...base,
      register: { available: true, index_cut: "2026-10-17", last_cut: "2026-10-17", waiting: false, gaps: [] },
    });
    expect(s).toContain("this instance holds 2026-10-17");
    expect(s).toContain("No other national register is polled");
    expect(tierSentence(base)).toContain("National registers are never polled");
  });
});

describe("the token", () => {
  it("prefers the URL's token over the stored one", () => {
    expect(tokenFromLocation("?token=abc", "stored")).toBe("abc");
    expect(tokenFromLocation("?lei=x", "stored")).toBe("stored");
    expect(tokenFromLocation("", null)).toBeNull();
  });
});

describe("feedHelp (Phase 234)", () => {
  it("says an unopened list is deleted, and that the feed reader counts", async () => {
    const { feedHelp } = await import("./watchlist");
    expect(feedHelp()).toContain("90 days is deleted");
    expect(feedHelp()).toContain("feed reader");
  });
});

describe("describeChildren (Phase 302)", () => {
  const names = { A: "Alpha Ltd", B: "Beta GmbH" };

  it("words the feed's sentences, word for word", () => {
    expect(describeChildren([], ["A"], names)).toBe("GLEIF lists a new direct subsidiary: Alpha Ltd (A).");
    expect(describeChildren(["A"], ["B", "C"], names)).toBe(
      "GLEIF lists 2 new direct subsidiaries: Beta GmbH (B); C. GLEIF no longer lists a direct subsidiary: Alpha Ltd (A).",
    );
    expect(describeChildren(["A"], ["A"], names)).toBe("GLEIF direct subsidiaries changed.");
  });

  it("is what describeChange says, and headlines a subsidiaries-only entry", () => {
    expect(describeChange({ kind: "gleif_field", field: "direct_children", old: [], new: ["A"], names })).toBe(
      "GLEIF lists a new direct subsidiary: Alpha Ltd (A).",
    );
    expect(fieldWords("direct_children")).toBe("direct subsidiaries");
    const changes = [{ kind: "gleif_field", field: "direct_children" }] as WatchEntry["changes"];
    expect(entryHeadline(entry({ changes }))).toBe("Vosper Ltd: direct subsidiaries changed");
    const mixed = [...changes, { kind: "gleif_field", field: "legal_name" }] as WatchEntry["changes"];
    expect(entryHeadline(entry({ changes: mixed }))).toBe("Vosper Ltd: GLEIF record changed");
  });
});

describe("gleifLogLines (Phase 303)", () => {
  const item = (date: string, label: string, old: string | null, nw: string | null, type = "UPDATE") => ({
    date, record: "LEI", label, type, old, new: nw,
  });

  it("words the feed's lines, word for word", () => {
    expect(logLineWords(item("2026-09-16", "registration status", "ISSUED", "LAPSED"))).toBe(
      "2026-09-16: registration status ISSUED → LAPSED",
    );
    expect(logLineWords(item("2026-09-30", "legal entity event type", null, "LIQUIDATION", "INITIAL"))).toBe(
      "2026-09-30: legal entity event type set to LIQUIDATION",
    );
    expect(logLineWords(item("2026-09-30", "other name", "X", null, "DELETE"))).toBe("2026-09-30: other name removed (was X)");
  });

  it("leads with the span, caps at ten lines and counts the rest", () => {
    const items = Array.from({ length: 13 }, (_, i) => item(`2026-09-${String(14 - i).padStart(2, "0")}`, "legal name", "A", "B"));
    const out = gleifLogLines({ available: true, since: "2026-09-01T00:00:00Z", items, more: 1 });
    expect(out?.lead).toBe("GLEIF's own modification log since 2026-09-01:");
    expect(out?.items.length).toBe(11);
    expect(out?.items[10]).toBe("and 4 more.");
  });

  it("says when GLEIF had nothing, or could not be read, and nothing when no log was fetched", () => {
    expect(gleifLogLines({ available: true, since: "2026-09-01T00:00:00Z", items: [], more: 0 })?.lead).toBe(
      "GLEIF's own modification log has no change to this record since 2026-09-01 other than renewal dates.",
    );
    expect(
      gleifLogLines({ available: false, reason: "GLEIF's API was rate-limited", since: "2026-09-01T00:00:00Z", items: [], more: 0 })?.lead,
    ).toBe("GLEIF's own modification log could not be read since 2026-09-01: GLEIF's API was rate-limited.");
    expect(gleifLogLines(null)).toBeNull();
    expect(gleifLogLines({ available: true, since: null, items: [], more: 0 }, "GLEIF's log")?.lead).toBe(
      "GLEIF's log has no change to this record other than renewal dates.",
    );
  });
});

describe("materiality (pKYC stage A)", () => {
  it("renders what the entry carries, with the tone the tier asserts", async () => {
    const { materialityChip, isMaterial, filterEntries, hiddenByFilterSentence, MATERIALITY_LABELS } = await import("./watchlist");
    expect(materialityChip("escalate")).toEqual({ label: "Escalate", tone: "risk" });
    expect(materialityChip("recheck")).toEqual({ label: "Recheck the source", tone: "warn" });
    expect(materialityChip("note")).toEqual({ label: "Note only", tone: "neutral" });
    expect(materialityChip(undefined)).toBeNull();
    // note is never red; escalate is never neutral
    expect(materialityChip("note")?.tone).not.toBe("risk");
    expect(materialityChip("escalate")?.tone).not.toBe("neutral");
    expect(Object.keys(MATERIALITY_LABELS)).toEqual(["note", "recheck", "escalate"]);
    expect([isMaterial("note"), isMaterial("recheck"), isMaterial("escalate"), isMaterial(undefined)]).toEqual([
      false, true, true, false,
    ]);
    const entries = [
      { id: 1, materiality: "note" },
      { id: 2, materiality: "recheck" },
      { id: 3 },
    ] as unknown as import("./watchlist").WatchEntry[];
    expect(filterEntries(entries, "all")).toHaveLength(3);
    expect(filterEntries(entries, "material").map((e) => e.id)).toEqual([2]);
    expect(hiddenByFilterSentence(3, 1)).toBe("2 note-only entries are hidden; nothing shown here is recorded for the history alone.");
    expect(hiddenByFilterSentence(2, 1)).toBe("1 note-only entry is hidden; nothing shown here is recorded for the history alone.");
    expect(hiddenByFilterSentence(2, 2)).toBeNull();
  });

  it("reads the filter from the URL first, then storage, and never throws without storage", async () => {
    const { filterFromLocation, storeFilter, FILTER_KEY } = await import("./watchlist");
    // The lib suite runs in node: no window at all, which is also what a
    // blocked-storage browser looks like to these helpers.
    expect(filterFromLocation("?show=material")).toBe("material");
    expect(filterFromLocation("?show=all")).toBe("all");
    expect(filterFromLocation("")).toBe("all");
    expect(() => storeFilter("material")).not.toThrow();
    const store = new Map<string, string>();
    vi.stubGlobal("window", {
      localStorage: {
        getItem: (k: string) => store.get(k) ?? null,
        setItem: (k: string, v: string) => void store.set(k, v),
        removeItem: (k: string) => void store.delete(k),
      },
    });
    try {
      storeFilter("material");
      expect(store.get(FILTER_KEY)).toBe("material");
      expect(filterFromLocation("")).toBe("material");
      storeFilter("all");
      expect(store.has(FILTER_KEY)).toBe(false);
      expect(filterFromLocation("")).toBe("all");
    } finally {
      vi.unstubAllGlobals();
    }
  });

  // ---- pKYC stage B: what the watcher expected ----------------------------

  it("words a contradicted expectation in warn and a met one in ok, and nothing without one", async () => {
    const { expectationSentence, entryContradicted, EXPECTATION_WORDS } = await import("./watchlist");
    const miss = { kind: "gleif_field", field: "ultimate_parent_lei", expectation: { field: "ultimate_parent_lei", expected: "X", met: false } };
    const hit = { kind: "jurisdiction", expectation: { field: "jurisdiction", expected: "GB", met: true } };
    expect(expectationSentence(miss as never)).toEqual({ text: "Your watch expected X.", tone: "warn" });
    expect(expectationSentence(hit as never)).toEqual({ text: "As your watch expected.", tone: "ok" });
    expect(expectationSentence({ kind: "legal_name" })).toBeNull();
    // The words are the backend's (pinned by tests/test_watchlist_context.py).
    expect(EXPECTATION_WORDS.contradicted).toContain("{expected}");
    const entry = { changes: [hit, miss] } as unknown as import("./watchlist").WatchEntry;
    expect(entryContradicted(entry)).toBe(true);
    expect(entryContradicted({ changes: [hit] } as unknown as import("./watchlist").WatchEntry)).toBe(false);
  });

  it("words a baseline disagreement, reads a saved report id from a link, and knows an empty context", async () => {
    const { disagreementSentence, reportIdFromInput, hasContext } = await import("./watchlist");
    expect(disagreementSentence({ field: "jurisdiction", expected: "GB", actual: "GR" })).toBe(
      "Baseline jurisdiction is GR; your watch expected GB.",
    );
    expect(disagreementSentence({ field: "ultimate_parent_lei", expected: "X", actual: "Y" }, "Top Co")).toBe(
      "Baseline ultimate parent is Y (Top Co); your watch expected X.",
    );
    const id = "AbCdEfGhIjKlMnOpQrStUv";
    expect(reportIdFromInput(`https://opencheck.world/report/${id}`)).toBe(id);
    expect(reportIdFromInput(`https://api.opencheck.world/share/saved/${id}/`)).toBe(id);
    expect(reportIdFromInput(` ${id} `)).toBe(id);
    expect(reportIdFromInput("https://example.com/x")).toBeNull();
    expect(reportIdFromInput("")).toBeNull();
    expect(hasContext(undefined)).toBe(false);
    expect(hasContext({ expected_jurisdiction: null, expected_parent_lei: null, rationale: null, onboarding_report_id: null, set_at: null })).toBe(false);
    expect(hasContext({ expected_jurisdiction: "GB", expected_parent_lei: null, rationale: null, onboarding_report_id: null, set_at: "t" })).toBe(true);
  });

  // ---- pKYC stage C: when OpenCheck last looked -----------------------------

  it("shows a Review due chip in warn only when the review is due, and words the review tier", async () => {
    const { reviewChip, tierChip, entryHeadline, triggerSentence, REVIEW_WORDS } = await import("./watchlist");
    const base = { last_run_at: null, last_run_days: 3, never_rerun: false, due_days: 90, sources_total: 2, sources_from_older_copy: [], sources_not_reached: [], sentence: "x" };
    expect(reviewChip(undefined)).toBeNull();
    expect(reviewChip({ ...base, due: false })).toBeNull();
    expect(reviewChip({ ...base, due: true })).toEqual({ label: REVIEW_WORDS.due_chip, tone: "warn" });
    expect(tierChip("review")).toEqual({ label: "Review", tone: "neutral" });
    const entry = {
      id: 9, lei: "", legal_name: null, created_at: "2026-10-18T00:00:00Z", tier: "review",
      trigger: { reason: "list_expiring", stale_days: 90, grace_days: 30, delete_after: "2026-11-17T00:00:00Z" },
      changes: [], checked: [], degraded: [], materiality: "note", materiality_rules: 1,
    } as unknown as import("./watchlist").WatchEntry;
    expect(entryHeadline(entry)).toBe("This list: nobody has opened it for a while — it will be deleted unless opened");
    expect(triggerSentence(entry)).toBe(
      "Nobody has opened or polled this watchlist for 90 days. It will be deleted, with its watches and this log, after 2026-11-17 unless it is opened or its feed is read before then. Opening it keeps it.",
    );
  });
});
