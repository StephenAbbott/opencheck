/**
 * What the watchlist says (Phase 215). The backend's diff vocabulary is a
 * closed list (`opencheck/watchlist.py::CHANGE_KINDS`); `describeChange` must
 * word every kind, and the two honesty rules — absence is a finding only when
 * the source answered; say what fired and when each source was reached — are
 * pinned as sentences.
 */

import { describe, expect, it } from "vitest";

import {
  checkedSentence,
  describeChange,
  describeChildren,
  describeEvents,
  fieldWords,
  entryHeadline,
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

describe("describeEvents", () => {
  const liq = { type: "LIQUIDATION", status: "IN_PROGRESS", effective: "2026-10-05T23:00:00Z", recorded: "2026-10-06T08:00:00Z" };
  const done = { ...liq, status: "COMPLETED" };
  const ma = { type: "MERGERS_AND_ACQUISITIONS", status: "COMPLETED", effective: "2026-10-01T00:00:00Z", recorded: "2026-10-01T00:00:00Z" };

  it("words the feed's sentences, word for word", () => {
    expect(describeEvents(null, [liq])).toBe("GLEIF recorded a legal entity event: liquidation (in progress), effective 2026-10-05.");
    expect(describeEvents([liq], [done])).toBe(
      "GLEIF recorded a legal entity event: liquidation: in progress → completed, effective 2026-10-05.",
    );
    expect(describeEvents([], [liq, ma])).toBe(
      "GLEIF recorded legal entity events: liquidation (in progress), effective 2026-10-05; merger or acquisition (completed), effective 2026-10-01.",
    );
    expect(describeEvents([liq], null)).toBe("GLEIF no longer lists: liquidation (in progress), effective 2026-10-05.");
  });

  it("is what describeChange says for a corporate_events change", () => {
    expect(describeChange({ kind: "gleif_field", field: "corporate_events", old: null, new: [liq] })).toBe(
      "GLEIF recorded a legal entity event: liquidation (in progress), effective 2026-10-05.",
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
