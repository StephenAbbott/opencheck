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
  entryHeadline,
  tierSentence,
  tokenFromLocation,
  triggerSentence,
  WATCH_PROMISE,
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
});

describe("the token", () => {
  it("prefers the URL's token over the stored one", () => {
    expect(tokenFromLocation("?token=abc", "stored")).toBe("abc");
    expect(tokenFromLocation("?lei=x", "stored")).toBe("stored");
    expect(tokenFromLocation("", null)).toBeNull();
  });
});

describe("the promise", () => {
  it("says what watching is before the button is pressed, and what it is not", () => {
    expect(WATCH_PROMISE).toMatch(/only when GLEIF or OpenSanctions publish a change/);
    expect(WATCH_PROMISE).toMatch(/no polling/);
    expect(WATCH_PROMISE).toMatch(/no account/);
  });
});
