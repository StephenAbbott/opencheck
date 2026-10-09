import { describe, expect, it } from "vitest";
import {
  ageInDays,
  dataAsOf,
  livenessLabel,
  livenessTitle,
  STALE_AFTER_DAYS,
  type SourceLiveness,
} from "./LivenessBadge";

const make = (over: Partial<SourceLiveness>): SourceLiveness => ({
  liveness: "live",
  label: "Live",
  retrieved_at: null,
  detail: null,
  ...over,
});

describe("ageInDays", () => {
  it("returns null when nothing was retrieved", () => {
    expect(ageInDays(null)).toBeNull();
  });

  it("returns null for an unparseable timestamp", () => {
    expect(ageInDays("not a date")).toBeNull();
  });

  it("counts whole days back", () => {
    const tenDaysAgo = new Date(Date.now() - 10 * 86_400_000).toISOString();
    expect(ageInDays(tenDaysAgo)).toBe(10);
  });

  it("treats a fresh timestamp as zero days old", () => {
    expect(ageInDays(new Date().toISOString())).toBe(0);
  });
});

describe("livenessTitle", () => {
  it("states plainly when no source was contacted", () => {
    expect(livenessTitle(make({ liveness: "stub", label: "Stub" }))).toContain(
      "No source was contacted",
    );
  });

  it("does not invent a retrieval date for curated data", () => {
    // A committed fixture's mtime records when git wrote it locally, which
    // says nothing about when the data left the register.
    const title = livenessTitle(
      make({ liveness: "curated", label: "Curated", detail: "Curated set" }),
    );
    expect(title).toContain("Retrieval date not recorded");
    expect(title).not.toMatch(/\d{4}/);
  });

  it("reports the observed retrieval date when there is one", () => {
    const title = livenessTitle(
      make({
        liveness: "snapshot",
        label: "Snapshot",
        retrieved_at: "2026-02-28T00:00:00Z",
        detail: "Open Ownership bulk dataset",
      }),
    );
    expect(title).toContain("Open Ownership bulk dataset");
    expect(title).toContain("2026");
  });
});

describe("staleness threshold", () => {
  it("is a sane number of days", () => {
    expect(STALE_AFTER_DAYS).toBeGreaterThan(30);
    expect(STALE_AFTER_DAYS).toBeLessThan(400);
  });

  it("classifies an old snapshot as stale and a recent one as not", () => {
    const old = ageInDays(
      new Date(Date.now() - (STALE_AFTER_DAYS + 5) * 86_400_000).toISOString(),
    );
    const recent = ageInDays(new Date(Date.now() - 3 * 86_400_000).toISOString());
    expect(old).not.toBeNull();
    expect(recent).not.toBeNull();
    expect(old! >= STALE_AFTER_DAYS).toBe(true);
    expect(recent! >= STALE_AFTER_DAYS).toBe(false);
  });
});

describe("livenessLabel", () => {
  it("says a live source is live, rather than saying nothing", () => {
    // Until Phase 126 the badge returned null for a live source — "the
    // unmarked default". But on a report where other rows are snapshots or
    // curated sets, no badge is ambiguous: a reader cannot tell live from a
    // badge that failed to render, and cannot scan the column at all. It also
    // sat against this project's rule that an absence is stated in the same
    // voice as a presence.
    const now = new Date("2026-08-22T18:00:00Z");
    expect(
      livenessLabel(make({ liveness: "live", retrieved_at: "2026-08-22T09:00:00Z" }), now),
    ).toBe("Checked today");
  });

  it("dates a live check that was not today", () => {
    const now = new Date("2026-08-22T18:00:00Z");
    expect(
      livenessLabel(make({ liveness: "live", retrieved_at: "2026-08-19T09:00:00Z" }), now),
    ).toMatch(/^Checked 19 Aug/);
  });

  it("does not claim a date it does not have", () => {
    expect(livenessLabel(make({ liveness: "live", retrieved_at: undefined }))).toBe(
      "Checked live",
    );
  });

  it("keeps placeholder data unmistakable", () => {
    // The one state a reader must never read as real data.
    expect(livenessLabel(make({ liveness: "stub", label: "Stub" }))).toBe("Placeholder data");
  });

  it("labels every liveness state", () => {
    for (const liveness of ["live", "cached", "snapshot", "curated", "stub"] as const) {
      expect(livenessLabel(make({ liveness })), liveness).toBeTruthy();
    }
  });
});

// Phase 314: a bulk source carries two clocks. The chip and the staleness rule
// read the register's cut; the tooltip names both.
describe("two clocks (Phase 314)", () => {
  const snap = make({
    liveness: "snapshot",
    retrieved_at: "2026-10-02T04:00:00Z", // OpenCheck rebuilt the index
    source_as_of: "2026-05-31T00:00:00Z", // the register cut the data
  });

  it("dates a snapshot by the register's cut, not the rebuild", () => {
    expect(livenessLabel(snap)).toMatch(/^Snapshot · 31 May 2026/);
  });

  it("falls back to the retrieval when no cut was declared", () => {
    expect(dataAsOf(make({ retrieved_at: "2026-10-02T04:00:00Z" }))).toBe(
      "2026-10-02T04:00:00Z",
    );
    expect(dataAsOf(make({}))).toBeNull();
  });

  it("names both dates in the tooltip", () => {
    const title = livenessTitle(snap);
    expect(title).toContain("Data as of 31 May 2026");
    expect(title).toContain("Retrieved 2 Oct 2026");
  });

  it("ages a snapshot by its cut, so a fresh rebuild of an old dump stays stale", () => {
    const old = new Date(Date.now() - (STALE_AFTER_DAYS + 5) * 86_400_000).toISOString();
    const info = make({
      liveness: "snapshot",
      retrieved_at: new Date().toISOString(),
      source_as_of: old,
    });
    expect(ageInDays(dataAsOf(info))).toBeGreaterThanOrEqual(STALE_AFTER_DAYS);
  });
});
