import { describe, expect, it } from "vitest";
import {
  ageInDays,
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
