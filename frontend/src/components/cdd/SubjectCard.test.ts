import { describe, expect, it } from "vitest";
import { replayAgeLabel } from "./SubjectCard";

describe("replayAgeLabel", () => {
  const now = new Date("2026-07-16T12:10:00Z");

  it("renders sub-minute ages as 'just now'", () => {
    expect(replayAgeLabel("2026-07-16T12:09:30+00:00", now)).toBe("just now");
    expect(replayAgeLabel("2026-07-16T12:10:00+00:00", now)).toBe("just now");
  });

  it("renders whole minutes", () => {
    expect(replayAgeLabel("2026-07-16T12:09:00+00:00", now)).toBe("1 min ago");
    expect(replayAgeLabel("2026-07-16T11:58:00+00:00", now)).toBe("12 min ago");
  });

  it("clamps a future timestamp (clock skew) to 'just now'", () => {
    expect(replayAgeLabel("2026-07-16T12:11:00+00:00", now)).toBe("just now");
  });

  it("falls back to 'recently' on an unparseable timestamp", () => {
    expect(replayAgeLabel("not-a-date", now)).toBe("recently");
  });
});
