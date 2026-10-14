import { describe, expect, it } from "vitest";

import type { SourceLiveness } from "./api";
import { addRetried, mergeRetryLiveness, retriedSinceRunNote } from "./sourceRetry";

const STUB: SourceLiveness = {
  liveness: "stub",
  label: "Placeholder data",
  retrieved_at: null,
  detail: null,
};
const SNAPSHOT: SourceLiveness = {
  liveness: "snapshot",
  label: "Snapshot",
  retrieved_at: "2026-10-13T07:38:47Z",
  source_as_of: "2026-10-13T00:00:00Z",
  detail: null,
};

describe("mergeRetryLiveness", () => {
  it("lays the retry's entry over the failed attempt's stub", () => {
    const run = { gleif: SNAPSHOT, jar_lithuania: STUB };
    const merged = mergeRetryLiveness(run, { source_liveness: { jar_lithuania: SNAPSHOT } });
    expect(merged.jar_lithuania).toEqual(SNAPSHOT);
    expect(merged.gleif).toBe(SNAPSHOT);
  });

  it("changes nothing for a server that sends no liveness", () => {
    const run = { jar_lithuania: STUB };
    expect(mergeRetryLiveness(run, {})).toBe(run);
    expect(mergeRetryLiveness(run, { source_liveness: {} })).toBe(run);
  });
});

describe("addRetried", () => {
  it("keeps the retry order and names a source once", () => {
    expect(addRetried(addRetried(addRetried([], "kvk"), "brreg"), "kvk")).toEqual(["kvk", "brreg"]);
  });
});

describe("retriedSinceRunNote", () => {
  it("says nothing before a retry", () => {
    expect(retriedSinceRunNote([])).toBeNull();
  });

  it("names the source, what is out of date and how to fix it", () => {
    const note = retriedSinceRunNote(["jar_lithuania"], { jar_lithuania: "Lithuania JAR" });
    expect(note).toContain("Lithuania JAR was retried");
    expect(note).toContain("verdict and the coverage count");
    expect(note).toContain("Run the check again");
  });

  it("agrees in number with several sources", () => {
    const note = retriedSinceRunNote(["kvk", "brreg"], { kvk: "KvK", brreg: "Brreg" });
    expect(note).toContain("KvK and Brreg were retried");
  });
});
