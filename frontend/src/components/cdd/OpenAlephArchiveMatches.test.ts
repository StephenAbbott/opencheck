import { describe, expect, it } from "vitest";
import type { OpenAlephScreeningMatch } from "../../lib/api";
import {
  PREVIEW_COUNT,
  archiveRowKey,
  visibleArchiveMatches,
} from "./OpenAlephArchiveMatches";

function match(i: number): OpenAlephScreeningMatch {
  return {
    statement_id: `stmt-${i}`,
    search_name: `Name ${i}`,
    kind: "person",
    matched_name: `Name ${i}`,
    entity_id: `oa-${i}`,
    collection: "Test Collection",
    url: "",
    topics: [],
    surface_form: `Name ${i}`,
    percolator_match: ["name"],
    score: 1,
  };
}

const many = Array.from({ length: PREVIEW_COUNT + 15 }, (_, i) => match(i));

describe("visibleArchiveMatches", () => {
  it("shows everything when at or under the preview cap", () => {
    const few = many.slice(0, PREVIEW_COUNT);
    expect(visibleArchiveMatches(few, false)).toHaveLength(PREVIEW_COUNT);
    expect(visibleArchiveMatches(few.slice(0, 3), false)).toHaveLength(3);
  });

  it("caps long lists at PREVIEW_COUNT when collapsed", () => {
    const visible = visibleArchiveMatches(many, false);
    expect(visible).toHaveLength(PREVIEW_COUNT);
    // Order preserved — the cap is a prefix, not a sample.
    expect(visible[0]).toBe(many[0]);
    expect(visible[PREVIEW_COUNT - 1]).toBe(many[PREVIEW_COUNT - 1]);
  });

  it("shows the full list when expanded", () => {
    expect(visibleArchiveMatches(many, true)).toHaveLength(many.length);
  });

  it("handles an empty list", () => {
    expect(visibleArchiveMatches([], false)).toEqual([]);
  });
});

describe("archiveRowKey", () => {
  it("collapses two records for one party in one collection", () => {
    // Live on BP: KATHERINE ANNE THOMSON appeared twice, two OpenAleph
    // records in the Companies House PSC collection with nothing on the row
    // to tell them apart.
    const a = { ...match(1), entity_id: "oa-a", statement_id: "s-a" };
    const b = { ...match(1), entity_id: "oa-b", statement_id: "s-b" };
    expect(archiveRowKey(a)).toBe(archiveRowKey(b));
  });

  it("keeps parties, collections and topics apart", () => {
    const base = match(1);
    expect(archiveRowKey({ ...base, search_name: "Someone Else" })).not.toBe(
      archiveRowKey(base)
    );
    expect(archiveRowKey({ ...base, collection: "Another" })).not.toBe(
      archiveRowKey(base)
    );
    expect(archiveRowKey({ ...base, topics: ["poi"] })).not.toBe(
      archiveRowKey(base)
    );
  });

  it("does not care what order the topics arrived in", () => {
    const base = match(1);
    expect(archiveRowKey({ ...base, topics: ["poi", "crime.fin"] })).toBe(
      archiveRowKey({ ...base, topics: ["crime.fin", "poi"] })
    );
  });
});
