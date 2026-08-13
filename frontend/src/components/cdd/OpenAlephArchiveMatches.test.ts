import { describe, expect, it } from "vitest";
import type { OpenAlephScreeningMatch } from "../../lib/api";
import {
  PREVIEW_COUNT,
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
