/**
 * Climate TRACE is an ownership source whose card is on the Climate & ESG
 * tab, so its disclosure status is chipped there, as on a QuickCheck card.
 */
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";

import { EsgPanel } from "./EsgPanel";
import type { SourceHit } from "../../lib/api";
import type { DisclosureStatusPayload } from "../../lib/disclosureStatus";

const HIT = {
  source_id: "climatetrace",
  hit_id: "E100000000834",
  kind: "entity",
  name: "Shell PLC",
  summary: "GB · GEM entity",
  identifiers: { gem_entity_id: "E100000000834" },
  is_stub: false,
  raw: {},
} as unknown as SourceHit;

const STATUS: DisclosureStatusPayload = {
  as_of: "2026-11-04",
  not_covered: [],
  sources: {
    climatetrace: {
      values: ["declared"],
      ownership_source: true,
      entries: [{ value: "declared", covers: "shareholders", count: 1 }],
    },
  },
};

describe("EsgPanel disclosure chips", () => {
  it("chips Climate TRACE's value on its card", () => {
    render(
      <EsgPanel
        buckets={[{ sourceId: "climatetrace", sourceName: "Climate TRACE", hits: [HIT] }]}
        disclosureStatus={STATUS}
      />,
    );
    const list = screen.getByRole("list", { name: /owners disclosed by this source/i });
    expect(within(list).getAllByRole("listitem").map((li) => li.textContent)).toEqual([
      "Shareholders: Declared (1)",
    ]);
  });

  it("draws no chips when the run carried no status", () => {
    render(
      <EsgPanel buckets={[{ sourceId: "climatetrace", sourceName: "Climate TRACE", hits: [HIT] }]} />,
    );
    expect(screen.queryByRole("list", { name: /owners disclosed/i })).toBeNull();
  });
});
