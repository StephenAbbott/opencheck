/**
 * A row whose record maps to a single statement has no diagram to open, so it
 * offers no Diagram button (Phase 300). EveryPolitician rows were the case in
 * point: they arrived with no statement count, so every row showed the button,
 * and opening it loaded one person statement and the button vanished.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { SourceBucketCard, type SourceBucket } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

const HIT: SourceHit = {
  source_id: "everypolitician",
  hit_id: "Q1234",
  kind: "person",
  name: "Hilde Møllerstad",
  summary: "Member of the Storting",
  identifiers: {},
  is_stub: false,
  raw: {},
} as SourceHit;

const BUCKET: SourceBucket = {
  sourceId: "everypolitician",
  sourceName: "EveryPolitician",
  hits: [HIT],
};

const KEY = "everypolitician:Q1234";

describe("SourceBucketCard diagram button", () => {
  it("is not offered for a row known to map to one statement", () => {
    render(
      <SourceBucketCard
        bucket={BUCKET}
        riskByHit={{}}
        bodsCountMap={{ [KEY]: 1 }}
        bodsBreakdownMap={{ [KEY]: { entities: 0, persons: 1, relationships: 0 } }}
      />,
    );
    expect(screen.queryByRole("button", { name: /diagram|entit|person/i })).toBeNull();
    // The Data drawer is still there.
    expect(screen.getByRole("button", { name: /data/i })).toBeInTheDocument();
  });

  it("is offered when the record maps to a graph", () => {
    render(
      <SourceBucketCard
        bucket={BUCKET}
        riskByHit={{}}
        bodsCountMap={{ [KEY]: 3 }}
        bodsBreakdownMap={{ [KEY]: { entities: 1, persons: 1, relationships: 1 } }}
      />,
    );
    expect(screen.getByRole("button", { name: /1 entity|person/i })).toBeInTheDocument();
  });

  it("is still offered while the count is unknown", () => {
    render(<SourceBucketCard bucket={BUCKET} riskByHit={{}} />);
    expect(screen.getByRole("button", { name: "Diagram" })).toBeInTheDocument();
  });
});
