/**
 * The disclosure-status chips on a source card: one per value the source
 * supports, labelled with what it covers, the source's own words beneath, and
 * nothing for a source that is not an ownership source.
 */
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";

import { SourceBucketCard, type SourceBucket } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";
import type { SourceDisclosure } from "../../lib/disclosureStatus";

const HIT: SourceHit = {
  source_id: "companies_house",
  hit_id: "00102498",
  kind: "entity",
  name: "GAP CO LTD",
  summary: "GB · registered entity",
  identifiers: {},
  is_stub: false,
  raw: {},
} as SourceHit;

const BUCKET: SourceBucket = {
  sourceId: "companies_house",
  sourceName: "UK Companies House",
  hits: [HIT],
};

const CH: SourceDisclosure = {
  values: ["declared", "not_provided"],
  ownership_source: true,
  entries: [
    { value: "declared", covers: "beneficial_owners", count: 2 },
    {
      value: "not_provided",
      covers: "beneficial_owners",
      details: ["The company has not yet identified a registrable person"],
    },
  ],
};

describe("SourceBucketCard disclosure chips", () => {
  it("lists every value with what it covers, and the source's words", () => {
    render(<SourceBucketCard bucket={BUCKET} riskByHit={{}} disclosure={CH} />);
    const list = screen.getByRole("list", { name: /owners disclosed by this source/i });
    const items = within(list).getAllByRole("listitem");
    expect(items.map((li) => li.textContent)).toEqual([
      "Beneficial owners: Declared (2)",
      "Beneficial owners: Not provided",
    ]);
    expect(screen.getByText(/As the source puts it: .*not yet identified/)).toBeInTheDocument();
  });

  it("draws nothing for a source that is not an ownership source", () => {
    render(
      <SourceBucketCard
        bucket={BUCKET}
        riskByHit={{}}
        disclosure={{
          values: ["not_covered"],
          ownership_source: false,
          entries: [{ value: "not_covered", covers: null }],
        }}
      />,
    );
    expect(screen.queryByRole("list", { name: /owners disclosed/i })).toBeNull();
  });

  it("draws nothing when the run carried no status", () => {
    render(<SourceBucketCard bucket={BUCKET} riskByHit={{}} />);
    expect(screen.queryByRole("list", { name: /owners disclosed/i })).toBeNull();
  });
});
