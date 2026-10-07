/**
 * The EveryPolitician card folds its rows after five. It is screened once per
 * related person, so a large company's card can carry a dozen rows (twelve
 * for Equinor) and push every card below it off the screen. Other sources
 * have no limit and render every row.
 */
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { COLLAPSED_ROW_LIMIT, SourceBucketCard, type SourceBucket } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function hit(sourceId: string, i: number): SourceHit {
  return {
    source_id: sourceId,
    hit_id: `NK-person-${i}`,
    kind: "person",
    name: `Politician ${i}`,
    summary: `Member of parliament ${i}`,
    identifiers: {},
    is_stub: false,
    raw: {},
  } as SourceHit;
}

function bucket(sourceId: string, n: number): SourceBucket {
  return {
    sourceId,
    sourceName: sourceId === "everypolitician" ? "EveryPolitician" : "Other source",
    hits: Array.from({ length: n }, (_, i) => hit(sourceId, i + 1)),
  };
}

function rowNames(): string[] {
  const card = screen.getByRole("article");
  const list = within(card).getAllByRole("list")[0];
  return within(list)
    .getAllByRole("listitem")
    .map((li) => li.textContent ?? "")
    .map((t) => t.match(/Politician \d+/)?.[0] ?? "");
}

describe("SourceBucketCard row limit", () => {
  it("is five for EveryPolitician", () => {
    expect(COLLAPSED_ROW_LIMIT.everypolitician).toBe(5);
  });

  it("shows the first five EveryPolitician rows and folds the rest", async () => {
    render(<SourceBucketCard bucket={bucket("everypolitician", 12)} riskByHit={{}} />);
    expect(rowNames()).toEqual([1, 2, 3, 4, 5].map((i) => `Politician ${i}`));

    const more = screen.getByRole("button", { name: "Show 7 more" });
    expect(more).toHaveAttribute("aria-expanded", "false");

    await userEvent.click(more);
    expect(rowNames()).toHaveLength(12);
    expect(rowNames()[11]).toBe("Politician 12");

    const fewer = screen.getByRole("button", { name: "Show fewer" });
    expect(fewer).toHaveAttribute("aria-expanded", "true");
    await userEvent.click(fewer);
    expect(rowNames()).toHaveLength(5);
  });

  it("offers nothing when five or fewer rows", () => {
    render(<SourceBucketCard bucket={bucket("everypolitician", 5)} riskByHit={{}} />);
    expect(rowNames()).toHaveLength(5);
    expect(screen.queryByRole("button", { name: /^Show \d+ more$/ })).toBeNull();
  });

  it("counts rendered rows, not records — identical records collapse first", () => {
    const b = bucket("everypolitician", 6);
    // Two records that render identically are one row, so six records
    // become five rows and nothing is folded.
    b.hits[5] = { ...b.hits[4], hit_id: "NK-duplicate" };
    render(<SourceBucketCard bucket={b} riskByHit={{}} />);
    expect(rowNames()).toHaveLength(5);
    expect(screen.queryByRole("button", { name: /^Show \d+ more$/ })).toBeNull();
  });

  it("leaves other sources unlimited", () => {
    render(<SourceBucketCard bucket={bucket("opensanctions", 12)} riskByHit={{}} />);
    expect(rowNames()).toHaveLength(12);
    expect(screen.queryByRole("button", { name: /^Show \d+ more$/ })).toBeNull();
  });
});
