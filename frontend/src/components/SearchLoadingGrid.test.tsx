/**
 * The loading grid while a run waits for a slot (Phase 238).
 *
 * The sentence is pinned in `lib/lookupProgress.test.ts`; what this tier pins
 * is the markup — that the waiting line is what the grid shows and announces,
 * that no source chips are drawn for a run that has not started, and that the
 * line gives way to the run's own progress once it has.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import SearchLoadingGrid from "./SearchLoadingGrid";

describe("SearchLoadingGrid while queued", () => {
  it("shows and announces the place in the queue, with no chips", () => {
    render(<SearchLoadingGrid sources={[]} queuePosition={2} />);
    const line = "OpenCheck is busy — waiting for a free slot, 1 check ahead of yours…";
    // Once visible (aria-hidden) and once in the throttled live region.
    expect(screen.getAllByText(line)).toHaveLength(2);
    expect(screen.getByRole("status").textContent).toBe(line);
    expect(screen.queryAllByRole("listitem")).toHaveLength(0);
  });

  it("drops the queue line once the run has started", () => {
    render(
      <SearchLoadingGrid sources={[]} queuePosition={1} started={new Set(["gleif"])} />,
    );
    expect(screen.queryByText(/free slot/)).toBeNull();
    expect(screen.getByRole("status").textContent).toBe("Resolving the entity in GLEIF…");
  });
});
