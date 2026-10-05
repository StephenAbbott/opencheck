/**
 * DegradedScreensNotice — the re-run button (Phase 279).
 *
 * A screen that stopped at OpenCheck's related-party limit ("truncated")
 * selects the same parties when re-run, so offering "Re-run screening" for it
 * alone promises something the button cannot do.
 *
 * And the collapsed bar: the count of checks that did not run is always on
 * screen; the detail, the principle and the re-run sit behind "Show more".
 */
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

import { DegradedScreensNotice } from "./ReportNotices";
import type { DegradedSource } from "../../lib/api";

const capped: DegradedSource = {
  source_id: "opencheck",
  check: "cross_source_names",
  reason: "truncated",
  detail: "Related-party sanctions and PEP screening covered the 25 highest-ranked of 49 related parties; 24 were not screened.",
  affected_signals: [],
};
const timedOut: DegradedSource = {
  source_id: "opensanctions",
  check: "cross_source_names",
  reason: "timeout",
  detail: "Search failed for 3 of 25 related-party name(s).",
  affected_signals: [],
};

describe("DegradedScreensNotice", () => {
  it("offers no re-run when every gap is a capped screen, and names the limit", () => {
    render(<DegradedScreensNotice degraded={[capped]} onRetry={vi.fn()} open />);
    expect(screen.queryByRole("button", { name: "Re-run screening" })).toBeNull();
    expect(screen.getByText(/per-lookup limit on related parties was reached/)).toBeInTheDocument();
  });

  it("still offers the re-run when an upstream failure sits alongside the cap", () => {
    render(<DegradedScreensNotice degraded={[capped, timedOut]} onRetry={vi.fn()} open />);
    expect(screen.getByRole("button", { name: "Re-run screening" })).toBeInTheDocument();
  });

  it("collapses to the one-line count by default, hiding the detail", () => {
    render(<DegradedScreensNotice degraded={[capped, timedOut]} onRetry={vi.fn()} />);
    expect(screen.getByText(/Screening incomplete — 2 checks did not fully run/)).toBeInTheDocument();
    const toggle = screen.getByRole("button", { name: /Show more/ });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText(/Search failed for 3 of 25/)).toBeNull();
    expect(screen.queryByText(/not evidence of absence/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Re-run screening" })).toBeNull();
  });

  it("shows and hides the detail with Show more / Show less", () => {
    render(<DegradedScreensNotice degraded={[capped, timedOut]} onRetry={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: /Show more/ }));
    const toggle = screen.getByRole("button", { name: /Show less/ });
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText(/Search failed for 3 of 25/)).toBeInTheDocument();
    expect(screen.getByText(/not evidence of absence/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Re-run screening" })).toBeInTheDocument();
    fireEvent.click(toggle);
    expect(screen.queryByText(/Search failed for 3 of 25/)).toBeNull();
  });

  it("reports toggles to a controlling parent instead of changing itself", () => {
    const onToggle = vi.fn();
    render(<DegradedScreensNotice degraded={[timedOut]} open={false} onToggle={onToggle} />);
    expect(screen.getByText(/1 check did not fully run/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Show more/ }));
    expect(onToggle).toHaveBeenCalledWith(true);
    expect(screen.queryByText(/Search failed for 3 of 25/)).toBeNull();
  });
});
