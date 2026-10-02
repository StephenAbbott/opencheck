/**
 * DegradedScreensNotice — the re-run button (Phase 279).
 *
 * A screen that stopped at OpenCheck's related-party limit ("truncated")
 * selects the same parties when re-run, so offering "Re-run screening" for it
 * alone promises something the button cannot do.
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

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
    render(<DegradedScreensNotice degraded={[capped]} onRetry={vi.fn()} />);
    expect(screen.queryByRole("button", { name: "Re-run screening" })).toBeNull();
    expect(screen.getByText(/per-lookup limit on related parties was reached/)).toBeInTheDocument();
  });

  it("still offers the re-run when an upstream failure sits alongside the cap", () => {
    render(<DegradedScreensNotice degraded={[capped, timedOut]} onRetry={vi.fn()} />);
    expect(screen.getByRole("button", { name: "Re-run screening" })).toBeInTheDocument();
  });
});
