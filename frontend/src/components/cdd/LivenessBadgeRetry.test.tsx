/**
 * Phase 331 — a retried source's freshness chip describes the retry.
 *
 * The run gives a failed source stub provenance, and the chip read
 * "Placeholder data" on a card the retry had just filled with real register
 * data, because `retrySource` replaced the hits and never touched
 * `sourceLiveness`. The chip is drawn from the merged map.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { LivenessBadge } from "./LivenessBadge";
import { mergeRetryLiveness } from "../../lib/sourceRetry";
import type { SourceLiveness } from "../../lib/api";

const STUB: SourceLiveness = { liveness: "stub", label: "Placeholder data", retrieved_at: null, detail: null };
const RETRIED: SourceLiveness = {
  liveness: "snapshot",
  label: "Snapshot",
  retrieved_at: "2026-10-13T07:38:47Z",
  source_as_of: "2026-10-13T00:00:00Z",
  detail: null,
};

describe("the freshness chip after a retry", () => {
  it("reads the retry, not the failed attempt", () => {
    const run = { jar_lithuania: STUB };
    const { rerender } = render(<LivenessBadge info={run.jar_lithuania} />);
    expect(screen.getByText(/Placeholder data/)).toBeInTheDocument();

    const merged = mergeRetryLiveness(run, { source_liveness: { jar_lithuania: RETRIED } });
    rerender(<LivenessBadge info={merged.jar_lithuania} />);
    expect(screen.queryByText(/Placeholder data/)).toBeNull();
    expect(screen.getByText(/^Snapshot · 13 Oct 2026/)).toBeInTheDocument();
  });
});
