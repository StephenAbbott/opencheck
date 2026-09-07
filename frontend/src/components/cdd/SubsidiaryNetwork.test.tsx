/**
 * SubsidiaryNetwork — the claim that lives in the markup (Phase 180).
 *
 * A network the backend chose to serve from its GLEIF mirror is not degraded
 * — nothing was refused, `degraded_detail` is rightly null — but the reader is
 * owed where the rows came from and the Golden Copy date. That caption can
 * only be seen by rendering: it must be on screen when `snapshot_source` is
 * "mirror", and absent when the network came live. The wording itself is
 * pinned in `lib/vocab.test.ts`.
 */
import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const getSubsidiaries = vi.fn();
vi.mock("../../lib/api", () => ({
  getSubsidiaries: (...args: unknown[]) => getSubsidiaries(...args),
}));

import { SubsidiaryNetwork } from "./SubsidiaryNetwork";
import type { SubsidiariesResponse } from "../../lib/api";

function network(over: Partial<SubsidiariesResponse> = {}): SubsidiariesResponse {
  return {
    lei: "2138000000000000T178",
    available: true,
    reason: null,
    children_available: true,
    direct_available: true,
    ultimate_available: true,
    snapshot_fallback: false,
    snapshot_date: null,
    snapshot_source: null,
    degraded_detail: null,
    direct_total: 1,
    ultimate_total: 2,
    distinct_fetched: 2,
    indirect_only: 1,
    node_estimate: 2,
    render_mode: "graph",
    truncated: false,
    jurisdictions: [{ code: "GB", count: 2 }],
    children: [
      { lei: "2138000000000000P178", name: "Mirror Parent Holdings Ltd", jurisdiction: "GB", status: "ACTIVE", relation: "both", link: "#" },
      { lei: "2138001EXFNP9E7AYB46", name: "EASY POWER", jurisdiction: "GR", status: "ACTIVE", relation: "ultimate", link: "#" },
    ],
    bods: null,
    ...over,
  } as SubsidiariesResponse;
}

async function reveal() {
  await userEvent.click(screen.getByRole("button", { name: /reveal subsidiary network/i }));
  await screen.findByRole("button", { name: /download bods/i });
}

describe("SubsidiaryNetwork mirror caption", () => {
  beforeEach(() => getSubsidiaries.mockReset());

  it("says the network came from the GLEIF mirror, with the Golden Copy date", async () => {
    getSubsidiaries.mockResolvedValue(
      network({ snapshot_source: "mirror", snapshot_fallback: true, snapshot_date: "2026-09-07" }),
    );
    render(<SubsidiaryNetwork lei="2138000000000000T178" />);
    await reveal();
    const caption = screen.getByTestId("mirror-caption");
    expect(caption.textContent).toContain("GLEIF mirror");
    expect(caption.textContent).toContain("2026-09-07");
    // Not a degradation: no status notice, and the counts render as normal.
    expect(screen.queryByRole("status")).toBeNull();
    expect(screen.getByRole("button", { name: /download bods/i })).toBeTruthy();
  });

  it("says nothing of the kind when the network came live", async () => {
    getSubsidiaries.mockResolvedValue(network());
    render(<SubsidiaryNetwork lei="2138000000000000T178" />);
    await reveal();
    expect(screen.queryByTestId("mirror-caption")).toBeNull();
  });

  it("keeps the degradation notice for a forced fallback, without the mirror caption", async () => {
    getSubsidiaries.mockResolvedValue(
      network({
        snapshot_source: "fallback",
        snapshot_fallback: true,
        snapshot_date: "2026-09-07",
        degraded_detail:
          "GLEIF did not answer for this network, so it is shown from OpenCheck's Golden Copy snapshot (extract of 2026-09-07) rather than live.",
      }),
    );
    render(<SubsidiaryNetwork lei="2138000000000000T178" />);
    await reveal();
    expect(screen.getByRole("status").textContent).toContain("did not answer");
    expect(screen.queryByTestId("mirror-caption")).toBeNull();
  });
});
