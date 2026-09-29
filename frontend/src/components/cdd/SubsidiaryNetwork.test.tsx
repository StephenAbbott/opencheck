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

/**
 * Phase 261 — the list shows the Phase 255 data: the country spread, a lapsed
 * LEI, the direct parent of an ultimate-only child, the relationship dates,
 * and a note when GLEIF would not give the dates and paths.
 */
describe("SubsidiaryNetwork rows (Phase 261)", () => {
  beforeEach(() => getSubsidiaries.mockReset());

  const HEAD = "2138000000000000T178";
  const MID = "213800C2Y6KDQCD2WZ09";
  const DEEP = "549300XNL1VRVIODFM92";
  const STRANDED = "5493000000000000BG01";
  const OUTSIDE = "213800AAAAAAAAAAAA99";

  function shell(over: Partial<SubsidiariesResponse> = {}) {
    return network({
      direct_total: 1,
      ultimate_total: 3,
      distinct_fetched: 3,
      indirect_only: 2,
      jurisdictions: [
        { code: "US-DE", count: 17 },
        { code: "CA-AB", count: 6 },
        { code: "CA", count: 5 },
        { code: "US", count: 1 },
      ],
      countries: [
        { code: "US", count: 18 },
        { code: "CA", count: 11 },
      ],
      enriched: true,
      children: [
        {
          lei: MID, name: "SHELL DIRECT HOLDINGS LIMITED", jurisdiction: "GB", status: "ACTIVE",
          relation: "both", link: "#", relationship_start: "2013-11-01",
          lei_registration: { status: "ISSUED", label: "Issued", flag: false, since: null, next_renewal_date: "2027-01-01" },
        },
        {
          lei: DEEP, name: "SHELL DEEP B.V.", jurisdiction: "NL", status: "ACTIVE",
          relation: "ultimate", link: "#", relationship_start: "2020-11-02",
          direct_parent_lei: MID, direct_parent_in_network: true,
          lei_registration: { status: "LAPSED", label: "Lapsed", flag: true, since: "2019-10-19", next_renewal_date: "2019-10-19" },
        },
        {
          lei: STRANDED, name: "BG INTERNATIONAL LIMITED", jurisdiction: "GB", status: "ACTIVE",
          relation: "ultimate", link: "#", direct_parent_lei: OUTSIDE, direct_parent_in_network: false,
        },
      ],
      ...over,
    });
  }

  it("shows the spread by country, not by state or province", async () => {
    getSubsidiaries.mockResolvedValue(shell());
    render(<SubsidiaryNetwork lei={HEAD} entityName="SHELL PLC" />);
    await reveal();
    const spread = screen.getByTestId("country-spread").textContent ?? "";
    expect(spread).toContain("US 18");
    expect(spread).toContain("CA 11");
    expect(spread).not.toContain("US-DE");
    expect(spread).not.toContain("CA-AB");
  });

  it("chips a lapsed LEI in the context tone, saying it is the LEI record's status", async () => {
    getSubsidiaries.mockResolvedValue(shell());
    render(<SubsidiaryNetwork lei={HEAD} entityName="SHELL PLC" />);
    await reveal();
    const chip = screen.getByText(/^LEI lapsed since/);
    const pill = chip.closest("span.rounded-full");
    expect(pill?.className).toContain("oo-info");
    expect(pill?.className).not.toContain("oo-risk");
    expect(pill?.textContent).toContain("not the company");
    // An ISSUED LEI carries no chip: exactly one row is chipped.
    expect(screen.getAllByText(/^LEI lapsed/)).toHaveLength(1);
  });

  it("names the direct parent of an ultimate-only child and links to its row", async () => {
    getSubsidiaries.mockResolvedValue(shell());
    render(<SubsidiaryNetwork lei={HEAD} entityName="SHELL PLC" />);
    await reveal();
    const lines = screen.getAllByTestId("direct-parent");
    expect(lines).toHaveLength(2);
    const via = lines.find((l) => l.textContent?.startsWith("via"));
    const link = via?.querySelector("a");
    expect(link?.textContent).toBe("SHELL DIRECT HOLDINGS LIMITED");
    expect(link?.getAttribute("href")).toBe(`#subsidiary-row-${MID}`);
    expect(document.getElementById(`subsidiary-row-${MID}`)).toBeTruthy();
    await userEvent.click(link!);
    expect(document.activeElement?.id).toBe(`subsidiary-row-${MID}`);
    // A parent outside the network: its LEI, and the path said to be not shown.
    const outside = lines.find((l) => l !== via);
    expect(outside?.textContent).toContain(OUTSIDE);
    expect(outside?.textContent).toContain("not shown");
  });

  it("opens the list when the parent's row is past the first twelve", async () => {
    const filler = Array.from({ length: 11 }, (_, i) => ({
      lei: `5493000000000000A${String(i).padStart(3, "0")}`, name: `AAA DIRECT ${i}`, jurisdiction: "GB",
      status: "ACTIVE", relation: "direct" as const, link: "#",
    }));
    const base = shell();
    // The parent is an ultimate-only child sorted after the fillers.
    const parent = { ...base.children[2], lei: "5493000000000000ZZ01", name: "ZZ HOLDINGS", direct_parent_lei: null, direct_parent_in_network: null };
    const child = { ...base.children[1], direct_parent_lei: parent.lei, direct_parent_in_network: true };
    getSubsidiaries.mockResolvedValue(shell({ render_mode: "table", children: [...filler, child, parent] }));
    render(<SubsidiaryNetwork lei={HEAD} entityName="SHELL PLC" />);
    await reveal();
    expect(document.getElementById(`subsidiary-row-${parent.lei}`)).toBeNull();
    await userEvent.click(screen.getByRole("link", { name: "ZZ HOLDINGS" }));
    expect(document.getElementById(`subsidiary-row-${parent.lei}`)).toBeTruthy();
    expect(document.activeElement?.id).toBe(`subsidiary-row-${parent.lei}`);
  });

  it("dates each relationship from GLEIF's relationship record", async () => {
    getSubsidiaries.mockResolvedValue(shell());
    render(<SubsidiaryNetwork lei={HEAD} entityName="SHELL PLC" />);
    await reveal();
    const dates = screen.getAllByTestId("relationship-dates").map((d) => d.textContent);
    expect(dates).toContain("Consolidated since 1 Nov 2013");
    expect(dates).toContain("Consolidated since 2 Nov 2020");
    expect(screen.queryByTestId("enrichment-note")).toBeNull();
  });

  it("says the dates and paths could not be read when the enrichment was refused", async () => {
    getSubsidiaries.mockResolvedValue(shell({ enriched: false }));
    render(<SubsidiaryNetwork lei={HEAD} entityName="SHELL PLC" />);
    await reveal();
    expect(screen.getByTestId("enrichment-note").textContent).toContain("could not be read");
  });
});
