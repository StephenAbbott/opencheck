/**
 * KimdisContractsList — the ΚΗΜΔΗΣ card lists the contracts themselves, the
 * way the AusTender card lists its notices. Checked here: each contract links
 * to its ΚΗΜΔΗΣ document, a contract marked cancelled is never called
 * cancelled, a read stopped at the cap and a period that did not answer are
 * both said, and the list expands past five.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { KimdisContractsList } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function contract(i: number, over: Record<string, unknown> = {}) {
  return {
    adam: `26SYMV02009707${i}`,
    published: `2026-10-${String(28 - i).padStart(2, "0")}`,
    buyer: "ΔΗΜΟΣ ΘΕΣΣΑΛΟΝΙΚΗΣ",
    title: `Οδοποιία ${i}`,
    value_eur: 1000 * (i + 1),
    start_date: "2026-10-07",
    end_date: "2027-10-06",
    procedure: "Ανοιχτή διαδικασία",
    contract_type: "Έργα",
    bids: 3,
    amends_earlier: false,
    marked_cancelled: false,
    other_contractors: 0,
    url: `https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/26SYMV02009707${i}`,
    ...over,
  };
}

function hit(contracts: unknown[], over: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "kimdis_greece",
    hit_id: "094207780",
    kind: "entity",
    name: "ΑΚΤΩΡ",
    summary: "",
    identifiers: {},
    is_stub: false,
    raw: {
      afm: "094207780",
      supplier: { contracts: 127, value_eur: 916979598, buyers: 34 },
      read_from: "2017-01-01",
      truncated: false,
      gaps: [],
      contracts,
      ...over,
    },
  } as SourceHit;
}

describe("KimdisContractsList", () => {
  it("links each contract to its ΚΗΜΔΗΣ document with authority and value", () => {
    render(<KimdisContractsList hit={hit([contract(0, { amends_earlier: true })])} />);
    const link = screen.getByRole("link", { name: /Οδοποιία 0/ });
    expect(link.getAttribute("href")).toContain("/khmdhs-opendata/contract/attachment/");
    const meta = screen.getByText(/ΔΗΜΟΣ ΘΕΣΣΑΛΟΝΙΚΗΣ/).textContent ?? "";
    expect(meta).toContain("without VAT");
    expect(meta).toContain("amends an earlier version");
    expect(screen.getByText(/127 contracts posted since 2017/)).toBeTruthy();
    expect(screen.getByText(/ΓΕΜΗ record publishes/)).toBeTruthy();
  });

  it("never calls a contract cancelled, only marked so in the data", () => {
    render(<KimdisContractsList hit={hit([contract(0, { marked_cancelled: true })])} />);
    expect(screen.getByText("marked cancelled in the data")).toBeTruthy();
    expect(screen.queryByText(/^cancelled$/i)).toBeNull();
  });

  it("says where the read stopped and which period did not answer", () => {
    render(
      <KimdisContractsList
        hit={hit([contract(0)], {
          truncated: true,
          read_from: "2021-06-09",
          gaps: [{ from: "2018-06-25", to: "2018-12-21" }],
        })}
      />,
    );
    expect(screen.getByText(/contracts posted before 2021-06-09 were not read/)).toBeTruthy();
    expect(screen.getByText(/did not answer for 2018-06-25 to 2018-12-21/)).toBeTruthy();
  });

  it("shows five and expands to the rest", async () => {
    render(<KimdisContractsList hit={hit(Array.from({ length: 8 }, (_, i) => contract(i)))} />);
    expect(screen.getAllByRole("link")).toHaveLength(5);
    await userEvent.click(screen.getByRole("button"));
    expect(screen.getAllByRole("link")).toHaveLength(8);
  });

  it("renders nothing for another source", () => {
    const { container } = render(
      <KimdisContractsList hit={{ ...hit([contract(0)]), source_id: "austender" } as SourceHit} />,
    );
    expect(container.textContent).toBe("");
  });
});
