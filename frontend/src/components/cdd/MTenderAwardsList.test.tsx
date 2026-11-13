/**
 * MTenderAwardsList — the MTender card lists the awards themselves, the way
 * the AusTender card lists its notices. Checked here: each award links to its
 * procurement on MTender, rows say "awarded" and never "contract", the
 * footnote names the period the copy covers, and the list expands past five.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { MTenderAwardsList } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function award(i: number, over: Record<string, unknown> = {}) {
  return {
    ocid: `ocds-b3wdp1-MD-176829458523${i}`,
    awarded: `2026-01-${String(20 - i).padStart(2, "0")}`,
    buyer: "Grădinița nr. 6",
    title: `Produse igienice ${i}`,
    value: 1000 * (i + 1),
    currency: "MDL",
    procedure: "Low-value purchase, reported (direct award)",
    category: "goods",
    lots: 1,
    url: `https://mtender.gov.md/tenders/ocds-b3wdp1-MD-176829458523${i}`,
    ...over,
  };
}

function hit(awards: unknown[], over: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "mtender_moldova",
    hit_id: "1016600004811",
    kind: "entity",
    name: 'S.R.L. "KAUFLAND"',
    summary: "",
    identifiers: {},
    is_stub: false,
    raw: {
      idno: "1016600004811",
      supplier: {
        awards: 12,
        value_mdl: 149090,
        other_currency: 1,
        first_date: "2025-11-02",
        last_date: "2026-01-15",
      },
      data_to: "2026-03-03",
      awards,
      ...over,
    },
  } as SourceHit;
}

describe("MTenderAwardsList", () => {
  it("links each award to MTender with buyer, value, procedure and lots", () => {
    render(<MTenderAwardsList hit={hit([award(0, { lots: 3 })])} />);
    const link = screen.getByRole("link", { name: /Produse igienice 0/ });
    expect(link.getAttribute("href")).toBe("https://mtender.gov.md/tenders/ocds-b3wdp1-MD-1768294585230");
    const meta = screen.getByText(/Grădinița nr\. 6/).textContent ?? "";
    expect(meta).toContain("MDL 1,000");
    expect(meta).toContain("awarded 2026-01-20");
    expect(meta).toContain("Low-value purchase, reported (direct award)");
    expect(meta).toContain("3 lots");
    expect(screen.getByText(/12 procurements awarded/)).toBeTruthy();
    expect(screen.getByText(/1 in another currency not added/)).toBeTruthy();
    expect(screen.getByText(/holds awards up to 2026-03-03/)).toBeTruthy();
  });

  it("says awarded, never contract", () => {
    const { container } = render(<MTenderAwardsList hit={hit([award(0)])} />);
    expect(screen.getByText("awarded")).toBeTruthy();
    const text = container.textContent ?? "";
    expect(text).toContain("awards, not contracts");
    expect(text.replace("awards, not contracts", "")).not.toMatch(/contract/i);
  });

  it("shows five awards, then the rest on request", async () => {
    render(<MTenderAwardsList hit={hit(Array.from({ length: 8 }, (_, i) => award(i)))} />);
    expect(screen.getAllByRole("link")).toHaveLength(5);
    await userEvent.click(screen.getByRole("button"));
    expect(screen.getAllByRole("link")).toHaveLength(8);
  });

  it("renders nothing for another source or no awards", () => {
    const { container } = render(<MTenderAwardsList hit={hit([])} />);
    expect(container.textContent).toBe("");
    const other = { ...hit([award(0)]), source_id: "austender" } as SourceHit;
    const second = render(<MTenderAwardsList hit={other} />);
    expect(second.container.textContent).toBe("");
  });
});
