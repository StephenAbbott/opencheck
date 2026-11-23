/**
 * ProzorroContractsList — the Prozorro card lists the contracts themselves,
 * the way the MTender card lists its awards. Checked here: each contract
 * links to its page on Prozorro, a terminated contract says so, the footnote
 * names what is counted and the period the harvest covers, and the list
 * expands past five.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ProzorroContractsList } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function contract(i: number, over: Record<string, unknown> = {}) {
  return {
    id: `c8b824ddd082463aa9a29fa51bbbec8${i}`,
    ref: `UA-2026-11-2${i}-001568-a-c1`,
    signed: `2026-11-${String(20 - i).padStart(2, "0")}`,
    buyer: "Київська міська рада",
    title: `Будівельні матеріали ${i}`,
    value: 1000 * (i + 1),
    currency: "UAH",
    procedure: "Purchase without the electronic system, reported (direct contract)",
    category: "goods",
    status: "active",
    url: `https://prozorro.gov.ua/contract/UA-2026-11-2${i}-001568-a-c1`,
    ...over,
  };
}

function hit(contracts: unknown[], over: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "prozorro_ukraine",
    hit_id: "32490244",
    kind: "entity",
    name: 'ТОВ "ЕПІЦЕНТР К"',
    summary: "",
    identifiers: {},
    is_stub: false,
    raw: {
      edrpou: "32490244",
      supplier: {
        contracts: 12,
        terminated: 2,
        value_uah: 149090,
        other_currency: 1,
        first_date: "2025-11-02",
        last_date: "2026-11-20",
      },
      harvest_started_from: "2025-11-23",
      contracts,
      ...over,
    },
  } as SourceHit;
}

describe("ProzorroContractsList", () => {
  it("links each contract to Prozorro with buyer, value, day and procedure", () => {
    render(<ProzorroContractsList hit={hit([contract(0)])} />);
    const link = screen.getByRole("link", { name: /Будівельні матеріали 0/ });
    expect(link.getAttribute("href")).toBe("https://prozorro.gov.ua/contract/UA-2026-11-20-001568-a-c1");
    const meta = screen.getByText(/Київська міська рада/).textContent ?? "";
    expect(meta).toContain("UAH 1,000");
    expect(meta).toContain("signed 2026-11-20");
    expect(meta).toContain("reported (direct contract)");
    expect(screen.getByText(/12 contracts signed/)).toBeTruthy();
    expect(screen.getByText(/2 since terminated/)).toBeTruthy();
    expect(screen.getByText(/1 in another currency not added/)).toBeTruthy();
    expect(screen.getByText(/changed on Prozorro since 2025-11-23/)).toBeTruthy();
  });

  it("marks a terminated contract and only that one", () => {
    render(
      <ProzorroContractsList hit={hit([contract(0), contract(1, { status: "terminated" })])} />,
    );
    expect(screen.getAllByText("terminated")).toHaveLength(1);
  });

  it("names the contract by its Prozorro number when it has no title", () => {
    render(<ProzorroContractsList hit={hit([contract(0, { title: "" })])} />);
    expect(screen.getByRole("link", { name: /Contract UA-2026-11-20-001568-a-c1/ })).toBeTruthy();
  });

  it("shows five contracts, then the rest on request", async () => {
    render(<ProzorroContractsList hit={hit(Array.from({ length: 8 }, (_, i) => contract(i)))} />);
    expect(screen.getAllByRole("link")).toHaveLength(5);
    await userEvent.click(screen.getByRole("button"));
    expect(screen.getAllByRole("link")).toHaveLength(8);
  });

  it("renders nothing for another source or no contracts", () => {
    const { container } = render(<ProzorroContractsList hit={hit([])} />);
    expect(container.textContent).toBe("");
    const other = { ...hit([contract(0)]), source_id: "mtender_moldova" } as SourceHit;
    const second = render(<ProzorroContractsList hit={other} />);
    expect(second.container.textContent).toBe("");
  });
});
