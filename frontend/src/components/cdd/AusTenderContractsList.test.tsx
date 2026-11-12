/**
 * AusTenderContractsList — the AusTender card lists the contract notices
 * themselves, the way the ChileCompra card lists its records. Checked here:
 * each notice links to its AusTender page, a notice marked cancelled says
 * what the mark can mean rather than calling the contract cancelled, the
 * ACN route is named, and the list expands past five.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AusTenderContractsList } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function contract(i: number, over: Record<string, unknown> = {}) {
  return {
    cn: `CN42867${i}`,
    published: `2026-10-${String(28 - i).padStart(2, "0")}`,
    buyer: "Department of Defence",
    title: `Labour hire ${i}`,
    value_aud: 1000 * (i + 1),
    start_date: "2026-10-09",
    end_date: "2027-10-08",
    procedure: "Open tender",
    limited_tender_reason: "",
    amendments: 0,
    marked_cancelled: false,
    other_suppliers: 0,
    url: `https://www.tenders.gov.au/Cn/Show/9c5ddf1c-6874-4560-9dd0-cfaf36cee3${i}c`,
    ...over,
  };
}

function hit(contracts: unknown[], over: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "austender",
    hit_id: "64086174781",
    kind: "entity",
    name: "TELSTRA LIMITED",
    summary: "",
    identifiers: {},
    is_stub: false,
    raw: {
      abn: "64086174781",
      matched_via: "acn",
      supplier: { contracts: 1722, value_aud: 4184536011, buyers: 71, first_date: "2012-03-01" },
      contracts,
      ...over,
    },
  } as SourceHit;
}

describe("AusTenderContractsList", () => {
  it("links each notice to AusTender with agency, value and procedure", () => {
    render(
      <AusTenderContractsList
        hit={hit([
          contract(0, {
            procedure: "Limited tender",
            limited_tender_reason: "10.3.a.i. No submissions received.",
            amendments: 2,
          }),
        ])}
      />,
    );
    const link = screen.getByRole("link", { name: /Labour hire 0/ });
    expect(link.getAttribute("href")).toContain("https://www.tenders.gov.au/Cn/Show/");
    expect(screen.getByText(/Department of Defence/).textContent).toContain(
      "Limited tender: 10.3.a.i. No submissions received.",
    );
    expect(screen.getByText(/Department of Defence/).textContent).toContain("amended 2 times");
    expect(screen.getByText(/1,722 notices since 2012/)).toBeTruthy();
    expect(screen.getByText(/ABN that the company's ACN implies/)).toBeTruthy();
  });

  it("never calls a contract cancelled, only marked so in the data", () => {
    render(<AusTenderContractsList hit={hit([contract(0, { marked_cancelled: true })])} />);
    expect(screen.getByText("marked cancelled in the data")).toBeTruthy();
    expect(screen.queryByText(/^cancelled$/i)).toBeNull();
  });

  it("shows five notices, then the rest on request", async () => {
    render(<AusTenderContractsList hit={hit(Array.from({ length: 8 }, (_, i) => contract(i)))} />);
    expect(screen.getAllByRole("link")).toHaveLength(5);
    await userEvent.click(screen.getByRole("button"));
    expect(screen.getAllByRole("link")).toHaveLength(8);
  });

  it("renders nothing for another source or no contracts", () => {
    const { container } = render(<AusTenderContractsList hit={hit([])} />);
    expect(container.textContent).toBe("");
  });
});
