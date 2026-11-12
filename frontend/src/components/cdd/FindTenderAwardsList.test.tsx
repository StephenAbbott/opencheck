/**
 * FindTenderAwardsList — the Find a Tender card lists the awards themselves,
 * the way TED's and ChileCompra's cards list their records. What is checked
 * here can only be seen by rendering: each award links to its notice, a
 * shared framework's value says how many suppliers share it, a cancelled
 * contract and the "largest" pick are chips, and the footnote states the
 * coverage limits.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { FindTenderAwardsList } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function hit(records: unknown[], over: Partial<SourceHit> = {}): SourceHit {
  return {
    source_id: "find_tender_uk",
    hit_id: "00832117",
    kind: "entity",
    name: "MCPHILLIPS (WELLINGTON) LIMITED",
    summary: "",
    identifiers: {},
    is_stub: false,
    raw: { company_number: "00832117", supplier: { awards: 21 }, via_ppon: 0, records },
    ...over,
  } as SourceHit;
}

const framework = {
  ocid: "ocds-h6vhtk-0aaaaa",
  award_id: "lot-9",
  notice: "094489-2026",
  url: "https://www.find-tender.service.gov.uk/Notice/094489-2026",
  date: "2026-10-02",
  date_basis: "awarded",
  title: "Lot 09 - New Build",
  buyer: "LHC Procurement Group",
  value: 1950000000,
  currency: "GBP",
  award_status: "pending",
  suppliers: 20,
  lots: 5,
  kept_as: "latest",
};

const cancelled = {
  ocid: "ocds-h6vhtk-04e7a5",
  award_id: "1",
  date: "2026-03-10",
  date_basis: "awarded",
  title: "Doris Robinson Court",
  buyer: "Stoke on Trent City Council",
  value: 9630343.49,
  currency: "GBP",
  award_status: "active",
  contract_status: "cancelled",
  suppliers: 1,
  kept_as: "largest",
};

describe("FindTenderAwardsList", () => {
  it("links an award to its notice and says who shares a framework's value", () => {
    render(<FindTenderAwardsList hit={hit([framework])} />);
    const link = screen.getByRole("link", { name: /Lot 09 - New Build/ });
    expect(link.getAttribute("href")).toBe("https://www.find-tender.service.gov.uk/Notice/094489-2026");
    expect(screen.getByText(/£1,950,000,000, shared by 20 suppliers · 5 lots · awarded 2026-10-02/)).toBeTruthy();
    expect(screen.getByText("awarded, contract not yet signed")).toBeTruthy();
  });

  it("shows an award with no notice as plain text, with its chips", () => {
    render(<FindTenderAwardsList hit={hit([cancelled])} />);
    expect(screen.queryByRole("link")).toBeNull();
    expect(screen.getByText("Doris Robinson Court")).toBeTruthy();
    expect(screen.getByText("contract cancelled")).toBeTruthy();
    expect(screen.getByText("largest")).toBeTruthy();
  });

  it("states the total and the coverage limits", () => {
    render(<FindTenderAwardsList hit={hit([framework])} />);
    expect(screen.getByText(/21 awards in all/)).toBeTruthy();
    expect(screen.getByText(/not added up/)).toBeTruthy();
  });

  it("renders nothing for another source", () => {
    const { container } = render(<FindTenderAwardsList hit={hit([framework], { source_id: "ted_eu" })} />);
    expect(container.innerHTML).toBe("");
  });
});
