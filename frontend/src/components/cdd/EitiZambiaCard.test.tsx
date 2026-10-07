/**
 * EitiZambiaCard — claims that live in the markup: the match is shown as a
 * name match with its basis, a year without published amounts says so, the
 * reconciliation report is labelled as the company's own disclosure, and a
 * licence share is never read as a company share.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { EitiZambiaCard } from "./EitiZambiaCard";
import type { SourceHit } from "../../lib/api";

function hit(over: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "eiti_zambia",
    hit_id: "9845008756E7B43CDE48",
    name: "CHAMBISHI COPPER SMELTER LIMITED",
    summary: "ZM-TPIN 1001831030",
    raw: {
      lei: "9845008756E7B43CDE48",
      gleif_legal_name: "CHAMBISHI COPPER SMELTER LIMITED",
      tpins: ["1001831030"],
      names_as_filed: ["CHAMBISHI COPPER SMELTER LIMITED", "C.C.S"],
      match: { method: "tpin_via_name", confidence: "medium", aliases: [] },
      zra_tax: [
        { year: "2024", dataset: "zra-tax-revenue-2024", payments: 12, amounts_summed: false,
          by_tax_type: [{ tax_type: "Pay As You Earn", payments: 12 }] },
        { year: "2023", dataset: "zra-tax-payment-in-2023-detailed-service-values", payments: 118,
          amounts_summed: true, total_zmw: 1_200_000_000,
          by_tax_type: [{ tax_type: "Mineral Royalty", zmw: 800_000_000 }] },
      ],
      eiti_reconciliation: [
        { year: "2023", total_zmw: 900_000_000, total_usd: 0, lines: 27,
          by_receiving_entity: [{ entity: "ZRA", zmw: 900_000_000, usd: 0 }] },
      ],
      employment: [{ year: "2023", employees: 1926, domestic: 1900, expatriate: 26, women_share: 0.035 }],
      licences: [{ code: "39168-HQ-LEL", type: "LEL", status: "Active", commodities: "Cu", area: "1 ha",
        location: "Copperbelt", grant_date: null, expiry_date: null, holder_as_filed: "x", holder_share_pct: 100 }],
      water_offences: [{ name_as_filed: "Chambishi Copper Smelters",
        offence: "Illegal Water Abstraction from Kafue River", activity: "Mining", permit: "Not permitted" }],
      datasets: { "payment-report": { name: "Payment Report", url: "https://portal.zambiaeiti.org/datasets/payment-report" } },
      ...over,
    },
  } as unknown as SourceHit;
}

describe("EitiZambiaCard", () => {
  it("shows the TPIN and grades the LEI as a name match with its basis", () => {
    render(<EitiZambiaCard hit={hit()} />);
    expect(screen.getByText("ZM-TPIN 1001831030")).toBeInTheDocument();
    expect(screen.getByText(/Possible match/)).toBeInTheDocument();
    expect(screen.getByText(/name ZRA files with this TPIN/)).toBeInTheDocument();
    expect(screen.getByText(/“C\.C\.S”/)).toBeInTheDocument();
  });

  it("says when a year's amounts are not published instead of showing a total", () => {
    render(<EitiZambiaCard hit={hit()} />);
    expect(screen.getByText("12 payments · amounts not published")).toBeInTheDocument();
    expect(screen.getByText("ZMW 1.2bn")).toBeInTheDocument();
  });

  it("labels the reconciliation report as the company's own disclosure", () => {
    render(<EitiZambiaCard hit={hit()} />);
    expect(screen.getByText(/The company's own disclosure/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Payment Report/ })).toHaveAttribute(
      "href", "https://portal.zambiaeiti.org/datasets/payment-report",
    );
  });

  it("quotes the WARMA listing verbatim with the name it was listed under", () => {
    render(<EitiZambiaCard hit={hit()} />);
    expect(screen.getByText("Illegal Water Abstraction from Kafue River")).toBeInTheDocument();
    expect(screen.getByText(/listed as “Chambishi Copper Smelters”/)).toBeInTheDocument();
  });

  it("never reads a licence share as a company share", () => {
    render(<EitiZambiaCard hit={hit()} />);
    expect(screen.getByText("100%")).toBeInTheDocument();
    expect(screen.getByText(/share of the licence, not of the company/)).toBeInTheDocument();
  });

  it("omits blocks with nothing in them", () => {
    render(<EitiZambiaCard hit={hit({ water_offences: [], licences: [], eiti_reconciliation: [] })} />);
    expect(screen.queryByLabelText("WARMA water-permit offences")).toBeNull();
    expect(screen.queryByLabelText("Mining rights")).toBeNull();
    expect(screen.queryByLabelText("EITI reconciliation payments")).toBeNull();
  });
});
