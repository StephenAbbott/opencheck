/**
 * EstmaCard — claims that live in the markup: the total is labelled as a sum
 * of payee lines and the project view as the same payments, two reports for
 * one year stay apart, a filed-in-another-report entity never reads as
 * owned or as paying nothing, and the LEI is a graded match.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { EstmaCard } from "./EstmaCard";
import type { SourceHit } from "../../lib/api";

const report = (over: Record<string, unknown> = {}) => ({
  year: "2025",
  period_start: "2025-01-01",
  period_end: "2025-12-31",
  report_type: "CON",
  report_type_label: "Consolidated Report",
  url: "https://example.com/suncor-2025",
  included_in: null,
  payee_total_cad: 160,
  payee_count: 2,
  project_count: 2,
  by_payment_type: [{ code: "ROYALT", label: "Royalties", cad: 100 }],
  by_country: [{ code: "CA", name: "Canada", cad: 150, payees: 1 }, { code: "NO", name: "Norway", cad: 10, payees: 1 }],
  payees: [{ name: "PROVINCE OF ALBERTA", country_code: "CA", country_name: "Canada", total_cad: 150, by_payment_type: [] }],
  projects: [{ name: "OIL SANDS", country_code: "CA", country_name: "Canada", total_cad: 150, by_payment_type: [] }],
  ...over,
});

function hit(entityOver: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "estma",
    hit_id: "894500XO86FKXQGPYV71",
    name: "SUNCOR ENERGY INC.",
    summary: "ESTMA E405435 · 2025",
    raw: {
      lei: "894500XO86FKXQGPYV71",
      gleif_legal_name: "Suncor Energy Inc.",
      latest_year: "2025",
      cut: "2026-11-05",
      entities: [
        {
          estma_id: "E405435",
          name: "SUNCOR ENERGY INC.",
          reports_payments: true,
          includes: { year: "2025", entities: [{ estma_id: "E984046", name: "CANADIAN OIL SANDS PARTNERSHIP" }] },
          match: { method: "exact name, one live LEI", basis: "the names are the same", gleif_name: "Suncor Energy Inc.", note: "" },
          reports: [report()],
          ...entityOver,
        },
      ],
    },
  } as unknown as SourceHit;
}

describe("EstmaCard", () => {
  it("labels the total as a sum of payee lines and the project view as the same payments", () => {
    render(<EstmaCard hit={hit()} />);
    expect(screen.getByText("CAD 160")).toBeInTheDocument();
    expect(screen.getByText(/OpenCheck's sum of the 2 payee lines/)).toBeInTheDocument();
    expect(screen.getByText("By project (the same payments)")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Read the report/ })).toHaveAttribute("href", "https://example.com/suncor-2025");
  });

  it("grades the LEI as a match, not an identifier ESTMA publishes", () => {
    render(<EstmaCard hit={hit()} />);
    expect(screen.getByText(/Possible match/)).toBeInTheDocument();
    expect(screen.getByText(/carries no LEI or business number/)).toBeInTheDocument();
  });

  it("keeps two reports for one year apart", () => {
    render(<EstmaCard hit={hit({ reports: [report(), report({ payee_total_cad: 5, report_type: "IND_ST", url: "https://example.com/stub" })] })} />);
    expect(screen.getByText(/2 reports were filed for 2025/)).toBeInTheDocument();
    expect(screen.getByText("CAD 160")).toBeInTheDocument();
    expect(screen.getByText("CAD 5")).toBeInTheDocument();
    expect(screen.queryByText("CAD 165")).toBeNull();
  });

  it("says a reporting arrangement is not ownership", () => {
    render(
      <EstmaCard
        hit={hit({
          includes: null,
          reports: [report({ included_in: { estma_id: "E405435", name: "SUNCOR ENERGY INC." }, payee_total_cad: undefined })],
        })}
      />,
    );
    expect(screen.getByText(/in the report of/)).toBeInTheDocument();
    expect(screen.getByText(/not a statement of ownership/)).toBeInTheDocument();
    expect(screen.queryByText("CAD 0")).toBeNull();
  });

  it("lists the entities the report also covers, as a reporting arrangement", () => {
    render(<EstmaCard hit={hit()} />);
    expect(screen.getByText("CANADIAN OIL SANDS PARTNERSHIP")).toBeInTheDocument();
    expect(screen.getByText(/not a list of subsidiaries/)).toBeInTheDocument();
  });
});
