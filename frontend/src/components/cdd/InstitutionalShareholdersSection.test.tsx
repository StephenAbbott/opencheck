/**
 * Phase 336 — the "Institutional shareholders" section under Securities.
 *
 * `lib/form13f.test.ts` pins the words. What only rendering shows: collapsed
 * there is one row per security and opening shows twenty, the toggle says
 * what it controls, the disclaimer and the attribution are visible in both
 * states, a percentage is shown with its basis, a value in thousands is not
 * shown, an ADR is labelled and has no % column, and nothing is totalled.
 */
import { describe, expect, it } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";

import { InstitutionalShareholdersSection } from "./InstitutionalShareholdersSection";
import { THOUSANDS, payload, pos, sec } from "../../test/form13fFixture";

function bodyRows(table: HTMLElement) {
  return within(table).getAllByRole("row").slice(1); // minus the header row
}

describe("InstitutionalShareholdersSection", () => {
  it("renders nothing without a payload", () => {
    const { container } = render(<InstitutionalShareholdersSection payload={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("shows the largest manager collapsed and twenty when opened", () => {
    render(<InstitutionalShareholdersSection payload={payload()} />);
    expect(screen.getByRole("heading", { name: "Institutional shareholders" })).toBeInTheDocument();
    const table = screen.getByRole("table", { name: /MOODYS CORP · COM/ });
    expect(bodyRows(table)).toHaveLength(1);
    expect(within(table).getByText("Berkshire Hathaway Inc")).toBeInTheDocument();
    expect(within(table).getByText("24,669,778")).toBeInTheDocument();
    expect(within(table).getByText("14.24%")).toBeInTheDocument();
    // The basis travels with the percentage, collapsed too.
    expect(screen.getByText(/computed by OpenCheck/)).toBeInTheDocument();

    const toggle = screen.getByRole("button", { name: "Show the next 19" });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(toggle);
    expect(bodyRows(screen.getByRole("table"))).toHaveLength(20);
    expect(screen.getByRole("button", { name: "Show only the largest" })).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByText(/1,401 managers reported a position; the 20 largest/)).toBeInTheDocument();
    expect(screen.getByText(/so they are never added together/)).toBeInTheDocument();
  });

  it("keeps the disclaimer and the attribution visible while collapsed", () => {
    render(<InstitutionalShareholdersSection payload={payload()} />);
    expect(screen.getByText(/It is not a record of beneficial ownership/)).toBeInTheDocument();
    const link = screen.getByRole("link", { name: /Form 13F data sets/ });
    expect(link).toHaveAttribute("href", "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets");
    expect(link).toHaveAttribute("rel", "noopener noreferrer");
    expect(screen.getByText(/not verified by the SEC/)).toBeInTheDocument();
  });

  it("never shows a value reported in thousands, and says why", () => {
    const s = sec({ positions: [pos("T. Rowe Price Associates", 900_000, { value: null, value_note: THOUSANDS })] }, 1);
    render(<InstitutionalShareholdersSection payload={payload({ securities: [s] })} />);
    expect(screen.getAllByText(/Not shown/).length).toBeGreaterThan(0);
    expect(screen.getByText(THOUSANDS)).toBeInTheDocument();
    expect(screen.queryByText(/\$/)).toBeNull();
  });

  it("labels an ADR and gives it no percentage column", () => {
    const adr = sec(
      {
        issuer_name: "SHELL PLC",
        title_of_class: "SPON ADS",
        is_adr: true,
        adr_note: "American depositary shares traded in the US. No percentage is computed: one depositary share can stand for more or fewer than one underlying share.",
        shares_outstanding: null,
        positions: [pos("FMR LLC", 87_332_346)],
      },
      1,
    );
    render(<InstitutionalShareholdersSection payload={payload({ securities: [adr] })} />);
    expect(screen.getByText("American depositary shares")).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: /Percentage/ })).toBeNull();
    expect(screen.getByText(/No percentage is computed/)).toBeInTheDocument();
    // One row only: nothing to open.
    expect(screen.queryByRole("button", { name: /Show the next/ })).toBeNull();
  });

  it("lists two classes apart and adds them up nowhere", () => {
    const b = sec({ cusip: "084670702", issuer_name: "BERKSHIRE HATHAWAY INC DEL", title_of_class: "CL B NEW", shares_outstanding: null });
    const a = sec({ cusip: "084670108", issuer_name: "BERKSHIRE HATHAWAY INC DEL", title_of_class: "CL A", shares_outstanding: null });
    for (const s of [a, b]) for (const p of s.positions) p.percent = null;
    render(<InstitutionalShareholdersSection payload={payload({ securities: [b, a] })} />);
    expect(screen.getAllByRole("table")).toHaveLength(2);
    expect(screen.getByRole("button", { name: "Show the next 19 for each security" })).toBeInTheDocument();
    expect(screen.queryByText(/total/i)).toBeNull();
    expect(screen.queryByText(/%/)).toBeNull();
  });

  it("says an out-of-date index in its line, with no table", () => {
    render(
      <InstitutionalShareholdersSection
        payload={payload({
          status: "out_of_date",
          securities: [],
          line: "The newest Form 13F data OpenCheck holds is for the quarter ending 2025-12-31, too old to show.",
        })}
      />,
    );
    expect(screen.getByText(/too old to show/)).toBeInTheDocument();
    expect(screen.queryByRole("table")).toBeNull();
  });
});
