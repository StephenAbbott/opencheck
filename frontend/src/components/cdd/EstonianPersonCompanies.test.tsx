/**
 * The Estonian person's companies: nothing is fetched until asked for, the
 * answer is a list (one row per company, the person's positions under it),
 * and a saved report shows no control at all — the list is today's, not the
 * saved run's.
 */
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

const HASH = "77a468f2-248a-5e41-8356-c975af9613a5";

const fetchMock = vi.fn((_hash: string) =>
  Promise.resolve({
    officer_id: HASH,
    name: "Veronika Oškina",
    birth_date: null,
    is_stub: false,
    total_results: 2,
    active_count: 2,
    appointments: [
      { company_name: "007 Agent & Partners OÜ", company_number: "16752073", company_status: "Entered in the register",
        role: "Shareholder (osanik)", appointed_on: "2023-06-03", resigned_on: null, register_list: "shareholders",
        share_percent: "100.00" },
      { company_name: "Eesti Energia AS", company_number: "10421629", company_status: "Entered in the register",
        role: "Supervisory board member (nõukogu liige)", appointed_on: "2025-06-01", resigned_on: null,
        register_list: "off_card" },
    ],
    bods: [],
    attribution: "Estonian e-Business Register",
    caveat: "Positions are as recorded under one pseudonymous person key.",
    scheme: "EE-ARIREGISTER-HASH",
    source_id: "ariregister",
    as_of: "2026-10-16",
    company_count: 2,
    truncated: false,
  }),
);

vi.mock("../../lib/api", () => ({ estonianPersonCompanies: (h: string) => fetchMock(h) }));

import { EstonianPersonCompanies } from "./EstonianPersonCompanies";
import { SavedReportContext } from "./savedReportContext";

describe("EstonianPersonCompanies", () => {
  it("fetches only when asked, then lists each company once", async () => {
    render(<EstonianPersonCompanies personHash={HASH} name="Veronika Oškina" />);
    expect(fetchMock).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /Companies on the Estonian register for Veronika Oškina/ }));
    expect(await screen.findByText("Eesti Energia AS")).toBeTruthy();
    expect(fetchMock).toHaveBeenCalledWith(HASH);
    const link = screen.getByRole("link", { name: "007 Agent & Partners OÜ" });
    expect(link.getAttribute("href")).toBe("https://ariregister.rik.ee/eng/company/16752073");
    expect(screen.getByText(/Supervisory board member \(nõukogu liige\) · since 1 June 2025/)).toBeTruthy();
    expect(screen.getByText(/Named on the current lists of 2 companies/)).toBeTruthy();
  });

  it("is absent from a saved report", () => {
    const { container } = render(
      <SavedReportContext.Provider value={{} as never}>
        <EstonianPersonCompanies personHash={HASH} />
      </SavedReportContext.Provider>,
    );
    expect(container.textContent).toBe("");
  });
});
