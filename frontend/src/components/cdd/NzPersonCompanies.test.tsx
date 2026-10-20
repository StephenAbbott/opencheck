/**
 * The New Zealand person's other companies, on the BackgroundCheck card:
 * nothing is fetched until asked for, the register is asked about the name as
 * filed, every row carries its match strength, and a saved report shows no
 * control at all.
 */
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

const fetchMock = vi.fn((_n: string, _name: string) =>
  Promise.resolve({
    company_number: "1166320",
    name: "Mark Julian Verbiest",
    available: true,
    reason: null,
    subject_name: "MERIDIAN ENERGY LIMITED",
    person: {
      name: "Mark Julian Verbiest",
      role_here: ["director"],
      other_company_count: 2,
      high_confidence_count: 1,
      address_match_count: 1,
      name_only_count: 1,
      as_director: 2,
      as_shareholder: 0,
      total_records_under_name: 2,
      truncated: false,
      companies: [
        { number: "111", name: "ALPHA LIMITED", nzbn: "9429000000111", roles: ["director"], share_percentage: null,
          confidence: "high", basis: "Same registered address",
          link: "https://www.nzbn.govt.nz/mynzbn/nzbndetails/9429000000111/" },
        { number: "222", name: "BETA LIMITED", nzbn: null, roles: ["director"], share_percentage: null,
          confidence: "low", basis: "Same name — may differ", link: null },
      ],
    },
  }),
);

vi.mock("../../lib/api", () => ({
  nzPersonAssociations: (n: string, name: string) => fetchMock(n, name),
}));

import { NzPersonCompanies } from "./NzPersonCompanies";
import { SavedReportContext } from "./savedReportContext";

describe("NzPersonCompanies", () => {
  it("fetches only when asked, with the filed name, and grades every row", async () => {
    render(<NzPersonCompanies companyNumber="1166320" filedName="Mark Julian Verbiest" name="Mark Verbiest" />);
    expect(fetchMock).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /Companies on the New Zealand register for Mark Verbiest/ }));
    expect(await screen.findByText("BETA LIMITED")).toBeTruthy();
    expect(fetchMock).toHaveBeenCalledWith("1166320", "Mark Julian Verbiest");
    expect(screen.getByRole("link", { name: "ALPHA LIMITED" }).getAttribute("href")).toBe(
      "https://www.nzbn.govt.nz/mynzbn/nzbndetails/9429000000111/",
    );
    expect(screen.getByText(/1 with a matching address, 1 on the name alone/)).toBeTruthy();
    expect(screen.getByText(/Strong match/)).toBeTruthy();
    expect(screen.getByText(/Name only/)).toBeTruthy();
  });

  it("is absent from a saved report", () => {
    const { container } = render(
      <SavedReportContext.Provider value={{} as never}>
        <NzPersonCompanies companyNumber="1166320" filedName="X Y" />
      </SavedReportContext.Provider>,
    );
    expect(container.textContent).toBe("");
  });
});
