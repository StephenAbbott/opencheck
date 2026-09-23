/**
 * Phase 236 — the primary-listing line on the subject card.
 *
 * `lib/listing.test.ts` pins the words. What only rendering shows: the link
 * has an accessible name saying where it goes and opens safely in a new tab,
 * the explanation is reachable by keyboard (an `Explain` button, never a
 * `title`), a failed check is said on the line, and no payload renders no
 * line at all — the card never says a company is unlisted.
 */
import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

import type { PrimaryListing } from "../../lib/api";
import { SubjectCard } from "./SubjectCard";

const LISTED: PrimaryListing = {
  source_id: "permid",
  attribution: "LSEG PermID (CC-BY 4.0)",
  licence: "CC-BY-4.0",
  lei: "21380068P1DRHMJ8KU70",
  status: "listed",
  as_of: "2026-09-24",
  organisation: { permid: "4295885039", url: "https://permid.org/1-4295885039" },
  quote: {
    permid: "55836049491",
    url: "https://permid.org/1-55836049491",
    name: "SHELL ORD",
    ticker: "SHEL",
    mic: "XLON",
    ric: "SHEL.L",
    exchange_code: "LSE",
    instrument_permid: "8590936103",
  },
  exchange: { name: "London Stock Exchange", country: "GB" },
  link: { url: "https://www.londonstockexchange.com/stock/SHEL/x/company-page", kind: "listing" },
};

function card(listing: PrimaryListing | null) {
  return render(
    <SubjectCard
      lei="21380068P1DRHMJ8KU70"
      legalName="SHELL PLC"
      jurisdiction="GB"
      onPdf={() => {}}
      onMarkdown={() => {}}
      listing={listing}
    />,
  );
}

describe("SubjectCard primary listing", () => {
  it("links the venue page with a name that says where it goes", () => {
    card(LISTED);
    const link = screen.getByRole("link", { name: "London Stock Exchange page for SHEL (opens in a new tab)" });
    expect(link.getAttribute("href")).toBe("https://www.londonstockexchange.com/stock/SHEL/x/company-page");
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.getAttribute("rel")).toContain("noopener");
    expect(link.className).toContain("underline");
    expect(screen.getByText("Primary listing:")).toBeTruthy();
    expect(screen.getByText("(PermID)")).toBeTruthy();
  });

  it("explains the line in flow, by keyboard-reachable button", () => {
    card(LISTED);
    fireEvent.click(screen.getByRole("button", { name: "About the primary listing" }));
    expect(screen.getByText(/Other listings of the same company are not shown/)).toBeTruthy();
  });

  it("says a failed check on the line, with no link", () => {
    card({ ...LISTED, status: "unavailable", quote: null, exchange: null, link: null });
    expect(screen.getByText("could not be checked — PermID did not answer")).toBeTruthy();
    expect(screen.queryByRole("link", { name: /opens in a new tab/ })).toBeNull();
  });

  it("renders no line when nothing was checked or no quote is recorded", () => {
    const { unmount } = card(null);
    expect(screen.queryByText("Primary listing:")).toBeNull();
    unmount();
    card({ ...LISTED, status: "not_listed", quote: null, exchange: null, link: null });
    expect(screen.queryByText("Primary listing:")).toBeNull();
  });
});
