import { describe, expect, it } from "vitest";

import type { PrimaryListing } from "./api";
import { LISTING_EXPLANATION, UNAVAILABLE_TEXT, listingView } from "./listing";

function listed(over: Partial<PrimaryListing> = {}): PrimaryListing {
  return {
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
    ...over,
  };
}

describe("listingView", () => {
  it("names the venue and ticker and links the venue page", () => {
    const v = listingView(listed());
    expect(v?.text).toBe("London Stock Exchange · SHEL");
    expect(v?.href).toBe("https://www.londonstockexchange.com/stock/SHEL/x/company-page");
    expect(v?.linkLabel).toBe("London Stock Exchange page for SHEL (opens in a new tab)");
    expect(v?.unavailable).toBe(false);
  });

  it("says a Euronext link is a search page", () => {
    const v = listingView(
      listed({
        exchange: { name: "Euronext Amsterdam", country: "NL" },
        link: { url: "https://live.euronext.com/en/search_instruments/HEIA", kind: "search" },
        quote: { ...listed().quote!, ticker: "HEIA", mic: "XAMS" },
      }),
    );
    expect(v?.linkLabel).toBe("Search Euronext Amsterdam for HEIA (opens in a new tab)");
  });

  it("shows an unnamed venue by its MIC, with no link", () => {
    const v = listingView(
      listed({ exchange: null, link: null, quote: { ...listed().quote!, mic: "ZZZZ" } }),
    );
    expect(v?.text).toBe("MIC ZZZZ · SHEL");
    expect(v?.href).toBeNull();
  });

  it("says nothing when PermID records no primary quote, or nothing was checked", () => {
    expect(listingView(listed({ status: "not_listed", quote: null, exchange: null, link: null }))).toBeNull();
    expect(listingView(null)).toBeNull();
    expect(listingView(undefined)).toBeNull();
  });

  it("says a failed check on the line", () => {
    const v = listingView(listed({ status: "unavailable", quote: null, exchange: null, link: null }));
    expect(v?.text).toBe(UNAVAILABLE_TEXT);
    expect(v?.unavailable).toBe(true);
    expect(v?.href).toBeNull();
  });

  it("never calls the link a filing, and says other listings are not shown", () => {
    expect(LISTING_EXPLANATION).toMatch(/not its regulatory filings/);
    expect(LISTING_EXPLANATION).toMatch(/Other listings of the same company are not shown/);
    expect(LISTING_EXPLANATION).toMatch(/CC-BY 4\.0/);
  });
});
