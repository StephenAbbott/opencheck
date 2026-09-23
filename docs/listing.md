# Primary listing (LSEG PermID)

Phase 236. For a listed company, OpenCheck shows **where its primary listing
is** — the exchange, the ticker, and a link to the exchange's page for the
security — on the subject card, at the top of the Securities panel, in the
PDF / Markdown report, in the MCP `opencheck_lookup` result (`listing`) and
batch rows (`primary_listing`), and in the BODS export
(`publicListing.securitiesListings` on the subject's GLEIF entity statement).

Code: `backend/opencheck/listing.py` (fetch, venues, BODS) and
`frontend/src/lib/listing.ts` (wording). Tests: `backend/tests/test_listing.py`,
`frontend/src/lib/listing.test.ts`, `frontend/src/components/cdd/SubjectCardListing.test.tsx`.

## Why PermID

GLEIF publishes LEI → ISIN and nothing about where a security trades — Shell
has 1,814 ISINs, mostly debt, none marked as the share. OpenFIGI types an ISIN
but cannot say which of a company's ISINs is its ordinary share. PermID can:
an organisation record carries the LEI and names its **primary quote**, and
the quote carries the ticker, the RIC and the venue's ISO 10383 MIC. In a
22-LEI test (24 Sept 2026) all 21 listed companies resolved and an unlisted
control correctly had no quote.

## How it runs

Three requests, started right after `gleif_done` so they overlap the source
fan-out, awaited before `subject_profile`:

1. `GET https://api-eit.refinitiv.com/permid/search?q=lei:{LEI}` → organisation
2. `GET https://permid.org/1-{org}?format=json-ld` → `tr-org:hasLEI` (checked
   against the LEI — the search is a text search) and
   `hasOrganizationPrimaryQuote`
3. `GET https://permid.org/1-{quote}?format=json-ld` → MIC, ticker, RIC

The result is one `listing` lookup event, folded into `LookupResponse.listing`
**as recorded** — a saved report, the exports and MCP read the frozen payload
and never re-fetch. It is not a registered source and does not count in the
coverage numbers.

| `status` | Meaning | What the reader sees |
|---|---|---|
| `listed` | PermID names a primary quote | "Primary listing: London Stock Exchange · SHEL (PermID)" |
| `not_listed` | PermID has no primary quote for the LEI | nothing — OpenCheck never says a company is unlisted |
| `unavailable` | PermID did not answer (`reason`: `rate_limited`, `timeout`, `upstream_error`) | "Primary listing: could not be checked — PermID did not answer" |
| *(no event)* | no `PERMID_API_KEY`, or live calls off | nothing |

A failure is said **on the listing line**, deliberately not in
`degraded_sources`: the verdict, the MCP caution and the batch chip all read
that list as "a screen did not run", and a listing lookup is not a screen.

Cache: 30 days for a listing, 7 days for "no primary quote"; failures are
never cached.

## Venue links

Only patterns browser-verified on 24 Sept 2026 are linked. Every other venue
shows its name (or its MIC) and ticker with no link.

| MIC | Link |
|---|---|
| XLON | `londonstockexchange.com/stock/{TICKER}/x/company-page` |
| XNGS, XNMS, XNCM, XNAS | `nasdaq.com/market-activity/stocks/{ticker}` |
| XNYS | `nyse.com/quote/XNYS:{TICKER}` |
| XTSE | `money.tmx.com/en/quote/{TICKER}` |
| XASX | `asx.com.au/markets/company/{TICKER}` |
| XCSE, XSTO | `nasdaq.com/european-market-activity/shares/{ticker, space→-}` |
| XNSA | `ngxgroup.com/exchange/data/company-profile/?symbol={TICKER}&directory=companydirectory` |
| Euronext (XAMS, XPAR, XBRU, XLIS, XMSM, XOSL, XMIL) | the ticker **search** page, `live.euronext.com/en/search_instruments/{TICKER}` — the direct page needs an ISIN, which PermID does not publish |

These are listing pages, **never filings**; BODS `companyFilingsURLs` stays
empty. Frankfurt/Xetra, SIX and Euronext's direct page need an ISIN; NSE and
JPX block automated checks; B3 has no ticker-based pattern.

## BODS

`publicListing = {hasPublicListing: true, securitiesListings: [{stockExchangeJurisdiction, stockExchangeName, marketIdentifierCode, security: {ticker}}]}`
on a **copy** of the subject's GLEIF entity statement, plus a `linking`
annotation (`url` = the PermID quote) saying OpenCheck added it from PermID.
`security.idScheme` is omitted: PermID is not in the closed
`securitiesIdentifierSchemes` codelist (isin, figi, cusip, cins). A venue with
no known name or jurisdiction adds nothing to BODS.

## Licence and limits

PermID's basic fields are CC-BY 4.0 by API for registered users ("API keys
may be used for any purpose, including commercial applications"); the extended
CC-NC fields are not used. 5,000 requests a day per key, 1–4 a second.
The key is sent as the `access-token` query parameter, so errors are described
through `secret_scrub`, never stringified.

## Setup

Set `PERMID_API_KEY` in `.env` locally and in the Render dashboard. Without it
the feature is off and nothing is requested.
