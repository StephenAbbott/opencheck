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
| `unavailable` | PermID gave no usable answer — see the table below | "Primary listing: could not be checked — …", naming the failure |
| *(no event)* | no `PERMID_API_KEY`, or live calls off | nothing |

A failure is said **on the listing line**, deliberately not in
`degraded_sources`: the verdict, the MCP caution and the batch chip all read
that list as "a screen did not run", and a listing lookup is not a screen.

An `unavailable` event carries a `reason`, and the line names it (Phase 285 —
until then every failure read "PermID did not answer", which was untrue of an
error response):

| `reason` | What happened | The line |
|---|---|---|
| `timeout` | No answer within 10 s per request, or 25 s for the chain | "could not be checked — PermID did not answer" |
| `rate_limited` | HTTP 429 | "could not be checked — PermID is limiting requests" |
| `upstream_error` | Any other HTTP error, or a network failure | "could not be checked — PermID returned an error" |
| `bad_response` | A 2xx whose body is not a JSON object | "could not be checked — PermID returned an error" |

`detail` is secret-scrubbed and never carries a URL (the access token is a
query parameter). For `bad_response` it says what PermID actually sent — the
status, the content type and the first 120 characters of the body, e.g.
`200 application/ld+json: "An error has occurred."`.

**Why `bad_response` exists.** On 3 October 2026 PermID's organisation-record
service failed for every entity and every format (`json-ld`, `turtle`) while
search kept working. It answered `200`, `content-type: application/ld+json`,
body `An error has occurred.` — every marker of success — so
`raise_for_status()` passed it, `.json()` failed, and production reported
`JSONDecodeError … char 0` under "PermID did not answer". permid.org also
serves its website at any path with a `200 text/html` page (`format=json`
lands there). Neither can be caught by status code; both are caught by
requiring a JSON object.

Search alone cannot stand in for a broken record service: a quote search
result's `isQuoteOf` names an *instrument*, not the organisation, and quote
search by organisation PermID returns nothing — the only link from an LEI's
organisation to its primary quote is `hasOrganizationPrimaryQuote` on the
record. During such an outage the line says "could not be checked".

Cache: 30 days for a listing, 7 days for "no primary quote", **one hour for a
failure** (Phase 285), under its own key (`permid/unavailable/<LEI>`) so a
failure can never overwrite a listing. Before Phase 285 failures were never
cached, and during the outage every lookup re-spent its calls against the
5,000-a-day key on a chain that could not succeed.

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
