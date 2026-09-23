/**
 * The values layer for the primary stock-exchange listing (Phase 236).
 *
 * The backend (`opencheck/listing.py`) resolves an LEI to PermID's primary
 * quote and freezes it into the `listing` lookup event; this module turns
 * that payload into the words every surface shows, so the subject card, the
 * Securities panel and the tests read one wording. Pure, so the logic-only
 * suite can pin it.
 *
 * Rules (the PermID Notion ticket, Stephen 24 Sept 2026):
 * - "Primary listing", attributed to PermID — PermID names one main quote,
 *   so the line never implies it is the company's only listing.
 * - Nothing at all when there is no payload or PermID records no quote:
 *   OpenCheck does not assert that a company is unlisted.
 * - A failure is said on the line ("could not be checked"), never as a
 *   degraded screen.
 * - A link is a listing page or, for Euronext, a search page — never
 *   "filings".
 */

import type { PrimaryListing } from "./api";

export const LISTING_LABEL = "Primary listing";

export const UNAVAILABLE_TEXT = "could not be checked — PermID did not answer";

/** The explanation behind the line's ⓘ. */
export const LISTING_EXPLANATION =
  "From LSEG PermID (CC-BY 4.0), which names one main quote for a listed company. " +
  "Other listings of the same company are not shown. The link opens the exchange's " +
  "page for the security, not its regulatory filings.";

export interface ListingView {
  /** "London Stock Exchange · SHEL", or the could-not-check text. */
  text: string;
  /** The venue page, when a verified pattern exists. */
  href: string | null;
  /** Accessible name for the link: says where it goes. */
  linkLabel: string | null;
  /** True when PermID was asked and did not answer. */
  unavailable: boolean;
  /** The security PermID names ("Shell Ord Shs", "Petroleo Brasileiro Pref Shs"). */
  security: string | null;
}

/** What to render for a `listing` payload, or null for nothing at all. */
export function listingView(listing: PrimaryListing | null | undefined): ListingView | null {
  if (!listing) return null;
  if (listing.status === "unavailable") {
    return { text: UNAVAILABLE_TEXT, href: null, linkLabel: null, unavailable: true, security: null };
  }
  if (listing.status !== "listed" || !listing.quote) return null;
  const q = listing.quote;
  const venue = listing.exchange?.name ?? (q.mic ? `MIC ${q.mic}` : null);
  const text = [venue, q.ticker].filter(Boolean).join(" · ");
  if (!text) return null;
  const href = listing.link?.url ?? null;
  const where = listing.exchange?.name ?? "the exchange";
  const linkLabel = href
    ? listing.link?.kind === "search"
      ? `Search ${where} for ${q.ticker ?? "this security"} (opens in a new tab)`
      : `${where} page for ${q.ticker ?? "this security"} (opens in a new tab)`
    : null;
  return { text, href, linkLabel, unavailable: false, security: q.name ?? null };
}
