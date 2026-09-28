/**
 * What the Securities section says about GLEIF's ISIN list (Phase 253).
 *
 * `/securities` now caches GLEIF's ISIN page for a day and lets an older page
 * (up to thirty days) stand in when GLEIF cannot be asked. When nothing can
 * stand in, the response says *why*: OpenCheck kept its last GLEIF requests
 * for lookups (`held_for_lookups` — nothing was sent), GLEIF is refusing
 * (`rate_limited`), or GLEIF did not answer (`unreachable`).
 *
 * Before this the notice said "GLEIF is rate-limiting or unreachable" in all
 * three cases. On 28 Sept 2026 a Quantexa lookup showed it for a call
 * OpenCheck had refused itself in 0.32 s — GLEIF was never asked. The
 * sentences live here, not in JSX, so the node suite can pin them.
 */

import { utcDate } from "./savedReport";

export type IsinListUnavailableReason = "held_for_lookups" | "rate_limited" | "unreachable";

/** `openfigi` is not a lookup source, so the lookup's name map lacks it and
 *  `sourceLabel` would invent "Openfigi". This is how OpenFIGI spells itself. */
export const SECURITIES_SOURCE_NAMES: Record<string, string> = {
  openfigi: "OpenFIGI",
};

const UNKNOWN_COUNT =
  "how many securities are mapped to this LEI is unknown for the moment — try again in a minute.";

/** The amber notice when no ISIN list could be shown at all. */
export function isinListUnavailableNotice(
  reason: string | null | undefined,
  sanctionedCount: number,
): string {
  let lead: string;
  switch (reason) {
    case "held_for_lookups":
      lead =
        "OpenCheck did not ask GLEIF for this LEI's ISIN list just now: it keeps its " +
        "last few GLEIF requests each minute for lookups, and they were in use. So " +
        UNKNOWN_COUNT;
      break;
    case "rate_limited":
      lead =
        "GLEIF is rate-limiting OpenCheck's requests, so its ISIN list could not be " +
        "fetched and " +
        UNKNOWN_COUNT;
      break;
    case "unreachable":
      lead = "GLEIF did not answer, so its ISIN list could not be fetched and " + UNKNOWN_COUNT;
      break;
    default:
      // An older backend that sends no reason: say what is known, blame no one.
      lead = "GLEIF's ISIN list could not be fetched, so " + UNKNOWN_COUNT;
  }
  if (sanctionedCount > 0) return lead;
  return (
    lead +
    " The sanctioned-securities check did run, from OpenCheck's local OpenSanctions " +
    "index: it records no sanctioned securities for this entity."
  );
}

/** Why a stand-in page is not today's, as a clause. */
function staleCause(reason: string | null | undefined): string {
  switch (reason) {
    case "held_for_lookups":
      return "OpenCheck kept its last GLEIF requests for lookups";
    case "rate_limited":
      return "GLEIF is rate-limiting OpenCheck's requests";
    case "unreachable":
      return "GLEIF did not answer";
    default:
      return "GLEIF could not be asked";
  }
}

/** The line under the count when an older page stood in, or `null` when the
 *  list is current. Dated by when GLEIF was asked, never by today. */
export function isinListStaleLine(
  stale: boolean | undefined,
  asOf: string | null | undefined,
  reason: string | null | undefined,
): string | null {
  if (!stale) return null;
  const when = asOf ? `as of ${utcDate(asOf)}` : "from an earlier check";
  return `GLEIF's ISIN list ${when} — OpenCheck could not re-check it just now (${staleCause(reason)}).`;
}
