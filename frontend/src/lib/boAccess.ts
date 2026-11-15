// The words of the beneficial-ownership access note on a national register's
// source card (`BoAccessFootnote` in SourceBucketCard.tsx).
//
// The note names the register that holds the beneficial owners, because it is
// not always the register the card shows: Ireland's CRO card sits beside the
// Register of Beneficial Owners (RBO), a separate register run by a different
// department. "Beneficial ownership data from Ireland is not public", printed
// inside the CRO card, read as if the CRO withheld it.

import type { BoAccessNotice } from "./api";

export const BO_ACCESS_HEADING = "Beneficial ownership register";

export function formatAccessDate(iso: string): string {
  const d = new Date(iso + "T00:00:00");
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric" });
}

/** The note's sentence. `register_name` is optional: a notice without one
 * (an older backend, a row with no register name) keeps the country wording. */
export function boAccessSentence(notice: BoAccessNotice): string {
  const { status, country_name, effective_date } = notice;
  const register = notice.register_name?.trim();
  const where = register
    ? `Beneficial ownership information for ${country_name} is kept in the ${register}`
    : `Beneficial ownership data from ${country_name}`;
  if (status === "becoming_restricted" && effective_date) {
    return register
      ? `${where}. It is currently public but will be restricted to certain groups from ${formatAccessDate(effective_date)}.`
      : `${where} is currently public but will be restricted to certain groups from ${formatAccessDate(effective_date)}.`;
  }
  return register
    ? `${where}, not in this source. It is not public: available to certain groups only.`
    : `${where} is not public: available to certain groups only.`;
}

export function boAccessLinkText(notice: BoAccessNotice): string {
  return notice.status === "becoming_restricted"
    ? "Learn how to apply for access after then"
    : "Learn how to apply for access";
}
