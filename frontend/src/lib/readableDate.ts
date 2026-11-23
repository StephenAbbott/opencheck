/**
 * Dates as a reader writes them — "25 Feb 1964", "Sept 2026", "1964" — never
 * "1964-02-25". The 23 Nov 2026 production check found ISO dates on the
 * BackgroundCheck tab (birth dates, role dates) and in source details ("RNS
 * cut 2026-09-18, IGJ 2026-09"). The day form is `utcDate`'s, the saved-report
 * format, so one date reads the same everywhere.
 */

import { utcDate } from "./savedReport";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"];

/** A date at whatever precision the source gave it: a day, a month or a
 *  year. Anything that is not an ISO date comes back unchanged. */
export function readableDate(value: string | null | undefined): string {
  const text = (value ?? "").trim();
  if (!text) return "";
  let m = text.match(/^(\d{4})-(\d{2})$/);
  if (m) {
    const month = Number(m[2]);
    return month >= 1 && month <= 12 ? `${MONTHS[month - 1]} ${m[1]}` : text;
  }
  m = text.match(/^\d{4}-\d{2}-\d{2}(?:[T ].*)?$/);
  if (m) return utcDate(text.length === 10 ? `${text}T00:00:00Z` : text);
  return text;
}

/** Every ISO day (and year-month) inside a free-text sentence, reworded. */
export function readableDatesIn(text: string): string {
  return text
    .replace(/\b\d{4}-\d{2}-\d{2}(?:T[\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)?\b/g, (d) => readableDate(d))
    .replace(/\b(\d{4})-(0[1-9]|1[0-2])\b(?!-\d)/g, (d) => readableDate(d));
}
