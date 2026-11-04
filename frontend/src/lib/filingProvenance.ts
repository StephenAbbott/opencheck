/**
 * Which register filing declared a relationship — the declarant-and-filing-
 * provenance ticket (Open Ownership's "basic disclosure information").
 *
 * The backend (`backend/opencheck/bods/ch_filings.py`) puts one `commenting`
 * annotation per event on a Companies House PSC relationship, carrying the
 * matched filing under {@link FILING_PROPERTY}. This reads it back and words
 * the "Declared" row on the relationship card. The constants are mirrored
 * from the Python module and pinned by `backend/tests/test_ch_filings.py`,
 * which parses this file.
 */

import { formatProfileDate } from "./subjectProfile";

/** The annotation property carrying the filing, as the backend writes it. */
export const FILING_PROPERTY = "companiesHouseFiling";

/** How a notification was found — the backend's closed vocabulary. */
export const FILING_MATCHES = ["filing", "incorporation", "confirmation_statement", "none"] as const;
export type FilingMatch = (typeof FILING_MATCHES)[number];

export type FilingEvent = "notification" | "cessation";

export interface FilingFacts {
  event: FilingEvent;
  match: FilingMatch;
  declarant?: string;
  type?: string;
  transactionId?: string;
  date?: string;
  documentUrl?: string;
}

/** One line for the card: the words, and the filing to link where there is one. */
export interface DeclaredLine {
  text: string;
  linkText?: string;
  url?: string;
}

function isFacts(value: unknown): value is FilingFacts {
  if (!value || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return (
    (v.event === "notification" || v.event === "cessation") &&
    (FILING_MATCHES as readonly unknown[]).includes(v.match)
  );
}

/** The filing facts of one event on a statement, or `null`. */
export function filingOf(stmt: unknown, event: FilingEvent = "notification"): FilingFacts | null {
  const anns = (stmt as { annotations?: unknown } | null)?.annotations;
  if (!Array.isArray(anns)) return null;
  for (const ann of anns) {
    const facts = (ann as Record<string, unknown> | null)?.[FILING_PROPERTY];
    if (isFacts(facts) && facts.event === event) return facts;
  }
  return null;
}

function day(iso: string | undefined): string {
  return iso ? formatProfileDate(iso) : "an unrecorded day";
}

function form(facts: FilingFacts): string {
  return facts.type || "an unnamed form";
}

/** Companies House names no presenter: the company is the filer, and only that is said. */
const FILED_BY = "filed by the company";

/**
 * "Declared on 30 Oct 2023 via PSC02 (filing)" and its siblings. Absence is
 * worded in the same voice as presence, never as a finding.
 */
export function declaredLine(facts: FilingFacts | null): DeclaredLine | null {
  if (!facts) return null;
  const link = facts.documentUrl ? { linkText: "filing", url: facts.documentUrl } : {};
  if (facts.event === "cessation") {
    if (facts.match === "filing") {
      return { text: `Ceased on ${day(facts.date)} via ${form(facts)}, ${FILED_BY}`, ...link };
    }
    return { text: "No cessation filing in the register's filing history" };
  }
  switch (facts.match) {
    case "filing":
      return { text: `Declared on ${day(facts.date)} via ${form(facts)}, ${FILED_BY}`, ...link };
    case "incorporation":
      return {
        text: `Declared at incorporation, ${day(facts.date)} (${form(facts)}), ${FILED_BY}`,
        ...link,
      };
    case "confirmation_statement":
      return {
        text:
          `On no PSC form; before June 2017 PSCs were given on the next confirmation ` +
          `statement — ${form(facts)}, filed ${day(facts.date)}, ${FILED_BY}`,
        ...link,
      };
    default:
      return { text: "No notifying filing in the register's filing history" };
  }
}
