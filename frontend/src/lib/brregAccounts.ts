/**
 * Brreg annual accounts (Regnskapsregisteret) on the Norwegian register card.
 *
 * The backend reads the accounts register into the brreg hit's
 * `raw.accounts` (backend/opencheck/sources/brreg_accounts.py): facts about
 * the latest filed period and a link to the register's copy of the filed
 * accounts. Stephen's decisions (26 Oct 2026): facts only, never a figure;
 * the PDF is linked, never fetched; nothing in BODS. The row's sentence
 * (`finding_brreg`) already says what was filed, with group accounts, audit
 * opt-out and liquidation accounts; this list adds the details a reader
 * checks against the filing itself, without repeating the sentence.
 *
 * Read from the hit, so a saved report shows what was recorded on the day
 * of the run and nothing is fetched.
 */

import type { SourceHit } from "./api";
import { formatProfileDate } from "./subjectProfile";

export type BrregAccountsStatus = "filed" | "none" | "unsupported" | "unavailable";

export interface BrregAccounts {
  status: BrregAccountsStatus;
  period_start?: string | null;
  period_end?: string | null;
  year?: number | null;
  journal_number?: string | null;
  group_accounts?: boolean;
  group_parent?: boolean;
  audit_opted_out?: boolean;
  not_audited?: boolean;
  liquidation_accounts?: boolean;
  small_company?: boolean;
  currency?: string | null;
  rules?: string | null;
  pdf_url?: string | null;
  source_url?: string | null;
  format?: string;
  format_words?: string;
}

/** `regnkapsprinsipper.regnskapsregler`, in English — the backend's
 *  `RULES_WORDS` (brreg_accounts.py), pinned by test_frontend_parity.py. */
export const RULES_WORDS: Record<string, string> = {
  IFRS: "IFRS",
  forenkletAnvendelseIFRS: "simplified IFRS",
  regnskapslovenAlminneligRegler: "Norwegian Accounting Act, general rules",
};

export const ACCOUNTS_HEADING = "Latest annual accounts";

export const ACCOUNTS_SOURCE_NOTE =
  "From Brønnøysundregistrene's accounts register (Regnskapsregisteret). " +
  "OpenCheck shows what was filed, not the figures in it.";

export function brregAccountsOf(hit: SourceHit): BrregAccounts | null {
  if (hit.source_id !== "brreg") return null;
  const raw = hit.raw as Record<string, unknown> | undefined;
  const accounts = raw?.accounts as BrregAccounts | undefined | null;
  if (!accounts || typeof accounts !== "object") return null;
  return accounts;
}

function periodText(a: BrregAccounts): string | null {
  if (!a.period_start || !a.period_end) return null;
  return `${formatProfileDate(a.period_start)} – ${formatProfileDate(a.period_end)}`;
}

/** The detail lines for a filed period, in reading order. Empty for any
 *  other status: the row's sentence already says "none", "not read" or
 *  "a format the open API does not serve". */
export function brregAccountsFacts(a: BrregAccounts | null): string[] {
  if (!a || a.status !== "filed") return [];
  const facts: string[] = [];
  const period = periodText(a);
  if (period) facts.push(`Period ${period}`);
  if (a.journal_number) facts.push(`Journal no. ${a.journal_number}`);
  if (a.currency) facts.push(`Reported in ${a.currency}`);
  if (a.rules) facts.push(RULES_WORDS[a.rules] ?? a.rules);
  if (a.small_company) facts.push("Small-company rules");
  if (a.group_parent) facts.push("Filed as a parent company");
  return facts;
}

/** The link to the register's copy of the filed accounts, when it lists one. */
export function brregAccountsPdf(a: BrregAccounts | null): { url: string; label: string } | null {
  if (!a || a.status !== "filed" || !a.pdf_url) return null;
  const year = a.year ? ` ${a.year}` : "";
  return { url: a.pdf_url, label: `Filed annual accounts${year} (PDF)` };
}
