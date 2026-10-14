/**
 * The values layer for institutional shareholders from SEC Form 13F
 * (Phase 336, the frontend half of Phases 333–334).
 *
 * The backend (`opencheck/form13f.py`) freezes one `form13f` event into the
 * run; this module turns it into the words the QuickCheck section shows, so
 * the logic-only suite can pin them. Stephen's rules (14 Oct 2026):
 *
 * - **A 13F filer is an investment manager with discretion, not a beneficial
 *   owner.** Nothing here is ownership: no graph node, no signal, no total.
 *   Every sentence this module writes is held to `BANNED_VOCABULARY` — the
 *   same list as `form13f.BANNED_VOCABULARY`, which
 *   `backend/tests/test_form13f.py` parses this file to compare — except the
 *   backend's `disclaimer`, the one sentence that says what 13F is not.
 * - The section is "Institutional shareholders", under Securities. Collapsed
 *   it shows the largest manager by shares; opened, the next nineteen.
 * - **Never a total.** Positions overlap, so no sum is computed anywhere.
 * - A percentage only where the backend computed one, always with its basis
 *   sentence, labelled "computed by OpenCheck".
 * - A value still reported in thousands shows its shares and not its value.
 * - ADRs are labelled and never get a percentage.
 */

import type { Form13fPayload, Form13fPosition, Form13fSecurity } from "./api";
import { utcDate } from "./savedReport";

export const FORM13F_HEADING = "Institutional shareholders";

/** Rows shown while the section is collapsed, and when it is open (the
 *  backend's `TOP`). */
export const COLLAPSED_ROWS = 1;
export const OPEN_ROWS = 20;

/** Mirrors `form13f.BANNED_VOCABULARY` (parsed by the backend test). */
export const BANNED_VOCABULARY = ["owner", "owns", "owned", "beneficial", "stake", "control"] as const;

/** The section's aside, in the place Securities says "not in the graph". */
export const FORM13F_ASIDE = "Form 13F · not in the graph";

export const ADR_LABEL = "American depositary shares";

export const VALUE_NOT_SHOWN = "Not shown";

const NUMBER = new Intl.NumberFormat("en-GB");
const USD = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

export function sharesText(shares: number): string {
  return NUMBER.format(shares);
}

/** "$11.2B" — the reports print the exact figure; the page rounds it. */
export function valueText(p: Pick<Form13fPosition, "value" | "value_note">): string {
  if (p.value_note || p.value === null || p.value === undefined) return VALUE_NOT_SHOWN;
  return USD.format(p.value);
}

/** "14.24%", or null when the backend computed none. */
export function percentText(p: Pick<Form13fPosition, "percent">): string | null {
  return p.percent === null || p.percent === undefined ? null : `${p.percent.toFixed(2)}%`;
}

/** "MOODYS CORP · COM" — the issuer as the filers name it, and the class. */
export function securityLabel(s: Form13fSecurity): string {
  return [s.issuer_name || s.cusip, s.title_of_class].filter(Boolean).join(" · ");
}

/** "Positions as at 30 June 2026, from reports filed up to 31 Aug 2026." */
export function periodLine(payload: Pick<Form13fPayload, "period" | "filed_through">): string | null {
  if (!payload.period) return null;
  const filed = payload.filed_through ? `, from reports filed up to ${utcDate(payload.filed_through)}` : "";
  return `Positions as at ${utcDate(payload.period)}${filed}.`;
}

/** Rows hidden while collapsed — across every listed security. */
export function hiddenRows(securities: Form13fSecurity[]): number {
  return securities.reduce((n, s) => n + hiddenOf(s), 0);
}

function hiddenOf(s: Form13fSecurity): number {
  return Math.max(s.positions.length - COLLAPSED_ROWS, 0);
}

/** The toggle's words: how many more opening shows, or how to shut it.
 *  "Show the next 19" for one security; "… for each security" when every
 *  listed security hides the same number; otherwise "Show all N". */
export function toggleLabel(open: boolean, securities: Form13fSecurity[]): string {
  if (open) return "Show only the largest";
  const counts = securities.map(hiddenOf).filter((n) => n > 0);
  if (counts.length === 0) return "";
  const n = counts[0];
  const one = n === 1 ? "Show the next manager" : `Show the next ${n}`;
  if (counts.length === 1 && securities.length === 1) return one;
  if (counts.length === securities.length && counts.every((c) => c === n)) return `${one} for each security`;
  return `Show all ${NUMBER.format(hiddenRows(securities) + securities.length)} rows`;
}

/** The sentences under one security's rows, when the section is open —
 *  `security_footer` in the backend, in the page's words. */
export function securityFooter(s: Form13fSecurity): string[] {
  const out: string[] = [];
  const reported = s.managers_reported || 0;
  const shown = s.positions.length;
  out.push(
    reported > shown
      ? `${NUMBER.format(reported)} managers reported a position; the ${shown} largest by shares are shown.`
      : `${NUMBER.format(reported)} manager${reported === 1 ? "" : "s"} reported a position.`,
  );
  if (s.shares_outstanding?.basis) out.push(s.shares_outstanding.basis);
  if (s.adr_note) out.push(s.adr_note);
  if (s.left_out) {
    out.push(
      s.left_out === 1
        ? "1 reported position was left out because its figures cannot all be right (for example shares and value transposed)."
        : `${s.left_out} reported positions were left out because their figures cannot all be right (for example shares and value transposed).`,
    );
  }
  return out;
}

/** Whether any shown row has its value withheld (reported in thousands). */
export function thousandsNote(s: Form13fSecurity, rows: number): string | null {
  const p = s.positions.slice(0, rows).find((x) => x.value_note);
  return p?.value_note ?? null;
}

export interface Form13fView {
  /** "found" with securities, or "out_of_date" with only `line`. */
  status: Form13fPayload["status"];
  line: string;
  period: string | null;
  disclaimer: string;
  securities: Form13fSecurity[];
  notes: string[];
  notListed: string | null;
  attribution: string;
  caveat: string;
  sourceUrl: string;
}

/** What to render, or null for nothing (no payload, nothing listed). */
export function form13fView(payload: Form13fPayload | null | undefined): Form13fView | null {
  if (!payload) return null;
  const securities = (payload.securities ?? []).filter((s) => s.positions?.length);
  if (payload.status === "found" && securities.length === 0) return null;
  if (payload.status !== "found" && !payload.line) return null;
  return {
    status: payload.status,
    line: payload.line,
    period: periodLine(payload),
    disclaimer: payload.disclaimer,
    securities: payload.status === "found" ? securities : [],
    notes: payload.notes ?? [],
    notListed: payload.not_listed_note ?? null,
    attribution: payload.attribution,
    caveat: payload.caveat,
    sourceUrl: payload.source_url,
  };
}

/** Every sentence this module can put on the page for a payload, except the
 *  backend's disclaimer — for the vocabulary test. */
export function pageSentences(payload: Form13fPayload): string[] {
  const view = form13fView(payload);
  if (!view) return [];
  const out = [FORM13F_HEADING, FORM13F_ASIDE, ADR_LABEL, VALUE_NOT_SHOWN, view.line, view.period ?? ""];
  out.push(toggleLabel(false, view.securities), toggleLabel(true, view.securities));
  for (const s of view.securities) {
    out.push(securityLabel(s), ...securityFooter(s), thousandsNote(s, OPEN_ROWS) ?? "");
  }
  out.push(...view.notes, view.notListed ?? "", view.caveat, view.attribution);
  return out.filter(Boolean);
}
