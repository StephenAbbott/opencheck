/** Moody's as production served it on 14 Oct 2026, trimmed — shared by
 *  `lib/form13f.test.ts` and the section's markup test. */
import type { Form13fPayload, Form13fPosition, Form13fSecurity } from "../lib/api";

export const THOUSANDS =
  "Value reported in thousands of dollars, as before 2023 — the shares are shown and the value is not.";

export function pos(manager: string, shares: number, extra: Partial<Form13fPosition> = {}): Form13fPosition {
  return {
    manager,
    manager_cik: manager.length.toString() + manager,
    shares,
    value: shares * 452,
    value_note: null,
    value_check: "ok",
    percent: null,
    discretion: "SOLE",
    filed: "2026-08-14",
    amended: false,
    confidential_treatment: false,
    report_type: "13F HOLDINGS REPORT",
    included_managers: 0,
    ...extra,
  };
}

export function sec(extra: Partial<Form13fSecurity> = {}, n = 20): Form13fSecurity {
  return {
    cusip: "615369105",
    issuer_name: "MOODYS CORP",
    title_of_class: "COM",
    is_adr: false,
    adr_note: null,
    matched_via: ["gleif_isin"],
    managers_reported: 1401,
    managers_shown: n,
    managers_more: 1401 - n,
    left_out: 7,
    shares_outstanding: {
      shares: 173_200_000,
      as_of: "2026-06-30",
      accession: "0001628280-26-049398",
      basis: "Of 173.2 million shares outstanding on Moody’s Corporation's 10-Q/10-K cover, 30 June 2026 — computed by OpenCheck.",
    },
    positions: Array.from({ length: n }, (_, i) =>
      pos(i === 0 ? "Berkshire Hathaway Inc" : `Manager ${i}`, 24_669_778 - i * 1000, {
        percent: i === 0 ? 14.24 : 1,
      }),
    ),
    ...extra,
  };
}

// The payload as production served it for Moody's on 14 Oct 2026, trimmed.
export function payload(extra: Partial<Form13fPayload> = {}): Form13fPayload {
  return {
    status: "found",
    period: "2026-06-30",
    filed_through: "2026-08-31",
    built_at: "2026-10-14T13:05:44+00:00",
    attribution:
      "Contains data from the SEC's Form 13F data sets (U.S. Securities and Exchange Commission), public domain.",
    caveat: "Reported by the filers and not verified by the SEC; no substitute for the filings themselves.",
    disclaimer:
      "Form 13F lists the positions institutional investment managers have discretion over for funds and clients. It is not a record of beneficial ownership: it never enters the ownership graph and no risk signal reads it.",
    notes: [
      "Positions overlap — a manager's report can include another manager that also reported the same shares — so they are never added together.",
      "Long positions only, as at the quarter end and filed up to 45 days later. A manager may be allowed to leave a position out under confidential treatment, so a manager missing here says nothing.",
      "A manager's Form 13F figure and its Schedule 13D/13G figure can differ: they are dated differently and filed under different rules.",
    ],
    source_url: "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets",
    line: "1,401 institutional investment managers reported a position in MOODYS CORP (COM) on Form 13F, as at 30 June 2026; the largest is Berkshire Hathaway Inc with 24,669,778 shares, 14.24%.",
    securities: [sec()],
    ...extra,
  };
}

