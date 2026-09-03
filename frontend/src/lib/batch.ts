/**
 * Batch screening — the page's decisions, kept out of the JSX (Phase 166).
 *
 * The frontend suite is logic-only, so every claim the `/batch` page makes
 * lives here where it can be pinned: how a paste is read, which rows sort
 * first, what the coverage cell says, what the cost line promises, and
 * what the CSV contains. `BatchPage.tsx` renders these and nothing else.
 *
 * Three rules from the ticket:
 *
 * - **Tolerant paste.** Newlines, commas, semicolons, spaces or tabs — a
 *   column copied out of a spreadsheet just works. Each token is checked
 *   for shape and ISO 17442 check digits *before* anything is sent, so the
 *   reader sees "check digits do not match" beside the token rather than
 *   an unknown-LEI row two minutes later. The backend re-checks; this is
 *   the same rule run early, never a substitute.
 * - **Twenty rows.** A cold anchor costs 4–6 GLEIF calls; twenty is about
 *   two minutes at the shared throttle. Anything beyond the cap is counted
 *   and said, never silently dropped.
 * - **A row that could not be checked is a row.** Failed and degraded rows
 *   sort to the top, because a list whose clean rows are read first is a
 *   list whose gaps are read last. Within a state, rows sort by risk count
 *   and then name — a sort, not a grade: OpenCheck does not rank companies
 *   by severity (Phase 132).
 */

import type { BatchFailedRow, BatchRow } from "./api";
import { coverageCopy, type CoverageCopy } from "./lookupProgress";

export const BATCH_CAP = 20;

/** Pipelines the backend runs at once — stated on the page, so keep it true. */
export const BATCH_CONCURRENCY = 2;

const SPLIT = /[\s,;]+/;
const SHAPE = /^[0-9A-Z]{18}[0-9]{2}$/;

export interface RejectedToken {
  token: string;
  reason: string;
}

export interface ParsedPaste {
  leis: string[];
  rejected: RejectedToken[];
  /** Valid, unique LEIs beyond the cap that were not taken. */
  overflow: number;
}

/**
 * ISO 17442 check digits: the LEI with letters as 10–35, digits as
 * themselves, mod 97 must be 1. The backend's `lei_check_digits_ok`,
 * in TypeScript, so a typo is caught before a request is spent on it.
 */
export function leiCheckDigitsOk(lei: string): boolean {
  if (!SHAPE.test(lei)) return false;
  let remainder = 0;
  for (const ch of lei) {
    const value = ch >= "A" && ch <= "Z" ? ch.charCodeAt(0) - 55 : Number(ch);
    // Feed the value's decimal digits one at a time — letters contribute two.
    for (const d of String(value)) remainder = (remainder * 10 + Number(d)) % 97;
  }
  return remainder === 1;
}

export function parseLeiPaste(text: string, cap: number = BATCH_CAP): ParsedPaste {
  const out: ParsedPaste = { leis: [], rejected: [], overflow: 0 };
  const seen = new Set<string>();
  for (const raw of (text || "").split(SPLIT)) {
    if (!raw) continue;
    const lei = raw.trim().toUpperCase();
    if (lei.length !== 20) {
      out.rejected.push({ token: raw, reason: `${lei.length} characters — an LEI has 20` });
      continue;
    }
    if (!SHAPE.test(lei)) {
      out.rejected.push({ token: raw, reason: "not an LEI: 18 letters or digits then two digits" });
      continue;
    }
    if (!leiCheckDigitsOk(lei)) {
      out.rejected.push({ token: raw, reason: "check digits do not match — a typo?" });
      continue;
    }
    if (seen.has(lei)) {
      out.rejected.push({ token: raw, reason: "duplicate" });
      continue;
    }
    seen.add(lei);
    if (out.leis.length >= cap) {
      out.overflow += 1;
      continue;
    }
    out.leis.push(lei);
  }
  return out;
}

/** "3 valid · 1 rejected · 2 beyond the cap of 20" — the live count under the box. */
export function pasteSummary(p: ParsedPaste, cap: number = BATCH_CAP): string {
  const parts = [`${p.leis.length} valid`];
  if (p.rejected.length) parts.push(`${p.rejected.length} rejected`);
  if (p.overflow) parts.push(`${p.overflow} beyond the cap of ${cap}`);
  return parts.join(" · ");
}

/**
 * What the run will cost, said before the button is pressed. Two pipelines
 * at a time against a shared upstream budget; a company seen in the last
 * fifteen minutes is free, so this is the ceiling, not a promise.
 */
export function costLine(n: number, concurrency: number = BATCH_CONCURRENCY): string {
  if (n <= 0) return "";
  const company = n === 1 ? "company" : "companies";
  if (n === 1) return "A few seconds for a company not seen recently.";
  // ~6 s per cold anchor per pipeline (4–6 GLEIF calls at 50/min, shared).
  const seconds = Math.ceil(n / concurrency) * 6;
  const time =
    seconds < 45
      ? "under a minute"
      : seconds < 90
        ? "about a minute"
        : `about ${Math.round(seconds / 60)} minutes`;
  return `${capitalise(time)} for ${n} ${company} not seen recently — OpenCheck runs ${concurrency} at a time and queues behind GLEIF's rate limit rather than tripping it.`;
}

function capitalise(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/** The three states a row can be in on the page. */
export type RowState = "running" | "done" | "degraded";

export type TableRow =
  | { state: "running"; lei: string }
  | { state: "done" | "degraded"; lei: string; row: BatchRow }
  | { state: "degraded"; lei: string; failed: BatchFailedRow };

export function rowState(row: BatchRow): "done" | "degraded" {
  return row.degraded ? "degraded" : "done";
}

/**
 * Companies that could not be checked at all first, then rows whose check
 * did not fully run, then done rows by risk count descending and name.
 * Running rows keep paste order at the end so the table does not reshuffle
 * under the reader while the stream is open.
 */
export function sortRows(rows: TableRow[]): TableRow[] {
  const rank = (r: TableRow): number =>
    "failed" in r ? 0 : r.state === "degraded" ? 1 : r.state === "done" ? 2 : 3;
  const risk = (r: TableRow): number => ("row" in r ? r.row.risk_count : -1);
  const name = (r: TableRow): string => ("row" in r ? (r.row.legal_name ?? "") : "");
  return rows
    .map((r, i) => ({ r, i }))
    .sort((a, b) => {
      const d = rank(a.r) - rank(b.r);
      if (d) return d;
      const rd = risk(b.r) - risk(a.r);
      if (rd) return rd;
      const nd = name(a.r).localeCompare(name(b.r));
      if (nd) return nd;
      return a.i - b.i;
    })
    .map(({ r }) => r);
}

/**
 * The coverage cell, from the same `coverageCopy` the single report uses
 * (Phase 156). The row counts the GLEIF anchor in both figures already, so
 * it is taken back out before the helper adds it.
 */
export function rowCoverage(row: BatchRow, registryTotal: number | null): CoverageCopy {
  return coverageCopy({
    answered: Math.max(0, row.coverage.answered - 1),
    applicable: Math.max(0, row.coverage.applicable - 1),
    total: registryTotal,
    jurisdiction: row.jurisdiction,
    screening: false,
    anchorAnswered: true,
  });
}

const CSV_HEADER = [
  "lei",
  "legal_name",
  "jurisdiction",
  "register_status",
  "verdict",
  "risk_count",
  "risk_codes",
  "context_count",
  "context_codes",
  "sources_applicable",
  "sources_answered",
  "degraded",
  "degraded_sources",
  "state",
  "reason",
  "report_url",
];

function csvCell(v: unknown): string {
  const s = v === null || v === undefined ? "" : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/**
 * The table as CSV — every screened LEI, one line each, failed rows
 * included with their reason so the file never reads as a clean list of
 * the ones that worked. `report_url` is absolute when `origin` is given.
 */
export function rowsToCsv(rows: TableRow[], origin = ""): string {
  const lines = [CSV_HEADER.join(",")];
  for (const r of rows) {
    if (r.state === "running") continue;
    if ("failed" in r) {
      lines.push(
        [
          r.lei, "", "", "", "", "", "", "", "", "", "", "true", "", "not checked",
          r.failed.reason, `${origin}/?lei=${r.lei}`,
        ].map(csvCell).join(","),
      );
      continue;
    }
    const row = r.row;
    lines.push(
      [
        row.lei,
        row.legal_name ?? "",
        row.jurisdiction ?? "",
        row.register_status?.liveness ?? "",
        row.verdict ?? "",
        row.risk_count,
        row.risk_codes.join(" "),
        row.context_count,
        row.context_codes.join(" "),
        row.coverage.applicable,
        row.coverage.answered,
        row.degraded ? "true" : "false",
        row.degraded_sources.join(" "),
        row.degraded ? "degraded" : "done",
        "",
        `${origin}${row.report_url}`,
      ].map(csvCell).join(","),
    );
  }
  return lines.join("\r\n") + "\r\n";
}

/** `opencheck-batch-2026-09-03.csv` */
export function csvFilename(now: Date = new Date()): string {
  return `opencheck-batch-${now.toISOString().slice(0, 10)}.csv`;
}
