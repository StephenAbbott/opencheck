/**
 * savedReport — every decision the saved-report page makes, as values
 * (Phase 217).
 *
 * A saved report (`/report/{id}`, Phase 216 backend) is the record of exactly
 * what OpenCheck showed for one company on one date. The page renders it by
 * replaying the stored lookup events through the same handlers a live check
 * drives (`replayLookupEvents` in `lib/api.ts`), so almost nothing about the
 * report itself is new. What is new is everything around it, and all of it
 * lives here so the logic-only suite can pin it:
 *
 * - **the fifth clock.** Phases 99/100 gave a report four dates (when a fact
 *   was true, when a source declared it, when OpenCheck retrieved it, when
 *   OpenCheck published it). A saved report adds when a reader chose to keep
 *   it. The banner names it, beside when the check finished, and says in the
 *   same breath that nothing on the page was re-checked — a saved page must
 *   never be mistaken for a live one;
 * - **what a saved report holds.** QuickCheck, FullCheck, the summary and its
 *   sign-off. The other tabs and the live-fetching sections are not in it
 *   (Stephen, 16 Sept 2026), and say so rather than silently showing today;
 * - **when the Save action is honest.** The server copies its own held run and
 *   refuses one it no longer holds; the menu says why before the reader tries;
 * - **the manage token**, kept in this browser like the watchlist's.
 */

import type {
  DeepenResponse,
  LookupStreamDoneEvent,
  RiskSignal,
  SavedReportEvent,
  SavedReportMeta,
} from "./api";
import type { CheckMode } from "./checkMode";

// ---------------------------------------------------------------------------
// Addressing
// ---------------------------------------------------------------------------

/** `secrets.token_urlsafe(16)` → 22 characters of the URL-safe alphabet. */
const REPORT_PATH_RE = /^\/report\/([A-Za-z0-9_-]{22})\/?$/;

/** The report id a path addresses, or null when it is not a saved report. */
export function reportIdFromPath(pathname: string): string | null {
  const m = REPORT_PATH_RE.exec(pathname);
  return m ? m[1] : null;
}

export function reportPath(reportId: string): string {
  return `/report/${reportId}`;
}

/** The link a reader shares: this site's own `/report/{id}` page. */
export function savedReportLink(reportId: string, origin: string): string {
  return `${origin.replace(/\/$/, "")}${reportPath(reportId)}`;
}

// ---------------------------------------------------------------------------
// Dates — "16 Sept 2026, 15:18 UTC"
// ---------------------------------------------------------------------------

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"];

function parse(iso: string): Date | null {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? null : d;
}

/** "16 Sept 2026". Always UTC: a saved record is read across time zones,
 *  and a date that shifts with the reader's clock is not one date. */
export function utcDate(iso: string): string {
  const d = parse(iso);
  if (!d) return iso;
  return `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
}

/** "15:18 UTC". */
export function utcTime(iso: string): string {
  const d = parse(iso);
  if (!d) return iso;
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  return `${hh}:${mm} UTC`;
}

export function utcDateTime(iso: string): string {
  return `${utcDate(iso)}, ${utcTime(iso)}`;
}

// ---------------------------------------------------------------------------
// The banner
// ---------------------------------------------------------------------------

export interface BannerText {
  heading: string;
  /** When it was kept, and when the check it keeps finished. */
  clocks: string;
  /** What that means for every date on the page below. */
  notLive: string;
  kept: string;
}

/**
 * The banner's sentences. The check's completion time gets its date too when
 * it fell on a different day from the save — which can happen, because a run
 * is held for fifteen minutes and a save a minute after midnight UTC keeps a
 * run from the day before.
 */
export function bannerText(meta: Pick<SavedReportMeta, "saved_at" | "expires_at">, runCompletedAt: string): BannerText {
  const sameDay = utcDate(meta.saved_at) === utcDate(runCompletedAt);
  const ran = sameDay ? utcTime(runCompletedAt) : utcDateTime(runCompletedAt);
  return {
    heading: "Saved report",
    clocks: `Saved ${utcDateTime(meta.saved_at)}, from a check that finished at ${ran}.`,
    notLive:
      "Nothing on this page has been re-checked since it was saved.",
    kept: `Kept until ${utcDate(meta.expires_at)}.`,
  };
}

/** The first 12 hex characters, for display beside the full hash. */
export function shortHash(hash: string): string {
  return hash.slice(0, 12);
}

// ---------------------------------------------------------------------------
// What a saved report holds (Stephen, 16 Sept 2026)
// ---------------------------------------------------------------------------

/** The check modes a saved report carries. Both fold over the lookup events. */
export const SAVED_MODES: ReadonlySet<CheckMode> = new Set<CheckMode>(["quick", "full"]);

export function modeInSavedReport(mode: CheckMode): boolean {
  return SAVED_MODES.has(mode);
}

/** The sentence in place of a tab a saved report does not hold. */
export function notInSavedReport(label: string): string {
  return `${label} is not part of a saved report. It looks records up when you open it, so what it showed on the day was not kept. Run a live check to see it as it stands today.`;
}

/** What the banner's disclosure says a saved report is. */
export const SCOPE_SENTENCE =
  "A saved report keeps QuickCheck, FullCheck, the summary and any sign-off exactly as they were. Background check, Subsidiaries, History, Climate & ESG, securities and New Zealand associations look records up when they are opened, so they are not kept.";

/** How to check the page is the record that was saved. */
export const VERIFY_SENTENCE =
  "The SHA-256 of the saved JSON is printed below. Download it and run shasum -a 256 on the file: the two match only if not a byte has changed.";

// ---------------------------------------------------------------------------
// Saving a live check
// ---------------------------------------------------------------------------

/** How long the server holds a finished run (routers/lookup.py `_REPLAY_TTL_SECONDS`). */
export const HOLD_WINDOW_MS = 15 * 60 * 1000;

export interface SaveEligibility {
  canSave: boolean;
  /** Why not, in the reader's terms. Null when saving is possible. */
  reason: string | null;
}

/**
 * Whether the Save action can honestly be offered for the check on screen.
 * The server is the authority (it refuses a run it does not hold, 409); this
 * says why *before* the reader presses, for the cases the page can see.
 */
export function saveEligibility(input: {
  streaming: boolean;
  runCompletedAt: string | null;
  retried: boolean;
  now?: Date;
}): SaveEligibility {
  if (input.streaming) {
    return { canSave: false, reason: "Available when the check finishes." };
  }
  if (input.retried) {
    return {
      canSave: false,
      reason: "A source was retried, so this page no longer matches one check. Run the check again to save it.",
    };
  }
  if (!input.runCompletedAt) {
    return { canSave: false, reason: "Run the check again to save it." };
  }
  const done = parse(input.runCompletedAt);
  const now = (input.now ?? new Date()).getTime();
  if (!done || now - done.getTime() >= HOLD_WINDOW_MS) {
    return {
      canSave: false,
      reason: "A check can be saved for 15 minutes after it finishes. Run it again to save it.",
    };
  }
  return { canSave: true, reason: null };
}

/** The run's name, off the `done` event. Null for runs cached before it existed. */
export function runCompletedAtFrom(done: LookupStreamDoneEvent): string | null {
  return typeof done.run_completed_at === "string" && done.run_completed_at ? done.run_completed_at : null;
}

/** The confirmation under the subject card after a save. */
export function savedConfirmation(meta: Pick<SavedReportMeta, "expires_at">, withSummary: boolean, copied: boolean): string {
  const parts = [
    withSummary ? "Saved with its summary." : "Saved.",
    copied ? "The link is copied." : null,
    `Kept until ${utcDate(meta.expires_at)}.`,
  ];
  return parts.filter(Boolean).join(" ");
}

/** A narrative the server did not write from this run is not saved — the
 *  report is saved without it, and the reader is told. */
export const SAVED_WITHOUT_SUMMARY =
  "Saved without the summary: it was not written from this check. The findings are saved as shown.";

// ---------------------------------------------------------------------------
// Refusals, in the reader's terms
// ---------------------------------------------------------------------------

export function openErrorMessage(status: number, detail: string): string {
  if (status === 404) return "There is no saved report at this address. It may have been deleted, or the link may be incomplete.";
  if (status === 410) return detail || "This saved report has expired and has been deleted.";
  if (status === 503) return "Saved reports are not available on this instance.";
  return detail;
}

// ---------------------------------------------------------------------------
// Source drawers and the network, from the saved events
// ---------------------------------------------------------------------------

interface DeepenResultData {
  source_id: string;
  hit_id: string;
  bods: Record<string, unknown>[];
  bods_issues?: string[];
  risk_signals?: RiskSignal[];
  license?: string;
  license_notice?: string | null;
}

function deepenResults(events: SavedReportEvent[]): DeepenResultData[] {
  return events
    .filter((e) => e.event === "deepen_result" && e.data && typeof e.data === "object")
    .map((e) => e.data as DeepenResultData);
}

/**
 * What a source drawer shows in a saved report: the mapping that was made for
 * that result when the check ran, never a `/deepen` call for today's record.
 * Null when the source was not deepened in that check. The raw source record
 * is not saved, so `raw` is empty — the drawer's JSON view shows the BODS.
 */
export function savedDeepen(events: SavedReportEvent[], sourceId: string, hitId: string): DeepenResponse | null {
  const d = deepenResults(events).find((r) => r.source_id === sourceId && r.hit_id === hitId);
  if (!d) return null;
  return {
    source_id: d.source_id,
    hit_id: d.hit_id,
    raw: {},
    bods: d.bods ?? [],
    bods_issues: d.bods_issues ?? [],
    license: d.license ?? "",
    license_notice: d.license_notice ?? null,
    risk_signals: d.risk_signals ?? [],
  };
}

// ---------------------------------------------------------------------------
// Downloads (Phase 218)
// ---------------------------------------------------------------------------

/** Under the format picker on a saved report. */
export const SAVED_DOWNLOADS =
  "Every download here is built from the saved report — the same records, not a new check — and carries the licence position assessed when it was saved.";

/** Under the format picker on a live check. It used to call the export
 *  "reproducible", which a download that re-runs the check is not. */
export const LIVE_DOWNLOADS =
  "A download runs the check as it stands now, so one made later can differ. Save the report to keep a copy that does not change.";

/** Said in a source drawer for a result that was not mapped in the saved check. */
export const NOT_DEEPENED_IN_SAVED =
  "This source's records were not mapped when the check was saved, so there is nothing to show for it here.";

/** Said where a live drawer shows the source's raw response. */
export const RAW_NOT_SAVED =
  "The source's original response is not kept in a saved report — only the records mapped from it, above.";

/**
 * The BODS statements FullCheck draws: every `deepen_result`'s statements, in
 * order — the same concatenation `fold_lookup_events` gives `LookupResponse.bods`,
 * which is what a live FullCheck fetches.
 */
export function savedStatements(events: SavedReportEvent[]): Record<string, unknown>[] {
  return deepenResults(events).flatMap((d) => d.bods ?? []);
}

// ---------------------------------------------------------------------------
// The manage token — this browser only
// ---------------------------------------------------------------------------

export const MANAGE_KEY = "opencheck.savedReports.manage";

function readTokens(): Record<string, string> {
  try {
    const raw = window.localStorage.getItem(MANAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" ? (parsed as Record<string, string>) : {};
  } catch {
    return {};
  }
}

export function rememberManageToken(reportId: string, token: string): void {
  try {
    const all = readTokens();
    all[reportId] = token;
    window.localStorage.setItem(MANAGE_KEY, JSON.stringify(all));
  } catch {
    /* storage absent or blocked: the report is saved; only extending needs it */
  }
}

export function manageTokenFor(reportId: string): string | null {
  const t = readTokens()[reportId];
  return typeof t === "string" && t ? t : null;
}
