/**
 * The watchlist's values layer (Phase 215) — what the page and the Watch
 * button *say*, kept in `lib/` so the logic-only suite can pin it.
 *
 * The backend's diff vocabulary (`opencheck/watchlist.py::CHANGE_KINDS`) is
 * a closed list; `describeChange` words every kind and a test fails if one
 * is added there without words here. Two rules the wording keeps:
 *
 * - **Absence is a finding only when the source answered.** A
 *   `signal_unchecked` / `coverage_unchecked` change is worded as "could not
 *   re-check", never as the signal going away (the Phase 146 rule).
 * - **Say what fired, and when each source was actually reached.** An entry
 *   opens with the tier that triggered it (GLEIF's delta, OpenSanctions'
 *   delta, or a hand re-check) and closes with how many sources were
 *   checked as a result — the four-clock rule from Phases 99/100.
 *
 * The token is a capability, not an identity: it lives in this browser's
 * localStorage under `TOKEN_KEY` and nowhere else. Every read and write is
 * wrapped, because storage can be absent or throw.
 */

export const TOKEN_KEY = "opencheck.watchlist.token";

export const WATCH_LABEL = "Watch for changes";
export const WATCHING_LABEL = "On your watchlist";

/** Said once when a held token turned out to name no list on this
 *  instance and a fresh list was started in its place. */
export const UNKNOWN_LIST_NOTICE =
  "Your previous watchlist is no longer available on this instance — started a new one.";

/** The same fact on /watchlist, where there is no list to start yet. */
export const UNKNOWN_LIST_PAGE_NOTICE =
  "Your previous watchlist is no longer available on this instance; it has been forgotten. Watching a company will start a new one.";

export interface WatchBaseline {
  register_status: { liveness?: string; since?: string; raw?: string; source_id?: string } | null;
  risk_codes: string[];
  context_codes: string[];
  coverage: { applicable: number; answered: number } | null;
  verdict: string | null;
  checked: CheckedSource[];
  degraded_sources: string[];
}

export interface CheckedSource {
  source_id: string;
  liveness: string | null;
  retrieved_at: string | null;
}

/** Phase 303 — GLEIF's own field-modification log, as stored on an entry
 *  (since that list's baseline) or on a watch (the 30 days before it). */
export interface GleifLogItem {
  date: string | null;
  record: string;
  label: string;
  type: string;
  old: string | null;
  new: string | null;
  relationship?: { type?: string | null; end_node?: string | null };
}
export interface GleifLog {
  available: boolean;
  reason?: string;
  since: string | null;
  items: GleifLogItem[];
  more: number;
}

const LOG_LIMIT = 10;

function logValue(v: unknown): string {
  return v === null || v === undefined || v === "" ? "—" : String(v);
}

/** One line of GLEIF's log — the same words as `log_line_words` on the feed. */
export function logLineWords(i: GleifLogItem): string {
  const kind = String(i.type || "UPDATE").toUpperCase();
  const what = i.label || "a field";
  let body: string;
  if (kind === "INITIAL" || kind === "INSERT") body = `${what} set to ${logValue(i.new)}`;
  else if (kind === "DELETE") body = `${what} removed (was ${logValue(i.old)})`;
  else body = `${what} ${logValue(i.old)} → ${logValue(i.new)}`;
  return `${i.date || "undated"}: ${body}`;
}

/** The feed's `gleif_log_lines`, split for the page: a lead sentence and
 *  the lines under it. `null` when no log was fetched. */
export function gleifLogLines(
  log: GleifLog | null | undefined,
  heading = "GLEIF's own modification log",
): { lead: string; items: string[] } | null {
  if (!log) return null;
  const since = String(log.since ?? "").slice(0, 10);
  const span = since ? ` since ${since}` : "";
  if (!log.available) return { lead: `${heading} could not be read${span}: ${log.reason || "unavailable"}.`, items: [] };
  const items = log.items ?? [];
  if (!items.length) return { lead: `${heading} has no change to this record${span} other than renewal dates.`, items: [] };
  const shown = items.slice(0, LOG_LIMIT).map(logLineWords);
  const rest = items.length - shown.length + (log.more || 0);
  if (rest > 0) shown.push(`and ${rest} more.`);
  return { lead: `${heading}${span}:`, items: shown };
}

export interface Watch {
  lei: string;
  added_at: string;
  legal_name: string | null;
  jurisdiction: string | null;
  gleif_facts: Record<string, string | null> | null;
  gleif_watermark: string | null;
  snapshot_at: string | null;
  last_checked_at: string | null;
  baseline: WatchBaseline;
  /** Phase 303: GLEIF's log for the 30 days before this list started watching. */
  gleif_history?: GleifLog | null;
  /** pKYC stage B: what the watcher expected. Absent on an older server. */
  context?: WatchContext;
  /** pKYC stage C: when OpenCheck last fully looked, and how fresh that was. */
  review?: WatchReview;
  /** pKYC stage D: every decision recorded on this company's entries, oldest first. */
  review_history?: WatchReviewHistoryItem[];
}

// ---------------------------------------------------------------------------
// pKYC stage C — when OpenCheck last looked
// ---------------------------------------------------------------------------

/** Computed by the backend on read (`opencheck/watchlist_review.py`), never
 *  stored. The page renders the sentence it carries. */
export interface WatchReview {
  last_run_at: string | null;
  last_run_days: number | null;
  never_rerun: boolean;
  due: boolean;
  due_days: number;
  sources_total: number;
  sources_from_older_copy: { source_id: string; retrieved_at: string | null }[];
  sources_not_reached: string[];
  sentence: string;
}

/** The list-level review block on the payload. */
export interface ListReview {
  due: number;
  total: number;
  due_days: number;
  sentence: string | null;
}

/** A copy of the backend's `REVIEW_WORDS`, pinned by
 *  `tests/test_watchlist_review.py`. The sentences themselves come from the
 *  backend; the page uses `due_chip` and reads the rest for parity. */
export const REVIEW_WORDS = {
  last_run: "Last fully re-run {n} days ago",
  last_run_today: "Last fully re-run today",
  never_rerun: "Baseline taken {n} days ago and never re-run — no delta has named this company since",
  all_live: "every source answered live",
  older_copy: "{n} of {total} sources answered from an older copy ({names})",
  not_reached: "{names} not reached",
  action_refresh: "Re-check by hand to refresh.",
  action_confirm: "Re-check by hand to confirm.",
  due_chip: "Review due",
  list_due: "{n} of {total} companies have not been fully re-run in {days} days.",
};

/** The chip beside a due watch: `warn` — look again — never `risk`; a
 *  review falling due is not a finding against the company. `null` when the
 *  watch is not due or the server is older. */
export function reviewChip(review: WatchReview | undefined): { label: string; tone: "warn" } | null {
  if (!review?.due) return null;
  return { label: REVIEW_WORDS.due_chip, tone: "warn" };
}

// ---------------------------------------------------------------------------
// pKYC stage B — what the watcher expected
// ---------------------------------------------------------------------------

/** The four fields a watcher may set on a watch, as the backend stores them
 *  (`opencheck/watchlist_context.py`). The backend validates and compares;
 *  the page renders what the watch and the entry carry. */
export interface WatchContextFields {
  expected_jurisdiction: string | null;
  expected_parent_lei: string | null;
  rationale: string | null;
  onboarding_report_id: string | null;
}

export interface WatchContext extends WatchContextFields {
  set_at: string | null;
  /** Resolved from the GLEIF mirror on read; never stored. */
  expected_parent_name?: string | null;
  /** What the baseline says that the watcher did not expect — computed on read. */
  baseline_disagrees?: Disagreement[];
  onboarding_report?: {
    report_id: string;
    available: boolean;
    saved_at?: string | null;
    expires_at?: string | null;
    report_path?: string;
  } | null;
}

export interface Disagreement {
  field: "jurisdiction" | "ultimate_parent_lei";
  expected: string;
  actual: string;
}

/** The comparison a change carries, made at write time and kept. */
export interface Expectation {
  field: "jurisdiction" | "ultimate_parent_lei" | "direct_parent_lei";
  expected: string;
  met: boolean;
}

/** The body a `PATCH /watch/{token}/items/{lei}` sends: a key present with
 *  `null` clears the field, a key left out leaves it alone. */
export type WatchContextPatch = Partial<WatchContextFields>;

/** One line of free text; the backend's cap (`RATIONALE_MAX`). */
export const RATIONALE_MAX = 280;

/** The clause after a change's sentence — a copy of the backend's
 *  `EXPECTATION_WORDS`, pinned by `tests/test_watchlist_context.py`.
 *  `{expected}` is the value the watcher wrote (a code or an LEI). */
export const EXPECTATION_WORDS = {
  contradicted: "your watch expected {expected}",
  met: "as your watch expected",
};

/** The sentence beside a change, and the tone it wears: a contradiction is
 *  `warn` (look again — never `risk`, the tier already says how much it
 *  matters), a met expectation is `ok`. `null` when the change carries no
 *  expectation. */
export function expectationSentence(c: Change): { text: string; tone: "warn" | "ok" } | null {
  const e = c.expectation;
  if (!e) return null;
  if (e.met) return { text: EXPECTATION_WORDS.met[0].toUpperCase() + EXPECTATION_WORDS.met.slice(1) + ".", tone: "ok" };
  const text = EXPECTATION_WORDS.contradicted.replace("{expected}", e.expected);
  return { text: text[0].toUpperCase() + text.slice(1) + ".", tone: "warn" };
}

/** True when any change on the entry contradicted an expectation. */
export function entryContradicted(e: WatchEntry): boolean {
  return e.changes.some((c) => c.expectation?.met === false);
}

export const CONTEXT_FIELD_LABELS: Record<keyof WatchContextFields, string> = {
  expected_jurisdiction: "Expected jurisdiction",
  expected_parent_lei: "Expected ultimate parent (LEI)",
  rationale: "Why this company is watched",
  onboarding_report_id: "Onboarding record (saved report)",
};

/** The form's standing note — the no-personal-data rule, said where the
 *  text is typed. */
export const CONTEXT_FORM_NOTE =
  "About the company, not a person: one line, no e-mail addresses, no links other than an OpenCheck saved report, no personal codes.";

/** The watch row's line when the baseline already disagrees with what the
 *  watcher expects — said at once, written to no entry. */
export function disagreementSentence(d: Disagreement, parentName?: string | null): string {
  if (d.field === "jurisdiction") {
    return `Baseline jurisdiction is ${d.actual}; your watch expected ${d.expected}.`;
  }
  const actual = parentName ? `${d.actual} (${parentName})` : d.actual;
  return `Baseline ultimate parent is ${actual}; your watch expected ${d.expected}.`;
}

/** Whether a watch carries any context at all. */
export function hasContext(ctx: WatchContext | undefined): boolean {
  if (!ctx) return false;
  return Boolean(ctx.expected_jurisdiction || ctx.expected_parent_lei || ctx.rationale || ctx.onboarding_report_id);
}

/** The id of a saved report from a pasted link or a bare id, or null. */
export function reportIdFromInput(value: string): string | null {
  const v = value.trim();
  if (!v) return null;
  const m = v.match(/(?:\/report\/|\/share\/saved\/)([A-Za-z0-9_-]{22})\/?$/) ?? v.match(/^([A-Za-z0-9_-]{22})$/);
  return m ? m[1] : null;
}

/** Phase 260: ``catch_up`` = every watched company re-run because
 * OpenSanctions versions could not be read (aged out of its version list,
 * or a listed delta file gone). */
export type Tier = "gleif" | "opensanctions" | "manual" | "catch_up" | "register" | "review";

/** pKYC stage A — does the change matter? Graded by the backend
 *  (`opencheck/watchlist_materiality.py`) once, at write time, and carried on
 *  the entry and on each change. The page renders what the entry carries and
 *  never re-grades; the three labels and sentences below are copies of the
 *  backend's, pinned by `tests/test_watchlist_materiality.py`. */
export type Materiality = "note" | "recheck" | "escalate";

export const MATERIALITY_TIERS: readonly Materiality[] = ["note", "recheck", "escalate"];

export const MATERIALITY_LABELS: Record<Materiality, string> = {
  note: "Note only",
  recheck: "Recheck the source",
  escalate: "Escalate",
};

export const MATERIALITY_SENTENCES: Record<Materiality, string> = {
  note: "Recorded for the history; nothing to act on by itself.",
  recheck: "Something about the company's record or exposure moved; open the report and look.",
  escalate: "A finding against the subject, or the subject ceased or changed hands; route to whoever decides.",
};

/** The chip beside an entry's headline. The tone is what the tier *asserts*:
 *  a finding against the subject is `risk` (the only red-family tier), a
 *  record that moved is `warn` (incomplete, look again), a note is `neutral`.
 *  Never `risk` for note, never `neutral` for escalate. */
export function materialityChip(tier: Materiality | null | undefined): {
  label: string;
  tone: "risk" | "warn" | "neutral";
} | null {
  if (tier === "escalate") return { label: MATERIALITY_LABELS.escalate, tone: "risk" };
  if (tier === "recheck") return { label: MATERIALITY_LABELS.recheck, tone: "warn" };
  if (tier === "note") return { label: MATERIALITY_LABELS.note, tone: "neutral" };
  return null;
}

/** "Material only" = recheck and escalate. */
export function isMaterial(tier: Materiality | null | undefined): boolean {
  return tier === "recheck" || tier === "escalate";
}

/** The filter the page offers over the change log. `all` is the default —
 *  a stale list must stay visible; the filter is a per-viewer convenience
 *  carried in `?show=material` / `?show=needs_review`. */
export type ChangeLogFilter = "all" | "material" | "needs_review";

export const FILTER_LABELS: Record<ChangeLogFilter, string> = {
  all: "All",
  material: "Material only",
  needs_review: "Needs review",
};

export function filterEntries(entries: WatchEntry[], filter: ChangeLogFilter): WatchEntry[] {
  if (filter === "material") return entries.filter((e) => isMaterial(e.materiality));
  if (filter === "needs_review") return entries.filter(isUndecidedMaterial);
  return entries;
}

/** The sentence under the filter when it hides something. */
export function hiddenByFilterSentence(all: number, shown: number, filter: ChangeLogFilter = "material"): string | null {
  const hidden = all - shown;
  if (hidden <= 0) return null;
  if (filter === "needs_review") {
    return `${hidden} ${hidden === 1 ? "entry is" : "entries are"} hidden: note-only, or already decided on.`;
  }
  return `${hidden} note-only ${hidden === 1 ? "entry is" : "entries are"} hidden; nothing shown here is recorded for the history alone.`;
}

// ---------------------------------------------------------------------------
// pKYC stage D — the review record: change → review → decision
// ---------------------------------------------------------------------------

/** The three decisions a reader can record on an entry. A copy of the
 *  backend's `STATUSES` / `STATUS_LABELS` / `STATUS_MEANINGS` / `EVIDENCE_WORDS`
 *  (`opencheck/watchlist_decisions.py`), pinned by
 *  `tests/test_watchlist_decisions.py`. The status is the reader's statement,
 *  never set by what else the request did. */
export type DecisionStatus = "reviewed_no_action" | "rechecked" | "escalated";

export const DECISION_STATUSES: readonly DecisionStatus[] = ["reviewed_no_action", "rechecked", "escalated"];

export const DECISION_LABELS: Record<DecisionStatus, string> = {
  reviewed_no_action: "Reviewed, no action",
  rechecked: "Re-checked",
  escalated: "Escalated",
};

export const DECISION_MEANINGS: Record<DecisionStatus, string> = {
  reviewed_no_action: "Looked at; nothing to do.",
  rechecked: "Looked at and the company was re-run.",
  escalated: "Routed to whoever decides; what they decided is not recorded here.",
};

export type EvidenceKind = "entry_run" | "recheck";

export const EVIDENCE_WORDS = {
  entry_run: "the re-run that produced this entry",
  recheck: "a fresh re-check made when the decision was recorded",
  none: "no evidence pinned",
  expired: "the pinned saved report has since expired",
  run_not_held: "That re-run is no longer held (a finished run is held for 15 minutes); re-check and keep instead.",
};

/** The note follows stage B's rationale rule: the same cap, the same words. */
export const DECISION_NOTE_MAX = 280;

export interface DecisionReport {
  report_id: string;
  report_path: string;
  available: boolean;
  saved_at?: string | null;
  expires_at?: string | null;
  content_hash?: string | null;
}

export interface WatchDecision {
  id: number;
  entry_id: number;
  lei: string | null;
  status: DecisionStatus;
  note: string | null;
  saved_report_id: string | null;
  evidence_kind: EvidenceKind | null;
  decided_at: string;
  report: DecisionReport | null;
}

/** The list-level block on the payload. */
export interface ListDecisions {
  undecided_material: number;
  total: number;
  /** Whether this server can keep a saved report as evidence at all. */
  evidence_available: boolean;
}

/** The chip on a decided entry: `ok` for reviewed/no action, `context` for
 *  re-checked, `warn` for escalated — never `risk`: a decision is the reader's
 *  act, not a finding against the company. */
export function decisionChip(d: WatchDecision | null | undefined): {
  label: string;
  tone: "ok" | "context" | "warn";
} | null {
  if (!d) return null;
  const tone = d.status === "escalated" ? "warn" : d.status === "rechecked" ? "context" : "ok";
  return { label: DECISION_LABELS[d.status] ?? d.status, tone };
}

/** The one line under a decided entry. */
export function decisionSentence(d: WatchDecision): string {
  const label = (DECISION_LABELS[d.status] ?? d.status).toLowerCase();
  let out = `Decision: ${label} — ${d.decided_at.slice(0, 10)}`;
  if (d.note) out += `; note: ${d.note}`;
  if (d.saved_report_id) {
    const kind = d.evidence_kind ? EVIDENCE_WORDS[d.evidence_kind] : "";
    out += `; evidence: saved report ${d.saved_report_id}${kind ? ` (${kind})` : ""}`;
    if (d.report && d.report.available === false) out += ` — ${EVIDENCE_WORDS.expired}`;
  }
  return out + ".";
}

/** Material and nobody has decided on it — the "Needs review" filter. */
export function isUndecidedMaterial(e: WatchEntry): boolean {
  return isMaterial(e.materiality) && !e.decision;
}

/** The label of the "keep the evidence" option: the entry's own run while
 *  the server still holds it, else a fresh re-check. */
export function evidenceOption(e: WatchEntry): { kind: EvidenceKind; label: string } {
  if (e.run_held) return { kind: "entry_run", label: "… and keep this re-run as the evidence (a saved report)" };
  return { kind: "recheck", label: "… and keep a fresh re-check as the evidence (a saved report)" };
}

/** The line on a watch row: how many of its entries carry a decision. */
export function reviewHistoryLine(history: WatchReviewHistoryItem[] | undefined): string | null {
  if (!history || history.length === 0) return null;
  const last = history[history.length - 1];
  const n = history.length;
  return `${n} decision${n === 1 ? "" : "s"} recorded; latest ${(DECISION_LABELS[last.status] ?? last.status).toLowerCase()} on ${last.decided_at.slice(0, 10)}.`;
}

export interface WatchReviewHistoryItem {
  entry_id: number;
  status: DecisionStatus;
  decided_at: string;
  saved_report_id: string | null;
  evidence_kind: EvidenceKind | null;
}

/** The sentence over the log when material entries await a decision. */
export function undecidedSentence(n: number): string | null {
  if (n <= 0) return null;
  return `${n} material ${n === 1 ? "entry has" : "entries have"} no decision recorded.`;
}

/** One line for an entry graded after the fact. */
export const GRADED_TODAY_NOTE = "Graded under today's table; this entry predates grading.";

export interface Change {
  kind: string;
  /** pKYC stage A: this change's own tier. */
  materiality?: Materiality;
  /** pKYC stage B: how the change compared with what the watcher expected. */
  expectation?: Expectation;
  field?: string;
  scheme?: string;
  code?: string;
  sources?: string[];
  degraded?: string[];
  missing?: string[];
  old?: unknown;
  /** Phase 302: names of the subsidiaries a `direct_children` change moved. */
  names?: Record<string, string>;
  new?: unknown;
  /** pKYC stage E, Route 2 — an `ownership_changed` change: which source,
   *  what happened, the party's kind (never its name), its interests, the
   *  share bands before and after, the beneficial-ownership flag, and
   *  whether a ≥25 % band was added or ended (the escalate rule). An
   *  `ownership_unchecked` change carries `source_id` and `parties`. */
  change?: "added" | "ended" | "changed";
  source_id?: string;
  party?: "person" | "entity";
  key?: string;
  interests?: string[];
  bands_before?: Band[] | null;
  bands_after?: Band[] | null;
  bo?: boolean;
  crossed_25?: boolean;
  uncompared?: number;
  parties?: number;
}

/** A share band: `[low, high, exclusive low, exclusive high]`; an exact
 *  share is a band of one value, an open end is `null`. */
export type Band = [number | null, number | null, boolean, boolean];

/** A register tier trigger's changes: a list as its entries before and
 *  after (never who), a field as its old and new value, two flags. */
export interface RegisterChanges {
  officers?: [number, number];
  shareholders?: [number, number];
  beneficial_owners?: [number, number];
  supervisory_board?: [number, number];
  name?: [string | null, string | null];
  status?: [string | null, string | null];
  legal_form?: [string | null, string | null];
  added?: boolean;
  removed?: boolean;
}

export interface WatchEntry {
  id: number;
  lei: string;
  legal_name: string | null;
  created_at: string;
  tier: Tier;
  trigger: {
    tier?: Tier;
    publish?: string;
    fields?: string[];
    /** Phase 301: queued by the one-shot re-read after a mirror rebuild. */
    resync?: boolean;
    version?: string;
    op?: string;
    caption?: string | null;
    entity_id?: string | null;
    datasets?: string[];
    topics?: string[];
    matched_on?: "lei" | "name";
    score?: number;
    reason?: "aged_out" | "delta_missing" | "register_gap" | "list_expiring";
    /** The review tier (stage C): the expiry notice's dates and numbers. */
    notice_at?: string;
    delete_after?: string;
    stale_days?: number;
    grace_days?: number;
    after?: string | null;
    before?: string | null;
    /** The register tier: which register, the company's code there, the
     *  index days read, and each list's entries before and after. */
    register?: "ariregister";
    code?: string;
    cuts?: string[];
    changes?: RegisterChanges;
  };
  changes: Change[];
  /** Phase 303: GLEIF's own log since this list's baseline (GLEIF-triggered entries). */
  gleif_log?: GleifLog | null;
  /** pKYC stage A: the entry's tier (the worst of its changes'), and the
   *  version of the table it was graded under — `null` when graded on read
   *  because the entry predates grading. */
  materiality?: Materiality;
  materiality_rules?: number | null;
  checked: CheckedSource[];
  degraded: { source_id: string; check?: string; affected_signals?: string[] }[];
  /** pKYC stage D: the run that produced this entry (the replay-cache key),
   *  whether the server still holds it, the latest decision and the history.
   *  Absent on an older server. */
  run_completed_at?: string | null;
  deepen_top?: number | null;
  run_held?: boolean;
  decision?: WatchDecision | null;
  decision_count?: number;
  decision_history?: WatchDecision[];
}

export interface WatchlistTiers {
  gleif: {
    available: boolean;
    watermark: string | null;
    refresh_enabled: boolean;
    last_applied_at: string | null;
    last_delta: string | null;
    rows_applied: Record<string, number>;
    record_count: number | null;
  };
  opensanctions: {
    available: boolean;
    last_version: string | null;
    last_checked_at: string | null;
    /** Phase 260: versions listed but not read yet (drained oldest first). */
    backlog?: number;
    /** Phase 260: the most recent versions that could not be read at all. */
    gaps?: { reason: "aged_out" | "delta_missing"; after: string | null; before: string | null; detected_at: string }[];
  };
  /** The register tier (Estonia's daily index diff). Absent on an older server. */
  register?: {
    available: boolean;
    index_cut: string | null;
    last_cut: string | null;
    waiting: boolean;
    gaps: { after: string | null; before: string | null; detail?: string; detected_at: string }[];
  };
  worker: { enabled: boolean; interval_s: number };
}

export interface WatchlistPayload {
  watches: Watch[];
  /** pKYC stage C. Absent on an older server. */
  review?: ListReview;
  /** pKYC stage D. Absent on an older server. */
  decisions?: ListDecisions;
  entries: WatchEntry[];
  caps: { per_list: number; total: number; in_list: number; total_watched: number };
  feed_url: string;
  tiers: WatchlistTiers;
}

// ---------------------------------------------------------------------------
// The token
// ---------------------------------------------------------------------------

export function readToken(): string | null {
  try {
    const v = window.localStorage.getItem(TOKEN_KEY);
    return v && v.trim() ? v.trim() : null;
  } catch {
    return null;
  }
}

export function storeToken(token: string): void {
  try {
    window.localStorage.setItem(TOKEN_KEY, token);
  } catch {
    /* storage absent or blocked: the token lives in the URL for this visit */
  }
}

export function forgetToken(): void {
  try {
    window.localStorage.removeItem(TOKEN_KEY);
  } catch {
    /* nothing to forget */
  }
}

export const FILTER_KEY = "opencheck.watchlist.filter";

/** The change-log filter a visit starts with: the URL's `?show=material`
 *  first, else what this browser kept, else all. */
function asFilter(value: string | null): ChangeLogFilter | null {
  return value === "material" || value === "needs_review" ? value : null;
}

export function filterFromLocation(search: string): ChangeLogFilter {
  const fromUrl = asFilter(new URLSearchParams(search).get("show"));
  if (fromUrl) return fromUrl;
  try {
    return asFilter(window.localStorage.getItem(FILTER_KEY)) ?? "all";
  } catch {
    return "all";
  }
}

export function storeFilter(filter: ChangeLogFilter): void {
  try {
    if (filter !== "all") window.localStorage.setItem(FILTER_KEY, filter);
    else window.localStorage.removeItem(FILTER_KEY);
  } catch {
    /* a per-viewer convenience: nothing lost when storage is absent */
  }
}

/** The token a `/watchlist` visit should use: the URL's `?token=` first (a
 *  shared or bookmarked link), else what this browser kept. */
export function tokenFromLocation(search: string, stored: string | null = readToken()): string | null {
  const fromUrl = new URLSearchParams(search).get("token");
  return fromUrl && fromUrl.trim() ? fromUrl.trim() : stored;
}

// ---------------------------------------------------------------------------
// Wording
// ---------------------------------------------------------------------------

const FIELD_WORDS: Record<string, string> = {
  legal_name: "legal name",
  entity_status: "entity status",
  registration_status: "LEI registration status",
  jurisdiction: "jurisdiction",
  legal_form: "legal form",
  successor_lei: "successor entity",
  successors: "successor entities",
  direct_parent_lei: "direct parent",
  ultimate_parent_lei: "ultimate parent",
  direct_exception: "direct-parent reporting exception",
  ultimate_exception: "ultimate-parent reporting exception",
  creation_date: "entity creation date",
  expiration_date: "entity expiration date",
  expiration_reason: "entity expiration reason",
  // Phase 300 — the register identifier, the address countries and the
  // record's classification joined the material set. Keep in step with
  // FIELD_WORDS in routers/watch.py.
  registered_at: "registration authority",
  registered_as: "register number",
  validated_at: "validation authority",
  legal_address_country: "legal address country",
  hq_address_country: "headquarters country",
  category: "entity category",
  sub_category: "entity sub-category",
  conformity_flag: "policy conformity flag",
  corporate_events: "legal entity events",
  direct_children: "direct subsidiaries",
};

/** One sentence for a `direct_children` change (Phase 302) — the same
 *  reading as `describe_children` on the feed. */
export function describeChildren(oldValue: unknown, newValue: unknown, names?: Record<string, string> | null): string {
  const n = names ?? {};
  const before = new Set((Array.isArray(oldValue) ? oldValue : []).map(String));
  const after = new Set((Array.isArray(newValue) ? newValue : []).map(String));
  const added = [...after].filter((x) => !before.has(x)).sort();
  const removed = [...before].filter((x) => !after.has(x)).sort();
  const words = (lei: string) => (n[lei] ? `${n[lei]} (${lei})` : lei);
  const parts: string[] = [];
  if (added.length) {
    const noun = added.length === 1 ? "a new direct subsidiary" : `${added.length} new direct subsidiaries`;
    parts.push(`GLEIF lists ${noun}: ${added.map(words).join("; ")}.`);
  }
  if (removed.length) {
    const noun = removed.length === 1 ? "a direct subsidiary" : `${removed.length} direct subsidiaries`;
    parts.push(`GLEIF no longer lists ${noun}: ${removed.map(words).join("; ")}.`);
  }
  return parts.join(" ") || "GLEIF direct subsidiaries changed.";
}

// Phase 301 — GLEIF Legal Entity Events in words. Keep in step with
// EVENT_TYPE_WORDS / EVENT_STATUS_WORDS in bods/gleif_events.py (Phase 305;
// routers/watch.py reads them from there). test_gleif_events.py parses this
// table and fails if the two differ.
const EVENT_TYPE_WORDS: Record<string, string> = {
  CHANGE_LEGAL_NAME: "legal name change",
  CHANGE_LEGAL_FORM: "legal form change",
  MERGERS_AND_ACQUISITIONS: "merger or acquisition",
  SPINOFF: "spin-off",
  TRANSFORMATION_UMBRELLA_TO_STANDALONE: "fund transformation (umbrella to standalone)",
};
const EVENT_STATUS_WORDS: Record<string, string> = {
  COMPLETED: "completed",
  IN_PROGRESS: "in progress",
  WITHDRAWN_CANCELLED: "withdrawn or cancelled",
};

export interface CorporateEvent {
  type?: string | null;
  status?: string | null;
  effective?: string | null;
  recorded?: string | null;
}

function eventTypeWords(t: string | null | undefined): string {
  const k = String(t ?? "");
  return EVENT_TYPE_WORDS[k] ?? (k.toLowerCase().replace(/_/g, " ") || "event");
}

function eventStatusWords(st: string | null | undefined): string {
  const k = String(st ?? "");
  return EVENT_STATUS_WORDS[k] ?? (k.toLowerCase().replace(/_/g, " ") || "status not given");
}

/** The calendar day a GLEIF event timestamp stands for (Phase 305) — the
 *  rule of `event_day` in bods/gleif_events.py, which the BODS
 *  `dissolutionDate` uses too. GLEIF stores the filer's local midnight as
 *  UTC (`2024-10-24T22:00:00Z` is 25 Oct in Europe), so a UTC time at or
 *  after 12:00 is the next day; any other offset keeps its own date; a bare
 *  date is itself; anything unreadable is "". */
export function eventDay(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$/.exec(raw);
  if (!m) return "";
  const [, y, mo, d, hh, , zone] = m;
  const day = new Date(Date.UTC(Number(y), Number(mo) - 1, Number(d)));
  if (day.getUTCFullYear() !== Number(y) || day.getUTCMonth() !== Number(mo) - 1 || day.getUTCDate() !== Number(d)) return "";
  const utc = zone === undefined || zone === "Z" || /^[+-]00:?00$/.test(zone);
  if (hh !== undefined && utc && Number(hh) >= 12) day.setUTCDate(day.getUTCDate() + 1);
  return day.toISOString().slice(0, 10);
}

function eventWords(e: CorporateEvent): string {
  const when = eventDay(e.effective);
  return `${eventTypeWords(e.type)} (${eventStatusWords(e.status)})${when ? `, effective ${when}` : ""}`;
}

function sameEvent(a: CorporateEvent, b: CorporateEvent): boolean {
  return a.type === b.type && a.status === b.status && a.effective === b.effective && a.recorded === b.recorded;
}

/** One sentence for a `corporate_events` change — the same reading as
 *  `describe_events` on the feed: what GLEIF now lists that it did not, a
 *  status move on an event of the same type worded as one. */
export function describeEvents(oldValue: unknown, newValue: unknown): string {
  const before = (Array.isArray(oldValue) ? oldValue : []).filter((e) => e && typeof e === "object") as CorporateEvent[];
  const after = (Array.isArray(newValue) ? newValue : []).filter((e) => e && typeof e === "object") as CorporateEvent[];
  const added = after.filter((e) => !before.some((b) => sameEvent(b, e)));
  const removed = before.filter((e) => !after.some((a) => sameEvent(a, e)));
  const parts: string[] = [];
  for (const e of added) {
    const i = removed.findIndex((r) => r.type === e.type);
    if (i >= 0) {
      const prior = removed.splice(i, 1)[0];
      const when = eventDay(e.effective);
      parts.push(
        `${eventTypeWords(e.type)}: ${eventStatusWords(prior.status)} → ${eventStatusWords(e.status)}${when ? `, effective ${when}` : ""}`,
      );
    } else {
      parts.push(eventWords(e));
    }
  }
  if (parts.length) {
    return `GLEIF recorded ${parts.length === 1 ? "a legal entity event" : "legal entity events"}: ${parts.join("; ")}.`;
  }
  if (removed.length) return `GLEIF no longer lists: ${removed.map(eventWords).join("; ")}.`;
  return "GLEIF legal entity events changed.";
}

export function fieldWords(field: string | undefined): string {
  if (!field) return "a field";
  return FIELD_WORDS[field] ?? field.replace(/_/g, " ");
}

function v(x: unknown): string {
  if (x === null || x === undefined || x === "") return "—";
  // A list value (successor entities) reads as one clause per item.
  if (Array.isArray(x)) return x.length ? x.map((i) => String(i)).join("; ") : "—";
  if (typeof x === "object") {
    const o = x as { liveness?: unknown; answered?: unknown; applicable?: unknown };
    if ("liveness" in o) return String(o.liveness ?? "—");
    if ("answered" in o) return `${o.answered} of ${o.applicable}`;
  }
  return String(x);
}

function list(xs: string[] | undefined, sourceLabel: (id: string) => string): string {
  return (xs ?? []).map(sourceLabel).join(", ");
}

/** One sentence per change. `sourceLabel` is `vocab.sourceLabel` in the
 *  app and the identity function in tests. */
export function describeChange(c: Change, sourceLabel: (id: string) => string = (s) => s): string {
  switch (c.kind) {
    case "gleif_field":
      if (c.field === "corporate_events") return describeEvents(c.old, c.new);
      if (c.field === "direct_children") return describeChildren(c.old, c.new, c.names);
      return `GLEIF ${fieldWords(c.field)}: ${v(c.old)} → ${v(c.new)}.`;
    case "legal_name":
      return `Legal name: ${v(c.old)} → ${v(c.new)}.`;
    case "jurisdiction":
      return `Jurisdiction: ${v(c.old)} → ${v(c.new)}.`;
    case "register_status": {
      const src = (c.new as { source_id?: string } | null)?.source_id;
      return `Register status: ${v(c.old)} → ${v(c.new)}${src ? ` (${sourceLabel(src)})` : ""}.`;
    }
    case "founding_date":
      return `Incorporation date: ${v(c.old)} → ${v(c.new)}.`;
    case "legal_form":
      return `Legal form: ${v(c.old)} → ${v(c.new)}.`;
    case "dissolution_date":
      return `Dissolution date: ${v(c.old)} → ${v(c.new)}.`;
    case "identifier":
      return `Identifier ${c.scheme ?? ""}: ${v(c.old)} → ${v(c.new)}.`;
    case "signal_new":
      return `New risk signal ${c.code} (${list(c.sources, sourceLabel)}).`;
    case "context_new":
      return `New context signal ${c.code} (${list(c.sources, sourceLabel)}).`;
    case "signal_retired":
      return `Risk signal ${c.code} no longer reported — ${list(c.sources, sourceLabel)} answered and did not report it.`;
    case "context_retired":
      return `Context signal ${c.code} no longer reported — ${list(c.sources, sourceLabel)} answered and did not report it.`;
    case "signal_unchecked":
      return `Could not re-check risk signal ${c.code}: ${list(c.degraded?.length ? c.degraded : c.sources, sourceLabel)} did not answer. Not a clean result.`;
    case "context_unchecked":
      return `Could not re-check context signal ${c.code}: ${list(c.degraded?.length ? c.degraded : c.sources, sourceLabel)} did not answer.`;
    case "coverage_changed":
      return `Coverage: ${v(c.old)} → ${v(c.new)} sources answered.`;
    case "coverage_unchecked":
      return `Coverage fell to ${v(c.new)} because ${list(c.missing, sourceLabel)} could not be reached — a fact about the check, not the company.`;
    case "verdict":
      return `Verdict now reads: ${v(c.new)}`;
    case "ownership_changed":
    case "ownership_unchecked":
      return ownershipChangeSentence(c, sourceLabel(c.source_id ?? ""));
    default:
      return `${c.kind.replace(/_/g, " ")}.`;
  }
}

// The ownership diff's words (pKYC stage E, Route 2). Keep in step with
// INTEREST_WORDS / PARTY_WORDS / CHANGE_WORDS in opencheck/watchlist_ownership.py;
// test_watchlist_ownership.py parses these tables.
const INTEREST_WORDS: Record<string, string> = {
  shareholding: "shareholding",
  votingRights: "voting rights",
  appointmentOfBoard: "right to appoint the board",
  otherInfluenceOrControl: "other influence or control",
  seniorManagingOfficial: "senior managing official",
  settlor: "settlor",
  trustee: "trustee",
  protector: "protector",
  beneficiaryOfLegalArrangement: "beneficiary",
  rightsToSurplusAssetsOnDissolution: "rights to surplus assets",
  rightsToProfitOrIncome: "rights to profit or income",
  rightsGrantedByContract: "rights granted by contract",
  conditionalRightsGrantedByContract: "conditional rights granted by contract",
  controlViaCompanyRulesOrAgreement: "control via company rules or agreement",
  controlByLegalFramework: "control by legal framework",
  boardMember: "board membership",
  boardChair: "board chair",
  enjoymentAndUseOfAssets: "enjoyment and use of assets",
  nominee: "nominee holding",
  nominator: "nominator",
  unknownInterest: "interest of unstated kind",
  unpublishedInterest: "interest whose details are withheld",
};
const PARTY_WORDS: Record<string, string> = {
  person: "A person",
  entity: "A company",
  bo: "A registered beneficial owner",
};
const CHANGE_WORDS: Record<string, string> = {
  added: "added",
  ended: "ended",
  changed: "changed",
};

function pct(n: number): string {
  return `${Number.isInteger(n) ? n : +n.toFixed(4)}%`;
}

/** One band in words — the feed's `band_words`. */
export function bandWords(b: Band | null | undefined): string {
  if (!b) return "an unstated share";
  const [lo, hi, exlo, exhi] = b;
  if (lo !== null && hi !== null && lo === hi) return `exactly ${pct(lo)}`;
  if (lo !== null && hi !== null) {
    const l = +lo.toFixed(4);
    const h = +hi.toFixed(4);
    if (exlo && exhi) return `over ${l}% to under ${h}%`;
    if (exlo) return `over ${l}% to ${h}%`;
    if (exhi) return `${l}% to under ${h}%`;
    return `${l}–${h}%`;
  }
  if (lo !== null) return `${exlo ? "more than" : "at least"} ${pct(lo)}`;
  return `${exhi ? "under" : "at most"} ${pct(hi as number)}`;
}

function interestWords(types: string[] | undefined): string {
  const words = (types ?? []).map((t) => INTEREST_WORDS[t] ?? t);
  if (!words.length) return "interest";
  if (words.length === 1) return words[0];
  return `${words.slice(0, -1).join(", ")} and ${words[words.length - 1]}`;
}

function bandsWords(bands: Band[] | null | undefined): string {
  return (bands ?? []).map(bandWords).join(" and ");
}

/** The sentence for an ownership change — never a name. */
export function ownershipChangeSentence(c: Change, source: string): string {
  if (c.kind === "ownership_unchecked") {
    const n = c.parties ?? 0;
    return `${n} ${n === 1 ? "party's" : "parties'"} holdings could not be re-checked — ${source} did not answer.`;
  }
  const who = c.bo ? PARTY_WORDS.bo : PARTY_WORDS[c.party ?? ""] ?? "A party";
  const what = interestWords(c.interests);
  const before = bandsWords(c.bands_before);
  const after = bandsWords(c.bands_after);
  let body: string;
  if (c.change === "changed") {
    body =
      before !== after
        ? `${who}'s ${what} changed from ${before || "an unstated share"} to ${after || "an unstated share"}`
        : `${who}'s ${what} changed`;
  } else {
    const band = c.change === "added" ? after : before;
    body = `${who}'s ${what}${band ? ` of ${band}` : ""} ${CHANGE_WORDS[c.change ?? ""] ?? "changed"}`;
  }
  return `${body} (${source}).`;
}

/** "N parties could not be compared (source publishes no person key)" —
 *  one line per source, from the count each ownership change carries. */
export function uncomparedLines(changes: Change[], sourceLabel: (id: string) => string = (s) => s): string[] {
  const seen = new Map<string, number>();
  for (const c of changes) {
    if (c.kind === "ownership_changed" && c.uncompared) seen.set(c.source_id ?? "", c.uncompared);
  }
  return Array.from(seen.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([sid, n]) => `${n} ${n === 1 ? "party" : "parties"} could not be compared (${sourceLabel(sid)} publishes no person key).`);
}

// The register tier's words. Keep in step with REGISTER_CHANGE_WORDS in
// opencheck/watchlist.py; test_watchlist_register.py parses this table.
const REGISTER_CHANGE_WORDS: Record<string, string> = {
  officers: "Persons on the registry card",
  shareholders: "Shareholders",
  beneficial_owners: "Beneficial owners",
  supervisory_board: "Supervisory board",
  name: "Name",
  status: "Status",
  legal_form: "Legal form",
};

// The mapper's English words for the register's status codes
// (EE_STATUS_LABELS in bods/mappers/estonia.py; pinned by the same test).
const EE_STATUS_WORDS: Record<string, string> = {
  R: "Entered in the register",
  L: "In liquidation",
  N: "Bankrupt",
  K: "Deleted",
};

const REGISTER_LISTS = ["officers", "shareholders", "beneficial_owners", "supervisory_board"] as const;
const REGISTER_FIELDS = ["name", "status", "legal_form"] as const;

/** One sentence per change a register tier trigger carries — the feed's
 *  `register_change_lines`. */
export function registerChangeLines(changes: RegisterChanges | undefined): string[] {
  const c = changes ?? {};
  const out: string[] = [];
  if (c.removed) out.push("The company is no longer in the register's open data — deleted companies are not published.");
  if (c.added) out.push("The company appeared in the register's open data.");
  for (const key of REGISTER_LISTS) {
    const pair = c[key];
    if (!Array.isArray(pair) || pair.length !== 2) continue;
    const [before, after] = pair;
    const noun = after === 1 ? "entry" : "entries";
    out.push(
      before === after
        ? `${REGISTER_CHANGE_WORDS[key]}: changed (${after} ${noun} before and after).`
        : `${REGISTER_CHANGE_WORDS[key]}: ${before || "none"} → ${after || "none"} ${noun}.`,
    );
  }
  for (const key of REGISTER_FIELDS) {
    const pair = c[key];
    if (!Array.isArray(pair) || pair.length !== 2) continue;
    let [o, n] = pair;
    if (key === "status") {
      o = o == null ? o : (EE_STATUS_WORDS[String(o)] ?? o);
      n = n == null ? n : (EE_STATUS_WORDS[String(n)] ?? n);
    }
    out.push(`${REGISTER_CHANGE_WORDS[key]}: ${o || "—"} → ${n || "—"}.`);
  }
  return out;
}

function daysWords(cuts: string[] | undefined): string {
  const c = (cuts ?? []).filter(Boolean);
  if (!c.length) return "its latest open data";
  if (c.length === 1) return `its open data of ${c[0]}`;
  return `its open data of ${c.slice(0, -1).join(", ")} and ${c[c.length - 1]}`;
}

/** Why the entry exists: the tier that fired, in its own words. */
export function triggerSentence(e: WatchEntry): string {
  const t = e.trigger ?? {};
  if (e.tier === "gleif") {
    const fields = t.fields?.length ? ` (${t.fields.map(fieldWords).join(", ")})` : "";
    if (t.resync) {
      return `OpenCheck's GLEIF mirror was rebuilt from the ${t.publish ?? "latest"} Golden Copy and now reads legal entity events; GLEIF's record changed after this company was first watched${fields}.`;
    }
    return t.publish
      ? `GLEIF published a change to this record in its ${t.publish} delta${fields}.`
      : `GLEIF published a change to this record${fields}.`;
  }
  if (e.tier === "opensanctions") {
    const op = t.op === "ADD" ? "added" : t.op === "DEL" ? "removed" : "changed";
    const who = t.caption || t.entity_id || "an entity";
    const how = t.matched_on === "lei" ? "by LEI" : "by name";
    const sets = t.datasets?.length ? ` — ${t.datasets.join(", ")}` : "";
    return `OpenSanctions ${op} ${who}, matching this company ${how}${sets}.`;
  }
  if (e.tier === "register") {
    return `The Estonian e-Business Register recorded a change to this company in ${daysWords(t.cuts)}${
      t.code ? ` (registry code ${t.code})` : ""
    }.`;
  }
  if (e.tier === "review") {
    return `Nobody has opened or polled this watchlist for ${t.stale_days ?? "a number of"} days. It will be deleted, with its watches and this log, after ${
      (t.delete_after ?? "").slice(0, 10) || "the grace period"
    } unless it is opened or its feed is read before then. Opening it keeps it.`;
  }
  if (e.tier === "catch_up") {
    if (t.reason === "register_gap") {
      return `The Estonian e-Business Register's changes between ${t.after ?? "?"} and ${t.before ?? "?"} could not be read, so every watched Estonian company was re-checked.`;
    }
    return t.reason === "delta_missing"
      ? `OpenSanctions listed version ${t.after ?? "?"} but its delta file could not be downloaded, so every watched company was re-checked.`
      : `OpenSanctions versions published after ${t.after ?? "?"} were no longer listed when the watcher came to read them, so every watched company was re-checked.`;
  }
  return "Re-checked by hand.";
}

/** The chip on an entry: which tier fired, and its tone. */
export function tierChip(tier: Tier): { label: string; tone: "risk" | "accent" | "neutral" | "context" } {
  if (tier === "gleif") return { label: "GLEIF delta", tone: "accent" };
  if (tier === "opensanctions") return { label: "OpenSanctions delta", tone: "risk" };
  if (tier === "catch_up") return { label: "Catch-up re-check", tone: "context" };
  if (tier === "register") return { label: "Register change", tone: "accent" };
  if (tier === "review") return { label: "Review", tone: "neutral" };
  return { label: "By hand", tone: "neutral" };
}

/** The one-line headline for an entry. */
export function entryHeadline(e: WatchEntry): string {
  if (e.tier === "review") return "This list: nobody has opened it for a while — it will be deleted unless opened";
  const name = e.legal_name || e.lei;
  const kinds = new Set(e.changes.map((c) => c.kind));
  if (!e.changes.length && e.tier === "register") return `${name}: Estonian register record changed`;
  if (!e.changes.length) return `${name}: re-run found no difference`;
  if (e.changes.some((c) => c.kind === "gleif_field" && c.field === "corporate_events")) {
    return `${name}: legal entity event recorded`;
  }
  if (kinds.has("register_status") || kinds.has("dissolution_date")) return `${name}: register status changed`;
  if (kinds.has("signal_new")) return `${name}: new risk signal`;
  if (kinds.has("ownership_changed")) return `${name}: ownership changed`;
  if (kinds.has("gleif_field")) {
    const gleif = e.changes.filter((c) => c.kind === "gleif_field");
    if (gleif.every((c) => c.field === "direct_children")) return `${name}: direct subsidiaries changed`;
    return `${name}: GLEIF record changed`;
  }
  const n = e.changes.length;
  return `${name}: ${n} change${n === 1 ? "" : "s"}`;
}

/** "N sources checked on D" — the retrieval clock, per entry. */
export function checkedSentence(checked: CheckedSource[]): string | null {
  const reached = checked.filter((c) => c.liveness && c.liveness !== "stub");
  if (!reached.length) return null;
  const dates = Array.from(
    new Set(reached.map((c) => (c.retrieved_at ?? "").slice(0, 10)).filter(Boolean)),
  ).sort();
  const when = dates.length ? ` on ${dates.join(" and ")}` : "";
  return `${reached.length} source${reached.length === 1 ? "" : "s"} checked as a result${when}.`;
}

/** The honesty line under the list: what each tier is actually doing. */
export function tierSentence(t: WatchlistTiers | null): string {
  if (!t) return "";
  const parts: string[] = [];
  if (t.gleif.available) {
    const rows = t.gleif.rows_applied?.entities;
    const wm = t.gleif.watermark ? ` as of ${t.gleif.watermark} UTC` : "";
    const count = t.gleif.record_count ? `${t.gleif.record_count.toLocaleString("en-GB")} LEI records` : "GLEIF";
    parts.push(
      `GLEIF: OpenCheck holds ${count}${wm}; the last delta applied changed ${
        typeof rows === "number" ? rows.toLocaleString("en-GB") : "some"
      } of them, and only a watched LEI in that delta is re-run.`,
    );
  } else {
    parts.push("GLEIF: no mirror on this instance, so the GLEIF tier cannot fire here.");
  }
  parts.push(
    t.opensanctions.available
      ? `OpenSanctions: every published entity delta is read${
          t.opensanctions.last_version ? ` (last version ${t.opensanctions.last_version})` : ""
        } and matched against the watched names.`
      : "OpenSanctions: the sanctions tier is off on this instance.",
  );
  const backlog = t.opensanctions.available ? (t.opensanctions.backlog ?? 0) : 0;
  if (backlog > 0) {
    parts.push(
      `${backlog} OpenSanctions version${backlog === 1 ? " is" : "s are"} still to be read, oldest first.`,
    );
  }
  const gap = t.opensanctions.gaps?.length ? t.opensanctions.gaps[t.opensanctions.gaps.length - 1] : null;
  if (gap) {
    parts.push(
      `On ${gap.detected_at.slice(0, 10)} some OpenSanctions versions could not be read, so every watched company was re-checked instead.`,
    );
  }
  const reg = t.register;
  if (reg?.available) {
    parts.push(
      `Estonia: OpenCheck rebuilds the e-Business Register's open data every day${
        reg.index_cut ? ` (this instance holds ${reg.index_cut})` : ""
      }, and a watched Estonian company whose people, name, status or legal form changed is re-run.`,
    );
    const regGap = reg.gaps?.length ? reg.gaps[reg.gaps.length - 1] : null;
    if (regGap) {
      parts.push(
        `On ${regGap.detected_at.slice(0, 10)} some days of Estonian register changes could not be read, so every watched Estonian company was re-checked instead.`,
      );
    }
  }
  parts.push(
    reg?.available
      ? "No other national register is polled; they are re-fetched only when one of those tiers fires."
      : "National registers are never polled; they are re-fetched only when one of those two fires.",
  );
  return parts.join(" ");
}

export function feedHelp(): string {
  // Phase 234: the server deletes a list unopened for
  // OPENCHECK_WATCHLIST_STALE_DAYS (default 90); the feed reader counts.
  return (
    "Subscribe in any feed reader. The address is the key to this list — anyone holding it can read it. " +
    "A list that is neither opened here nor read by a feed reader for 90 days is deleted."
  );
}

/** Newest first, as the log reads. */
export function sortEntries(entries: WatchEntry[]): WatchEntry[] {
  return [...entries].sort((a, b) => b.id - a.id);
}
