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
}

/** Phase 260: ``catch_up`` = every watched company re-run because
 * OpenSanctions versions could not be read (aged out of its version list,
 * or a listed delta file gone). */
export type Tier = "gleif" | "opensanctions" | "manual" | "catch_up";

export interface Change {
  kind: string;
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
    reason?: "aged_out" | "delta_missing";
    after?: string | null;
    before?: string | null;
  };
  changes: Change[];
  /** Phase 303: GLEIF's own log since this list's baseline (GLEIF-triggered entries). */
  gleif_log?: GleifLog | null;
  checked: CheckedSource[];
  degraded: { source_id: string; check?: string; affected_signals?: string[] }[];
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
  worker: { enabled: boolean; interval_s: number };
}

export interface WatchlistPayload {
  watches: Watch[];
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
// EVENT_TYPE_WORDS / EVENT_STATUS_WORDS in routers/watch.py.
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

function eventWords(e: CorporateEvent): string {
  const when = String(e.effective ?? "").slice(0, 10);
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
      const when = String(e.effective ?? "").slice(0, 10);
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
    default:
      return `${c.kind.replace(/_/g, " ")}.`;
  }
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
  if (e.tier === "catch_up") {
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
  return { label: "By hand", tone: "neutral" };
}

/** The one-line headline for an entry. */
export function entryHeadline(e: WatchEntry): string {
  const name = e.legal_name || e.lei;
  const kinds = new Set(e.changes.map((c) => c.kind));
  if (!e.changes.length) return `${name}: re-run found no difference`;
  if (e.changes.some((c) => c.kind === "gleif_field" && c.field === "corporate_events")) {
    return `${name}: legal entity event recorded`;
  }
  if (kinds.has("register_status") || kinds.has("dissolution_date")) return `${name}: register status changed`;
  if (kinds.has("signal_new")) return `${name}: new risk signal`;
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
  parts.push("National registers are never polled; they are re-fetched only when one of those two fires.");
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
