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

/** What pressing Watch commits to — said beside the button, before it is
 *  pressed, not in the alert afterwards. */
export const WATCH_PROMISE =
  "Watching re-checks this company only when GLEIF or OpenSanctions publish a change to its record — no polling, no account, no email.";

export const WATCH_LABEL = "Watch for changes";
export const WATCHING_LABEL = "On your watchlist";

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
}

export type Tier = "gleif" | "opensanctions" | "manual";

export interface Change {
  kind: string;
  field?: string;
  scheme?: string;
  code?: string;
  sources?: string[];
  degraded?: string[];
  missing?: string[];
  old?: unknown;
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
    version?: string;
    op?: string;
    caption?: string | null;
    entity_id?: string | null;
    datasets?: string[];
    topics?: string[];
    matched_on?: "lei" | "name";
    score?: number;
  };
  changes: Change[];
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
  opensanctions: { available: boolean; last_version: string | null; last_checked_at: string | null };
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
  direct_parent_lei: "direct parent",
  ultimate_parent_lei: "ultimate parent",
  direct_exception: "direct-parent reporting exception",
  ultimate_exception: "ultimate-parent reporting exception",
  creation_date: "entity creation date",
  expiration_date: "entity expiration date",
  expiration_reason: "entity expiration reason",
};

export function fieldWords(field: string | undefined): string {
  if (!field) return "a field";
  return FIELD_WORDS[field] ?? field.replace(/_/g, " ");
}

function v(x: unknown): string {
  if (x === null || x === undefined || x === "") return "—";
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
  return "Re-checked by hand.";
}

/** The one-line headline for an entry. */
export function entryHeadline(e: WatchEntry): string {
  const name = e.legal_name || e.lei;
  const kinds = new Set(e.changes.map((c) => c.kind));
  if (!e.changes.length) return `${name}: re-run found no difference`;
  if (kinds.has("register_status") || kinds.has("dissolution_date")) return `${name}: register status changed`;
  if (kinds.has("signal_new")) return `${name}: new risk signal`;
  if (kinds.has("gleif_field")) return `${name}: GLEIF record changed`;
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
  parts.push("National registers are never polled; they are re-fetched only when one of those two fires.");
  return parts.join(" ");
}

export function feedHelp(): string {
  return "Subscribe in any feed reader. The address is the key to this list — anyone holding it can read it.";
}

/** Newest first, as the log reads. */
export function sortEntries(entries: WatchEntry[]): WatchEntry[] {
  return [...entries].sort((a, b) => b.id - a.id);
}
