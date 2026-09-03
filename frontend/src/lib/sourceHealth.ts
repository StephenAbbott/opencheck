/**
 * Source health, as the sources page says it (Phase 161).
 *
 * `/source-health` is the last weekly sweep's verdict on every source, read
 * back from the release asset the sweep uploads; this module decides how a
 * verdict is *worded* on a catalogue card. It is in `lib/` because the
 * frontend suite is logic-only: the status word, its tone, the freshness
 * sentence and the rule for which cards open their details are the claims,
 * and a claim that lives only inside JSX cannot be pinned.
 *
 * Two things the page must not do:
 *
 * - **Say "healthy" of a source that was not tested.** A source the sweep
 *   skipped for want of a credential is *not tested*, in the neutral tone —
 *   a fact with no valence — never the ok tone, and never omitted, because
 *   omission on a page where every other card carries a status reads as ok.
 * - **Colour a degradation as a failure.** The sweep draws the line
 *   deliberately (a rate limit, a snapshot due a refresh, a register that
 *   refuses datacentre IPs are caveats, not outages) so that a monitor that
 *   is permanently red for something that is not broken keeps being read.
 *   Degraded is the `context` tone — a structural observation, not an
 *   adverse finding — and failed is `warn`, the tone for something that did
 *   not run. Nothing here is `risk`: a source's health is a fact about
 *   OpenCheck's plumbing, never a finding about a subject.
 */

import type { SourceHealthReport, SourceHealthRow, SourceHealthStatus } from "./api";
import type { ChipTone } from "../components/ui/Chip";

export const STATUS_WORD: Record<SourceHealthStatus, string> = {
  ok: "Healthy",
  degraded: "Degraded",
  fail: "Failed",
  skipped: "Not tested",
};

export const STATUS_TONE: Record<SourceHealthStatus, ChipTone> = {
  ok: "ok",
  degraded: "context",
  fail: "warn",
  skipped: "neutral",
};

/** The colour class for a history dot — the chip tone's text colour, so the
 *  two encodings of a status can never disagree. */
export const STATUS_DOT: Record<SourceHealthStatus, string> = {
  ok: "bg-oo-ok-text",
  degraded: "bg-oo-info-text",
  fail: "bg-oo-warn-text",
  skipped: "bg-oo-rule",
};

const LIVENESS_PHRASE: Record<NonNullable<SourceHealthRow["liveness"]>, string> = {
  live: "Answered live",
  cached: "Served from OpenCheck's cache",
  snapshot: "Snapshot",
  curated: "Curated fixture",
  stub: "Placeholder data",
};

/** "Mon 31 Aug 2026, 07:31 UTC" from the sweep's ISO timestamp. */
export function formatSweepTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const day = d.toLocaleDateString("en-GB", {
    weekday: "short",
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
  const time = d.toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
  });
  // en-GB puts a comma after the weekday ("Mon, 31 Aug"); the page reads
  // "Mon 31 Aug 2026, 07:31 UTC", one comma, before the time.
  return `${day.replace(",", "")}, ${time} UTC`;
}

/** "31 Aug 2026" — for a snapshot's own date, where the time is noise. */
export function formatSweepDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
}

export interface HealthSummary {
  /** "Mon 31 Aug 2026, 07:31 UTC" */
  sweptAt: string;
  counts: Record<SourceHealthStatus, number>;
  /** Present when the API served its last good copy because the asset could not be re-read. */
  staleNote: string | null;
}

/** The strip under the page intro, or null when there is nothing to say —
 *  the page then renders exactly as it did before health existed. */
export function healthSummary(report: SourceHealthReport | null | undefined): HealthSummary | null {
  if (!report || !report.available) return null;
  return {
    sweptAt: formatSweepTime(report.generated_at),
    counts: report.counts,
    staleNote: report.stale
      ? "The latest sweep could not be read just now; this is the last one OpenCheck holds."
      : null,
  };
}

export interface HealthDetailRow {
  label: string;
  value: string;
}

export interface CardHealth {
  status: SourceHealthStatus;
  word: string;
  tone: ChipTone;
  /** "checked Mon 31 Aug 2026, 07:31 UTC" — the sweep, not the source's data. */
  checked: string;
  /** Oldest first, this sweep last. */
  history: SourceHealthStatus[];
  rows: HealthDetailRow[];
  /** The reason it is not ok, and any known gap — in the card's own words. */
  notes: string[];
  /** A card that has something to explain opens its details unasked. */
  openByDefault: boolean;
  /** One sentence for assistive technology, in place of the chip + rows. */
  summary: string;
}

/** How fresh the source's *data* was, as distinct from when it was checked. */
export function freshnessPhrase(row: Pick<SourceHealthRow, "liveness" | "retrieved_at">): string {
  if (!row.liveness) return "—";
  const phrase = LIVENESS_PHRASE[row.liveness];
  if (row.liveness === "snapshot" || row.liveness === "curated" || row.liveness === "cached") {
    return row.retrieved_at ? `${phrase} · ${formatSweepDate(row.retrieved_at)}` : phrase;
  }
  return phrase;
}

function statementsPhrase(row: SourceHealthRow): string {
  if (row.statement_total === null) return "—";
  const n = row.statement_total;
  const base = `${n.toLocaleString("en-GB")} ${n === 1 ? "statement" : "statements"}`;
  if (row.statement_collapse) {
    const worst = Object.entries(row.statement_collapse)
      .map(([kind, d]) => `${kind} ${d.was} → ${d.now}`)
      .join(", ");
    return `${base} — fell since the previous sweep (${worst})`;
  }
  return base;
}

export function cardHealth(
  sourceId: string,
  report: SourceHealthReport | null | undefined,
  sourceName?: string,
): CardHealth | null {
  if (!report || !report.available) return null;
  const row = report.sources[sourceId];
  if (!row) return null;
  const word = STATUS_WORD[row.status];
  const checked = `checked ${formatSweepTime(report.generated_at)}`;
  // A row the sweep could not fill is left out rather than printed as "—":
  // on a degraded card the reason below says why, and three dashes above it
  // would only push that down.
  const candidates: HealthDetailRow[] =
    row.status === "skipped"
      ? []
      : [
          { label: "Freshness", value: freshnessPhrase(row) },
          { label: "Latency", value: row.latency_ms === null ? "—" : `${(row.latency_ms / 1000).toFixed(1)} s` },
          { label: "Statements last sweep", value: statementsPhrase(row) },
          { label: "Attempts", value: row.attempts === 2 && row.status === "ok" ? "passed on retry" : "—" },
        ];
  const rows = candidates.filter((r) => r.value !== "—");
  const notes: string[] = [];
  if (row.status !== "ok" && row.reason) notes.push(row.reason);
  if (row.known_gap) notes.push(`Known gap: ${row.known_gap}`);
  if (row.status === "skipped" && !row.reason) {
    notes.push("Not tested: the sweep holds no credential for this source. Live lookups use the deployment's key.");
  }
  const name = sourceName ?? sourceId;
  return {
    status: row.status,
    word,
    tone: STATUS_TONE[row.status],
    checked,
    history: row.history,
    rows,
    notes,
    openByDefault: row.status !== "ok" || Boolean(row.known_gap) || Boolean(row.statement_collapse),
    summary: `${name}: ${word.toLowerCase()} in the sweep of ${formatSweepTime(report.generated_at)}.${
      notes.length ? ` ${notes.join(" ")}` : ""
    }`,
  };
}
