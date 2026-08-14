/**
 * How current a source card's data is.
 *
 * OpenCheck republishes other people's data, and until Phase 99 every source
 * card looked identical whether its payload came from a live register call, a
 * cached response, a months-old bulk snapshot or a committed fixture. This is
 * the visual half of that fix: the sibling of the degraded-source notice, on
 * the same principle — data that is not current must not read as live, just as
 * a check that could not run must not read as clean.
 *
 * Live is the unmarked default. Badging every card would put a second row of
 * chips beside the risk signals and dilute both, so only the cases a reader
 * would otherwise misread get visual weight.
 */

export type Liveness = "live" | "cached" | "snapshot" | "curated" | "stub";

export interface SourceLiveness {
  liveness: Liveness;
  label: string;
  retrieved_at: string | null;
  detail: string | null;
}

/** Short, absolute date — "3 Jun 2026". Absolute beats relative here: "2 months
 *  ago" is friendlier but a reader checking a company wants the actual date. */
function formatDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

function formatDateTime(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const sameDay = new Date().toDateString() === d.toDateString();
  if (sameDay) {
    return d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
  }
  return formatDate(iso);
}

/** Days since a timestamp, or null when unparseable. */
export function ageInDays(iso: string | null): number | null {
  if (!iso) return null;
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return null;
  return Math.floor((Date.now() - then) / 86_400_000);
}

/** A snapshot older than this reads as materially stale and gets warmer styling. */
export const STALE_AFTER_DAYS = 120;

const STYLES: Record<Liveness, string> = {
  // Muted, not green: being live is the expected baseline, not an achievement.
  live: "border-oo-rule bg-white text-oo-muted",
  cached: "border-oo-rule bg-oo-bg text-oo-muted",
  snapshot: "border-sky-200 bg-sky-50 text-sky-800",
  curated: "border-indigo-200 bg-indigo-50 text-indigo-800",
  // Stub is the one a reader must never mistake for real data.
  stub: "border-amber-300 bg-amber-50 text-amber-800",
};

const STALE_STYLE = "border-orange-300 bg-orange-50 text-orange-800";

export function livenessTitle(info: SourceLiveness): string {
  const parts: string[] = [];
  if (info.detail) parts.push(info.detail);
  if (info.retrieved_at) {
    parts.push(`Retrieved ${formatDate(info.retrieved_at)}`);
  } else if (info.liveness === "stub") {
    parts.push("No source was contacted");
  } else {
    parts.push("Retrieval date not recorded");
  }
  return parts.join(" · ");
}

export function LivenessBadge({
  info,
  className = "",
}: {
  info: SourceLiveness | undefined;
  className?: string;
}) {
  if (!info) return null;

  const days = ageInDays(info.retrieved_at);
  const stale =
    (info.liveness === "snapshot" || info.liveness === "cached") &&
    days !== null &&
    days >= STALE_AFTER_DAYS;

  // A live card is the unmarked default — badging it adds noise without
  // telling the reader anything they would not have assumed.
  if (info.liveness === "live" && !stale) return null;

  let text = info.label;
  if (info.liveness === "snapshot" && info.retrieved_at) {
    text = `Snapshot · ${formatDate(info.retrieved_at)}`;
  } else if (info.liveness === "cached" && info.retrieved_at) {
    text = `Cached · ${formatDateTime(info.retrieved_at)}`;
  } else if (info.liveness === "curated") {
    text = info.retrieved_at
      ? `Curated set · ${formatDate(info.retrieved_at)}`
      : "Curated set";
  } else if (info.liveness === "stub") {
    text = "Placeholder data";
  }

  return (
    <span
      className={`inline-flex items-center gap-1 rounded-oo border px-2 py-0.5 text-[10px] font-semibold whitespace-nowrap ${
        stale ? STALE_STYLE : STYLES[info.liveness]
      } ${className}`}
      title={livenessTitle(info)}
    >
      {info.liveness === "stub" && (
        <svg width="10" height="10" viewBox="0 0 16 16" fill="none" aria-hidden="true">
          <path
            d="M8 2.5 14 13H2z"
            stroke="currentColor"
            strokeWidth="1.3"
            strokeLinejoin="round"
          />
          <path d="M8 6.5v3" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
          <circle cx="8" cy="11.2" r="0.6" fill="currentColor" />
        </svg>
      )}
      {text}
      <span className="sr-only"> — {livenessTitle(info)}</span>
    </span>
  );
}
