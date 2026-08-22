/**
 * The four checks a report can show, and the pure functions that put one in
 * the URL and the browser tab.
 *
 * Phase 122. `esg` joined the three depth modes here; it had previously
 * rendered as a section inside QuickCheck, which made it reachable by
 * scrolling and by nothing else. It is deliberately last and marked as a
 * topic rather than a depth — the tab strip separates it for the same
 * reason.
 *
 * These live in `lib/` rather than in App.tsx so they can be tested: the
 * frontend suite is logic-only (no jsdom), so anything worth pinning has to
 * be reachable without rendering a component.
 */

export type CheckMode = "quick" | "full" | "background" | "esg";

export const CHECK_MODES: CheckMode[] = ["quick", "full", "background", "esg"];

/**
 * `?mode=` → a mode. Anything unrecognised falls back to quick rather than
 * throwing or rendering an empty report: a stale or hand-edited link should
 * open the fastest check, not an error.
 */
export function parseMode(raw: string | null | undefined): CheckMode {
  return CHECK_MODES.includes((raw ?? "") as CheckMode) ? (raw as CheckMode) : "quick";
}

/**
 * The `?mode=` value to write for a mode — `null` means "remove the
 * parameter". QuickCheck is the default, so it stays out of the URL and a
 * shared QuickCheck link keeps the short form it has always had.
 */
export function modeParam(mode: CheckMode): string | null {
  return mode === "quick" ? null : mode;
}

/** Human label, used in the document title and in analytics-free copy. */
export function modeLabel(mode: CheckMode): string {
  switch (mode) {
    case "full":
      return "FullCheck";
    case "background":
      return "BackgroundCheck";
    case "esg":
      return "Climate & ESG";
    default:
      return "QuickCheck";
  }
}

/**
 * What the browser tab says for a report.
 *
 * QuickCheck deliberately keeps the bare `NAME - OpenCheck` form, hyphen and
 * all: that string is the server-rendered /entity page's template from the
 * SEO ticket, and the two must stay identical for the default view. Only a
 * non-default mode adds a segment, so a row of restored tabs is readable
 * without changing what search engines have already indexed.
 */
export function documentTitleFor(mode: CheckMode, name: string): string {
  if (mode === "quick") return `${name} - OpenCheck`;
  return `${name} — ${modeLabel(mode)} — OpenCheck`;
}
