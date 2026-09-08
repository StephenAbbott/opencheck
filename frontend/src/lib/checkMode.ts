/**
 * The five checks a report can show, and the pure functions that put one in
 * the URL and the browser tab.
 *
 * Phase 122. `esg` joined the three depth modes here; it had previously
 * rendered as a section inside QuickCheck, which made it reachable by
 * scrolling and by nothing else. It is deliberately last and marked as a
 * topic rather than a depth — the tab strip separates it for the same
 * reason.
 *
 * Phase 185. `subsidiaries` joined as a second topic, placed after the
 * divider and before ESG: it asks a different question from the three depth
 * modes (what does this company own, rather than who owns it) but it is
 * still a question about ownership, so it sits beside them rather than after
 * the climate tab. Before this the same lists were scattered — GLEIF's at the
 * bottom of FullCheck, MEIP's at the bottom of QuickCheck, EITI's inside the
 * ESG card — and none of them had a URL.
 *
 * These live in `lib/` rather than in App.tsx so they can be tested: the
 * frontend suite is logic-only (no jsdom), so anything worth pinning has to
 * be reachable without rendering a component.
 */

export type CheckMode = "quick" | "full" | "background" | "subsidiaries" | "esg";

export const CHECK_MODES: CheckMode[] = ["quick", "full", "background", "subsidiaries", "esg"];

/** The modes that are a different question rather than a further depth —
 *  the tab strip draws a divider before the first of them. */
export const TOPIC_MODES: ReadonlySet<CheckMode> = new Set<CheckMode>(["subsidiaries", "esg"]);

/**
 * Each mode's accent — the tab's active bar, the glyph colour, the badge ring.
 *
 * Three are the logo's own node colours (`oo.node.*`); Climate & ESG's teal
 * was invented in Phase 122 because the logo has three nodes and a fourth
 * mode had none. Subsidiaries takes `oo.graph.control`, the colour the graph
 * already draws control edges in: the tab lists what this company controls,
 * as FullCheck (ownership blue) follows who owns it. Nothing new invented.
 *
 * This file is the token file for these five values — the design-system
 * lint allows a literal here for the same reason it allows one in
 * `lib/features.ts`: the tab paints its bar with an inline style, so the
 * value has to be a string, and this is the one place it is written.
 */
export const MODE_ACCENT: Record<CheckMode, string> = {
  quick: "#22c55e", // oo.node.green
  full: "#3b82f6", // oo.node.blue
  background: "#7c3aed", // oo.node.purple
  subsidiaries: "#e65100", // oo.graph.control
  esg: "#0d9488", // oo.node.teal
};

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
    case "subsidiaries":
      return "Subsidiaries";
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
