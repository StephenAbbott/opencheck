/**
 * The app's top-level views and their addresses.
 *
 * Moved out of `App()` in Phase 246, where they were redeclared on every
 * render. Path → view mapping: /sources, /about and the rest are real URLs;
 * everything else falls through to "main" (the SPA rewrite in render.yaml
 * serves index.html for all paths so deep links work). A report is "main"
 * with `?lei=` (or `/report/{id}` for a saved one), never a view of its own.
 */

export type View =
  | "main"
  | "sources"
  | "behind"
  | "api"
  | "changelog"
  | "batch"
  | "watchlist"
  | "features";

/** Views that already render a heading of their own: `main` has the hero, or
 *  the report's sr-only heading once a lookup is on screen, and `batch` has
 *  BatchPage's "Screen a list". */
export type SelfTitledView = "main" | "batch" | "watchlist";

/** The page title every other view puts in the document outline. The
 *  `Exclude` is the point: a new view cannot be added without either giving
 *  it a title here or declaring that it titles itself. */
export const PAGE_TITLES: Record<Exclude<View, SelfTitledView>, string> = {
  sources: "The sources OpenCheck queries",
  features: "What OpenCheck can do",
  behind: "About OpenCheck",
  api: "The OpenCheck API",
  changelog: "OpenCheck development history",
};

/**
 * The header nav, constant across every view (Phase 122). One label per
 * destination: v1 called the same page "Behind the scenes →", "About",
 * "How it works →" and "Behind the Scenes" depending on where you met it,
 * and replaced the whole nav with "← Back" on every sub-page, so /api and
 * /sources were unreachable from each other.
 *
 * No "Search" item. It did exactly what the logo beside it does — go to the
 * homepage — and now that the header carries a real search field, a nav link
 * labelled "Search" that is not the search field is a third thing pointing at
 * two behaviours. The logo remains the way home.
 */
export const NAV_ITEMS: { view: View; label: string }[] = [
  { view: "sources", label: "Sources" },
  { view: "api", label: "API" },
  // Phase 175: "About" gave the top nav its slot to the page that explains
  // the architecture; a first-time visitor wants to know what the thing
  // DOES before how it is built. /about keeps its URL and its footer link.
  { view: "features", label: "Features" },
];

export function pathToView(path: string): View {
  if (path === "/sources") return "sources";
  if (path === "/features") return "features";
  if (path === "/about") return "behind";
  if (path === "/api") return "api";
  if (path === "/changelog") return "changelog";
  if (path === "/batch") return "batch";
  if (path === "/watchlist") return "watchlist";
  return "main";
}

export function viewToPath(v: View): string {
  if (v === "sources") return "/sources";
  if (v === "features") return "/features";
  if (v === "behind") return "/about";
  if (v === "api") return "/api";
  if (v === "changelog") return "/changelog";
  if (v === "batch") return "/batch";
  if (v === "watchlist") return "/watchlist";
  return "/";
}

/** `document.title` for every view but a report, whose title names the
 *  subject (`documentTitleFor` in lib/checkMode.ts). */
export const VIEW_DOCUMENT_TITLES: Record<Exclude<View, "main">, string> = {
  sources: "Data Sources — OpenCheck",
  features: "Features — OpenCheck",
  behind: "Behind the Scenes — OpenCheck",
  api: "API — OpenCheck",
  changelog: "Changelog — OpenCheck",
  batch: "Screen a list — OpenCheck",
  watchlist: "Watchlist — OpenCheck",
};
