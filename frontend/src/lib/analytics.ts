/**
 * Privacy-respecting analytics — GoatCounter (Phase 89).
 *
 * Rules (from the "Privacy-respecting analytics" ticket):
 *  - Cookie-less, no fingerprinting (GoatCounter's defaults).
 *  - NO LEIs and NO query strings in any recorded path: every pageview goes
 *    through {@link canonicalPath}, which rolls subject-carrying URLs up to
 *    a fixed set of buckets ("/lookup", "/person-check", "/entity", …).
 *    Per-entity search interest comes from Google Search Console instead.
 *  - Feature usage is counted as named events ({@link trackEvent}), never as
 *    subjects.
 *
 * SPA integration: count.js knows nothing about pushState navigation, so we
 * load it with `no_onload`, record the initial view ourselves, and patch
 * history.pushState (+ listen to popstate) to record subsequent views.
 * replaceState is deliberately NOT patched — the /entity/{LEI} deep-link
 * normalisation in App.tsx uses it, and counting it would double-count the
 * initial pageview.
 *
 * The endpoint is baked in: the production SPA is built inside the backend
 * Docker image with no env vars, and the recorded site is only ever
 * opencheck.world (see the hostname guard). A different GoatCounter site
 * code is a one-line change here.
 */

// The GoatCounter site (create at https://www.goatcounter.com — site code
// "opencheck"). Free for non-commercial use; self-hostable later.
export const GOATCOUNTER_ENDPOINT = "https://opencheck.goatcounter.com/count";

/** Hosts where analytics is live. Everything else (localhost, previews) no-ops. */
const LIVE_HOSTS = new Set(["opencheck.world", "www.opencheck.world"]);

type GoatCounter = {
  count: (vars: { path: string; title?: string; event?: boolean }) => void;
  no_onload?: boolean;
};

declare global {
  interface Window {
    goatcounter?: GoatCounter;
  }
}

/**
 * Collapse a browser URL to a subject-free analytics path.
 *
 * Exported for tests — this function is the privacy contract: whatever the
 * address bar holds, the recorded path must never contain an LEI, a person
 * name, or any query string.
 */
export function canonicalPath(pathname: string, search: string): string {
  const params = new URLSearchParams(search);
  if (params.has("person")) return "/person-check";
  if (params.has("lei")) return "/lookup";
  if (pathname.startsWith("/entity")) return "/entity";
  if (pathname.startsWith("/browse")) return "/browse";
  // Known SPA views are already subject-free ("/", "/sources", "/about",
  // "/api", "/changelog"); anything unrecognised rolls up to "/" rather than
  // risk recording a path we did not anticipate.
  const known = new Set(["/", "/sources", "/about", "/api", "/changelog"]);
  return known.has(pathname) ? pathname : "/";
}

function enabled(): boolean {
  return typeof window !== "undefined" && LIVE_HOSTS.has(window.location.hostname);
}

function record(path: string, event = false): void {
  const gc = window.goatcounter;
  if (gc && typeof gc.count === "function") {
    gc.count({ path, event });
  }
}

function pageview(): void {
  record(canonicalPath(window.location.pathname, window.location.search));
}

/**
 * Count a feature event (e.g. "lookup_run", "pdf_export"). Safe to call
 * anywhere: no-ops off the live host or before count.js has loaded — a lost
 * early event is preferable to queueing machinery.
 */
export function trackEvent(name: string): void {
  if (!enabled()) return;
  record(name, true);
}

let initialised = false;

/** Call once from main.tsx. Loads count.js and wires SPA pageview tracking. */
export function initAnalytics(): void {
  if (initialised || !enabled()) return;
  initialised = true;

  // Settings must exist before count.js loads; no_onload because the SPA
  // records its own (canonicalised) pageviews.
  window.goatcounter = { no_onload: true } as GoatCounter;

  const script = document.createElement("script");
  script.async = true;
  script.src = "https://gc.zgo.at/count.js";
  script.dataset.goatcounter = GOATCOUNTER_ENDPOINT;
  script.addEventListener("load", pageview); // the initial view
  document.head.appendChild(script);

  // SPA navigations: our own pushState calls + browser back/forward.
  const origPushState = window.history.pushState.bind(window.history);
  window.history.pushState = (...args: Parameters<History["pushState"]>) => {
    origPushState(...args);
    pageview();
  };
  window.addEventListener("popstate", pageview);
}
