/**
 * Knowing that the page is an old build (Phase 331).
 *
 * The production build writes `version.json` beside `index.html` naming its
 * entry chunk (`assets/index-<hash>.js`; the Vite plugin in
 * `vite.config.ts`). The entry's hash covers the names of every chunk it can
 * load, so two builds with the same entry can load the same chunks and two
 * with different entries may not. A page compares the entry it was served
 * with the one the server names now; when they differ, OpenCheck has been
 * deployed since the page was opened and any lazy chunk it has not yet
 * loaded may no longer be the one it asks for. The page then says so, with
 * a reload — before a tab goes blank, not after.
 *
 * Checked when the tab comes back into view, every `VERSION_CHECK_INTERVAL_MS`
 * while it is visible, and at once when a chunk fails to load. Nothing is
 * checked in development (there is no `version.json`), and a failed check
 * says nothing: an unreachable file is not news about the build.
 *
 * The state is module-scoped, like the "as filed" toggle: one page, one
 * answer, read by the banner and by every panel boundary.
 */

export const VERSION_PATH = "/version.json";
export const VERSION_CHECK_INTERVAL_MS = 10 * 60 * 1000;

/** The entry chunk this page was served with, as a path without its leading
 *  slash (`assets/index-AbC123.js`), or null outside a production build. */
export function currentEntry(doc: Pick<Document, "querySelector"> | null = globalThis.document ?? null): string | null {
  const script = doc?.querySelector<HTMLScriptElement>('script[type="module"][src*="/assets/"]');
  const src = script?.getAttribute("src");
  if (!src) return null;
  try {
    return new URL(src, "http://x").pathname.replace(/^\//, "");
  } catch {
    return null;
  }
}

/** The entry `version.json` names, or null when it cannot be read. */
export function entryFromManifest(body: unknown): string | null {
  if (!body || typeof body !== "object") return null;
  const entry = (body as { entry?: unknown }).entry;
  return typeof entry === "string" && entry.length > 0 ? entry.replace(/^\//, "") : null;
}

/** Whether the server now serves a different build. Unknown on either side
 *  is "no": the banner asserts a deploy, so it needs both entries. */
export function isNewerBuild(current: string | null, served: string | null): boolean {
  return Boolean(current && served && current !== served);
}

// ── the module-scoped state ───────────────────────────────────────────

let newVersion = false;
const listeners = new Set<() => void>();

export function getNewVersionAvailable(): boolean {
  return newVersion;
}

export function subscribeNewVersion(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function markNewVersion(): void {
  if (newVersion) return;
  newVersion = true;
  listeners.forEach((l) => l());
}

/** Tests only: forget what has been seen. */
export function resetBuildVersionForTests(): void {
  newVersion = false;
  listeners.clear();
}

type Fetcher = (url: string, init?: RequestInit) => Promise<Pick<Response, "ok" | "json">>;

/**
 * Ask the server which build it serves, and remember if it is a newer one.
 * Resolves to whether a newer build is known. Never throws.
 */
export async function checkForNewVersion(
  fetcher: Fetcher = (url, init) => fetch(url, init),
  current: string | null = currentEntry(),
): Promise<boolean> {
  if (newVersion) return true;
  if (!current) return false;
  try {
    const res = await fetcher(`${VERSION_PATH}?t=${Date.now()}`, { cache: "no-store" });
    if (!res.ok) return false;
    if (isNewerBuild(current, entryFromManifest(await res.json()))) markNewVersion();
  } catch {
    // Offline, or the file is unreadable: say nothing.
  }
  return newVersion;
}

/**
 * Start checking: on return to the tab and on an interval while visible.
 * Returns the cleanup. Does nothing outside a production build.
 */
export function startVersionWatch(
  win: Window = window,
  check: () => Promise<boolean> = () => checkForNewVersion(),
): () => void {
  if (!currentEntry(win.document)) return () => {};
  const onVisible = () => {
    if (win.document.visibilityState === "visible") void check();
  };
  win.document.addEventListener("visibilitychange", onVisible);
  const timer = win.setInterval(() => {
    if (win.document.visibilityState === "visible") void check();
  }, VERSION_CHECK_INTERVAL_MS);
  return () => {
    win.document.removeEventListener("visibilitychange", onVisible);
    win.clearInterval(timer);
  };
}
