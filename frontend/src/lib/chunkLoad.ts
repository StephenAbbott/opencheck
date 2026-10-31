/**
 * Lazy chunks that survive a deploy (Phase 331).
 *
 * Every check mode, the person report and the ownership graph are lazy
 * chunks. A chunk can fail to load for reasons that have nothing to do with
 * the page: a deploy lands while a reader has the report open, the chunk
 * answers 502 during the swap (seen on production, 10 Sept 2026 — FullCheck
 * went blank), a phone drops its connection for a second. Phase 246 put an
 * error boundary round each lazy panel so the failure stays inside it; this
 * module makes the first failure cost nothing.
 *
 * `importWithRetry` tries the import once more after a short pause. The
 * second attempt goes to a cache-busted copy of the chunk's own URL when the
 * browser's error names it: a browser may remember a failed module import
 * and answer the same specifier with the same failure, and a different URL
 * is a different module request. The busted copy is a second instance of
 * that one module; the chunks it imports are shared as before.
 */
import { lazy, type ComponentType, type LazyExoticComponent } from "react";
import { checkForNewVersion } from "./buildVersion";

/** Pause before the second attempt — long enough for a deploy swap to settle. */
export const CHUNK_RETRY_DELAY_MS = 1500;

/** The messages the three browser engines give a module chunk that did not load. */
const CHUNK_ERROR_PATTERNS = [
  /Failed to fetch dynamically imported module/i, // Chromium
  /error loading dynamically imported module/i, // Firefox
  /Importing a module script failed/i, // Safari
  /Unable to preload CSS/i, // Vite's own preload helper
];

/** Whether `error` is a chunk that failed to load, rather than a component
 *  that threw while rendering. The two are worded differently to a reader. */
export function isChunkLoadError(error: unknown): boolean {
  const message =
    error instanceof Error ? error.message : typeof error === "string" ? error : "";
  return CHUNK_ERROR_PATTERNS.some((re) => re.test(message));
}

/** The chunk URL a browser's chunk-load error names, if it names one. */
export function chunkUrlFrom(error: unknown): string | null {
  const message = error instanceof Error ? error.message : "";
  const match = message.match(/https?:\/\/\S+?\.(?:m?js|css)(?:\?\S*)?(?=\s|$)/);
  return match ? match[0] : null;
}

/** `url` with a query that makes it a new module request. */
export function bustedUrl(url: string, token: string | number): string {
  return `${url}${url.includes("?") ? "&" : "?"}retry=${encodeURIComponent(String(token))}`;
}

export interface RetryOptions {
  delayMs?: number;
  /** Injected for tests; the browser's own `import()` otherwise. */
  importUrl?: (url: string) => Promise<unknown>;
  /** Injected for tests. */
  sleep?: (ms: number) => Promise<void>;
  /** Called when both attempts failed. By default it asks the server which
   *  build it serves, so the boundary can say "OpenCheck has been updated". */
  onGiveUp?: (error: unknown) => void;
}

const defaultImport = (url: string): Promise<unknown> =>
  import(/* @vite-ignore */ url);

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Run a dynamic-import factory, and once more if it fails to load.
 *
 * Only a chunk-load failure is retried: a module that loaded and threw while
 * evaluating would throw again, and the boundary should say so at once.
 */
export async function importWithRetry<T>(
  factory: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const {
    delayMs = CHUNK_RETRY_DELAY_MS,
    importUrl = defaultImport,
    sleep = defaultSleep,
    onGiveUp = () => void checkForNewVersion(),
  } = options;
  try {
    return await factory();
  } catch (first) {
    if (!isChunkLoadError(first)) throw first;
    await sleep(delayMs);
    const url = chunkUrlFrom(first);
    try {
      return url ? ((await importUrl(bustedUrl(url, Date.now()))) as T) : await factory();
    } catch (second) {
      onGiveUp?.(second);
      throw second;
    }
  }
}

/** `React.lazy` with `importWithRetry` in front of the factory.
 *
 *  The factory must be a bare `import()` of a module with a default export.
 *  The retry imports the chunk's URL directly, so anything chained onto the
 *  `import()` (`.then((m) => ({ default: m.Page }))`) runs on the first try
 *  only, and the retry renders `undefined` (React error 306). For a named
 *  export use `lazyNamedWithRetry`, which picks the export after the retry. */
export function lazyWithRetry<T extends ComponentType<any>>( // eslint-disable-line @typescript-eslint/no-explicit-any
  factory: () => Promise<{ default: T }>,
  options: RetryOptions = {},
): LazyExoticComponent<T> {
  return lazy(() => importWithRetry(factory, options));
}

/** `importWithRetry`, then the named export as `{ default }` — picked after
 *  the retry, so the second attempt yields the same component as the first. */
export async function importNamedWithRetry<M, K extends keyof M>(
  factory: () => Promise<M>,
  name: K,
  options: RetryOptions = {},
): Promise<{ default: M[K] }> {
  const mod = await importWithRetry(factory, options);
  const component = mod?.[name];
  if (component === undefined) {
    throw new Error(`Lazy chunk has no export named ${String(name)}`);
  }
  return { default: component };
}

/** `lazyWithRetry` for a module whose component is a named export. */
export function lazyNamedWithRetry<
  M extends Record<K, ComponentType<any>>, // eslint-disable-line @typescript-eslint/no-explicit-any
  K extends keyof M,
>(factory: () => Promise<M>, name: K, options: RetryOptions = {}): LazyExoticComponent<M[K]> {
  return lazy(() => importNamedWithRetry(factory, name, options));
}
