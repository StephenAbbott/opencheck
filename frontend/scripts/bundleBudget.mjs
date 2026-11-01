/**
 * What a first visit downloads before the homepage can draw, measured the way
 * the browser receives it (the "Speed up first load" ticket, 1 Nov 2026).
 *
 * The page cannot paint until the entry chunk, every `modulepreload` it
 * declares and the stylesheet have arrived and run. On 31 Oct 2026 the entry
 * alone was 1,434 KB (495 KB brotli), because the changelog's 882 KB status
 * table and seven other pages had been imported statically; nothing failed,
 * nothing warned, and every first visit waited for it. This is the check that
 * would have said so. It reads `dist/index.html` for the list, so a chunk the
 * build starts preloading is counted the day it appears.
 *
 * Pure functions here; `check-bundle-budget.mjs` reads the files and writes
 * the budget, and `bundleBudget.test.mjs` pins the rules.
 */
import { brotliCompressSync, constants } from "node:zlib";

/** Headroom `--update` leaves above the measured size, so an ordinary edit
 *  does not fail the build. */
export const HEADROOM = 0.1;

/** A budget more than this far above the measured size is stale: the gain is
 *  locked in with `--update`, or it can be spent back without anyone noticing. */
export const STALE_SLACK = 0.25;

/** The files `index.html` makes the browser fetch before first paint, as
 *  paths under `dist/` — the module entry, its preloads and the stylesheets.
 *  Off-site URLs (fonts, analytics) are not the build's to budget. */
export function criticalAssets(indexHtml) {
  const found = [];
  const tag = /<(script|link)\b[^>]*>/gi;
  for (const [element] of indexHtml.matchAll(tag)) {
    const attr = (name) => element.match(new RegExp(`\\b${name}=["']([^"']+)["']`, "i"))?.[1];
    const isScript = /^<script/i.test(element);
    const rel = attr("rel");
    const url = isScript ? attr("src") : attr("href");
    if (!url || /^(https?:)?\/\//i.test(url)) continue;
    if (isScript ? attr("type") !== "module" : rel !== "modulepreload" && rel !== "stylesheet") {
      continue;
    }
    const path = url.replace(/^\//, "").split(/[?#]/)[0];
    if (!found.includes(path)) found.push(path);
  }
  return found;
}

/** Bytes after brotli at the highest quality, which is what the CDN serves. */
export function brotliSize(bytes) {
  return brotliCompressSync(bytes, {
    params: { [constants.BROTLI_PARAM_QUALITY]: constants.BROTLI_MAX_QUALITY },
  }).length;
}

/** The budget `--update` writes for a measured size. */
export function budgetFor(measured) {
  return Math.ceil((measured * (1 + HEADROOM)) / 1024) * 1024;
}

/**
 * The verdict on one measurement.
 *
 * `over` fails the build. `stale` fails it too, so a gain is recorded rather
 * than quietly available to spend (the design-lint ratchet's rule).
 */
export function judge(measured, budget) {
  if (measured > budget) return { status: "over", by: measured - budget };
  if (measured < budget * (1 - STALE_SLACK)) return { status: "stale", by: budget - measured };
  return { status: "ok", by: budget - measured };
}

/** Whether `--update` may write `next` over `current` without `--allow-increase`. */
export function updateAllowed(current, next, allowIncrease) {
  return allowIncrease || current === undefined || next <= current;
}

export function kb(bytes) {
  return `${(bytes / 1024).toFixed(1)} KB`;
}
