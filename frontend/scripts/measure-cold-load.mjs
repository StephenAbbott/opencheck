#!/usr/bin/env node
/**
 * How long a first-time visitor waits (the "Speed up first load" ticket,
 * 31 Oct 2026).
 *
 *   node scripts/measure-cold-load.mjs [url] [--runs N]
 *
 * Opens the page in headless Chromium with the browser cache disabled, once
 * on an unthrottled connection and once on simulated slow 4G (150 ms
 * latency, 1.6 Mbps down, 750 kbps up), and prints time to first byte,
 * first and largest contentful paint, the load event, what was transferred,
 * and the largest resources. Each figure is the median of `--runs` loads
 * (default 3). Run it before and after a change and paste both tables into
 * the ticket. `PLAYWRIGHT_CHROMIUM_PATH` points it at a browser Playwright
 * did not download.
 */
import { chromium } from "@playwright/test";

const args = process.argv.slice(2);
const url = args.find((a) => !a.startsWith("--")) ?? "https://opencheck.world/";
const runsAt = args.indexOf("--runs");
const runs = runsAt >= 0 ? Number(args[runsAt + 1]) : 3;

const PROFILES = {
  unthrottled: null,
  "slow 4G": { latency: 150, downloadThroughput: (1.6 * 1024 * 1024) / 8, uploadThroughput: (750 * 1024) / 8 },
};

const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
};

async function once(browser, throttle) {
  const context = await browser.newContext();
  const page = await context.newPage();
  const cdp = await context.newCDPSession(page);
  await cdp.send("Network.enable");
  await cdp.send("Network.setCacheDisabled", { cacheDisabled: true });
  if (throttle) await cdp.send("Network.emulateNetworkConditions", { offline: false, ...throttle });
  await page.goto(url, { waitUntil: "load", timeout: 120_000 });
  await page.waitForLoadState("networkidle", { timeout: 120_000 }).catch(() => {});
  const m = await page.evaluate(async () => {
    const nav = performance.getEntriesByType("navigation")[0];
    const fcp = performance.getEntriesByName("first-contentful-paint")[0];
    const lcp = await new Promise((resolve) => {
      new PerformanceObserver((list) => {
        const e = list.getEntries();
        resolve(e[e.length - 1].startTime);
      }).observe({ type: "largest-contentful-paint", buffered: true });
      setTimeout(() => resolve(null), 1500);
    });
    const res = performance.getEntriesByType("resource");
    return {
      ttfb: nav.responseStart,
      fcp: fcp ? fcp.startTime : null,
      lcp,
      load: nav.loadEventEnd,
      transferKb: (res.reduce((a, r) => a + r.transferSize, 0) + nav.transferSize) / 1024,
      requests: res.length + 1,
      largest: res
        .sort((a, b) => b.transferSize - a.transferSize)
        .slice(0, 5)
        .map((r) => `${Math.round(r.transferSize / 1024)} KB ${new URL(r.name).pathname}`),
    };
  });
  await context.close();
  return m;
}

const browser = await chromium.launch(
  process.env.PLAYWRIGHT_CHROMIUM_PATH ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_PATH } : {},
);
console.log(`Cold load of ${url} — median of ${runs} runs, browser cache disabled\n`);
console.log("| Profile | TTFB | FCP | LCP | Load | Transferred | Requests |");
console.log("|---|---|---|---|---|---|---|");
let largest = [];
for (const [name, throttle] of Object.entries(PROFILES)) {
  const all = [];
  for (let i = 0; i < runs; i++) all.push(await once(browser, throttle));
  const ms = (k) => `${(median(all.map((r) => r[k] ?? NaN)) / 1000).toFixed(2)} s`;
  console.log(
    `| ${name} | ${ms("ttfb")} | ${ms("fcp")} | ${ms("lcp")} | ${ms("load")} | ` +
      `${Math.round(median(all.map((r) => r.transferKb)))} KB | ${median(all.map((r) => r.requests))} |`,
  );
  largest = all[0].largest;
}
console.log(`\nLargest resources: ${largest.join(" · ")}`);
await browser.close();
