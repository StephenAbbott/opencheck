import { defineConfig, devices } from "@playwright/test";

/**
 * The smoke tier (Phase 168).
 *
 * The vitest suite next door is fast and runs on every push; this one starts
 * a real backend and a production build of the SPA, and exists for the
 * failures that only appear once a whole page is on screen — the v1/v2
 * component mix, the verdict rendered twice, the mode tabs overflowing their
 * strip at phone width. Each of those reached production past a green suite.
 *
 * It is deliberately offline: the backend runs with no API keys, so every
 * adapter serves its stub or the committed Open Ownership bulk BODS data,
 * which is exactly what the curated examples on the homepage use. No network,
 * no rate limits, no upstream blip failing a merge.
 */
const BACKEND = "http://127.0.0.1:8000";
const FRONTEND = "http://127.0.0.1:4173";

export default defineConfig({
  testDir: "./e2e",
  // A curated lookup walks eight adapters and the whole BODS mapping; on a
  // cold cache in CI that is not a five-second page.
  timeout: 180_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [["github"], ["list"]] : [["list"]],
  use: {
    baseURL: FRONTEND,
    // Phase 144 refuses declared automated clients on /lookup-stream, and
    // headless Chromium announces itself as HeadlessChrome — so the report
    // page would 403 and the smoke would "find" a bug that only it has. This
    // is the one place a real browser's UA has to be stated rather than
    // inherited.
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        // Escape hatch for environments that ship a browser rather than
        // download one — an offline CI image, a sandbox, a distro package.
        // Unset (the normal case, including GitHub Actions after
        // `playwright install chromium`) Playwright uses its own build.
        ...(process.env.PLAYWRIGHT_CHROMIUM_PATH
          ? { launchOptions: { executablePath: process.env.PLAYWRIGHT_CHROMIUM_PATH } }
          : {}),
      },
    },
  ],
  webServer: [
    {
      command: "uv run uvicorn opencheck.app:app --host 127.0.0.1 --port 8000",
      cwd: "../backend",
      url: `${BACKEND}/health`,
      timeout: 180_000,
      reuseExistingServer: !process.env.CI,
      env: { OPENCHECK_CORS_ORIGIN: "*" },
    },
    {
      // Built, not `vite dev`: the dev server proxies /lookup to the backend,
      // and a proxy is exactly the thing production does not have. The base
      // URL is baked in at build time, so the build belongs to this command.
      command: "npm run build && npm run preview",
      url: FRONTEND,
      timeout: 180_000,
      reuseExistingServer: !process.env.CI,
      env: { VITE_API_BASE_URL: BACKEND },
    },
  ],
});
