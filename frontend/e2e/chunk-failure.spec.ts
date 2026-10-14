import { expect, test, type Page, type Route } from "@playwright/test";

/**
 * A lazy chunk that fails to load (Phase 331).
 *
 * Found on production on 10 Sept 2026: a deploy landed while a report was
 * open, the old `FullCheckPanel-*.js` answered 502 during the swap, and
 * switching to FullCheck blanked the whole page. The smoke spec next door
 * could never see it — it runs against one build, so the chunk hashes always
 * agree. This file breaks the chunk on purpose, the way a deploy does.
 */

/** BP: the cheapest complete report — bulk BODS, no keys. */
const BP = "213800LH1BZH3DI6G760";

const FULLCHECK_CHUNK = /\/assets\/FullCheckPanel-[^/]+\.js$/;

/** Nothing leaves the machine — the smoke spec's rule. */
async function localOnly(page: Page) {
  await page.route("**/*", (route) => {
    const url = new URL(route.request().url());
    if (url.hostname === "127.0.0.1" || url.hostname === "localhost") return route.continue();
    return route.fulfill({ status: 200, contentType: "text/css", body: "" });
  });
}

/** Route the FullCheck chunk (any query) through `handler`; returns a
 *  reader of how many times the browser asked for it. */
async function onFullCheckChunk(
  page: Page,
  handler: (route: Route, attempt: number) => Promise<void>,
): Promise<() => number> {
  let attempt = 0;
  await page.route(
    (url) => FULLCHECK_CHUNK.test(url.pathname),
    async (route) => {
      attempt += 1;
      await handler(route, attempt);
    },
  );
  return () => attempt;
}

async function openReport(page: Page) {
  await page.goto(`/?lei=${BP}`);
  await expect(page.getByRole("region", { name: "What this check found" })).toBeVisible({
    timeout: 150_000,
  });
}

test("one failed chunk load is retried, and the tab opens", async ({ page }) => {
  await localOnly(page);
  const attempts = await onFullCheckChunk(page, async (route, attempt) => {
    if (attempt === 1) return route.fulfill({ status: 502, body: "Bad Gateway" });
    return route.continue();
  });
  await openReport(page);

  await page.getByRole("tab", { name: /FullCheck/ }).click();
  const panel = page.locator("#panel-full");
  await expect(panel.getByText("Loading FullCheck…")).toHaveCount(0, { timeout: 30_000 });
  await expect(panel.getByRole("alert")).toHaveCount(0);
  expect(attempts(), "the chunk was asked for twice: the 502, then the retry").toBe(2);
});

test("a chunk that will not load breaks only its tab, and says OpenCheck was updated", async ({
  page,
}) => {
  await localOnly(page);
  // The deploy: every request for the old chunk fails, and the server now
  // names a different entry chunk.
  await onFullCheckChunk(page, (route) => route.fulfill({ status: 404, body: "Not Found" }));
  await page.route(
    (url) => url.pathname === "/version.json",
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ entry: "assets/index-NEWBUILD.js" }),
      }),
  );
  await openReport(page);

  await page.getByRole("tab", { name: /FullCheck/ }).click();
  const alert = page.locator("#panel-full").getByRole("alert");
  await expect(alert).toContainText(
    "OpenCheck has been updated since this page was opened, and the FullCheck tab belongs to the new version",
    { timeout: 30_000 },
  );
  await expect(alert.getByRole("button", { name: "Reload the page" })).toBeVisible();

  // The page around it survives: the subject, the verdict, the tabs, the banner.
  await expect(page.locator("h1")).toHaveCount(1);
  await expect(page.getByRole("region", { name: "What this check found" })).toBeVisible();
  await expect(page.getByRole("tablist", { name: "Check mode" })).toBeVisible();
  await expect(page.getByTestId("new-version-banner")).toBeVisible();

  // And another tab still works.
  await page.getByRole("tab", { name: /QuickCheck/ }).click();
  await expect(page.locator("#panel-quick").getByRole("alert")).toHaveCount(0);
});
