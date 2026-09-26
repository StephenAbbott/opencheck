import { expect, test, type ConsoleMessage, type Page } from "@playwright/test";
import { CHECK_MODES } from "../src/lib/checkMode";

/**
 * Six pages and a report (Phase 168).
 *
 * The vitest suite could not have caught any of the three failures that made
 * the case for this file, because none of them was a wrong value:
 *
 *  - **the v1/v2 mix** — two design-system generations on one page;
 *  - **the double verdict** — one sentence, rendered twice;
 *  - **the tab overflow** — four modes in a strip that fits three, at 390px.
 *
 * All three are properties of a rendered page: how many of a thing there
 * are, and whether it fits. So the assertions below are mostly counting and
 * measuring, and they run over every route a reader can reach plus one
 * curated report. What each page *says* is asserted next door, in the unit
 * and component suites, where a failure names the sentence rather than the
 * screenshot.
 */

/** The homepage examples; BP is the cheapest complete one — bulk BODS, no keys. */
const BP = "213800LH1BZH3DI6G760";

/**
 * Nothing leaves the machine. The page asks Google Fonts for three families
 * and that is the only third party it talks to; letting the request through
 * makes the run depend on a CDN — closed in a sandbox, slow on a bad day —
 * to prove something about fonts that this file does not test.
 *
 * Answered empty rather than aborted: an aborted request is itself a console
 * error, which would have made the console assertion below assert the mock.
 */
test.beforeEach(async ({ page }) => {
  await page.route("**/*", (route) => {
    const url = new URL(route.request().url());
    if (url.hostname === "127.0.0.1" || url.hostname === "localhost") return route.continue();
    return route.fulfill({ status: 200, contentType: "text/css", body: "" });
  });
});

const ROUTES = [
  { path: "/", name: "home" },
  { path: "/sources", name: "sources" },
  { path: "/features", name: "features" },
  { path: "/about", name: "about" },
  { path: "/api", name: "api" },
  { path: "/changelog", name: "changelog" },
  { path: "/batch", name: "batch" },
] as const;

/**
 * Console noise we accept. Deliberately short: an allowlist that grows
 * without argument is how a console check stops meaning anything.
 */
const IGNORED_CONSOLE = [
  /favicon/i,
  /Download the React DevTools/i,
  // The graph measures itself on a hidden tab and Cytoscape says so.
  /has no size|container has no/i,
];

function watchConsole(page: Page): string[] {
  const errors: string[] = [];
  page.on("console", (msg: ConsoleMessage) => {
    if (msg.type() !== "error") return;
    const text = msg.text();
    if (!IGNORED_CONSOLE.some((re) => re.test(text))) errors.push(text);
  });
  page.on("pageerror", (err) => errors.push(`pageerror: ${err.message}`));
  return errors;
}

/** Nothing may stick out sideways: the whole class the tab overflow belongs to. */
async function expectNoHorizontalOverflow(page: Page) {
  const overflow = await page.evaluate(() => {
    const doc = document.documentElement;
    // 1px of tolerance for sub-pixel layout, and report the widest offender
    // so a failure says which element to look at rather than just "wider".
    if (doc.scrollWidth <= window.innerWidth + 1) return null;
    let worst: { tag: string; right: number } | null = null;
    for (const el of document.body.querySelectorAll<HTMLElement>("*")) {
      const right = el.getBoundingClientRect().right;
      if (right > window.innerWidth + 1 && (!worst || right > worst.right)) {
        worst = {
          tag: `${el.tagName.toLowerCase()}.${(el.className || "").toString().split(" ")[0]}`,
          right,
        };
      }
    }
    return { scrollWidth: doc.scrollWidth, innerWidth: window.innerWidth, worst };
  });
  expect(overflow, "page scrolls sideways").toBeNull();
}

test.describe("every page a reader can reach", () => {
  for (const route of ROUTES) {
    test(`${route.name} renders, once, without console errors`, async ({ page }) => {
      const errors = watchConsole(page);
      await page.goto(route.path);

      // One first-level heading per page: the outline is a decision, and a
      // second <h1> is how two page templates end up stacked on one route.
      await expect(page.locator("h1")).toHaveCount(1);
      await expect(page.locator("header")).toHaveCount(1);
      await expect(page.getByRole("contentinfo")).toHaveCount(1);
      await expectNoHorizontalOverflow(page);
      expect(errors, `console errors on ${route.path}`).toEqual([]);
    });
  }

  test("the routes are real URLs, not just in-page state", async ({ page }) => {
    // Deep-linking and the back button are the same mechanism; the nav
    // pushes history, so a reload of /sources has to serve /sources.
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "About the sources" })).toBeVisible();
    await page.goto("/");
    await page.getByRole("link", { name: "Sources", exact: true }).first().click();
    await expect(page).toHaveURL(/\/sources$/);
    await page.goBack();
    await expect(page).toHaveURL(/\/$/);
  });
});

test("the sources page carries the weekly sweep's verdict", async ({ page }) => {
  await page.goto("/sources");
  const cards = page.getByRole("listitem");
  await expect(cards.first()).toBeVisible();
  expect(await cards.count()).toBeGreaterThan(30);

  // Health is read from a release asset the deployment may or may not reach.
  // When it is there, every card the sweep knows carries a verdict and a
  // disclosure; when it is not, the catalogue renders exactly as before —
  // both are correct, so the smoke asserts the pair rather than one of them.
  const strip = page.getByText(/Last sweep ·/);
  if (await strip.count()) {
    await expect(strip.first()).toBeVisible();
    const details = page.getByRole("button", { name: /Health details/ });
    expect(await details.count()).toBeGreaterThan(0);
    const first = details.first();
    const before = await first.getAttribute("aria-expanded");
    await first.click();
    await expect(first).toHaveAttribute("aria-expanded", before === "true" ? "false" : "true");
  } else {
    await expect(page.getByRole("button", { name: /Health details/ })).toHaveCount(0);
  }
});

test("a curated report answers once, and says so at the top", async ({ page }) => {
  const errors = watchConsole(page);
  await page.goto(`/?lei=${BP}`);

  const verdict = page.getByRole("region", { name: "What this check found" });
  await expect(verdict).toBeVisible({ timeout: 150_000 });

  // The double verdict, pinned where it happened. One strip, one subject.
  await expect(verdict).toHaveCount(1);
  await expect(page.getByRole("heading", { name: "What we found" })).toHaveCount(1);
  await expect(page.getByRole("heading", { name: "Coverage" })).toHaveCount(1);
  await expect(page.locator("h1")).toHaveCount(1);

  await expect(page.getByText(/BP P\.L\.C\./i).first()).toBeVisible();
  await expect(page.getByRole("tablist", { name: "Check mode" })).toHaveCount(1);

  // Coverage is a claim about the registry, not about this run's luck: it
  // must name a denominator, never "10 of 10" alone (Phase 156).
  await expect(verdict.getByText(/sources? (apply|answered)/).first()).toBeVisible();

  await expectNoHorizontalOverflow(page);
  expect(errors, "console errors on a curated report").toEqual([]);
});

test.describe("the report at phone width", () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test("shows every check mode without a sideways scroll", async ({ page }) => {
    await page.goto(`/?lei=${BP}`);
    await expect(page.getByRole("region", { name: "What this check found" })).toBeVisible({
      timeout: 150_000,
    });

    const tabs = page.getByRole("tablist", { name: "Check mode" }).getByRole("tab");
    // Five since Phase 185 (Subsidiaries). Read the count from the strip's
    // own source of truth rather than restating it: CHECK_MODES is what
    // App.tsx renders, so a sixth mode moves this assertion by itself.
    await expect(tabs).toHaveCount(CHECK_MODES.length);

    // The failure was not that the tabs were missing — it was that two of
    // them sat outside the viewport, so they did not exist unless you knew
    // to swipe. Phase 157 stacked them into a 2-column grid below `sm` (an
    // odd fifth cell spans the last row, Phase 185); this measures the
    // outcome rather than the class names.
    for (const tab of await tabs.all()) {
      await expect(tab).toBeVisible();
      const box = (await tab.boundingBox())!;
      expect(box.x, "a tab starts off-screen").toBeGreaterThanOrEqual(-1);
      expect(box.x + box.width, "a tab ends off-screen").toBeLessThanOrEqual(391);
      // Phase 157 asked for 56px targets; anything under 44 fails WCAG 2.5.5.
      expect(box.height, "tab target too small to hit").toBeGreaterThanOrEqual(44);
    }

    await expectNoHorizontalOverflow(page);
  });

  test("puts the mode tabs directly under the subject, above the verdict (Phase 245)", async ({ page }) => {
    await page.goto(`/?lei=${BP}`);
    const verdict = page.getByRole("region", { name: "What this check found" });
    await expect(verdict).toBeVisible({ timeout: 150_000 });
    const tabsBox = (await page.getByRole("tablist", { name: "Check mode" }).boundingBox())!;
    const verdictBox = (await verdict.boundingBox())!;
    // The Opus 5.5 check measured the tabs ~1,290px down Shell at 390px,
    // under every verdict column. Measured here as an order, and against the
    // first screen, rather than as class names.
    expect(tabsBox.y, "the tabs sit below the verdict").toBeLessThan(verdictBox.y);
    expect(tabsBox.y, "the tabs start below the first screen").toBeLessThan(844);
    // One search entry point on a report: the header's, not a row above the subject.
    await expect(page.getByRole("button", { name: "Search for another company or person" })).toBeVisible();
    await expect(page.getByText("More search options — national ID, person name")).toHaveCount(0);
  });
});

test("at 1280×800 the mode tabs are on the first screen (Phase 245)", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto(`/?lei=${BP}`);
  await expect(page.getByRole("region", { name: "What this check found" })).toBeVisible({
    timeout: 150_000,
  });
  const box = (await page.getByRole("tablist", { name: "Check mode" }).boundingBox())!;
  // Whole strip in view, not just its top edge.
  expect(box.y + box.height, "the tabs end below the first screen").toBeLessThanOrEqual(800);
  await expect(page.getByRole("button", { name: "More search options" })).toBeVisible();
});

test.describe("every page at 320px (Phase 241)", () => {
  // WCAG 1.4.10: content reflows at 320 CSS px. The Opus 5.5 check found
  // /changelog, /api and /about scrolling sideways there, and the header nav
  // clipped to "Fea" — invisible to the overflow check above, because the
  // header's `overflow-hidden` swallowed it while the link stayed focusable.
  test.use({ viewport: { width: 320, height: 720 } });

  for (const route of ROUTES) {
    test(`${route.name} reflows without a sideways scroll`, async ({ page }) => {
      await page.goto(route.path);
      await expect(page.locator("h1")).toHaveCount(1);
      await expectNoHorizontalOverflow(page);
      // Every nav item is wholly on screen, not clipped by the header.
      const items = page.getByRole("navigation", { name: "Site navigation" }).getByRole("button");
      expect(await items.count()).toBeGreaterThan(0);
      for (const item of await items.all()) {
        const box = (await item.boundingBox())!;
        expect(box.x, "a nav item starts off-screen").toBeGreaterThanOrEqual(0);
        expect(box.x + box.width, "a nav item is clipped").toBeLessThanOrEqual(320);
      }
    });
  }
});

test.describe("the header is one row on a phone report (Phase 251)", () => {
  // On an iPhone (390px) a report page's header broke onto two lines: the mark
  // on one, the search icon and the three links on the next. 360px is the
  // narrowest common phone; below it (320) the row may wrap, and the Phase 241
  // test above holds it to wrapping rather than clipping.
  test.use({ viewport: { width: 360, height: 800 } });

  test("mark, search icon and every nav link share one line", async ({ page }) => {
    await page.goto(`/?lei=${BP}`);
    await expect(page.getByRole("region", { name: "What this check found" })).toBeVisible({
      timeout: 150_000,
    });
    const banner = page.getByRole("banner");
    const mark = banner.getByRole("button", { name: "OpenCheck — back to homepage" });
    const items = banner.getByRole("navigation", { name: "Site navigation" }).getByRole("button");
    await expect(
      items.filter({ has: page.locator("svg") }),
      "the report page shows the search icon",
    ).toHaveCount(1);
    const markBox = (await mark.boundingBox())!;
    const markMid = markBox.y + markBox.height / 2;
    for (const item of await items.all()) {
      const box = (await item.boundingBox())!;
      expect(
        Math.abs(box.y + box.height / 2 - markMid),
        `"${(await item.getAttribute("aria-label")) ?? (await item.textContent())}" wrapped below the mark`,
      ).toBeLessThanOrEqual(4);
      expect(box.x + box.width, "a nav item is clipped").toBeLessThanOrEqual(360);
    }
  });
});

test("the arrow keys walk every check mode (Phase 241)", async ({ page }) => {
  await page.goto(`/?lei=${BP}`);
  await expect(page.getByRole("region", { name: "What this check found" })).toBeVisible({
    timeout: 150_000,
  });
  const tablist = page.getByRole("tablist", { name: "Check mode" });
  await tablist.getByRole("tab", { selected: true }).focus();
  // Before Phase 241 focus moved into the panel after the first →, so the
  // second press did nothing: a keyboard user moved one tab at a time.
  for (let i = 1; i < CHECK_MODES.length; i++) {
    await page.keyboard.press("ArrowRight");
    await expect(tablist.getByRole("tab", { selected: true })).toHaveAttribute("id", `tab-${CHECK_MODES[i]}`);
    await expect(page.locator(`#tab-${CHECK_MODES[i]}`)).toBeFocused();
  }
  await page.keyboard.press("Home");
  await expect(page.locator(`#tab-${CHECK_MODES[0]}`)).toBeFocused();
  await page.keyboard.press("End");
  await expect(page.locator(`#tab-${CHECK_MODES[CHECK_MODES.length - 1]}`)).toBeFocused();
});
