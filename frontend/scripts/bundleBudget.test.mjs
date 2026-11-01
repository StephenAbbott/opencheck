import { describe, expect, it } from "vitest";

import {
  brotliSize,
  budgetFor,
  criticalAssets,
  judge,
  STALE_SLACK,
  updateAllowed,
} from "./bundleBudget.mjs";

const BUILT_INDEX = `<!doctype html><html><head>
<link rel="icon" href="/favicon.svg" type="image/svg+xml" />
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bitter" />
<script type="module" crossorigin src="/assets/index-CYeJ4crf.js"></script>
<link rel="modulepreload" crossorigin href="/assets/jsx-runtime-CY_jTiqj.js">
<link rel="modulepreload" crossorigin href="/assets/ui-DD0rKdl9.js">
<link rel="stylesheet" crossorigin href="/assets/index-BoLehO8Y.css">
<script async src="https://gc.zgo.at/count.js"></script>
</head><body><div id="root"></div></body></html>`;

describe("criticalAssets", () => {
  it("lists the entry, its preloads and the stylesheet, in page order", () => {
    expect(criticalAssets(BUILT_INDEX)).toEqual([
      "assets/index-CYeJ4crf.js",
      "assets/jsx-runtime-CY_jTiqj.js",
      "assets/ui-DD0rKdl9.js",
      "assets/index-BoLehO8Y.css",
    ]);
  });

  it("leaves out icons, preconnects and off-site files", () => {
    const listed = criticalAssets(BUILT_INDEX).join(" ");
    expect(listed).not.toMatch(/favicon|googleapis|zgo/);
  });

  it("counts a file named twice once", () => {
    const twice = `<script type="module" src="/assets/a.js"></script><link rel="modulepreload" href="/assets/a.js">`;
    expect(criticalAssets(twice)).toEqual(["assets/a.js"]);
  });
});

describe("judge", () => {
  it("fails a measurement over the budget and says by how much", () => {
    expect(judge(110_000, 100_000)).toEqual({ status: "over", by: 10_000 });
  });

  it("passes one inside it", () => {
    expect(judge(95_000, 100_000).status).toBe("ok");
  });

  it("fails a budget left far above the measurement, so a gain is locked in", () => {
    const measured = Math.floor(100_000 * (1 - STALE_SLACK)) - 1;
    expect(judge(measured, 100_000).status).toBe("stale");
  });
});

describe("budgetFor / updateAllowed", () => {
  it("leaves headroom above the measurement, rounded up to a whole KB", () => {
    const budget = budgetFor(100_000);
    expect(budget).toBeGreaterThan(100_000);
    expect(budget % 1024).toBe(0);
    expect(judge(100_000, budget).status).toBe("ok");
  });

  it("will not raise a budget unless asked to", () => {
    expect(updateAllowed(100_000, 90_000, false)).toBe(true);
    expect(updateAllowed(100_000, 120_000, false)).toBe(false);
    expect(updateAllowed(100_000, 120_000, true)).toBe(true);
    expect(updateAllowed(undefined, 120_000, false)).toBe(true);
  });
});

describe("brotliSize", () => {
  it("measures compressed bytes, which repetition shrinks", () => {
    const text = Buffer.from("const a = 1;\n".repeat(1000));
    expect(brotliSize(text)).toBeLessThan(text.length / 10);
  });
});
