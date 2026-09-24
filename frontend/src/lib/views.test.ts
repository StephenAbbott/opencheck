import { describe, expect, it } from "vitest";
import { PAGE_TITLES, VIEW_DOCUMENT_TITLES, pathToView, viewToPath, type View } from "./views";

const VIEWS: View[] = [
  "main",
  "sources",
  "behind",
  "api",
  "changelog",
  "batch",
  "watchlist",
  "features",
];

describe("views", () => {
  it("round-trips every view through its path", () => {
    for (const v of VIEWS) expect(pathToView(viewToPath(v))).toBe(v);
  });

  it("keeps /about as the address of the page called behind", () => {
    expect(viewToPath("behind")).toBe("/about");
  });

  it("falls through to the main view for anything else, a report included", () => {
    expect(pathToView("/report/abc123")).toBe("main");
    expect(pathToView("/entity/213800LH1BZH3DI6G760-bp")).toBe("main");
  });

  it("gives every view but main a document title, and every page title a view", () => {
    for (const v of VIEWS.filter((x) => x !== "main")) {
      expect(VIEW_DOCUMENT_TITLES[v as Exclude<View, "main">]).toMatch(/— OpenCheck$/);
    }
    for (const v of Object.keys(PAGE_TITLES)) expect(VIEWS).toContain(v);
  });
});
