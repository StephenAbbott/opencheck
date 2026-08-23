import { describe, expect, it } from "vitest";
import { nextMenuIndex } from "./ExportMenu";

describe("nextMenuIndex", () => {
  // Share, PDF, Markdown, Download data. The share item is optional, so the
  // menu can also be 3 — which is why roving focus counts the items that
  // exist rather than a constant.
  const COUNT = 4;

  it("moves down and wraps", () => {
    expect(nextMenuIndex(0, "ArrowDown", COUNT)).toBe(1);
    expect(nextMenuIndex(2, "ArrowDown", COUNT)).toBe(3);
    expect(nextMenuIndex(3, "ArrowDown", COUNT)).toBe(0);
  });

  it("moves up and wraps", () => {
    expect(nextMenuIndex(3, "ArrowUp", COUNT)).toBe(2);
    expect(nextMenuIndex(0, "ArrowUp", COUNT)).toBe(3);
  });

  it("Home and End jump to the edges", () => {
    expect(nextMenuIndex(1, "Home", COUNT)).toBe(0);
    expect(nextMenuIndex(1, "End", COUNT)).toBe(3);
  });

  it("recovers when nothing is focused yet (current = -1)", () => {
    expect(nextMenuIndex(-1, "ArrowDown", COUNT)).toBe(0);
    expect(nextMenuIndex(-1, "ArrowUp", COUNT)).toBe(2); // wraps from -1
  });

  it("works for the share-less menu too", () => {
    // Surfaces with no link to copy render three items; the same helper has
    // to wrap at three, which is why the count is a parameter and not a
    // module constant.
    expect(nextMenuIndex(2, "ArrowDown", 3)).toBe(0);
    expect(nextMenuIndex(0, "ArrowUp", 3)).toBe(2);
    expect(nextMenuIndex(1, "End", 3)).toBe(2);
  });

  it("ignores non-navigation keys and empty menus", () => {
    expect(nextMenuIndex(0, "Enter", COUNT)).toBeNull();
    expect(nextMenuIndex(0, "a", COUNT)).toBeNull();
    expect(nextMenuIndex(0, "ArrowDown", 0)).toBeNull();
  });
});
