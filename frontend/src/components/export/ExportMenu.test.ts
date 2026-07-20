import { describe, expect, it } from "vitest";
import { nextMenuIndex } from "./ExportMenu";

describe("nextMenuIndex", () => {
  const COUNT = 3; // PDF, Markdown, Download data

  it("moves down and wraps", () => {
    expect(nextMenuIndex(0, "ArrowDown", COUNT)).toBe(1);
    expect(nextMenuIndex(1, "ArrowDown", COUNT)).toBe(2);
    expect(nextMenuIndex(2, "ArrowDown", COUNT)).toBe(0);
  });

  it("moves up and wraps", () => {
    expect(nextMenuIndex(2, "ArrowUp", COUNT)).toBe(1);
    expect(nextMenuIndex(0, "ArrowUp", COUNT)).toBe(2);
  });

  it("Home and End jump to the edges", () => {
    expect(nextMenuIndex(1, "Home", COUNT)).toBe(0);
    expect(nextMenuIndex(1, "End", COUNT)).toBe(2);
  });

  it("recovers when nothing is focused yet (current = -1)", () => {
    expect(nextMenuIndex(-1, "ArrowDown", COUNT)).toBe(0);
    expect(nextMenuIndex(-1, "ArrowUp", COUNT)).toBe(1); // wraps from -1
  });

  it("ignores non-navigation keys and empty menus", () => {
    expect(nextMenuIndex(0, "Enter", COUNT)).toBeNull();
    expect(nextMenuIndex(0, "a", COUNT)).toBeNull();
    expect(nextMenuIndex(0, "ArrowDown", 0)).toBeNull();
  });
});
