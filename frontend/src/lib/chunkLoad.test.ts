import { describe, expect, it, vi } from "vitest";

import { bustedUrl, chunkUrlFrom, importWithRetry, isChunkLoadError } from "./chunkLoad";

const CHROME = new TypeError(
  "Failed to fetch dynamically imported module: https://opencheck.world/assets/FullCheckPanel-DQ5mKt65.js",
);
const SAFARI = new TypeError("Importing a module script failed.");
const noSleep = () => Promise.resolve();

describe("isChunkLoadError", () => {
  it("knows the three engines' wording", () => {
    expect(isChunkLoadError(CHROME)).toBe(true);
    expect(isChunkLoadError(SAFARI)).toBe(true);
    expect(isChunkLoadError(new TypeError("error loading dynamically imported module: x"))).toBe(true);
  });

  it("does not mistake a render error for one", () => {
    expect(isChunkLoadError(new Error("Cannot read properties of undefined"))).toBe(false);
    expect(isChunkLoadError(undefined)).toBe(false);
  });
});

describe("chunkUrlFrom / bustedUrl", () => {
  it("reads the chunk the browser named", () => {
    expect(chunkUrlFrom(CHROME)).toBe("https://opencheck.world/assets/FullCheckPanel-DQ5mKt65.js");
    expect(chunkUrlFrom(SAFARI)).toBeNull();
  });

  it("makes a new request of the same file", () => {
    expect(bustedUrl("https://x/a.js", 7)).toBe("https://x/a.js?retry=7");
    expect(bustedUrl("https://x/a.js?v=1", 7)).toBe("https://x/a.js?v=1&retry=7");
  });
});

describe("importWithRetry", () => {
  it("returns the first load when it works", async () => {
    const factory = vi.fn().mockResolvedValue({ default: "panel" });
    await expect(importWithRetry(factory, { sleep: noSleep })).resolves.toEqual({ default: "panel" });
    expect(factory).toHaveBeenCalledTimes(1);
  });

  it("retries a failed chunk once, at a new URL, after a pause", async () => {
    const factory = vi.fn().mockRejectedValue(CHROME);
    const importUrl = vi.fn().mockResolvedValue({ default: "panel" });
    const sleep = vi.fn(noSleep);
    await expect(importWithRetry(factory, { importUrl, sleep, delayMs: 1500 })).resolves.toEqual({
      default: "panel",
    });
    expect(sleep).toHaveBeenCalledWith(1500);
    expect(importUrl).toHaveBeenCalledTimes(1);
    expect(importUrl.mock.calls[0][0]).toMatch(/FullCheckPanel-DQ5mKt65\.js\?retry=\d+$/);
  });

  it("calls the factory again when the error names no URL", async () => {
    const factory = vi.fn().mockRejectedValueOnce(SAFARI).mockResolvedValueOnce({ default: "panel" });
    await expect(importWithRetry(factory, { sleep: noSleep })).resolves.toEqual({ default: "panel" });
    expect(factory).toHaveBeenCalledTimes(2);
  });

  it("gives up after the second failure and says so", async () => {
    const factory = vi.fn().mockRejectedValue(CHROME);
    const importUrl = vi.fn().mockRejectedValue(CHROME);
    const onGiveUp = vi.fn();
    await expect(importWithRetry(factory, { importUrl, sleep: noSleep, onGiveUp })).rejects.toBe(CHROME);
    expect(onGiveUp).toHaveBeenCalledTimes(1);
  });

  it("does not retry a module that loaded and threw", async () => {
    const boom = new Error("module top-level threw");
    const factory = vi.fn().mockRejectedValue(boom);
    const sleep = vi.fn(noSleep);
    await expect(importWithRetry(factory, { sleep })).rejects.toBe(boom);
    expect(factory).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });
});
