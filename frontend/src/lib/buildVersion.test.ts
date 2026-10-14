import { afterEach, describe, expect, it, vi } from "vitest";

import {
  checkForNewVersion,
  currentEntry,
  entryFromManifest,
  getNewVersionAvailable,
  isNewerBuild,
  resetBuildVersionForTests,
  subscribeNewVersion,
} from "./buildVersion";

afterEach(() => resetBuildVersionForTests());

const docWith = (src: string | null) => ({
  querySelector: () => (src ? ({ getAttribute: () => src } as unknown as Element) : null),
}) as unknown as Pick<Document, "querySelector">;

const answer = (body: unknown, ok = true) =>
  vi.fn().mockResolvedValue({ ok, json: () => Promise.resolve(body) });

describe("reading the two entries", () => {
  it("reads the page's own entry script", () => {
    expect(currentEntry(docWith("/assets/index-BYAZ4S_F.js"))).toBe("assets/index-BYAZ4S_F.js");
    expect(currentEntry(docWith("https://opencheck.world/assets/index-X.js"))).toBe("assets/index-X.js");
    expect(currentEntry(docWith(null))).toBeNull();
  });

  it("reads version.json and refuses anything else", () => {
    expect(entryFromManifest({ entry: "assets/index-X.js" })).toBe("assets/index-X.js");
    expect(entryFromManifest({ entry: "" })).toBeNull();
    expect(entryFromManifest("<!doctype html>")).toBeNull();
    expect(entryFromManifest(null)).toBeNull();
  });

  it("calls it newer only when both sides are known and differ", () => {
    expect(isNewerBuild("assets/index-A.js", "assets/index-B.js")).toBe(true);
    expect(isNewerBuild("assets/index-A.js", "assets/index-A.js")).toBe(false);
    expect(isNewerBuild(null, "assets/index-B.js")).toBe(false);
    expect(isNewerBuild("assets/index-A.js", null)).toBe(false);
  });
});

describe("checkForNewVersion", () => {
  it("remembers a newer build and tells its listeners once", async () => {
    const listener = vi.fn();
    subscribeNewVersion(listener);
    const fetcher = answer({ entry: "assets/index-B.js" });
    await expect(checkForNewVersion(fetcher, "assets/index-A.js")).resolves.toBe(true);
    await expect(checkForNewVersion(fetcher, "assets/index-A.js")).resolves.toBe(true);
    expect(getNewVersionAvailable()).toBe(true);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(fetcher).toHaveBeenCalledTimes(1); // known once, never asked again
    expect(fetcher.mock.calls[0][1]).toEqual({ cache: "no-store" });
  });

  it("says nothing for the same build, a failed fetch, or a development page", async () => {
    await expect(checkForNewVersion(answer({ entry: "assets/index-A.js" }), "assets/index-A.js")).resolves.toBe(false);
    await expect(checkForNewVersion(answer({}, false), "assets/index-A.js")).resolves.toBe(false);
    await expect(checkForNewVersion(vi.fn().mockRejectedValue(new Error("offline")), "assets/index-A.js")).resolves.toBe(false);
    const fetcher = answer({ entry: "assets/index-B.js" });
    await expect(checkForNewVersion(fetcher, null)).resolves.toBe(false);
    expect(fetcher).not.toHaveBeenCalled();
    expect(getNewVersionAvailable()).toBe(false);
  });
});
