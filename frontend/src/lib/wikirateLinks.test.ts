import { describe, expect, it } from "vitest";
import {
  WIKIRATE_LEGACY_HOST,
  wikirateLegacyEntityUrl,
  wikiratePlatformUrl,
} from "./wikirateLinks";

describe("wikirateLegacyEntityUrl", () => {
  it("links the data points to the legacy platform, never the new wikirate.org", () => {
    const url = wikirateLegacyEntityUrl("BP plc.");
    expect(url).toBe("https://legacy.wikirate.org/BP_plc");
    expect(url.startsWith("https://wikirate.org/")).toBe(false);
  });

  it("slugs spaces and encodes what a path cannot carry", () => {
    expect(wikirateLegacyEntityUrl("  Eli Lilly & Co ")).toBe(
      "https://legacy.wikirate.org/Eli_Lilly_%26_Co",
    );
  });

  it("names the host the link goes to", () => {
    expect(WIKIRATE_LEGACY_HOST).toBe("legacy.wikirate.org");
  });
});

describe("wikiratePlatformUrl", () => {
  it("moves a pre-launch platform.wikirate.org link onto wikirate.org", () => {
    expect(wikiratePlatformUrl("https://platform.wikirate.org/company/56477")).toBe(
      "https://wikirate.org/company/56477",
    );
  });

  it("keeps a wikirate.org link as it is", () => {
    expect(wikiratePlatformUrl("https://wikirate.org/company/637")).toBe(
      "https://wikirate.org/company/637",
    );
  });

  it("gives null when the bundle has no platform link", () => {
    expect(wikiratePlatformUrl(undefined)).toBeNull();
    expect(wikiratePlatformUrl(null)).toBeNull();
    expect(wikiratePlatformUrl("")).toBeNull();
  });
});
