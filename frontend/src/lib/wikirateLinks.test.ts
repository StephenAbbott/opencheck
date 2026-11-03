import { describe, expect, it } from "vitest";
import {
  WIKIRATE_LEGACY_HOST,
  wikirateCardLinks,
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

describe("wikirateCardLinks", () => {
  const bp = { name: "BP plc.", card_id: 637, total_answers: 2295 };

  it("a REST run links the company page once, with the count", () => {
    const links = wikirateCardLinks({
      ...bp,
      api: "rest",
      platform_url: "https://wikirate.org/company/637",
    });
    expect(links.entity).toBe("https://wikirate.org/company/637");
    expect(links.footer).toEqual([
      {
        href: "https://wikirate.org/company/637",
        label: "BP plc. on wikirate.org (2,295 data points) →",
      },
    ]);
  });

  it("a REST run never links the legacy host", () => {
    const links = wikirateCardLinks({ ...bp, api: "rest", platform_url: null });
    const hrefs = [links.entity, ...links.footer.map((l) => l.href)];
    expect(hrefs.some((h) => h.includes("legacy.wikirate.org"))).toBe(false);
    expect(links.entity).toBe("https://wikirate.org/company/637");
  });

  it("a legacy run keeps the legacy data-point link and the new company page", () => {
    const links = wikirateCardLinks({
      ...bp,
      total_answers: 2290,
      platform_url: "https://platform.wikirate.org/company/637",
    });
    expect(links.entity).toBe("https://legacy.wikirate.org/BP_plc");
    expect(links.footer.map((l) => l.label)).toEqual([
      "View all 2,290 data points on legacy.wikirate.org →",
      "Company page on the new Wikirate platform →",
    ]);
    expect(links.footer[1].href).toBe("https://wikirate.org/company/637");
  });

  it("a legacy run from before Phase 321 has no company-page link", () => {
    expect(wikirateCardLinks(bp).footer).toHaveLength(1);
  });
});

