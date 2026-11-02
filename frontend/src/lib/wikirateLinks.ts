// Links on the Climate & ESG card's Wikirate block.
//
// On 1 Nov 2026 Wikirate 2.0 took over wikirate.org and the legacy platform —
// where the data points OpenCheck reads live — moved to legacy.wikirate.org,
// which Wikirate keeps up until 31 Dec 2026. The backend reads the legacy API
// (sources/wikirate.py); these links follow the same split.

export const WIKIRATE_LEGACY_BASE = "https://legacy.wikirate.org";
export const WIKIRATE_PLATFORM_BASE = "https://wikirate.org";
/** The last day Wikirate keeps the legacy platform up (`LEGACY_RETIRES`). */
export const WIKIRATE_LEGACY_RETIRES = "2026-12-31";

/** Host label for the "View all N data points on …" link. */
export const WIKIRATE_LEGACY_HOST = "legacy.wikirate.org";

/**
 * The company's page on the legacy platform, by name.
 *
 * The numbered-card form (`~{card_id}`) redirects anonymous visitors to a
 * sign-in page; the named-card form ("BP_plc") reaches the entity page
 * directly, so browser links use a slug built from the company name.
 */
export function wikirateLegacyEntityUrl(name: string): string {
  const slug = name.trim().replace(/\.+$/, "").replace(/\s+/g, "_");
  return `${WIKIRATE_LEGACY_BASE}/${encodeURIComponent(slug)}`;
}

/**
 * The company's page on Wikirate 2.0, from the bundle's `platform_url`.
 *
 * Bundles built between 10 Oct and 1 Nov 2026 (saved reports, the replay
 * cache) carry `platform.wikirate.org/company/{id}`, the pre-launch host;
 * those are moved onto wikirate.org, which is the new platform now. Anything
 * else is returned as filed; no URL → `null`.
 */
export function wikiratePlatformUrl(url: string | null | undefined): string | null {
  if (!url) return null;
  return url.replace(/^https:\/\/platform\.wikirate\.org\//, `${WIKIRATE_PLATFORM_BASE}/`);
}
