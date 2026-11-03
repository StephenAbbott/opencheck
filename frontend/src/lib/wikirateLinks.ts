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

/** The fields of a Wikirate bundle the card's links are built from. */
export interface WikirateLinkFields {
  name: string;
  card_id: number;
  total_answers?: number | null;
  platform_url?: string | null;
  /** "rest" when Wikirate 2.0's REST API answered; absent from legacy runs. */
  api?: string | null;
}

export interface WikirateCardLinks {
  /** The "Wikirate" link beside the data-point count. */
  entity: string;
  /** The links under the sample answers, in order. */
  footer: { href: string; label: string }[];
}

/**
 * The card's links, by the API that answered.
 *
 * Legacy runs link the data points to the legacy platform and add the
 * company's page on Wikirate 2.0. A REST run has no public page listing every
 * data point (the new site's data explorer needs a login), so it links the
 * company's page once and says how many data points Wikirate holds.
 */
export function wikirateCardLinks(raw: WikirateLinkFields): WikirateCardLinks {
  const total = (raw.total_answers ?? 0).toLocaleString("en-GB");
  const platform =
    wikiratePlatformUrl(raw.platform_url) ??
    `${WIKIRATE_PLATFORM_BASE}/company/${raw.card_id}`;
  if (raw.api === "rest") {
    return {
      entity: platform,
      footer: [
        { href: platform, label: `${raw.name} on wikirate.org (${total} data points) →` },
      ],
    };
  }
  const legacy = wikirateLegacyEntityUrl(raw.name);
  const footer = [
    { href: legacy, label: `View all ${total} data points on ${WIKIRATE_LEGACY_HOST} →` },
  ];
  if (raw.platform_url) {
    footer.push({ href: platform, label: "Company page on the new Wikirate platform →" });
  }
  return { entity: legacy, footer };
}

