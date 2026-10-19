/**
 * The homepage "New features" band (Phase 344).
 *
 * Three promo cards under the curated examples, where BatchInvite ("Screen a
 * list") used to sit; batch screening keeps its place on /features. The cards
 * are a different thing from the curated examples above them — a curated card
 * runs a lookup, a promo card links to a page — so they look different on
 * purpose: an illustrated panel in the feature's own token tier, a one-line
 * description and a secondary button, against the examples' plain white
 * tiles.
 *
 * Content lives here, not in the component, so the logic-only suite can pin
 * what the band says (`newFeatures.test.ts`) and swapping a feature is one
 * edit plus an SVG in `public/new-features/`.
 *
 * Rules the copy keeps (the hardcoded-claims lesson in CLAUDE.md):
 *  - no counts that move with the engine or the registry — a source count, a
 *    dataset count, a number of watched companies;
 *  - the illustrations name no real company and assert nothing about one, so
 *    they cannot go stale the way a rendered result can;
 *  - CTAs are plain hrefs the app already parses (the /features rule), so a
 *    right-click works and nothing drifts from App's state machine.
 */

/** Which token tier a card's illustrated panel wears. */
export type NewFeatureTone = "risk" | "ownership" | "soft";

export interface NewFeature {
  id: string;
  title: string;
  /** One sentence. */
  description: string;
  /** Shown after "New ·" above the title. */
  eyebrow: string;
  href: string;
  cta: string;
  /** Path under `public/`. */
  image: string;
  imageAlt: string;
  tone: NewFeatureTone;
}

export const NEW_FEATURES: readonly NewFeature[] = [
  {
    id: "sanctions-50-test",
    title: "Sanctions 50% test",
    description:
      "Adds up what sanctioned owners hold, layer by layer, and says whether the OFAC, EU or UK 50% ownership test is met.",
    eyebrow: "Risk",
    href: "/?lei=213800HTIY6D1KMBG757",
    cta: "See a worked example",
    image: "/new-features/sanctions-50-test.svg",
    imageAlt:
      "Illustration: two sanctioned owners holding 30% and 25% of a company — 55% combined, past the 50% line.",
    tone: "risk",
  },
  {
    id: "ownership-map-mcp",
    title: "Ownership map via MCP",
    description:
      "Ask your AI assistant who owns a company and get the structure back as a drawn map, with the table behind it.",
    eyebrow: "API & MCP",
    href: "/api",
    cta: "Connect over MCP",
    image: "/new-features/ownership-map-mcp.svg",
    imageAlt:
      "Illustration: the opencheck_ownership_map tool, called from an AI assistant, returning a drawn ownership map.",
    tone: "ownership",
  },
  {
    id: "watchlist",
    title: "Watchlist",
    description:
      "Re-run only when GLEIF, OpenSanctions or a followed register records a change; see whether it matters; keep your decisions on record.",
    eyebrow: "Monitoring",
    href: "/features#watchlist",
    cta: "How the watchlist works",
    image: "/new-features/watchlist.svg",
    imageAlt:
      "Illustration: a watchlist feed with a GLEIF name change, an OpenSanctions listing and a re-run that found no difference.",
    tone: "soft",
  },
];

/**
 * Panel and eyebrow classes per tone. Every value is an `oo-*` token: the
 * rose is the risk tier the sanctions-control chip already wears, the blue is
 * the ownership-edge tint, and the watchlist takes the soft "active" pair.
 */
export const NEW_FEATURE_TONES: Record<NewFeatureTone, { panel: string; eyebrow: string }> = {
  risk: {
    panel: "bg-oo-risk-bg border-oo-risk-tintBorder",
    eyebrow: "text-oo-risk-text",
  },
  ownership: {
    panel: "bg-oo-graph-ownershipTint border-oo-graph-ownershipTintBorder",
    eyebrow: "text-oo-graph-ownershipText",
  },
  soft: {
    panel: "bg-oo-soft border-oo-softBorder",
    eyebrow: "text-oo-blue",
  },
};
