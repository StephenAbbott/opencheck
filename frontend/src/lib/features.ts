/**
 * What the /features page says, in `lib/` so the logic-only suite can pin it.
 *
 * The page exists because the answer to "what can OpenCheck do" was spread
 * across `/changelog`, three Medium posts and a `docs/` file — every one of
 * them a record of *how* something was built rather than an invitation to use
 * it. This list is the invitation.
 *
 * Three rules, all enforced by `features.test.ts`:
 *
 * - **Two sentences, no more.** A features page that runs to paragraphs is a
 *   changelog with pictures. The limit is the point.
 * - **One call to action each.** Every feature ends in something the reader
 *   can open, and the href is a real URL the app already understands
 *   (`/?lei=…&mode=…`, `/batch`) rather than a click handler — so the links
 *   survive a right-click, a share and any future change to App's state
 *   machine.
 * - **The copy asserts nothing the product does not.** The same discipline as
 *   `findings.py`: absence is stated in the same voice as presence, and no
 *   sentence here promises certainty the risk layer refuses to.
 *
 * `accent` / `glyph` are the badge colours, and they are the SAME values as
 * the 640px wordmark badges in `outputs/mode-badges/` — the two must not
 * drift, which is why they are written here as literals with the token name
 * beside them rather than looked up. See CLAUDE.md, "Brand: Check-mode
 * badges".
 *
 * That makes this the badge tier's token file, and it is allowlisted for raw
 * hex in `scripts/lint-design-system.mjs` for the same reason
 * `lib/graphStyle.ts` is: the value is handed to an inline style as a string,
 * not applied as a class, so there is no class name to write instead.
 */

import type { IconName } from "../components/ui/Icon";

/** What tier of thing this is. Shown as the eyebrow above the name. */
export type FeatureKind = "Check mode" | "Workflow" | "Capability";

export interface Feature {
  /** The anchor, so `/features#time-machine` is a linkable address. */
  id: string;
  name: string;
  kind: FeatureKind;
  /** A name in `components/ui/Icon.tsx` — never a second copy of the path. */
  icon: IconName;
  /** Badge ring colour. */
  accent: string;
  /** Badge glyph colour: a lighter step of `accent`. */
  glyph: string;
  /** At most two sentences. */
  description: string;
  image: { src: string; alt: string; width: number; height: number };
  cta: { label: string; href: string };
}

export const FEATURES: Feature[] = [
  {
    id: "quickcheck",
    name: "QuickCheck",
    kind: "Check mode",
    icon: "quickcheck",
    accent: "#22c55e", // oo.node.green
    glyph: "#86efac",
    description:
      "Screens a company on its own — sanctions, control and structure — with results streaming in from 40 open sources as each one answers. Every finding names the source it came from, and a source that could not be reached is reported as a gap rather than a clean screen.",
    image: {
      src: "/features/quickcheck.png",
      alt: "A QuickCheck report: the subject company with its LEI and register status, a verdict sentence, three risk chips, and source cards filling in one by one as each source answers.",
      width: 1648,
      height: 645,
    },
    cta: { label: "Run a QuickCheck on BP p.l.c.", href: "/?lei=213800LH1BZH3DI6G760" },
  },
  {
    id: "fullcheck",
    name: "FullCheck",
    kind: "Check mode",
    icon: "fullcheck",
    accent: "#3b82f6", // oo.node.blue
    glyph: "#93c5fd",
    description:
      "Follows the ownership chain outwards layer by layer, then screens everything it reaches. Related-party risk is drawn onto the network itself, so a sanctioned owner two levels up appears on the diagram rather than in a footnote.",
    image: {
      src: "/features/fullcheck.png",
      alt: "An ownership and control network: a chain of companies above the subject, a controlling entity and a director beside it, with a sanctions badge on the ultimate parent and a PEP badge on the director.",
      width: 1648,
      height: 846,
    },
    cta: {
      label: "Open a FullCheck on Taqa Bratani",
      href: "/?lei=213800E11LI1SCETU492&mode=full",
    },
  },
  {
    id: "backgroundcheck",
    name: "BackgroundCheck",
    kind: "Check mode",
    icon: "backgroundcheck",
    accent: "#7c3aed", // oo.node.purple
    glyph: "#c4b5fd",
    description:
      "Screens the people rather than the company: the officers, directors and beneficial owners named in the records, each checked against sanctions, PEP and offshore-leaks sources. Every result is a potential match with its similarity score — never a confirmed identity.",
    image: {
      src: "/features/backgroundcheck.png",
      alt: "Three people named in a company's records, each with a match-confidence chip: one possible match, one checked with no match, and one that could not be screened because a source was unavailable.",
      width: 1648,
      height: 629,
    },
    cta: {
      label: "Screen the people behind Ørsted A/S",
      href: "/?lei=W9NG6WMZIYEU8VEDOG48&mode=background",
    },
  },
  {
    id: "batch-screening",
    name: "Batch screening",
    kind: "Workflow",
    icon: "batch",
    // Invented, on the grounds Phase 122 invented oo.node.teal: oo.blue
    // #3d30d4 is too dark to read as a ring on the #0d1b3e badge, and the
    // lighter indigos collide with BackgroundCheck's violet.
    accent: "#22d3ee",
    glyph: "#a5f3fc",
    description:
      "Paste up to twenty LEIs and screen them all in one pass, then download the result as a CSV. Companies that could not be fully checked sort to the top and are never rendered as clean.",
    image: {
      src: "/features/batch.png",
      alt: "The batch screening page: a paste box holding four LEIs with a count and a time estimate beneath it, and a results table whose top row is tinted amber because that company could not be fully checked.",
      width: 1648,
      height: 566,
    },
    cta: { label: "Screen a list", href: "/batch" },
  },
  {
    id: "time-machine",
    name: "Time Machine",
    kind: "Capability",
    icon: "history",
    accent: "#b45309", // oo.graph.same
    glyph: "#fcd34d", // oo.warn.border
    description:
      "Rebuilds how a company's ownership, control and identity have changed over time, merging GLEIF, Companies House, Denmark's CVR and more onto one axis. Routine administrative filings are folded away, and every event says whether its date is the effective one or the recorded one.",
    image: {
      src: "/features/time-machine.png",
      alt: "A timeline of a company's changes: a new parent, a legal form change and a name change, each with its date, whether that date is effective or recorded, and the registers that reported it.",
      width: 1648,
      height: 736,
    },
    cta: { label: "See the timeline on BP p.l.c.", href: "/?lei=213800LH1BZH3DI6G760" },
  },
  {
    id: "network-visualisations",
    name: "Network visualisations",
    kind: "Capability",
    icon: "network",
    accent: "#93c5fd", // oo.mark.line — the logo's own network-edge colour
    glyph: "#ffffff",
    description:
      "Every source's data is drawn as one BODS ownership graph, using Open Ownership's Beneficial Ownership Visualisation System for the people, companies, trusts and interests between them. Each graph carries the same statements as a list underneath, so the picture is never the only way to get the information.",
    image: {
      src: "/features/network.png",
      alt: "An ownership graph beside its text equivalent: a state body controlling the subject, two subsidiaries below it and a director beside it, with the same five relationships listed as an indented tree.",
      width: 1648,
      height: 686,
    },
    cta: {
      label: "Explore a network",
      href: "/?lei=213800LH1BZH3DI6G760&mode=full",
    },
  },
];

/**
 * Sentence count, for the two-sentence rule.
 *
 * Deliberately crude — it counts full stops that end a sentence. The copy in
 * `FEATURES` contains no abbreviations, decimals or ellipses, and the test
 * that uses this also asserts that, so a future edit that introduces one
 * fails loudly here rather than silently making the rule unenforceable.
 */
export function sentenceCount(text: string): number {
  return text.split(/[.!?](?:\s|$)/).filter((s) => s.trim().length > 0).length;
}

/** The feature an anchor names, or `undefined` — used by the index rail. */
export function featureById(id: string): Feature | undefined {
  return FEATURES.find((f) => f.id === id);
}
