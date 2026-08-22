/**
 * MatchConfidenceChip — how sure we are that two names are the same party
 * (Phase 125).
 *
 * ## Why this is not the confidence in `Chip.tsx`
 *
 * The app has **two** confidences and they are not interchangeable:
 *
 * - **Corroboration** — how many sources assert a thing. Lives in
 *   `ui/Chip.tsx` as `CONFIDENCE_GLYPH` / `CONFIDENCE_LABEL`, rendered as
 *   ●◐○, and its prose says "Corroborated by two or more sources".
 * - **Match strength** — how sure we are that this record is *the same party*
 *   as the subject. That is what `ClusterGroup` and `NzAssociations` were both
 *   rendering, graded from a person-pair comparison and from an address tier
 *   respectively.
 *
 * The Phase 124 audit listed four "confidence chip" designs and suggested one
 * component. Two of them are this one. The other two are not confidence at
 * all — `SubsidiaryNetwork`'s `RelationBadge` renders a *relation kind*
 * (direct / ultimate / both), and `RiskChip`'s glyphs are corroboration, which
 * Phase 124 already unified with `ui/Chip`. Folding match strength into the
 * corroboration vocabulary would put "Corroborated by two or more sources"
 * beside a single-source name match — a false statement in the one place the
 * product is most careful, since a name match is explicitly *not* an identity
 * claim.
 *
 * So the words here are about matching, and they never borrow the ●◐○ glyphs.
 *
 * ## What the levels mean
 *
 * Deliberately phrased as what was compared, not as a grade. "High" alone
 * tells a reader nothing about what would make it low.
 */

export type MatchLevel = "high" | "medium" | "low";

/** The level, as the label a reader sees. */
const LEVEL_LABEL: Record<MatchLevel, string> = {
  high: "Strong match",
  medium: "Possible match",
  low: "Name only",
};

/** What each level actually rests on — the sentence the old chips lacked. */
const LEVEL_MEANING: Record<MatchLevel, string> = {
  high: "more than the name agrees",
  medium: "the name agrees and nothing contradicts it",
  low: "only the name agrees — not an identity match",
};

const LEVEL_STYLE: Record<MatchLevel, string> = {
  high: "border-oo-node-purple/50 bg-violet-100 text-violet-900",
  medium: "border-oo-softBorder bg-oo-soft text-oo-blue",
  low: "border-oo-warn-border bg-oo-warn-bg text-oo-warn-text",
};

const DOT_STYLE: Record<MatchLevel, string> = {
  high: "bg-oo-node-purple",
  medium: "bg-oo-blue",
  low: "bg-oo-warn-text",
};

/** Anything the backend sends that is not a known level reads as the weakest,
 *  because over-stating a match is the failure that matters. */
export function matchLevel(raw: string | null | undefined): MatchLevel {
  const v = (raw ?? "").toLowerCase();
  return v === "high" || v === "medium" ? v : "low";
}

export default function MatchConfidenceChip({
  confidence,
  basis,
  className = "",
}: {
  confidence: string;
  /** What the grade was drawn from, when the caller knows — an address tier, a
   *  shared birth year. Rendered after the level rather than replacing it. */
  basis?: string;
  className?: string;
}) {
  const level = matchLevel(confidence);
  return (
    <span
      className={`shrink-0 inline-flex items-center gap-1.5 rounded-oo border px-2 py-0.5 text-oo-meta font-semibold ${LEVEL_STYLE[level]} ${className}`}
    >
      <span aria-hidden="true" className={`h-1.5 w-1.5 rounded-full ${DOT_STYLE[level]}`} />
      {LEVEL_LABEL[level]}
      {basis ? <span className="font-normal"> · {basis}</span> : null}
      {/* The grade is a judgement; what it rests on is the useful part, and
          neither old chip said it anywhere. */}
      <span className="sr-only"> — {LEVEL_MEANING[level]}</span>
    </span>
  );
}
