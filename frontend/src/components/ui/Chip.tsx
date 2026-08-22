/**
 * Chip — the one chip in the design system.
 *
 * Phase 122 inventory found about twelve chip families across the app
 * (RiskChip, CitationChip, LicenseChip defined twice, IdentifierPill, the
 * search status pill, the "stub" tag, TED role badges, BtsBadge, the
 * timeline SourceChip, the ESG parent chip, MentionsBreakdown pills,
 * SourceLegend pills) with four different roundings and no shared meaning
 * for a colour. These are the six meanings they were expressing.
 *
 * The tone is chosen by what the chip *asserts*, never by how alarming it
 * should look:
 *   risk    — an adverse finding about the subject
 *   context — a structural observation that is not an adverse finding
 *   warn    — something did not run, or a restriction applies
 *   ok      — corroborated, or freely reusable
 *   neutral — a fact with no valence (a count, an identifier, a status)
 *   accent  — an OpenCheck affordance, not a claim about the data
 */

export type ChipTone = "risk" | "context" | "warn" | "ok" | "neutral" | "accent";
export type ChipSize = "sm" | "md";

const TONES: Record<ChipTone, string> = {
  risk: "bg-rose-100 border-rose-400 text-rose-800",
  context: "bg-oo-info-bg border-oo-info-border text-oo-info-text",
  warn: "bg-oo-warn-bg border-oo-warn-border text-oo-warn-text",
  ok: "bg-oo-ok-bg border-oo-ok-border text-oo-ok-text",
  neutral: "bg-oo-bg border-oo-rule text-oo-burst",
  accent: "bg-oo-soft border-oo-softBorder text-oo-blue",
};

const SIZES: Record<ChipSize, string> = {
  sm: "px-2.5 py-0.5 text-oo-meta",
  md: "px-3 py-1 text-oo-small",
};

export function chipClasses(
  tone: ChipTone = "neutral",
  size: ChipSize = "sm",
  className = "",
): string {
  return [
    "inline-flex items-center gap-1.5 rounded-full border font-body",
    SIZES[size],
    TONES[tone],
    className,
  ]
    .filter(Boolean)
    .join(" ")
    .trim();
}

/**
 * Confidence is shown as a glyph *and* named in text for assistive
 * technology. v1 marked the glyph `aria-hidden` and gave the level nowhere
 * else, so a screen-reader user heard "Sanctioned" with no indication of
 * whether one source or three said so.
 */
export const CONFIDENCE_GLYPH: Record<string, string> = {
  high: "●",
  medium: "◐",
  low: "○",
};

export const CONFIDENCE_LABEL: Record<string, string> = {
  high: "Corroborated by two or more sources",
  medium: "One source only",
  low: "Inferred",
};

export function Chip({
  tone = "neutral",
  size = "sm",
  confidence,
  className = "",
  children,
  ...rest
}: {
  tone?: ChipTone;
  size?: ChipSize;
  /** Renders the confidence glyph plus its screen-reader label. */
  confidence?: string;
} & React.HTMLAttributes<HTMLSpanElement>) {
  return (
    <span className={chipClasses(tone, size, className)} {...rest}>
      {confidence ? (
        <>
          <span aria-hidden="true">{CONFIDENCE_GLYPH[confidence] ?? "•"}</span>
          <span className="sr-only">
            {CONFIDENCE_LABEL[confidence] ?? confidence}:{" "}
          </span>
        </>
      ) : null}
      {children}
    </span>
  );
}
