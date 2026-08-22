/**
 * SectionLabel — the eyebrow, once.
 *
 * A `SectionLabel` already existed in App.tsx but no component imported it,
 * so eight variants of the same treatment grew in parallel: 11px/10px, with
 * `tracking-oo-eyebrow` or `tracking-widest` or `tracking-[0.08em]` or no
 * tracking at all, and rendered variously as `h2`, `p` and `div` — which is
 * why the heading outline of the results page had gaps in it.
 *
 * `as` is required to be a heading where the label names a section, so the
 * document outline is a decision rather than an accident.
 */

export type SectionLabelTone = "muted" | "accent" | "ink";

const TONES: Record<SectionLabelTone, string> = {
  muted: "text-oo-muted",
  accent: "text-oo-blue",
  ink: "text-oo-ink",
};

export function sectionLabelClasses(
  tone: SectionLabelTone = "muted",
  className = "",
): string {
  return [
    "font-body text-oo-meta font-bold uppercase tracking-oo-eyebrow",
    TONES[tone],
    className,
  ]
    .filter(Boolean)
    .join(" ")
    .trim();
}

export function SectionLabel({
  as: Tag = "p",
  tone = "muted",
  className = "",
  children,
  ...rest
}: {
  as?: "h2" | "h3" | "h4" | "p";
  tone?: SectionLabelTone;
} & React.HTMLAttributes<HTMLElement>) {
  return (
    <Tag className={sectionLabelClasses(tone, className)} {...rest}>
      {children}
    </Tag>
  );
}

/**
 * SectionHeading — the visible section title (16px Bitter), separate from
 * the eyebrow so components stop using a 10px uppercase label as an `h2`.
 */
export function SectionHeading({
  as: Tag = "h2",
  className = "",
  children,
  ...rest
}: {
  as?: "h2" | "h3" | "h4";
} & React.HTMLAttributes<HTMLHeadingElement>) {
  return (
    <Tag
      className={`font-head font-bold text-oo-head text-oo-ink ${className}`.trim()}
      {...rest}
    >
      {children}
    </Tag>
  );
}
