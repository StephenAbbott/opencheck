/**
 * FeatureMark — the check-mode badge, at the size a page can actually use.
 *
 * `outputs/mode-badges/*.svg` are 640px stamps built for social overlays:
 * navy circle, accent ring, glyph, Bitter wordmark, drop shadow. At the
 * 40-56px a page renders one at, the wordmark is illegible and the shadow is
 * mud — so this is the same mark with the two parts that only work large
 * removed, and nothing else changed. Ring and glyph colours come from the
 * caller (`lib/features.ts`), which carries the same values as the SVGs.
 *
 * The glyph is `<Icon>`, not a second copy of the path: `Icon` strokes with
 * `currentColor`, so setting `color` on this wrapper drives it. That is what
 * keeps the mode tab, the badge and the logo from drifting apart — the same
 * rule `esg-badge.svg` follows by using the literal leaf from the icon set.
 */

import { Icon, type IconName } from "./Icon";

export function FeatureMark({
  icon,
  accent,
  glyph,
  size = 48,
  className = "",
}: {
  icon: IconName;
  /** Ring colour. */
  accent: string;
  /** Glyph colour — a lighter step of `accent`. */
  glyph: string;
  size?: number;
  className?: string;
}) {
  return (
    <span
      // Always decorative: every mark on the page sits beside the feature's
      // own name, and a label here would have assistive technology read it
      // twice.
      aria-hidden="true"
      // `bg-oo-mark-navy` rather than an inline colour: the badge navy is
      // already a token (tailwind.config.js `oo.mark.navy`), and the
      // design-system lint's first rule exists because seven copies of
      // oo-blue's literal value were written out beside the token that names
      // it (the lint counts one in a comment too, which is why this sentence
      // names the token rather than quoting the value).
      className={`inline-flex items-center justify-center rounded-full flex-shrink-0 bg-oo-mark-navy ${className}`.trim()}
      style={{
        width: size,
        height: size,
        // The ring, as an inset rather than a border, so `size` stays the
        // rendered size and a row of marks lines up whatever the ring weight.
        boxShadow: `inset 0 0 0 ${Math.max(2, Math.round(size / 22))}px ${accent}`,
        color: glyph,
      }}
    >
      <Icon name={icon} size={Math.round(size * 0.5)} strokeWidth={1.7} />
    </span>
  );
}
