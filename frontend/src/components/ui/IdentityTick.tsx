/**
 * IdentityTick — Companies House has verified this person's identity (Phase 203).
 *
 * One drawing, used on the graph canvas (at the node's SE compass point), on
 * the "Read as text" tree row, in the graph legend and on BackgroundCheck
 * cards, so the mark a reader learns in the legend is the mark they meet
 * everywhere else.
 *
 * Always `aria-hidden`: the tick is never the only place the fact is stated.
 * Each caller puts `IDENTITY_VERIFIED_LABEL` (`lib/identityVerification.ts`)
 * beside it — visibly in the legend and on the cards, `sr-only` on the tree row
 * whose legend chip carries the same words. No `title=` (Phase 124).
 *
 * The tick asserts a positive fact only. There is no "unverified" variant, by
 * decision (Stephen, 10 Sept 2026).
 */

import { Icon } from "./Icon";

export function IdentityTick({ size = 12 }: { size?: number }) {
  return (
    <span
      aria-hidden="true"
      className="inline-flex items-center justify-center rounded-full bg-oo-ok-text text-white flex-shrink-0"
      style={{ width: size, height: size }}
    >
      <Icon name="check" size={Math.round(size * 0.75)} strokeWidth={3.5} />
    </span>
  );
}
