/**
 * PanelSection — one band of the report card (Phase 126).
 *
 * The v2 design renders a check as **one white card** whose sections are bands
 * separated by a single rule, joined to the selected mode tab. The live page
 * instead rendered every section as its own detached white card floating on the
 * grey page background, with Risk signals and the source list not carded at
 * all — so the tab strip connected to nothing beneath it, and the page read as
 * a stack of unrelated widgets rather than one report.
 *
 * That difference is not decoration. The tab strip claims the panel below it;
 * detached cards break the claim, which is why a reader could not tell which
 * parts of the page the mode switch governed.
 *
 * Two rules this encodes:
 *
 * - **The band owns its padding, the caller owns its content.** Sections had
 *   drifted to `mb-8` margins of their own, which is what made them read as
 *   separate objects. Spacing belongs to the container.
 * - **`title` renders as a real heading** via `SectionHeading` — 16px Bitter,
 *   an `<h2>`. The live page titled its sections with `SectionLabel`, a 12px
 *   uppercase eyebrow in a `<p>`: the wrong element, at half the size, in the
 *   wrong typeface, on every section of the report. `SectionHeading` was built
 *   in Phase 122 for exactly this and nothing had adopted it.
 */

import { SectionHeading } from "./SectionLabel";

export default function PanelSection({
  title,
  /** Right-aligned counterpart to the title — a count, a legend, a control.
   *  The mockup calls this a split-head and uses it for "10 of 11 sources
   *  answered" and for the confidence legend. */
  aside,
  id,
  last = false,
  className = "",
  children,
}: {
  title?: React.ReactNode;
  aside?: React.ReactNode;
  id?: string;
  /** The final band, which carries no rule beneath it. */
  last?: boolean;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <section
      id={id}
      // Focus target: several controls jump to a section by id, and a section
      // that cannot take focus leaves a keyboard user where they were.
      tabIndex={id ? -1 : undefined}
      className={`px-4 py-[18px] sm:px-6 sm:py-[22px] ${
        last ? "" : "border-b border-oo-rule"
      } ${className}`.trim()}
    >
      {(title || aside) && (
        <div className="mb-3 flex flex-col items-start gap-1 sm:flex-row sm:items-baseline sm:justify-between sm:gap-4">
          {title ? <SectionHeading>{title}</SectionHeading> : <span />}
          {aside ? <div className="text-oo-small text-oo-muted">{aside}</div> : null}
        </div>
      )}
      {children}
    </section>
  );
}

/**
 * The card the bands sit in.
 *
 * Rendered by the tabpanel so it is visually welded to the active tab: the tab
 * already draws a white bottom edge over the strip's border, and this supplies
 * the surface that edge opens onto.
 */
export function PanelCard({
  className = "",
  children,
  ...rest
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={`mb-8 rounded-oo rounded-tl-none border border-oo-rule bg-white ${className}`.trim()}
      {...rest}
    >
      {children}
    </div>
  );
}
