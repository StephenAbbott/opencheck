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
  open,
  onToggle,
  last = false,
  className = "",
  children,
}: {
  title?: React.ReactNode;
  aside?: React.ReactNode;
  id?: string;
  /** Makes the band a disclosure: the title row becomes the control and the
   *  children render only when open. Omit for the usual always-open band.
   *
   *  Used where the section answers a question the reader has not asked yet —
   *  identity corroboration is reassurance, and reassurance that occupies a
   *  screen before anyone doubted anything is in the way. The affordance that
   *  opens it is the subject card's own identifier badge. */
  open?: boolean;
  onToggle?: (open: boolean) => void;
  /** Rarely needed: `PanelCard` already strips the rule from its last child in
   *  CSS, which is more reliable than every call site remembering to pass a
   *  flag — the first version defaulted to false and no caller ever set it, so
   *  every card drew a doubled line at its foot. */
  last?: boolean;
  className?: string;
  children: React.ReactNode;
}) {
  const collapsible = open !== undefined;
  const bodyId = `${id ?? "oo-panel-section"}-body`;
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
      {(title || aside) && !collapsible && (
        <div className="mb-3 flex flex-col items-start gap-1 sm:flex-row sm:items-baseline sm:justify-between sm:gap-4">
          {title ? <SectionHeading>{title}</SectionHeading> : <span />}
          {aside ? <div className="text-oo-small text-oo-muted">{aside}</div> : null}
        </div>
      )}
      {collapsible && (
        // The WAI-ARIA disclosure pattern: an <h2> wrapping the button, not a
        // button labelled like a heading. The heading has to stay in the
        // outline whether the band is open or shut.
        <SectionHeading className="m-0">
          <button
            type="button"
            aria-expanded={open}
            aria-controls={bodyId}
            onClick={() => onToggle?.(!open)}
            className="w-full flex flex-col items-start gap-1 text-left sm:flex-row sm:items-baseline sm:justify-between sm:gap-4"
          >
            <span className="flex items-center gap-2">
              {title}
              <svg
                width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"
                aria-hidden="true"
                className={`shrink-0 text-oo-blue transition-transform ${open ? "rotate-180" : ""}`}
              >
                <path d="m6 9 6 6 6-6" />
              </svg>
            </span>
            {aside ? (
              <span className="font-body font-normal text-oo-small text-oo-muted">
                {aside}
              </span>
            ) : null}
          </button>
        </SectionHeading>
      )}
      {collapsible ? (
        open ? (
          <div id={bodyId} className="mt-3">
            {children}
          </div>
        ) : null
      ) : (
        children
      )}
    </section>
  );
}

/**
 * The card the bands sit in, rendered by the tabpanel so the mode tab claims
 * what is beneath it.
 */
export function PanelCard({
  className = "",
  children,
  ...rest
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      // `[&>*:last-child]:border-b-0` removes the doubled line where the final
      // band's rule meets the card's own border.
      //
      // Fully rounded, matching the mockup's own `.card`. A first version
      // squared the top-left corner to weld the card to the active tab; the
      // degraded-screens and panel-error notices can sit between the two, so
      // it pointed at empty grey and read as a rendering glitch. Phase 128
      // removed the tablist's bottom margin so the two meet when no notice
      // intervenes — the seam is then the tablist's rule over this border,
      // which is exactly what the template renders.
      className={`mb-8 overflow-hidden rounded-oo border border-oo-rule bg-white [&>*:last-child]:border-b-0 ${className}`.trim()}
      {...rest}
    >
      {children}
    </div>
  );
}
