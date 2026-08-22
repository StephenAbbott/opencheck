/**
 * Explain — the replacement for `title=` as a sole carrier of meaning
 * (Phase 124).
 *
 * The Phase 122 audit counted 33 `title` attributes; the Phase 124 sweep found
 * 18 of them were the *only* place a piece of substantive information appeared.
 * A native tooltip is invisible to keyboard users, invisible on touch, cannot
 * be styled, truncates unpredictably at length, and is announced inconsistently
 * across screen readers. Some of the worst were not garnish: what a match
 * percentage measures, that a GLEIF Level 2 link is accounting consolidation
 * and not shareholding, a licence's plain-English terms, a 240-character
 * paragraph about what a download contains.
 *
 * Two shapes, because the cases are two different problems:
 *
 * - **`Explain`** — a focusable ⓘ button beside the thing it explains, which
 *   toggles the text in flow. For explanations long enough that a reader needs
 *   to choose to read them. In flow rather than floating because a floating
 *   layer needs collision detection, a portal and dismissal handling to be
 *   correct, and there is no popover library here; an expanded block that
 *   pushes its siblings down is plainer and cannot be clipped or land offscreen.
 * A second shape, `Described`, was written for the short cases —
 * `aria-describedby` pointing at a separate element — and **deleted unused in
 * Phase 125**. Every short case turned out to want inline text appended to the
 * control's own accessible name, not a separately-wired description, and the
 * one place that genuinely needs `aria-describedby` (the national-ID format
 * warning in `App.tsx`, which points a form field at a `role="status"`) already
 * does it directly. An unused primitive in a design system is worse than none:
 * it invites the wrong pattern to be adopted because it exists.
 *
 * **`Explain` itself has one caller, and that is deliberate** (decided in
 * Phase 125). Only `BackgroundCheckPanel`'s person-subgraph download had an
 * explanation long enough to be worth a disclosure; every other case fitted
 * inline. It is kept rather than inlined so the next long explanation does not
 * have to reinvent the focus and `aria-expanded` handling — so do not delete
 * this as dead code on a one-caller count alone.
 *
 * The rule this encodes: **if a `title` is the only place something is said,
 * it is not said.** A `title` that duplicates a visible label or an `aria-label`
 * is fine and several were left alone; a `title` that duplicated the *worse* of
 * two strings was deleted.
 */

import { useId, useState } from "react";

export function Explain({
  label,
  children,
  className = "",
}: {
  /** What the button offers, for assistive technology: "Explain the match score". */
  label: string;
  /** The explanation itself. */
  children: React.ReactNode;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const id = useId();
  return (
    <>
      <button
        type="button"
        aria-expanded={open}
        aria-controls={id}
        aria-label={label}
        onClick={() => setOpen((v) => !v)}
        className={`inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-oo-rule bg-white text-oo-meta font-semibold leading-none text-oo-muted transition-colors hover:border-oo-blue hover:text-oo-blue focus-visible:outline focus-visible:outline-2 focus-visible:outline-oo-blue ${className}`}
      >
        <span aria-hidden="true">i</span>
      </button>
      {open && (
        <span
          id={id}
          className="mt-1.5 block basis-full text-oo-meta text-oo-ink bg-white border border-oo-rule rounded-oo px-2.5 py-1.5 leading-[1.5]"
        >
          {children}
        </span>
      )}
    </>
  );
}
