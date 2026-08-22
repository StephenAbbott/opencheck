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
 * - **`Described`** — no control at all: renders the text visibly (or, when the
 *   layout genuinely has no room, `sr-only`) and wires `aria-describedby`. For
 *   the short cases, where a disclosure would be more furniture than the
 *   sentence it hides.
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

/**
 * `aria-describedby` without a control — for a short explanation that belongs
 * on screen anyway.
 *
 * `visible` defaults to true on purpose. Hiding the sentence from sighted users
 * and showing it to screen readers reproduces the original bug with the
 * audiences swapped; pass `visible={false}` only where the layout truly cannot
 * hold it (a dense pointer row, a table cell).
 */
export function Described({
  id,
  children,
  visible = true,
  className = "",
}: {
  id: string;
  children: React.ReactNode;
  visible?: boolean;
  className?: string;
}) {
  return (
    <span id={id} className={visible ? className : "sr-only"}>
      {children}
    </span>
  );
}
