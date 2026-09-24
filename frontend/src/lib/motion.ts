/**
 * Reduced motion, in one place (Phase 241).
 *
 * `ExportMenu` honoured `prefers-reduced-motion` when it scrolled to the
 * download section; the four other smooth scrolls and the graph's re-centring
 * animation did not, so a reader who had asked the OS for no motion still got
 * the page gliding and the canvas panning (WCAG 2.3.3). Every call site now
 * asks here.
 */

export function prefersReducedMotion(): boolean {
  try {
    return Boolean(window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches);
  } catch {
    return false;
  }
}

/** `scrollIntoView`'s behaviour: instant for a reader who asked for no motion. */
export function scrollBehavior(): ScrollBehavior {
  return prefersReducedMotion() ? "auto" : "smooth";
}

/** Duration for a Cytoscape animation: 0 for a reader who asked for no motion. */
export function animationMs(ms: number): number {
  return prefersReducedMotion() ? 0 : ms;
}
