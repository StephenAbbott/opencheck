/**
 * Keep the HTML overlay layer (BOVS icon, jurisdiction flag, risk badge,
 * collapse toggle) in step with the Cytoscape canvas beneath it.
 *
 * The overlay is positioned from each node's model position, the zoom and
 * the pan. A pan or zoom fires Cytoscape's `viewport` event, but dragging a
 * node moves it without touching the viewport, so listening to `viewport`
 * alone left a dragged node's icon and flag behind until the next pan/zoom
 * (most visibly on touch screens, where a tap elsewhere nudges the viewport
 * and snaps them back). Node `position` events fire on every drag step and on
 * any programmatic move, so they are subscribed too.
 *
 * Both are coalesced into at most one update per animation frame: a drag
 * fires `position` far more often than the screen repaints, and a re-layout
 * fires it once per node.
 */
import type { Core } from "cytoscape";

export interface FrameScheduler {
  request: (cb: () => void) => number;
  cancel: (id: number) => void;
}

const browserFrames: FrameScheduler = {
  request: (cb) =>
    typeof requestAnimationFrame === "function"
      ? requestAnimationFrame(cb)
      : (setTimeout(cb, 16) as unknown as number),
  cancel: (id) =>
    typeof cancelAnimationFrame === "function" ? cancelAnimationFrame(id) : clearTimeout(id),
};

/** Subscribe `update` to viewport changes and node moves; returns an unbind. */
export function syncOverlayWithGraph(
  cy: Core,
  update: () => void,
  frames: FrameScheduler = browserFrames,
): () => void {
  let pending: number | null = null;
  const schedule = () => {
    if (pending !== null) return;
    pending = frames.request(() => {
      pending = null;
      update();
    });
  };
  // Pan/zoom stays synchronous, as it was: the overlay must not trail the
  // canvas by a frame while the reader pinches or scrolls.
  cy.on("viewport", update);
  cy.on("position", "node", schedule);
  return () => {
    cy.off("viewport", update);
    cy.off("position", "node", schedule);
    if (pending !== null) frames.cancel(pending);
    pending = null;
  };
}
