import { describe, expect, it, vi } from "vitest";
import cytoscape from "cytoscape";
import { syncOverlayWithGraph, type FrameScheduler } from "./overlaySync";

/** A frame scheduler the test steps by hand. */
function manualFrames() {
  let next = 1;
  const queued = new Map<number, () => void>();
  const frames: FrameScheduler = {
    request: (cb) => { const id = next++; queued.set(id, cb); return id; },
    cancel: (id) => { queued.delete(id); },
  };
  const flush = () => {
    const cbs = [...queued.values()];
    queued.clear();
    cbs.forEach((cb) => cb());
  };
  return { frames, flush, pending: () => queued.size };
}

function graph() {
  return cytoscape({
    headless: true,
    elements: [
      { data: { id: "a" }, position: { x: 0, y: 0 } },
      { data: { id: "b" }, position: { x: 100, y: 0 } },
      { data: { id: "ab", source: "a", target: "b" } },
    ],
  });
}

describe("syncOverlayWithGraph", () => {
  it("re-places the overlay when a node is dragged (no viewport change)", () => {
    // The bug: a drag moves a node but fires no `viewport` event, so the
    // icon and flag stayed where the node had been.
    const cy = graph();
    const { frames, flush } = manualFrames();
    const update = vi.fn();
    syncOverlayWithGraph(cy, update, frames);

    cy.getElementById("a").position({ x: 40, y: 25 });
    expect(update).not.toHaveBeenCalled(); // coalesced to the next frame
    flush();
    expect(update).toHaveBeenCalledTimes(1);
  });

  it("coalesces many drag steps into one update per frame", () => {
    const cy = graph();
    const { frames, flush, pending } = manualFrames();
    const update = vi.fn();
    syncOverlayWithGraph(cy, update, frames);

    for (let i = 1; i <= 20; i += 1) cy.getElementById("a").position({ x: i, y: i });
    cy.getElementById("b").position({ x: 300, y: 0 });
    expect(pending()).toBe(1);
    flush();
    expect(update).toHaveBeenCalledTimes(1);

    cy.getElementById("a").position({ x: 99, y: 99 });
    flush();
    expect(update).toHaveBeenCalledTimes(2);
  });

  it("keeps pan and zoom synchronous", () => {
    const cy = graph();
    const { frames } = manualFrames();
    const update = vi.fn();
    syncOverlayWithGraph(cy, update, frames);

    cy.pan({ x: 10, y: 10 });
    expect(update).toHaveBeenCalledTimes(1);
    cy.zoom(2);
    expect(update).toHaveBeenCalledTimes(2);
  });

  it("stops after unbinding, cancelling a queued frame", () => {
    const cy = graph();
    const { frames, flush, pending } = manualFrames();
    const update = vi.fn();
    const unbind = syncOverlayWithGraph(cy, update, frames);

    cy.getElementById("a").position({ x: 5, y: 5 });
    unbind();
    expect(pending()).toBe(0); // a frame already queued is cancelled
    cy.getElementById("a").position({ x: 6, y: 6 });
    cy.pan({ x: 3, y: 3 });
    flush();
    expect(update).not.toHaveBeenCalled();
  });
});
