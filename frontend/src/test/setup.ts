/**
 * Shared setup for the test suites (Phase 168).
 *
 * Loaded for every test file, including the `node`-environment `lib/` ones,
 * so everything here has to be harmless without a DOM: registering matchers
 * is, touching `document` is not — hence the guard.
 */
import "@testing-library/jest-dom/vitest";
import { afterEach } from "vitest";

if (typeof document !== "undefined") {
  const { cleanup } = await import("@testing-library/react");
  // Without this a second `render()` in the same file finds the first one's
  // markup still in the document, and a "renders once" assertion — which is
  // the assertion the double verdict needed — passes while counting two.
  afterEach(() => cleanup());

  // jsdom implements neither, and both are called during a normal render:
  // matchMedia by the responsive helpers, ResizeObserver by the graph.
  if (!window.matchMedia) {
    window.matchMedia = ((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    })) as unknown as typeof window.matchMedia;
  }
  if (!window.ResizeObserver) {
    window.ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
  }
  if (!window.scrollTo) {
    window.scrollTo = (() => {}) as typeof window.scrollTo;
  }
}
