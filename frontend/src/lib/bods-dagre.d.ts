/**
 * Module shim for @openownership/bods-dagre.
 *
 * The library is a UMD-shaped webpack bundle that pollutes the global
 * scope with a ``BODSDagre`` object — *not* an ES module despite
 * ``"type": "module"`` in its package.json. We import it for side
 * effects only and read the global from ``window`` at call time.
 */
declare module "@openownership/bods-dagre";

interface Window {
  BODSDagre?: {
    /**
     * Render a directed beneficial-ownership graph into ``container``.
     *
     * @param data BODS v0.4 statements (array of entity / person /
     *             relationship statements).
     * @param container Target DOM element. The library replaces its
     *                  contents with the generated SVG.
     * @param imagesPath URL prefix where bundled flag + icon SVGs are
     *                   served (e.g. "/bods-dagre-images").
     * @param labelLimit Optional — node count above which labels are
     *                   suppressed for readability.
     */
    draw: (
      data: unknown[],
      container: HTMLElement,
      imagesPath: string,
      labelLimit?: number
    ) => void;
  };
}
