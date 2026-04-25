/**
 * Module shim for @openownership/bods-dagre.
 *
 * The library is published as an ES module with no bundled types; we
 * declare just the surface we use (``draw``).
 */
declare module "@openownership/bods-dagre" {
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
  export function draw(
    data: unknown[],
    container: HTMLElement,
    imagesPath: string,
    labelLimit?: number
  ): void;
}
