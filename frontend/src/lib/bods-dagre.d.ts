/**
 * Module shim for @openownership/bods-dagre.
 *
 * The library is a UMD-shaped webpack bundle that pollutes the global
 * scope with a ``BODSDagre`` object — *not* an ES module despite
 * ``"type": "module"`` in its package.json. We import it for side
 * effects only and read the global from ``window`` at call time.
 */
declare module "@openownership/bods-dagre";

interface BODSDagreDrawOptions {
  /** BODS v0.4 statements (array of entity / person / relationship). */
  data: unknown[];
  /** Subset of ``data`` to render — required. Pass ``data`` for "render all". */
  selectedData: unknown[];
  /** Target DOM element. The library injects its SVG into this node. */
  container: HTMLElement;
  /** URL prefix where bundled flag + icon SVGs are served. */
  imagesPath: string;
  /** Node count above which labels are suppressed for readability. */
  labelLimit?: number;
  /** Dagre layout direction — ``"TB"`` (default) | ``"LR"`` | ``"BT"`` | ``"RL"``. */
  rankDir?: "TB" | "LR" | "BT" | "RL";
  /** Disclosure-widget controls for the inspector pane. */
  viewProperties?: unknown;
  /** Enable Tippy tooltips on nodes / edges. */
  useTippy?: boolean;
}

interface Window {
  BODSDagre?: {
    /**
     * Render a directed beneficial-ownership graph into ``container``.
     *
     * The library destructures a single options object; the README's
     * positional ``(data, container, imagesPath)`` signature is out of
     * date for v0.4.x.
     */
    draw: (options: BODSDagreDrawOptions) => void;
  };
}
