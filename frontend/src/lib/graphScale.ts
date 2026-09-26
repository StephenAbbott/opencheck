/**
 * The graph at scale (Phase 243) — sibling clusters and wrapped ranks.
 *
 * Shell's FullCheck network is sixteen owners and officers, SHELL PLC, and 132
 * subsidiaries that GLEIF, OpenCorporates and OpenSanctions report under it —
 * every one a leaf with a single parent. Dagre lays a rank out as one row, so
 * the canvas was two rows a few thousand pixels wide: at "Fit" the labels were
 * about 3px, the nodes 12px, and most of the canvas was empty (Opus 5.5 check,
 * D-H3).
 *
 * Two display transforms, both pure so the logic-only suite can pin them:
 *
 * - **`findSiblingClusters`** — on a rank wider than `RANK_CLUSTER_THRESHOLD`,
 *   the leaf siblings one hub shares, grouped by the source and kind of the
 *   edge that joins them ("GLEIF: 103 subsidiaries"), become one expandable
 *   node. A node carrying a risk signal is **never** put in a cluster (Stephen,
 *   24 Sept 2026): a badge hidden inside a count is a finding the reader cannot
 *   see, which is the "checked and clean" failure Phase 109 closed.
 * - **`wrapWideRanks`** — after layout, a rank of leaves (or of roots) wider
 *   than `WRAP_MAX_COLS` is folded into rows, and the ranks beyond it are moved
 *   out of the way. That keeps an expanded cluster, or sixteen officers, from
 *   stretching the canvas back into a strip.
 *
 * Neither touches the model. The tree under the canvas ("Read as text") still
 * lists every row: the text equivalent is complete whatever the picture groups.
 */

import { computeLevels, computeVisibility, nodesWithChildren, type EdgeCategory, type GraphModel } from "./bodsGraph";

/** A rank with more nodes than this is a candidate for clustering. */
export const RANK_CLUSTER_THRESHOLD = 40;
/** A group smaller than this stays as individual nodes. */
export const MIN_CLUSTER_SIZE = 5;
/** A wrapped rank holds at most this many nodes per row. */
export const WRAP_MAX_COLS = 10;

export type ClusterDirection = "below" | "above";

export interface SiblingCluster {
  /** Stable across network growth: hub + direction + source + kind + ended. */
  id: string;
  hub: string;
  /** "below" = the hub's leaf children; "above" = its leaf parents. */
  direction: ClusterDirection;
  /** The source that reported the joining edges, shortened for a label. */
  source: string;
  category: EdgeCategory;
  ended: boolean;
  members: string[];
  label: string;
  /** The joining edges' shared label, when every member's edge says the same
   *  thing ("Consolidated (direct)"); empty when they differ. */
  edgeLabel: string;
}

const NOUN_BELOW = "subsidiaries";
const NOUN_ABOVE: Record<EdgeCategory, string> = {
  ownership: "owners",
  control: "controllers",
  role: "officers",
  unknown: "related parties",
  possiblySame: "related parties",
};

/** A source's display name, short enough for a node label. */
export function shortSourceName(name: string): string {
  let s = name.split(" (")[0].split(",")[0].trim();
  if (s.length > 32) s = `${s.slice(0, 31).trimEnd()}…`;
  return s || "Unattributed";
}

export function clusterLabel(source: string, n: number, direction: ClusterDirection, category: EdgeCategory, ended: boolean): string {
  const noun = direction === "below" ? NOUN_BELOW : NOUN_ABOVE[category];
  return `${source}: ${n} ${ended ? "former " : ""}${noun}`;
}

/**
 * The clusters for one model. `flagged` are the node ids carrying a signal
 * badge and `keep` any the reader has pointed at (the selected node) — neither
 * is ever grouped.
 */
export function findSiblingClusters(
  model: GraphModel,
  {
    flagged = new Set<string>(),
    keep = new Set<string>(),
    rankThreshold = RANK_CLUSTER_THRESHOLD,
    minSize = MIN_CLUSTER_SIZE,
  }: {
    flagged?: Set<string>;
    keep?: Set<string>;
    rankThreshold?: number;
    minSize?: number;
  } = {},
): SiblingCluster[] {
  const levels = computeLevels(model);
  const width = new Map<number, number>();
  for (const l of levels.values()) width.set(l, (width.get(l) ?? 0) + 1);

  const parentEdges = new Map<string, typeof model.edges>();
  const childEdges = new Map<string, typeof model.edges>();
  for (const e of model.edges) {
    (parentEdges.get(e.target) ?? parentEdges.set(e.target, []).get(e.target)!).push(e);
    (childEdges.get(e.source) ?? childEdges.set(e.source, []).get(e.source)!).push(e);
  }

  const groups = new Map<string, { hub: string; direction: ClusterDirection; source: string; category: EdgeCategory; ended: boolean; members: string[]; labels: Set<string> }>();
  for (const n of model.nodes) {
    if (flagged.has(n.id) || keep.has(n.id)) continue;
    if ((width.get(levels.get(n.id) ?? 0) ?? 0) <= rankThreshold) continue;
    const ups = parentEdges.get(n.id) ?? [];
    const downs = childEdges.get(n.id) ?? [];
    let edge;
    let direction: ClusterDirection;
    if (downs.length === 0 && ups.length > 0 && new Set(ups.map((e) => e.source)).size === 1) {
      edge = ups[0];
      direction = "below";
    } else if (ups.length === 0 && downs.length > 0 && new Set(downs.map((e) => e.target)).size === 1) {
      edge = downs[0];
      direction = "above";
    } else continue;
    const hub = direction === "below" ? edge.source : edge.target;
    const source = shortSourceName(edge.sources[0] ?? "");
    const ended = edge.ended === true;
    const key = [hub, direction, source, edge.category, ended ? "ended" : "current"].join("~");
    const g = groups.get(key) ?? { hub, direction, source, category: edge.category, ended, members: [], labels: new Set<string>() };
    g.members.push(n.id);
    g.labels.add(edge.label);
    groups.set(key, g);
  }

  const clusters: SiblingCluster[] = [];
  for (const [key, g] of groups) {
    if (g.members.length < minSize) continue;
    clusters.push({
      id: `cluster~${key}`,
      hub: g.hub,
      direction: g.direction,
      source: g.source,
      category: g.category,
      ended: g.ended,
      members: g.members,
      label: clusterLabel(g.source, g.members.length, g.direction, g.category, g.ended),
      edgeLabel: g.labels.size === 1 ? [...g.labels][0] : "",
    });
  }
  // Largest first, so the reader meets the biggest group first in the tab order.
  return clusters.sort((a, b) => b.members.length - a.members.length || a.id.localeCompare(b.id));
}

/** node id → the id of the (unexpanded) cluster that hides it. */
export function hiddenByCluster(clusters: SiblingCluster[], expanded: Set<string>): Map<string, string> {
  const map = new Map<string, string>();
  for (const c of clusters) {
    if (expanded.has(c.id)) continue;
    for (const m of c.members) map.set(m, c.id);
  }
  return map;
}

// ---------------------------------------------------------------------------
// Wrapping wide ranks after layout
// ---------------------------------------------------------------------------

export interface PlacedNode {
  id: string;
  x: number;
  y: number;
  /** No visible edge leaves it downwards (a leaf of the drawn graph). */
  leaf: boolean;
  /** No visible edge reaches it from above (a root of the drawn graph). */
  root: boolean;
  /** Drawn width, so a row of wide cluster boxes is spaced for them. */
  w?: number;
}

export interface WrapOptions {
  /** Rows never hold fewer than this before a rank is folded. */
  maxCols?: number;
  /** The canvas's width ÷ height. A long rank folds into rows that match it,
   *  so 94 subsidiaries become 14 × 7, not 10 × 10 in a corner. */
  aspect?: number;
  colSep?: number;
  rowSep?: number;
  /**
   * Phase 250: the canvas's width and its tallest height, in px. Given, a
   * rank is folded into however many columns lets "Fit" zoom furthest in —
   * which can mean folding a rank narrower than `maxCols` — and only when
   * the unfolded graph would fit below `FOLD_ZOOM`. Shell's 17 owners and
   * officers were two rows of 8 at zoom 0.58; three rows of 6 fit at 0.8.
   */
  viewport?: { width: number; height: number };
}

/** Folding for the viewport only starts below this fit zoom. */
export const FOLD_ZOOM = 0.9;
/** Fit padding, per side, in px (`cy.fit(…, 32)`). */
const FIT_PAD = 32;
/** How far a node and its label reach round its centre, in graph units. */
const REACH = { x: 70, up: 45, down: 110 };

interface Extent {
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
}

function extentOf(nodes: { x: number; y: number; w?: number }[]): Extent {
  const e = { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity };
  for (const n of nodes) {
    const half = Math.max(REACH.x, (n.w ?? 0) / 2);
    e.minX = Math.min(e.minX, n.x - half);
    e.maxX = Math.max(e.maxX, n.x + half);
    e.minY = Math.min(e.minY, n.y - REACH.up);
    e.maxY = Math.max(e.maxY, n.y + REACH.down);
  }
  return e;
}

/** The zoom "Fit" would reach for a drawing `w` × `h` in `viewport`. */
export function fitZoom(w: number, h: number, viewport: { width: number; height: number }): number {
  return Math.min((viewport.width - 2 * FIT_PAD) / w, (viewport.height - 2 * FIT_PAD) / h);
}

/**
 * Rows for folding `rank` so the whole drawing fits at the highest zoom,
 * or 1 (leave it) when folding gains under 5% or the drawing already fits
 * at `FOLD_ZOOM`. `extraH` is the height earlier folds have already added.
 */
function rowsForViewport(
  rank: PlacedNode[],
  all: PlacedNode[],
  viewport: { width: number; height: number },
  colSep: number,
  rowSep: number,
  extraH: number,
): number {
  const whole = extentOf(all);
  const others = extentOf(all.filter((n) => !rank.includes(n)));
  const xs = rank.map((n) => n.x);
  const cx = (Math.min(...xs) + Math.max(...xs)) / 2;
  const baseH = whole.maxY - whole.minY + extraH;
  const zoomFor = (cols: number) => {
    const rows = Math.ceil(rank.length / cols);
    const half = ((Math.min(cols, rank.length) - 1) * colSep) / 2 + REACH.x;
    const w = Math.max(others.maxX, cx + half) - Math.min(others.minX, cx - half);
    return fitZoom(w, baseH + (rows - 1) * rowSep, viewport);
  };
  const flat = zoomFor(rank.length);
  if (flat >= FOLD_ZOOM) return 1;
  let bestCols = rank.length;
  let best = flat;
  for (let cols = rank.length - 1; cols >= 2; cols -= 1) {
    const z = zoomFor(cols);
    if (z > best * 1.01) {
      best = z;
      bestCols = cols;
    }
  }
  return best > flat * 1.05 ? Math.ceil(rank.length / bestCols) : 1;
}

/**
 * Fold every rank that is all leaves (downwards) or all roots (upwards) and
 * wider than `maxCols` into rows of at most `maxCols`, centred where the rank
 * was, and move the ranks beyond it by the height it grew. A rank that mixes
 * leaves with nodes that have children is left alone: moving a parent would
 * drag its subtree's edges across the rows.
 *
 * Returns id → new position for every node given.
 */
export function wrapWideRanks(
  nodes: PlacedNode[],
  { maxCols = WRAP_MAX_COLS, aspect = 0, colSep = 140, rowSep = 150, viewport }: WrapOptions = {},
): Map<string, { x: number; y: number }> {
  const pos = new Map(nodes.map((n) => [n.id, { x: n.x, y: n.y }]));
  const ranks = new Map<number, PlacedNode[]>();
  for (const n of nodes) {
    const y = Math.round(n.y);
    (ranks.get(y) ?? ranks.set(y, []).get(y)!).push(n);
  }
  const ys = [...ranks.keys()].sort((a, b) => a - b);

  // How many rows a rank folds into: the Phase 243 rule (wider than
  // `maxCols`, at the canvas's proportions), or — with a viewport — whichever
  // fold lets Fit zoom furthest in, and never fewer rows than the old rule.
  const foldRows = (rank: PlacedNode[], extraH: number): number => {
    const legacy = rank.length > maxCols
      ? Math.ceil(rank.length / colsFor(rank.length, maxCols, aspect, colSep, rowSep))
      : 1;
    if (!viewport) return legacy;
    return Math.max(legacy, rowsForViewport(rank, nodes, viewport, sepFor(rank, colSep), rowSep, extraH));
  };

  // Downward folds, top to bottom; each pushes everything under it down.
  let pushDown = 0;
  const shiftBelow = new Map<number, number>();
  for (const y of ys) {
    shiftBelow.set(y, pushDown);
    const rank = ranks.get(y)!;
    if (!rank.every((n) => n.leaf) || rank.length < 3) continue;
    const rows = foldRows(rank, pushDown);
    if (rows <= 1) continue;
    placeRows(rank, rows, sepFor(rank, colSep), rowSep, 1, pos);
    pushDown += (rows - 1) * rowSep;
  }
  // Upward folds, bottom to top; each pushes everything above it up.
  let pushUp = 0;
  const shiftAbove = new Map<number, number>();
  for (const y of [...ys].reverse()) {
    shiftAbove.set(y, pushUp);
    const rank = ranks.get(y)!;
    if (!rank.every((n) => n.root) || rank.every((n) => n.leaf) || rank.length < 3) continue;
    const rows = foldRows(rank, pushDown + pushUp);
    if (rows <= 1) continue;
    placeRows(rank, rows, sepFor(rank, colSep), rowSep, -1, pos);
    pushUp += (rows - 1) * rowSep;
  }
  for (const [y, rank] of ranks) {
    const dy = (shiftBelow.get(y) ?? 0) - (shiftAbove.get(y) ?? 0);
    if (dy === 0) continue;
    for (const n of rank) {
      const p = pos.get(n.id)!;
      pos.set(n.id, { x: p.x, y: p.y + dy });
    }
  }
  return pos;
}

/** Columns for a folded rank of `n`: at least `maxCols`, more when the
 *  canvas is wide enough that a squarer block would waste it. */
function colsFor(n: number, maxCols: number, aspect: number, colSep: number, rowSep: number): number {
  if (aspect <= 0) return maxCols;
  return Math.max(maxCols, Math.ceil(Math.sqrt((n * aspect * rowSep) / colSep)));
}

/** Column spacing that clears the widest node in the rank. */
function sepFor(rank: PlacedNode[], colSep: number): number {
  const widest = Math.max(0, ...rank.map((n) => n.w ?? 0));
  return Math.max(colSep, widest + 20);
}

function placeRows(
  rank: PlacedNode[],
  rows: number,
  colSep: number,
  rowSep: number,
  dir: 1 | -1,
  pos: Map<string, { x: number; y: number }>,
): void {
  const sorted = [...rank].sort((a, b) => a.x - b.x);
  const cols = Math.ceil(sorted.length / rows);
  const cx = (sorted[0].x + sorted[sorted.length - 1].x) / 2;
  const y0 = sorted[0].y;
  sorted.forEach((n, i) => {
    const r = Math.floor(i / cols);
    const inRow = Math.min(cols, sorted.length - r * cols);
    const c = i - r * cols;
    pos.set(n.id, { x: cx + (c - (inRow - 1) / 2) * colSep, y: y0 + dir * r * rowSep });
  });
}

// ---------------------------------------------------------------------------
// Readable at Fit (Phase 250)
// ---------------------------------------------------------------------------

/** Node labels are drawn at this size in the graph's own units. */
export const LABEL_FONT_PX = 11;
/** The smallest a node label may render at Fit before the canvas acts. */
export const READABLE_LABEL_PX = 9;
/** Labels grow at most to this, in graph units: past it a two-line name no
 *  longer fits the gap between rows and the labels start covering nodes. */
export const LABEL_FONT_MAX_PX = 14;
/** Room a label may take across, in graph units, at the base size. */
export const LABEL_MAX_WIDTH = 120;

/** Whether labels at `zoom` render below `READABLE_LABEL_PX`. */
export function labelsUnreadable(zoom: number, font = LABEL_FONT_PX): boolean {
  return font * zoom < READABLE_LABEL_PX - 0.05;
}

/**
 * The label size, in graph units, that renders at `READABLE_LABEL_PX` at
 * `zoom` — never below the base size, never above `LABEL_FONT_MAX_PX`.
 *
 * Shell PLC's FullCheck fits at 0.59: 11px labels render at 6.5px. Growing
 * the label in graph units as the fit zooms out keeps what is drawn at
 * reading size; the cap is where it would start to collide instead.
 */
export function labelFontFor(zoom: number): number {
  if (!(zoom > 0)) return LABEL_FONT_PX;
  const want = Math.ceil((READABLE_LABEL_PX / zoom) * 10) / 10;
  return Math.min(LABEL_FONT_MAX_PX, Math.max(LABEL_FONT_PX, want));
}

/**
 * The canvas height that fits a laid-out graph of `bbox` at `width` without
 * leaving most of it empty — clamped, so a two-node graph is not a sliver and
 * a tall one does not take over the page.
 */
export const CANVAS_MIN_HEIGHT = 360;
export const CANVAS_MAX_HEIGHT = 640;

export function canvasHeightFor(
  bbox: { w: number; h: number },
  width: number,
  { min = CANVAS_MIN_HEIGHT, max = CANVAS_MAX_HEIGHT }: { min?: number; max?: number } = {},
): number {
  if (bbox.w <= 0 || width <= 0) return min;
  return Math.round(Math.min(max, Math.max(min, (width * bbox.h) / bbox.w)));
}

// ---------------------------------------------------------------------------
// The overlay as one tab stop (WCAG 2.4.3 / 2.1.1)
// ---------------------------------------------------------------------------

/** Minimum pointer target for an overlay control (WCAG 2.5.8, AA). */
export const MIN_TARGET_PX = 24;
/** Smallest type an overlay mark is drawn at, whatever the zoom. */
export const MIN_BADGE_FONT_PX = 11;

/**
 * A hit box at least `MIN_TARGET_PX` square, centred on the visible mark —
 * the mark keeps its drawn size inside a transparent wrapper.
 */
export function hitBox(cx: number, cy: number, w: number, h: number): { left: number; top: number; width: number; height: number } {
  const width = Math.max(MIN_TARGET_PX, w);
  const height = Math.max(MIN_TARGET_PX, h);
  return { left: cx - width / 2, top: cy - height / 2, width, height };
}

// ---------------------------------------------------------------------------
// Marks that do not collide (Phase 250)
// ---------------------------------------------------------------------------

/**
 * The drawn size of a signal badge on a node of screen radius `r`. The badge
 * keeps a floor in pixels whatever the zoom (its type never drops below
 * `MIN_BADGE_FONT_PX`), which is exactly why marks collide on a zoomed-out
 * network: the nodes shrink and the marks do not. The width is estimated
 * generously from the text, so a collision check built on it errs apart.
 */
export function signalPillSize(r: number, text: string, stacked: boolean): { fontPx: number; w: number; h: number } {
  const badgePx = Math.max(18, r * 0.55);
  const fontPx = Math.max(MIN_BADGE_FONT_PX, badgePx * 0.42);
  const h = Math.max(badgePx * 0.9, fontPx + 6);
  const floor = Math.max(stacked ? badgePx * 1.5 : badgePx * 1.8, fontPx * 2.4);
  const w = Math.max(floor, fontPx * 0.72 * text.length + fontPx);
  return { fontPx, w, h };
}

/** The drawn size of a collapse / group toggle ("−", "+132"). */
export function togglePillSize(r: number, label: string): { fontPx: number; w: number; h: number } {
  const h = Math.max(16, r * 0.42);
  const fontPx = Math.max(MIN_BADGE_FONT_PX, h * 0.55);
  const w = Math.max(h * 2, fontPx * (label.length * 0.62 + 1.4));
  return { fontPx, w, h };
}

export interface Rect {
  left: number;
  top: number;
  width: number;
  height: number;
}

/** A mark's hit box, the node it belongs to, and which way it may move. */
export interface MarkBox extends Rect {
  key: string;
  owner: string;
  /** Signal badges sit above their node and move up; toggles hang below and move down. */
  move: "up" | "down";
}

function overlaps(a: Rect, b: Rect, gap: number): boolean {
  return (
    a.left < b.left + b.width + gap &&
    b.left < a.left + a.width + gap &&
    a.top < b.top + b.height + gap &&
    b.top < a.top + a.height + gap
  );
}

/**
 * Vertical offsets that keep every mark's hit box clear of every other mark
 * and of every other node (Phase 250).
 *
 * After "+1 layer" on Shell PLC the canvas fits a network ten times wider at a
 * third of the zoom: nodes 42px apart carrying badges 35px wide, packed
 * shoulder to shoulder, and a group's "+174" pill across the next node and its
 * badge. Moving nodes would need the zoom, which needs the layout — so the
 * marks move instead, in screen space, after the fit: a badge rises clear of
 * whatever it would cover, a toggle drops. Marks are placed top to bottom,
 * left to right, so the result does not depend on model order. A mark never
 * avoids its own node — it sits on that node's rim by design.
 *
 * Returns `key → dy` for the marks that moved.
 */
export function resolveMarkCollisions(
  marks: MarkBox[],
  obstacles: (Rect & { owner: string })[],
  { gap = 2, maxSteps = 8 }: { gap?: number; maxSteps?: number } = {},
): Map<string, number> {
  const placed: (Rect & { owner: string; key?: string })[] = [];
  const moved = new Map<string, number>();
  const order = [...marks].sort((a, b) =>
    Math.abs(a.top - b.top) > 1 ? a.top - b.top : a.left - b.left,
  );
  for (const m of order) {
    let dy = 0;
    for (let step = 0; step < maxSteps; step += 1) {
      const at = { left: m.left, top: m.top + dy, width: m.width, height: m.height };
      const hit =
        placed.find((p) => overlaps(at, p, gap)) ??
        obstacles.find((o) => o.owner !== m.owner && overlaps(at, o, gap));
      if (!hit) break;
      dy =
        m.move === "up"
          ? hit.top - gap - (m.top + m.height)
          : hit.top + hit.height + gap - m.top;
    }
    if (dy !== 0) moved.set(m.key, dy);
    placed.push({ left: m.left, top: m.top + dy, width: m.width, height: m.height, owner: m.owner, key: m.key });
  }
  return moved;
}

/**
 * The overlay's controls in reading order — top to bottom, then left to
 * right, rows grouped within `rowTolerance` px — so the arrow keys move the
 * way the eye does rather than in model order.
 */
export function readingOrder<T extends { cx: number; cy: number }>(items: T[], rowTolerance = 24): T[] {
  return [...items].sort((a, b) =>
    Math.abs(a.cy - b.cy) > rowTolerance ? a.cy - b.cy : a.cx - b.cx,
  );
}

/**
 * Where a roving-tabindex key press moves focus. `null` for a key the group
 * does not handle, so the browser's own behaviour (Tab out) survives.
 */
export function rovingTarget(count: number, index: number, key: string): number | null {
  if (count === 0) return null;
  switch (key) {
    case "ArrowRight":
    case "ArrowDown":
      return (index + 1) % count;
    case "ArrowLeft":
    case "ArrowUp":
      return (index - 1 + count) % count;
    case "Home":
      return 0;
    case "End":
      return count - 1;
    default:
      return null;
  }
}

/** Below this width the text equivalent starts open (Phase 243). */
export const TEXT_FIRST_MAX_WIDTH_PX = 639;

/** True on a phone-width viewport. False wherever there is no `window` or no
 *  `matchMedia` (tests, server rendering) — the desktop default. */
export function prefersTextFirst(
  win: { matchMedia?: (q: string) => { matches: boolean } } | undefined =
    typeof window === "undefined" ? undefined : window,
): boolean {
  try {
    return win?.matchMedia?.(`(max-width: ${TEXT_FIRST_MAX_WIDTH_PX}px)`).matches ?? false;
  } catch {
    return false;
  }
}

/**
 * The nodes whose collapse toggle would do something (Phase 243): collapsing
 * hides at least one node, or the node is collapsed already (so it can be
 * opened). In a DAG a parent whose children all have another visible parent
 * hides nothing — Shell's sixteen officers each carried a "−" that did
 * nothing, sixteen more controls in the way of the ones that do.
 */
export function collapsibleNodes(model: GraphModel, collapsed: Set<string>): Set<string> {
  const base = computeVisibility(model, collapsed).hidden.size;
  const out = new Set<string>();
  for (const id of nodesWithChildren(model)) {
    if (collapsed.has(id)) {
      out.add(id);
      continue;
    }
    const next = new Set(collapsed).add(id);
    if (computeVisibility(model, next).hidden.size > base) out.add(id);
  }
  return out;
}

/**
 * The collapsed set with every collapsed ancestor of `id` opened, so the
 * canvas draws `id` (Phase 250). Returns `collapsed` itself when nothing
 * hides it, so a React state update is a no-op.
 *
 * The text version stopped following the canvas's collapse in Phase 250
 * (Stephen, 26 Sept 2026: collapsing is a canvas control; the text lists
 * every row) — so a row chosen there can name a node the canvas is hiding.
 */
export function revealIn(model: GraphModel, collapsed: Set<string>, id: string): Set<string> {
  if (collapsed.size === 0) return collapsed;
  const parents = new Map<string, string[]>();
  for (const e of model.edges) (parents.get(e.target) ?? parents.set(e.target, []).get(e.target)!).push(e.source);
  const ancestors = new Set<string>();
  const stack = [...(parents.get(id) ?? [])];
  while (stack.length) {
    const u = stack.pop()!;
    if (ancestors.has(u)) continue;
    ancestors.add(u);
    stack.push(...(parents.get(u) ?? []));
  }
  const open = [...collapsed].filter((c) => ancestors.has(c));
  if (open.length === 0) return collapsed;
  const next = new Set(collapsed);
  for (const c of open) next.delete(c);
  return next;
}
