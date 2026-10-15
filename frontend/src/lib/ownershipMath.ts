/**
 * ownershipMath — integrated ownership of a group of owners in one company.
 *
 * The TypeScript twin of `backend/opencheck/ownership_math.py` (Phase 338,
 * extended in Phase 339): T-Rank's arithmetic as its white papers publish it
 * (docs.trank.no/white_papers) — multiply along a path, add across paths,
 * give a company's own shares back to its other holders. Line for line the
 * same algorithm, so the FullCheck band computes what the lookup froze; the
 * shared case table `integratedOwnershipCases.json` is run by both sides.
 *
 * Pure: edges in, an interval out. An interval, never a point it does not
 * have; a missing share is never a stand-in ("at least X%"); no figure rather
 * than an approximation, with the reason.
 */

export const MAX_ITERATIONS = 500;
export const TOLERANCE = 1e-12;
const OVER_100 = 1.0 + 1e-6;

export const REASON_NO_PATH = "no_path";
export const REASON_NO_SHARES = "no_shares";
export const REASON_OVER_100 = "shares_exceed_100";
export const REASON_NOT_CONVERGED = "cycle_not_settled";
export const REASON_SELF_OWNED = "self_owned";

export const REASON_WORDS: Record<string, string> = {
  [REASON_NO_PATH]: "no current ownership path reaches the company",
  [REASON_NO_SHARES]: "no share is stated on any path",
  [REASON_OVER_100]: "the stated holdings add up to more than 100%",
  [REASON_NOT_CONVERGED]: "a circular holding could not be resolved",
  [REASON_SELF_OWNED]: "a company on the path holds all of its own shares",
};

/** `owner` holds `low`–`high` (fractions, 0–1) of `owned`; `high` null when
 *  the share is not stated. */
export interface Edge {
  owner: string;
  owned: string;
  low: number;
  high: number | null;
}

export interface Integrated {
  minimum?: number;
  maximum?: number;
  exact?: number;
  incomplete?: boolean;
  reason?: string;
  via?: string[];
}

/** Python's `round(x, d)`: ties go to the even neighbour. */
export function pyRound(x: number, d: number): number {
  const m = 10 ** d;
  const y = x * m;
  const f = Math.floor(y);
  if (y - f === 0.5) return (f % 2 === 0 ? f : f + 1) / m;
  return Number(x.toFixed(d));
}

/** A BODS `share` as [low, high] fractions, or null when it states nothing. */
export function shareBounds(share: unknown): [number, number | null] | null {
  if (!share || typeof share !== "object") return null;
  const s = share as Record<string, unknown>;
  const num = (...keys: string[]): number | null => {
    for (const k of keys) {
      const v = s[k];
      if (v === null || v === undefined || typeof v === "boolean") continue;
      const f = typeof v === "number" ? v : typeof v === "string" && v.trim() !== "" ? Number(v) : NaN;
      if (!Number.isFinite(f)) continue;
      return Math.max(0, Math.min(f, 100)) / 100;
    }
    return null;
  };
  const exact = num("exact");
  if (exact !== null) return [exact, exact];
  const lo = num("minimum", "exclusiveMinimum");
  const hi = num("maximum", "exclusiveMaximum");
  if (lo === null && hi === null) return null;
  return [lo ?? 0, hi];
}

const key = (a: string, b: string) => `${a}\u0001${b}`;
const unkey = (k: string): [string, string] => k.split("\u0001") as [string, string];

function solve(
  group: Set<string>,
  target: string,
  weights: Map<string, number>,
): { value: number | null; nodes: Set<string>; reason: string | null } {
  const G = "\u0000group";
  const w = new Map<string, number>();
  const back = new Map<string, number>();
  for (const [k, v] of weights) {
    const [a, b] = unkey(k);
    if (group.has(b)) {
      if (group.size === 1 && !group.has(a)) back.set(a, (back.get(a) ?? 0) + v);
      continue;
    }
    const src = group.has(a) ? G : a;
    if (src === b && src === G) continue;
    const kk = key(src, b);
    w.set(kk, (w.get(kk) ?? 0) + v);
  }
  const direct = new Map<string, number>();
  for (const [k, v] of w) {
    const [a, b] = unkey(k);
    if (a === G) direct.set(b, (direct.get(b) ?? 0) + v);
  }
  for (const v of direct.values()) if (v > OVER_100) return { value: null, nodes: new Set(), reason: REASON_OVER_100 };

  const selfShare = new Map<string, number>();
  for (const [k, v] of w) {
    const [a, b] = unkey(k);
    if (a === b) selfShare.set(b, v);
  }
  for (const s of selfShare.values()) if (s >= 1 - 1e-12) return { value: null, nodes: new Set(), reason: REASON_SELF_OWNED };
  const outEdges = new Map<string, [string, number][]>();
  const inEdges = new Map<string, [string, number][]>();
  for (const [k, v] of w) {
    const [a, b] = unkey(k);
    if (a === b) continue;
    const s = selfShare.get(b) ?? 0;
    const val = s ? v / (1 - s) : v;
    (outEdges.get(a) ?? outEdges.set(a, []).get(a)!).push([b, val]);
    (inEdges.get(b) ?? inEdges.set(b, []).get(b)!).push([a, val]);
  }
  const walk = (starts: string[], edges: Map<string, [string, number][]>, seen: Set<string>) => {
    const stack = [...starts];
    while (stack.length) {
      const n = stack.pop()!;
      for (const [m] of edges.get(n) ?? []) {
        if (!seen.has(m)) {
          seen.add(m);
          stack.push(m);
        }
      }
    }
    return seen;
  };
  const down = walk([G], outEdges, new Set());
  const up = walk([target], inEdges, new Set());
  if (!down.has(target)) return { value: null, nodes: new Set(), reason: REASON_NO_PATH };
  const backStarts = [...back.keys()].filter((a) => down.has(a));
  const upBack = walk(backStarts, inEdges, new Set(backStarts));
  const nodes = new Set<string>([...down].filter((n) => up.has(n) || upBack.has(n)));
  nodes.add(target);

  const value = new Map<string, number>();
  for (const n of nodes) value.set(n, 0);
  let settled = false;
  for (let i = 0; i < MAX_ITERATIONS; i++) {
    let delta = 0;
    for (const n of nodes) {
      let total = 0;
      for (const [a, v] of inEdges.get(n) ?? []) {
        if (a === G) total += v;
        else if (value.has(a)) total += value.get(a)! * v;
      }
      delta = Math.max(delta, Math.abs(total - value.get(n)!));
      value.set(n, total);
    }
    if (delta < TOLERANCE) {
      settled = true;
      break;
    }
  }
  if (!settled) return { value: null, nodes: new Set(), reason: REASON_NOT_CONVERGED };
  let result = value.get(target)!;
  let returned = 0;
  for (const [a, v] of back) returned += (value.get(a) ?? 0) * v;
  if (returned >= 1 - 1e-12) return { value: null, nodes: new Set(), reason: REASON_NOT_CONVERGED };
  if (returned > 0) result /= 1 - returned;
  if (result > OVER_100) return { value: null, nodes: new Set(), reason: REASON_OVER_100 };
  const onPath = new Set([...nodes].filter((n) => up.has(n) && n !== target));
  return { value: Math.min(result, 1), nodes: onPath, reason: null };
}

function topology(group: Set<string>, target: string, pairs: Iterable<string>): { nodes: Set<string>; reason: string | null } {
  const G = "\u0000group";
  const outEdges = new Map<string, Set<string>>();
  const inEdges = new Map<string, Set<string>>();
  for (const k of pairs) {
    const [a, b] = unkey(k);
    if (group.has(b) || a === b) continue;
    const src = group.has(a) ? G : a;
    (outEdges.get(src) ?? outEdges.set(src, new Set()).get(src)!).add(b);
    (inEdges.get(b) ?? inEdges.set(b, new Set()).get(b)!).add(src);
  }
  const walk = (start: string, edges: Map<string, Set<string>>) => {
    const seen = new Set<string>();
    const stack = [start];
    while (stack.length) {
      const n = stack.pop()!;
      for (const m of edges.get(n) ?? []) {
        if (!seen.has(m)) {
          seen.add(m);
          stack.push(m);
        }
      }
    }
    return seen;
  };
  const down = walk(G, outEdges);
  const up = walk(target, inEdges);
  if (!down.has(target)) return { nodes: new Set(), reason: REASON_NO_PATH };
  return { nodes: new Set([...down].filter((n) => up.has(n) && n !== target && n !== G)), reason: null };
}

/** How much of `target` the `group` holds, directly and through companies it
 *  holds, as a percentage interval. A repeated (owner, owned) pair is one
 *  holding: the widest reading is kept, never the sum. */
export function integratedOwnership(edges: Iterable<Edge>, group: Iterable<string>, target: string): Integrated {
  const members = new Set(group);
  const pairs = new Map<string, Edge>();
  for (const e of edges) {
    const k = key(e.owner, e.owned);
    const prev = pairs.get(k);
    if (!prev) {
      pairs.set(k, e);
      continue;
    }
    const widest = prev.high === null || e.high === null ? null : Math.max(prev.high, e.high);
    pairs.set(k, { owner: e.owner, owned: e.owned, low: Math.max(prev.low, e.low), high: widest });
  }
  if (members.size === 0 || members.has(target)) return { reason: REASON_NO_PATH };

  const lowW = new Map<string, number>();
  for (const [k, e] of pairs) lowW.set(k, e.low);
  const lo = solve(members, target, lowW);
  if (lo.reason === REASON_NO_PATH) return { reason: REASON_NO_PATH };
  if (lo.value === null) return { reason: lo.reason ?? undefined };

  const topo = topology(members, target, pairs.keys());
  const onPath = new Set([...topo.nodes, ...members, target]);
  let unstated = false;
  for (const [k, e] of pairs) {
    const [a, b] = unkey(k);
    if (e.high === null && onPath.has(a) && onPath.has(b) && !members.has(b)) unstated = true;
  }
  let hi: number | null = null;
  if (!unstated) {
    const highW = new Map<string, number>();
    for (const [k, e] of pairs) highW.set(k, e.high ?? 0);
    const h = solve(members, target, highW);
    if (h.value !== null) hi = h.value;
    else if (h.reason === REASON_OVER_100) hi = 1;
  }
  const via = [...topo.nodes].sort();
  if (lo.value <= 0 && hi === null) return { reason: REASON_NO_SHARES, incomplete: true, ...(via.length ? { via } : {}) };
  const out: Integrated = { minimum: pyRound(lo.value * 100, 6) };
  if (hi !== null) out.maximum = pyRound(Math.max(hi, lo.value) * 100, 6);
  if (out.maximum !== undefined && Math.abs(out.minimum! - out.maximum) < 1e-9) out.exact = out.minimum;
  if (hi === null) out.incomplete = true;
  if (via.length) out.via = via;
  return out;
}

/** 67 → "67%", 16.666… → "16.67%" — Python's `f"{round(v, 2):g}%"`. */
export function percentText(v: number): string {
  return `${String(pyRound(v, 2))}%`;
}

/** A figure in words: "67%", "50–75%", "at least 24%", "up to 10%"; null
 *  when it states nothing. */
export function figureText(fig: { minimum?: number | null; maximum?: number | null } | null | undefined): string | null {
  if (!fig) return null;
  const lo = fig.minimum ?? null;
  const hi = fig.maximum ?? null;
  if (lo === null && hi === null) return null;
  if (lo !== null && hi !== null) {
    if (pyRound(lo, 2) === pyRound(hi, 2)) return percentText(lo);
    if (lo <= 0) return `up to ${percentText(hi)}`;
    return `${percentText(lo).slice(0, -1)}–${percentText(hi)}`;
  }
  if (lo !== null && lo > 0) return `at least ${percentText(lo)}`;
  return null;
}
