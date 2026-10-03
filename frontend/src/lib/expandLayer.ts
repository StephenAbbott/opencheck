/**
 * What a FullCheck layer did, in words (Phase 234).
 *
 * `/expand-layer` used to answer every frontier node the same way: a hop that
 * failed returned nothing, and a node whose owners could not be fetched drew
 * exactly like a node with none. Since Phase 234 every LEI hop is a full lookup
 * charged to the reader's one lookup budget (shared with /lookup, the
 * watchlist and MCP), so a layer can also stop part-way. The server now names
 * both outcomes:
 *
 * - `deferred` — anchors not run because the budget was spent; they stay on
 *   the frontier and are asked for again after `retry_after_s`;
 * - `failed` — anchors that were run and could not be expanded, with a reason.
 *
 * Phase 283 adds two more:
 *
 * - `capped` — anchors past the server's per-call cap, not run; they stay on
 *   the frontier and FullCheck sends them straight back, ranked first;
 * - `degraded_sources` — what the layer's hops could not fully check, merged
 *   across hops. FullCheck accumulates it over the whole run, so a company
 *   whose screen failed is never drawn as a screened, clean one.
 *
 * This module turns that into the sentences the panel shows, and says which
 * nodes to ask for again. Pure, so the logic-only suite pins it.
 */

import type { DegradedSource, ExpandLayerResponse, LayerDegradation } from "./api";

export interface LayerFailure {
  anchor: string;
  lei?: string | null;
  status: number;
  reason: string;
}

/** The anchors the server did not run and the client should ask for again. */
export function deferredAnchors(res: Pick<ExpandLayerResponse, "deferred">): string[] {
  return res.deferred ?? [];
}

/** Seconds to wait before asking for the deferred anchors (never below 1). */
export function retryAfterSeconds(res: Pick<ExpandLayerResponse, "retry_after_s">): number {
  const s = res.retry_after_s ?? 0;
  return Number.isFinite(s) && s > 0 ? Math.ceil(s) : 1;
}

function companies(n: number): string {
  return `${n} ${n === 1 ? "company" : "companies"}`;
}

/** "3 companies could not be expanded (…)" — or null when none failed. */
export function failedSentence(failed: LayerFailure[] | undefined): string | null {
  const list = failed ?? [];
  if (list.length === 0) return null;
  const reasons = Array.from(new Set(list.map((f) => f.reason).filter(Boolean)));
  const why = reasons.length === 1 ? `: ${reasons[0].replace(/\.$/, "")}` : "";
  return `${companies(list.length)} could not be expanded${why}. Their owners are not shown, which is not the same as having none.`;
}

/** The same, when only a count is known (FullCheck's summary line). */
export function failedCountSentence(n: number): string | null {
  if (n <= 0) return null;
  return `${companies(n)} could not be expanded; their owners are not shown, which is not the same as having none.`;
}

/** "5 of 12 companies wait for your lookup budget — add the layer again in 40s." */
export function deferredSentence(
  res: Pick<ExpandLayerResponse, "deferred" | "retry_after_s" | "count">
): string | null {
  const d = deferredAnchors(res).length;
  if (d === 0) return null;
  return (
    `${d} of ${companies(res.count)} were not expanded yet: each one is a full lookup, and ` +
    `OpenCheck allows a set number a minute per reader. Add the layer again in ${retryAfterSeconds(res)}s ` +
    `to continue.`
  );
}

/** The progress line while FullCheck waits for budget mid-layer. */
export function waitingSentence(remaining: number, seconds: number): string {
  return `Waiting ${seconds}s for your lookup budget — ${companies(remaining)} left in this layer…`;
}

/** The anchors past the server's per-call cap (Phase 283). */
export function cappedAnchors(res: Pick<ExpandLayerResponse, "capped">): string[] {
  return res.capped ?? [];
}

/** "4 more companies at the edge were not expanded in this step — …", or null. */
export function cappedSentence(res: Pick<ExpandLayerResponse, "capped" | "count">): string | null {
  const n = cappedAnchors(res).length;
  if (n === 0) return null;
  return (
    `${companies(n)} at the edge ${n === 1 ? "was" : "were"} not expanded in this step — ` +
    `OpenCheck expands ${res.count} at a time, nearest the subject first. Add the layer again to continue.`
  );
}

/** FullCheck's stop reason when the session's expansion limit leaves a layer
 *  part-done. */
export function cappedStopSentence(n: number): string {
  return `the expansion limit was reached with ${companies(n)} at the edge of the network not expanded`;
}

/** What each check is, in the network notice's sentences. Mirrors
 *  `_CHECK_PHRASES` in `routers/expand.py`. */
const CHECK_PHRASES: Record<string, string> = {
  cross_source_names: "Sanctions and PEP screening",
  icij_offshore_leaks: "Offshore-leaks screening",
  openaleph_percolation: "OpenAleph screening",
  source_fetch: "The register",
  source_read: "The register's full record",
  gleif_subsidiaries: "The subsidiary list",
};

interface Accumulated {
  source_id: string;
  check: string;
  reason: DegradedSource["reason"];
  affected_signals: string[];
  hops: number;
}

/** Merge one layer's `degraded_sources` into the run's (Phase 283).
 *
 * Grouped by (source, check, reason), affected signals unioned, `hops`
 * summed — each company is expanded once per session, so the sum counts
 * companies. The detail is rewritten over the whole network ("… did not fully
 * run for 3 companies expanded in this network"): counts only, never names.
 * The result is in `DegradedSource` shape, for `DegradedScreensNotice`. */
export function accumulateDegraded(
  prev: DegradedSource[],
  incoming: LayerDegradation[] | undefined
): DegradedSource[] {
  if (!incoming || incoming.length === 0) return prev;
  const groups = new Map<string, Accumulated>();
  const add = (d: DegradedSource, hops: number) => {
    const k = `${d.source_id}|${d.check}|${d.reason}`;
    const g = groups.get(k);
    if (!g) {
      groups.set(k, {
        source_id: d.source_id,
        check: d.check,
        reason: d.reason,
        affected_signals: [...d.affected_signals],
        hops,
      });
      return;
    }
    for (const code of d.affected_signals) if (!g.affected_signals.includes(code)) g.affected_signals.push(code);
    g.hops += hops;
  };
  for (const d of prev) add(d, (d as Partial<LayerDegradation>).hops ?? 1);
  for (const d of incoming) add(d, d.hops ?? 1);
  return Array.from(groups.values()).map((g) => ({
    ...g,
    detail: `${CHECK_PHRASES[g.check] ?? "A check"} did not fully run for ${companies(g.hops)} expanded in this network`,
  }));
}
