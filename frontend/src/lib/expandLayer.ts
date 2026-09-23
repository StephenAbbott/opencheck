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
 * This module turns that into the sentences the panel shows, and says which
 * nodes to ask for again. Pure, so the logic-only suite pins it.
 */

import type { ExpandLayerResponse } from "./api";

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
