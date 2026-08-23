/**
 * What the verdict strip's ownership-network column is allowed to say.
 *
 * The column is an invitation into FullCheck with numbers on it, and numbers
 * on an invitation are a claim. `graph_shape` (backend, `_graph_shape` in
 * `routers/lookup.py`) counts the statements *this* check mapped — deduplicated
 * by statementId, because several sources describe the same party — and never
 * the GLEIF subsidiary total or anything FullCheck would go on to discover.
 *
 * This module decides when there is enough to render at all. Two rules, both
 * of which exist because the alternative reads as a finding:
 *
 * - **One company is not a network.** Every lookup produces at least the
 *   subject's own entity statement, so a card saying "1 company" would appear
 *   on every report including the ones where nothing else was found — an
 *   invitation to explore a graph with a single node in it.
 * - **Depth is only spoken when it was measured.** `COMPLEX_OWNERSHIP_LAYERS`
 *   carries the longest chain the risk layer actually walked. Absent, the
 *   column says nothing about depth rather than saying "1 layer deep", which
 *   would state a flat graph the check never established.
 */

import type { GraphShape } from "./api";

export interface NetworkSummary {
  companies: number;
  people: number;
  relationships: number;
  /** "four layers deep", or null when no chain was measured. */
  depthPhrase: string | null;
}

/** Smallest graph worth calling a network: the subject plus something else. */
const MIN_COMPANIES = 2;

const DEPTH_WORD = [
  "",
  "one",
  "two",
  "three",
  "four",
  "five",
  "six",
  "seven",
  "eight",
  "nine",
  "ten",
];

/** "four layers deep" — words up to ten, digits past it, so a 14-layer chain
 *  reads as "14 layers deep" rather than as an invented word. */
export function depthPhrase(depth: number | null | undefined): string | null {
  if (typeof depth !== "number" || !Number.isFinite(depth) || depth < 1) return null;
  const n = Math.floor(depth);
  const word = DEPTH_WORD[n] ?? String(n);
  return `${word} ${n === 1 ? "layer" : "layers"} deep`;
}

/**
 * The column's content, or `null` when there is nothing honest to put in it.
 *
 * `null` covers three cases that are all the same case: the event has not
 * landed, the backend is older than `graph_shape`, or the graph is just the
 * subject. In each of them the check has not established that a network
 * exists, so it must not offer to show one.
 */
export function networkSummary(shape?: GraphShape | null): NetworkSummary | null {
  if (!shape) return null;
  const companies = numberOr(shape.companies);
  const people = numberOr(shape.people);
  const relationships = numberOr(shape.relationships);
  // Relationships matter as well as companies: two unconnected entity
  // statements from two sources are two records of the same subject, not a
  // network, and the ownership view would render them side by side with no
  // edge between them.
  if (companies < MIN_COMPANIES && people === 0) return null;
  if (relationships === 0) return null;
  return { companies, people, relationships, depthPhrase: depthPhrase(shape.depth) };
}

function numberOr(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? Math.floor(value)
    : 0;
}
