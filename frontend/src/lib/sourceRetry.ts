/**
 * The per-source retry's values layer (Phase 331).
 *
 * `/lookup-source` re-runs one source after the run has finished. Two things
 * on the page describe that source and were left describing the attempt that
 * failed:
 *
 *  - the freshness chip, fed from `sourceLiveness`, which was set once from
 *    the run's `risk_signals` event. A failed source carries stub provenance,
 *    so a card the retry had just filled with real register data read
 *    "Placeholder data". The response now carries `source_liveness`, the entry
 *    the run itself would have given the source, and it is merged over the
 *    run's map here;
 *  - the verdict, the degraded-sources notice and the coverage count, which
 *    were worked out from the run's own events. A retry cannot change them —
 *    they are the server's sentence about one run — so the page says they
 *    predate the retry rather than letting them read as current.
 */
import type { LookupSourceResponse, SourceLiveness } from "./api";
import { sourceList } from "./vocab";

/** The run's liveness map with the retry's own entries laid over it. A
 *  response without `source_liveness` (an older server) changes nothing. */
export function mergeRetryLiveness(
  prev: Record<string, SourceLiveness>,
  res: Pick<LookupSourceResponse, "source_liveness">,
): Record<string, SourceLiveness> {
  const fresh = res.source_liveness;
  if (!fresh || Object.keys(fresh).length === 0) return prev;
  return { ...prev, ...fresh };
}

/** `ids` plus `sourceId`, once, in the order the sources were retried. */
export function addRetried(ids: readonly string[], sourceId: string): string[] {
  return ids.includes(sourceId) ? [...ids] : [...ids, sourceId];
}

/**
 * The line under the verdict once a source has been retried, or null when
 * none has. Names the sources, says what is out of date and what brings it
 * up to date — the reader cannot be expected to infer any of the three.
 */
export function retriedSinceRunNote(
  retried: readonly string[],
  names?: Record<string, string>,
): string | null {
  if (retried.length === 0) return null;
  const list = sourceList([...retried], names);
  const verb = new Set(retried).size === 1 ? "was" : "were";
  return (
    `The verdict and the coverage count were worked out before ${list} ${verb} ` +
    "retried, so they do not reflect the retry. Run the check again to bring them up to date."
  );
}
