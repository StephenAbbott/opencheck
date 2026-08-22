/**
 * What a lookup is actually doing, from the events it actually emitted
 * (Phase 124).
 *
 * `SearchLoadingGrid` used to be a lie with a good excuse. Its docstring said
 * "because the backend returns all results in a single response there is no
 * real per-source progress signal, so we simulate staggered completion" — true
 * when it was written, false since Phase 47, when `_lookup_pipeline` began
 * emitting `sources_applicable`, `source_started`, `source_completed` and
 * `source_error`. By Phase 124 it was driving every chip off `setTimeout`, with
 * a random shuffle recomputed each cycle, a progress bar derived from a
 * simulated count, and — worst — a `role="status"` line announcing
 * "Queried 39 sources" to a screen reader on a timer, for a lookup that had not
 * returned, before looping back to "Querying…".
 *
 * Two further inaccuracies of substance: it rendered the **entire registry**,
 * ESG adapters included, when a lookup dispatches only the sources applicable
 * to the subject's jurisdiction and identifiers; and it was mounted only
 * between stream-open and `gleif_done` — precisely the window in which no
 * per-source information exists yet — so every chip it drew was invented by
 * construction.
 *
 * This module is the honest replacement, kept pure so the logic-only frontend
 * suite can pin it. The rule it encodes: **a source is only shown in a state
 * the stream has said it is in.** Before `sources_applicable` arrives, the
 * answer is not a list of chips at zero percent, it is "we do not know yet" —
 * and the phase says so.
 */

/** Where the lookup is. Ordered: each phase can only follow the one before. */
export type LookupPhase = "connecting" | "anchoring" | "querying" | "finishing";

export type SourceProgressState = "waiting" | "querying" | "done" | "failed";

export interface SourceProgress {
  sourceId: string;
  state: SourceProgressState;
}

export interface LookupProgress {
  phase: LookupPhase;
  /** One entry per applicable source, in dispatch order. Empty until the
   *  stream has said which sources apply — never a guess. */
  sources: SourceProgress[];
  /** Sources that have finished, successfully or not. */
  settled: number;
  /** How many were dispatched. `null` while unknown — not 0, which would
   *  render as a complete progress bar. */
  total: number | null;
  /** Present-tense description of the phase, for a `role="status"` line.
   *  Never claims completion the stream has not reported. */
  label: string;
}

const PHASE_LABEL: Record<LookupPhase, string> = {
  connecting: "Connecting…",
  anchoring: "Resolving the entity in GLEIF…",
  querying: "Querying sources…",
  finishing: "Screening for risk signals…",
};

export function lookupProgress({
  anchored,
  applicable,
  started,
  completed,
  errored,
  finished,
}: {
  /** `gleif_done` has arrived. */
  anchored: boolean;
  /** `sources_applicable.source_ids`, or empty if it has not arrived. */
  applicable: string[];
  /** Source ids that emitted `source_started`. */
  started: ReadonlySet<string>;
  /** Source ids that emitted `source_completed`. */
  completed: ReadonlySet<string>;
  /** Source ids that emitted `source_error`. */
  errored: ReadonlySet<string>;
  /** Every applicable source has settled — the risk stage is running. */
  finished?: boolean;
}): LookupProgress {
  const sources: SourceProgress[] = applicable.map((sourceId) => ({
    sourceId,
    state: errored.has(sourceId)
      ? "failed"
      : completed.has(sourceId)
        ? "done"
        : started.has(sourceId)
          ? "querying"
          : "waiting",
  }));

  const settled = sources.filter((s) => s.state === "done" || s.state === "failed").length;
  const total = applicable.length > 0 ? applicable.length : null;

  // A source can complete before its `source_started` is processed, so
  // "everything settled" is the honest end of the querying phase, not
  // "nothing left started".
  const allSettled = total !== null && settled === total;
  const phase: LookupPhase = !anchored
    ? applicable.length > 0 || started.size > 0
      ? "anchoring"
      : "connecting"
    : total === null
      ? "anchoring"
      : allSettled || finished
        ? "finishing"
        : "querying";

  return { phase, sources, settled, total, label: PHASE_LABEL[phase] };
}

/**
 * The completion line, in the tense the stream justifies.
 *
 * The old grid flipped to the past tense "Queried N sources" on a timer. This
 * only reaches the past tense when every applicable source has actually
 * settled, and it counts failures separately rather than folding them into a
 * success total — a source that errored was not queried successfully, and
 * saying "39 of 39" when three failed is the same class of untruth as the
 * simulated bar.
 */
export function progressLabel(p: LookupProgress, failedCount: number): string {
  if (p.total === null) return p.label;
  const of = `${p.settled} of ${p.total} source${p.total === 1 ? "" : "s"}`;
  const failed = failedCount > 0 ? `, ${failedCount} did not answer` : "";
  if (p.phase === "finishing") return `Queried ${of}${failed}`;
  return `Querying — ${of} answered${failed}`;
}
