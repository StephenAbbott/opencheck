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
export type LookupPhase =
  | "connecting"
  | "anchoring"
  | "dispatching"
  | "querying"
  | "finishing";

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
  dispatching: "Working out which sources apply…",
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
      ? // Anchored but no sources_applicable yet. Repeating "resolving the
        // entity" here would claim the lookup is doing something it has
        // finished — the same class of untruth as the simulated bar, just
        // smaller. This is also the phase an older backend, or an empty
        // applicable list, would sit in for the whole run.
        "dispatching"
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

/**
 * How many of the dispatched sources answered — the verdict strip's Coverage
 * figure.
 *
 * It must be computed against `applicable`, not by counting the completed set,
 * because the GLEIF anchor emits `source_started` / `source_completed` **before**
 * `sources_applicable` and is never in that list. Counting the raw set let the
 * figure overshoot its own denominator: production rendered "13 of 12 sources
 * answered" directly above "Every applicable source answered." A coverage
 * number that exceeds its own total undermines the one figure on the page whose
 * job is to say how much was actually checked.
 */
export function answeredCount(
  applicable: string[],
  completed: ReadonlySet<string>,
  /** Sources that emitted `source_error`. App adds an errored source to the
   *  completed set as well, so without this a source whose own card reads
   *  "Did not answer" is counted as one that answered — and the strip can say
   *  "13 of 13 sources answered · Every applicable source answered" directly
   *  above it. */
  errored: ReadonlySet<string> = new Set()
): number {
  return applicable.filter((id) => completed.has(id) && !errored.has(id)).length;
}

// ---------------------------------------------------------------------------
// Coverage copy (Phase 156)
// ---------------------------------------------------------------------------

/**
 * What the Coverage column says, and the aside on "What each source said".
 *
 * "10 of 10 sources answered" sat under a homepage promising forty. Both
 * numbers were true — ten sources apply to a British company, ten answered —
 * and nothing on the page said that the other thirty were never in question,
 * so the sentence read either as thirty sources failing silently or as forty
 * being hype. The denominator a reader needs is the registry; the numerator
 * they need is how many of it apply to *this* company; and only then does
 * "every one answered" mean anything.
 *
 * The GLEIF anchor is counted. `sources_applicable` never lists it (it has
 * answered before that event fires — Phase 126), so the two stream-derived
 * figures exclude it, but it is one of the forty and it did answer: a figure
 * that says "10 of 40 apply" while the GLEIF card sits first in the list below
 * is off by one in the direction that undercounts what was checked.
 *
 * Pure, so the suite can pin every sentence: the strip and the aside both
 * call this, which is what keeps them from disagreeing.
 */
export interface CoverageCopy {
  /** Sources that answered, GLEIF included — the stat numeral. */
  answered: number;
  /** Sources that apply to this company, GLEIF included. */
  applicable: number;
  /** "N sources answered" — the noun beside the numeral. */
  statNoun: string;
  /** The sentence under the numeral. */
  detail: string;
  /** "10 of 11 sources answered · 1 still running…" for the aside while
   *  streaming; "11 of 11 sources answered" once settled. */
  aside: string;
}

/** "a GB company" / "a US company" / "this company". A code with a region
 *  suffix (US-DE) names the country, which is what the registry is keyed on. */
export function jurisdictionPhrase(jurisdiction: string | null | undefined): string {
  const code = (jurisdiction || "").trim().toUpperCase().split("-")[0];
  return code ? `a ${code} company` : "this company";
}

export function coverageCopy({
  answered,
  applicable,
  total,
  jurisdiction,
  screening,
  pending = 0,
  anchorAnswered = true,
}: {
  /** From `answeredCount` — excludes the GLEIF anchor. */
  answered: number;
  /** `sources_applicable` length — excludes the GLEIF anchor. */
  applicable: number;
  /** Registry size from `/sources`, or null until it has loaded. */
  total: number | null;
  jurisdiction: string | null | undefined;
  screening: boolean;
  /** Sources still running, for the aside. */
  pending?: number;
  /** Whether the GLEIF anchor resolved — it did if there is a report at all. */
  anchorAnswered?: boolean;
}): CoverageCopy {
  const anchor = anchorAnswered ? 1 : 0;
  const a = answered + anchor;
  const p = applicable + anchor;
  const who = jurisdictionPhrase(jurisdiction);
  const applyClause =
    total && total >= p
      ? `${p} of OpenCheck's ${total} sources ${p === 1 ? "applies" : "apply"} to ${who}`
      : `${p} ${p === 1 ? "source applies" : "sources apply"} to ${who}`;

  let detail: string;
  if (screening) {
    detail = `${applyClause}; ${p - a} still answering.`;
  } else if (a >= p) {
    detail = `${applyClause}; every one answered.`;
  } else {
    detail = `${applyClause}; ${a} answered.`;
  }

  const aside =
    pending > 0
      ? `${a} of ${p} sources answered · ${pending} still running…`
      : `${a} of ${p} ${p === 1 ? "source" : "sources"} answered`;

  return {
    answered: a,
    applicable: p,
    statNoun: a === 1 ? "source answered" : "sources answered",
    detail,
    aside,
  };
}
