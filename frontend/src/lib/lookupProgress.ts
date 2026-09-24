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
  | "queued"
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
  queued: "Waiting for a free slot…",
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
  queuePosition = null,
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
  /** Phase 238: the latest `queued` event's position (1 = next), or null.
   *  Only read until the run starts — any later event means it has a slot. */
  queuePosition?: number | null;
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
  // Queued only while nothing else has happened: the first `source_started`
  // (GLEIF) is the run taking its slot, so a stale position can never
  // outlive the wait it described.
  const queued =
    queuePosition !== null && queuePosition > 0 && !anchored && started.size === 0 && applicable.length === 0;
  const phase: LookupPhase = queued
    ? "queued"
    : !anchored
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

  const label = queued && queuePosition !== null ? queueLabel(queuePosition) : PHASE_LABEL[phase];
  return { phase, sources, settled, total, label };
}

/**
 * The line shown while a run waits for a slot (Phase 238).
 *
 * Before this, a run that could not get one of the server's slots within a
 * minute ended in "OpenCheck is running as many checks as it can at once" —
 * an error for what is only a wait. The stream now waits longer and says so,
 * counting the checks ahead rather than naming a time it cannot promise.
 */
export function queueLabel(position: number): string {
  if (position <= 1) return "OpenCheck is busy — your check is next in line for a free slot…";
  const ahead = position - 1;
  return `OpenCheck is busy — waiting for a free slot, ${ahead} ${ahead === 1 ? "check" : "checks"} ahead of yours…`;
}

/**
 * The one progress count (Phase 241).
 *
 * Mid-stream on one lookup the progress bar read "6 of 8 sources answered ·
 * 6/8" while the section header under it read "7 of 9 sources answered · 2
 * still running": the bar left the GLEIF anchor out, the header put it in
 * (Phase 156's rule), and the bar counted a source that failed as answered
 * while the header did not. Every surface that counts sources during or after
 * a run now calls this — the loading grid, the "What each source said"
 * header and the Coverage column — so the three cannot disagree.
 *
 * The anchor is counted once it has resolved, as `coverageCopy` counts it:
 * it is one of the registry's sources, it answered, and its card is the first
 * one in the list. A source that errored is `failed`, never `answered`.
 */
export interface SettledCount {
  /** Applicable sources that replied — with a record or with none. */
  answered: number;
  /** Applicable sources that errored. */
  failed: number;
  /** Applicable sources still running. */
  pending: number;
  /** Every applicable source, anchor included. */
  total: number;
}

export function settledCount({
  anchored,
  applicable,
  completed,
  errored,
}: {
  anchored: boolean;
  applicable: readonly string[];
  completed: ReadonlySet<string>;
  errored: ReadonlySet<string>;
}): SettledCount {
  const anchor = anchored ? 1 : 0;
  const failed = applicable.filter((id) => errored.has(id)).length;
  const answered = applicable.filter((id) => completed.has(id) && !errored.has(id)).length;
  return {
    answered: answered + anchor,
    failed,
    pending: applicable.length - answered - failed,
    total: applicable.length + anchor,
  };
}

/** "7 of 9 sources answered · 1 did not answer" — the shared clause. */
export function settledLine(c: SettledCount): string {
  const noun = c.total === 1 ? "source" : "sources";
  const failed = c.failed > 0 ? ` · ${c.failed} did not answer` : "";
  return `${c.answered} of ${c.total} ${noun} answered${failed}`;
}

/**
 * The completion line, in the tense the stream justifies.
 *
 * The old grid flipped to the past tense "Queried N sources" on a timer. This
 * only reaches the past tense when every applicable source has actually
 * settled, and it counts failures separately rather than folding them into a
 * success total — a source that errored was not queried successfully, and
 * saying "39 of 39" when three failed is the same class of untruth as the
 * simulated bar. Phase 241: the figures are `settledCount`'s, the same ones
 * the section header prints.
 */
export function progressLabel(p: LookupProgress, c: SettledCount): string {
  if (p.total === null) return p.label;
  if (p.phase === "finishing") return settledLine(c);
  return `Querying — ${settledLine(c)}`;
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
  failed = 0,
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
  /** Sources that errored (Phase 241) — named in the aside, as the loading
   *  grid names them. */
  failed?: number;
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

  const line = settledLine({ answered: a, failed, pending, total: p });
  const aside = pending > 0 ? `${line} · ${pending} still running…` : line;

  return {
    answered: a,
    applicable: p,
    statNoun: a === 1 ? "source answered" : "sources answered",
    detail,
    aside,
  };
}

/**
 * The applicable sources that answered and hold nothing on this company
 * (Phase 241).
 *
 * A source that completed with no result produced no card, so on a Moldovan
 * company the Coverage column said "9 of 9 sources answered" above two cards
 * — OpenSanctions, TED, EveryPolitician and the rest were never named, even
 * collapsed. Silence reads as "nothing to see", which is the one thing the
 * findings rules forbid. These are named in a row of their own under "What
 * each source said", in registry order. A source that errored has a card
 * already ("Did not answer"), so it is never listed here.
 */
export function noRecordSources(
  applicable: readonly string[],
  completed: ReadonlySet<string>,
  errored: ReadonlySet<string>,
  withCard: ReadonlySet<string>
): string[] {
  return applicable.filter((id) => completed.has(id) && !errored.has(id) && !withCard.has(id));
}
