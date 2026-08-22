/**
 * SearchLoadingGrid — what the lookup is really doing (Phase 124).
 *
 * This component used to simulate its own contents. Every chip's state came
 * from a `setTimeout`, ordered by a random shuffle recomputed on each loop; the
 * progress bar was derived from that simulated count; the source list was the
 * entire registry rather than the sources the lookup dispatches; and its
 * `role="status"` line announced "Queried 39 sources" to a screen reader on a
 * timer, for a lookup that had not returned, before looping back to
 * "Querying…". Its docstring's premise — "there is no real per-source progress
 * signal" — stopped being true in Phase 47, when `_lookup_pipeline` began
 * emitting `sources_applicable`, `source_started`, `source_completed` and
 * `source_error`.
 *
 * It now renders those events and nothing else. The logic is in
 * `lib/lookupProgress.ts` so the logic-only suite can pin the rule that
 * matters: **a source is shown only in a state the stream has said it is in**,
 * and before `sources_applicable` arrives there are no chips at all, because
 * there is nothing true to draw. That pre-anchor window is one honest line
 * naming the step — resolving the entity in GLEIF — which is what a lookup is
 * actually doing before it knows which sources apply.
 */

import { useEffect, useRef, useState } from "react";
import type { SourceInfo } from "../lib/api";
import { lookupProgress, progressLabel } from "../lib/lookupProgress";
import { sourceLabel } from "../lib/vocab";

const CHIP_STYLE: Record<string, string> = {
  waiting: "bg-oo-bg text-oo-muted border-oo-rule",
  querying: "bg-oo-light text-oo-blue border-oo-softBorder",
  done: "bg-oo-ok-bg text-oo-ok-text border-oo-ok-border",
  failed: "bg-oo-warn-bg text-oo-warn-text border-oo-warn-border",
};

/** The mark beside each chip. A glyph as well as a colour, so state is not
 *  carried by colour alone (WCAG 1.4.1). */
const CHIP_MARK: Record<string, string> = {
  waiting: "·",
  querying: "◌",
  done: "✓",
  failed: "!",
};

const STATE_WORD: Record<string, string> = {
  waiting: "not started",
  querying: "querying",
  done: "answered",
  failed: "did not answer",
};

export default function SearchLoadingGrid({
  sources,
  anchored = false,
  applicable = [],
  started,
  completed,
  errored,
}: {
  /** The registry, used only to put a display name on a chip. */
  sources: SourceInfo[];
  anchored?: boolean;
  applicable?: string[];
  started?: ReadonlySet<string>;
  completed?: ReadonlySet<string>;
  errored?: ReadonlySet<string>;
}) {
  const empty: ReadonlySet<string> = new Set();
  const progress = lookupProgress({
    anchored,
    applicable,
    started: started ?? empty,
    completed: completed ?? empty,
    errored: errored ?? empty,
  });
  const names = Object.fromEntries(sources.map((s) => [s.id, s.name]));
  const failedCount = progress.sources.filter((s) => s.state === "failed").length;
  // The live region is throttled, not live-per-event. Replacing an animation
  // that announced on a timer with one that announces on every
  // `source_completed` would queue ~39 utterances — a firehose is as unusable
  // as a fiction. The phase is announced whenever it changes, and the count at
  // most every few seconds. (App.tsx makes the same trade for its per-source
  // failure announcements.)
  const [announced, setAnnounced] = useState("");
  const pending = useRef("");
  pending.current = progress.total === null ? progress.label : progressLabel(progress, failedCount);
  const phase = progress.phase;
  useEffect(() => {
    setAnnounced(pending.current);
    const t = setInterval(() => setAnnounced(pending.current), 4000);
    return () => clearInterval(t);
  }, [phase]);
  const pct = progress.total ? (progress.settled / progress.total) * 100 : 0;

  return (
    <div className="bg-white border border-oo-rule rounded-oo p-4 mb-6">
      <div className="flex items-center gap-3 mb-2">
        <p className="text-oo-meta text-oo-muted flex-1" aria-hidden="true">
          {progress.total === null ? progress.label : progressLabel(progress, failedCount)}
        </p>
        <p role="status" className="sr-only">
          {announced}
        </p>
        {progress.total !== null && (
          <span aria-hidden="true" className="text-oo-meta font-mono text-oo-muted">
            {progress.settled} / {progress.total}
          </span>
        )}
      </div>

      {/* Indeterminate until the stream says how many sources apply — a bar at
          0 of an unknown total is a claim, not a placeholder. */}
      <div aria-hidden="true" className="h-0.5 bg-oo-rule rounded-full overflow-hidden mb-3">
        {progress.total === null ? (
          <div className="h-full w-1/3 rounded-full bg-oo-blue/40 animate-pulse" />
        ) : (
          <div
            className="h-full rounded-full bg-oo-blue transition-all duration-300"
            style={{ width: `${pct}%` }}
          />
        )}
      </div>

      {progress.sources.length > 0 && (
        <ul className="grid grid-cols-2 sm:grid-cols-3 gap-1.5">
          {progress.sources.map((s) => (
            <li
              key={s.sourceId}
              className={`flex items-center gap-1.5 px-2 py-1.5 rounded border text-oo-meta overflow-hidden transition-colors duration-200 ${CHIP_STYLE[s.state]}`}
            >
              <span aria-hidden="true" className="font-mono">
                {CHIP_MARK[s.state]}
              </span>
              <span className="truncate">{sourceLabel(s.sourceId, names)}</span>
              <span className="sr-only"> — {STATE_WORD[s.state]}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
