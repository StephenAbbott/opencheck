import { useEffect, useMemo, useRef, useState } from "react";
import type {
  BodsBreakdown,
  BodsCountsEvent,
  CrossSourceLink,
  DegradedSource,
  GraphShape,
  KnowabilityChain,
  KnowabilityStatement,
  LookupStreamHandlers,
  OpenAlephScreeningMatch,
  PossiblySameEntity,
  PrimaryListing,
  RiskSignal,
  SourceHit,
  SourceLiveness,
  SubjectProfile,
} from "../lib/api";
import { answeredCount, settledCount } from "../lib/lookupProgress";
import { runCompletedAtFrom } from "../lib/savedReport";

/*
 * Everything one lookup run puts on the page, and the handlers that put it
 * there. Moved out of App() in Phase 246 (App held 59 `useState`s).
 *
 * `handlers()` is the one builder for the live stream (`streamLookup`) and a
 * saved report's replay (`replayLookupEvents`), so the two cannot set state
 * differently — which is what makes a saved report render as the same report
 * (Phase 217). `reset()` clears the run before another takes its place; the
 * page state that is not the run's (the selected chip, the open identity
 * band, exports, the save controls) is App's to reset beside it.
 */
export function useLookupStream() {
  // streamingLei is set once GLEIF resolves (replaces the old `result !== null` guard).
  const [streamingLei, setStreamingLei] = useState<string | null>(null);
  const [legalName, setLegalName] = useState<string | null>(null);
  const [subjectJurisdiction, setSubjectJurisdiction] = useState<string | null>(null);
  const [hits, setHits] = useState<SourceHit[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [crossSourceLinks, setCrossSourceLinks] = useState<CrossSourceLink[]>([]);
  const [possiblySame, setPossiblySame] = useState<PossiblySameEntity[]>([]);
  // What the registers say the subject *is* (Phase 154) — its own event,
  // arriving once the deepened bundles are in. Identity, not the answer.
  const [subjectProfile, setSubjectProfile] = useState<SubjectProfile | null>(null);
  const [knowability, setKnowability] = useState<KnowabilityStatement | null>(null);
  // Phase 236: the primary listing from PermID, as the `listing` event carried it.
  const [primaryListing, setPrimaryListing] = useState<PrimaryListing | null>(null);
  const [knowabilityChain, setKnowabilityChain] = useState<KnowabilityChain | null>(null);
  const [riskSignals, setRiskSignals] = useState<RiskSignal[]>([]);
  // Derived checks that did not fully run (issue #50) — rendered as a
  // warning above the risk panel; empty signals + non-empty degraded is
  // NOT a clean screen.
  const [degradedSources, setDegradedSources] = useState<DegradedSource[]>([]);
  // The answer-first sentence, rendered above the evidence. Built in the
  // backend (opencheck/verdict.py) from the signals and degradations, so
  // the page, the PDF, the share card and the API cannot disagree — and
  // so it costs no model call.
  const [verdict, setVerdict] = useState<string | null>(null);
  // How current each source's payload is, keyed by source_id. Arrives on the
  // risk_signals event alongside degraded_sources — the two answer the same
  // shape of question: what in this result should not be read at face value.
  const [sourceLiveness, setSourceLiveness] = useState<
    Record<string, SourceLiveness>
  >({});
  // Informational OpenAleph percolation matches (Phase 96) — related-party
  // names found in archive/watchlist collections whose topics map to no
  // RELATED_* code. Name-derived; never identifier corroboration.
  const [oaScreening, setOaScreening] = useState<OpenAlephScreeningMatch[]>([]);
  // How big the mapped graph is, for the verdict strip's ownership-network
  // column. Rides on the same event as the signals, the degradations and the
  // verdict sentence, so the three columns describe one run.
  const [graphShape, setGraphShape] = useState<GraphShape | null>(null);
  const [applicableSources, setApplicableSources] = useState<string[]>([]);
  const [completedSources, setCompletedSources] = useState<Set<string>>(new Set());
  // Phase 124: the loading grid used to simulate per-source progress. It now
  // renders `source_started` / `source_completed` / `source_error`, so it needs
  // the started set the stream was already sending and nothing was reading.
  const [startedSources, setStartedSources] = useState<Set<string>>(new Set());
  // Phase 238: where this run stands in the server's queue while it waits for
  // a pipeline slot (the `queued` event). Null when it is not waiting.
  const [queuePosition, setQueuePosition] = useState<number | null>(null);
  // Derived rather than stored: `errors` is already the record of which sources
  // failed, and a second set could disagree with it.
  const erroredSources = useMemo(() => new Set(Object.keys(errors)), [errors]);
  // Coverage counts only sources that were dispatched. `completedSources` also
  // holds the GLEIF anchor, which emits source_started/source_completed BEFORE
  // sources_applicable and is never in that list — so the raw size could exceed
  // the total and the strip read "13 of 12 sources answered", above the line
  // "Every applicable source answered." A coverage figure that overshoots its
  // own denominator undermines the one number on the page whose whole job is
  // to say how much was checked.
  const answeredApplicable = useMemo(
    () => answeredCount(applicableSources, completedSources, erroredSources),
    [applicableSources, completedSources, erroredSources]
  );
  // Phase 241: the one progress count — the loading grid computes the same
  // figures from the same sets, so the header and the bar cannot disagree.
  const settled = useMemo(
    () =>
      settledCount({
        anchored: true,
        applicable: applicableSources,
        completed: completedSources,
        errored: erroredSources,
      }),
    [applicableSources, completedSources, erroredSources]
  );
  const [streaming, setStreaming] = useState(false);
  // Maps "source_id:hit_id" → BODS statement count; populated by the bods_counts SSE event.
  const [bodsCountMap, setBodsCountMap] = useState<Record<string, number>>({});
  // Same key → entity / relationship split, for the source-card graph CTA subtitle.
  const [bodsBreakdownMap, setBodsBreakdownMap] = useState<
    Record<string, BodsBreakdown>
  >({});
  // True when the SSE connection dropped AFTER the GLEIF anchor resolved —
  // partial results are on screen and a "Resume lookup" banner is shown.
  const [streamDropped, setStreamDropped] = useState(false);
  // Wall-clock ISO time the on-screen results were originally fetched, when
  // they came from the backend replay cache rather than a fresh run. Null for
  // live runs. Drives the "Results from a check N min ago" badge.
  const [replayedAt, setReplayedAt] = useState<string | null>(null);
  // Source IDs with an in-flight per-source retry (/lookup-source).
  const [retryingSources, setRetryingSources] = useState<Set<string>>(new Set());
  // `runCompletedAt` names the run on screen (its `done` event carries it);
  // a save names that run and the server copies its own held copy (Phase 217).
  const [runCompletedAt, setRunCompletedAt] = useState<string | null>(null);

  // Holds the SSE close function for the current in-flight stream.
  const cleanupRef = useRef<(() => void) | null>(null);

  // Close any open stream when the component unmounts.
  useEffect(() => () => { cleanupRef.current?.(); }, []);

  /** Clear the run on screen before another — live or saved — replaces it. */
  function reset() {
    setStreamingLei(null);
    setLegalName(null);
    setHits([]);
    setErrors({});
    setCrossSourceLinks([]);
    setPossiblySame([]);
    setSubjectProfile(null);
    setKnowability(null);
    setKnowabilityChain(null);
    setPrimaryListing(null);
    setRiskSignals([]);
    setDegradedSources([]);
    setVerdict(null);
    setSourceLiveness({});
    setOaScreening([]);
    setGraphShape(null);
    setApplicableSources([]);
    setCompletedSources(new Set());
    setStartedSources(new Set());
    setQueuePosition(null);
    setStreaming(false);
    setBodsCountMap({});
    setBodsBreakdownMap({});
    setStreamDropped(false);
    setRetryingSources(new Set());
    setReplayedAt(null);
    setRunCompletedAt(null);
  }

  /**
   * The handlers that turn lookup events into page state. `cb.onAnchor` runs
   * once GLEIF resolves the subject; `cb.onFailure` only for a failure before
   * that — after it, a dropped connection keeps the partial results and
   * raises `streamDropped` for the "Resume lookup" banner.
   */
  function handlers(cb: {
    onAnchor: (e: { lei: string; legal_name: string | null }) => void;
    onFailure: (detail: string) => void;
  }): LookupStreamHandlers {
    // Tracks whether the GLEIF anchor resolved: a connection drop before
    // it is a hard error; after it, we keep partial results and offer a
    // "Resume lookup" instead.
    let anchored = false;
    return {
      // Served from the backend replay cache — badge the result with the
      // original completion time so a cached run never looks live.
      onReplayed: (e) => setReplayedAt(e.fetched_at),
      onQueued: (e) => setQueuePosition(e.position),
      onGleifDone: (e) => {
        anchored = true;
        setStreamingLei(e.lei);
        setLegalName(e.legal_name);
        setSubjectJurisdiction(e.jurisdiction);
        setStreaming(true);
        cb.onAnchor({ lei: e.lei, legal_name: e.legal_name });
      },
      onSourcesApplicable: (e) => setApplicableSources(e.source_ids),
      onSourceStarted: (e) =>
        setStartedSources((prev) => new Set([...prev, e.source_id])),
      // Dedup by source_id:hit_id — in dev, React StrictMode runs the lookup
      // effect twice, so two streams can each deliver the same hit. The guard
      // makes hit accumulation idempotent (no-op in production, where
      // StrictMode doesn't double-invoke).
      onHit: (e) =>
        setHits((prev) =>
          prev.some((h) => h.source_id === e.source_id && h.hit_id === e.hit_id)
            ? prev
            : [...prev, e]
        ),
      onSourceCompleted: (e) =>
        setCompletedSources((prev) => new Set([...prev, e.source_id])),
      onSourceError: (e) => {
        setErrors((prev) => ({ ...prev, [e.source_id]: e.error }));
        setCompletedSources((prev) => new Set([...prev, e.source_id]));
      },
      onCrossSourceLinks: (e) => setCrossSourceLinks(e.links),
      onPossiblySame: (e) => setPossiblySame(e.pairs),
      onSubjectProfile: (e) => setSubjectProfile(e.profile),
      onKnowability: (e) => setKnowability(e),
      onKnowabilityChain: (e) => setKnowabilityChain(e),
      onListing: (e) => setPrimaryListing(e),
      onRiskSignals: (e) => {
        setRiskSignals(e.signals);
        setDegradedSources(e.degraded_sources ?? []);
        setVerdict(e.verdict ?? null);
        setSourceLiveness(e.source_liveness ?? {});
        setOaScreening(e.openaleph_screening ?? []);
        setGraphShape(e.graph_shape ?? null);
      },
      onBodsCounts: (e: BodsCountsEvent) => {
        setBodsCountMap(e.counts);
        if (e.breakdown) setBodsBreakdownMap(e.breakdown);
      },
      onDone: (e) => {
        setStreaming(false);
        setStreamDropped(false);
        setRunCompletedAt(runCompletedAtFrom(e));
        cleanupRef.current = null;
      },
      onError: (detail) => {
        setStreaming(false);
        cleanupRef.current = null;
        if (anchored) {
          // Mid-lookup drop (e.g. Render cold start, flaky network):
          // keep the partial results and surface the resume banner.
          setStreamDropped(true);
        } else {
          cb.onFailure(detail);
        }
      },
    };
  }

  return {
    streamingLei,
    legalName,
    subjectJurisdiction,
    hits,
    setHits,
    errors,
    setErrors,
    crossSourceLinks,
    possiblySame,
    subjectProfile,
    knowability,
    primaryListing,
    knowabilityChain,
    riskSignals,
    degradedSources,
    verdict,
    sourceLiveness,
    oaScreening,
    graphShape,
    applicableSources,
    completedSources,
    startedSources,
    queuePosition,
    erroredSources,
    answeredApplicable,
    settled,
    streaming,
    bodsCountMap,
    bodsBreakdownMap,
    streamDropped,
    replayedAt,
    runCompletedAt,
    retryingSources,
    setRetryingSources,
    cleanupRef,
    reset,
    handlers,
  };
}
