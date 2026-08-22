import type { DegradedSource, RiskSignal } from "../../lib/api";
import { RiskChip } from "../risk/RiskChip";
import { SectionLabel } from "../ui";

/** How many chips sit beside the sentence before the rest are left to the strip below. */
const CHIP_PREVIEW = 3;

/**
 * VerdictStrip — the answer-first layer (Phase 122).
 *
 * v1's results page opened with a hero about three million companies, then
 * a search panel, then the subject, then three mode cards, then an AI
 * summary — and only *after* the summary, the amber notice saying some
 * checks had not run. So the page's first conclusion was read before its
 * caveat.
 *
 * This strip sits directly under the subject and answers the two questions
 * an analyst has before reading anything else: what did you find, and how
 * much of the check actually ran. Both halves are rendered from the same
 * `risk_signals` event, so they cannot disagree.
 *
 * The sentence itself is built in the backend (`opencheck/verdict.py`) and
 * arrives on the lookup response. It is a template, not a model call: the
 * AI summary further down the page is unchanged, and this adds no API
 * calls to a lookup.
 */
export function VerdictStrip({
  verdict,
  riskSignals,
  contextSignals,
  degraded,
  sourcesAnswered,
  sourcesApplicable,
  onRerun,
  screening = false,
}: {
  /** The deterministic sentence from the backend. */
  verdict?: string | null;
  riskSignals: RiskSignal[];
  contextSignals: RiskSignal[];
  degraded: DegradedSource[];
  sourcesAnswered: number;
  sourcesApplicable: number;
  /** Re-runs the lookup bypassing the replay cache. */
  onRerun?: () => void;
  /** Sources are still streaming: counts are partial, so say nothing yet. */
  screening?: boolean;
}) {
  const total = riskSignals.length + contextSignals.length;
  const preview = riskSignals.slice(0, CHIP_PREVIEW);

  // Nothing to say yet, and a half-finished verdict is worse than none.
  if (screening && total === 0 && !verdict) return null;

  const degradedCount = new Set(degraded.map((d) => d.source_id)).size;

  return (
    <section
      aria-label="What this check found"
      className="mb-6 bg-white border border-oo-rule rounded-oo p-5 lg:p-7"
    >
      {verdict && (
        <p className="text-oo-head font-medium leading-snug text-oo-ink max-w-[76ch]">
          {verdict}
        </p>
      )}

      <div className="mt-5 grid grid-cols-1 sm:grid-cols-2 gap-5 sm:gap-6">
        <div className="flex flex-col gap-2.5">
          <SectionLabel as="h2">What we found</SectionLabel>
          <p className="text-oo-small text-oo-ink">
            <span className="font-head font-bold text-oo-stat">{total}</span>{" "}
            {total === 1 ? "signal" : "signals"}
            {total > 0 && (
              <>
                {" — "}
                {riskSignals.length} risk, {contextSignals.length} structural
              </>
            )}
          </p>
          {preview.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {preview.map((sig) => (
                <RiskChip key={sig.code} signal={sig} compact interactive={false} />
              ))}
              {riskSignals.length > preview.length && (
                <span className="text-oo-small text-oo-muted self-center">
                  +{riskSignals.length - preview.length} more below
                </span>
              )}
            </div>
          ) : (
            <p className="text-oo-small text-oo-muted">
              {screening
                ? "Still checking."
                : "No risk signals surfaced across the sources that answered."}
            </p>
          )}
        </div>

        <div className="flex flex-col gap-2.5 sm:pl-6 sm:border-l border-oo-rule">
          <SectionLabel as="h2">Coverage</SectionLabel>
          <p className="text-oo-small text-oo-ink">
            <span className="font-head font-bold text-oo-stat">{sourcesAnswered}</span>{" "}
            of {sourcesApplicable} {sourcesApplicable === 1 ? "source" : "sources"} answered
          </p>
          {degradedCount > 0 ? (
            <div className="flex items-start gap-2 text-oo-small text-oo-warn-text bg-oo-warn-bg border border-oo-warn-border rounded-oo px-2.5 py-2">
              <svg
                width="14"
                height="14"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                aria-hidden="true"
                className="shrink-0 mt-0.5"
              >
                <path d="M12 9v4" />
                <path d="M12 17h.01" />
                <path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0Z" />
              </svg>
              <span>
                {degradedCount === 1 ? "One check" : `${degradedCount} checks`} did not run, so
                this is not a clean screen.{" "}
                {onRerun && (
                  <button
                    type="button"
                    onClick={onRerun}
                    className="font-bold underline underline-offset-2 hover:no-underline"
                  >
                    Run it again
                  </button>
                )}
              </span>
            </div>
          ) : (
            <p className="text-oo-small text-oo-muted">
              {screening ? "Sources are still answering." : "Every applicable source answered."}
            </p>
          )}
        </div>
      </div>
    </section>
  );
}
