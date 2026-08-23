import type { DegradedSource, GraphShape, RiskSignal } from "../../lib/api";
import { networkSummary } from "../../lib/graphShape";
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
 *
 * **Three columns, not two.** The third is the ownership network, and it is
 * the only route into FullCheck that a reader meets before scrolling. It
 * shipped as two columns for four phases, which left the mode tabs as the
 * sole invitation into the deeper check — a tab strip does not say what is
 * behind it, and the numbers do.
 */
export function VerdictStrip({
  verdict,
  riskSignals,
  contextSignals,
  degraded,
  sourcesAnswered,
  sourcesApplicable,
  graphShape,
  onOpenNetwork,
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
  /** How big the mapped graph is — `graph_shape` on the `risk_signals`
   *  event. Counts the statements this check produced, never what FullCheck
   *  might go on to find. Absent until the event lands. */
  graphShape?: GraphShape | null;
  /** Switches the report to FullCheck. Omitted when there is no network to
   *  open, which is also when the column does not render. */
  onOpenNetwork?: () => void;
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
  const network = networkSummary(graphShape);
  const showNetwork = Boolean(network && onOpenNetwork);

  return (
    <section
      aria-label="What this check found"
      className="border-t border-oo-rule px-4 py-[18px] sm:px-7 sm:py-6"
    >
      {verdict && (
        <p className="text-oo-head font-medium leading-snug text-oo-ink max-w-[76ch]">
          {verdict}
        </p>
      )}

      <div
        className={`mt-5 grid grid-cols-1 sm:grid-cols-2 gap-5 sm:gap-6 ${
          showNetwork ? "lg:grid-cols-3" : ""
        }`.trim()}
      >
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
              {/* Interactive, like the chips in the Risk signals section
                  below. They shipped inert here, which made the first three
                  signals a reader meets the only ones that would not open —
                  the same chip, in the same colours, behaving differently
                  depending on how far down the page it sat. */}
              {preview.map((sig) => (
                <RiskChip key={sig.code} signal={sig} compact />
              ))}
              {riskSignals.length > preview.length && (
                <span className="text-oo-small text-oo-muted self-center">
                  +{riskSignals.length - preview.length} more
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

        {showNetwork && network && onOpenNetwork && (
          // The heading and the counts sit *outside* the control, and only the
          // call to action is the button. The first version wrapped the whole
          // card, which put an `<h2>` inside a `<button>` — invalid content
          // model, and in practice a control whose accessible name was the
          // entire card, ~30 words long, while the heading dropped out of the
          // page outline wherever AT flattens button descendants. This column
          // was then the only one of the three without a heading.
          <div className="flex flex-col items-start gap-2.5 rounded-oo border border-oo-graph-ownershipTintBorder bg-oo-graph-ownershipTint px-4 py-3.5">
            <SectionLabel as="h2" className="text-oo-graph-ownershipText">
              Ownership network
            </SectionLabel>
            <span className="text-oo-small text-oo-ink">
              <span className="font-head font-bold text-oo-stat">{network.companies}</span>{" "}
              {network.companies === 1 ? "company" : "companies"}
              {network.people > 0 && (
                <>
                  {" and "}
                  <span className="font-head font-bold text-oo-stat">{network.people}</span>{" "}
                  {network.people === 1 ? "person" : "people"}
                </>
              )}
              {network.depthPhrase && <>, {network.depthPhrase}</>}
            </span>
            <button
              type="button"
              onClick={onOpenNetwork}
              className="inline-flex items-center gap-2 text-left text-oo-body font-bold text-oo-graph-ownershipText hover:underline underline-offset-2 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-oo-graph-ownershipText rounded"
            >
              <svg
                width="17"
                height="17"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.75"
                strokeLinecap="round"
                strokeLinejoin="round"
                aria-hidden="true"
              >
                <circle cx="6" cy="6" r="2.3" />
                <circle cx="18" cy="6" r="2.3" />
                <circle cx="12" cy="18" r="2.3" />
                <path d="M8 7.5 10.7 15.6M16 7.5 13.3 15.6M8.5 6h7" />
              </svg>
              Explore the full ownership network
            </button>
            <span className="text-oo-small text-oo-muted">
              Expand owners and controllers layer by layer, then explore the whole network in
              one graph.
            </span>
          </div>
        )}
      </div>
    </section>
  );
}
