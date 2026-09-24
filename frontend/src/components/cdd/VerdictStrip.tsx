import type { DegradedSource, GraphShape, KnowabilityStatement, RiskSignal } from "../../lib/api";
import { networkSummary } from "../../lib/graphShape";
import { knowabilityView } from "../../lib/knowability";
import { coverageCopy } from "../../lib/lookupProgress";
import { Chip, SectionLabel } from "../ui";
import { Explain } from "../ui/Explain";

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
 *
 * **A fourth band, under the columns (Phase 224): what can be known.**
 * Coverage says how much of the check ran; it cannot say what the sources
 * *could* have told us. "10 of 10 answered" for a Cayman company reads as a
 * clean screen when no Cayman register publishes beneficial owners and
 * OpenCheck reads none. The band carries the backend's dated, per-jurisdiction
 * statement (`opencheck/knowability.py`, the `knowability` event — Stephen's
 * Notion table, never typed here) verbatim, with the per-field list behind a
 * ⓘ. It describes and dates; it is rendered in context/neutral tones only and
 * never becomes a chip in the "What we found" column — the AMLA geographic-
 * risk work (Aug 2026) rejected a jurisdiction-level "no register" signal
 * because a coverage gap is not an accusation. Full width rather than inside
 * the Coverage column because the sentence is two to four clauses long and
 * the column is a third of the strip.
 *
 * **Phase 245 — nothing said twice.** The Opus 5.5 check counted, in one
 * QuickCheck on Shell, the related-party chips twice (here and under Risk
 * signals), the source count three times and "one check did not run" twice.
 * So: "What we found" is a count and a link to `#risk-signals`, where the
 * chips live; Coverage is one coverage statement, and a gap is a link to the
 * notice that names it rather than a second copy of that notice; and the
 * knowability band folded into the Coverage column behind an `Explain`, so the
 * mode tabs start higher. The dated badge stays visible, the sentence is one
 * press away, and it is still the server's sentence, verbatim.
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
  onShowSignals,
  onShowDegraded,
  saved = false,
  screening = false,
  registryTotal = null,
  jurisdiction = null,
  knowability = null,
}: {
  /** The deterministic sentences from the backend. */
  verdict?: string | null;
  riskSignals: RiskSignal[];
  contextSignals: RiskSignal[];
  degraded: DegradedSource[];
  /** From `answeredCount` — excludes the GLEIF anchor, which `coverageCopy`
   *  adds back: it is one of the registry's sources and it answered. */
  sourcesAnswered: number;
  /** `sources_applicable` length — excludes the GLEIF anchor likewise. */
  sourcesApplicable: number;
  /** Registry size from `/sources`, or null until it has loaded. The
   *  denominator a reader needs (Phase 156): "10 of 10 answered" under a
   *  homepage promising forty read as thirty sources failing silently. */
  registryTotal?: number | null;
  /** The subject's jurisdiction, for "apply to a GB company". */
  jurisdiction?: string | null;
  /** The `knowability` event for the subject's jurisdiction (Phase 224), or
   *  null before it lands / when the jurisdiction is unknown / on a payload
   *  saved before the event existed — the line is simply absent then. */
  knowability?: KnowabilityStatement | null;
  /** How big the mapped graph is — `graph_shape` on the `risk_signals`
   *  event. Counts the statements this check produced, never what FullCheck
   *  might go on to find. Absent until the event lands. */
  graphShape?: GraphShape | null;
  /** Switches the report to FullCheck. Omitted when there is no network to
   *  open — and on the FullCheck tab itself (Phase 245), where an invitation
   *  to the page the reader is already on is noise. */
  onOpenNetwork?: () => void;
  /** Goes to the Risk signals section, where the chips are (Phase 245). */
  onShowSignals?: () => void;
  /** Goes to the notice naming the checks that did not run (Phase 245). */
  onShowDegraded?: () => void;
  /** Phase 217: a saved report — its network is drawn as saved, not expanded. */
  saved?: boolean;
  /** Sources are still streaming: counts are partial, so say nothing yet. */
  screening?: boolean;
}) {
  const total = riskSignals.length + contextSignals.length;

  // Nothing to say yet, and a half-finished verdict is worse than none.
  if (screening && total === 0 && !verdict) return null;

  const degradedCount = new Set(degraded.map((d) => d.source_id)).size;
  const coverage = coverageCopy({
    answered: sourcesAnswered,
    applicable: sourcesApplicable,
    total: registryTotal ?? null,
    jurisdiction,
    screening,
  });
  const network = networkSummary(graphShape);
  const showNetwork = Boolean(network && onOpenNetwork);
  const knowable = knowability ? knowabilityView(knowability) : null;
  const gapLabel = `${degradedCount === 1 ? "One check" : `${degradedCount} checks`} did not run`;

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
        <div className="flex flex-col items-start gap-2.5">
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
          {/* A count and a way to the chips, never the chips themselves: they
              are the Risk signals section's, one screen down, and printing
              them here as well was the first repeat on the Opus 5.5 list. */}
          {total > 0 && onShowSignals ? (
            <a
              href="#risk-signals"
              onClick={(e) => {
                e.preventDefault();
                onShowSignals();
              }}
              className="text-oo-small font-semibold text-oo-blue underline underline-offset-2 hover:no-underline"
            >
              {total === 1 ? "Read it under Risk signals" : "Read them under Risk signals"}
            </a>
          ) : screening ? (
            <p className="text-oo-small text-oo-muted">Still checking.</p>
          ) : total === 0 && !verdict ? (
            // The verdict says this when there is one; without it the column
            // must still say what a zero means.
            <p className="text-oo-small text-oo-muted">
              No risk signals surfaced across the sources that answered.
            </p>
          ) : null}
        </div>

        <div className="flex flex-col items-start gap-2.5 sm:pl-6 sm:border-l border-oo-rule">
          <SectionLabel as="h2">Coverage</SectionLabel>
          <p className="text-oo-small text-oo-ink">
            <span className="font-head font-bold text-oo-stat">{coverage.answered}</span> of{" "}
            {coverage.applicable} {coverage.applicable === 1 ? "source" : "sources"} answered
          </p>
          {degradedCount > 0 ? (
            // One link to the notice that names the gap, not a second copy of
            // it: the notice below the tabs carries the detail and the re-run.
            onShowDegraded ? (
              <a
                href="#screening-incomplete"
                onClick={(e) => {
                  e.preventDefault();
                  onShowDegraded();
                }}
                className="text-oo-small font-semibold text-oo-warn-text underline underline-offset-2 hover:no-underline"
              >
                {gapLabel} — see which
              </a>
            ) : (
              <p className="text-oo-small font-semibold text-oo-warn-text">{gapLabel}.</p>
            )
          ) : (
            <p className="text-oo-small text-oo-muted">
              {screening ? coverage.detail : coverage.scope}
            </p>
          )}

          {knowable && (
            <div
              className="mt-1 flex w-full flex-wrap items-center gap-x-2 gap-y-1.5 border-t border-oo-rule pt-3"
              data-testid="knowability-band"
            >
              {/* Phase 245: the band that sat full-width under the columns,
                  folded into Coverage — it answers the same question from the
                  other side (what could the registers have told us). The
                  dated badge stays in view; the sentence and the register
                  facts behind it are one press away, verbatim. */}
              <SectionLabel as="h3">{knowable.heading}</SectionLabel>
              <span className="text-oo-meta text-oo-muted">{knowable.subheading}</span>
              <Chip tone={knowable.badge.tone} size="sm">
                {knowable.badge.label}
              </Chip>
              <Explain
                label={`What can be known about ${knowable.subheading}: the statement and the register facts behind it`}
              >
                <span className="block text-oo-small">{knowable.sentence}</span>
                {knowable.rows.length > 0 && (
                  <dl className="mt-1.5 grid grid-cols-1 gap-y-1 sm:grid-cols-[max-content_1fr] sm:gap-x-4">
                    {knowable.rows.map((row) => (
                      <div key={row.label} className="contents">
                        <dt className="font-semibold text-oo-muted">{row.label}</dt>
                        <dd>{row.value}</dd>
                      </div>
                    ))}
                  </dl>
                )}
                {knowable.sources.length > 0 && (
                  <span className="mt-1.5 block">
                    <span className="font-semibold text-oo-muted">Sources</span>{" "}
                    {knowable.sources.map((src, i) => (
                      <span key={src.url}>
                        {i > 0 && " · "}
                        <a
                          href={src.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="underline underline-offset-2 hover:no-underline break-all"
                        >
                          {src.title}
                        </a>
                      </span>
                    ))}
                  </span>
                )}
                {knowable.asOfLine && (
                  <span className="mt-1.5 block text-oo-muted">
                    {knowable.asOfLine}. The facts are maintained by hand in OpenCheck&apos;s
                    jurisdiction table and describe the register, not this company.
                  </span>
                )}
              </Explain>
            </div>
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
              {saved
                ? "Every record the saved check mapped, in one graph."
                : "Expand owners and controllers layer by layer."}
            </span>
          </div>
        )}
      </div>
    </section>
  );
}
