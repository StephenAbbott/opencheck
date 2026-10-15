/**
 * One signal's evidence, said out loud (Phase 129, generalised in Phase 131).
 *
 * The Risk signals section used to be a row of chips and a caption asking the
 * reader to select one. That is a menu, not a finding: the most serious thing
 * the check turned up was a word in a pill, and the sentence explaining it —
 * which the chip has always carried — appeared only after a click. This box
 * puts that sentence on the page.
 *
 * It began as the *lead's* box and only ever showed the worst signal, which
 * left the section with two ways of saying one thing: this box in the v2
 * style, and, if the reader then selected a chip, that chip's own expansion
 * underneath in the v1 style — a second box, differently styled, saying the
 * same kind of sentence. Raised from production, and correctly: the box also
 * gave no sign of *why* it was open, so a reader who had clicked nothing read
 * it as an expansion that had opened itself. The section now owns the
 * selection and renders exactly one of these, for the lead until the reader
 * picks another chip; the chip that is being explained is marked selected.
 *
 * It states three things and no more: what the signal says, how many distinct
 * sources asserted it about the same party and when they were last read, and
 * where the records are. The link scrolls to the source card that produced it
 * rather than duplicating the evidence here, because a second copy of a record
 * is a second thing that can go stale.
 *
 * **Only sources that have a card get a link.** Not every source_id on a
 * signal produces one: `icij` is not a registered adapter, so `OFFSHORE_LEAKS`
 * carries its name but the report has no `#source-icij` to scroll to. Rendered
 * as a button anyway, it looked exactly like the working one beside it and did
 * nothing at all — on a live BP lookup that was the headline finding's own
 * evidence link. Sources without a card are named in one sentence, which is
 * what they are: the attribution, not a place to go.
 *
 * `hasCard` / `onShowSource` are optional because the chips outside this
 * section — the verdict strip, a source card, the graph legend — render the
 * same box with no report body to scroll to.
 */

import { RISK_PRESENTATION } from "./RiskChip";
import {
  attributionSentence,
  evidenceFooter,
  splitEvidenceSources,
  type SignalEvidenceData,
} from "../../lib/signalEvidence";
import { recordConsistencyRows } from "../../lib/recordConsistency";
import { sanctionsOwnershipNotes, sanctionsOwnershipRows } from "../../lib/sanctionsOwnership";
import { sourceLabel } from "../../lib/vocab";

export function SignalEvidence({
  lead,
  sourceNames,
  hasCard = () => false,
  onShowSource,
}: {
  lead: SignalEvidenceData;
  sourceNames?: Record<string, string>;
  /** Which source ids actually have a card on this report. */
  hasCard?: (sourceId: string) => boolean;
  /** Scroll to (and flash) a source card. */
  onShowSource?: (sourceId: string) => void;
}) {
  const label = RISK_PRESENTATION[lead.signal.code]?.label ?? lead.signal.code;
  const footer = evidenceFooter(lead);
  const { linked, named } = splitEvidenceSources(
    lead.sourceIds,
    (id) => Boolean(onShowSource) && hasCard(id)
  );
  const attribution = attributionSentence(named, sourceNames);

  return (
    <div className="rounded-oo border border-oo-rule bg-oo-bg px-3 py-2.5 text-oo-small leading-[1.6] text-oo-ink">
      <span className="font-bold">{label}</span> — {lead.signal.summary}
      {footer && <span className="text-oo-muted"> {footer}</span>}
      {/* One control per source that has a card, rather than one "Show the
          records" that has to guess which of several to open. Each says what
          it does in its own text, so a screen reader announcing the buttons
          alone still gets two actions rather than two source names. */}
      {linked.map((id) => (
        <span key={id}>
          {" "}
          <button
            type="button"
            onClick={() => onShowSource?.(id)}
            className="font-medium text-oo-blue underline underline-offset-2 hover:no-underline"
          >
            Show the {sourceLabel(id, sourceNames)} record
          </button>
        </span>
      ))}
      {/* The rest are attribution, not navigation, and they belong in one
          sentence — "From OpenSanctions. From EveryPolitician." read as two
          findings about the same party. */}
      {attribution && <span className="text-oo-muted"> {attribution}</span>}
      <RecordConsistencyTable lead={lead} sourceNames={sourceNames} />
      <SanctionsOwnershipTable lead={lead} />
    </div>
  );
}

/**
 * The per-field table behind a "Records compared" chip (Phase 337).
 *
 * One row per comparison the signal says — field, source A and its value,
 * source B and its value — so a reader can see which two records the
 * sentence is about. Renders nothing for any other signal, and nothing when
 * the evidence carries no rows (two sources, nothing comparable between
 * them: the sentence already says so). Agreements are listed with the
 * disagreements, in the same voice: the table says what was compared, not
 * only what differed.
 */
function RecordConsistencyTable({
  lead,
  sourceNames,
}: {
  lead: SignalEvidenceData;
  sourceNames?: Record<string, string>;
}) {
  const rows = recordConsistencyRows(lead.signal);
  if (rows.length === 0) return null;
  return (
    <table className="mt-2.5 w-full text-oo-meta leading-snug">
      <caption className="sr-only">Records compared, by field</caption>
      <thead>
        <tr className="text-left text-oo-muted">
          <th scope="col" className="pr-3 pb-1 font-medium">Field</th>
          <th scope="col" className="pr-3 pb-1 font-medium">Source</th>
          <th scope="col" className="pr-3 pb-1 font-medium">Value</th>
          <th scope="col" className="pr-3 pb-1 font-medium">Source</th>
          <th scope="col" className="pb-1 font-medium">Value</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr
            key={row.key}
            className={`border-t border-oo-rule align-top ${row.relation === "disagree" ? "font-medium" : ""}`}
          >
            <td className="pr-3 py-1">{row.fieldLabel}</td>
            <td className="pr-3 py-1">{sourceLabel(row.sourceA, sourceNames)}</td>
            <td className="pr-3 py-1">{row.valueA}</td>
            <td className="pr-3 py-1">{sourceLabel(row.sourceB, sourceNames)}</td>
            <td className="py-1">
              {row.valueB}
              {row.relation === "disagree" && (
                <span className="sr-only"> (the two sources differ)</span>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/**
 * The per-regime table behind a sanctions-ownership chip (Phase 342).
 *
 * One row per regime and source — OFAC, the EU and the UK side by side, the
 * rule each applies, the result, the holding and who holds it — then how the
 * test was computed and what it does not compute. Renders nothing for any
 * other signal.
 */
function SanctionsOwnershipTable({ lead }: { lead: SignalEvidenceData }) {
  const rows = sanctionsOwnershipRows(lead.signal);
  if (rows.length === 0) return null;
  const notes = sanctionsOwnershipNotes(lead.signal);
  return (
    <>
      <table className="mt-2.5 w-full text-oo-meta leading-snug">
        <caption className="sr-only">Sanctions ownership test, by regime and source</caption>
        <thead>
          <tr className="text-left text-oo-muted">
            <th scope="col" className="pr-3 pb-1 font-medium">Regime</th>
            <th scope="col" className="pr-3 pb-1 font-medium">Result</th>
            <th scope="col" className="pr-3 pb-1 font-medium">Holding</th>
            <th scope="col" className="pb-1 font-medium">Source</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={row.key}
              className={`border-t border-oo-rule align-top ${row.result === "meets" ? "font-medium" : ""}`}
            >
              <td className="pr-3 py-1">
                {row.regime}
                <span className="block text-oo-muted font-normal">{row.rule}</span>
              </td>
              <td className="pr-3 py-1">
                {row.resultWords}
                {row.ifConfirmed && (
                  <span className="block text-oo-muted font-normal">
                    Would meet it if a name-only match were confirmed
                  </span>
                )}
              </td>
              <td className="pr-3 py-1">
                {row.holding}
                {row.holders && <span className="block text-oo-muted font-normal">{row.holders}</span>}
              </td>
              <td className="py-1">{row.source}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {notes.map((n) => (
        <p key={n} className="mt-1.5 text-oo-meta text-oo-muted">{n}</p>
      ))}
    </>
  );
}
