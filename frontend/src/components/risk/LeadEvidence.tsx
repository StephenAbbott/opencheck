/**
 * The worst signal, said out loud (Phase 129).
 *
 * The Risk signals section used to be a row of chips and a caption asking the
 * reader to select one. That is a menu, not a finding: the most serious thing
 * the check turned up was a word in a pill, and the sentence explaining it —
 * which the chip has always carried — appeared only after a click. This box
 * puts that sentence on the page.
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
 * evidence link. Sources without a card are named as plain text, which is what
 * they are: the attribution, not a place to go.
 */

import { RISK_PRESENTATION } from "./RiskChip";
import {
  attributionSentence,
  evidenceFooter,
  splitEvidenceSources,
  type LeadSignal,
} from "../../lib/leadSignal";
import { sourceLabel } from "../../lib/vocab";

export function LeadEvidence({
  lead,
  sourceNames,
  hasCard,
  onShowSource,
}: {
  lead: LeadSignal;
  sourceNames?: Record<string, string>;
  /** Which source ids actually have a card on this report. */
  hasCard: (sourceId: string) => boolean;
  /** Scroll to (and flash) a source card. */
  onShowSource: (sourceId: string) => void;
}) {
  const label = RISK_PRESENTATION[lead.signal.code]?.label ?? lead.signal.code;
  const footer = evidenceFooter(lead);
  const { linked, named } = splitEvidenceSources(lead.sourceIds, hasCard);
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
            onClick={() => onShowSource(id)}
            className="font-medium text-oo-blue hover:underline underline-offset-2"
          >
            Show the {sourceLabel(id, sourceNames)} record
          </button>
        </span>
      ))}
      {/* The rest are attribution, not navigation, and they belong in one
          sentence — "From OpenSanctions. From EveryPolitician." read as two
          findings about the same party. */}
      {attribution && <span className="text-oo-muted"> {attribution}</span>}
    </div>
  );
}
