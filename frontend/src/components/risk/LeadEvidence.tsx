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
 * sources asserted it and when they were last read, and where the records are.
 * "Show the N records" scrolls to the source cards that produced it — the same
 * `#source-<id>` anchors the cross-source identifier table jumps to — rather
 * than duplicating the evidence here, because a second copy of a record is a
 * second thing that can go stale.
 */

import { RISK_PRESENTATION } from "./RiskChip";
import { evidenceFooter, type LeadSignal } from "../../lib/leadSignal";
import { sourceLabel } from "../../lib/vocab";

export function LeadEvidence({
  lead,
  sourceNames,
  onShowSource,
}: {
  lead: LeadSignal;
  sourceNames?: Record<string, string>;
  /** Scroll to (and flash) a source card. */
  onShowSource: (sourceId: string) => void;
}) {
  const label = RISK_PRESENTATION[lead.signal.code]?.label ?? lead.signal.code;
  const footer = evidenceFooter(lead);
  const shown = lead.sourceIds.filter(Boolean);

  return (
    <div className="rounded-oo border border-oo-rule bg-oo-bg px-3 py-2.5 text-oo-small leading-[1.6] text-oo-ink">
      <span className="font-bold">{label}</span> — {lead.signal.summary}
      {footer && <span className="text-oo-muted"> {footer}</span>}
      {shown.length > 0 && (
        <>
          {" "}
          {/* One button per source rather than one "Show the records" that has
              to guess which of several to open. With a single source it reads
              as one link, which is the common case. */}
          {shown.map((id, i) => (
            <span key={id}>
              {i > 0 && <span className="text-oo-muted"> · </span>}
              <button
                type="button"
                onClick={() => onShowSource(id)}
                className="font-medium text-oo-blue hover:underline underline-offset-2"
              >
                {shown.length === 1
                  ? `Show the ${sourceLabel(id, sourceNames)} record`
                  : sourceLabel(id, sourceNames)}
              </button>
            </span>
          ))}
          {shown.length > 1 && (
            <span className="text-oo-muted"> — show the records</span>
          )}
        </>
      )}
    </div>
  );
}
