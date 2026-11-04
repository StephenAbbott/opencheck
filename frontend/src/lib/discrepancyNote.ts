/**
 * The discrepancy note (the "Discrepancy report export for registers" ticket).
 *
 * The backend builds a one-page note per register that holds a disputed
 * value, from the run's `RECORD_CONSISTENCY` disagreements, and says where
 * that register takes discrepancy reports. This is the values layer the
 * export menu and the "Records compared" evidence read: whether the run has
 * anything to export, and the words the control wears either way. Pure, so
 * the logic-only suite pins them.
 *
 * Only a shown `disagree` item qualifies — the same rule as
 * `opencheck/discrepancy_note.py`'s `qualifying_items`, which decides what the
 * route answers; this side exists so the menu can disable the item with its
 * reason instead of letting a click find a 404.
 */

import type { RiskSignal } from "./api";
import { recordConsistencyRows } from "./recordConsistency";

export const DISCREPANCY_NOTE_LABEL = "Discrepancy note";
export const DISCREPANCY_NOTE_BUSY = "Preparing discrepancy note…";
/** The item's description when a note can be made. */
export const DISCREPANCY_NOTE_DESCRIPTION =
  "One page per register whose record differs, with where to send it";
/** The item's description when nothing qualifies — a greyed-out control
 *  that does not say why is a dead end. */
export const DISCREPANCY_NOTE_UNAVAILABLE =
  "No register record differs in this report, so there is nothing to send";
/** The link under the Records compared table. */
export const DISCREPANCY_NOTE_LINK = "Export a discrepancy note for the registers";

/** The RECORD_CONSISTENCY signal of a run, if it has one. */
export function consistencySignal(signals: RiskSignal[]): RiskSignal | null {
  return signals.find((s) => s.code === "RECORD_CONSISTENCY") ?? null;
}

/** Whether the run carries at least one shown disagreement — the only
 *  finding a note is built from. */
export function discrepancyNoteAvailable(signals: RiskSignal[]): boolean {
  const sig = consistencySignal(signals);
  if (!sig) return false;
  return recordConsistencyRows(sig).some((r) => r.relation === "disagree");
}

/** The export-menu item's words for a run. */
export function discrepancyNoteItem(
  signals: RiskSignal[],
  busy: boolean,
): { label: string; description: string; disabled: boolean } {
  const available = discrepancyNoteAvailable(signals);
  return {
    label: busy ? DISCREPANCY_NOTE_BUSY : DISCREPANCY_NOTE_LABEL,
    description: available ? DISCREPANCY_NOTE_DESCRIPTION : DISCREPANCY_NOTE_UNAVAILABLE,
    disabled: !available || busy,
  };
}
