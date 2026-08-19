/**
 * evidenceDisclosure — when and how the AI summary's Evidence section
 * collapses.
 *
 * Long evidence lists (Rosneft: 15 claims × ~4 citation chips each) pushed
 * the rest of the results page two-plus screens down, especially on curated
 * examples where the summary renders automatically. The section therefore
 * collapses to a short preview with a clear "Show all N" call to action —
 * but the collapse must never hide two things:
 *
 * - **The grounding promise.** The card's subtitle says "every statement
 *   links to its source", so a preview of real claims with live citation
 *   chips stays visible, and the header always states the full count
 *   ("15 statements, cited to 8 sources") even while collapsed.
 * - **The analyst control plane.** When sign-off is active (a live run with
 *   disposition controls), collapsing would hide the accept/dispute buttons
 *   whose state is embedded in the PDF record — so sign-off disables the
 *   default collapse entirely.
 *
 * The logic lives here rather than inline in NarrativePanel for the same
 * reason `signalKind.ts` does: pure, testable rules kept out of a large
 * component.
 */
/** The slice of a citation target this module needs (facts and risks both
 *  carry `source_name`; `EvidencePacket` satisfies this structurally). */
interface CitedEvidence {
  id: string;
  source_name: string;
}

/** Claims shown while the evidence section is collapsed. */
export const EVIDENCE_PREVIEW_COUNT = 3;

/** Lists at or below this length never collapse — the CTA would save no space. */
export const EVIDENCE_COLLAPSE_THRESHOLD = 5;

/**
 * Should the evidence section start collapsed?
 *
 * True only for long lists on summaries with no active sign-off (curated
 * examples, or live runs viewed read-only). `canSignOff` mirrors the
 * component's own flag: a live narrative whose disposition controls render.
 */
export function shouldCollapseEvidence(claimCount: number, canSignOff: boolean): boolean {
  return claimCount > EVIDENCE_COLLAPSE_THRESHOLD && !canSignOff;
}

/**
 * Distinct source labels cited across the claims, in first-seen order.
 * Resolves each cited id against the packet the way the citation chips do:
 * facts and risks carry a `source_name`; gap ids render as "Limitation"
 * chips and are not sources, so they are excluded from the count.
 */
export function citedSourceLabels(
  claims: { fact_ids: string[] }[],
  packet: { facts: CitedEvidence[]; risks: CitedEvidence[] },
): string[] {
  const labels: string[] = [];
  for (const claim of claims) {
    for (const id of claim.fact_ids) {
      const fact = packet.facts.find((f) => f.id === id);
      const label = fact
        ? fact.source_name
        : packet.risks.find((r) => r.id === id)?.source_name;
      if (label && !labels.includes(label)) labels.push(label);
    }
  }
  return labels;
}
