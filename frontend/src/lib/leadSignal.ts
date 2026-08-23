/**
 * The one signal the Risk signals section says out loud (Phase 129).
 *
 * The section rendered a row of chips and a caption telling the reader to
 * select one. That is a menu, not a finding: the most serious thing the check
 * turned up was a word in a pill, and the sentence explaining it — which the
 * chip has always carried — only appeared once someone clicked. The v2 design
 * puts the worst signal's evidence on the page, always visible, underneath the
 * chips.
 *
 * Everything here is derived from the signals the backend already sent. Three
 * rules keep it from saying more than they support:
 *
 * - **Corroboration is counted in distinct sources, not in signals.** The risk
 *   layer emits one signal per matching hit, so OpenSanctions alone can
 *   produce three for one code. "Corroborated by three sources" would then be
 *   a claim about one source's thoroughness.
 * - **"Most recently checked" is a retrieval time OpenCheck observed**, taken
 *   from `source_liveness`, and omitted entirely when none of the contributing
 *   sources reported one — never "today" by default.
 * - **The lead is the worst, and severity is `SIGNAL_STYLE`'s**, the same
 *   ordering the graph badges stack by. A second ordering would let the
 *   section headline a different signal than the graph marks as worst.
 */

import { SIGNAL_STYLE } from "./graphStyle";
import type { RiskSignal, SourceLiveness } from "./api";

export interface LeadSignal {
  signal: RiskSignal;
  /** Distinct `source_id`s that asserted this code. */
  sourceCount: number;
  /** Their display ids, in first-seen order — the records to show. */
  sourceIds: string[];
  /** The most recent retrieval OpenCheck observed across those sources, or
   *  null when none of them reported one. */
  checkedAt: string | null;
}

/** Severity for a code, or -1 when the code is not in the style table — an
 *  unknown code must never outrank a known one by accident. */
function severityOf(code: string): number {
  return SIGNAL_STYLE[code]?.severity ?? -1;
}

const CONFIDENCE_RANK: Record<string, number> = { high: 2, medium: 1, low: 0 };

/**
 * The signal to lead with, with the corroboration behind it.
 *
 * Returns `null` when there is nothing to lead with — which is a state the
 * caller must render as itself, not as an empty box.
 */
export function leadSignal(
  signals: RiskSignal[],
  liveness: Record<string, SourceLiveness> = {}
): LeadSignal | null {
  const risks = signals.filter((s) => (s.kind ?? "risk") === "risk");
  if (risks.length === 0) return null;

  let best = risks[0];
  for (const s of risks.slice(1)) {
    const bySeverity = severityOf(s.code) - severityOf(best.code);
    if (bySeverity > 0) {
      best = s;
      continue;
    }
    if (bySeverity < 0) continue;
    // Same code or same severity: prefer the better-corroborated instance, so
    // the sentence shown is the strongest evidence for the lead finding.
    if ((CONFIDENCE_RANK[s.confidence] ?? 0) > (CONFIDENCE_RANK[best.confidence] ?? 0)) {
      best = s;
    }
  }

  const sourceIds: string[] = [];
  for (const s of risks) {
    if (s.code !== best.code) continue;
    if (s.source_id && !sourceIds.includes(s.source_id)) sourceIds.push(s.source_id);
  }

  let checkedAt: string | null = null;
  for (const id of sourceIds) {
    const at = liveness[id]?.retrieved_at;
    if (typeof at === "string" && (checkedAt === null || at > checkedAt)) checkedAt = at;
  }

  return { signal: best, sourceCount: sourceIds.length, sourceIds, checkedAt };
}

/**
 * The corroboration clause: "Corroborated by two sources" / "Reported by one
 * source". Numbers up to ten as words, because this is a sentence.
 *
 * "Corroborated" is used only at two or more — it is the word `ui/Chip`'s
 * confidence legend defines that way, and applying it to a single source would
 * make the section's own vocabulary disagree with the legend beside it.
 */
const WORD = [
  "",
  "one",
  "two",
  "three",
  "four",
  "five",
  "six",
  "seven",
  "eight",
  "nine",
  "ten",
];

export function corroborationClause(sourceCount: number): string {
  if (sourceCount <= 0) return "";
  const n = WORD[sourceCount] ?? String(sourceCount);
  if (sourceCount === 1) return "Reported by one source";
  return `Corroborated by ${n} sources`;
}

/** "most recently checked 21 August 2026", or "" when nothing was observed. */
export function checkedClause(
  iso: string | null,
  locale = "en-GB"
): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const formatted = d.toLocaleDateString(locale, {
    day: "numeric",
    month: "long",
    year: "numeric",
  });
  return `most recently checked ${formatted}`;
}

/** The whole second sentence, with only the parts that are true. */
export function evidenceFooter(lead: LeadSignal, locale = "en-GB"): string {
  const parts = [corroborationClause(lead.sourceCount), checkedClause(lead.checkedAt, locale)]
    .filter(Boolean);
  return parts.length === 0 ? "" : `${parts.join(", ")}.`;
}
