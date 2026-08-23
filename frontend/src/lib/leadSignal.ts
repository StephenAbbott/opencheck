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
 * - **Corroboration is counted in distinct sources, about the same party.**
 *   Counting by code alone was wrong twice over: the risk layer emits one
 *   signal per matching hit, so one source can produce three for a code; and
 *   two sources can carry the same code about *different* parties. A live BP
 *   lookup produced exactly that — OpenAleph flagged BP itself in the
 *   OffshoreLeaks collection while ICIJ flagged a subsidiary in the Bahamas
 *   Leaks — and the box said "Corroborated by two sources" under a sentence
 *   about the subsidiary. Signals corroborate only when they name the same
 *   code *and* the same statement.
 * - **The checked date is the oldest across the contributing sources, and is
 *   omitted unless every one of them reported a retrieval time.** The oldest,
 *   not the newest, for the reason `provenance.Recorder.resolve` takes the
 *   oldest: a claim is only as current as its stalest component. Sources that
 *   never reach the dispatch loop — `icij` is not a registered adapter — have
 *   no `source_liveness` entry at all, and generalising a sibling's date onto
 *   them stated a currency nothing had established.
 * - **The lead is the worst, and severity is `SIGNAL_STYLE`'s**, the same
 *   ordering the graph badges stack by. A second ordering would let the
 *   section headline a different signal than the graph marks as worst. Ties
 *   break on confidence and then alphabetically by code — several codes share
 *   a severity (PEP and DEBARMENT are both 4), and without the last step the
 *   headline was decided by the order two `out.append` calls happen to appear
 *   in a Python function.
 */

import { SIGNAL_STYLE } from "./graphStyle";
import { isRiskFinding } from "./signalKind";
import { sourceList } from "./vocab";
import type { RiskSignal, SourceLiveness } from "./api";

export interface LeadSignal {
  signal: RiskSignal;
  /** Distinct `source_id`s that asserted this code about this party. */
  sourceCount: number;
  /** Their display ids, in first-seen order — the records to show. */
  sourceIds: string[];
  /** The oldest retrieval OpenCheck observed across those sources, or null
   *  when any of them reported none. */
  checkedAt: string | null;
}

/**
 * What a signal is *about*, for corroboration.
 *
 * `subject_statement_id` is set on `RELATED_*` findings and names the related
 * party; `statement_id` is set on subject-level ones. Falling back to the code
 * means signals that carry neither are treated as being about the subject,
 * which is what they are.
 */
export function signalSubjectKey(signal: RiskSignal): string {
  const e = (signal.evidence ?? {}) as Record<string, unknown>;
  const subject = e.subject_statement_id;
  if (typeof subject === "string" && subject) return `subject:${subject}`;
  const statement = e.statement_id;
  if (typeof statement === "string" && statement) return `statement:${statement}`;
  return "self";
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
  const risks = signals.filter(isRiskFinding);
  if (risks.length === 0) return null;
  return evidenceFor(risks, pickWorst(risks), liveness);
}

/**
 * The same evidence, for a code the reader chose rather than the worst one.
 *
 * The section shows one box: the worst signal by default, and whichever chip
 * the reader selects after that. Before this, each chip owned its own
 * expansion, so selecting one opened a *second* box below the first in a
 * different style — one sentence about the finding rendered two ways on one
 * screen, which is what "some things are showing twice" meant.
 *
 * Structural context codes are eligible here and are not eligible for the
 * lead: a reader can select one to read it, but a fact about how the company
 * is put together can never be what the section leads with.
 */
export function evidenceForCode(
  signals: RiskSignal[],
  code: string,
  liveness: Record<string, SourceLiveness> = {}
): LeadSignal | null {
  const matching = signals.filter((s) => s.code === code);
  if (matching.length === 0) return null;
  return evidenceFor(signals, pickWorst(matching), liveness);
}

/** The worst of a non-empty list, by severity then confidence then code. */
function pickWorst(signals: RiskSignal[]): RiskSignal {
  let best = signals[0];
  for (const s of signals.slice(1)) {
    const bySeverity = severityOf(s.code) - severityOf(best.code);
    if (bySeverity > 0) {
      best = s;
      continue;
    }
    if (bySeverity < 0) continue;
    // Same severity: prefer the better-corroborated instance, so the sentence
    // shown is the strongest evidence for the lead finding.
    const byConfidence =
      (CONFIDENCE_RANK[s.confidence] ?? 0) - (CONFIDENCE_RANK[best.confidence] ?? 0);
    if (byConfidence > 0) {
      best = s;
      continue;
    }
    if (byConfidence < 0) continue;
    // Still tied, and several codes share a severity. Alphabetical is
    // arbitrary but *stable*; without it the headline depended on the order
    // the backend happened to append two signals.
    if (s.code < best.code) best = s;
  }
  return best;
}

/**
 * The corroboration behind one chosen signal, counted over the whole list.
 *
 * `signals` is the full set, not just the instances of the chosen code: the
 * count is "how many distinct sources said *this*", so it has to look at
 * everything that could have said it.
 */
function evidenceFor(
  signals: RiskSignal[],
  best: RiskSignal,
  liveness: Record<string, SourceLiveness>
): LeadSignal {
  const subject = signalSubjectKey(best);
  const sourceIds: string[] = [];
  for (const s of signals) {
    if (s.code !== best.code || signalSubjectKey(s) !== subject) continue;
    if (s.source_id && !sourceIds.includes(s.source_id)) sourceIds.push(s.source_id);
  }

  // Oldest, and only when every contributing source reported one.
  let checkedAt: string | null = null;
  for (const id of sourceIds) {
    const at = liveness[id]?.retrieved_at;
    if (typeof at !== "string" || !at) {
      checkedAt = null;
      break;
    }
    if (checkedAt === null || at < checkedAt) checkedAt = at;
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

/** "last checked 21 August 2026", or "" when nothing was observed. */
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
  return `last checked ${formatted}`;
}

/**
 * The sources that named the finding but have no card to scroll to, as one
 * sentence.
 *
 * They used to be rendered one per source — "From OpenSanctions. From
 * EveryPolitician." — because the box mapped over every source id and gave the
 * unlinked branch a full stop of its own. Two attributions for one finding read
 * as two findings. `sourceList` is the project's English list joiner and
 * already deduplicates, so the sentence is built from it rather than from a
 * second `join`.
 */
export function attributionSentence(
  sourceIds: string[],
  names?: Record<string, string>
): string {
  const list = sourceList(sourceIds, names);
  return list ? `From ${list}.` : "";
}

/**
 * Split the contributing sources into the ones the report can scroll to and
 * the ones it can only name. Pure, so the rule is pinned by the logic-only
 * suite rather than living inside JSX.
 */
export function splitEvidenceSources(
  sourceIds: string[],
  hasCard: (sourceId: string) => boolean
): { linked: string[]; named: string[] } {
  const linked: string[] = [];
  const named: string[] = [];
  for (const id of sourceIds) {
    if (!id) continue;
    (hasCard(id) ? linked : named).push(id);
  }
  return { linked, named };
}

/** The whole second sentence, with only the parts that are true. */
export function evidenceFooter(lead: LeadSignal, locale = "en-GB"): string {
  const parts = [corroborationClause(lead.sourceCount), checkedClause(lead.checkedAt, locale)]
    .filter(Boolean);
  return parts.length === 0 ? "" : `${parts.join(", ")}.`;
}
