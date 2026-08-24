/**
 * What the Risk signals section says when a reader selects a chip.
 *
 * The section is a row of chips and one box under them. The chip is the
 * question; this builds the answer — the signal's own sentence, how many
 * distinct sources asserted it about the same party, when they were last
 * read, and which of them have a record on the page.
 *
 * **It no longer picks anything.** Until Phase 132 the box opened on the
 * *worst* signal, chosen by the severity ordering the graph badges stack by,
 * and captioned "the most serious signal is shown above". That is OpenCheck
 * grading a company's findings: the product's own rule is that a signal is a
 * pointer to a record and not a conclusion, and deciding which finding a
 * reader meets first is a conclusion. Severity still exists in
 * `lib/graphStyle.ts`, where the graph needs it to stack overlapping badges
 * on one node; nothing in the report reads it.
 *
 * Two rules keep the box from saying more than the signals support:
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
 */

import { sourceList } from "./vocab";
import type { RiskSignal, SourceLiveness } from "./api";

export interface SignalEvidenceData {
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

const CONFIDENCE_RANK: Record<string, number> = { high: 2, medium: 1, low: 0 };

/**
 * The evidence for the code the reader selected.
 *
 * `null` when the code is not in the list — which happens when a selection
 * outlives a re-run that no longer produces it. That is the same state as
 * "nothing selected yet", and the section renders no box for either.
 *
 * Risk findings and structural context are both eligible: the section makes
 * the distinction with a caption and a sub-block, not by refusing to explain
 * one of them.
 */
export function evidenceForCode(
  signals: RiskSignal[],
  code: string,
  liveness: Record<string, SourceLiveness> = {}
): SignalEvidenceData | null {
  const matching = signals.filter((s) => s.code === code);
  if (matching.length === 0) return null;
  return evidenceFor(signals, pickStrongest(matching), liveness);
}

/**
 * Which instance of one code to show.
 *
 * A code can arrive several times — one signal per matching hit. They all say
 * the same thing about the same company, so this is not a ranking of findings
 * (see the note at the top); it picks the best-evidenced sentence for the one
 * finding the chip stands for: highest confidence, then a stable tiebreak so
 * the box does not depend on the order two `out.append` calls happen to appear
 * in a Python function.
 */
function pickStrongest(signals: RiskSignal[]): RiskSignal {
  let best = signals[0];
  for (const s of signals.slice(1)) {
    const byConfidence =
      (CONFIDENCE_RANK[s.confidence] ?? 0) - (CONFIDENCE_RANK[best.confidence] ?? 0);
    if (byConfidence > 0) {
      best = s;
      continue;
    }
    if (byConfidence < 0) continue;
    // Tied. Alphabetical by hit id is arbitrary but *stable*; the alternative
    // is whichever the backend appended first.
    if ((s.hit_id ?? "") < (best.hit_id ?? "")) best = s;
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
): SignalEvidenceData {
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
export function evidenceFooter(lead: SignalEvidenceData, locale = "en-GB"): string {
  const parts = [corroborationClause(lead.sourceCount), checkedClause(lead.checkedAt, locale)]
    .filter(Boolean);
  return parts.length === 0 ? "" : `${parts.join(", ")}.`;
}
