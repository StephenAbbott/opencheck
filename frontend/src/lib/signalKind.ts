/**
 * signalKind — risk findings vs structural context.
 *
 * The backend classifies every signal with `kind`: `"risk"` (an adverse
 * finding) or `"context"` (a structural observation that is worth showing
 * but is not a risk). The canonical example is `NON_EU_JURISDICTION` —
 * neither the AMLA CDD RTS nor AMLR Annex III treats being outside the EU
 * as a risk factor in itself, so reporting it as one, at high confidence,
 * next to genuine adverse findings, overstated it.
 *
 * This lives in one module rather than inline in App.tsx for the same
 * reason `signalScope.ts` does: the classification is read by the chip
 * strip, the subject card and (server-side) the OG share card and share
 * meta description. Hand-kept copies of that rule in four places is how
 * the curated homepage claims drifted before.
 *
 * A missing `kind` means `"risk"`, so responses cached before the field
 * existed behave exactly as they did.
 */
import type { RiskSignal } from "./api";

export function signalKind(signal: RiskSignal): "risk" | "context" {
  return signal.kind === "context" ? "context" : "risk";
}

export function isRiskFinding(signal: RiskSignal): boolean {
  return signalKind(signal) === "risk";
}

export function isContextObservation(signal: RiskSignal): boolean {
  return signalKind(signal) === "context";
}

/**
 * How many distinct risk findings there are, counted the way the report
 * counts them.
 *
 * The risk layer emits one signal per matching hit, so a list of nine can be
 * two findings. The verdict strip has always deduplicated by code — "4
 * signals — 2 risk, 2 structural" — while FullCheck's network-risk line read
 * `signals.length` straight off the array and said "QuickCheck flagged 9
 * signals on the subject" one screen below it. Two counts of the same thing,
 * on the same page, both labelled as what the check found.
 */
export function riskFindingCount(signals: RiskSignal[]): number {
  return new Set(signals.filter(isRiskFinding).map((s) => s.code)).size;
}

/** Split signals into `[risk, context]`, preserving input order in each. */
export function partitionByKind(
  signals: RiskSignal[],
): [RiskSignal[], RiskSignal[]] {
  const risk: RiskSignal[] = [];
  const context: RiskSignal[] = [];
  for (const sig of signals) {
    (isRiskFinding(sig) ? risk : context).push(sig);
  }
  return [risk, context];
}

/** One chip's worth of signals: every instance of one code (Phase 250). */
export interface SignalGroup {
  code: string;
  /** The instance the chip shows: the most confident, first on ties. */
  lead: RiskSignal;
  /** Every instance, in input order — the chip's evidence lists them all. */
  signals: RiskSignal[];
}

const CONFIDENCE_RANK: Record<string, number> = { high: 3, medium: 2, low: 1 };

/**
 * Group signals by code, in first-seen order.
 *
 * FullCheck's Network risk box said "5 risk signals" over twelve chips, three
 * of them "Outside EU/EEA": the sentence counted distinct risk codes
 * (`riskFindingCount`), the chips drew one per instance, and two of the codes
 * drawn were context, which the sentence does not count. Grouping here is the
 * same rule `riskFindingCount` applies, so a chip row built from
 * `groupSignalsByCode(risk)` has exactly `riskFindingCount(risk)` chips.
 */
export function groupSignalsByCode(signals: RiskSignal[]): SignalGroup[] {
  const groups = new Map<string, SignalGroup>();
  for (const sig of signals) {
    const g = groups.get(sig.code);
    if (!g) {
      groups.set(sig.code, { code: sig.code, lead: sig, signals: [sig] });
      continue;
    }
    g.signals.push(sig);
    if ((CONFIDENCE_RANK[sig.confidence] ?? 0) > (CONFIDENCE_RANK[g.lead.confidence] ?? 0)) g.lead = sig;
  }
  return [...groups.values()];
}

/** Risk groups then context groups — what the Network risk box draws. */
export function groupNetworkSignals(signals: RiskSignal[]): { risk: SignalGroup[]; context: SignalGroup[] } {
  const [risk, context] = partitionByKind(signals);
  return { risk: groupSignalsByCode(risk), context: groupSignalsByCode(context) };
}
