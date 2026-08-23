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
