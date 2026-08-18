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
