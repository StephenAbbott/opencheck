/**
 * Which chips a homepage example card shows (Phase 248).
 *
 * `EXAMPLE_LEIS` in `components/HomePanels.tsx` carries **every** distinct
 * risk code production returns for each curated subject — the claim it makes
 * is "this is what the check finds", so it has to be the whole list. A card
 * has room for three chips, so this picks them: worst graph severity first
 * (`SIGNAL_STYLE`, the same ranking the graph's stacked badge uses), then
 * confidence, then a finding on the company itself before one on a related
 * party, and the rest are counted in a "+N more" note rather than dropped
 * silently (Stephen, 25 Sept 2026). Rosneft had 14 codes and a card showing
 * two with no hint that there were more.
 *
 * Pure so the logic-only suite can pin it (`exampleCards.test.ts`).
 */
import { SIGNAL_STYLE } from "./graphStyle";

export interface CardSignal {
  code: string;
  confidence: "high" | "medium" | "low";
}

/** Chips a card shows before the "+N more" note. */
export const MAX_CARD_CHIPS = 3;

const CONFIDENCE_RANK: Record<string, number> = { high: 3, medium: 2, low: 1 };

function severity(code: string): number {
  return SIGNAL_STYLE[code]?.severity ?? 0;
}

/** Worst first: severity, confidence, subject before related. Stable. */
export function rankCardSignals<T extends CardSignal>(signals: readonly T[]): T[] {
  return signals
    .map((sig, i) => ({ sig, i }))
    .sort((a, b) => {
      const bySeverity = severity(b.sig.code) - severity(a.sig.code);
      if (bySeverity) return bySeverity;
      const byConfidence =
        (CONFIDENCE_RANK[b.sig.confidence] ?? 0) - (CONFIDENCE_RANK[a.sig.confidence] ?? 0);
      if (byConfidence) return byConfidence;
      const related = (s: CardSignal) => (s.code.startsWith("RELATED_") ? 1 : 0);
      const byScope = related(a.sig) - related(b.sig);
      if (byScope) return byScope;
      return a.i - b.i;
    })
    .map(({ sig }) => sig);
}

/** The chips to draw and how many more there are. */
export function cardChips<T extends CardSignal>(
  signals: readonly T[] | undefined,
  max: number = MAX_CARD_CHIPS,
): { shown: T[]; more: number } {
  const ranked = rankCardSignals(signals ?? []);
  return { shown: ranked.slice(0, max), more: Math.max(0, ranked.length - max) };
}

/** "+11 more findings" — the note after the chips; empty when none. */
export function moreFindingsLabel(more: number): string {
  if (more <= 0) return "";
  return `+${more} more finding${more === 1 ? "" : "s"}`;
}
