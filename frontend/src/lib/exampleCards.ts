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
import { CHECK_MODES, modeParam, type CheckMode } from "./checkMode";
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

// ---------------------------------------------------------------------------
// The six views on each card (the curated-example views ticket, 16 Nov 2026)
// ---------------------------------------------------------------------------


/**
 * What each view answers, in a few words, for the tile under its name. The
 * full sentence is the report's blurb (`MODE_TABS` in `cdd/ModeTabs.tsx`);
 * a tile has room for one short line, so these say only what the view is
 * for, and assert nothing about what it finds for any one company.
 */
export const EXAMPLE_VIEW_ANSWERS: Record<CheckMode, string> = {
  quick: "Who owns it, in one screen",
  full: "Follow owners up the chain",
  background: "Screen the people",
  subsidiaries: "What it owns",
  history: "How its records changed",
  esg: "Emissions and payments",
};

/** The views a card links to, in the report's tab order. */
export const EXAMPLE_VIEWS: readonly CheckMode[] = CHECK_MODES;

/**
 * The deep link for one view of one example: the report's own shareable
 * form, `/?lei=…` plus `&mode=…` for anything but QuickCheck (which keeps
 * the short form, as `modeParam` says).
 */
export function exampleViewHref(lei: string, mode: CheckMode): string {
  const param = modeParam(mode);
  const params = new URLSearchParams({ lei });
  if (param) params.set("mode", param);
  return `/?${params.toString()}`;
}

/**
 * The examples a phone shows (Stephen, 16 Nov 2026). With six tiles each, four
 * stacked cards made the homepage very long on a phone, so below `md` (where
 * the cards stack) only these two render; from `md` up every example does.
 * Rosneft (sanctions) and Eesti Energia AS (state control, a related PEP) keep
 * two different kinds of finding on the small screen.
 */
export const PHONE_EXAMPLE_LEIS: ReadonlySet<string> = new Set([
  "253400JT3MQWNDKMJE44", // Rosneft
  "5493005044RTLQ5RZU70", // Eesti Energia AS
]);

/** Classes that hide an example below `md` unless a phone shows it. */
export function exampleVisibilityClass(lei: string): string {
  return PHONE_EXAMPLE_LEIS.has(lei) ? "" : "hidden md:block";
}
