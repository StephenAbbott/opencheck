/**
 * ConfidenceLegend — what ●, ◐ and ○ mean, on screen (Phase 124).
 *
 * The three glyphs appear on risk chips, citation chips and the verdict strip.
 * Their meaning was written down in exactly one place — `CONFIDENCE_LABEL` in
 * `Chip.tsx` — and that string was only ever emitted `sr-only`. A sighted user
 * had no way to learn that ◐ means one source said this and ● means two or
 * more, which is the difference between a lead and a corroborated finding.
 *
 * It renders beside the chips it explains rather than in a page-level key,
 * because a legend the reader has to go and find is a legend they will not read.
 */

import { CONFIDENCE_GLYPH, CONFIDENCE_LABEL } from "./Chip";

/** The levels, strongest first — the order the glyphs rank in. */
export const CONFIDENCE_ORDER = ["high", "medium", "low"] as const;

export default function ConfidenceLegend({ className = "" }: { className?: string }) {
  return (
    <dl className={`flex flex-wrap items-center gap-x-3 gap-y-0.5 text-oo-meta text-oo-muted ${className}`}>
      {CONFIDENCE_ORDER.map((level) => (
        <div key={level} className="flex items-center gap-1">
          <dt aria-hidden="true">{CONFIDENCE_GLYPH[level]}</dt>
          <dd>{CONFIDENCE_LABEL[level]}</dd>
        </div>
      ))}
    </dl>
  );
}
