import { useState } from "react";
import { CONFIDENCE_GLYPH, CONFIDENCE_LABEL } from "../ui/Chip";
import { SignalEvidence } from "./SignalEvidence";
import type { RiskSignal } from "../../lib/api";
import type { LeadSignal } from "../../lib/leadSignal";
import { sourceLabel } from "../../lib/vocab";

/**
 * Map a risk signal code to a colour palette + short display label.
 * Codes are stable strings from the backend ``opencheck.risk`` module.
 */
export const RISK_PRESENTATION: Record<
  string,
  { label: string; classes: string }
> = {
  PEP: {
    label: "PEP",
    classes: "bg-violet-50 text-violet-700 border-violet-200",
  },
  SANCTIONED: {
    label: "Sanctioned",
    classes: "bg-rose-50 text-rose-700 border-rose-200",
  },
  // Slate, not rose and not amber. OpenSanctions' own label for the
  // `sanction.counter` topic, and deliberately outside the sanctions colour
  // ramp: a Russian MFA retaliation listing is a fact about the listing
  // regime as much as about the person, and rendering it red made a
  // counter-designation indistinguishable from an OFAC one.
  COUNTER_SANCTIONED: {
    label: "Counter-sanctioned",
    classes: "bg-slate-100 text-slate-700 border-slate-300",
  },
  SANCTIONED_SECURITY: {
    label: "Sanctioned securities",
    classes: "bg-rose-50 text-rose-700 border-rose-300 font-semibold",
  },
  // Label mirrors OpenSanctions' own display name for the `sanction.control`
  // topic, so a user cross-checking a record on opensanctions.org sees the
  // same words in both tools.
  SANCTIONS_CONTROLLED: {
    label: "Sanction ownership or control",
    classes: "bg-rose-100 text-rose-800 border-rose-400 font-semibold",
  },
  SANCTIONS_LINKED: {
    label: "Sanctions-linked",
    classes: "bg-amber-50 text-amber-800 border-amber-300",
  },
  DEBARMENT: {
    label: "Debarred",
    classes: "bg-orange-100 text-orange-900 border-orange-400 font-semibold",
  },
  // Export-control family (Phase 118). Labels mirror OpenSanctions' own
  // display names for the topics ("Export controlled", "Export
  // control-linked", "Trade risk") — same cross-check rationale as
  // SANCTIONS_CONTROLLED above. No suppression within the family: upstream
  // declares no superset relationship among the export topics.
  EXPORT_CONTROLLED: {
    label: "Export controlled",
    classes: "bg-rose-100 text-rose-800 border-rose-400 font-semibold",
  },
  EXPORT_CONTROL_LINKED: {
    label: "Export control-linked",
    classes: "bg-amber-50 text-amber-800 border-amber-300",
  },
  EXPORT_RISK: {
    label: "Trade risk",
    classes: "bg-orange-50 text-orange-800 border-orange-300",
  },
  OFFSHORE_LEAKS: {
    label: "Offshore leaks",
    classes: "bg-amber-50 text-amber-800 border-amber-200",
  },
  OPAQUE_OWNERSHIP: {
    label: "Opaque ownership",
    classes: "bg-slate-100 text-slate-700 border-slate-300",
  },
  // Context, not risk (kind="context") — a *permitted* GLEIF Level 2
  // reporting exception (NATURAL_PERSONS, NO_KNOWN_PERSON, NON_CONSOLIDATING,
  // NO_LEI). Same quiet slate family as NON_EU_JURISDICTION: it must read as
  // structural information, never as a warning. The NON_PUBLIC family of
  // exceptions fires OPAQUE_OWNERSHIP instead.
  GLEIF_REPORTING_EXCEPTION: {
    label: "No parent in GLEIF (exempt)",
    classes: "bg-slate-50 text-slate-700 border-slate-200",
  },
  TRUST_OR_ARRANGEMENT: {
    label: "Trust / arrangement",
    classes: "bg-indigo-50 text-indigo-700 border-indigo-200",
  },
  // Context, not risk (kind="context"). Deliberately slate rather than
  // orange: it must not read as a warning, and it must not share a palette
  // with STATE_CONTROLLED, which is a different kind of claim entirely.
  NON_EU_JURISDICTION: {
    label: "Outside EU/EEA",
    classes: "bg-slate-50 text-slate-700 border-slate-200",
  },
  STATE_CONTROLLED: {
    label: "State-controlled",
    classes: "bg-orange-50 text-orange-700 border-orange-200",
  },
  NOMINEE: {
    label: "Nominee",
    classes: "bg-fuchsia-50 text-fuchsia-700 border-fuchsia-200",
  },
  COMPLEX_OWNERSHIP_LAYERS: {
    label: "≥3 layers",
    classes: "bg-sky-50 text-sky-700 border-sky-200",
  },
  COMPLEX_CORPORATE_STRUCTURE: {
    label: "Complex corporate structure (AMLA)",
    classes: "bg-red-50 text-red-700 border-red-300 font-semibold",
  },
  POSSIBLE_OBFUSCATION: {
    label: "Possible obfuscation (advisory)",
    classes: "bg-yellow-50 text-yellow-800 border-yellow-300",
  },
  RELATED_PEP: {
    label: "Related PEP",
    classes: "bg-violet-50 text-violet-700 border-violet-300",
  },
  RELATED_SANCTIONED: {
    label: "Related sanctioned",
    classes: "bg-rose-50 text-rose-700 border-rose-300 font-semibold",
  },
  RELATED_COUNTER_SANCTIONED: {
    label: "Related counter-sanctioned",
    classes: "bg-slate-100 text-slate-700 border-slate-300",
  },
  RELATED_SANCTIONS_CONTROLLED: {
    label: "Related sanction ownership or control",
    classes: "bg-rose-100 text-rose-800 border-rose-400 font-semibold",
  },
  RELATED_SANCTIONS_LINKED: {
    label: "Related sanctions-linked",
    classes: "bg-amber-50 text-amber-800 border-amber-300",
  },
  RELATED_DEBARMENT: {
    label: "Related debarred",
    classes: "bg-orange-50 text-orange-800 border-orange-300",
  },
  RELATED_EXPORT_CONTROLLED: {
    label: "Related export controlled",
    classes: "bg-rose-100 text-rose-800 border-rose-400 font-semibold",
  },
  RELATED_EXPORT_CONTROL_LINKED: {
    label: "Related export control-linked",
    classes: "bg-amber-50 text-amber-800 border-amber-300",
  },
  RELATED_EXPORT_RISK: {
    label: "Related trade risk",
    classes: "bg-orange-50 text-orange-800 border-orange-300",
  },
  FATF_BLACK_LIST: {
    label: "FATF black list",
    classes: "bg-red-100 text-red-800 border-red-400 font-semibold",
  },
  EU_HIGH_RISK_THIRD_COUNTRY: {
    label: "EU high-risk country",
    classes: "bg-red-50 text-red-800 border-red-300 font-semibold",
  },
  FATF_GREY_LIST: {
    label: "FATF grey list",
    classes: "bg-orange-50 text-orange-800 border-orange-400",
  },
};

/** Re-export, not a second copy. Phase 122 introduced CONFIDENCE_GLYPH in
 *  `ui/Chip.tsx` while this map stayed put, so the app had two definitions of
 *  the same three glyphs and only one of them carried the prose meaning. */
export { CONFIDENCE_GLYPH as CONFIDENCE_DOT } from "../ui/Chip";

export function rank(confidence: string): number {
  return confidence === "high" ? 3 : confidence === "medium" ? 2 : 1;
}

/**
 * A chip, and — where nothing else owns the explanation — the box under it.
 *
 * Three modes, in order of preference:
 *
 * - **Selectable** (`onSelect` given). The chip is a control in a set and
 *   renders no box of its own; the section it lives in shows one
 *   `SignalEvidence` for whichever chip is selected. This is the Risk signals
 *   section, where a per-chip box meant the reader could have two boxes open
 *   saying the same kind of sentence in two different styles.
 * - **Self-expanding** (the default). For chips scattered outside a section
 *   that could hold a box — the verdict strip, a source card, the graph
 *   legend. The box is the same `SignalEvidence` component, so an expansion
 *   here and the section's box are one design rather than two.
 * - **Static** (`interactive={false}`). The summary rides in the accessible
 *   name and there is nothing to activate.
 *
 * A self-expanded chip claims **no corroboration**: it knows one signal, and
 * "Reported by one source" would be a claim about the whole check made by a
 * component that has seen one row of it. It names its own source and stops.
 */
export function RiskChip({
  signal,
  compact = false,
  interactive = true,
  selected = false,
  onSelect,
}: {
  signal: RiskSignal;
  compact?: boolean;
  interactive?: boolean;
  /** Selectable mode: whether this chip is the one being explained. */
  selected?: boolean;
  /** Selectable mode: hand the selection to the section that owns the box. */
  onSelect?: (signal: RiskSignal) => void;
}) {
  const [open, setOpen] = useState(false);
  const presentation =
    RISK_PRESENTATION[signal.code] ?? {
      label: signal.code,
      classes: "bg-slate-100 text-slate-700 border-slate-200",
    };
  const padding = compact
    ? "px-2 py-0.5 text-[12px] font-medium"
    : "px-3 py-1 text-[13px] font-semibold";
  // Phase 124: `title` is gone from both branches. It was the only carrier of
  // `signal.summary` on the interactive chip — a screen-reader user who did
  // not activate the button heard the label and nothing else — and it leaked
  // the raw `source_id/hit_id` pair as prose. The summary now rides in the
  // accessible name on both branches, and the expansion names the source
  // properly.
  const described = `${signal.summary}${
    signal.source_id ? ` Source: ${sourceLabel(signal.source_id)}.` : ""
  }`;
  const chipContent = (
    <>
      <span aria-hidden className="text-oo-meta">{CONFIDENCE_GLYPH[signal.confidence] ?? "•"}</span>
      <span className="sr-only">{CONFIDENCE_LABEL[signal.confidence] ?? signal.confidence}: </span>
      <span>{presentation.label}</span>
    </>
  );

  if (!interactive) {
    return (
      <span
        className={`inline-flex items-center gap-1.5 border rounded-full shadow-sm ${padding} ${presentation.classes}`}
      >
        {chipContent}
        <span className="sr-only">{described}</span>
      </span>
    );
  }

  // Selectable: the section owns the box, so the chip is a pressed/unpressed
  // control and nothing more. `aria-pressed` rather than `aria-expanded` —
  // there is no region under *this* control to expand, and announcing one
  // would send a screen-reader user looking for it.
  if (onSelect) {
    return (
      <button
        type="button"
        aria-pressed={selected}
        onClick={() => onSelect(signal)}
        className={`inline-flex items-center gap-1.5 border rounded-full shadow-sm ${padding} ${presentation.classes} ${
          selected ? "ring-2 ring-oo-navy/40 ring-offset-1" : ""
        }`}
      >
        {chipContent}
        <span className="sr-only">{described}</span>
      </button>
    );
  }

  return (
    <>
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        className={`inline-flex items-center gap-1.5 border rounded-full shadow-sm ${padding} ${presentation.classes}`}
      >
        {chipContent}
        {/* The chip's own text is the rule that fired; the summary is why.
            Both belong in the accessible name — activating a control should
            not be the only way to learn what it is about. */}
        <span className="sr-only">{described}</span>
      </button>
      {open && (
        <span className="basis-full">
          <SignalEvidence lead={chipEvidence(signal)} />
        </span>
      )}
    </>
  );
}

/**
 * The `LeadSignal` shape for a chip that has only itself.
 *
 * `sourceCount: 0` on purpose — see the note on the component above. The
 * source still goes in `sourceIds`, so the box names it.
 */
export function chipEvidence(signal: RiskSignal): LeadSignal {
  return {
    signal,
    sourceCount: 0,
    sourceIds: signal.source_id ? [signal.source_id] : [],
    checkedAt: null,
  };
}
