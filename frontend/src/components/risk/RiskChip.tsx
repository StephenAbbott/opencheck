import { useState } from "react";
import type { RiskSignal } from "../../lib/api";

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

export const CONFIDENCE_DOT: Record<string, string> = {
  high: "●",
  medium: "◐",
  low: "○",
};

export function rank(confidence: string): number {
  return confidence === "high" ? 3 : confidence === "medium" ? 2 : 1;
}

export function RiskChip({
  signal,
  compact = false,
  interactive = true,
}: {
  signal: RiskSignal;
  compact?: boolean;
  interactive?: boolean;
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
  const title = `${signal.summary}\n\nSource: ${signal.source_id}/${signal.hit_id}\nConfidence: ${signal.confidence}`;
  const chipContent = (
    <>
      <span aria-hidden className="text-[10px]">{CONFIDENCE_DOT[signal.confidence] ?? "•"}</span>
      <span className="sr-only">{signal.confidence} confidence</span>
      <span>{presentation.label}</span>
    </>
  );

  if (!interactive) {
    return (
      <span
        title={title}
        className={`inline-flex items-center gap-1.5 border rounded-full shadow-sm ${padding} ${presentation.classes}`}
      >
        {chipContent}
        <span className="sr-only">
          {signal.summary}
          {signal.source_id ? ` Source: ${signal.source_id}.` : ""}
        </span>
      </span>
    );
  }

  return (
    <>
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        title={title}
        className={`inline-flex items-center gap-1.5 border rounded-full shadow-sm ${padding} ${presentation.classes}`}
      >
        {chipContent}
      </button>
      {open && (
        <span className="basis-full text-left text-[12px] text-oo-ink bg-white border border-oo-rule rounded px-2.5 py-1.5 leading-[1.5]">
          {signal.summary}
          {signal.source_id ? <> · Source: {signal.source_id}</> : null}
          {" · "}
          {signal.confidence} confidence
        </span>
      )}
    </>
  );
}
