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
  OFFSHORE_LEAKS: {
    label: "Offshore leaks",
    classes: "bg-amber-50 text-amber-800 border-amber-200",
  },
  OPAQUE_OWNERSHIP: {
    label: "Opaque ownership",
    classes: "bg-slate-100 text-slate-700 border-slate-300",
  },
  TRUST_OR_ARRANGEMENT: {
    label: "Trust / arrangement",
    classes: "bg-indigo-50 text-indigo-700 border-indigo-200",
  },
  NON_EU_JURISDICTION: {
    label: "Non-EU jurisdiction",
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
  FATF_BLACK_LIST: {
    label: "FATF black list",
    classes: "bg-red-100 text-red-800 border-red-400 font-semibold",
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
}: {
  signal: RiskSignal;
  compact?: boolean;
}) {
  const presentation =
    RISK_PRESENTATION[signal.code] ?? {
      label: signal.code,
      classes: "bg-slate-100 text-slate-700 border-slate-200",
    };
  const padding = compact
    ? "px-2 py-0.5 text-[12px] font-medium"
    : "px-3 py-1 text-[13px] font-semibold";
  return (
    <span
      title={`${signal.summary}\n\nSource: ${signal.source_id}/${signal.hit_id}\nConfidence: ${signal.confidence}`}
      className={`inline-flex items-center gap-1.5 border rounded-full shadow-sm ${padding} ${presentation.classes}`}
    >
      <span aria-hidden className="text-[10px]">{CONFIDENCE_DOT[signal.confidence] ?? "•"}</span>
      <span>{presentation.label}</span>
    </span>
  );
}
