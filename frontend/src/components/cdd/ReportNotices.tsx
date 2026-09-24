import type { DegradedSource } from "../../lib/api";
import { panelLabel, type PanelError } from "../../lib/panelErrors";
import { RISK_PRESENTATION } from "../risk/RiskChip";
import { shortSourceName } from "./CrossSourceIdentifiersTable";

/*
 * The two "part of this report did not run" notices that sit above the mode
 * panels. Moved out of App.tsx in Phase 246, unchanged.
 */

/**
 * Failures in the panels that fetch outside the lookup pipeline.
 *
 * Separate from DegradedScreensNotice on purpose — see lib/panelErrors.ts for
 * why these must not be merged into `degraded_sources`. Same amber, same
 * closing principle, different sentence: a section that is not on screen
 * because its fetch failed must not be read as a section with nothing in it.
 */
export function PanelErrorsNotice({ errors }: { errors: PanelError[] }) {
  if (errors.length === 0) return null;
  return (
    <section
      role="status"
      aria-label="Part of this report could not be loaded"
      className="mt-6 mb-8 rounded-oo border border-oo-warn-border bg-oo-warn-bg p-5"
    >
      <h2 className="font-head font-bold text-oo-body text-oo-warn-text">
        Part of this report could not be loaded
      </h2>
      <ul className="mt-2 space-y-1.5">
        {errors.map((e) => (
          <li key={e.panel} className="text-oo-meta text-oo-warn-text leading-[1.6]">
            <span className="font-semibold">{panelLabel(e.panel)}</span> — {e.detail}. You
            are not seeing {e.missing}.
          </li>
        ))}
      </ul>
      <p className="mt-2 text-oo-meta text-oo-warn-text">
        These sections are missing from the page, not empty. Their absence is not
        evidence of absence.
      </p>
    </section>
  );
}

/** Human phrasing for the closed degradation-reason vocabulary. */
const DEGRADED_REASON_LABELS: Record<string, string> = {
  upstream_error: "the upstream service errored",
  timeout: "the upstream service timed out",
  not_configured: "the required API credential is not configured",
  rate_limited: "the upstream service rate-limited the request",
};

/**
 * Warning box for degraded upstream screens (issue #50). Sits above the
 * risk panel and renders whenever the backend reports that a derived
 * check (related-party sanctions/PEP screening, ICIJ offshore-leaks
 * reconciliation) did not fully run — including when there are zero risk
 * signals, which is precisely the case that must not pass for a clean
 * screen. Details are counts only; the backend never sends the
 * related-party names that were being screened.
 */
export function DegradedScreensNotice({
  degraded,
  sourceNames = {},
  onRetry,
}: {
  degraded: DegradedSource[];
  sourceNames?: Record<string, string>;
  /** Re-runs the lookup bypassing the replay cache; absent while streaming. */
  onRetry?: () => void;
}) {
  if (degraded.length === 0) return null;
  return (
    <section
      id="screening-incomplete"
      role="status"
      aria-label="Screening incomplete"
      className="scroll-mt-4 mt-6 mb-8 rounded-oo border border-amber-300 bg-amber-50 p-5"
    >
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div className="flex items-start gap-3 min-w-0">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.75"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mt-0.5 h-4 w-4 shrink-0 text-amber-600"
            aria-hidden="true"
          >
            <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z" />
            <path d="M12 9v4" />
            <path d="M12 17h.01" />
          </svg>
          <div className="min-w-0">
            <p className="font-head font-bold text-oo-body text-amber-900">
              Screening incomplete — {degraded.length} check
              {degraded.length === 1 ? "" : "s"} did not fully run
            </p>
            <ul className="mt-2 space-y-1.5 text-[12.5px] leading-[1.6] text-amber-900">
              {degraded.map((d, i) => (
                <li key={`${d.source_id}:${d.check}:${i}`}>
                  <span className="font-semibold">
                    {d.source_id === "opencheck"
                      ? "OpenCheck"
                      : shortSourceName(d.source_id, sourceNames)}
                  </span>{" "}
                  — {d.detail}{" "}
                  <span className="text-amber-800">
                    ({DEGRADED_REASON_LABELS[d.reason] ?? d.reason})
                  </span>
                  {d.affected_signals.length > 0 && (
                    <span className="ml-1.5 inline-flex flex-wrap gap-1 align-middle">
                      {d.affected_signals.map((code) => (
                        <span
                          key={code}
                          className="text-[10px] font-semibold uppercase tracking-wide bg-white/70 border border-amber-300 rounded px-1.5 py-0.5 text-amber-900"
                        >
                          {RISK_PRESENTATION[code]?.label ??
                            code.replace(/_/g, " ")}
                        </span>
                      ))}
                    </span>
                  )}
                </li>
              ))}
            </ul>
            <p className="mt-2 text-oo-meta text-amber-800">
              The absence of the signals above is not evidence of absence —
              an empty result here is not a clean screen.
            </p>
          </div>
        </div>
        {onRetry && (
          <button
            type="button"
            onClick={onRetry}
            className="shrink-0 rounded border border-amber-400 px-3 py-1.5 text-oo-meta font-semibold text-amber-900 transition-colors hover:bg-amber-100"
          >
            Re-run screening
          </button>
        )}
      </div>
    </section>
  );
}
