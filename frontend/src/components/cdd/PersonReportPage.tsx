/**
 * PersonReportPage — the URL-addressable person report (Phase E).
 *
 * Rendered when `?person=<name>[&person_birth_year=<yyyy>]` is present:
 * runs the on-demand /person-check for that person and renders the same
 * evidence-based result block used inside BackgroundCheck, plus a
 * copyable link. Reached from the "Open person report page" link on a
 * connected-person card (and, tentatively, the homepage person tab).
 *
 * Same evidence discipline as everywhere else: name-based potential
 * matches with scores, risk chips from strong matches only, failed
 * sources surfaced, absence never presented as proof.
 */

import { useEffect, useState } from "react";
import { personCheck, type PersonCheckResponse } from "../../lib/api";
import { CheckResult } from "./BackgroundCheckPanel";

type ReportState =
  | { status: "loading" }
  | { status: "done"; result: PersonCheckResponse; fetchedAt: string }
  | { status: "error"; message: string };

export default function PersonReportPage({
  name,
  birthYear,
  onBack,
}: {
  name: string;
  birthYear?: number;
  onBack: () => void;
}) {
  const [state, setState] = useState<ReportState>({ status: "loading" });
  const [copied, setCopied] = useState(false);

  const run = () => {
    setState({ status: "loading" });
    personCheck(name, birthYear)
      .then((result) =>
        setState({
          status: "done",
          result,
          fetchedAt: new Date().toISOString(),
        })
      )
      .catch((e) =>
        setState({ status: "error", message: (e as Error).message })
      );
  };

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(run, [name, birthYear]);

  useEffect(() => {
    document.title = `${name} — Person report — OpenCheck`;
    return () => {
      document.title = "OpenCheck";
    };
  }, [name]);

  const copyLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Clipboard unavailable — the URL bar still has the link.
    }
  };

  return (
    <section aria-label={`Person report for ${name}`}>
      <button
        type="button"
        onClick={onBack}
        className="text-[12px] text-oo-blue underline hover:no-underline mb-3"
      >
        ← Back
      </button>

      <div className="mb-4 rounded-oo border border-violet-300 bg-violet-50 px-4 py-3">
        <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-violet-700 mb-1">
          Person report · BackgroundCheck
        </p>
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div>
            <h2 className="font-head font-bold text-[20px] text-oo-ink">
              {name}
              {birthYear && (
                <span className="ml-2 font-sans font-normal text-[13px] text-oo-muted">
                  b. {birthYear}
                </span>
              )}
            </h2>
            <p className="text-[12px] text-violet-900/80 leading-[1.6] mt-1">
              Screened by <span className="font-medium">name</span>
              {birthYear ? " and birth year" : ""} across every person-capable
              source. Results are potential matches with their evidence shown —
              never confirmed identities — and a clean screen is not proof of
              absence.
            </p>
          </div>
          <button
            type="button"
            onClick={copyLink}
            className="shrink-0 rounded-oo border border-violet-300 bg-white px-3 py-1.5 text-[12px] font-medium text-violet-800 hover:bg-violet-50"
          >
            {copied ? "Link copied ✓" : "Copy link to this report"}
          </button>
        </div>
      </div>

      {state.status === "loading" && (
        <p className="text-[13px] text-oo-muted italic" aria-live="polite">
          Screening {name} across person-capable sources…
        </p>
      )}
      {state.status === "error" && (
        <div role="alert" className="rounded-oo border border-red-200 bg-red-50 px-4 py-3">
          <p className="text-[13px] text-red-800 mb-1.5">
            Check failed: {state.message}
          </p>
          <button
            type="button"
            onClick={run}
            className="text-[12px] text-red-800 underline hover:no-underline"
          >
            Retry
          </button>
        </div>
      )}
      {state.status === "done" && (
        <div className="rounded-oo border border-violet-200 bg-white overflow-hidden">
          <div className="px-4 py-2.5 flex items-center justify-between gap-2 flex-wrap">
            <p className="text-[12px] text-oo-muted">
              {state.result.matches.length} match
              {state.result.matches.length === 1 ? "" : "es"} across{" "}
              {state.result.sources.length} source
              {state.result.sources.length === 1 ? "" : "s"}
            </p>
            <button
              type="button"
              onClick={run}
              className="text-[12px] text-violet-800 underline hover:no-underline"
            >
              Re-run check
            </button>
          </div>
          <CheckResult result={state.result} fetchedAt={state.fetchedAt} />
        </div>
      )}
    </section>
  );
}
