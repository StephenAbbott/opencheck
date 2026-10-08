/**
 * Leads for a retired LEI that names no successor (Phase 308).
 *
 * Rendered in the identity band under the profile rows, only when
 * `leadsEligible(profile)` — the company has ended and GLEIF names no
 * successor — and never on a saved report (`SavedReportContext`): a saved
 * page is the record of that entity on the day it was saved, and a list of
 * today's look-alikes beside it is exactly what Phase 217 forbids. Fetches
 * `GET /leads` once per LEI; a failure is said in words, never as an empty
 * list. The wording lives in `lib/leads.ts`.
 */

import { useEffect, useState } from "react";

import { getLeads, type LeadsResponse } from "../../lib/api";
import {
  CANDIDATES_LABEL,
  LEADS_HEADING,
  LEADS_INTRO,
  OPEN_LABEL,
  PARENT_LABEL,
  UNAVAILABLE_LINES,
  leadsLines,
} from "../../lib/leads";
import { SectionLabel as Eyebrow } from "../ui";
import MatchConfidenceChip from "../ui/MatchConfidenceChip";
import { useSavedReport } from "./savedReportContext";

type State =
  | { kind: "loading" }
  | { kind: "ready"; leads: LeadsResponse }
  | { kind: "failed" };

export function RetiredLeiLeads({ lei, legalName }: { lei: string; legalName: string | null }) {
  const saved = useSavedReport();
  const [state, setState] = useState<State>({ kind: "loading" });

  useEffect(() => {
    if (saved) return;
    let live = true;
    setState({ kind: "loading" });
    getLeads(lei, legalName ?? "")
      .then((leads) => {
        if (live) setState({ kind: "ready", leads });
      })
      .catch(() => {
        if (live) setState({ kind: "failed" });
      });
    return () => {
      live = false;
    };
  }, [lei, legalName, saved]);

  if (saved) return null;

  return (
    <section
      aria-labelledby="retired-lei-leads"
      className="mt-5 rounded-oo border border-oo-softBorder bg-oo-soft/40 px-4 py-3"
    >
      <Eyebrow as="h3" id="retired-lei-leads">
        {LEADS_HEADING}
      </Eyebrow>
      <p className="mt-1.5 text-oo-small text-oo-ink">{LEADS_INTRO}</p>
      {state.kind === "loading" && (
        <p className="mt-2 text-oo-meta text-oo-muted" aria-live="polite">
          Searching GLEIF for a similar name…
        </p>
      )}
      {state.kind === "failed" && (
        <p className="mt-2 text-oo-meta text-oo-muted">{UNAVAILABLE_LINES.unreachable}</p>
      )}
      {state.kind === "ready" && <LeadsBody leads={state.leads} />}
    </section>
  );
}

function LeadsBody({ leads }: { leads: LeadsResponse }) {
  const lines = leadsLines(leads);
  return (
    <dl className="mt-2.5 grid grid-cols-1 gap-y-3">
      <div className="flex flex-col gap-0.5 min-w-0">
        <dt className="font-body text-oo-meta font-bold uppercase tracking-oo-eyebrow text-oo-muted">
          {PARENT_LABEL}
        </dt>
        <dd className="m-0 text-oo-small text-oo-ink break-words">
          {lines.parent}
          {lines.parentHref && (
            <>
              {" "}
              <a href={lines.parentHref} className="font-semibold underline underline-offset-2 whitespace-nowrap">
                {OPEN_LABEL} →
              </a>
            </>
          )}
        </dd>
      </div>
      <div className="flex flex-col gap-0.5 min-w-0">
        <dt className="font-body text-oo-meta font-bold uppercase tracking-oo-eyebrow text-oo-muted">
          {CANDIDATES_LABEL}
        </dt>
        {lines.candidatesNote && (
          <dd className="m-0 text-oo-small text-oo-ink">{lines.candidatesNote}</dd>
        )}
        {lines.candidates.map((c) => (
          <dd key={c.href} className="m-0 flex flex-wrap items-center gap-x-2 gap-y-1 text-oo-small text-oo-ink">
            <span className="break-words">{c.line}</span>
            <MatchConfidenceChip confidence="low" />
            <a href={c.href} className="font-semibold underline underline-offset-2 whitespace-nowrap">
              {OPEN_LABEL} →
            </a>
          </dd>
        ))}
      </div>
    </dl>
  );
}
