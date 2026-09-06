/**
 * EitiAssessmentCard — the EITI Company Assessment, on the Climate & ESG tab.
 *
 * Three blocks, in the order a reader needs them:
 *
 * 1. **Beneficial ownership disclosure.** EITI's expectation 6 result, as a
 *    chip, with the sentence that makes the advocacy point: where a company
 *    does disclose, what it published is a *document*.
 * 2. **The declared subsidiaries.** A list of names the company typed into an
 *    EITI form. The caption saying so is visible, permanently, above the list —
 *    never a `title` attribute — because an uncaptioned list of 156 company
 *    names reads as 156 companies OpenCheck has identified.
 * 3. **A cross-reference** against lists OpenCheck already holds. Today that is
 *    the GLEIF Level 2 children; B4 adds OECD-UNSD MEIP when its BODS release
 *    lands, which is why `crossReference` takes N lists and this card renders
 *    one summary per list rather than a single hard-wired comparison.
 *
 * ## What this card must never do
 *
 * **Turn an absent assessment into an adverse one.** "Not available" means EITI
 * did not assess the company, and it carries a `neutral` tone. The mapping is in
 * `lib/eitiAssessment.ts` and pinned by its test.
 *
 * **Imply the names are matched companies.** They are compared on name only,
 * the chip says so, and a name in both lists is a *comparison* result, not an
 * identification. None of these names is in the BODS graph and none of them is
 * asserted as an identifier — see the adapter and mapper docstrings.
 *
 * **Report a fetch failure as a finding.** If `/subsidiaries` could not be
 * asked, every row would otherwise render as "declared to EITI only" — a
 * transport failure printed as a discrepancy. The comparison is suppressed and
 * reported through `lib/panelErrors.ts` instead (Phase 124 honest-failure rule).
 */
import { useEffect, useRef, useState } from "react";

import { getSubsidiaries } from "../../lib/api";
import type { SourceHit } from "../../lib/api";
import {
  assessmentYears,
  boDisclosure,
  crossReference,
  disclosureLink,
  disclosureSentence,
  expectation,
  latestAssessmentYear,
  methodBasis,
  methodMatchLevel,
  SUBSIDIARY_EXPECTATION,
  type ComparisonList,
  type CrossReference,
  type EitiAssessmentBundle,
} from "../../lib/eitiAssessment";
import {
  panelError,
  subsidiariesPartial,
  subsidiariesUnavailable,
  type PanelError,
  type PanelId,
} from "../../lib/panelErrors";
import {
  compareKey,
  crossListDirection,
  crossListSummary,
  EITI_DECLARED_CAPTION,
  NAME_ONLY_COMPARISON,
  NOT_IN_GRAPH,
} from "../../lib/vocab";
import { Button } from "../ui/Button";
import { Chip } from "../ui/Chip";
import MatchConfidenceChip from "../ui/MatchConfidenceChip";
import { ESG_SOURCE_META } from "./esgSources";

/** Rows shown before the list collapses behind a control. */
const VISIBLE_ROWS = 12;

/** What GLEIF Level 2 actually measures. Not control — accounting
 *  consolidation, which is why its list and EITI's differ in both directions
 *  and neither is the complete one. */
const GLEIF_MEASURES =
  "GLEIF Level 2 is the accounting consolidation a company reports for the " +
  "whole group, while EITI's list covers extractive operations in " +
  "implementing countries";

function Eyebrow({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mb-1.5">
      {children}
    </div>
  );
}

export function EitiAssessmentCard({
  hit,
  onPanelError,
  onRecovered,
}: {
  hit: SourceHit;
  /** Report a `/subsidiaries` failure to the report-level notice — this card
   *  sits outside the lookup pipeline, so nothing else knows it failed. */
  onPanelError?: (e: PanelError) => void;
  onRecovered?: (panel: PanelId) => void;
}) {
  const raw = hit.raw as unknown as EitiAssessmentBundle;
  const meta = ESG_SOURCE_META.eiti_assessment;
  const year = latestAssessmentYear(raw);
  const bo = boDisclosure(raw);
  const link = disclosureLink(raw);
  const sentence = disclosureSentence(raw);
  const subs = raw?.subsidiaries ?? [];
  const exp2 = expectation(raw, SUBSIDIARY_EXPECTATION, year);
  const matchedName = (raw?.match?.gleif_legal_name ?? "").trim();
  const eitiName = (raw?.name ?? "").trim();
  const namesDiffer =
    !!matchedName && compareKey(matchedName) !== compareKey(eitiName);

  const [showAll, setShowAll] = useState(false);
  const [showNote, setShowNote] = useState(false);

  // --- the comparison lists -------------------------------------------------
  // Only GLEIF today. `lists` is an array so B4 appends MEIP rather than
  // rewriting this block and everything that reads it.
  const [gleif, setGleif] = useState<ComparisonList | null>(null);
  const [comparing, setComparing] = useState(false);
  const [compareFailed, setCompareFailed] = useState(false);
  const asked = useRef(false);

  const lei = (raw?.lei ?? "").trim();

  useEffect(() => {
    // Nothing to compare against an empty list, and no reason to spend a GLEIF
    // call on it.
    if (!lei || subs.length === 0 || asked.current) return;
    asked.current = true;
    let live = true;
    setComparing(true);
    getSubsidiaries(lei, "summary")
      .then((res) => {
        if (!live) return;
        if (res.children_available === false) {
          // Phase 146: a degraded 200. No list was returned, so there is
          // nothing to compare and saying "EITI only" would be false.
          setCompareFailed(true);
          onPanelError?.(subsidiariesUnavailable(res.degraded_detail));
          return;
        }
        if (!res.direct_available || !res.ultimate_available) {
          // Half a network is still worth comparing, but a reader must not
          // read the misses as absences.
          onPanelError?.(subsidiariesPartial(res.degraded_detail));
        } else {
          onRecovered?.("subsidiaries");
        }
        const names = (res.children ?? [])
          .map((c) => (c.name ?? "").trim())
          .filter(Boolean);
        const leiByKey: Record<string, string> = {};
        for (const child of res.children ?? []) {
          const key = compareKey(child.name ?? "");
          if (key && child.lei && !leiByKey[key]) leiByKey[key] = child.lei;
        }
        setGleif({
          id: "gleif",
          label: "GLEIF Level 2",
          measures: GLEIF_MEASURES,
          names,
          leiByKey,
          available: true,
        });
      })
      .catch((e) => {
        if (!live) return;
        setCompareFailed(true);
        onPanelError?.(panelError("subsidiaries", e));
      })
      .finally(() => {
        if (live) setComparing(false);
      });
    return () => {
      live = false;
    };
  }, [lei, subs.length, onPanelError, onRecovered]);

  const lists: ComparisonList[] = gleif ? [gleif] : [];
  const xref: CrossReference = crossReference(subs, lists);
  const rows = showAll ? xref.rows : xref.rows.slice(0, VISIBLE_ROWS);

  return (
    <div className="rounded-oo border border-emerald-200 bg-emerald-50/40 overflow-hidden">
      <div className="px-5 pt-4 pb-4 border-b border-emerald-200/60">
        <div className="mb-2 -mt-0.5 flex items-center gap-1.5">
          <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-500" />
          <span className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-emerald-800">
            Data from{" "}
            <a
              href={meta.href}
              target="_blank"
              rel="noreferrer"
              className="underline underline-offset-2 hover:text-emerald-900"
            >
              {meta.org}
              <span className="sr-only"> (opens in new tab)</span>
            </a>{" "}
            · {meta.licence}
          </span>
        </div>

        <h3 className="font-head font-bold text-oo-lead text-emerald-950 leading-snug">
          {eitiName || hit.name}
        </h3>
        <div className="text-oo-meta font-mono text-emerald-800 mt-0.5">
          {[raw?.hq_country, ...(raw?.sectors ?? []), raw?.company_type]
            .filter(Boolean)
            .join(" · ")}
        </div>

        {/* The LEI this record is keyed on, and the name that LEI belongs to.
            Both are shown whenever they differ: Chevron Corporation, Ma'aden
            and MMG Limited have no LEI of their own and are anchored on the
            only LEI-bearing entity in the group, so a card that printed one
            name silently as the other would misstate which legal entity this
            is about. */}
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <span className="text-oo-meta font-mono text-emerald-800">{lei}</span>
          <MatchConfidenceChip
            confidence={methodMatchLevel(raw?.match?.method)}
            basis={methodBasis(raw?.match?.method)}
          />
        </div>
        {namesDiffer && (
          <p className="mt-2 text-oo-small text-emerald-800">
            EITI names this supporting company{" "}
            <span className="font-semibold">{eitiName}</span>. That LEI belongs
            to <span className="font-semibold">{matchedName}</span> — where the
            two differ, the EITI record describes the corporate group and the
            LEI identifies one legal entity within it.
          </p>
        )}
      </div>

      {/* ---------------------------------------------------------------
          Block 1 — beneficial ownership disclosure
          --------------------------------------------------------------- */}
      {bo && (
        <div className="px-5 py-4 border-b border-emerald-200/60">
          <Eyebrow>
            Beneficial ownership disclosure · EITI expectation 6 · {bo.year}
          </Eyebrow>
          <Chip tone={bo.tone} size="md">
            {bo.label}
          </Chip>
          {sentence && (
            <p className="mt-2 text-oo-small leading-relaxed text-emerald-900">
              {sentence}
            </p>
          )}
          {link && (
            <p className="mt-2 text-oo-small">
              <a
                href={link.url}
                target="_blank"
                rel="noreferrer"
                className="text-emerald-900 underline underline-offset-2 hover:text-emerald-950"
              >
                Beneficial ownership disclosure ({link.year})
                <span className="sr-only"> (opens in new tab)</span>
              </a>
            </p>
          )}
          {bo.comment && (
            <div className="mt-2">
              <Button
                variant="ghost"
                size="sm"
                aria-expanded={showNote}
                onClick={() => setShowNote((v) => !v)}
              >
                {showNote ? "Hide EITI's assessment note" : "EITI's assessment note"}
              </Button>
              {showNote && (
                <p className="mt-2 text-oo-small leading-relaxed text-emerald-900 whitespace-pre-line">
                  {bo.comment}
                </p>
              )}
            </div>
          )}
        </div>
      )}

      {/* ---------------------------------------------------------------
          Block 2 — what the declared list is
          --------------------------------------------------------------- */}
      <div className="px-5 py-4 border-b border-emerald-200/60">
        <Eyebrow>Declared controlled subsidiaries · EITI expectation 2</Eyebrow>
        {subs.length > 0 ? (
          <>
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-head font-bold text-oo-stat text-emerald-800 tabular-nums">
                {subs.length.toLocaleString()}
              </span>
              <span className="text-oo-small font-semibold text-emerald-700">
                names declared{year ? ` · ${year} assessment` : ""}
              </span>
              <Chip tone="context">{NOT_IN_GRAPH}</Chip>
            </div>
            {/* Visible, always, for as long as the list is. */}
            <p
              data-testid="eiti-declared-caption"
              className="mt-2 text-oo-small leading-relaxed text-emerald-800"
            >
              {EITI_DECLARED_CAPTION}
            </p>
          </>
        ) : (
          <p className="text-oo-small leading-relaxed text-emerald-800">
            {exp2?.result
              ? `EITI recorded "${exp2.result}" against expectation 2 (publish a list of controlled subsidiaries)${year ? ` for ${year}` : ""}, and carries no list for this company in its data.`
              : "EITI carries no subsidiary list for this company."}
          </p>
        )}
      </div>

      {/* ---------------------------------------------------------------
          Block 3 — cross-reference, then the list itself
          --------------------------------------------------------------- */}
      {subs.length > 0 && (
        <div className="px-5 py-4">
          <Eyebrow>Cross-referenced against what OpenCheck already holds</Eyebrow>

          {comparing && (
            <p className="text-oo-small text-emerald-800 italic">
              Comparing against the GLEIF subsidiary network…
            </p>
          )}

          {compareFailed && (
            <p
              role="status"
              className="text-oo-small leading-relaxed text-oo-warn-text bg-oo-warn-bg border border-oo-warn-border rounded-oo px-3 py-2"
            >
              The GLEIF subsidiary network could not be fetched, so no
              comparison was run. The names below are EITI's list on its own —
              not a statement that GLEIF holds none of them.
            </p>
          )}

          {xref.lists
            .filter((l) => l.available)
            .map((l) => (
              <div key={l.id} className="mb-3">
                <p className="text-oo-body font-semibold text-emerald-900">
                  {crossListSummary(xref.declared, l.label, l.overlap, l.eitiOnly)}
                </p>
                <p className="mt-1 text-oo-small leading-relaxed text-emerald-800">
                  {crossListDirection({
                    declared: xref.declared,
                    label: l.label,
                    listed: l.listed,
                    overlap: l.overlap,
                    onlyInList: l.onlyInList,
                    eitiOnly: l.eitiOnly,
                    measures: l.measures,
                  })}
                </p>
                <p className="mt-1 text-oo-small leading-relaxed text-emerald-800">
                  {NAME_ONLY_COMPARISON}
                </p>
              </div>
            ))}

          <ul className="space-y-1.5" data-testid="eiti-declared-list">
            {rows.map((row, i) => (
              <li
                key={`${row.name}-${i}`}
                className="flex flex-wrap items-center gap-2 border-b border-emerald-200/40 pb-1.5 last:border-b-0"
              >
                <span className="text-oo-small text-emerald-900">{row.name}</span>
                {row.country && <Chip tone="neutral">{row.country}</Chip>}
                {row.alsoIn.length > 0 ? (
                  <>
                    <Chip tone="ok">
                      Also in{" "}
                      {row.alsoIn
                        .map((id) => xref.lists.find((l) => l.id === id)?.label ?? id)
                        .join(" · ")}
                    </Chip>
                    <MatchConfidenceChip confidence="low" basis="name only" />
                  </>
                ) : (
                  xref.lists.some((l) => l.available) && (
                    <Chip tone="neutral">Declared to EITI only</Chip>
                  )
                )}
              </li>
            ))}
          </ul>

          {xref.rows.length > VISIBLE_ROWS && (
            <Button
              variant="ghost"
              size="sm"
              className="mt-2"
              aria-expanded={showAll}
              onClick={() => setShowAll((v) => !v)}
            >
              {showAll
                ? `Show the first ${VISIBLE_ROWS}`
                : `Show all ${xref.rows.length.toLocaleString()} declared names`}
            </Button>
          )}

          <p className="mt-3 text-oo-meta text-emerald-800">
            EITI Company Assessment ·{" "}
            {assessmentYears(raw).join(", ") || "no assessment year recorded"} ·
            EITI International Secretariat, eiti.org
          </p>
        </div>
      )}
    </div>
  );
}
