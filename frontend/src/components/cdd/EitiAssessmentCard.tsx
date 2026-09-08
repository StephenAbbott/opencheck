/**
 * EitiAssessmentCard — the EITI Company Assessment, on the Climate & ESG tab.
 *
 * Two blocks, in the order a reader needs them:
 *
 * 1. **Beneficial ownership disclosure.** EITI's expectation 6 result, as a
 *    chip, with the sentence that makes the advocacy point: where a company
 *    does disclose, what it published is a *document*.
 * 2. **The declared subsidiaries — as a count and a pointer.** The list of
 *    names the company typed into an EITI form, and its cross-reference
 *    against the other lists OpenCheck holds, moved to the Subsidiaries tab
 *    in Phase 185: this card had fetched the GLEIF network a second time to
 *    compare against it, and the comparison the tab runs now covers MEIP and
 *    GEM as well. The count and expectation-2 result stay here, because they
 *    are the assessment; the list is a subsidiaries question.
 *
 * ## What this card must never do
 *
 * **Turn an absent assessment into an adverse one.** "Not available" means EITI
 * did not assess the company, and it carries a `neutral` tone. The mapping is in
 * `lib/eitiAssessment.ts` and pinned by its test.
 *
 * **Imply the names are matched companies.** None of these names is in the
 * BODS graph and none of them is asserted as an identifier — see the adapter
 * and mapper docstrings, and `lib/subsidiariesMode.ts` for how the tab offers
 * a name match without asserting one.
 */
import { useState } from "react";

import type { SourceHit } from "../../lib/api";
import {
  assessmentYears,
  boDisclosure,
  disclosureLink,
  disclosureSentence,
  expectation,
  latestAssessmentYear,
  methodBasis,
  methodMatchLevel,
  SUBSIDIARY_EXPECTATION,
  type EitiAssessmentBundle,
} from "../../lib/eitiAssessment";
import type { PanelError, PanelId } from "../../lib/panelErrors";
import { subsidiaryHref } from "../../lib/subsidiariesMode";
import { compareKey, EITI_DECLARED_CAPTION, NOT_IN_GRAPH } from "../../lib/vocab";
import { Button } from "../ui/Button";
import { Chip } from "../ui/Chip";
import MatchConfidenceChip from "../ui/MatchConfidenceChip";
import { ESG_SOURCE_META } from "./esgSources";

function Eyebrow({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mb-1.5">
      {children}
    </div>
  );
}

export function EitiAssessmentCard({
  hit,
  onOpenSubsidiaries,
}: {
  hit: SourceHit;
  /** Switch to the Subsidiaries tab in place; the pointer is a real link so
   *  it survives a right-click. */
  onOpenSubsidiaries?: () => void;
  /** Kept on the signature so EsgPanel's call site is unchanged; the fetch
   *  that used them moved to the Subsidiaries tab in Phase 185. */
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

  const [showNote, setShowNote] = useState(false);

  const lei = (raw?.lei ?? "").trim();

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
          Block 2 — the declared list, as a count and a pointer
          --------------------------------------------------------------- */}
      <div className="px-5 py-4">
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
            {/* Visible, always, for as long as the count is. */}
            <p
              data-testid="eiti-declared-caption"
              className="mt-2 text-oo-small leading-relaxed text-emerald-800"
            >
              {EITI_DECLARED_CAPTION}
            </p>
            <p className="mt-2 text-oo-small leading-relaxed text-emerald-800">
              The names, and how they compare with the other lists OpenCheck holds, are in the{" "}
              <a
                href={subsidiaryHref(lei)}
                onClick={(e) => {
                  if (!onOpenSubsidiaries) return;
                  e.preventDefault();
                  onOpenSubsidiaries();
                }}
                className="font-semibold underline underline-offset-2 hover:text-emerald-950"
                data-testid="eiti-subsidiaries-link"
              >
                Subsidiaries tab
              </a>
              .
            </p>
          </>
        ) : (
          <p className="text-oo-small leading-relaxed text-emerald-800">
            {exp2?.result
              ? `EITI recorded "${exp2.result}" against expectation 2 (publish a list of controlled subsidiaries)${year ? ` for ${year}` : ""}, and carries no list for this company in its data.`
              : "EITI carries no subsidiary list for this company."}
          </p>
        )}
        <p className="mt-3 text-oo-meta text-emerald-800">
          EITI Company Assessment ·{" "}
          {assessmentYears(raw).join(", ") || "no assessment year recorded"} ·
          EITI International Secretariat, eiti.org
        </p>
      </div>
    </div>
  );
}
