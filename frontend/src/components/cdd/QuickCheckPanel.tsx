import { useCallback, useMemo } from "react";
import type {
  BodsBreakdown,
  CrossSourceLink,
  OpenAlephScreeningMatch,
  PossiblySameEntity,
  PrimaryListing,
  RiskSignal,
  SavedReport,
  SourceHit,
  SourceLiveness,
  SubjectProfile,
} from "../../lib/api";
import { identityBandContents } from "../../lib/identifierBadge";
import { coverageCopy, type SettledCount } from "../../lib/lookupProgress";
import { scrollBehavior } from "../../lib/motion";
import type { PanelError, PanelId } from "../../lib/panelErrors";
import { evidenceForCode } from "../../lib/signalEvidence";
import { profileRows } from "../../lib/subjectProfile";
import { sourceLabel } from "../../lib/vocab";
import { ExportPanel } from "../export/ExportPanel";
import { RiskChip } from "../risk/RiskChip";
import { SignalEvidence } from "../risk/SignalEvidence";
import { Chip, SectionHeading, SectionLabel as Eyebrow } from "../ui";
import ConfidenceLegend from "../ui/ConfidenceLegend";
import { Explain } from "../ui/Explain";
import PanelSection, { PanelCard } from "../ui/PanelSection";
import { CrossSourceIdentifiersTable } from "./CrossSourceIdentifiersTable";
import { ModeBlurb } from "./ModeTabs";
import { NarrativePanel, type ReportExportPayload } from "./NarrativePanel";
import { OpenAlephArchiveMatches } from "./OpenAlephArchiveMatches";
import { PossiblySameTable } from "./PossiblySameTable";
import { SecuritiesSection } from "./SecuritiesSection";
import { SkeletonSourceCard, SourceBucketCard, type SourceBucket } from "./SourceBucketCard";

/**
 * The QuickCheck tab — the subject screened on its own: the summary, the risk
 * signals, "Is this the right company?", what each source said, securities
 * and the export panel.
 *
 * Moved out of App.tsx in Phase 246 and loaded lazily, like the other five
 * modes. Its JSX is unchanged; the memos that only it read (the identity
 * band's profile rows, GLEIF's mapped identifiers and children count, the
 * per-hit risk index, the selected signal's evidence) moved with it. The
 * state stays in App — the selected chip and the identity band's open state
 * are reset by a new lookup, and a lazy panel remounts on every mode switch.
 */
export default function QuickCheckPanel({
  lei,
  legalName,
  jurisdiction,
  streaming,
  sourceNames,
  registryTotal,
  hits,
  riskSignals,
  riskCodes,
  contextCodes,
  sourceLiveness,
  selectedSignalCode,
  onSelectSignal,
  subjectProfile,
  crossSourceLinks,
  possiblySame,
  identityOpen,
  onIdentityToggle,
  cddBuckets,
  esgBuckets,
  pendingCddSources,
  answeredNoRecord,
  applicableCount,
  answeredApplicable,
  settled,
  bodsCountMap,
  bodsBreakdownMap,
  oaScreening,
  primaryListing,
  savedReport,
  savedNarrative,
  onExportPayload,
  onRetrySource,
  retryingSources,
  onPanelError,
  onPanelRecovered,
}: {
  lei: string;
  legalName: string | null;
  jurisdiction: string | null;
  streaming: boolean;
  sourceNames: Record<string, string>;
  registryTotal: number | null;
  hits: SourceHit[];
  riskSignals: RiskSignal[];
  riskCodes: RiskSignal[];
  contextCodes: RiskSignal[];
  sourceLiveness: Record<string, SourceLiveness>;
  selectedSignalCode: string | null;
  onSelectSignal: (code: string | null) => void;
  subjectProfile: SubjectProfile | null;
  crossSourceLinks: CrossSourceLink[];
  possiblySame: PossiblySameEntity[];
  identityOpen: boolean;
  onIdentityToggle: (open: boolean) => void;
  cddBuckets: SourceBucket[];
  esgBuckets: SourceBucket[];
  pendingCddSources: string[];
  answeredNoRecord: string[];
  applicableCount: number;
  answeredApplicable: number;
  settled: SettledCount;
  bodsCountMap: Record<string, number>;
  bodsBreakdownMap: Record<string, BodsBreakdown>;
  oaScreening: OpenAlephScreeningMatch[];
  primaryListing: PrimaryListing | null;
  savedReport: SavedReport | null;
  savedNarrative: ReportExportPayload | null;
  onExportPayload: (payload: ReportExportPayload) => void;
  onRetrySource: (sourceId: string) => void;
  retryingSources: Set<string>;
  onPanelError: (e: PanelError) => void;
  onPanelRecovered: (panel: PanelId) => void;
}) {
  // The identity band's profile rows (Phase 154) — derived once per profile.
  const profileRowsForBand = useMemo(
    () => profileRows(subjectProfile, sourceNames),
    [subjectProfile, sourceNames],
  );

  // Extract GLEIF LEI Mapping identifiers from the GLEIF hit's raw attributes.
  // These are published by the GLEIF LEI Mapping programme (GODIN) and are not
  // surfaced through cross_source_links because they don't require corroboration
  // from a second source — GLEIF is the authoritative bridge.
  const gleifMappedIds = useMemo<{ scheme: string; value: string }[]>(() => {
    const gleifHit = hits.find((h) => h.source_id === "gleif");
    if (!gleifHit) return [];
    const attrs = (gleifHit.raw as Record<string, unknown>) ?? {};
    const result: { scheme: string; value: string }[] = [];
    const ocid = attrs["ocid"];
    if (ocid && typeof ocid === "string")
      result.push({ scheme: "OpenCorporates ID", value: ocid });
    const bic = attrs["bic"];
    if (bic) {
      const bicVal = Array.isArray(bic) ? bic[0] : bic;
      if (typeof bicVal === "string") result.push({ scheme: "BIC (ISO 9362)", value: bicVal });
    }
    const mic = attrs["mic"];
    if (mic) {
      const micVal = Array.isArray(mic) ? mic[0] : mic;
      if (typeof micVal === "string") result.push({ scheme: "MIC (ISO 10383)", value: micVal });
    }
    const spglobal = attrs["spglobal"];
    if (spglobal) {
      const spVal = Array.isArray(spglobal) ? spglobal[0] : spglobal;
      if (typeof spVal === "string") result.push({ scheme: "S&P CIQ Company ID", value: spVal });
    }
    return result;
  }, [hits]);

  // The worst signal, with the corroboration behind it. Derived from the
  // signals the backend already sent — see lib/signalEvidence.ts for the
  // rules that keep the sentence from claiming more than they support.
  // The signal the one evidence box is explaining — the reader's choice, and
  // nothing before they make one.
  //
  // It used to open on the worst signal, picked by a severity ordering. That
  // is OpenCheck grading findings: it put "the most serious signal is shown
  // above" on the page, and it decided which of a company's findings a reader
  // met first. The product's own rule is that a signal is a pointer to a
  // record, not a conclusion about the company, and ranking them is a
  // conclusion. The chips are the menu; the box answers whichever one is
  // asked. `leadSignal` is gone with it.
  //
  // A selection that a re-run no longer produces resolves to null, which is
  // the same state as "nothing selected yet" — not an empty box.
  const shownSignal = useMemo(
    () =>
      selectedSignalCode
        ? evidenceForCode(riskSignals, selectedSignalCode, sourceLiveness)
        : null,
    [selectedSignalCode, riskSignals, sourceLiveness]
  );

  /** Scroll to a source card and flash it — the same affordance narrative
   *  citations and the identifier table already use. */
  const showSourceCard = useCallback((sourceId: string) => {
    const el = document.getElementById(`source-${sourceId}`);
    if (!el) return;
    el.scrollIntoView({ behavior: scrollBehavior(), block: "start" });
    if (el.tabIndex < 0) el.tabIndex = -1;
    el.focus({ preventScroll: true });
    el.classList.add("oc-cite-flash");
    window.setTimeout(() => el.classList.remove("oc-cite-flash"), 1600);
  }, []);

  // Extract GLEIF direct-children counts from the GLEIF hit's raw dict.
  // The adapter fetches only the first page (≤ 10) so we surface both
  // the fetched count and the total reported by GLEIF pagination.
  const gleifChildrenInfo = useMemo<{ fetched: number; total: number } | null>(() => {
    const gleifHit = hits.find((h) => h.source_id === "gleif");
    if (!gleifHit) return null;
    const raw = (gleifHit.raw as Record<string, unknown>) ?? {};
    const total = typeof raw["direct_children_total"] === "number" ? raw["direct_children_total"] : 0;
    const fetched = typeof raw["direct_children_fetched"] === "number" ? raw["direct_children_fetched"] : 0;
    return total > 0 ? { fetched, total } : null;
  }, [hits]);

  // Index risk signals by `${source_id}:${hit_id}` so hit rows can
  // pull their own chips without re-scanning the whole list.
  const riskByHit = useMemo(() => {
    const out: Record<string, RiskSignal[]> = {};
    for (const sig of riskSignals) {
      const k = `${sig.source_id}:${sig.hit_id}`;
      (out[k] = out[k] ?? []).push(sig);
    }
    return out;
  }, [riskSignals]);

  const totalHits = cddBuckets.reduce((n, b) => n + b.hits.length, 0);

  return (
    <PanelCard>
      {/* The mode's own sentence, as the card's first band. The strings have
          been on MODE_TABS since Phase 122 and rendered nowhere: a tab
          labelled "QuickCheck" says what it is called, not what it does, and
          a reader arriving on a shared link has no other way to find out
          which of the four they are looking at. */}
      <ModeBlurb mode="quick" />
      {(
        <NarrativePanel
          lei={lei}
          onExportPayload={onExportPayload}
          saved={savedNarrative}
        />
      )}

      {/* Risk signals, with structural context as a captioned sub-block
          inside it rather than a peer section. Two sibling sections put a
          structural observation at the same weight as an adverse finding,
          and printed the confidence legend twice on one screen. Neither the
          AMLA CDD RTS nor AMLR Annex III treats a non-EU jurisdiction as a
          risk factor in itself, so the distinction still has to be made —
          it is made by the caption, in a sentence, which is what the v2
          design does. */}
      {(riskCodes.length > 0 || contextCodes.length > 0) && (
        <PanelSection
          id="risk-signals"
          title="Risk signals"
          aside={<ConfidenceLegend />}
        >
          {riskCodes.length > 0 ? (
            <>
              <div className="flex flex-wrap gap-2">
                {riskCodes.map((sig) => (
                  <RiskChip
                    key={sig.code}
                    signal={sig}
                    selected={shownSignal?.signal.code === sig.code}
                    onSelect={(s) => onSelectSignal(s.code)}
                  />
                ))}
              </div>
              {/* One box, not one per chip: a chip that opened its own
                  expansion left two boxes on screen saying the same kind of
                  sentence in two different styles. It shows whichever chip
                  the reader selected and nothing before that — see
                  `shownSignal` for why it no longer opens on a signal of
                  OpenCheck's choosing. */}
              {shownSignal && (
                <div className="mt-3.5">
                  <SignalEvidence
                    lead={shownSignal}
                    sourceNames={sourceNames}
                    hasCard={(id: string) =>
                      cddBuckets.some((b) => b.sourceId === id)
                    }
                    onShowSource={showSourceCard}
                  />
                </div>
              )}
              {/* Plain sentence, and the regulatory detail behind it kept in
                  an `Explain` rather than deleted: "chips aligned to AMLA
                  (the EU Anti-Money Laundering Authority) read BODS
                  (Beneficial Ownership Data Standard) records" is an
                  accurate thing to be able to find and a poor thing to open
                  a section with. */}
              <p className="text-oo-small text-oo-muted mt-2.5">
                Select any chip to read the record behind it.{" "}
                <Explain label="Where these come from">
                  Signals are derived from open data by deterministic rules,
                  never by a model. Those aligned to AMLA — the EU
                  Anti-Money Laundering Authority — are read from BODS
                  (Beneficial Ownership Data Standard) records; jurisdiction
                  signals come from the FATF (Financial Action Task Force)
                  and EU lists. A signal is a pointer to a record, not a
                  conclusion about the company.
                </Explain>
              </p>
            </>
          ) : (
            // The verdict strip already says "No risk signals surfaced
            // across the sources that answered" one screen above; repeating
            // it verbatim here reads as a rendering fault. This section only
            // exists in that case to hold the structural chips, so it says
            // what it is holding.
            <p className="text-oo-small text-oo-muted">
              Nothing adverse surfaced. The structural facts below describe how
              the company is put together.
            </p>
          )}

          {contextCodes.length > 0 && (
            <div className="mt-5" id="structural-context">
              <p className="text-oo-small text-oo-muted mb-2">
                Structural context — how the company is put together, not a
                finding against it.
              </p>
              {/* These stay self-expanding rather than driving the box
                  above them. The box sits under the risk chips, and a
                  control that updates something off-screen above it is
                  worse than one that opens in place — the styles are the
                  same either way, because the expansion is the same
                  `SignalEvidence` component. */}
              <div className="flex flex-wrap gap-2">
                {contextCodes.map((sig) => (
                  <RiskChip key={sig.code} signal={sig} />
                ))}
              </div>
            </div>
          )}
        </PanelSection>
      )}

      {/* "Archive matches — OpenAleph" (informational percolation matches,
          Phase 97) renders underneath the OpenAleph source card in the
          sources list below — see OpenAlephArchiveMatches. */}

      {/* One question, not two boxes.

          "Cross-source identifiers" and "Possibly the same entity" are the
          same enquiry from two directions — what corroborates that this is
          the right company, and what suggests the records might not all be
          it. Splitting them into two collapsibles with two eyebrow labels
          made a reader open two things to answer one question, and put the
          reassuring half and the doubtful half in separate boxes where
          neither qualified the other.

          The band keeps `id="cross-source-identifiers"` because the subject
          card's identifier badge scrolls to it by that id, and because a
          shared report link may already carry the anchor. */}
      {(crossSourceLinks.length > 0 ||
        gleifMappedIds.length > 0 ||
        possiblySame.length > 0 ||
        profileRowsForBand.length > 0) && (
          <PanelSection
            id="cross-source-identifiers"
            title="Is this the right company?"
            // Shut on arrival. Identity corroboration is reassurance, and
            // reassurance that occupies a screen before anyone doubted
            // anything is in the way of the finding. The subject card's
            // "LEI confirmed by N sources" badge is what opens it —
            // which is the moment a reader is actually asking.
            open={identityOpen}
            onToggle={onIdentityToggle}
            // Phase 245: what is inside, never a second corroboration
            // count. "2 identifiers matched across 5 independent sources"
            // sat one screen below the subject card's "LEI confirmed by 4
            // sources" — two numbers answering two questions, read as one.
            // The badge makes the claim; the band holds the evidence.
            aside={identityBandContents({
              profile: profileRowsForBand.length > 0,
              identifiers: crossSourceLinks.length + gleifMappedIds.length,
              candidatePairs: possiblySame.length,
            })
            }
          >
            {/* The profile leads the band (Phase 154): legal form,
                register status, incorporation date and registered address
                are answers to *which* company, and this is where a reader
                asking that already looks — the LEI badge on the subject
                card opens it. Each row names the sources stating it, never
                a count: two sources that copy each other would read as
                two. A fact no source stated is absent, not "unknown". */}
            {profileRowsForBand.length > 0 && (
              <div
                className={
                  crossSourceLinks.length > 0 || gleifMappedIds.length > 0
                    ? "mb-5 border-b border-oo-rule pb-4"
                    : ""
                }
              >
                <Eyebrow as="h3">Company profile — what the registers say</Eyebrow>
                <dl className="mt-2.5 grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-3">
                  {profileRowsForBand.map((row) => (
                    <div key={row.label} className="flex flex-col gap-0.5 min-w-0">
                      <dt className="font-body text-oo-meta font-bold uppercase tracking-oo-eyebrow text-oo-muted">
                        {row.label}
                      </dt>
                      <dd className="m-0 text-oo-small text-oo-ink break-words">{row.value}</dd>
                      <dd className="m-0 text-oo-meta text-oo-muted">{row.sources}</dd>
                    </div>
                  ))}
                </dl>
              </div>
            )}

            {(crossSourceLinks.length > 0 || gleifMappedIds.length > 0) && (
              <CrossSourceIdentifiersTable
                links={crossSourceLinks}
                gleifMapped={gleifMappedIds}
                sourceNames={sourceNames}
              />
            )}

            {possiblySame.length > 0 && (
              <div
                id="possibly-same"
                className={
                  crossSourceLinks.length > 0 || gleifMappedIds.length > 0
                    ? "mt-5 border-t border-oo-rule pt-4 scroll-mt-4"
                    : "scroll-mt-4"
                }
              >
                {/* A sub-block, not a peer section — the same treatment
                    structural context gets inside Risk signals. These pairs
                    qualify the corroboration above them, and a reader who
                    sees "2 identifiers matched across 5 sources" needs to
                    meet them in the same breath rather than in the next box
                    down. */}
                <SectionHeading as="h3">Possibly the same entity</SectionHeading>
                <p className="mt-1 text-oo-small text-oo-muted">
                  <span className="font-semibold">
                    {possiblySame.length} candidate pair
                    {possiblySame.length === 1 ? "" : "s"}
                  </span>{" "}
                  flagged for review — same name &amp; jurisdiction, no shared
                  identifier
                </p>
                <div className="mt-3">
                  <PossiblySameTable pairs={possiblySame} />
                </div>
              </div>
            )}
          </PanelSection>
        )}

      {(cddBuckets.length > 0 || pendingCddSources.length > 0 || answeredNoRecord.length > 0) && (
        <PanelSection
          title="What each source said"
          // Only while sources are still answering (Phase 245). Once they
          // have, the verdict strip's Coverage column is where the count
          // lives: the same number here was the third statement of it in
          // one QuickCheck. Each card below says what its source did.
          aside={
            settled.pending > 0 ? (
              <span className="text-oo-blue">
                {coverageCopy({
                  answered: answeredApplicable,
                  applicable: applicableCount,
                  total: registryTotal,
                  jurisdiction: jurisdiction,
                  screening: true,
                  pending: settled.pending,
                  failed: settled.failed,
                }).aside}
              </span>
            ) : undefined
          }
        >
          <div className="space-y-4">
            {cddBuckets.map((b) => (
              <div key={b.sourceId} id={`source-${b.sourceId}`} className="scroll-mt-4">
                <SourceBucketCard
                  bucket={b}
                  riskByHit={riskByHit}
                  subjectSignals={riskSignals}
                  bodsCountMap={bodsCountMap}
                  bodsBreakdownMap={bodsBreakdownMap}
                  onRetry={b.error && !savedReport ? () => onRetrySource(b.sourceId) : undefined}
                  retrying={retryingSources.has(b.sourceId)}
                  liveness={sourceLiveness[b.sourceId]}
                  footnote={
                    b.sourceId === "gleif" && gleifChildrenInfo && gleifChildrenInfo.total > 100
                      ? `Showing the first ${gleifChildrenInfo.fetched} of ${gleifChildrenInfo.total.toLocaleString()} direct subsidiaries in BODS statements (GLEIF Level 2) — the whole network is in the Subsidiaries tab`
                      : undefined
                  }
                  /* Informational percolation matches (Phase 97) sit with
                     the source they came from — inside its card, not as a
                     second card stacked underneath it. */
                  extra={
                    b.sourceId === "openaleph" && oaScreening.length > 0 ? (
                      <OpenAlephArchiveMatches matches={oaScreening} />
                    ) : undefined
                  }
                />
              </div>
            ))}
            {pendingCddSources.map((id) => (
              <SkeletonSourceCard key={id} />
            ))}
            {answeredNoRecord.length > 0 && (
              <div id="sources-no-record" className="scroll-mt-4">
                <SectionHeading as="h3">Answered with no record</SectionHeading>
                <p className="mt-1 text-oo-small text-oo-muted">
                  These sources were asked about this company and hold nothing on it.
                </p>
                <ul className="mt-2 flex flex-wrap gap-1.5">
                  {answeredNoRecord.map((id) => (
                    <li key={id}>
                      <Chip tone="neutral">{sourceLabel(id, sourceNames)}</Chip>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {/* Percolation can match related parties even when the subject
                lookup produced no OpenAleph card — keep the matches
                visible in that case rather than dropping them. */}
            {oaScreening.length > 0 &&
              !cddBuckets.some((b) => b.sourceId === "openaleph") && (
                <OpenAlephArchiveMatches matches={oaScreening} standalone />
              )}
          </div>
        </PanelSection>
      )}

      {lei && !savedReport && (
        <SecuritiesSection
          lei={lei}
          onError={onPanelError}
          onRecovered={onPanelRecovered}
          sourceNames={sourceNames}
          listing={primaryListing}
        />
      )}

      {/* Phase 208: MEIP is a source. Its statements arrive on the source
          cards above like any other register's, and a group head's list
          lives on the Subsidiaries tab — nothing about MEIP renders here
          outside a card. */}

      {lei && !streaming && totalHits > 0 && (
        <ExportPanel
          lei={lei}
          legalName={legalName}
          contributingSourceIds={[...cddBuckets, ...esgBuckets]
            .filter((b) => b.hits.some((h) => !h.is_stub))
            .map((b) => b.sourceId)}
          saved={
            savedReport
              ? { reportId: savedReport.report_id, licensing: savedReport.payload.licensing }
              : null
          }
        />
      )}
    </PanelCard>
  );
}
