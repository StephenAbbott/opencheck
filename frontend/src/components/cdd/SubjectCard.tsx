import { useState } from "react";
import { ExportMenu } from "../export/ExportMenu";
import { trackEvent } from "../../lib/analytics";
import { BASE_URL } from "../../lib/api";
import type { StatusChip } from "../../lib/subjectProfile";
import { chipClasses } from "../ui/Chip";


/** "just now" / "1 min ago" / "12 min ago" from an ISO timestamp. */
export function replayAgeLabel(fetchedAt: string, now: Date = new Date()): string {
  const then = new Date(fetchedAt).getTime();
  if (Number.isNaN(then)) return "recently";
  const mins = Math.max(0, Math.floor((now.getTime() - then) / 60_000));
  if (mins < 1) return "just now";
  return `${mins} min ago`;
}

/**
 * SubjectCard — top-of-page summary of the LEI lookup subject: name,
 * jurisdiction flag, LEI, a compact risk-signal summary (the headline
 * finding, promoted from further down the page), and the share link.
 *
 * The signal summary mirrors the share card's hierarchy — name → count →
 * top chips — so the page and its social preview agree on what matters.
 * `signals` should be the aggregated (distinct-code) list; `screening`
 * keeps the row honest while sources are still streaming.
 */
export function SubjectCard({
  lei,
  legalName,
  jurisdiction,
  replayedAt = null,
  onRefresh,
  identifierSources = 0,
  onShowIdentifiers,
  status = null,
  pdfBusy = false,
  mdBusy = false,
  onPdf,
  onMarkdown,
  exportError,
}: {
  lei: string;
  legalName: string | null;
  jurisdiction?: string | null;
  /** ISO completion time of the original run when results are replayed from cache. */
  replayedAt?: string | null;
  /** Re-runs the lookup bypassing the replay cache (?refresh=true). */
  onRefresh?: () => void;
  /** Distinct sources independently publishing the subject's LEI — not
   *  sources sharing just any identifier (QID, national number, …): the
   *  badge renders beside the LEI, so its number must be scoped to the LEI
   *  (see countLeiConfirmingSources in lib/identifierBadge.ts). The badge
   *  only renders from 2 (a lone source confirms nothing). Worded
   *  "confirmed by" rather than "corroborated by" — the sources agree on the
   *  identifier, they do not corroborate each other's substance. And it
   *  names the **LEI**, not "identifier": the section this badge opens
   *  counts every identifier across every source that shares one, so on BP
   *  the two numbers sat a screen apart reading "Identifier confirmed by 6
   *  sources" and "3 identifiers matched across 8 sources". Both were
   *  right, and nothing on the page said they were answers to different
   *  questions. */
  identifierSources?: number;
  /** Goes to the "Is this the right company?" section (switching to
   *  QuickCheck first — the section only renders there). */
  onShowIdentifiers?: () => void;
  /** Register status (Phase 154), from `lib/subjectProfile.statusChip`. Of
   *  the four profile facts this is the only one promoted to the card: a
   *  dissolved company with an ACTIVE LEI has to be met before the verdict,
   *  not found on a structured-records card. Legal form, incorporation date
   *  and address live in "Is this the right company?", which the badge
   *  beside this chip opens. Null renders nothing — absence is not active. */
  status?: StatusChip | null;
  /** Report downloads. They live on App because the payload they embed (the
   *  narrative and its dispositions) is produced by a different card. */
  pdfBusy?: boolean;
  mdBusy?: boolean;
  onPdf: () => void;
  onMarkdown: () => void;
  /** A failed export, reported beside the control that started it. */
  exportError?: string | null;
}) {
  const [copied, setCopied] = useState(false);
  const shareUrl = `${BASE_URL || "https://api.opencheck.world"}/share/${lei}`;
  const cc = (jurisdiction || "").trim().toLowerCase().split("-")[0];

  return (
    <section className="px-4 py-[18px] sm:px-7 sm:py-6">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-blue">
            Subject
          </p>
          {/* break-words: long single words (e.g. "Aktiengesellschaft") must
              wrap inside the min-w-0 column instead of overflowing under the
              right-hand column — that overflow is what made the identifier
              badge appear to cover the name on phones. */}
          <h2 className="font-head font-bold text-oo-ink mt-2 leading-tight break-words text-[clamp(1.25rem,2.5vw,1.6rem)]">
            {legalName || `LEI ${lei}`}
          </h2>
          {/* Identity line — small and muted so the card stays airy on mobile. */}
          <p className="mt-1.5 flex items-center gap-2 flex-wrap text-[12px] text-oo-muted">
            {cc && (
              <span className="inline-flex items-center gap-1.5">
                <img
                  src={`/bods-dagre-images/flags/${cc}.svg`}
                  alt=""
                  aria-hidden="true"
                  className="h-3 w-auto rounded-[2px] border border-oo-rule"
                  onError={(e) => {
                    (e.target as HTMLImageElement).style.display = "none";
                  }}
                />
                <span className="uppercase">{cc}</span>
              </span>
            )}
            {cc && <span aria-hidden>·</span>}
            <span className="font-mono break-all">LEI {lei}</span>
            {/* Links straight to this entity's raw JSON — the same
                /lookup?lei= the app itself calls. Scoped to the LEI it sits
                beside, so it lives in the identity row rather than the
                share-link corner; costs no extra vertical space on any
                breakpoint (it just wraps with the rest of the row). */}
            {/* Phase 122: the raw-JSON "API" chip that sat here came off.
                A developer affordance in the subject's identity row, beside
                the LEI, on a page a compliance analyst or journalist reads
                first — and the same endpoint is one labelled row inside the
                export panel, where someone looking for an API will look.
                See the Design v2 ticket, recommendation 3. */}
            {/* Desktop placement: inline pill beside the LEI it qualifies.
                Hidden on mobile, where the identity column is too narrow
                (~150px beside the share button) — the block placement below
                the header row takes over there. Both placements are in
                normal document flow, so neither can paint over the entity
                name (the old right-column mobile placement did exactly that
                once a long name overflowed its squeezed column). */}
            <IdentifierBadge
              count={identifierSources}
              onClick={onShowIdentifiers}
              className="hidden sm:inline-flex gap-1 rounded-full px-2.5 py-0.5"
            />
            <RegisterStatusChip status={status} className="hidden sm:inline-flex" />
          </p>
        </div>
        {/* The report's one export affordance, as the subject's primary
            control. It was three: "Copy share link" here, an "Export" menu in
            the AI summary card header halfway down the page, and the Download
            data section's own button — three entry points for one intention,
            and the one most readers want was the only one not on the menu. */}
        <div className="shrink-0 flex flex-col items-end gap-2">
          <ExportMenu
            label="Share and export"
            variant="primary"
            onShare={() => {
              navigator.clipboard?.writeText(shareUrl);
              trackEvent("share_link");
              setCopied(true);
              window.setTimeout(() => setCopied(false), 1500);
            }}
            shareCopied={copied}
            pdfBusy={pdfBusy}
            mdBusy={mdBusy}
            onPdf={onPdf}
            onMarkdown={onMarkdown}
          />
        </div>
      </div>

      {/* Outside the header row, not a third item inside it. The row is
          `justify-between` with no `flex-wrap`, so a sentence placed in it
          becomes a third column and crushes the `min-w-0` identity column:
          at 375px the company name went to zero width. The card's own
          comments record that this column has crushed the name once
          before. */}
      {exportError && (
        <p
          role="alert"
          className="mt-3 text-oo-small text-oo-warn-text bg-oo-warn-bg border border-oo-warn-border rounded-oo px-3 py-2"
        >
          {exportError}
        </p>
      )}

      <div>
        {/* Always-mounted live region so the copied confirmation is announced. */}
        <span role="status" className="sr-only">
          {copied ? "Share link copied" : ""}
        </span>
      </div>

      {/* Mobile placement of the identifier badge — full card width available
          below the header row, so it renders on one line directly under the
          LEI it qualifies (the LEI line is the bottom of the identity
          column). Same responsive treatment as the amber replay box: card
          radius, content width, in normal flow — it pushes content down
          rather than overlapping it. Hidden on sm+ where the inline pill
          beside the LEI takes over; the hidden instance is display:none so
          only one is in the accessibility tree. */}
      <IdentifierBadge
        count={identifierSources}
        onClick={onShowIdentifiers}
        className="sm:hidden inline-flex gap-1.5 max-w-full text-left rounded-oo px-3 py-1.5 mt-3"
      />
      {/* Same placement rule as the badge: its own line under the header row
          on mobile, so it can never squeeze the name; inline beside the LEI
          on sm+, where the hidden instance is display:none. */}
      <div className="sm:hidden mt-2">
        <RegisterStatusChip status={status} />
      </div>

      {/* Provenance badge — a replayed (cached) run must never look live.
          Amber note + a fresh-check action wired to ?refresh=true. Sits
          below the header row spanning the card on mobile (flex = full
          width, so it uses the whitespace instead of stacking tall in the
          narrow identity column); on sm+ it shrinks back to a content-width
          pill. Rounding is responsive for the same reason as the identifier
          badge: wrapped text + rounded-full clips into the corners. */}
      {replayedAt && (
        <p className="mt-3 flex sm:inline-flex items-center gap-2 flex-wrap text-[12px] text-[#92400e] bg-[#fef3c7] border border-[#fde68a] rounded-oo px-3 py-1.5 sm:rounded-full sm:py-1">
          <span>
            Results from a check run {replayAgeLabel(replayedAt)} — not re-queried.
          </span>
          {onRefresh && (
            <button
              type="button"
              onClick={onRefresh}
              className="font-semibold underline underline-offset-2 hover:no-underline"
            >
              Run a fresh check
            </button>
          )}
        </p>
      )}

      {/* The risk-signal summary that used to sit here was the v1 verdict:
          a count, the top chips and a "+N more" jump. `VerdictStrip` has done
          exactly that job since Phase 122 — sentence, "What we found", the same
          chips, the same overflow — and this was never removed, so a report
          rendered both, one above the other. Removed in Phase 126. The subject
          card is identity only; what was found belongs to the verdict. */}
    </section>
  );
}

/**
 * Register status chip (Phase 154). Neutral for a live status — a status is
 * a fact with no valence (`ui/Chip`) — warn while a terminal process is under
 * way, and a dark, un-tinted chip once the register says the company has
 * ended. Never the risk tone: dissolved is a fact about the company, not a
 * finding against it. The full sentence is read to assistive technology.
 */
function RegisterStatusChip({
  status,
  className = "",
}: {
  status: StatusChip | null;
  className?: string;
}) {
  if (!status) return null;
  const classes =
    status.tone === "terminal"
      ? `inline-flex items-center gap-1.5 rounded-full border font-body px-2.5 py-0.5 text-oo-meta bg-oo-navy border-oo-navy text-white ${className}`
      : chipClasses(status.tone, "sm", className);
  const dot =
    status.tone === "neutral"
      ? "bg-oo-node-green"
      : status.tone === "warn"
        ? "bg-oo-warn-border"
        : "bg-white";
  return (
    <span className={`${classes} font-medium`.trim()}>
      <span aria-hidden="true" className={`h-[7px] w-[7px] shrink-0 rounded-full ${dot}`} />
      <span aria-hidden="true">{status.label}</span>
      <span className="sr-only">{status.detail}</span>
    </span>
  );
}

/**
 * "LEI confirmed by N sources" badge. Rendered twice by SubjectCard —
 * an inline pill beside the LEI on sm+, and a full-width-capable box directly
 * below the header row (i.e. under the LEI) on mobile — with only one
 * instance visible per breakpoint (the hidden one is display:none, so it
 * also leaves the accessibility tree). Both are in normal document flow and
 * can never overlap the entity name. Renders nothing below 2 sources: a lone
 * source confirms nothing. `className` carries the per-placement layout
 * (visibility, radius, padding); the identity styling lives here.
 */
function IdentifierBadge({
  count,
  onClick,
  className,
}: {
  count: number;
  onClick?: () => void;
  className: string;
}) {
  if (count < 2 || !onClick) return null;
  return (
    <button
      type="button"
      onClick={onClick}
      className={`items-center text-oo-meta font-semibold text-emerald-700 bg-emerald-50 border border-emerald-200 hover:bg-emerald-100 transition-colors ${className}`}
    >
      <svg
        width="11"
        height="11"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
        className="shrink-0"
      >
        <path d="M20 6 9 17l-5-5" />
      </svg>
      <span>
        LEI confirmed by {count} source{count === 1 ? "" : "s"}
        <span className="sr-only">
          {" "}
          — this many independently publish it; goes to the &ldquo;Is this the
          right company?&rdquo; section, which covers every identifier
        </span>
      </span>
    </button>
  );
}
