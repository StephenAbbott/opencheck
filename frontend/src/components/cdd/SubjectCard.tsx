import { useState } from "react";
import { BASE_URL } from "../../lib/api";
import type { RiskSignal } from "../../lib/api";
import { RiskChip } from "../risk/RiskChip";

/** How many signal chips show inline before the "+N more" link. */
const SIGNAL_PREVIEW_COUNT = 4;

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
  signals = [],
  screening = false,
  replayedAt = null,
  onRefresh,
  identifierSources = 0,
  onShowIdentifiers,
}: {
  lei: string;
  legalName: string | null;
  jurisdiction?: string | null;
  signals?: RiskSignal[];
  screening?: boolean;
  /** ISO completion time of the original run when results are replayed from cache. */
  replayedAt?: string | null;
  /** Re-runs the lookup bypassing the replay cache (?refresh=true). */
  onRefresh?: () => void;
  /** Distinct sources publishing a shared identifier for this subject.
   *  The badge only renders from 2 (a lone source confirms nothing).
   *  Deliberately worded "Identifier confirmed by" — the sources agree on
   *  the identifier, they do not corroborate each other's substance. */
  identifierSources?: number;
  /** Expands + scrolls to the cross-source identifiers box. */
  onShowIdentifiers?: () => void;
}) {
  const [copied, setCopied] = useState(false);
  const shareUrl = `${BASE_URL || "https://api.opencheck.world"}/share/${lei}`;
  const cc = (jurisdiction || "").trim().toLowerCase().split("-")[0];
  const preview = signals.slice(0, SIGNAL_PREVIEW_COUNT);
  const overflow = signals.length - preview.length;

  return (
    <section className="mb-8 bg-white border border-oo-rule rounded-oo p-7 transition-shadow hover:shadow-oo-card">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-blue">
            Subject
          </p>
          <h2 className="font-head font-bold text-oo-ink mt-2 leading-tight text-[clamp(1.25rem,2.5vw,1.6rem)]">
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
            {/* Desktop placement: inline with the LEI it qualifies. On
                mobile the identity column is too narrow — the badge wrapped
                to three lines and rounded-full turned it into an ellipse —
                so this instance hides and the copy in the right-hand column
                (under the share button) takes over. */}
            <IdentifierBadge
              count={identifierSources}
              onClick={onShowIdentifiers}
              className="hidden sm:inline-flex gap-1 rounded-full px-2.5 py-0.5"
            />
          </p>
        </div>
        <div className="shrink-0 flex flex-col items-end gap-2">
          <button
            type="button"
            onClick={() => {
              navigator.clipboard?.writeText(shareUrl);
              setCopied(true);
              window.setTimeout(() => setCopied(false), 1500);
            }}
            title="Copies a link whose social-media preview shows a live summary card for this entity"
            className="inline-flex items-center gap-1.5 text-[12px] font-medium text-oo-blue border border-[#cfd6f5] bg-[#eef1fb] hover:bg-[#e2e7f9] rounded-full px-3 py-1.5 transition-colors"
          >
            <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true">
              <path
                d="M6.5 9.5 L9.5 6.5 M7.5 4.5 l2-2 a2.5 2.5 0 0 1 3.5 3.5 l-2 2 M8.5 11.5 l-2 2 a2.5 2.5 0 0 1-3.5-3.5 l2-2"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
              />
            </svg>
            {copied ? "Link copied" : "Copy share link"}
            <span className="sr-only">
              {" "}
              — copies a link whose social-media preview shows a live summary card for this entity
            </span>
          </button>
          {/* Mobile placement of the identifier badge — right column, under
              the share button, with the card radius (matching the replay
              badge) instead of a full pill so wrapped text keeps square-ish
              corners. max-w keeps a long label from squeezing the entity
              name in the left column. */}
          <IdentifierBadge
            count={identifierSources}
            onClick={onShowIdentifiers}
            className="sm:hidden inline-flex gap-1.5 rounded-oo px-3 py-1.5 max-w-[12rem] text-right justify-end"
          />
        </div>
        {/* Always-mounted live region so the copied confirmation is announced. */}
        <span role="status" className="sr-only">
          {copied ? "Share link copied" : ""}
        </span>
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

      {/* Compact risk-signal summary — the headline finding, up top. The full
          strip further down keeps the per-chip evidence and explanation. */}
      {(signals.length > 0 || !screening) && (
        <div className="mt-4 pt-4 border-t border-oo-rule flex items-center gap-2 flex-wrap">
          {signals.length > 0 ? (
            <>
              <span className="text-[13px] text-oo-ink shrink-0">
                <span className="font-head font-bold text-[17px]">{signals.length}</span>{" "}
                risk signal{signals.length === 1 ? "" : "s"}
              </span>
              {preview.map((sig) => (
                <RiskChip key={sig.code} signal={sig} compact />
              ))}
              {overflow > 0 && (
                // A button, not an <a href="#…">: hash navigation is a
                // same-document navigation, which fires popstate — and the
                // app's popstate handler re-runs the ?lei= lookup, making the
                // page appear to refresh.
                <button
                  type="button"
                  onClick={() => {
                    const el = document.getElementById("risk-signals");
                    if (!el) return;
                    el.scrollIntoView({ behavior: "smooth", block: "start" });
                    if (el.tabIndex < 0) el.tabIndex = -1;
                    el.focus({ preventScroll: true });
                  }}
                  className="text-[12px] font-medium text-oo-blue hover:underline"
                >
                  +{overflow} more
                </button>
              )}
            </>
          ) : (
            <span className="text-[13px] text-oo-muted">
              No risk signals surfaced across the sources checked.
            </span>
          )}
        </div>
      )}
    </section>
  );
}

/**
 * "Identifier confirmed by N sources" badge. Rendered twice by SubjectCard —
 * inline beside the LEI on sm+ and in the right-hand column on mobile — with
 * only one instance visible per breakpoint (the hidden one is display:none,
 * so it also leaves the accessibility tree). Renders nothing below 2 sources:
 * a lone source confirms nothing. `className` carries the per-placement
 * layout (visibility, radius, padding); the identity styling lives here.
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
      title="Independent sources publish a matching identifier for this entity — jump to the detail"
      className={`items-center text-[11px] font-semibold text-emerald-700 bg-emerald-50 border border-emerald-200 hover:bg-emerald-100 transition-colors ${className}`}
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
        Identifier confirmed by {count} source{count === 1 ? "" : "s"}
        <span className="sr-only">
          {" "}
          — expands the cross-source identifier detail
        </span>
      </span>
    </button>
  );
}
