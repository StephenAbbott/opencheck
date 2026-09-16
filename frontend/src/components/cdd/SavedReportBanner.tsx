/**
 * SavedReportBanner — the band above a saved report (Phase 217).
 *
 * **It never collapses.** A saved page renders through the same components as
 * a live check, so without this band nothing would distinguish the two, and a
 * saved page must never be mistaken for a live one. It names the fifth clock
 * (when a reader kept the check) beside the check's own, says that nothing on
 * the page has been re-checked since, and prints the SHA-256 that lets a
 * reader verify the page is the record that was saved.
 *
 * Wording lives in `lib/savedReport.ts` so the logic-only suite pins it.
 */

import { useState } from "react";
import { extendSavedReport, savedReportJsonUrl, type SavedReportMeta } from "../../lib/api";
import {
  SCOPE_SENTENCE,
  VERIFY_SENTENCE,
  bannerText,
  manageTokenFor,
  utcDate,
} from "../../lib/savedReport";
import { buttonClasses } from "../ui/Button";
import { Explain } from "../ui/Explain";

export function SavedReportBanner({
  meta,
  runCompletedAt,
  onRunLive,
  onExtended,
}: {
  meta: SavedReportMeta;
  runCompletedAt: string;
  /** Open a live check of the same company. */
  onRunLive: () => void;
  onExtended: (meta: SavedReportMeta) => void;
}) {
  const text = bannerText(meta, runCompletedAt);
  const token = manageTokenFor(meta.report_id);
  const [extending, setExtending] = useState(false);
  const [extendMessage, setExtendMessage] = useState<string | null>(null);

  async function extend() {
    if (!token) return;
    setExtending(true);
    setExtendMessage(null);
    try {
      const next = await extendSavedReport(meta.report_id, token);
      onExtended(next);
      setExtendMessage(`Kept until ${utcDate(next.expires_at)}.`);
    } catch (e) {
      setExtendMessage(e instanceof Error ? e.message : "Could not extend this saved report.");
    } finally {
      setExtending(false);
    }
  }

  return (
    <section
      aria-labelledby="saved-report-heading"
      className="mb-4 rounded-oo border border-oo-softBorder bg-oo-soft px-4 py-3.5 sm:px-6"
    >
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between sm:gap-6">
        <div className="min-w-0">
          <h2
            id="saved-report-heading"
            className="text-oo-meta font-semibold uppercase tracking-oo-eyebrow text-oo-blue"
          >
            {text.heading}
          </h2>
          <p className="mt-1 text-oo-body text-oo-ink leading-[1.55]">{text.clocks}</p>
          <p className="mt-0.5 text-oo-small text-oo-muted leading-[1.55]">
            {text.notLive} {text.kept}
          </p>
        </div>
        <button type="button" onClick={onRunLive} className={buttonClasses("secondary", "sm", "shrink-0 self-start")}>
          Run a live check
        </button>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-2 text-oo-meta text-oo-muted">
        <span className="min-w-0">
          SHA-256{" "}
          <code className="font-mono text-oo-ink break-all">{meta.content_hash}</code>
        </span>
        <a href={savedReportJsonUrl(meta.report_id)} className="text-oo-blue underline underline-offset-2 hover:text-oo-burst">
          Download the saved JSON
        </a>
        {token && (
          <button
            type="button"
            onClick={extend}
            disabled={extending}
            className="text-oo-blue underline underline-offset-2 hover:text-oo-burst disabled:opacity-60"
          >
            {extending ? "Extending…" : "Keep for another 90 days"}
          </button>
        )}
        <Explain label="What a saved report holds and how to check it">
          <span className="block">{SCOPE_SENTENCE}</span>
          <span className="mt-1 block">{VERIFY_SENTENCE}</span>
        </Explain>
      </div>
      <p role="status" className="text-oo-meta text-oo-muted empty:hidden mt-1">
        {extendMessage ?? ""}
      </p>
    </section>
  );
}

/** In place of a tab a saved report does not hold. */
export function SavedReportExcluded({
  sentence,
  onRunLive,
}: {
  sentence: string;
  onRunLive: () => void;
}) {
  return (
    <div className="px-4 py-[18px] sm:px-6 sm:py-[22px]">
      <p className="text-oo-small text-oo-muted leading-[1.6] max-w-[70ch]">{sentence}</p>
      <button type="button" onClick={onRunLive} className={buttonClasses("secondary", "sm", "mt-3")}>
        Run a live check
      </button>
    </div>
  );
}
