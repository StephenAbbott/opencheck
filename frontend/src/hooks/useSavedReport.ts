import { useCallback, useEffect, useMemo, useState } from "react";
import { trackEvent } from "../lib/analytics";
import {
  downloadReportMarkdown,
  downloadReportPdf,
  saveReport,
  savedReportShareUrl,
  SavedReportError,
  type SavedReport,
  type SavedReportMeta,
} from "../lib/api";
import {
  SAVED_WITHOUT_SUMMARY,
  rememberManageToken,
  saveEligibility,
  savedConfirmation,
  savedDeepen,
  savedStatements as savedStatementsFrom,
  utcDate,
  utcDateTime,
} from "../lib/savedReport";
import type { ReportExportPayload } from "../components/cdd/NarrativePanel";
import type { SavedReportContextValue } from "../components/cdd/savedReportContext";

/*
 * The report as a record: the saved copy on screen at /report/{id} (Phase
 * 217), saving the live run, and the PDF / Markdown exports — which embed
 * the same summary and dispositions a save does, and render a saved report's
 * own copy when one is open. Moved out of App() in Phase 246.
 *
 * Opening a saved report stays in App (`openSavedReport`): it replaces the
 * whole page — the run, the mode, the search field — through the same
 * handlers a live check uses.
 */
export function useSavedReport({
  streamingLei,
  runCompletedAt,
  streaming,
}: {
  streamingLei: string | null;
  runCompletedAt: string | null;
  streaming: boolean;
}) {
  // ── Saved reports (Phase 217) ──────────────────────────────────────
  // A save names the run on screen (`runCompletedAt`, from its `done` event)
  // and the server copies its own held copy. A per-source retry changes the
  // page without changing the run, so it blocks saving until the check is
  // run again.
  const [retriedSinceRun, setRetriedSinceRun] = useState(false);
  const [saveBusy, setSaveBusy] = useState(false);
  const [savedFromRun, setSavedFromRun] = useState<SavedReportMeta | null>(null);
  const [saveNotice, setSaveNotice] = useState<string | null>(null);
  // The saved report on screen at /report/{id}. Non-null means this page is
  // a record, not a live check: everything that would reach for today's data
  // is off, and the banner above the subject says so.
  const [savedReport, setSavedReport] = useState<SavedReport | null>(null);
  const [savedOpening, setSavedOpening] = useState(false);
  const [savedOpenError, setSavedOpenError] = useState<string | null>(null);

  // The report exports embed the narrative and its dispositions, which are
  // produced by NarrativePanel further down the page. That is why the control
  // used to live in *its* header; now the control is on the subject and the
  // payload comes up to here instead.
  const [exportPayload, setExportPayload] = useState<ReportExportPayload>({
    narrative: null,
    dispositions: null,
  });
  const [pdfBusy, setPdfBusy] = useState(false);
  const [mdBusy, setMdBusy] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);

  const downloadPdf = useCallback(async () => {
    if (!streamingLei) return;
    setPdfBusy(true);
    setExportError(null);
    try {
      await downloadReportPdf(
        streamingLei,
        exportPayload.narrative,
        exportPayload.dispositions,
        savedReport?.report_id ?? null
      );
      trackEvent("pdf_export");
    } catch (e) {
      setExportError(e instanceof Error ? e.message : "Could not generate the PDF.");
    } finally {
      setPdfBusy(false);
    }
  }, [streamingLei, exportPayload, savedReport]);

  const downloadMarkdown = useCallback(async () => {
    if (!streamingLei) return;
    setMdBusy(true);
    setExportError(null);
    try {
      // Same embedding rules as the PDF — the same record in a portable
      // format, and it works even where the PDF route is 503.
      await downloadReportMarkdown(
        streamingLei,
        exportPayload.narrative,
        exportPayload.dispositions,
        savedReport?.report_id ?? null
      );
    } catch (e) {
      setExportError(
        e instanceof Error ? e.message : "Could not generate the Markdown report."
      );
    } finally {
      setMdBusy(false);
    }
  }, [streamingLei, exportPayload, savedReport]);

  // Phase 217: a saved report is shared by link and never indexed. robots.txt
  // already disallows /report on this host; the meta tag covers a crawler that
  // renders the page anyway, and comes off again when the reader leaves.
  useEffect(() => {
    if (!savedReport) return;
    const meta = document.createElement("meta");
    meta.name = "robots";
    meta.content = "noindex, nofollow";
    document.head.appendChild(meta);
    return () => meta.remove();
  }, [savedReport]);

  /** Copy a link, reporting whether the clipboard took it. */
  async function copyLink(url: string): Promise<boolean> {
    try {
      await navigator.clipboard?.writeText(url);
      return Boolean(navigator.clipboard);
    } catch {
      return false;
    }
  }

  /**
   * Save the live check on screen (Phase 217). The server copies its own held
   * run; the summary goes with it only if the server wrote it from this run —
   * otherwise the report is saved without it and the reader is told, rather
   * than the save failing over the part they did not ask about.
   */
  async function saveThisReport() {
    if (!streamingLei || !runCompletedAt || saveBusy) return;
    setSaveBusy(true);
    setExportError(null);
    setSaveNotice(null);
    const narrativeRunId = exportPayload.narrative?.run_id || null;
    try {
      let meta: SavedReportMeta & { manage_token: string };
      let withSummary = Boolean(narrativeRunId);
      let droppedSummary = false;
      try {
        meta = await saveReport({ lei: streamingLei, run_completed_at: runCompletedAt, narrative_run_id: narrativeRunId });
      } catch (e) {
        if (!(e instanceof SavedReportError) || e.code !== "narrative_not_held" || !narrativeRunId) throw e;
        meta = await saveReport({ lei: streamingLei, run_completed_at: runCompletedAt });
        withSummary = false;
        droppedSummary = true;
      }
      rememberManageToken(meta.report_id, meta.manage_token);
      setSavedFromRun(meta);
      const copied = await copyLink(savedReportShareUrl(meta.report_id));
      setSaveNotice(
        droppedSummary
          ? `${SAVED_WITHOUT_SUMMARY} ${copied ? "The link is copied. " : ""}Kept until ${utcDate(meta.expires_at)}.`
          : savedConfirmation(meta, withSummary, copied),
      );
      trackEvent("report_saved");
    } catch (e) {
      setExportError(e instanceof Error ? e.message : "Could not save this report.");
    } finally {
      setSaveBusy(false);
    }
  }

  // ── Saved report wiring (Phase 217) ───────────────────────────────────
  const savedEvents = savedReport?.payload.events ?? null;
  const savedCtx = useMemo<SavedReportContextValue | null>(
    () =>
      savedEvents
        ? { deepen: (sourceId: string, hitId: string) => savedDeepen(savedEvents, sourceId, hitId) }
        : null,
    [savedEvents],
  );
  const savedNetwork = useMemo(
    () => (savedEvents ? savedStatementsFrom(savedEvents) : null),
    [savedEvents],
  );
  const savedNarrative = useMemo<ReportExportPayload | null>(
    () =>
      savedReport
        ? { narrative: savedReport.payload.narrative, dispositions: savedReport.payload.dispositions }
        : null,
    [savedReport],
  );
  const saveItem = useMemo(() => {
    if (savedReport || !streamingLei) return undefined;
    if (savedFromRun) {
      const link = savedReportShareUrl(savedFromRun.report_id);
      return {
        label: "Copy saved-report link",
        description: `Saved ${utcDateTime(savedFromRun.saved_at)} · kept until ${utcDate(savedFromRun.expires_at)}`,
        onSelect: (): void => {
          void copyLink(link).then((ok) =>
            setSaveNotice(ok ? "The saved-report link is copied." : link),
          );
        },
      };
    }
    const eligibility = saveEligibility({ streaming, runCompletedAt, retried: retriedSinceRun });
    return {
      label: saveBusy ? "Saving…" : "Save this report",
      description: eligibility.reason ?? "A fixed record of these findings, with a link, kept for 90 days",
      disabled: !eligibility.canSave || saveBusy,
      onSelect: (): void => {
        void saveThisReport();
      },
    };
    // saveThisReport and copyLink read state through their own closures on
    // each render; the memo only has to follow what decides the item.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [savedReport, streamingLei, savedFromRun, streaming, runCompletedAt, retriedSinceRun, saveBusy, exportPayload]);

  /** Forget anything saved from the run on screen — a new run replaces it. */
  function resetForNewRun() {
    setRetriedSinceRun(false);
    setSavedFromRun(null);
    setSaveNotice(null);
  }

  return {
    savedReport,
    setSavedReport,
    savedOpening,
    setSavedOpening,
    savedOpenError,
    setSavedOpenError,
    saveNotice,
    setRetriedSinceRun,
    resetForNewRun,
    saveItem,
    savedCtx,
    savedNetwork,
    savedNarrative,
    exportPayload,
    setExportPayload,
    exportError,
    setExportError,
    pdfBusy,
    mdBusy,
    downloadPdf,
    downloadMarkdown,
  };
}
