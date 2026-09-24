import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { trackEvent } from "./lib/analytics";
import { useMutation, useQuery } from "@tanstack/react-query";
import SearchLoadingGrid from "./components/SearchLoadingGrid";
import {
  savedReportShareUrl,
  fetchSources,
  isValidLei,
  getSavedReport,
  replayLookupEvents,
  retryLookupSource,
  streamLookup,
  SavedReportError,
  type LookupStreamHandlers,
  type BoAccessNotice,
  type RiskSignal,
} from "./lib/api";
import { countLeiConfirmingSources } from "./lib/identifierBadge";
import { rank } from "./components/risk/RiskChip";
import { partitionByKind } from "./lib/signalKind";
import { ChangelogPage } from "./components/ChangelogPage";
import { SubjectCard } from "./components/cdd/SubjectCard";
import { VerdictStrip } from "./components/cdd/VerdictStrip";
import { useIsPhone } from "./lib/viewport";
import { Button } from "./components/ui";
import { leiRegistrationChip, statusChip } from "./lib/subjectProfile";
import PanelSection, { PanelCard } from "./components/ui/PanelSection";
import { setSourceNames } from "./lib/vocab";
import { noRecordSources } from "./lib/lookupProgress";
import { scrollBehavior } from "./lib/motion";
import {
  deepLinkOptions,
  documentTitleFor,
  modeLabel,
  modeParam,
} from "./lib/checkMode";
import {
  modeInSavedReport,
  notInSavedReport,
  openErrorMessage,
  reportIdFromPath,
} from "./lib/savedReport";
import { SavedReportBanner, SavedReportExcluded } from "./components/cdd/SavedReportBanner";
import { SavedReportContext } from "./components/cdd/savedReportContext";
import type { CheckMode } from "./lib/checkMode";
import { SourcesPage } from "./components/SourcesPage";
import BatchPage from "./components/BatchPage";
import WatchlistPage from "./components/WatchlistPage";
import { ApiPage } from "./components/ApiPage";
import { BehindTheScenesPage } from "./components/BehindTheScenesPage";
import { FeaturesPage } from "./components/FeaturesPage";
import {
  BatchInvite,
  ExampleLeiPicker,
  HowItWorks,
} from "./components/HomePanels";
import {
  type SourceBucket,
} from "./components/cdd/SourceBucketCard";
import { EsgPanel } from "./components/cdd/EsgPanel";
import { clearPanelError, mergePanelError, type PanelError } from "./lib/panelErrors";
import {
  PAGE_TITLES,
  VIEW_DOCUMENT_TITLES,
  pathToView,
  viewToPath,
  type View,
} from "./lib/views";
import { DegradedScreensNotice, PanelErrorsNotice } from "./components/cdd/ReportNotices";
import { ModeBlurb, ModeTabs } from "./components/cdd/ModeTabs";
import { PanelBoundary, PanelLoading } from "./components/ui/PanelBoundary";
import { SiteFooter, SiteHeader } from "./components/SiteChrome";
import { SearchPanel } from "./components/SearchPanel";
import { useSearchForm } from "./hooks/useSearchForm";
import { useLookupStream } from "./hooks/useLookupStream";
import { useSavedReport } from "./hooks/useSavedReport";

// FullCheck (enhanced due diligence) view — lazy so Cytoscape/graph code only
// loads when a user switches into FullCheck mode.
const FullCheckPanel = lazy(() => import("./components/cdd/FullCheckPanel"));
const BackgroundCheckPanel = lazy(
  () => import("./components/cdd/BackgroundCheckPanel")
);
const SubsidiariesPanel = lazy(() => import("./components/cdd/SubsidiariesPanel"));
const HistoryPanel = lazy(() => import("./components/cdd/HistoryPanel"));
// Phase 246: QuickCheck is a lazy chunk too, like the five modes beside it.
const QuickCheckPanel = lazy(() => import("./components/cdd/QuickCheckPanel"));
const PersonReportPage = lazy(
  () => import("./components/cdd/PersonReportPage")
);

/** Parse `?person=` + `?person_birth_year=` from a search string. */
function personReportFromSearch(
  search: string
): { name: string; birthYear?: number } | null {
  const params = new URLSearchParams(search);
  const name = (params.get("person") ?? "").trim();
  if (!name) return null;
  const by = Number(params.get("person_birth_year"));
  return {
    name,
    birthYear: Number.isInteger(by) && by >= 1900 && by <= 2100 ? by : undefined,
  };
}

/**
 * OpenCheck — LEI-anchored customer due diligence UI.
 *
 * Workflow:
 *   1. User pastes a Legal Entity Identifier (ISO 17442, 20 chars).
 *   2. Backend hits GLEIF for the canonical record, derives bridge ids
 *      (UK CH number, Wikidata Q-ID), and dispatches to every other
 *      source using whichever identifier they understand.
 *   3. We render a single subject view on top of the unified result.
 */

export default function App() {
  // The search panel's fields and its two GLEIF searches (Phase 246: a hook,
  // because the header, the homepage and `resetToHome` read them too).
  const search = useSearchForm();
  const { nameSearchMutation, nationalIdSearchMutation } = search;

  // The lookup run on screen — every field its events set, and the one
  // handler builder the live stream and a saved report's replay share.
  const stream = useLookupStream();
  const {
    streamingLei,
    legalName,
    subjectJurisdiction,
    hits,
    errors,
    crossSourceLinks,
    possiblySame,
    subjectProfile,
    knowability,
    primaryListing,
    knowabilityChain,
    riskSignals,
    degradedSources,
    verdict,
    sourceLiveness,
    oaScreening,
    graphShape,
    applicableSources,
    completedSources,
    startedSources,
    queuePosition,
    erroredSources,
    answeredApplicable,
    settled,
    streaming,
    bodsCountMap,
    bodsBreakdownMap,
    streamDropped,
    replayedAt,
    runCompletedAt,
    retryingSources,
    cleanupRef,
  } = stream;

  // On mobile, the search inputs collapse once results are on screen (the
  // tab bar stays); this reopens them. Desktop is unaffected.
  const [mobileSearchOpen, setMobileSearchOpen] = useState(false);
  const phoneLayout = useIsPhone();
  /** The identity band is a disclosure, opened by the subject card's
   *  identifier badge. Reset per lookup, like everything else about a
   *  result. */
  const [identityOpen, setIdentityOpen] = useState(false);
  /** Which risk chip the reader selected, if any. Null means "the worst one",
   *  which is what the Risk signals section opens on. Declared with the other
   *  per-entity state because the lookup reset clears it. */
  const [selectedSignalCode, setSelectedSignalCode] = useState<string | null>(null);
  const searchPanelsCollapsed = !!streamingLei && !mobileSearchOpen;
  // Panels that fetch outside `_lookup_pipeline` (/securities, /subsidiaries).
  // Deliberately NOT merged into `degradedSources`: that list arrives on the
  // same event as `signals` and as the backend-built verdict sentence, and the
  // three are provably consistent with each other. Injecting a client-side
  // record would let the coverage count disagree with a sentence that knows
  // nothing about it — and `onRiskSignals` overwrites the list wholesale, so it
  // would be erased anyway. See lib/panelErrors.ts.
  const [panelErrors, setPanelErrors] = useState<PanelError[]>([]);
  // QuickCheck (subject screening, default) vs FullCheck (network EDD) vs
  // BackgroundCheck (screening the people connected to the entity). Reset to
  // QuickCheck on each new lookup so the headline experience is always QuickCheck.
  /**
   * Which check the report is showing. Phase 122 made this the report's
   * top-level structure rather than three cards in the middle of the page,
   * added Climate & ESG as a fourth (it used to render only inside
   * QuickCheck, so it was reachable by scrolling and by nothing else), and
   * put the value in the URL so a shared link opens where you left it.
   */
  const [mode, setMode] = useState<CheckMode>("quick");
  /**
   * `?focus=<statementId>` — a node the FullCheck graph should select once it
   * has it (Phase 200). Set by a board row on the History tab linking to the
   * person it names, and carried into `lookupLei` (never set beside it) so
   * the same link works cold, from a share or a refresh. See the reset in
   * the lookup mutation for why that distinction is the whole feature.
   */
  const [focusStatementId, setFocusStatementId] = useState<string | null>(null);
  // The saved copy on screen, saving the live run, and the exports (Phase 246).
  const saved = useSavedReport({ streamingLei, runCompletedAt, streaming });
  const {
    savedReport,
    setSavedReport,
    savedOpening,
    setSavedOpening,
    savedOpenError,
    setSavedOpenError,
    saveNotice,
    setRetriedSinceRun,
    saveItem,
    savedCtx,
    savedNetwork,
    savedNarrative,
    setExportPayload,
    exportError,
    setExportError,
    pdfBusy,
    mdBusy,
    downloadPdf,
    downloadMarkdown,
  } = saved;
  // Screen-reader announcement for per-source failures and retry outcomes,
  // rendered in the sr-only role="status" region in <main>.
  const [srAnnouncement, setSrAnnouncement] = useState("");

  const [view, setView] = useState<View>(() => pathToView(window.location.pathname));

  /** Navigate to a view, updating the browser URL. */
  function navigate(v: View) {
    const path = viewToPath(v);
    if (window.location.pathname !== path) {
      window.history.pushState({ view: v }, "", path);
    }
    setView(v);
  }

  // Dynamic document title — updates on lookup results and view changes.
  useEffect(() => {
    if (legalName && view === "main") {
      // Hyphen, not em-dash on QuickCheck: matches the server-rendered
      // /entity pages' exact "NAME OF SUBJECT - OpenCheck" template from the
      // SEO ticket. Other modes append a segment (Phase 122) so restored
      // tabs are distinguishable.
      document.title = savedReport
        ? `Saved report: ${documentTitleFor(mode, legalName)}`
        : documentTitleFor(mode, legalName);
    } else {
      document.title = view === "main" ? "OpenCheck" : VIEW_DOCUMENT_TITLES[view];
    }
  }, [legalName, view, mode, savedReport]);

  // Focus management — move focus to #main-content on view changes so keyboard
  // and screen reader users are oriented to the new page content (WCAG 2.4.3).
  // Skipped on the initial mount so it doesn't steal focus from the top of the
  // document (which would pre-empt the skip link).
  const viewFocusMounted = useRef(false);
  useEffect(() => {
    if (!viewFocusMounted.current) {
      viewFocusMounted.current = true;
      return;
    }
    const el = document.getElementById("main-content");
    if (el) el.focus({ preventScroll: true });
  }, [view]);

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchSources(),
  });

  /**
   * Clear everything that belongs to the report on screen, before a new one —
   * a live lookup or a saved report — takes its place. One function for both
   * so a saved report can never inherit a live check's state or the reverse.
   */
  function resetReportState() {
    stream.reset();
    setPanelErrors([]);
    setIdentityOpen(false);
    // A chip selection belongs to the entity it was made on. Carried into
    // the next lookup it either explains a signal the new subject does not
    // have, or — worse — silently lands on a code it does, so the box
    // opens on something nobody chose.
    setSelectedSignalCode(null);
    // The exports belong to the entity that was on screen. A failed PDF
    // left its alert sitting on the *next* subject, describing a download
    // that was never attempted for it — and the payload the report embeds
    // is the previous entity's summary and its analyst's signed decisions.
    setExportError(null);
    setExportPayload({ narrative: null, dispositions: null });
    // Phase 217: anything saved from the run.
    saved.resetForNewRun();
  }

  /**
   * The handlers that turn lookup events into page state. One builder, used
   * by the live stream and by a saved report's replay (Phase 217), so the two
   * cannot set state differently — which is what makes a saved report render
   * as the same report.
   */
  function buildLookupHandlers(cb: {
    onAnchor: (e: { lei: string; legal_name: string | null }) => void;
    onFailure: (detail: string) => void;
  }): LookupStreamHandlers {
    return stream.handlers({
      onAnchor: (e) => {
        setMobileSearchOpen(false); // re-collapse the mobile search inputs
        cb.onAnchor(e);
      },
      onFailure: cb.onFailure,
    });
  }

  /**
   * Open the saved report at `/report/{id}` (Phase 217): fetch the record and
   * replay its stored events through the same handlers a live check drives.
   * Nothing is looked up — the page is exactly what was saved.
   */
  async function openSavedReport(reportId: string, opts?: { mode?: CheckMode }) {
    cleanupRef.current?.();
    cleanupRef.current = null;
    setView("main");
    setSavedOpenError(null);
    setSavedOpening(true);
    resetReportState();
    setSavedReport(null);
    lookupMutation.reset();
    try {
      const report = await getSavedReport(reportId);
      resetReportState();
      setFocusStatementId(null);
      // A tab a saved report does not hold still opens, and says so.
      setMode(opts?.mode ?? "quick");
      search.setLeiInput(report.lei);
      setSavedReport(report);
      setExportPayload({
        narrative: report.payload.narrative,
        dispositions: report.payload.dispositions,
      });
      replayLookupEvents(
        report.payload.events,
        buildLookupHandlers({
          onAnchor: () => {},
          onFailure: (detail) => setSavedOpenError(detail),
        }),
      );
    } catch (e) {
      const status = e instanceof SavedReportError ? e.status : 0;
      setSavedOpenError(
        openErrorMessage(status, e instanceof Error ? e.message : "Could not open this saved report."),
      );
    } finally {
      setSavedOpening(false);
    }
  }

  // ── LEI lookup mutation ───────────────────────────────────────────────────
  // Opens the SSE stream for /lookup-stream. The mutation is considered
  // "pending" (i.e. showing the loading grid) until the backend emits the
  // gleif_done event confirming the entity; all subsequent streaming state
  // (hits, risk signals, cross-source links) is managed via useState below.
  const lookupMutation = useMutation<
    { lei: string; legal_name: string | null },
    Error,
    { lei: string; refresh?: boolean; mode?: CheckMode; focus?: string | null }
  >({
    mutationFn: ({ lei, refresh, mode: startMode, focus: startFocus }) =>
      new Promise((resolve, reject) => {
        if (!isValidLei(lei)) {
          reject(
            new Error(
              "Enter a 20-character ISO 17442 LEI " +
                "(e.g. 213800LH1BZH3DI6G760)."
            )
          );
          return;
        }
        // Reset streaming state before starting a new stream.
        resetReportState();
        setSavedReport(null);
        // `?focus=` rides in with the lookup for the same reason `?mode=`
        // does: this reset runs asynchronously, so a value set *beside* the
        // lookup is set first and wiped here a moment later. Phase 200
        // shipped with exactly that bug — a shared focused link opened the
        // right network with nothing selected. Null unless the caller asked,
        // so an ordinary lookup still clears a stale target: a statement id
        // means something in one subject's network and nothing in another's.
        setFocusStatementId(startFocus ?? null);
        // A new lookup opens on QuickCheck unless the caller asked for a
        // mode. It used to hardcode "quick", and because the mutationFn runs
        // asynchronously it landed *after* the deep-link handler's
        // setMode(parseMode(...)) — so ?mode=full opened on QuickCheck, which
        // is exactly what that handler's comment says must not happen.
        setMode(startMode ?? "quick");

        const cleanup = streamLookup(
          lei,
          buildLookupHandlers({
            onAnchor: resolve,
            onFailure: (detail) => reject(new Error(detail)),
          }),
          5,
          refresh === true,
        );
        cleanupRef.current = cleanup;
      }),
  });

  // ── Person report (Phase E) — URL-addressable via ?person= ─────────
  const [personReport, setPersonReport] = useState<
    { name: string; birthYear?: number } | null
  >(() => personReportFromSearch(window.location.search));

  /** Open the person report page, reflected in the URL for sharing. */
  function openPersonReport(name: string, birthYear?: number) {
    const url = new URL(window.location.href);
    url.searchParams.set("person", name);
    if (birthYear) url.searchParams.set("person_birth_year", String(birthYear));
    else url.searchParams.delete("person_birth_year");
    window.history.pushState({}, "", url);
    setPersonReport({ name, birthYear });
    window.scrollTo({ top: 0 });
  }

  /** Close the person report and drop its URL params. */
  function closePersonReport() {
    const url = new URL(window.location.href);
    url.searchParams.delete("person");
    url.searchParams.delete("person_birth_year");
    window.history.pushState({}, "", url);
    setPersonReport(null);
  }

  function lookupLei(
    rawLei: string,
    opts?: { refresh?: boolean; mode?: CheckMode; focus?: string | null }
  ) {
    const lei = rawLei.trim().toUpperCase();
    search.setLeiInput(lei);
    setView("main");
    // Leaving a saved report for a live check: the address must stop saying
    // /report/{id}, or a refresh would reopen the record, not the check.
    setSavedReport(null);
    setSavedOpenError(null);
    trackEvent("lookup_run"); // feature event only — the LEI is never recorded
    // Shareable URLs: reflect the lookup in ?lei= so refresh and copy/paste
    // re-run it (the backend replay cache makes repeats near-instant).
    const url = new URL(window.location.href);
    if (reportIdFromPath(url.pathname)) {
      url.pathname = "/";
      url.search = "";
    }
    if (url.pathname !== window.location.pathname || url.searchParams.get("lei") !== lei) {
      url.searchParams.set("lei", lei);
      window.history.pushState({}, "", url);
    }
    // A lookup started from the search box always opens on QuickCheck; a
    // deep link with ?mode= is honoured by the popstate/first-load effect
    // below instead, which runs after this.
    // Cancel any in-flight stream before starting a new one.
    cleanupRef.current?.();
    cleanupRef.current = null;
    lookupMutation.mutate({
      lei,
      refresh: opts?.refresh,
      mode: opts?.mode,
      focus: opts?.focus,
    });
  }

  /**
   * Switch check mode. The single entry point, because three things have to
   * happen together and v1 did none of them: the value goes into `?mode=`
   * so a shared link and a refresh land where you left off; focus moves to
   * the new panel, since switching unmounts most of the page and focus
   * would otherwise drop to <body>; and the analytics event fires once per
   * actual change rather than on every click of an already-active tab.
   */
  const selectMode = useCallback((next: CheckMode, opts: { focusPanel?: boolean } = {}) => {
    // Phase 241: the tablist's arrow keys keep focus on the tab they moved
    // to (`focusPanel: false`). Moving it into the panel on the next frame
    // left the following arrow press on the panel, where it did nothing.
    const focusPanel = opts.focusPanel ?? true;
    setMode((current) => {
      if (current === next) return current;
      if (next === "full") trackEvent("fullcheck_run");
      if (next === "background") trackEvent("backgroundcheck_run");
      if (next === "subsidiaries") trackEvent("subsidiaries_run");
      const url = new URL(window.location.href);
      const param = modeParam(next);
      if (param === null) url.searchParams.delete("mode");
      else url.searchParams.set("mode", param);
      window.history.replaceState({}, "", url);
      // After the panel swaps in. requestAnimationFrame rather than a
      // timeout so it lands on the next paint whatever the render cost.
      if (focusPanel) {
        requestAnimationFrame(() => {
          document.getElementById(`panel-${next}`)?.focus({ preventScroll: true });
        });
      }
      return next;
    });
  }, []);

  /**
   * Open the FullCheck network with a statement selected (Phase 200).
   *
   * The History tab names people the graph draws, but the two are different
   * modes and only one is mounted at a time, so "show me this person" is a
   * mode switch — not an in-tab interaction. The id goes into `?focus=`
   * before the switch so the address describes what is on screen, which is
   * the same contract `?mode=` has: the view can be shared and refreshed.
   *
   * It stays in the URL afterwards. A statement id is only meaningful within
   * one subject's network, and a new lookup clears it; within this one it
   * keeps naming the same person, so re-opening FullCheck lands where the
   * address says it will.
   */
  const focusPersonInNetwork = useCallback(
    (statementId: string) => {
      const url = new URL(window.location.href);
      url.searchParams.set("focus", statementId);
      window.history.replaceState({}, "", url);
      setFocusStatementId(statementId);
      selectMode("full");
    },
    [selectMode],
  );

  // Move focus to #main-content when an action unmounts the focused element
  // (e.g. picking a search result resets the picker) — without this, focus
  // drops to <body> for keyboard and screen reader users. The [view] effect
  // above only covers actual view changes; these paths stay on "main".
  function focusMain() {
    document.getElementById("main-content")?.focus({ preventScroll: true });
  }

  // On first load and on back/forward navigation, honour ?lei= in the URL.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    const fromUrl = (q: string | null) => (q ?? "").trim().toUpperCase();
    // Phase 88: /entity/{LEI}-{slug} is normally served by the backend as a
    // server-rendered page. If the SPA receives it (split-deploy fallback, or
    // a stale deep link), treat it as a deep link: normalise to the app's
    // canonical ?lei= form and run the check the visitor asked for. The ?lei=
    // read below then re-reads the rewritten URL, so a single lookup fires.
    const entityMatch = window.location.pathname.match(/^\/entity\/([0-9A-Za-z]{20})(?:-|\/|$)/);
    if (entityMatch && isValidLei(entityMatch[1].toUpperCase())) {
      window.history.replaceState({}, "", `/?lei=${entityMatch[1].toUpperCase()}`);
    }
    // Phase 217: /report/{id} is a saved report — a record, not a lookup.
    const savedId = reportIdFromPath(window.location.pathname);
    if (savedId) {
      void openSavedReport(savedId, deepLinkOptions(window.location.search));
    }
    const initial = savedId ? "" : fromUrl(new URLSearchParams(window.location.search).get("lei"));
    if (initial && isValidLei(initial)) {
      // The mode goes *into* the lookup rather than being set beside it: the
      // mutationFn's own reset runs later and would otherwise overwrite it.
      lookupLei(initial, deepLinkOptions(window.location.search));
    }

    const onPopState = () => {
      const poppedReport = reportIdFromPath(window.location.pathname);
      if (poppedReport) {
        void openSavedReport(poppedReport, deepLinkOptions(window.location.search));
        return;
      }
      // Handle non-main path views first (back/forward to /sources, /about etc.)
      const v = pathToView(window.location.pathname);
      if (v !== "main") {
        setView(v);
        return;
      }
      // Person report (Phase E) — ?person= takes render precedence; the
      // entity state underneath is left untouched so back/forward between
      // the two is instant.
      const person = personReportFromSearch(window.location.search);
      setPersonReport(person);
      if (person) return;
      // Back on main — honour ?lei= if present, otherwise clear results.
      const lei = fromUrl(new URLSearchParams(window.location.search).get("lei"));
      if (lei && isValidLei(lei)) {
        lookupLei(lei, deepLinkOptions(window.location.search));
      } else {
        // Navigated back to the landing page — clear the result view.
        cleanupRef.current?.();
        cleanupRef.current = null;
        setSavedReport(null);
        setSavedOpenError(null);
        stream.reset();
        lookupMutation.reset();
        setView("main");
      }
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  // Announce per-source failures to screen readers — once per count change,
  // not once per SSE event, so a burst of source_error events yields a single
  // summary rather than a stream of announcements.
  const errorCount = Object.keys(errors).length;
  const prevErrorCountRef = useRef(0);
  useEffect(() => {
    if (errorCount > prevErrorCountRef.current) {
      setSrAnnouncement(
        `${errorCount} source${errorCount === 1 ? "" : "s"} could not be queried — retry buttons are available below.`
      );
    }
    prevErrorCountRef.current = errorCount;
  }, [errorCount]);

  /** Re-run a single failed source via /lookup-source (per-source retry). */
  async function retrySource(sourceId: string) {
    if (!streamingLei) return;
    stream.setRetryingSources((prev) => new Set([...prev, sourceId]));
    // The page no longer shows one run once a source is re-run on its own,
    // so it can no longer be saved as one (the server has dropped the run
    // from its replay cache too).
    setRetriedSinceRun(true);
    const sourceName = sourceNameIndex[sourceId] ?? sourceId;
    try {
      const res = await retryLookupSource(streamingLei, sourceId);
      if (res.error) {
        stream.setErrors((prev) => ({ ...prev, [sourceId]: res.error as string }));
        setSrAnnouncement(`${sourceName} retry failed.`);
      } else {
        stream.setErrors((prev) => {
          const next = { ...prev };
          delete next[sourceId];
          return next;
        });
        stream.setHits((prev) => [
          ...prev.filter((h) => h.source_id !== sourceId),
          ...res.hits,
        ]);
        setSrAnnouncement(`${sourceName} retried successfully.`);
      }
    } catch (e) {
      stream.setErrors((prev) => ({
        ...prev,
        [sourceId]: e instanceof Error ? e.message : String(e),
      }));
      setSrAnnouncement(`${sourceName} retry failed.`);
    } finally {
      stream.setRetryingSources((prev) => {
        const next = new Set(prev);
        next.delete(sourceId);
        return next;
      });
    }
  }

  /**
   * Search GLEIF by company name using the public REST API.
   * On success, nameSearchMutation.data is populated for the user to pick from.
   * After selection the standard lookupLei flow takes over.
   */
  /**
   * The header field. A pasted LEI runs the lookup directly; anything else is
   * a company-name search, which means opening the panel that shows the
   * results to pick from — the header has no room to render them, and a
   * search whose results appear nowhere is worse than no search field.
   */
  function searchFromHeader(query: string) {
    const q = query.trim();
    if (!q) return;
    // The header is on every page, so the search has to *get to* the page that
    // can show a result. Without this it fired a real GLEIF request from
    // /sources and rendered the answer nowhere, and from a person report it
    // ran a whole 39-source lookup behind a screen that never changed —
    // leaving `?person=…&lei=…` in the URL as a permanently stuck link.
    showEntitySearchSurface();
    if (isValidLei(q.toUpperCase())) {
      lookupLei(q);
      return;
    }
    search.setSearchMode("name");
    setMobileSearchOpen(true);
    search.setNameQuery(q);
    nameSearchMutation.mutate(q);
    // The results render in the panel this just opened, so the page follows.
    requestAnimationFrame(() => {
      document
        .getElementById("panel-name")
        ?.scrollIntoView({ behavior: scrollBehavior(), block: "start" });
    });
  }

  /** Leave whatever page the header was clicked from and show the search
   *  surface, clearing a person report and its `?person=` parameter. */
  function showEntitySearchSurface() {
    // `navigate`, not `setView` — the path has to change too, or the reader
    // stays on /about with a search result rendered under it and a URL that
    // reloads back to the page they left.
    navigate("main");
    if (personReport) {
      setPersonReport(null);
      const url = new URL(window.location.href);
      url.searchParams.delete("person");
      url.searchParams.delete("person_birth_year");
      window.history.replaceState({}, "", url);
    }
  }

  // Build a set of source IDs that are categorised as ESG.
  const esgSourceIds = useMemo<Set<string>>(() => {
    if (!sourcesQuery.data) return new Set();
    return new Set(
      sourcesQuery.data.sources
        .filter((s) => s.category === "esg")
        .map((s) => s.id)
    );
  }, [sourcesQuery.data]);

  // source_id → display name, shared by the bucket cards and the
  // cross-source identifier chips (which scroll to the matching card).
  const sourceNameIndex = useMemo<Record<string, string>>(
    () =>
      sourcesQuery.data
        ? Object.fromEntries(sourcesQuery.data.sources.map((s) => [s.id, s.name]))
        : {},
    [sourcesQuery.data]
  );

  // Publish it, so a component holding a source id and no map still says the
  // registry's name rather than a prettified slug ("Opensanctions"). Threading
  // the prop reaches most call sites and missed the risk chips, the ESG cards
  // and the source legend; those read the published map instead.
  useEffect(() => {
    setSourceNames(sourceNameIndex);
  }, [sourceNameIndex]);

  // Group hits by source_id for the per-source bucket cards.
  // Built progressively from streaming hits — updates on every onHit / onSourceError.
  const bucketList = useMemo<SourceBucket[]>(() => {
    if (!streamingLei) return [];
    const byId = new Map<string, SourceBucket>();
    const adapterIndex: Record<string, string> = sourcesQuery.data
      ? Object.fromEntries(
          sourcesQuery.data.sources.map((s) => [s.id, s.name])
        )
      : {};
    const boAccessIndex: Record<string, BoAccessNotice | null> = sourcesQuery.data
      ? Object.fromEntries(
          sourcesQuery.data.sources.map((s) => [s.id, s.bo_access ?? null])
        )
      : {};
    for (const hit of hits) {
      const existing = byId.get(hit.source_id);
      if (existing) {
        existing.hits.push(hit);
      } else {
        byId.set(hit.source_id, {
          sourceId: hit.source_id,
          sourceName: adapterIndex[hit.source_id] ?? hit.source_id,
          hits: [hit],
          error: errors[hit.source_id],
          boAccess: boAccessIndex[hit.source_id] ?? null,
        });
      }
    }
    // Surface adapters that errored even when they returned no hits.
    for (const [source_id, errMsg] of Object.entries(errors)) {
      if (!byId.has(source_id)) {
        byId.set(source_id, {
          sourceId: source_id,
          sourceName: adapterIndex[source_id] ?? source_id,
          hits: [],
          error: errMsg,
          boAccess: boAccessIndex[source_id] ?? null,
        });
      }
    }
    return Array.from(byId.values());
  }, [streamingLei, hits, errors, sourcesQuery.data]);

  // Partition into CDD and ESG buckets.
  const cddBuckets = useMemo(
    () => bucketList.filter((b) => !esgSourceIds.has(b.sourceId)),
    [bucketList, esgSourceIds]
  );
  const esgBuckets = useMemo(
    () => bucketList.filter((b) => esgSourceIds.has(b.sourceId)),
    [bucketList, esgSourceIds]
  );

  const totalHits = cddBuckets.reduce((n, b) => n + b.hits.length, 0);

  // Distinct sources that independently publish the subject's LEI — the
  // SubjectCard badge number. Scoped to the LEI because the badge renders
  // beside it; see countLeiConfirmingSources for the rationale.
  const leiConfirmedSourceCount = useMemo(
    () =>
      streamingLei
        ? countLeiConfirmingSources(crossSourceLinks, streamingLei)
        : 0,
    [crossSourceLinks, streamingLei],
  );

  /** SubjectCard badge action: open the identity band, scroll to it and flash
   *  it (the same affordance narrative citations use).
   *
   *  The band is a disclosure and starts shut. That is not a return to v1's
   *  two collapsed boxes — it is one band answering one question, and the
   *  badge is what opens it, which is the moment a reader is actually asking.
   *  Corroboration that occupies a screen before anyone doubted anything sits
   *  between the reader and the finding. */
  const showCrossSourceIdentifiers = () => {
    // The band renders only in QuickCheck, so from any other mode this
    // control did nothing at all — a badge that looks like a link and
    // silently ignores the click. Switch first, then scroll on the next
    // frame, once the panel it points at exists.
    if (mode !== "quick") {
      selectMode("quick");
      requestAnimationFrame(() => requestAnimationFrame(flashIdentityBand));
      return;
    }
    flashIdentityBand();
  };

  /** Reopen the full search panel from the header (Phase 245). Focus has to
   *  be moved deliberately: the control that calls this may unmount itself,
   *  and dropping focus to <body> strands the keyboard entry path. */
  const openSearchPanel = () => {
    setMobileSearchOpen(true);
    requestAnimationFrame(() => {
      window.scrollTo({ top: 0, behavior: scrollBehavior() });
      document
        .querySelector<HTMLElement>(
          // Scoped to the search tablist: the mode tabs (QuickCheck and
          // friends) are role="tab" too, and one of those is always selected.
          '[role="tablist"][aria-label="Search method"] [role="tab"][aria-selected="true"]'
        )
        ?.focus();
    });
  };

  /** Scroll to and focus an element by id, once it exists (Phase 245). */
  const goToElement = (id: string) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.scrollIntoView({ behavior: scrollBehavior(), block: "start" });
    if (el.tabIndex < 0) el.tabIndex = -1;
    el.focus({ preventScroll: true });
  };

  /** The verdict strip's count links here rather than repeating the chips
   *  (Phase 245). Risk signals is a QuickCheck section, so switch first. */
  const showRiskSignals = () => {
    if (mode !== "quick") {
      selectMode("quick", { focusPanel: false });
      requestAnimationFrame(() => requestAnimationFrame(() => goToElement("risk-signals")));
      return;
    }
    goToElement("risk-signals");
  };

  const flashIdentityBand = () => {
    setIdentityOpen(true);
    const el = document.getElementById("cross-source-identifiers");
    if (el) {
      el.scrollIntoView({ behavior: scrollBehavior(), block: "start" });
      if (el.tabIndex < 0) el.tabIndex = -1;
      el.focus({ preventScroll: true });
      el.classList.add("oc-cite-flash");
      window.setTimeout(() => el.classList.remove("oc-cite-flash"), 1600);
    }
  };

  // Distinct codes — used for the top-level summary chip strip.
  const aggregatedCodes = useMemo(() => {
    const seen = new Map<string, RiskSignal>();
    for (const sig of riskSignals) {
      const existing = seen.get(sig.code);
      if (!existing || rank(sig.confidence) > rank(existing.confidence)) {
        seen.set(sig.code, sig);
      }
    }
    return Array.from(seen.values());
  }, [riskSignals]);

  // Risk findings vs structural context. The backend classifies each signal
  // with `kind`, so this split is not a hand-kept list of exceptions here —
  // the results page, the OG share card and the share-page meta description
  // all read the same field and cannot drift apart. A missing `kind` means
  // "risk", so cached responses predating the field behave as before.
  const [riskCodes, contextCodes] = useMemo(
    () => partitionByKind(aggregatedCodes),
    [aggregatedCodes],
  );

  // Phase 241: every applicable source is named on the report. One that
  // answered with nothing produces no card, so it gets a chip instead.
  const answeredNoRecord = useMemo(
    () =>
      noRecordSources(
        applicableSources,
        completedSources,
        erroredSources,
        new Set(bucketList.map((b) => b.sourceId))
      ),
    [applicableSources, completedSources, erroredSources, bucketList]
  );

  // Sources that are announced (sources_applicable) but not yet completed —
  // used to render skeleton placeholder cards while they are in flight.
  const pendingCddSources = useMemo(
    () => applicableSources.filter((id) => !completedSources.has(id) && !esgSourceIds.has(id)),
    [applicableSources, completedSources, esgSourceIds],
  );
  const pendingEsgSources = useMemo(
    () => applicableSources.filter((id) => !completedSources.has(id) && esgSourceIds.has(id)),
    [applicableSources, completedSources, esgSourceIds],
  );

  /**
   * Back to a fresh homepage. Extracted from the logo button in Phase 122
   * so the nav's "Search" item and the wordmark cannot drift apart — the
   * reset is thirty setters long, and a second copy would have gone stale
   * the first time state was added to it.
   */
  const resetToHome = useCallback(() => {
    cleanupRef.current?.();
    cleanupRef.current = null;
    navigate("main");
    setSavedReport(null);
    setSavedOpenError(null);
    setSavedOpening(false);
    saved.resetForNewRun();
    // The whole run (Phase 246: one `stream.reset()` rather than the thirty
    // setters that stood here, where a new field was exactly what got missed
    // — Phase 124 had to add two by hand).
    stream.reset();
    setPanelErrors([]);
    setIdentityOpen(false);
    setExportError(null);
    setExportPayload({ narrative: null, dispositions: null });
    lookupMutation.reset();
    search.reset();
    // Clear ?lei= so the address bar returns to a clean homepage URL.
    if (window.location.search || window.location.pathname !== "/") {
      const url = new URL(window.location.href);
      url.search = "";
      window.history.pushState({}, "", url);
    }
    // Every setter here is a stable useState setter and the mutations are
    // stable for the component's life, so the empty dep list is correct.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const runLiveFromSaved = useCallback(() => {
    if (savedReport) lookupLei(savedReport.lei);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [savedReport]);

  const HeroHeading = streamingLei ? "h2" : "h1";

  // ── Phase 245: the report's top, as pieces the two layouts arrange ──
  // `phoneLayout` changes the ORDER of the subject, the mode tabs and the
  // verdict (see where they render); Tailwind restyles, it cannot reorder
  // without splitting focus order from visual order.
  const verdictStrip = (
    <VerdictStrip
      verdict={verdict}
      riskSignals={riskCodes}
      contextSignals={contextCodes}
      degraded={degradedSources}
      sourcesAnswered={answeredApplicable}
      sourcesApplicable={applicableSources.length}
      registryTotal={sourcesQuery.data?.sources.length ?? null}
      jurisdiction={subjectJurisdiction}
      knowability={knowability}
      graphShape={graphShape}
      // Not on the FullCheck tab: an invitation to the page the reader is on.
      onOpenNetwork={mode === "full" ? undefined : () => selectMode("full")}
      onShowSignals={showRiskSignals}
      onShowDegraded={() => goToElement("screening-incomplete")}
      screening={streaming}
      saved={Boolean(savedReport)}
    />
  );

  const modeTabs = <ModeTabs mode={mode} onSelect={selectMode} />;

  return (
    <SavedReportContext.Provider value={savedCtx}>
    <div className="min-h-screen flex flex-col bg-oo-bg">
      {/* Skip-to-content link — visually hidden until focused (WCAG 2.4.1) */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-50 focus:px-4 focus:py-2 focus:bg-oo-blue focus:text-white focus:rounded focus:font-medium"
      >
        Skip to main content
      </a>
      <SiteHeader
        view={view}
        onNavigate={navigate}
        onHome={resetToHome}
        searchPanelsCollapsed={searchPanelsCollapsed}
        onOpenSearchPanel={openSearchPanel}
        onSearch={searchFromHeader}
      />

      <main
        id="main-content"
        role="main"
        tabIndex={-1}
        style={{ outline: "none" }}
        className="flex-1 px-4 sm:px-10 lg:px-16 py-5 sm:py-6 max-w-oo-page mx-auto w-full"
      >
        {/* One <h1> per page, and never two (Phase 168).
            The main view has had one all along — `HeroHeading` on the
            homepage, the sr-only report heading below once a lookup is on
            screen — but /sources, /about, /api and /changelog each had a
            page full of <h2>s under no <h1> at all, which the first run of
            the Playwright smoke found and no unit test could. `sr-only`
            because the design gives those pages an eyebrow rather than a
            visible title: that is a design decision, and "no page title in
            the outline" was not one. */}
        {view !== "main" && view !== "batch" && view !== "watchlist" && (
          <h1 className="sr-only">{PAGE_TITLES[view]}</h1>
        )}

        {/* Screen-reader live region — announces streaming lookup progress */}
        <div aria-live="polite" aria-atomic="false" className="sr-only">
          {lookupMutation.isPending && "Looking up entity, please wait…"}
          {streaming && legalName && `Loading results for ${legalName}…`}
          {streamingLei && !streaming && legalName && `Lookup complete for ${legalName}. ${totalHits} result${totalHits === 1 ? "" : "s"} found.`}
        </div>
        {/* Announces per-source failures and retry outcomes */}
        <div role="status" className="sr-only">
          {srAnnouncement}
        </div>
        {view === "main" && personReport && (
          <Suspense
            fallback={
              <p className="text-oo-small text-oo-muted italic">
                Loading person report…
              </p>
            }
          >
            <PersonReportPage
              name={personReport.name}
              birthYear={personReport.birthYear}
              onBack={closePersonReport}
            />
          </Suspense>
        )}
        {view === "main" && !personReport && (
        <>
        {/* ── Hero — homepage only (Phase 122) ───────────────────────────
            A report is not a landing page. v1 kept the hero and the
            four-tab search panel above every result, so the first thing an
            analyst read on a due-diligence report was marketing copy about
            three million companies, and the subject came fourth. On results
            the hero is gone and the panel collapses to a single "search for
            a different entity" row. */}
        {!streamingLei && (
        <div className="mb-3">
          <HeroHeading className="font-head font-bold text-oo-ink leading-tight text-[20px] sm:text-oo-display">
            Conduct due diligence on <span className="text-oo-blue">3 million</span> companies, starting from a single ID
          </HeroHeading>
          <p className="text-oo-small sm:text-sm text-oo-muted leading-snug mt-2">
            With a Legal Entity Identifier, OpenCheck pulls open corporate data from 49 sources into one graph using the Beneficial Ownership Data Standard
          </p>
        </div>
        )}
        <SearchPanel
          search={search}
          collapsed={searchPanelsCollapsed}
          onExpand={() => setMobileSearchOpen(true)}
          lookupPending={lookupMutation.isPending}
          onLookup={lookupLei}
          onOpenPerson={openPersonReport}
          onPicked={focusMain}
        />

        {/* No aria-live here — the role="alert" child announces itself */}
        <div>
          {lookupMutation.isError && (
            <div role="alert" className="mb-6 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
              {lookupMutation.error?.message}
            </div>
          )}
        </div>

        {(lookupMutation.isPending || streaming) && (
          <SearchLoadingGrid
            sources={sourcesQuery.data?.sources ?? []}
            anchored={!!streamingLei}
            applicable={applicableSources}
            started={startedSources}
            completed={completedSources}
            errored={erroredSources}
            queuePosition={queuePosition}
          />
        )}

        {savedOpening && (
          <p role="status" className="mb-6 text-oo-small text-oo-muted">
            Opening the saved report…
          </p>
        )}

        {savedOpenError && !savedOpening && (
          <div role="alert" className="mb-6 rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3">
            <p className="text-oo-small text-oo-warn-text">{savedOpenError}</p>
            <button
              type="button"
              onClick={resetToHome}
              className="mt-2 text-oo-small font-semibold text-oo-blue underline underline-offset-2"
            >
              Go to the homepage
            </button>
          </div>
        )}

        {!streamingLei && !savedOpening && !savedOpenError && !lookupMutation.isPending && !streaming && !lookupMutation.isError && !nameSearchMutation.data && !nameSearchMutation.isPending && !nationalIdSearchMutation.data && !nationalIdSearchMutation.isPending && (
          <>
            <ExampleLeiPicker onPick={lookupLei} disabled={lookupMutation.isPending || streaming} />
            <BatchInvite onOpen={() => navigate("batch")} />
            <HowItWorks />
          </>
        )}

        {streamDropped && streamingLei && (
          <div
            role="alert"
            className="mb-6 flex flex-wrap items-center justify-between gap-3 rounded-oo border border-amber-300 bg-amber-50 px-4 py-3"
          >
            <p className="text-oo-small leading-[1.6] text-amber-900">
              <span className="font-medium">Connection lost mid-lookup.</span>{" "}
              Showing partial results for {legalName ?? streamingLei}.
            </p>
            <Button variant="warn" size="sm" className="shrink-0" onClick={() => lookupLei(streamingLei)}>
              Resume lookup
            </Button>
          </div>
        )}

        {streamingLei && (
          <h1 className="sr-only">
            {savedReport ? "Saved due diligence report" : "Due diligence report"}:{" "}
            {legalName ?? streamingLei}
          </h1>
        )}

        {savedReport && streamingLei && (
          <SavedReportBanner
            meta={savedReport}
            runCompletedAt={savedReport.payload.run_completed_at}
            onRunLive={runLiveFromSaved}
            onExtended={(next) => setSavedReport((cur) => (cur ? { ...cur, ...next } : cur))}
          />
        )}

        {streamingLei && (
          <div className="mb-6 rounded-oo border border-oo-rule bg-white">
          <SubjectCard
            lei={streamingLei}
            legalName={legalName}
            jurisdiction={subjectJurisdiction}
            replayedAt={savedReport ? null : replayedAt}
            onRefresh={savedReport ? undefined : () => lookupLei(streamingLei, { refresh: true })}
            identifierSources={leiConfirmedSourceCount}
            onShowIdentifiers={showCrossSourceIdentifiers}
            status={statusChip(subjectProfile, sourceNameIndex)}
            leiRegistration={leiRegistrationChip(subjectProfile?.lei_registration)}
            pdfBusy={pdfBusy}
            mdBusy={mdBusy}
            onPdf={downloadPdf}
            onMarkdown={downloadMarkdown}
            exportError={exportError}
            onOpenWatchlist={() => navigate("watchlist")}
            savedShare={
              savedReport
                ? { url: savedReportShareUrl(savedReport.report_id) }
                : undefined
            }
            save={saveItem}
            notice={saveNotice}
            listing={primaryListing}
          />
        {/* ── The answer-first layer (Phase 122) ─────────────────────────
            Subject, then what the check found and how much of it ran, then
            the evidence. Both halves render from the same risk_signals
            event as each other and as the sentence, so a signal count and
            the count of screens that failed cannot drift apart on screen.
            One card with the subject (Phase 126): the verdict is what the
            subject amounts to, not a separate object floating beneath it. */}
          {!phoneLayout && verdictStrip}
          </div>
        )}

        {/* ── Phones (Phase 245): tabs directly under the subject ─────────
            At 390px the verdict strip is ~800px tall, so the mode tabs began
            ~1,300px down Shell's report — a reader had to scroll past every
            column to learn there were five other views. On a phone the
            order is subject, tabs, verdict. Rendered in that order, not
            reordered with CSS: focus and reading order follow the DOM, and
            they must match what the eye sees at every width. */}
        {streamingLei && phoneLayout && (
          <>
            {modeTabs}
            <div className="mb-3 rounded-oo border border-oo-rule bg-white">{verdictStrip}</div>
          </>
        )}

        {/* ── Modes as the report's structure (Phase 122) ─────────────
              Was three equal-weight cards in the middle of the page, chosen
              after the subject and forgotten on the next lookup. Now a
              tablist: the subject and verdict above stay put across a
              switch, the choice is in the URL, and each tab carries the
              `oo.node.*` accent that already names its badge — so the tab,
              the badge and (for ownership and role) the graph edge are one
              colour rather than three.

              Climate & ESG sits after a divider because it is a different
              question, not a fourth depth of check. It used to render as a
              section inside QuickCheck, reachable by scrolling and by
              nothing else. */}
        {/* Phase 245: from `sm` up the tabs sit under the subject-and-verdict
            card; on a phone they are rendered above the verdict instead
            (see `phoneLayout`), so the mode choice is on the first screen. */}
        {streamingLei && !phoneLayout && modeTabs}

        {/* Entity-scoped panels (risk signals, cross-source identifiers,
            possibly-same pairs) are hidden in BackgroundCheck mode — that
            view is about the connected people, not the subject entity. The
            screening-degradation notice stays: it reports related-party
            screening gaps, which are exactly about people.
            The AI summary box is additionally hidden in FullCheck mode: it
            sits above the network map and pushed the FullCheck content
            (the actual reason for switching modes) far down the page. */}
        {/* Screening-degradation detail (issue #50) — the per-check list,
            with the affected signals named. Phase 122 moved it ABOVE the AI
            summary: it used to render after it, so a reader reached a
            conclusion before learning which screens had not run. The
            headline count now sits higher still, in the verdict strip; this
            is the detail behind it, and it stays independent of the risk
            panel because zero signals with a degraded screen is exactly the
            case that must not read as a clean screen. */}
        {degradedSources.length > 0 && (
          <DegradedScreensNotice
            degraded={degradedSources}
            sourceNames={sourceNameIndex}
            onRetry={
              streamingLei && !streaming && !savedReport
                ? () => lookupLei(streamingLei, { refresh: true })
                : undefined
            }
          />
        )}

        {streamingLei && panelErrors.length > 0 && <PanelErrorsNotice errors={panelErrors} />}

        {/* Each mode's content is a labelled tabpanel with tabIndex={-1},
            so selectMode can move focus into it after the switch. Without
            that, switching tab unmounts most of the page and focus falls to
            <body> — v1's mode cards did exactly that. */}
        {savedReport && streamingLei && !modeInSavedReport(mode) ? (
          <div id={`panel-${mode}`} role="tabpanel" aria-labelledby={`tab-${mode}`} tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode={mode} />
              <SavedReportExcluded
                sentence={notInSavedReport(modeLabel(mode))}
                onRunLive={runLiveFromSaved}
              />
            </PanelCard>
          </div>
        ) : mode === "full" && streamingLei ? (
          <div id="panel-full" role="tabpanel" aria-labelledby="tab-full" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="full" />
              <PanelBoundary label="FullCheck">
                <Suspense fallback={<PanelLoading label="FullCheck" />}>
                  <FullCheckPanel
                    lei={streamingLei}
                    legalName={legalName}
                    signals={riskSignals}
                    focusStatementId={focusStatementId}
                    savedStatements={savedNetwork}
                    knowabilityChain={knowabilityChain}
                    onOpenSubsidiaries={() => selectMode("subsidiaries")}
                    onPanelError={(e) => setPanelErrors((prev) => mergePanelError(prev, e))}
                    onPanelRecovered={(panel) =>
                      setPanelErrors((prev) => clearPanelError(prev, panel))
                    }
                  />
                </Suspense>
              </PanelBoundary>
            </PanelCard>
          </div>
        ) : mode === "background" && streamingLei ? (
          <div id="panel-background" role="tabpanel" aria-labelledby="tab-background" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="background" />
              <PanelBoundary label="BackgroundCheck">
                <Suspense fallback={<PanelLoading label="BackgroundCheck" />}>
                  <BackgroundCheckPanel
                    lei={streamingLei}
                    legalName={legalName}
                    onOpenReport={openPersonReport}
                  />
                </Suspense>
              </PanelBoundary>
            </PanelCard>
          </div>
        ) : mode === "subsidiaries" && streamingLei ? (
          <div id="panel-subsidiaries" role="tabpanel" aria-labelledby="tab-subsidiaries" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="subsidiaries" />
              <PanelBoundary label="Subsidiaries">
                <Suspense fallback={<PanelLoading label="Subsidiaries" />}>
                  <SubsidiariesPanel
                    lei={streamingLei}
                    legalName={legalName}
                    signals={riskSignals}
                    onPanelError={(e) => setPanelErrors((prev) => mergePanelError(prev, e))}
                    onPanelRecovered={(panel) =>
                      setPanelErrors((prev) => clearPanelError(prev, panel))
                    }
                  />
                </Suspense>
              </PanelBoundary>
            </PanelCard>
          </div>
        ) : mode === "history" && streamingLei ? (
          <div id="panel-history" role="tabpanel" aria-labelledby="tab-history" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="history" />
              <PanelBoundary label="History">
                <Suspense fallback={<PanelLoading label="History" />}>
                  <HistoryPanel
                    lei={streamingLei}
                    legalName={legalName}
                    onFocusPerson={focusPersonInNetwork}
                    onPanelError={(e) => setPanelErrors((prev) => mergePanelError(prev, e))}
                    onPanelRecovered={(panel) =>
                      setPanelErrors((prev) => clearPanelError(prev, panel))
                    }
                  />
                </Suspense>
              </PanelBoundary>
            </PanelCard>
          </div>
        ) : mode === "esg" && streamingLei ? (
          <div id="panel-esg" role="tabpanel" aria-labelledby="tab-esg" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="esg" />
              {esgBuckets.length > 0 || pendingEsgSources.length > 0 ? (
                <EsgPanel
                  buckets={esgBuckets}
                  pendingCount={pendingEsgSources.length}
                  bodsCountMap={bodsCountMap}
                  bodsBreakdownMap={bodsBreakdownMap}
                  onPanelError={(e) => setPanelErrors((prev) => mergePanelError(prev, e))}
                  onRecovered={(panel) =>
                    setPanelErrors((prev) => clearPanelError(prev, panel))
                  }
                  onOpenSubsidiaries={() => selectMode("subsidiaries")}
                />
              ) : (
                <PanelSection>
                  <p className="text-oo-small text-oo-muted">
                    {streaming
                      ? "Checking the climate and extractives sources…"
                      : "No emissions or asset records were published about this company by the sources checked. That is an absence of records, not a finding about its emissions."}
                  </p>
                </PanelSection>
              )}
            </PanelCard>
          </div>
        ) : (
          <div id="panel-quick" role="tabpanel" aria-labelledby="tab-quick" tabIndex={-1}>
          {streamingLei && (
            <PanelBoundary label="QuickCheck">
              <Suspense
                fallback={
                  <PanelCard>
                    <PanelLoading label="QuickCheck" />
                  </PanelCard>
                }
              >
                <QuickCheckPanel
                  lei={streamingLei}
                  legalName={legalName}
                  jurisdiction={subjectJurisdiction}
                  streaming={streaming}
                  sourceNames={sourceNameIndex}
                  registryTotal={sourcesQuery.data?.sources.length ?? null}
                  hits={hits}
                  riskSignals={riskSignals}
                  riskCodes={riskCodes}
                  contextCodes={contextCodes}
                  sourceLiveness={sourceLiveness}
                  selectedSignalCode={selectedSignalCode}
                  onSelectSignal={setSelectedSignalCode}
                  subjectProfile={subjectProfile}
                  crossSourceLinks={crossSourceLinks}
                  possiblySame={possiblySame}
                  identityOpen={identityOpen}
                  onIdentityToggle={setIdentityOpen}
                  cddBuckets={cddBuckets}
                  esgBuckets={esgBuckets}
                  pendingCddSources={pendingCddSources}
                  answeredNoRecord={answeredNoRecord}
                  applicableCount={applicableSources.length}
                  answeredApplicable={answeredApplicable}
                  settled={settled}
                  bodsCountMap={bodsCountMap}
                  bodsBreakdownMap={bodsBreakdownMap}
                  oaScreening={oaScreening}
                  primaryListing={primaryListing}
                  savedReport={savedReport}
                  savedNarrative={savedNarrative}
                  onExportPayload={setExportPayload}
                  onRetrySource={retrySource}
                  retryingSources={retryingSources}
                  onPanelError={(e) => setPanelErrors((prev) => mergePanelError(prev, e))}
                  onPanelRecovered={(panel) => setPanelErrors((prev) => clearPanelError(prev, panel))}
                />
              </Suspense>
            </PanelBoundary>
          )}
          </div>
        )}
        </>
        )}

        {view === "sources" && (
          <SourcesPage sources={sourcesQuery.data?.sources} loading={sourcesQuery.isLoading} />
        )}

        {view === "features" && (
          <FeaturesPage sourceCount={sourcesQuery.data?.sources.length ?? null} />
        )}

        {view === "behind" && <BehindTheScenesPage />}

        {view === "api" && <ApiPage />}

        {view === "changelog" && <ChangelogPage />}

        {view === "batch" && (
          <BatchPage
            registryTotal={sourcesQuery.data?.sources.length ?? null}
            sourceNames={sourceNameIndex}
            onOpen={(lei) => lookupLei(lei)}
          />
        )}

        {view === "watchlist" && (
          <WatchlistPage sourceNames={sourceNameIndex} onOpen={(lei) => lookupLei(lei)} />
        )}
      </main>

      <SiteFooter onNavigate={navigate} />
    </div>
    </SavedReportContext.Provider>
  );
}
