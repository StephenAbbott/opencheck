import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { trackEvent } from "./lib/analytics";
import { useMutation, useQuery } from "@tanstack/react-query";
import SearchLoadingGrid from "./components/SearchLoadingGrid";
import {
  BASE_URL,
  downloadReportMarkdown,
  downloadReportPdf,
  fetchSources,
  isValidLei,
  retryLookupSource,
  streamLookup,
  type BoAccessNotice,
  type BodsBreakdown,
  type BodsCountsEvent,
  type CrossSourceLink,
  type DegradedSource,
  type SourceLiveness,
  type GraphShape,
  EXPORT_FORMATS,
  type MeipMatch,
  type OpenAlephScreeningMatch,
  type PossiblySameEntity,
  type RiskSignal,
  type SourceHit,
  type SubjectProfile,
} from "./lib/api";
import {
  searchByNationalId,
  type GleifSearchResult,
} from "./lib/gleifNationalId";
import { COUNTRY_OPTIONS, RA_CODES, raCodeFor, validateNationalId } from "./lib/raCodes";
import { countLeiConfirmingSources } from "./lib/identifierBadge";
import { independentCount } from "./lib/lineage";
import { partitionByKind } from "./lib/signalKind";
import {
  OpenCheckIcon,
  GleifIcon,
  StepKeyIcon,
  StepBridgeIcon,
  StepNetworkIcon,
  StepShieldIcon,
} from "./components/icons";
import { RiskChip, RISK_PRESENTATION, rank } from "./components/risk/RiskChip";
import { ExportPanel } from "./components/export/ExportPanel";
import { ChangelogPage } from "./components/ChangelogPage";
import { SubjectCard } from "./components/cdd/SubjectCard";
import { VerdictStrip } from "./components/cdd/VerdictStrip";
import { Icon, SectionHeading, SectionLabel as Eyebrow, buttonClasses } from "./components/ui";
import { profileRows, statusChip } from "./lib/subjectProfile";
import ConfidenceLegend from "./components/ui/ConfidenceLegend";
import PanelSection, { PanelCard } from "./components/ui/PanelSection";
import { PERSON_VERB, resultCount, setSourceNames, sourceLabel } from "./lib/vocab";
import { answeredCount, coverageCopy } from "./lib/lookupProgress";
import { documentTitleFor, modeParam, parseMode } from "./lib/checkMode";
import type { CheckMode } from "./lib/checkMode";
import type { IconName } from "./components/ui";
import { NarrativePanel } from "./components/cdd/NarrativePanel";
import { SignalEvidence } from "./components/risk/SignalEvidence";
import { evidenceForCode } from "./lib/signalEvidence";
import { Explain } from "./components/ui/Explain";
import { SourcesPage } from "./components/SourcesPage";
import BatchPage from "./components/BatchPage";
import type { ReportExportPayload } from "./components/cdd/NarrativePanel";
import {
  SourceBucketCard,
  SkeletonSourceCard,
  type SourceBucket,
} from "./components/cdd/SourceBucketCard";
import { OpenAlephArchiveMatches } from "./components/cdd/OpenAlephArchiveMatches";
import { EsgPanel } from "./components/cdd/EsgPanel";
import { MeipSignpost } from "./components/cdd/MeipSignpost";
import { SecuritiesSection } from "./components/cdd/SecuritiesSection";
import { clearPanelError, mergePanelError, panelLabel, type PanelError } from "./lib/panelErrors";

// FullCheck (enhanced due diligence) view — lazy so Cytoscape/graph code only
// loads when a user switches into FullCheck mode.
const FullCheckPanel = lazy(() => import("./components/cdd/FullCheckPanel"));
const BackgroundCheckPanel = lazy(
  () => import("./components/cdd/BackgroundCheckPanel")
);
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


/**
 * Curated demo subjects that have a pre-extracted Open Ownership BODS
 * bundle on disk (``data/cache/bods_data/``) — clicking any of them
 * resolves entirely offline. The list is small + opinionated; users
 * can paste any other LEI into the input.
 *
 * ``signals`` are pre-computed from the cached BODS bundles so the
 * picker cards show representative risk flags before the user clicks.
 * Confidence: high = definitively flagged; medium = structurally likely.
 */
interface ExampleSignal {
  code: string;
  confidence: "high" | "medium" | "low";
}

interface ExampleLei {
  lei: string;
  name: string;
  hint?: string;
  signals?: ExampleSignal[];
  /** GitHub raw URL for the per-entity Neo4j CSV zip */
  /** True when the example's graph is served from the pre-extracted Open
   *  Ownership bulk BODS datasets (UK PSC / GLEIF) — drives the blue
   *  "Curated example — pre-extracted data" banner on the results page.
   *  Examples without it run as ordinary live lookups. */
  bulkBods?: boolean;
}

  "https://github.com/StephenAbbott/opencheck/raw/main/data/demo/neo4j";

// Signals shown on the picker cards. These are CLAIMS ABOUT PRODUCTION
// OUTPUT and nothing fails when they drift — verify against production
// (the entity page, or the packet inside the regenerated curated
// narratives), never by reading the card back.
//
// Last verified 2026-08-18 against the post-Phase-111 curated narrative
// packets. Risk findings only: NON_EU_JURISDICTION is now kind="context"
// and the results page shows it under a separate "Structural context"
// heading, which a bare chip strip on a card cannot convey.
//
// Ordered by graph severity, then confidence — so the strongest finding
// leads rather than whichever code sorts first alphabetically.
const EXAMPLE_LEIS: ExampleLei[] = [
  {
    lei: "213800LH1BZH3DI6G760",
    name: "BP P.L.C.",
    hint: "UK oil major",
    signals: [
      { code: "OFFSHORE_LEAKS", confidence: "high" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
    bulkBods: true,
  },
  {
    lei: "253400JT3MQWNDKMJE44",
    name: "Rosneft",
    hint: "Russian state oil",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
      { code: "EXPORT_CONTROLLED", confidence: "high" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
    bulkBods: true,
  },
  {
    lei: "213800E11LI1SCETU492",
    name: "Taqa Bratani Limited",
    hint: "UAE-owned UK oil & gas",
    signals: [
      { code: "RELATED_SANCTIONS_CONTROLLED", confidence: "high" },
      { code: "RELATED_EXPORT_CONTROL_LINKED", confidence: "high" },
    ],
    bulkBods: true,
  },
  {
    lei: "5493005044RTLQ5RZU70",
    name: "Eesti Energia AS",
    hint: "Estonian energy company",
    signals: [
      { code: "RELATED_PEP", confidence: "medium" },
      { code: "STATE_CONTROLLED", confidence: "medium" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
  },
  {
    lei: "W9NG6WMZIYEU8VEDOG48",
    name: "Ørsted A/S",
    hint: "Danish offshore energy company",
    signals: [
      { code: "RELATED_PEP", confidence: "medium" },
      { code: "OFFSHORE_LEAKS", confidence: "medium" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
  },
  {
    lei: "FRDRIPF3EKNDJ2CQJL29",
    name: "Eli Lilly and Company",
    hint: "American pharmaceutical giant",
    signals: [
      { code: "RELATED_EXPORT_RISK", confidence: "high" },
      { code: "RELATED_PEP", confidence: "medium" },
      { code: "OFFSHORE_LEAKS", confidence: "high" },
      // Risk signals only. Eli Lilly also reports GLEIF_REPORTING_EXCEPTION
      // (kind="context": NATURAL_PERSONS — widely held, no consolidating
      // parent entity), but the picker card has no risk/context split, so a
      // context entry here would render as a fourth risk chip and overstate
      // the finding count the entity page shows.
    ],
  },
];


export default function App() {
  const [leiInput, setLeiInput] = useState("");

  // --- Streaming lookup state ---
  // streamingLei is set once GLEIF resolves (replaces the old `result !== null` guard).
  const [streamingLei, setStreamingLei] = useState<string | null>(null);
  const [legalName, setLegalName] = useState<string | null>(null);
  const [subjectJurisdiction, setSubjectJurisdiction] = useState<string | null>(null);
  // On mobile, the search inputs collapse once results are on screen (the
  // tab bar stays); this reopens them. Desktop is unaffected.
  const [mobileSearchOpen, setMobileSearchOpen] = useState(false);
  /** The header field's own value, kept apart from `nameQuery` so typing in
   *  one does not rewrite the other under the reader. */
  const [headerQuery, setHeaderQuery] = useState("");
  /** The identity band is a disclosure, opened by the subject card's
   *  identifier badge. Reset per lookup, like everything else about a
   *  result. */
  const [identityOpen, setIdentityOpen] = useState(false);
  /** Which risk chip the reader selected, if any. Null means "the worst one",
   *  which is what the Risk signals section opens on. Declared with the other
   *  per-entity state because the lookup reset clears it. */
  const [selectedSignalCode, setSelectedSignalCode] = useState<string | null>(null);
  const searchPanelsCollapsed = !!streamingLei && !mobileSearchOpen;
  const [hits, setHits] = useState<SourceHit[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [crossSourceLinks, setCrossSourceLinks] = useState<CrossSourceLink[]>([]);
  const [possiblySame, setPossiblySame] = useState<PossiblySameEntity[]>([]);
  const [meip, setMeip] = useState<MeipMatch | null>(null);
  // What the registers say the subject *is* (Phase 154) — its own event,
  // arriving once the deepened bundles are in. Identity, not the answer.
  const [subjectProfile, setSubjectProfile] = useState<SubjectProfile | null>(null);
  const [riskSignals, setRiskSignals] = useState<RiskSignal[]>([]);
  // Derived checks that did not fully run (issue #50) — rendered as a
  // warning above the risk panel; empty signals + non-empty degraded is
  // NOT a clean screen.
  const [degradedSources, setDegradedSources] = useState<DegradedSource[]>([]);
  // The answer-first sentence, rendered above the evidence. Built in the
  // backend (opencheck/verdict.py) from the signals and degradations, so
  // the page, the PDF, the share card and the API cannot disagree — and
  // so it costs no model call.
  const [verdict, setVerdict] = useState<string | null>(null);
  // How current each source's payload is, keyed by source_id. Arrives on the
  // risk_signals event alongside degraded_sources — the two answer the same
  // shape of question: what in this result should not be read at face value.
  const [sourceLiveness, setSourceLiveness] = useState<
    Record<string, SourceLiveness>
  >({});
  // Informational OpenAleph percolation matches (Phase 96) — related-party
  // names found in archive/watchlist collections whose topics map to no
  // RELATED_* code. Name-derived; never identifier corroboration.
  const [oaScreening, setOaScreening] = useState<OpenAlephScreeningMatch[]>([]);
  // How big the mapped graph is, for the verdict strip's ownership-network
  // column. Rides on the same event as the signals, the degradations and the
  // verdict sentence, so the three columns describe one run.
  const [graphShape, setGraphShape] = useState<GraphShape | null>(null);
  const [applicableSources, setApplicableSources] = useState<string[]>([]);
  // Panels that fetch outside `_lookup_pipeline` (/securities, /subsidiaries).
  // Deliberately NOT merged into `degradedSources`: that list arrives on the
  // same event as `signals` and as the backend-built verdict sentence, and the
  // three are provably consistent with each other. Injecting a client-side
  // record would let the coverage count disagree with a sentence that knows
  // nothing about it — and `onRiskSignals` overwrites the list wholesale, so it
  // would be erased anyway. See lib/panelErrors.ts.
  const [panelErrors, setPanelErrors] = useState<PanelError[]>([]);
  const [completedSources, setCompletedSources] = useState<Set<string>>(new Set());
  // Phase 124: the loading grid used to simulate per-source progress. It now
  // renders `source_started` / `source_completed` / `source_error`, so it needs
  // the started set the stream was already sending and nothing was reading.
  const [startedSources, setStartedSources] = useState<Set<string>>(new Set());
  // Derived rather than stored: `errors` is already the record of which sources
  // failed, and a second set could disagree with it.
  const erroredSources = useMemo(() => new Set(Object.keys(errors)), [errors]);
  // Coverage counts only sources that were dispatched. `completedSources` also
  // holds the GLEIF anchor, which emits source_started/source_completed BEFORE
  // sources_applicable and is never in that list — so the raw size could exceed
  // the total and the strip read "13 of 12 sources answered", above the line
  // "Every applicable source answered." A coverage figure that overshoots its
  // own denominator undermines the one number on the page whose whole job is
  // to say how much was checked.
  const answeredApplicable = useMemo(
    () => answeredCount(applicableSources, completedSources, erroredSources),
    [applicableSources, completedSources, erroredSources]
  );
  const [streaming, setStreaming] = useState(false);
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
  // Maps "source_id:hit_id" → BODS statement count; populated by the bods_counts SSE event.
  const [bodsCountMap, setBodsCountMap] = useState<Record<string, number>>({});
  // Same key → entity / relationship split, for the source-card graph CTA subtitle.
  const [bodsBreakdownMap, setBodsBreakdownMap] = useState<
    Record<string, BodsBreakdown>
  >({});
  // True when the SSE connection dropped AFTER the GLEIF anchor resolved —
  // partial results are on screen and a "Resume lookup" banner is shown.
  const [streamDropped, setStreamDropped] = useState(false);
  // Wall-clock ISO time the on-screen results were originally fetched, when
  // they came from the backend replay cache rather than a fresh run. Null for
  // live runs. Drives the "Results from a check N min ago" badge.
  const [replayedAt, setReplayedAt] = useState<string | null>(null);
  // Source IDs with an in-flight per-source retry (/lookup-source).
  const [retryingSources, setRetryingSources] = useState<Set<string>>(new Set());
  // Screen-reader announcement for per-source failures and retry outcomes,
  // rendered in the sr-only role="status" region in <main>.
  const [srAnnouncement, setSrAnnouncement] = useState("");

  // Cleanup ref — holds the SSE close function for the current in-flight stream.
  const cleanupRef = useRef<(() => void) | null>(null);

  // Close any open stream when the component unmounts.
  useEffect(() => () => { cleanupRef.current?.(); }, []);
  // Path → view mapping. /sources and /about are real URLs; everything
  // else falls through to "main" (the SPA rewrite in render.yaml serves
  // index.html for all paths so deep links work).
  type View = "main" | "sources" | "behind" | "api" | "changelog" | "batch";

  /**
   * The header nav, constant across every view (Phase 122). One label per
   * destination: v1 called the same page "Behind the scenes →", "About",
   * "How it works →" and "Behind the Scenes" depending on where you met it,
   * and replaced the whole nav with "← Back" on every sub-page, so /api and
   * /sources were unreachable from each other.
   */
    /**
   * The four checks, in the order they escalate: the subject alone, its
   * network, the people in it — then Climate & ESG, which is a different
   * question rather than a fourth depth, and is separated in the strip to
   * say so. Accents are the `oo.node.*` brand tier that already names each
   * mode's badge, so the tab, the badge and (for ownership and role) the
   * graph edge are one colour rather than three.
   */
  const MODE_TABS: {
    id: CheckMode;
    label: string;
    icon: IconName;
    accent: string;
    blurb: string;
    topic?: boolean;
  }[] = [
    {
      id: "quick",
      label: "QuickCheck",
      icon: "quickcheck",
      accent: "#22c55e",
      blurb: "Screening this company on its own — sanctions, control, structure. The fastest answer.",
    },
    {
      id: "full",
      label: "FullCheck",
      icon: "fullcheck",
      accent: "#3b82f6",
      blurb: "Following the ownership chain outwards, then screening everything it reaches.",
    },
    {
      id: "background",
      label: "BackgroundCheck",
      icon: "backgroundcheck",
      accent: "#7c3aed",
      blurb: "Screening the officers, directors and beneficial owners named in the records.",
    },
    {
      id: "esg",
      label: "Climate & ESG",
      icon: "esg",
      accent: "#0d9488",
      blurb: "Emissions and asset records published about this company — what it does, rather than who owns it.",
      topic: true,
    },
  ];

// No "Search" item. It did exactly what the logo beside it does — go to the
// homepage — and now that the header carries a real search field, a nav link
// labelled "Search" that is not the search field is a third thing pointing at
// two behaviours. The logo remains the way home.
/** The page title each view puts in the document outline. Keyed by `View`,
 *  so a new view cannot be added without deciding what the page is called. */
const PAGE_TITLES: Record<View, string> = {
  main: "OpenCheck — due diligence on a legal entity, from open data",
  sources: "The sources OpenCheck queries",
  behind: "About OpenCheck",
  api: "The OpenCheck API",
  changelog: "OpenCheck development history",
};

const NAV_ITEMS: { view: View; label: string }[] = [
    { view: "sources", label: "Sources" },
    { view: "api", label: "API" },
    { view: "behind", label: "About" },
  ];
  function pathToView(path: string): View {
    if (path === "/sources") return "sources";
    if (path === "/about") return "behind";
    if (path === "/api") return "api";
    if (path === "/changelog") return "changelog";
    if (path === "/batch") return "batch";
    return "main";
  }
  function viewToPath(v: View): string {
    if (v === "sources") return "/sources";
    if (v === "behind") return "/about";
    if (v === "api") return "/api";
    if (v === "changelog") return "/changelog";
    if (v === "batch") return "/batch";
    return "/";
  }
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
      document.title = documentTitleFor(mode, legalName);
    } else if (view === "sources") {
      document.title = "Data Sources — OpenCheck";
    } else if (view === "behind") {
      document.title = "Behind the Scenes — OpenCheck";
    } else if (view === "api") {
      document.title = "API — OpenCheck";
    } else if (view === "changelog") {
      document.title = "Changelog — OpenCheck";
    } else if (view === "batch") {
      document.title = "Screen a list — OpenCheck";
    } else {
      document.title = "OpenCheck";
    }
  }, [legalName, view, mode]);

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

  // Three-mode search: "name" = GLEIF name search; "nationalId" = registration
  // number reverse lookup; "lei" = paste LEI directly.
  // TENTATIVE (Phase E): the "person" tab is under evaluation — Stephen's
  // instinct is to keep person search as a follow-on from entity pages.
  // It is deliberately isolated in its own commit for a clean revert.
  const [searchMode, setSearchMode] = useState<"name" | "nationalId" | "lei" | "person">("name");
  // APG tabs keyboard pattern: Left/Right arrows (wrapping), Home and End move
  // both focus and selection across the search-mode tabs (roving tabindex).
  const SEARCH_TAB_ORDER = ["name", "nationalId", "lei", "person"] as const;
  const SEARCH_TAB_IDS: Record<(typeof SEARCH_TAB_ORDER)[number], string> = {
    name: "tab-name",
    nationalId: "tab-national-id",
    lei: "tab-lei",
    person: "tab-person",
  };
  function onSearchTabKeyDown(e: React.KeyboardEvent<HTMLButtonElement>) {
    const idx = SEARCH_TAB_ORDER.indexOf(searchMode);
    let next: number;
    if (e.key === "ArrowRight") next = (idx + 1) % SEARCH_TAB_ORDER.length;
    else if (e.key === "ArrowLeft") next = (idx + SEARCH_TAB_ORDER.length - 1) % SEARCH_TAB_ORDER.length;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = SEARCH_TAB_ORDER.length - 1;
    else return;
    e.preventDefault();
    const mode = SEARCH_TAB_ORDER[next];
    setSearchMode(mode);
    setMobileSearchOpen(true);
    document.getElementById(SEARCH_TAB_IDS[mode])?.focus();
  }
  const [nameQuery, setNameQuery] = useState("");
  // TENTATIVE person tab inputs (Phase E — see searchMode note above).
  const [personQuery, setPersonQuery] = useState("");
  const [personBirthYear, setPersonBirthYear] = useState("");
  const [nationalIdQuery, setNationalIdQuery] = useState("");
  // ISO 3166-1 alpha-2 country code for the national ID tab; defaults to UK.
  const [selectedCountry, setSelectedCountry] = useState("GB");
  // Tracks whether the national ID input has been blurred at least once.
  // Format warnings are suppressed until the field is touched so they don't
  // fire on every keystroke while the user is still typing.
  const [nationalIdTouched, setNationalIdTouched] = useState(false);

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchSources(),
  });

  // ── Name-search mutation ──────────────────────────────────────────────────
  // Queries GLEIF's public API by legal name. Returns a list of matching
  // entities for the user to pick from; selection hands off to lookupMutation.
  const nameSearchMutation = useMutation<GleifSearchResult[], Error, string>({
    mutationFn: async (q: string) => {
      const url =
        `https://api.gleif.org/api/v1/lei-records` +
        `?filter[entity.legalName]=${encodeURIComponent(q)}&page[size]=10`;
      const resp = await fetch(url, { headers: { Accept: "application/vnd.api+json" } });
      if (!resp.ok) throw new Error(`GLEIF API returned ${resp.status}`);
      const json = await resp.json();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (json.data ?? []).map((item: any) => {
        const attrs = item.attributes ?? {};
        const entity = attrs.entity ?? {};
        const reg = attrs.registration ?? {};
        return {
          lei: attrs.lei as string,
          legalName:
            (entity.legalName?.name as string) ??
            (entity.legalName as string) ??
            attrs.lei,
          country: entity.legalAddress?.country ?? "—",
          status: reg.status ?? "—",
        } satisfies GleifSearchResult;
      });
    },
  });

  // ── National-ID search mutation ──────────────────────────────────────────
  // Queries GLEIF's three registration-ID filter fields in parallel using
  // the RA code for the selected country. On single result, auto-navigates;
  // on multiple results, shows the same picker as the name search.
  const nationalIdSearchMutation = useMutation<
    GleifSearchResult[],
    Error,
    { raCode: string; id: string }
  >({
    mutationFn: ({ raCode, id }) => searchByNationalId(raCode, id),
  });

  // ── LEI lookup mutation ───────────────────────────────────────────────────
  // Opens the SSE stream for /lookup-stream. The mutation is considered
  // "pending" (i.e. showing the loading grid) until the backend emits the
  // gleif_done event confirming the entity; all subsequent streaming state
  // (hits, risk signals, cross-source links) is managed via useState below.
  const lookupMutation = useMutation<
    { lei: string; legal_name: string | null },
    Error,
    { lei: string; refresh?: boolean; mode?: CheckMode }
  >({
    mutationFn: ({ lei, refresh, mode: startMode }) =>
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
        setStreamingLei(null);
        setLegalName(null);
        // A new lookup opens on QuickCheck unless the caller asked for a
        // mode. It used to hardcode "quick", and because the mutationFn runs
        // asynchronously it landed *after* the deep-link handler's
        // setMode(parseMode(...)) — so ?mode=full opened on QuickCheck, which
        // is exactly what that handler's comment says must not happen.
        setMode(startMode ?? "quick");
        setHits([]);
        setErrors({});
        setCrossSourceLinks([]);
        setPossiblySame([]);
        setMeip(null);
        setSubjectProfile(null);
        setRiskSignals([]);
        setDegradedSources([]);
        setVerdict(null);
        setSourceLiveness({});
        setOaScreening([]);
        setGraphShape(null);
        setApplicableSources([]);
        setCompletedSources(new Set());
        setStartedSources(new Set());
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
        setStreaming(false);
        setBodsCountMap({});
        setBodsBreakdownMap({});
        setStreamDropped(false);
        setRetryingSources(new Set());
        setReplayedAt(null);

        // Tracks whether the GLEIF anchor resolved: a connection drop before
        // it is a hard error; after it, we keep partial results and offer a
        // "Resume lookup" instead.
        let anchored = false;

        const cleanup = streamLookup(
          lei,
          {
          // Served from the backend replay cache — badge the result with the
          // original completion time so a cached run never looks live.
          onReplayed: (e) => setReplayedAt(e.fetched_at),
          onGleifDone: (e) => {
            anchored = true;
            setStreamingLei(e.lei);
            setLegalName(e.legal_name);
            setSubjectJurisdiction(e.jurisdiction);
            setMobileSearchOpen(false); // re-collapse the mobile search inputs
            setStreaming(true);
            resolve({ lei: e.lei, legal_name: e.legal_name });
          },
          onSourcesApplicable: (e) => setApplicableSources(e.source_ids),
          onSourceStarted: (e) =>
            setStartedSources((prev) => new Set([...prev, e.source_id])),
          // Dedup by source_id:hit_id — in dev, React StrictMode runs the lookup
          // effect twice, so two streams can each deliver the same hit. The guard
          // makes hit accumulation idempotent (no-op in production, where
          // StrictMode doesn't double-invoke).
          onHit: (e) =>
            setHits((prev) =>
              prev.some((h) => h.source_id === e.source_id && h.hit_id === e.hit_id)
                ? prev
                : [...prev, e]
            ),
          onSourceCompleted: (e) =>
            setCompletedSources((prev) => new Set([...prev, e.source_id])),
          onSourceError: (e) => {
            setErrors((prev) => ({ ...prev, [e.source_id]: e.error }));
            setCompletedSources((prev) => new Set([...prev, e.source_id]));
          },
          onCrossSourceLinks: (e) => setCrossSourceLinks(e.links),
          onPossiblySame: (e) => setPossiblySame(e.pairs),
          onMeip: (e) => setMeip(e.match),
          onSubjectProfile: (e) => setSubjectProfile(e.profile),
          onRiskSignals: (e) => {
            setRiskSignals(e.signals);
            setDegradedSources(e.degraded_sources ?? []);
            setVerdict(e.verdict ?? null);
            setSourceLiveness(e.source_liveness ?? {});
            setOaScreening(e.openaleph_screening ?? []);
            setGraphShape(e.graph_shape ?? null);
          },
          onBodsCounts: (e: BodsCountsEvent) => {
            setBodsCountMap(e.counts);
            if (e.breakdown) setBodsBreakdownMap(e.breakdown);
          },
          onDone: () => {
            setStreaming(false);
            setStreamDropped(false);
            cleanupRef.current = null;
          },
          onError: (detail) => {
            setStreaming(false);
            cleanupRef.current = null;
            if (anchored) {
              // Mid-lookup drop (e.g. Render cold start, flaky network):
              // keep the partial results and surface the resume banner.
              setStreamDropped(true);
            } else {
              reject(new Error(detail));
            }
          },
          },
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
    opts?: { refresh?: boolean; mode?: CheckMode }
  ) {
    const lei = rawLei.trim().toUpperCase();
    setLeiInput(lei);
    setView("main");
    trackEvent("lookup_run"); // feature event only — the LEI is never recorded
    // Shareable URLs: reflect the lookup in ?lei= so refresh and copy/paste
    // re-run it (the backend replay cache makes repeats near-instant).
    const url = new URL(window.location.href);
    if (url.searchParams.get("lei") !== lei) {
      url.searchParams.set("lei", lei);
      window.history.pushState({}, "", url);
    }
    // A lookup started from the search box always opens on QuickCheck; a
    // deep link with ?mode= is honoured by the popstate/first-load effect
    // below instead, which runs after this.
    // Cancel any in-flight stream before starting a new one.
    cleanupRef.current?.();
    cleanupRef.current = null;
    lookupMutation.mutate({ lei, refresh: opts?.refresh, mode: opts?.mode });
  }

  /**
   * Switch check mode. The single entry point, because three things have to
   * happen together and v1 did none of them: the value goes into `?mode=`
   * so a shared link and a refresh land where you left off; focus moves to
   * the new panel, since switching unmounts most of the page and focus
   * would otherwise drop to <body>; and the analytics event fires once per
   * actual change rather than on every click of an already-active tab.
   */
  const selectMode = useCallback((next: CheckMode) => {
    setMode((current) => {
      if (current === next) return current;
      if (next === "full") trackEvent("fullcheck_run");
      if (next === "background") trackEvent("backgroundcheck_run");
      const url = new URL(window.location.href);
      const param = modeParam(next);
      if (param === null) url.searchParams.delete("mode");
      else url.searchParams.set("mode", param);
      window.history.replaceState({}, "", url);
      // After the panel swaps in. requestAnimationFrame rather than a
      // timeout so it lands on the next paint whatever the render cost.
      requestAnimationFrame(() => {
        document.getElementById(`panel-${next}`)?.focus({ preventScroll: true });
      });
      return next;
    });
  }, []);

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
    const initial = fromUrl(new URLSearchParams(window.location.search).get("lei"));
    if (initial && isValidLei(initial)) {
      // The mode goes *into* the lookup rather than being set beside it: the
      // mutationFn's own reset runs later and would otherwise overwrite it.
      lookupLei(initial, {
        mode: parseMode(new URLSearchParams(window.location.search).get("mode")),
      });
    }

    const onPopState = () => {
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
        lookupLei(lei, {
          mode: parseMode(new URLSearchParams(window.location.search).get("mode")),
        });
      } else {
        // Navigated back to the landing page — clear the result view.
        cleanupRef.current?.();
        cleanupRef.current = null;
        setStreamingLei(null);
        setLegalName(null);
        setHits([]);
        setErrors({});
        setStreaming(false);
        setStreamDropped(false);
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
    setRetryingSources((prev) => new Set([...prev, sourceId]));
    const sourceName = sourceNameIndex[sourceId] ?? sourceId;
    try {
      const res = await retryLookupSource(streamingLei, sourceId);
      if (res.error) {
        setErrors((prev) => ({ ...prev, [sourceId]: res.error as string }));
        setSrAnnouncement(`${sourceName} retry failed.`);
      } else {
        setErrors((prev) => {
          const next = { ...prev };
          delete next[sourceId];
          return next;
        });
        setHits((prev) => [
          ...prev.filter((h) => h.source_id !== sourceId),
          ...res.hits,
        ]);
        setSrAnnouncement(`${sourceName} retried successfully.`);
      }
    } catch (e) {
      setErrors((prev) => ({
        ...prev,
        [sourceId]: e instanceof Error ? e.message : String(e),
      }));
      setSrAnnouncement(`${sourceName} retry failed.`);
    } finally {
      setRetryingSources((prev) => {
        const next = new Set(prev);
        next.delete(sourceId);
        return next;
      });
    }
  }

  function runLookup(e: React.FormEvent) {
    e.preventDefault();
    lookupLei(leiInput);
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
  function submitHeaderSearch(e: React.FormEvent) {
    e.preventDefault();
    const q = headerQuery.trim();
    if (!q) return;
    // The header is on every page, so the search has to *get to* the page that
    // can show a result. Without this it fired a real GLEIF request from
    // /sources and rendered the answer nowhere, and from a person report it
    // ran a whole 39-source lookup behind a screen that never changed —
    // leaving `?person=…&lei=…` in the URL as a permanently stuck link.
    showEntitySearchSurface();
    setHeaderQuery("");
    if (isValidLei(q.toUpperCase())) {
      lookupLei(q);
      return;
    }
    setSearchMode("name");
    setMobileSearchOpen(true);
    setNameQuery(q);
    nameSearchMutation.mutate(q);
    // The results render in the panel this just opened, so the page follows.
    requestAnimationFrame(() => {
      document
        .getElementById("panel-name")
        ?.scrollIntoView({ behavior: "smooth", block: "start" });
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

  function searchByName(e: React.FormEvent) {
    e.preventDefault();
    const q = nameQuery.trim();
    if (!q) return;
    nameSearchMutation.mutate(q);
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
  // The identity band's profile rows (Phase 154) — derived once per profile.
  const profileRowsForBand = useMemo(
    () => profileRows(subjectProfile, sourceNameIndex),
    [subjectProfile, sourceNameIndex],
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

  // Distinct sources participating in cross-source identifier links — the
  // headline number for the collapsed reconciliation box ("N identifiers
  // matched across M sources"). NOT the SubjectCard badge number: that badge
  // sits next to the LEI, so it counts only LEI-confirming sources (below).
  const crossLinkedSourceCount = useMemo(() => {
    const srcs = new Set<string>();
    for (const link of crossSourceLinks)
      for (const h of link.hits) srcs.add(h.source_id);
    // Independent origins, not participating adapters: OpenCorporates and
    // Companies House sharing a company number is one register (lineage.ts).
    return independentCount([...srcs]);
  }, [crossSourceLinks]);

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
        exportPayload.dispositions
      );
      trackEvent("pdf_export");
    } catch (e) {
      setExportError(e instanceof Error ? e.message : "Could not generate the PDF.");
    } finally {
      setPdfBusy(false);
    }
  }, [streamingLei, exportPayload]);

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
        exportPayload.dispositions
      );
    } catch (e) {
      setExportError(
        e instanceof Error ? e.message : "Could not generate the Markdown report."
      );
    } finally {
      setMdBusy(false);
    }
  }, [streamingLei, exportPayload]);

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
    el.scrollIntoView({ behavior: "smooth", block: "start" });
    if (el.tabIndex < 0) el.tabIndex = -1;
    el.focus({ preventScroll: true });
    el.classList.add("oc-cite-flash");
    window.setTimeout(() => el.classList.remove("oc-cite-flash"), 1600);
  }, []);

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

  const flashIdentityBand = () => {
    setIdentityOpen(true);
    const el = document.getElementById("cross-source-identifiers");
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "start" });
      if (el.tabIndex < 0) el.tabIndex = -1;
      el.focus({ preventScroll: true });
      el.classList.add("oc-cite-flash");
      window.setTimeout(() => el.classList.remove("oc-cite-flash"), 1600);
    }
  };

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

  // Only show the national-ID format warning after the field has been blurred
  // (touched) so partial input during typing doesn't trigger an amber state.
  const nationalIdFormatOk =
    !nationalIdTouched || validateNationalId(selectedCountry, nationalIdQuery);

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
    setStreamingLei(null);
    setLegalName(null);
    setHits([]);
    setErrors({});
    setCrossSourceLinks([]);
    setPossiblySame([]);
    setMeip(null);
    setSubjectProfile(null);
    setRiskSignals([]);
    setDegradedSources([]);
    setVerdict(null);
    setSourceLiveness({});
    setOaScreening([]);
    setGraphShape(null);
    setApplicableSources([]);
    setCompletedSources(new Set());
    // Phase 124 added these two and this 30-setter reset is exactly the place
    // a new one gets missed: a stale panel-error notice would sit on an empty
    // landing page saying "this report" when there is no report.
    setStartedSources(new Set());
    setPanelErrors([]);
    setIdentityOpen(false);
    setExportError(null);
    setExportPayload({ narrative: null, dispositions: null });
    setStreaming(false);
    lookupMutation.reset();
    nameSearchMutation.reset();
    nationalIdSearchMutation.reset();
    setLeiInput("");
    setNameQuery("");
    setNationalIdQuery("");
    setSelectedCountry("GB");
    setNationalIdTouched(false);
    setSearchMode("name");
    // Clear ?lei= so the address bar returns to a clean homepage URL.
    if (window.location.search) {
      const url = new URL(window.location.href);
      url.search = "";
      window.history.pushState({}, "", url);
    }
    // Every setter here is a stable useState setter and the mutations are
    // stable for the component's life, so the empty dep list is correct.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const HeroHeading = streamingLei ? "h2" : "h1";

  return (
    <div className="min-h-screen flex flex-col bg-oo-bg">
      {/* Skip-to-content link — visually hidden until focused (WCAG 2.4.1) */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-50 focus:px-4 focus:py-2 focus:bg-oo-blue focus:text-white focus:rounded focus:font-medium"
      >
        Skip to main content
      </a>
      {/*
       * Header — full-width dark banner, BO design system.
       * Decorative blue radial gradient sits top-right (rgba 61,48,212,0.28)
       * fading to transparent. Inline style because Tailwind doesn't
       * have a clean utility for offset radial gradients.
       */}
      <header
        className="relative overflow-hidden bg-oo-navy text-white px-6 sm:px-10 lg:px-16 py-3 sm:py-4"
        role="banner"
        style={{
          backgroundImage:
            "radial-gradient(circle 500px at calc(100% + 80px) -80px, rgba(61, 48, 212, 0.28), transparent)",
        }}
      >
        <div className="max-w-oo-page mx-auto relative">
          <div className="flex items-center justify-between gap-4">
            {/* On mobile the search field is hidden, so this group is the only
                child of the row and the nav ended up crowded against the
                wordmark with the whole right half of the banner empty. Full
                width with the two ends pushed apart puts the mark at one edge
                and the links at the other; from `md` the search field takes
                the right-hand end and this reverts to sitting beside the
                mark. */}
            <div className="flex items-center gap-4 w-full justify-between md:w-auto md:justify-start">
              <button
                type="button"
                onClick={resetToHome}
                aria-label="OpenCheck — back to homepage"
                className="flex items-center gap-2.5 hover:opacity-80 transition-opacity text-left"
              >
                <OpenCheckIcon className="h-7 w-auto flex-shrink-0" />
                <span className="font-head font-bold text-white leading-tight text-xl">
                  Open<span className="text-[#93c5fd]">Check</span>
                </span>
              </button>
            <nav aria-label="Site navigation" className="flex items-center gap-4 sm:gap-5">
              {NAV_ITEMS.map((item) => {
                const current = view === item.view;
                return (
                  <button
                    key={item.view}
                    type="button"
                    onClick={() => navigate(item.view)}
                    aria-current={current ? "page" : undefined}
                    className={`min-h-[44px] text-[13px] transition-colors ${
                      current
                        ? "text-white font-medium border-b-2 border-[#93c5fd]"
                        : "text-white/80 hover:text-white"
                    }`}
                  >
                    {item.label}
                  </button>
                );
              })}
            </nav>
            </div>
            {/* The constant search field. On a report page the tabbed panel is
                collapsed to a single prompt row, which left the header — the
                one piece of chrome present on every page — with no way to
                start a search from. The source counts that used to sit here
                are said in the hero and listed in full on /sources; a stat
                does not need to be in the banner of a report about a company.

                It handles the two things a header field can honestly handle:
                a pasted LEI runs straight through, anything else goes to the
                company-name search. National ID and person search stay in the
                full panel, which the prompt row still opens. */}
            <form
              onSubmit={submitHeaderSearch}
              role="search"
              aria-label="Search for a company"
              className="hidden md:flex items-center gap-2 rounded-oo border border-white/25 bg-white/10 focus-within:bg-white/15 focus-within:border-white/45 px-3 py-1.5 min-w-[300px] transition-colors"
            >
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="2" strokeLinecap="round" aria-hidden="true" className="text-white/70 shrink-0">
                <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
              </svg>
              <label htmlFor="oo-header-search" className="sr-only">
                Company name or LEI
              </label>
              <input
                id="oo-header-search"
                type="text"
                value={headerQuery}
                onChange={(e) => setHeaderQuery(e.target.value)}
                placeholder="Company name or LEI"
                className="min-w-0 flex-1 bg-transparent text-oo-small text-white placeholder:text-white/60 focus:outline-none"
              />
              <button type="submit" className="sr-only">
                Search
              </button>
            </form>
          </div>
        </div>
      </header>

      <main
        id="main-content"
        role="main"
        tabIndex={-1}
        style={{ outline: "none" }}
        className="flex-1 px-6 sm:px-10 lg:px-16 py-5 sm:py-6 max-w-oo-page mx-auto w-full"
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
        {view !== "main" && <h1 className="sr-only">{PAGE_TITLES[view]}</h1>}

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
              <p className="text-[13px] text-oo-muted italic">
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
          <HeroHeading className="font-head font-bold text-oo-ink leading-tight text-[20px] sm:text-[26px]">
            Conduct due diligence on <span className="text-oo-blue">3 million</span> companies, starting from a single ID
          </HeroHeading>
          <p className="text-[13px] sm:text-sm text-oo-muted leading-snug mt-2">
            With a Legal Entity Identifier, OpenCheck pulls open corporate data from 40 sources into one graph using the Beneficial Ownership Data Standard
          </p>
        </div>
        )}
        <div className="mb-4 bg-white border border-oo-rule rounded-oo overflow-hidden">
          {/* Tab bar — homepage only.

              It used to stay on the report as a "landmark", with only the
              440 lines of input panels collapsing beneath it. That left four
              tabs and a search affordance above every result, which is the
              v1 page's opening move: search first, answer second. The v2
              report opens on the subject. The one-line prompt below is the
              whole search surface on a result page, and reopening it brings
              the tabs back with it. */}
          {!searchPanelsCollapsed && (
          <div role="tablist" aria-label="Search method" className="flex border-b border-oo-rule">
            <button
              type="button"
              role="tab"
              aria-selected={searchMode === "name"}
              aria-controls={searchMode === "name" ? "panel-name" : undefined}
              id="tab-name"
              tabIndex={searchMode === "name" ? 0 : -1}
              onKeyDown={onSearchTabKeyDown}
              onClick={() => { setSearchMode("name"); setMobileSearchOpen(true); }}
              className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-[12px] font-medium transition-colors bg-white ${
                searchMode === "name"
                  ? "text-oo-ink border-b-2 border-oo-blue"
                  : "text-oo-muted hover:text-oo-ink"
              }`}
            >
              <GleifIcon aria-hidden style={{ height: "1.1em", width: "auto", flexShrink: 0 }} />
              Company name
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={searchMode === "nationalId"}
              aria-controls={searchMode === "nationalId" ? "panel-national-id" : undefined}
              id="tab-national-id"
              tabIndex={searchMode === "nationalId" ? 0 : -1}
              onKeyDown={onSearchTabKeyDown}
              onClick={() => { setSearchMode("nationalId"); setMobileSearchOpen(true); }}
              className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-[12px] font-medium transition-colors border-l border-oo-rule bg-white ${
                searchMode === "nationalId"
                  ? "text-oo-ink border-b-2 border-oo-blue"
                  : "text-oo-muted hover:text-oo-ink"
              }`}
            >
              <svg aria-hidden="true" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M4 7h16M4 12h8m-8 5h16"/></svg>
              National ID
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={searchMode === "lei"}
              aria-controls={searchMode === "lei" ? "panel-lei" : undefined}
              id="tab-lei"
              tabIndex={searchMode === "lei" ? 0 : -1}
              onKeyDown={onSearchTabKeyDown}
              onClick={() => { setSearchMode("lei"); setMobileSearchOpen(true); }}
              className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-[12px] font-medium transition-colors border-l border-oo-rule bg-white ${
                searchMode === "lei"
                  ? "text-oo-ink border-b-2 border-oo-blue"
                  : "text-oo-muted hover:text-oo-ink"
              }`}
            >
              <svg aria-hidden="true" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="9" y="2" width="6" height="4" rx="1"/><path d="M8 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2h-2"/><path d="M12 12h4m-4 4h4m-8-4h.01M8 16h.01"/></svg>
              Paste an LEI
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={searchMode === "person"}
              aria-controls={searchMode === "person" ? "panel-person" : undefined}
              id="tab-person"
              tabIndex={searchMode === "person" ? 0 : -1}
              onKeyDown={onSearchTabKeyDown}
              onClick={() => { setSearchMode("person"); setMobileSearchOpen(true); }}
              className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-[12px] font-medium transition-colors border-l border-oo-rule bg-white ${
                searchMode === "person"
                  ? "text-oo-ink border-b-2 border-oo-blue"
                  : "text-oo-muted hover:text-oo-ink"
              }`}
            >
              <svg aria-hidden="true" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="8" r="3.5"/><path d="M5 20c.8-3.5 3.6-5.5 7-5.5s6.2 2 7 5.5"/></svg>
              Person name
            </button>
          </div>
          )}

          {/* Panels collapse once results are on screen — the tab bar stays
              as a landmark; the prompt row below reopens them. Phase 122
              widened this from mobile-only: 440 lines of search panel above
              a report is the same problem on a laptop as on a phone. */}
          <div className={searchPanelsCollapsed ? "hidden" : ""}>

          {/* ── Name search panel ── */}
          {searchMode === "name" && (
            <div id="panel-name" role="tabpanel" aria-labelledby="tab-name" className="p-4">
              <form onSubmit={searchByName}>
                <div className="flex flex-col sm:flex-row gap-3">
                  <input
                    id="name-input"
                    type="search"
                    value={nameQuery}
                    onChange={(e) => setNameQuery(e.target.value)}
                    placeholder="Search by company name"
                    autoComplete="off"
                    aria-label="Company name"
                    className="flex-1 border border-oo-rule rounded px-3 py-2.5 bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                  />
                  <button
                    type="submit"
                    disabled={nameSearchMutation.isPending || !nameQuery.trim()}
                    aria-busy={nameSearchMutation.isPending}
                    className="w-full sm:w-auto bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                  >
                    {nameSearchMutation.isPending ? "Searching…" : "Search"}
                  </button>
                </div>
              </form>

              {/* No aria-live here — the role="alert" children announce themselves */}
              <div>
                {nameSearchMutation.isError && (
                  <div role="alert" className="mt-4 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
                    {nameSearchMutation.error?.message ?? "Search failed"}
                  </div>
                )}
                {nameSearchMutation.isSuccess && nameSearchMutation.data.length === 0 && (
                  <div role="alert" className="mt-4 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
                    No entities found. Try a shorter or different spelling.
                  </div>
                )}
              </div>

              {nameSearchMutation.data && nameSearchMutation.data.length > 0 && (
                <div className="mt-4" aria-live="polite">
                  <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-3">
                    {resultCount(nameSearchMutation.data.length)} — select one to search it
                  </p>
                  <ul aria-label="Search results" className="divide-y divide-oo-rule border border-oo-rule rounded-oo overflow-hidden">
                    {nameSearchMutation.data.map((r) => (
                      <li key={r.lei}>
                        <button
                          type="button"
                          aria-label={`Search ${r.legalName}, LEI ${r.lei}`}
                          onClick={() => {
                            nameSearchMutation.reset();
                            setNameQuery("");
                            lookupLei(r.lei);
                            focusMain();
                          }}
                          className="w-full text-left px-4 py-3 hover:bg-oo-bg transition-colors focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oo-blue/40"
                        >
                          <div className="font-head font-bold text-[14px] text-oo-ink leading-snug">
                            {r.legalName}
                          </div>
                          <div className="flex items-center gap-3 mt-1">
                            <span className="font-mono text-[11px] text-oo-blue">
                              {r.lei}
                            </span>
                            <span className="text-[11px] text-oo-muted">{r.country}</span>
                            <span
                              className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${
                                r.status === "ISSUED"
                                  ? "bg-emerald-50 text-emerald-700 border-emerald-200"
                                  : "bg-oo-bg text-oo-muted border-oo-rule"
                              }`}
                            >
                              {r.status}
                            </span>
                          </div>
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {/* ── National ID panel ── */}
          {searchMode === "nationalId" && (
            <div id="panel-national-id" role="tabpanel" aria-labelledby="tab-national-id" className="p-4">
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  const q = nationalIdQuery.trim();
                  if (!q) return;
                  const entry = RA_CODES[selectedCountry];
                  if (!entry) return;
                  nationalIdSearchMutation.mutate(
                    // raCodeFor, not entry.raCode: a GB number beginning SC or
                    // NI belongs to a different Companies House authority, and
                    // scoping it to England & Wales returns nothing at all.
                    { raCode: raCodeFor(selectedCountry, q), id: q },
                    {
                      onSuccess: (results) => {
                        if (results.length === 1) {
                          // Single unambiguous match — go straight to the lookup.
                          nationalIdSearchMutation.reset();
                          setNationalIdQuery("");
                          lookupLei(results[0].lei);
                          focusMain();
                        }
                        // Multiple results: show the picker below (same as name search).
                      },
                    },
                  );
                }}
              >
                <div className="flex flex-col sm:flex-row sm:gap-3 sm:items-end gap-3">
                  <div className="sm:flex-none">
                    <label
                      htmlFor="national-id-country"
                      className="block text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2"
                    >
                      Country
                    </label>
                    <select
                      id="national-id-country"
                      value={selectedCountry}
                      onChange={(e) => {
                        setSelectedCountry(e.target.value);
                        nationalIdSearchMutation.reset();
                        setNationalIdQuery("");
                        setNationalIdTouched(false);
                      }}
                      className="w-full sm:w-auto border border-oo-rule rounded px-3 py-2.5 text-[13px] focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue bg-oo-bg sm:bg-white"
                    >
                      {COUNTRY_OPTIONS.map(({ code, entry }) => (
                        <option key={code} value={code}>
                          {entry.countryName}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="flex-1">
                    <label
                      htmlFor="national-id-input"
                      className="block text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2"
                    >
                      {RA_CODES[selectedCountry]?.idLabel ?? "Registration number"}
                    </label>
                    <input
                      id="national-id-input"
                      type="text"
                      value={nationalIdQuery}
                      onChange={(e) => setNationalIdQuery(e.target.value)}
                      onBlur={() => setNationalIdTouched(true)}
                      placeholder={RA_CODES[selectedCountry]?.placeholder ?? ""}
                      autoComplete="off"
                      spellCheck={false}
                      aria-label={RA_CODES[selectedCountry]?.idLabel ?? "Registration number"}
                      aria-describedby={!nationalIdFormatOk ? "national-id-format-warn" : undefined}
                      aria-invalid={!nationalIdFormatOk || undefined}
                      className={`w-full border rounded px-3 py-2.5 font-mono focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue ${
                        !nationalIdFormatOk
                          ? "border-amber-400 bg-amber-50/40"
                          : "border-oo-rule bg-oo-bg sm:bg-white"
                      }`}
                    />
                    {!nationalIdFormatOk && (
                      <p
                        id="national-id-format-warn"
                        role="status"
                        className="mt-1.5 text-[12px] text-amber-700"
                      >
                        Format looks unexpected — expected {RA_CODES[selectedCountry]?.formatHint?.toLowerCase()}.
                        You can still search; GLEIF may store the number differently.
                      </p>
                    )}
                  </div>
                  <button
                    type="submit"
                    disabled={nationalIdSearchMutation.isPending || !nationalIdQuery.trim()}
                    aria-busy={nationalIdSearchMutation.isPending}
                    className="w-full sm:w-auto sm:flex-none bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                  >
                    {nationalIdSearchMutation.isPending ? "Searching…" : "Search"}
                  </button>
                </div>
              </form>

              {/* No aria-live here — the role="alert" children announce themselves */}
              <div>
                {nationalIdSearchMutation.isError && (
                  <div role="alert" className="mt-4 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
                    {nationalIdSearchMutation.error?.message ?? "Search failed"}
                  </div>
                )}
                {nationalIdSearchMutation.isSuccess && nationalIdSearchMutation.data.length === 0 && (
                  <div role="alert" className="mt-4 bg-amber-50 border border-amber-200 text-amber-800 rounded-oo p-3 text-sm">
                    No LEI found for this registration number in GLEIF. The company may not have an LEI, or the number may be recorded differently.{" "}
                    <button
                      type="button"
                      onClick={() => {
                        nationalIdSearchMutation.reset();
                        setNationalIdQuery("");
                        setSearchMode("name");
                        focusMain();
                      }}
                      className="underline hover:no-underline"
                    >
                      Try searching by company name instead →
                    </button>
                  </div>
                )}
              </div>

              {nationalIdSearchMutation.data && nationalIdSearchMutation.data.length > 1 && (
                <div className="mt-4" aria-live="polite">
                  <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-3">
                    {resultCount(nationalIdSearchMutation.data.length)} — select one to search it
                  </p>
                  <ul aria-label="Search results" className="divide-y divide-oo-rule border border-oo-rule rounded-oo overflow-hidden">
                    {nationalIdSearchMutation.data.map((r) => (
                      <li key={r.lei}>
                        <button
                          type="button"
                          aria-label={`Search ${r.legalName}, LEI ${r.lei}`}
                          onClick={() => {
                            nationalIdSearchMutation.reset();
                            setNationalIdQuery("");
                            lookupLei(r.lei);
                            focusMain();
                          }}
                          className="w-full text-left px-4 py-3 hover:bg-oo-bg transition-colors focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oo-blue/40"
                        >
                          <div className="font-head font-bold text-[14px] text-oo-ink leading-snug">
                            {r.legalName}
                          </div>
                          <div className="flex items-center gap-3 mt-1">
                            <span className="font-mono text-[11px] text-oo-blue">
                              {r.lei}
                            </span>
                            <span className="text-[11px] text-oo-muted">{r.country}</span>
                            <span
                              className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${
                                r.status === "ISSUED"
                                  ? "bg-emerald-50 text-emerald-700 border-emerald-200"
                                  : "bg-oo-bg text-oo-muted border-oo-rule"
                              }`}
                            >
                              {r.status}
                            </span>
                          </div>
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {/* ── LEI paste panel ── */}
          {searchMode === "lei" && (
            <form onSubmit={runLookup} id="panel-lei" role="tabpanel" aria-labelledby="tab-lei" className="p-4">
              <div className="flex flex-col sm:flex-row gap-3">
                <input
                  id="lei-input"
                  type="text"
                  value={leiInput}
                  onChange={(e) => setLeiInput(e.target.value)}
                  placeholder="Paste a 20-character LEI"
                  spellCheck={false}
                  autoComplete="off"
                  aria-label="Legal Entity Identifier (20 characters)"
                  pattern="[A-Za-z0-9]{20}"
                  inputMode="text"
                  className="flex-1 border border-oo-rule rounded px-3 py-2.5 font-mono uppercase tracking-wide bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue placeholder:font-sans placeholder:normal-case placeholder:tracking-normal"
                  maxLength={20}
                />
                <button
                  type="submit"
                  disabled={lookupMutation.isPending || !leiInput.trim()}
                  aria-busy={lookupMutation.isPending}
                  className="w-full sm:w-auto bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                >
                  {lookupMutation.isPending ? "Searching…" : "Search"}
                </button>
              </div>
            </form>
          )}

          {/* ── Person search panel (TENTATIVE, Phase E) ── */}
          {searchMode === "person" && (
            <div id="panel-person" role="tabpanel" aria-labelledby="tab-person" className="p-4">
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  const name = personQuery.trim();
                  if (name.length < 2) return;
                  const by = Number(personBirthYear);
                  openPersonReport(
                    name,
                    Number.isInteger(by) && by >= 1900 && by <= 2100 ? by : undefined
                  );
                }}
              >
                <div className="flex flex-col sm:flex-row gap-3">
                  <input
                    id="person-input"
                    type="search"
                    value={personQuery}
                    onChange={(e) => setPersonQuery(e.target.value)}
                    placeholder="Search by person name"
                    autoComplete="off"
                    aria-label="Person name"
                    className="flex-1 border border-oo-rule rounded px-3 py-2.5 bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                  />
                  <input
                    type="text"
                    value={personBirthYear}
                    onChange={(e) => setPersonBirthYear(e.target.value)}
                    placeholder="Birth year (optional)"
                    inputMode="numeric"
                    pattern="[0-9]{4}"
                    maxLength={4}
                    aria-label="Birth year (optional, corroborates name matches)"
                    className="w-full sm:w-44 border border-oo-rule rounded px-3 py-2.5 bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                  />
                  <button
                    type="submit"
                    disabled={personQuery.trim().length < 2}
                    className="w-full sm:w-auto bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                  >
                    {PERSON_VERB}
                  </button>
                </div>
              </form>
              <p className="text-[11px] text-oo-muted leading-[1.6] mt-3">
                Screens a person by name across every source that holds people
                (Companies House officers, OpenSanctions, EveryPolitician,
                Wikidata, OpenAleph) for PEP, sanctions and offshore-leaks
                signals. Name-based: results are potential matches with their
                evidence shown, never confirmed identities. Adding a birth year
                helps corroborate matches. Tip: for people connected to a
                company, the BackgroundCheck view on the company's report gives
                the same screening with role context attached.
              </p>
            </div>
          )}

          </div>

          {searchPanelsCollapsed && (
            // Focus has to be moved deliberately: this button unmounts itself
            // on click, and with the tab bar no longer rendered on a report
            // page it is the *only* search affordance there — so dropping
            // focus to <body> strands the entire keyboard entry path.
            <button
              type="button"
              onClick={() => {
                setMobileSearchOpen(true);
                requestAnimationFrame(() => {
                  document
                    .querySelector<HTMLElement>(
                      // Scoped to the search tablist: the mode tabs (QuickCheck
                      // and friends) are role="tab" too, and one of those is
                      // always selected.
                      '[role="tablist"][aria-label="Search method"] [role="tab"][aria-selected="true"]'
                    )
                    ?.focus();
                });
              }}
              className="w-full flex items-center justify-center gap-2 px-4 py-2.5 min-h-[44px] text-[12px] font-medium text-oo-blue hover:bg-oo-bg transition-colors"
            >
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <circle cx="11" cy="11" r="7" />
                <path d="m21 21-4.3-4.3" />
              </svg>
              More search options — national ID, person name
            </button>
          )}
        </div>

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
          />
        )}

        {!streamingLei && !lookupMutation.isPending && !streaming && !lookupMutation.isError && !nameSearchMutation.data && !nameSearchMutation.isPending && !nationalIdSearchMutation.data && !nationalIdSearchMutation.isPending && (
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
            <p className="text-[13px] leading-[1.6] text-amber-900">
              <span className="font-medium">Connection lost mid-lookup.</span>{" "}
              Showing partial results for {legalName ?? streamingLei}.
            </p>
            <button
              type="button"
              onClick={() => lookupLei(streamingLei)}
              className="shrink-0 rounded border border-amber-400 px-3 py-1.5 text-[12px] font-semibold text-amber-900 transition-colors hover:bg-amber-100"
            >
              Resume lookup
            </button>
          </div>
        )}

        {streamingLei && (
          <h1 className="sr-only">
            Due diligence report: {legalName ?? streamingLei}
          </h1>
        )}

        {streamingLei && (
          <div className="mb-6 rounded-oo border border-oo-rule bg-white">
          <SubjectCard
            lei={streamingLei}
            legalName={legalName}
            jurisdiction={subjectJurisdiction}
            replayedAt={replayedAt}
            onRefresh={() => lookupLei(streamingLei, { refresh: true })}
            identifierSources={leiConfirmedSourceCount}
            onShowIdentifiers={showCrossSourceIdentifiers}
            status={statusChip(subjectProfile, sourceNameIndex)}
            pdfBusy={pdfBusy}
            mdBusy={mdBusy}
            onPdf={downloadPdf}
            onMarkdown={downloadMarkdown}
            exportError={exportError}
          />
        {/* ── The answer-first layer (Phase 122) ─────────────────────────
            Subject, then what the check found and how much of it ran, then
            the evidence. Both halves render from the same risk_signals
            event as each other and as the sentence, so a signal count and
            the count of screens that failed cannot drift apart on screen.
            One card with the subject (Phase 126): the verdict is what the
            subject amounts to, not a separate object floating beneath it. */}
          <VerdictStrip
            verdict={verdict}
            riskSignals={riskCodes}
            contextSignals={contextCodes}
            degraded={degradedSources}
            sourcesAnswered={answeredApplicable}
            sourcesApplicable={applicableSources.length}
            registryTotal={sourcesQuery.data?.sources.length ?? null}
            jurisdiction={subjectJurisdiction}
            graphShape={graphShape}
            onOpenNetwork={() => selectMode("full")}
            screening={streaming}
            onRerun={
              streamingLei && !streaming
                ? () => lookupLei(streamingLei, { refresh: true })
                : undefined
            }
          />
          </div>
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
        {streamingLei && (
          <div
            role="tablist"
            aria-label="Check mode"
            /* No bottom margin: the tab strip claims the card beneath it, and
               a 24px gap between them breaks the claim — the active tab's
               white edge has to meet the card's. The honesty notices that can
               sit between the two carry their own top margin instead, so they
               are the exception rather than the default spacing. */
            /* Phase 157: below `sm` the strip is a 2×2 grid of stacked
               icon-over-label cells — the pattern the search-method tablist
               already uses. The one-row strip needs ~657px, so on a 390px
               phone a reader saw "QuickCheck · FullCheck · Ba…" and two of
               the four modes did not exist unless they knew to swipe. From
               `sm` up the strip is unchanged (padding eases to px-3 until
               `md` so it still fits at 700), and the wrappers collapse to
               `contents` on phones so the buttons are the grid cells. */
            className="grid grid-cols-2 overflow-hidden rounded-oo border border-oo-rule bg-oo-bg mb-3 sm:mb-0 sm:flex sm:items-end sm:gap-1 sm:overflow-x-auto sm:overflow-y-auto sm:rounded-none sm:border-0 sm:border-b sm:bg-transparent"
          >
            {MODE_TABS.map((tab, i) => {
              const active = mode === tab.id;
              // Grid lines for the phone layout: a left rule on the right-hand
              // column, a top rule on the second row. Reset at `sm`, where the
              // button's own tab border takes over.
              const cellRules = `${i % 2 === 1 ? "border-l" : ""} ${i >= 2 ? "border-t" : ""}`.trim();
              return (
                <div
                  key={tab.id}
                  className={
                    tab.topic
                      ? "contents sm:flex sm:items-end sm:pl-2 sm:ml-1 md:pl-3 md:ml-2 sm:border-l sm:border-oo-rule"
                      : "contents sm:flex sm:items-end"
                  }
                >
                  <button
                    type="button"
                    role="tab"
                    id={`tab-${tab.id}`}
                    aria-selected={active}
                    aria-controls={`panel-${tab.id}`}
                    tabIndex={active ? 0 : -1}
                    onClick={() => selectMode(tab.id)}
                    onKeyDown={(e) => {
                      // Left/Right move between tabs (WAI-ARIA tabs pattern);
                      // the roving tabIndex above keeps one stop in the
                      // sequence rather than four.
                      if (e.key !== "ArrowRight" && e.key !== "ArrowLeft") return;
                      e.preventDefault();
                      const i = MODE_TABS.findIndex((t) => t.id === mode);
                      const next =
                        e.key === "ArrowRight"
                          ? MODE_TABS[(i + 1) % MODE_TABS.length]
                          : MODE_TABS[(i - 1 + MODE_TABS.length) % MODE_TABS.length];
                      selectMode(next.id);
                      document.getElementById(`tab-${next.id}`)?.focus();
                    }}
                    className={`relative flex flex-col items-center justify-center gap-1 px-2 py-2.5 min-h-[56px] text-oo-meta border-0 border-oo-rule ${cellRules} transition-colors sm:flex-row sm:shrink-0 sm:justify-start sm:gap-2 sm:rounded-t-oo sm:px-3 md:px-4 sm:pb-3 sm:pt-3 sm:text-[14px] sm:min-h-[44px] sm:border ${
                      active
                        ? "bg-white font-bold text-oo-ink sm:-mb-px sm:border-oo-rule sm:border-b-white"
                        : "font-medium text-oo-muted hover:text-oo-ink sm:border-transparent"
                    }`}
                  >
                    {active && (
                      <span
                        aria-hidden="true"
                        className="absolute inset-x-0 top-0 h-[3px] sm:inset-x-[-1px] sm:top-[-1px] sm:rounded-t-oo"
                        style={{ background: tab.accent }}
                      />
                    )}
                    {/* The glyph takes the mode accent when active and the
                        muted text colour otherwise, via currentColor on a
                        wrapper — Icon itself never takes a colour prop, so
                        there is exactly one way to colour an icon. */}
                    <span
                      className="inline-flex shrink-0"
                      style={active ? { color: tab.accent } : undefined}
                    >
                      <Icon name={tab.icon} size={17} />
                    </span>
                    {tab.label}
                  </button>
                </div>
              );
            })}
          </div>
        )}

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
              streamingLei && !streaming
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
        {mode === "full" && streamingLei ? (
          <div id="panel-full" role="tabpanel" aria-labelledby="tab-full" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="full" tabs={MODE_TABS} />
              <Suspense
                fallback={
                  <PanelSection>
                    <p className="text-oo-small text-oo-muted italic">Loading FullCheck…</p>
                  </PanelSection>
                }
              >
                <FullCheckPanel
                  lei={streamingLei}
                  legalName={legalName}
                  signals={riskSignals}
                  onPanelError={(e) => setPanelErrors((prev) => mergePanelError(prev, e))}
                  onPanelRecovered={(panel) =>
                    setPanelErrors((prev) => clearPanelError(prev, panel))
                  }
                />
              </Suspense>
            </PanelCard>
          </div>
        ) : mode === "background" && streamingLei ? (
          <div id="panel-background" role="tabpanel" aria-labelledby="tab-background" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="background" tabs={MODE_TABS} />
              <Suspense
                fallback={
                  <PanelSection>
                    <p className="text-oo-small text-oo-muted italic">
                      Loading BackgroundCheck…
                    </p>
                  </PanelSection>
                }
              >
                <BackgroundCheckPanel
                  lei={streamingLei}
                  legalName={legalName}
                  onOpenReport={openPersonReport}
                />
              </Suspense>
            </PanelCard>
          </div>
        ) : mode === "esg" && streamingLei ? (
          <div id="panel-esg" role="tabpanel" aria-labelledby="tab-esg" tabIndex={-1}>
            <PanelCard>
              <ModeBlurb mode="esg" tabs={MODE_TABS} />
              {esgBuckets.length > 0 || pendingEsgSources.length > 0 ? (
                <EsgPanel
                  buckets={esgBuckets}
                  pendingCount={pendingEsgSources.length}
                  bodsCountMap={bodsCountMap}
                  bodsBreakdownMap={bodsBreakdownMap}
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
          <PanelCard>
        {/* The mode's own sentence, as the card's first band. The strings have
            been on MODE_TABS since Phase 122 and rendered nowhere: a tab
            labelled "QuickCheck" says what it is called, not what it does, and
            a reader arriving on a shared link has no other way to find out
            which of the four they are looking at. */}
        <ModeBlurb mode="quick" tabs={MODE_TABS} />
        {streamingLei && mode === "quick" && (
          <NarrativePanel lei={streamingLei} onExportPayload={setExportPayload} />
        )}


        {/* Risk signals, with structural context as a captioned sub-block
            inside it rather than a peer section. Two sibling sections put a
            structural observation at the same weight as an adverse finding,
            and printed the confidence legend twice on one screen. Neither the
            AMLA CDD RTS nor AMLR Annex III treats a non-EU jurisdiction as a
            risk factor in itself, so the distinction still has to be made —
            it is made by the caption, in a sentence, which is what the v2
            design does. */}
        {(riskCodes.length > 0 || contextCodes.length > 0) && mode === "quick" && (
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
                      onSelect={(s) => setSelectedSignalCode(s.code)}
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
                      sourceNames={sourceNameIndex}
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
          profileRowsForBand.length > 0) &&
          mode === "quick" && (
            <PanelSection
              id="cross-source-identifiers"
              title="Is this the right company?"
              // Shut on arrival. Identity corroboration is reassurance, and
              // reassurance that occupies a screen before anyone doubted
              // anything is in the way of the finding. The subject card's
              // "LEI confirmed by N sources" badge is what opens it —
              // which is the moment a reader is actually asking.
              open={identityOpen}
              onToggle={setIdentityOpen}
              aside={
                crossLinkedSourceCount >= 2 ? (
                  <>
                    <span className="font-semibold">
                      {crossSourceLinks.length + gleifMappedIds.length} identifier
                      {crossSourceLinks.length + gleifMappedIds.length === 1 ? "" : "s"}
                    </span>{" "}
                    matched across{" "}
                    <span className="font-semibold">{crossLinkedSourceCount} independent sources</span>
                  </>
                ) : gleifMappedIds.length > 0 ? (
                  <>
                    <span className="font-semibold">
                      {gleifMappedIds.length} identifier
                      {gleifMappedIds.length === 1 ? "" : "s"}
                    </span>{" "}
                    mapped by GLEIF
                  </>
                ) : (
                  // Never a bare title with a chevron. The band is shut by
                  // default and the affordance that opens it — the subject
                  // card's identifier badge — renders only at two or more
                  // confirming sources, so on a lookup that has only
                  // possibly-same pairs the heading has to say what is inside
                  // it itself.
                  <>
                    <span className="font-semibold">
                      {possiblySame.length} candidate pair
                      {possiblySame.length === 1 ? "" : "s"}
                    </span>{" "}
                    flagged for review
                  </>
                )
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
                  sourceNames={sourceNameIndex}
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

        {(cddBuckets.length > 0 || pendingCddSources.length > 0) && (
          <PanelSection
            title="What each source said"
            // The same figures as the verdict strip's Coverage column, from
            // the same helper, GLEIF anchor included — the GLEIF card is the
            // first one in this list, so a count that excluded it read one
            // short of the cards beneath it (Phase 156).
            aside={
              pendingCddSources.length > 0 ? (
                <span className="text-oo-blue">
                  {coverageCopy({
                    answered: answeredApplicable,
                    applicable: applicableSources.length,
                    total: sourcesQuery.data?.sources.length ?? null,
                    jurisdiction: subjectJurisdiction,
                    screening: true,
                    pending: pendingCddSources.length,
                  }).aside}
                </span>
              ) : (
                coverageCopy({
                  answered: answeredApplicable,
                  applicable: applicableSources.length,
                  total: sourcesQuery.data?.sources.length ?? null,
                  jurisdiction: subjectJurisdiction,
                  screening: false,
                }).aside
              )
            }
          >
            {streamingLei && EXAMPLE_LEIS.some((e) => e.lei === streamingLei && e.bulkBods) && (
              <div className="mb-4 flex items-start gap-3 rounded-oo border border-blue-200 bg-blue-50 px-4 py-3 text-[13px] leading-[1.6] text-blue-900">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" className="mt-0.5 h-4 w-4 shrink-0 text-blue-600" aria-hidden="true"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.657 4.03 3 9 3s9-1.343 9-3V5"/><path d="M3 12c0 1.657 4.03 3 9 3s9-1.343 9-3"/></svg>
                <span>
                  <span className="font-medium">Curated example — pre-extracted data.</span>{" "}
                  Results use Open Ownership bulk BODS datasets (UK PSC · OGL v3.0, GLEIF · CC0), not live API calls.
                  Data may not reflect the current position.{" "}
                  <button
                    type="button"
                    className="underline hover:no-underline"
                    onClick={() => {
                      if (streamingLei) {
                        setLeiInput(streamingLei);
                        setSearchMode("lei");
                        window.scrollTo({ top: 0, behavior: "smooth" });
                      }
                    }}
                  >
                    Run a live lookup →
                  </button>
                </span>
              </div>
            )}
            <div className="space-y-4">
              {cddBuckets.map((b) => (
                <div key={b.sourceId} id={`source-${b.sourceId}`} className="scroll-mt-4">
                  <SourceBucketCard
                    bucket={b}
                    lei={streamingLei ?? undefined}
                    riskByHit={riskByHit}
                    subjectSignals={riskSignals}
                    bodsCountMap={bodsCountMap}
                    bodsBreakdownMap={bodsBreakdownMap}
                    onRetry={b.error ? () => retrySource(b.sourceId) : undefined}
                    retrying={retryingSources.has(b.sourceId)}
                    liveness={sourceLiveness[b.sourceId]}
                    footnote={
                      b.sourceId === "gleif" && gleifChildrenInfo && gleifChildrenInfo.total > 100
                        ? `Showing the first ${gleifChildrenInfo.fetched} of ${gleifChildrenInfo.total.toLocaleString()} direct subsidiaries in BODS statements (GLEIF Level 2)`
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

        {streamingLei && (
          <SecuritiesSection
            lei={streamingLei}
            onError={(e) => setPanelErrors((prev) => mergePanelError(prev, e))}
            onRecovered={(panel) => setPanelErrors((prev) => clearPanelError(prev, panel))}
            sourceNames={sourceNameIndex}
          />
        )}


        {/* MEIP signpost — bottom of the results page, beneath the richer
            data-source cards and the ESG box. Not a BODS source. */}
        <MeipSignpost match={meip} />

        {streamingLei && !streaming && totalHits > 0 && (
          <ExportPanel
            lei={streamingLei}
            legalName={legalName}
            contributingSourceIds={[...cddBuckets, ...esgBuckets]
              .filter((b) => b.hits.some((h) => !h.is_stub))
              .map((b) => b.sourceId)}
          />
        )}
          </PanelCard>
          )}
          </div>
        )}
        </>
        )}

        {view === "sources" && (
          <SourcesPage sources={sourcesQuery.data?.sources} loading={sourcesQuery.isLoading} />
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
      </main>

      {/* GODIN ribbon — permanent attribution banner. */}
      <aside
        aria-label="GODIN — Global Open Data Integration Network"
        className="px-6 sm:px-10 lg:px-16 py-4 text-white/90 text-[13px] leading-[1.6]"
        style={{
          background:
            "linear-gradient(90deg, rgb(7, 116, 95) 0%, rgb(11, 110, 92) 100%)",
        }}
      >
        <div className="max-w-oo-page mx-auto flex flex-wrap items-center gap-x-4 gap-y-2">
          <a
            href="https://godin.gleif.org/"
            target="_blank"
            rel="noreferrer"
            aria-label="GODIN — Global Open Data Integration Network (opens in new tab)"
          >
            <img
              src="https://godin.gleif.org/images/512/14456540/GODINRGBColourWide.png"
              alt="GODIN logo"
              className="h-8 w-auto"
              style={{ filter: "brightness(0) invert(1)" }}
            />
          </a>
          <p className="flex-1 min-w-0">
            OpenCheck is built on open data and open standards from{" "}
            <a
              href="https://godin.gleif.org/"
              target="_blank"
              rel="noreferrer"
              className="underline underline-offset-2 font-medium hover:text-white"
            >
              GODIN members
            </a>{" "}
            and others, and demonstrates the kind of interoperability GODIN
            exists to enable.{" "}
            <button
              type="button"
              onClick={() => navigate("behind")}
              className="underline underline-offset-2 font-medium hover:text-white"
            >
              How it works →
            </button>
          </p>
        </div>
      </aside>

      <footer className="border-t border-oo-rule bg-oo-bg px-6 sm:px-10 lg:px-16 pt-8 pb-6">
        <div className="max-w-oo-page mx-auto">
          {/* Two-column grid: brand + tagline left, link groups right */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-8 sm:gap-4">
            {/* Left: brand + tagline */}
            <div>
              <div className="font-head font-bold text-[15px] text-oo-ink">
                Open<span className="text-oo-blue">Check</span>
              </div>
              <p className="mt-2 text-[12px] text-oo-muted leading-relaxed max-w-[220px]">
                Customer due diligence checks powered by the Legal Entity
                Identifier and open standards.
              </p>
            </div>
            {/* Right: link groups */}
            <div className="flex gap-10 sm:justify-end">
              <div>
                <h3 className="font-body text-[10px] font-medium tracking-widest uppercase text-oo-muted mb-3">
                  Project
                </h3>
                <a
                  href="/api"
                  onClick={(e) => { e.preventDefault(); navigate("api"); }}
                  className="block font-mono text-[12px] text-oo-blue hover:text-oo-burst mb-2"
                >
                  API
                </a>
                <a
                  href="/changelog"
                  onClick={(e) => { e.preventDefault(); navigate("changelog"); }}
                  className="block font-mono text-[12px] text-oo-blue hover:text-oo-burst mb-2"
                >
                  Changelog
                </a>
                <a
                  href="https://github.com/StephenAbbott/opencheck"
                  target="_blank"
                  rel="noreferrer"
                  className="block font-mono text-[12px] text-oo-blue hover:text-oo-burst mb-2"
                >
                  GitHub
                </a>
                <a
                  href="/sources"
                  onClick={(e) => { e.preventDefault(); navigate("sources"); }}
                  className="block font-mono text-[12px] text-oo-blue hover:text-oo-burst mb-2"
                >
                  Sources
                </a>
                <a
                  href="/about"
                  onClick={(e) => { e.preventDefault(); navigate("behind"); }}
                  className="block font-mono text-[12px] text-oo-blue hover:text-oo-burst"
                >
                  Behind the scenes
                </a>
              </div>
              <div>
                <h3 className="font-body text-[10px] font-medium tracking-widest uppercase text-oo-muted mb-3">
                  Legal
                </h3>
                <a
                  href="https://github.com/StephenAbbott/opencheck?tab=License-1-ov-file"
                  target="_blank"
                  rel="noreferrer"
                  className="block font-mono text-[12px] text-oo-blue hover:text-oo-burst mb-2"
                >
                  MIT licence
                </a>
                <a
                  href="https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md"
                  target="_blank"
                  rel="noreferrer"
                  className="block font-mono text-[12px] text-oo-blue hover:text-oo-burst"
                >
                  ATTRIBUTIONS.md
                </a>
              </div>
            </div>
          </div>
          {/* Bottom strip */}
          <div className="mt-8 pt-4 border-t border-oo-rule text-[11px] text-oo-muted font-mono">
            Third-party data is licensed per source — see{" "}
            <a
              href="https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md"
              target="_blank"
              rel="noreferrer"
              className="text-oo-blue hover:text-oo-burst"
            >
              ATTRIBUTIONS.md
            </a>{" "}
            for details.
          </div>
        </div>
      </footer>
    </div>
  );
}

// ---------------------------------------------------------------------
// Source counter strip — animated count-up, shown below header description
// ---------------------------------------------------------------------

/**
 * Animates a number from 0 to `target` over `duration` ms.
 * Returns the current display value.
 */

// ---------------------------------------------------------------------
// Behind the Scenes page (Phase 5)
// Explains OpenCheck's architecture, standards spine, and GODIN thesis.
// ---------------------------------------------------------------------

function BtsCard({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="bg-white border border-oo-rule rounded-oo p-6">
      <h3 className="font-head font-bold text-[17px] text-oo-ink mb-3 leading-snug">
        {title}
      </h3>
      {children}
    </div>
  );
}

function BtsBadge({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-block font-mono text-[10px] bg-oo-bg border border-oo-rule rounded px-1.5 py-0.5 text-oo-ink mr-1 mb-1">
      {children}
    </span>
  );
}

// ---------------------------------------------------------------------
// API page — documents the read-only REST surface (api.opencheck.world).
// ---------------------------------------------------------------------

function CopyField({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex items-center gap-3 bg-oo-bg border border-oo-rule rounded-oo px-3 py-2">
      <code className="font-mono text-[13px] text-oo-ink flex-1 break-all">{value}</code>
      <button
        type="button"
        onClick={() => {
          navigator.clipboard?.writeText(value);
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1200);
        }}
        className="text-[11px] font-medium text-oo-blue hover:text-oo-burst shrink-0"
      >
        {copied ? "Copied" : "Copy"}
      </button>
      {/* Always-mounted status region so the copy confirmation is announced */}
      <span role="status" className="sr-only">
        {copied ? "Copied to clipboard" : ""}
      </span>
    </div>
  );
}

function ApiEndpoint({
  path,
  children,
  params,
  method = "GET",
}: {
  path: string;
  children: React.ReactNode;
  params?: [string, string][];
  method?: "GET" | "POST" | "PUT";
}) {
  const methodClasses =
    method === "GET"
      ? "bg-emerald-50 text-emerald-700 border-emerald-200"
      : "bg-blue-50 text-blue-700 border-blue-200";
  return (
    <div className="py-3.5 border-t border-oo-rule first:border-t-0 first:pt-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <span className={`font-mono text-[10px] font-semibold rounded px-1.5 py-0.5 border ${methodClasses}`}>
          {method}
        </span>
        <code className="font-mono text-[13px] text-oo-ink break-all">{path}</code>
      </div>
      <p className="text-[13px] leading-[1.7] text-oo-muted mt-1.5">{children}</p>
      {params && params.length > 0 && (
        <dl className="mt-2 space-y-1">
          {params.map(([k, v]) => (
            <div key={k} className="flex gap-2 text-[12.5px] leading-[1.6]">
              <dt className="font-mono text-oo-blue shrink-0">{k}</dt>
              <dd className="text-oo-muted">{v}</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  );
}

function ApiPage() {
  const base = BASE_URL || "https://api.opencheck.world";
  const mono = "font-mono text-[12px] bg-oo-bg px-1 rounded";
  return (
    <section aria-labelledby="api-heading">
      <h2
        id="api-heading"
        className="font-head font-bold text-[clamp(1.35rem,3vw,1.8rem)] text-oo-ink mb-2 leading-tight"
      >
        API
      </h2>
      <p className="text-[14px] leading-[1.75] text-oo-muted mb-6 max-w-2xl">
        OpenCheck exposes a small, read-only REST API. Every endpoint is a{" "}
        <code className={mono}>GET</code> that returns JSON — except{" "}
        <code className={mono}>/export</code> (a downloadable bundle) and the
        streaming endpoints (Server-Sent Events). Every result is expressed in the{" "}
        <a
          href="https://standard.openownership.org/en/0.4.0/"
          target="_blank"
          rel="noreferrer"
          className="underline text-oo-blue hover:text-oo-burst"
        >
          Beneficial Ownership Data Standard (BODS) v0.4
        </a>
        . No API key is required to read. OpenCheck also runs a{" "}
        <strong className="text-oo-ink font-semibold">Model Context Protocol (MCP)</strong>{" "}
        server, so AI agents can call the same pipeline as typed tools — see below.
      </p>

      <div className="mb-8 max-w-2xl">
        <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
          Base URL
        </div>
        <CopyField value={base} />
      </div>

      <div
        className="grid gap-6"
        style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 560px), 1fr))" }}
      >
        <BtsCard title="Lookup">
          <ApiEndpoint
            path="/lookup?lei=<LEI>"
            params={[
              ["lei", "20-character ISO 17442 LEI (required)."],
              ["deepen_top", "How many top hits to fully fetch + map + assess (default 3)."],
            ]}
          >
            <strong className="text-oo-ink font-semibold">Primary entry point.</strong>{" "}
            Resolves the company across every source and returns a unified BODS v0.4
            view — subject, related people and entities, ownership-or-control
            relationships, cross-source links and risk signals. The response also
            carries <code className={mono}>degraded_sources</code> (empty when every
            screen completed): derived checks — related-party sanctions/PEP
            screening, ICIJ offshore-leaks reconciliation — that did not fully run,
            each with <code className={mono}>source_id</code>, <code className={mono}>check</code>,{" "}
            <code className={mono}>affected_signals</code>, a counts-only{" "}
            <code className={mono}>detail</code> and a closed-vocabulary{" "}
            <code className={mono}>reason</code> (<code className={mono}>upstream_error</code>,{" "}
            <code className={mono}>timeout</code>, <code className={mono}>not_configured</code>,{" "}
            <code className={mono}>rate_limited</code>). An empty{" "}
            <code className={mono}>risk_signals</code> list alongside a non-empty{" "}
            <code className={mono}>degraded_sources</code> list is not a clean screen.
          </ApiEndpoint>
          <ApiEndpoint path="/lookup-stream?lei=<LEI>">
            The same synthesis streamed as Server-Sent Events (<code className={mono}>gleif_done</code>,
            per-source <code className={mono}>hit</code> / <code className={mono}>source_error</code>,
            then <code className={mono}>done</code>) so a client can render progressively.
            The <code className={mono}>risk_signals</code> event carries the same{" "}
            <code className={mono}>degraded_sources</code> field as <code className={mono}>/lookup</code>.
          </ApiEndpoint>
          <ApiEndpoint path="/lookup-source?lei=<LEI>&source_id=<id>">
            Re-run a single source for an existing lookup (the per-source “retry” in the UI).
          </ApiEndpoint>
          <ApiEndpoint path="/batch-stream?leis=<LEI,LEI,…>">
            Screen up to 20 LEIs in one request — the same pipeline as{" "}
            <code className={mono}>/lookup</code>, looped two at a time, streamed as{" "}
            <code className={mono}>batch_start</code>, one <code className={mono}>row_done</code> or{" "}
            <code className={mono}>row_failed</code> per LEI in completion order, then{" "}
            <code className={mono}>batch_done</code>. A failed row is a row with{" "}
            <code className={mono}>degraded: true</code>, never a dropped one. Heavy rate tier.
          </ApiEndpoint>
          <ApiEndpoint path="/batch-export?leis=<LEI,LEI,…>">
            The same list as one zip: <code className={mono}>bundle.json</code> (every company’s
            BODS statements, de-duplicated by <code className={mono}>statementId</code>),{" "}
            <code className={mono}>rows.csv</code>, <code className={mono}>manifest.json</code> and a{" "}
            <code className={mono}>LICENSES.md</code> over the union of sources — the most
            restrictive licence in the set applies to the whole bundle. Rows that could not be
            screened are listed in the manifest and the CSV, never omitted. Heavy rate tier.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="MCP server — for AI agents">
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
            OpenCheck speaks the{" "}
            <a
              href="https://modelcontextprotocol.io"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Model Context Protocol
            </a>
            , exposing the same pipeline as typed tools an AI agent can call
            directly — no glue code. It uses streamable HTTP, needs no API key,
            and carries the same source licence notices as the REST API.
          </p>
          <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
            Endpoint
          </div>
          <CopyField value={`${base}/mcp`} />
          <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mt-4 mb-2">
            Tools
          </div>
          <dl className="space-y-1.5">
            {([
              ["opencheck_search", "Find a company’s LEI from a name or free text."],
              ["opencheck_resolve_national_id", "Resolve a national company-registration number to its LEI."],
              ["opencheck_lookup", "Due diligence by LEI: identity, identifiers, risk signals, source coverage."],
              ["opencheck_batch_lookup", "Up to 20 LEIs at once: one compact row each, with failed rows kept apart."],
              ["opencheck_export_bods", "The full ownership-and-control graph as BODS v0.4 statements."],
              ["opencheck_list_sources", "Inventory of the data sources, with licence and live status."],
            ] as [string, string][]).map(([name, desc]) => (
              <div key={name} className="flex gap-2 text-[12.5px] leading-[1.6]">
                <dt className="font-mono text-oo-blue shrink-0 break-all">{name}</dt>
                <dd className="text-oo-muted">{desc}</dd>
              </div>
            ))}
          </dl>
          <p className="text-[13px] leading-[1.7] text-oo-muted mt-4">
            Add it as a custom connector in any MCP client (e.g. Claude Desktop →
            Settings → Connectors). It is discoverable via{" "}
            <a
              href="https://agenticresourcediscovery.org/"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Agentic Resource Discovery
            </a>
            ; the server descriptor is at{" "}
            <code className={mono}>/.well-known/mcp.json</code>.
          </p>
        </BtsCard>

        <BtsCard title="Search &amp; drill-down">
          <ApiEndpoint path="/search?q=<query>&kind=<entity|person>">
            Free-text fan-out search across every source. Power-user / debugging path;
            the LEI-anchored <code className={mono}>/lookup</code> is the precise one.
          </ApiEndpoint>
          <ApiEndpoint path="/stream?q=<query>&kind=<...>">
            The same search, streamed as Server-Sent Events.
          </ApiEndpoint>
          <ApiEndpoint path="/report?q=<query>&kind=<...>">
            One-shot free-text synthesis (the pre-LEI flow): search, reconcile, deepen
            the top hits, assess risk.
          </ApiEndpoint>
          <ApiEndpoint path="/deepen?source=<id>&hit_id=<id>">
            The full record for a single hit, mapped to BODS statements, with its risk signals.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Export &amp; licensing">
          <ApiEndpoint
            /* Generated from the shared EXPORT_FORMATS list rather than typed
               out: this line has drifted from the backend twice (Phase 81
               fixed it once), and a reference page that under-reports the API
               is worse than no reference page. */
            path={`/export?lei=<LEI>&format=<${EXPORT_FORMATS.join("|")}>&subsidiaries=<bool>`}
            params={[
              [
                "format",
                "zip ships bods.json + bods.jsonl + bods.xml + senzing.jsonl + ftm.jsonl + manifest.json + LICENSES.md; json / jsonl / xml return the statements only; csv returns the entity / person / ownership-edge tables as a zip; cypher returns a Neo4j Cypher script; senzing returns Senzing JSON entity records for entity resolution; ftm returns FollowTheMoney entities for OpenSanctions / OpenAleph workflows; gql returns a BigQuery property-graph zip; amlai returns Google AML AI input tables; rdf returns BODS RDF as TriG — one named graph per statement, a canonical licence URI on every statement, and bods:Annotation in two separated layers: the register's own words (nature-of-control codes, imprecise-date notes) in each statement's graph, and OpenCheck's risk signals and entity-resolution links in a separate analysis graph.",
              ],
              [
                "subsidiaries",
                "opt-in (default false): also fold the GLEIF subsidiary network (direct + ultimate children) into the bundle. Off by default — a large group can add hundreds of statements.",
              ],
            ]}
          >
            A reproducible, downloadable BODS bundle. Shares its synthesis with{" "}
            <code className={mono}>/lookup</code>, so the export mirrors exactly what you
            saw on screen.
          </ApiEndpoint>
          <ApiEndpoint path="/license-matrix?sources=<a,b,c>">
            Per-source licence terms (commercial use, attribution, share-alike) plus a
            combined commercial-use assessment for the listed sources — the data behind
            the export “licensing assistant”.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="AI narrative &amp; analyst sign-off">
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
            An optional AI-written plain-English summary of a lookup, plus the
            defensible audit trail an analyst builds around it. Generated only on
            request; each run is identified by a <code className={mono}>run_id</code>{" "}
            so dispositions stay pinned to the exact narrative they signed off.
          </p>
          <ApiEndpoint
            path="/narrative?lei=<LEI>"
            params={[
              ["deepen_top", "How many top hits to deepen before summarising (default 5)."],
              ["refresh", "Bypass the short-lived replay cache (default false)."],
            ]}
          >
            A cited, plain-English summary of the subject built from the same BODS
            synthesis as <code className={mono}>/lookup</code> — returns the{" "}
            <code className={mono}>summary</code>, per-sentence{" "}
            <code className={mono}>claims</code> with source citations, stated{" "}
            <code className={mono}>limitations</code>, the <code className={mono}>model</code> /{" "}
            <code className={mono}>prompt_version</code>, and a <code className={mono}>run_id</code>{" "}
            identifying this exact run.
          </ApiEndpoint>
          <ApiEndpoint
            method="PUT"
            path="/narrative/dispositions"
            params={[
              ["body", "{ lei, run_id, prompt_version, model, dispositions: [{ claim_id, status, comment }] }"],
              ["status", "accepted | disputed | needs_review — the analyst's decision per claim."],
            ]}
          >
            Persist the analyst’s claim dispositions for one narrative run (whole-sheet,
            last-write-wins). No model call — pure metadata around an existing narrative;
            <code className={mono}>decided_at</code> / <code className={mono}>updated_at</code>{" "}
            are stamped server-side.
          </ApiEndpoint>
          <ApiEndpoint
            path="/narrative/dispositions?lei=<LEI>&run_id=<id>"
          >
            The stored disposition sheet for a <code className={mono}>(lei, run_id)</code> run,
            or 404 if none has been saved. Rehydrates the sign-off state when the page reloads.
          </ApiEndpoint>
          <ApiEndpoint method="POST" path="/export/pdf">
            An accessible (tagged PDF/UA-1) due-diligence report for an LEI, built from
            the same cached lookup as <code className={mono}>/lookup</code>. The request
            body embeds the <code className={mono}>narrative</code> the client already
            generated (no fresh model call) and, when present, the analyst’s{" "}
            <code className={mono}>dispositions</code>, so the accept / dispute / needs-review
            decisions and notes are rendered into the report’s audit trail.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Enrichments — on demand">
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
            Heavier, source-specific views kept off the main lookup and fetched
            only when asked. Each returns JSON; results are cached.
          </p>
          <ApiEndpoint
            path="/subsidiaries?lei=<LEI>&format=<summary|bods>"
            params={[
              [
                "format",
                "summary (counts + tagged children, default) or bods (adds the BODS statements for the graph / export).",
              ],
            ]}
          >
            A company’s GLEIF Level-2{" "}
            <strong className="text-oo-ink font-semibold">subsidiary network</strong> —
            direct and ultimate children merged and tagged{" "}
            <code className={mono}>direct</code> / <code className={mono}>ultimate</code> /{" "}
            <code className={mono}>both</code>, with exact counts (even when the child
            list is page-capped), a jurisdiction spread, and a{" "}
            <code className={mono}>render_mode</code> hint (graph ≤ 150 nodes, else table).
          </ApiEndpoint>
          <ApiEndpoint path="/securities?lei=<LEI>&page=<n>">
            Securities (ISINs) mapped to the LEI from GLEIF + OpenFIGI, flagging any that
            are <strong className="text-oo-ink font-semibold">sanctioned</strong> (incl.
            GLEIF’s blind spot for issuers with no listed ISINs).
          </ApiEndpoint>
          <ApiEndpoint path="/history?lei=<LEI>&include_noise=<bool>">
            The <strong className="text-oo-ink font-semibold">Time Machine</strong>{" "}
            change-over-time timeline (GLEIF + Companies House) on one shared model;{" "}
            <code className={mono}>include_noise</code> folds in administrative changes.
          </ApiEndpoint>
          <ApiEndpoint path="/nz-associations?company_number=<n>">
            For a New Zealand company, the other companies its directors and shareholders
            are linked to — a nominee / mass-directorship review, graded by address.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Catalogue &amp; health">
          <ApiEndpoint path="/sources">
            Inventory of the source adapters: id, name, licence, description, category,
            and whether each is live.
          </ApiEndpoint>
          <ApiEndpoint path="/health">Liveness probe.</ApiEndpoint>
        </BtsCard>

        <BtsCard title="Quick start">
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
            Search for a company by its LEI and get the unified BODS view:
          </p>
          <CopyField value={`curl "${base}/lookup?lei=HWUPKR0MPOU8FGXBT394"`} />
          <p className="text-[13px] leading-[1.7] text-oo-muted mt-4">
            Full request/response detail is in{" "}
            <a
              href="https://github.com/StephenAbbott/opencheck/blob/main/docs/how-it-works.md#api-surface"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              docs/how-it-works.md
            </a>
            .
          </p>
        </BtsCard>
      </div>
    </section>
  );
}

function BehindTheScenesPage() {
  return (
    <section aria-labelledby="bts-heading">
      <h2
        id="bts-heading"
        className="font-head font-bold text-[clamp(1.35rem,3vw,1.8rem)] text-oo-ink mb-2 leading-tight"
      >
        Behind the Scenes
      </h2>
      <p className="text-[14px] leading-[1.75] text-oo-muted mb-8 max-w-2xl">
        OpenCheck is a proof-of-concept that shows what becomes possible when
        open data is anchored on the Legal Entity Identifier (LEI) and
        expressed in a common standard. This page explains how it works and
        the open ecosystem it draws on.
      </p>

      <div className="grid gap-6" style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 500px), 1fr))" }}>

        {/* Data pipeline */}
        <BtsCard title="How a lookup works">
          <p className="text-[13.5px] leading-[1.75] text-oo-muted mb-4">
            Paste or search for a Legal Entity Identifier. OpenCheck then:
          </p>
          <ol className="text-[13.5px] leading-[1.75] text-oo-muted space-y-2 list-none">
            {[
              ["01", "Resolves the LEI via GLEIF", "gets the canonical legal name, jurisdiction, registration authority code, and related identifiers."],
              ["02", "Derives bridge IDs", "maps the GLEIF record to national register IDs (UK company number, Dutch KvK number, Czech IČO, …) and cross-references (Wikidata Q-ID, CUSIP)."],
              ["03", "Fans out in parallel", "each source adapter receives whichever identifier it understands and fetches independently; results stream back as they arrive."],
              ["04", "Maps to BODS 0.4", "every source payload is run through a dedicated mapper, producing entity, person, and ownership/control statements in the Beneficial Ownership Data Standard."],
              ["05", "Aggregates risk signals", "the unified BODS graph is inspected for structural risk patterns: complex chains, non-EU jurisdiction, sanctions exposure."],
            ].map(([n, bold, rest]) => (
              <li key={n} className="flex gap-3">
                <span className="font-mono text-[11px] text-oo-blue shrink-0 mt-0.5">{n}</span>
                <span><strong className="text-oo-ink font-semibold">{bold}</strong> — {rest}</span>
              </li>
            ))}
          </ol>
        </BtsCard>

        {/* QuickCheck vs FullCheck */}
        <BtsCard title="The four checks">
          <p className="text-[13.5px] leading-[1.75] text-oo-muted mb-3">
            Every lookup opens in{" "}
            <strong className="text-oo-ink font-semibold">QuickCheck</strong> — a
            fast screen of the subject itself: who it is, the identifiers each
            source independently confirms, and any risk signals on the company
            and its immediate people. It also flags records that are{" "}
            <strong className="text-oo-ink font-semibold">likely the same
            entity</strong> — an exact name and jurisdiction match with no shared
            identifier — as a suggestion to review, never a silent merge.
          </p>
          <p className="text-[13px] text-oo-muted leading-[1.7]">
            Switch to{" "}
            <strong className="text-oo-ink font-semibold">FullCheck</strong> to
            walk the wider ownership-and-control network: OpenCheck expands
            layer by layer through owners and subsidiaries, overlays every
            source on one canvas, and{" "}
            <strong className="text-oo-ink font-semibold">reconciles</strong> the
            same real-world company across sources into a single node — so three
            sources agreeing reads as corroboration. The network can be exported
            as BODS v0.4 or projected to Neo4j.
          </p>
          {/* Phase 122: BackgroundCheck had shipped un-documented here, and
              Climate & ESG was not a mode at all — it rendered as a section
              inside QuickCheck. Both are tabs now, so both are described. */}
          <p className="text-[13px] text-oo-muted leading-[1.7] mt-3">
            <strong className="text-oo-ink font-semibold">BackgroundCheck</strong>{" "}
            screens the people rather than the company: the officers, directors
            and beneficial owners named in the records, each checked against the
            sanctions, PEP and offshore-leaks sources that can answer for a
            person. A person the sources could not answer for is reported as
            unscreened, never as clear.
          </p>
          <p className="text-[13px] text-oo-muted leading-[1.7] mt-3">
            <strong className="text-oo-ink font-semibold">Climate &amp; ESG</strong>{" "}
            is a different question from the other three, which is why it sits
            apart in the tab strip: not who owns the company, but what it does —
            the emissions and asset records published about it and the assets it
            controls. An empty result there is an absence of published records,
            not a finding about its emissions.
          </p>
        </BtsCard>

        {/* BODS spine */}
        <BtsCard title="The BODS spine">
          <p className="text-[13.5px] leading-[1.75] text-oo-muted mb-3">
            All data converges on the{" "}
            <a
              href="https://standard.openownership.org/en/0.4.0/"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Beneficial Ownership Data Standard (BODS) v0.4
            </a>
            , maintained by{" "}
            <a
              href="https://www.openownership.org/"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Open Ownership
            </a>
            . BODS defines three statement types:
          </p>
          <dl className="text-[13px] space-y-2">
            {[
              ["Entity statement", "blue", "A legal entity — company, trust, foundation."],
              ["Person statement", "violet", "A natural person (or anonymous/unknown person)."],
              ["Ownership/Control statement", "teal", "A relationship linking an interested party to a subject entity, with typed interests and share bands."],
            ].map(([term, colour, def]) => (
              <div key={term as string} className="flex gap-2 items-baseline">
                <dt className={`shrink-0 font-semibold text-[11px] px-1.5 py-0.5 rounded border font-mono
                  ${colour === "blue" ? "bg-blue-50 text-blue-700 border-blue-200" : ""}
                  ${colour === "violet" ? "bg-violet-50 text-violet-700 border-violet-200" : ""}
                  ${colour === "teal" ? "bg-teal-50 text-teal-700 border-teal-200" : ""}
                `}>{term}</dt>
                <dd className="text-oo-muted">{def}</dd>
              </div>
            ))}
          </dl>
          <p className="text-[13px] text-oo-muted mt-3 leading-[1.7]">
            Each source has a dedicated mapper in{" "}
            <code className="font-mono text-[11px] bg-oo-bg px-1 rounded">opencheck/bods/mapper.py</code>.
            Statement IDs are deterministic (SHA-256 of source + type + local key)
            so re-running a lookup always produces the same IDs — stable for
            deduplication and graph visualisation.
          </p>
        </BtsCard>

        {/* GLEIF + LEI */}
        <BtsCard title="GLEIF and the Legal Entity Identifier">
          <p className="text-[13.5px] leading-[1.75] text-oo-muted mb-3">
            The{" "}
            <a
              href="https://www.gleif.org/"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Global Legal Entity Identifier Foundation (GLEIF)
            </a>{" "}
            maintains the global LEI registry under ISO 17442. Every LEI record
            carries a Registration Authority code (e.g.{" "}
            <code className="font-mono text-[11px] bg-oo-bg px-1 rounded">RA000586</code>{" "}
            for Companies House) that OpenCheck uses to route to the right
            national register adapter.
          </p>
          <p className="text-[13px] text-oo-muted leading-[1.7]">
            Name search uses the{" "}
            <a
              href="https://mcp.gleif.org/gleif-api/mcp"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              GLEIF MCP server
            </a>
            . The full GLEIF ownership graph (Level 2 data) is available as a
            BODS 0.4 dataset and ingested via the{" "}
            <code className="font-mono text-[11px] bg-oo-bg px-1 rounded">bods_gleif</code>{" "}
            adapter.
          </p>
        </BtsCard>

        {/* GODIN */}
        <BtsCard title="GODIN — why interoperability matters">
          <p className="text-[13.5px] leading-[1.75] text-oo-muted mb-3">
            The{" "}
            <a
              href="https://godin.gleif.org/"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Global Open Data Integration Network (GODIN)
            </a>{" "}
            is a collaborative effort to enhance global data interoperability
            and accessibility by connecting organisations that publish open data
            or create open data standards and aligning data to a global
            framework like the Global Legal Entity Identifier (LEI) System.
          </p>
          <p className="text-[13px] text-oo-muted leading-[1.7]">
            OpenCheck is a concrete demonstration of the GODIN thesis: a single
            LEI, combined with open standards like BODS, lets a user pull
            information from 20+ independent registries into a unified,
            structured view — without any proprietary data agreements.
          </p>
        </BtsCard>

        {/* Tech stack */}
        <BtsCard title="Technical stack">
          <p className="text-[13.5px] leading-[1.75] text-oo-muted mb-3">
            OpenCheck is fully open-source under the MIT license.
          </p>
          <div className="space-y-3 text-[13px] text-oo-muted">
            <div>
              <p className="font-semibold text-oo-ink text-[12px] uppercase tracking-wide mb-1">Backend</p>
              <div>
                <BtsBadge>Python 3.12</BtsBadge>
                <BtsBadge>FastAPI</BtsBadge>
                <BtsBadge>Pydantic v2</BtsBadge>
                <BtsBadge>httpx</BtsBadge>
                <BtsBadge>SQLite (local caches)</BtsBadge>
              </div>
            </div>
            <div>
              <p className="font-semibold text-oo-ink text-[12px] uppercase tracking-wide mb-1">Frontend</p>
              <div>
                <BtsBadge>React 18 + TypeScript</BtsBadge>
                <BtsBadge>Vite</BtsBadge>
                <BtsBadge>Tailwind CSS</BtsBadge>
                <BtsBadge>@openownership/bods-dagre</BtsBadge>
                <BtsBadge>TanStack Query</BtsBadge>
              </div>
            </div>
            <div>
              <p className="font-semibold text-oo-ink text-[12px] uppercase tracking-wide mb-1">Standards</p>
              <div>
                <BtsBadge>ISO 17442 (LEI)</BtsBadge>
                <BtsBadge>BODS v0.4</BtsBadge>
                <BtsBadge>GLEIF Level 1 + 2</BtsBadge>
                <BtsBadge>FATF R24 guidance</BtsBadge>
              </div>
            </div>
          </div>
        </BtsCard>

        {/* Links */}
        <BtsCard title="Resources and further reading">
          <ul className="text-[13.5px] space-y-2.5">
            {[
              ["OpenCheck on GitHub", "https://github.com/StephenAbbott/opencheck"],
              ["BODS v0.4 documentation", "https://standard.openownership.org/en/0.4.0/"],
              ["Open Ownership", "https://www.openownership.org/"],
              ["GLEIF — Global LEI Foundation", "https://www.gleif.org/"],
              ["GODIN — Global Open Data Integration Network", "https://godin.gleif.org/"],
              ["GLEIF Level 2 in BODS 0.4", "https://www.openownership.org/en/news/global-legal-entity-ownership-data-available-in-line-with-latest-version-of-data-standard/"],
              ["FATF Recommendation 24 guidance (beneficial ownership)", "https://www.fatf-gafi.org/en/publications/Fatfrecommendations/Guidance-Beneficial-Ownership-Legal-Persons.html"],
            ].map(([label, href]) => (
              <li key={href as string}>
                <a
                  href={href as string}
                  target="_blank"
                  rel="noreferrer"
                  className="underline text-oo-blue hover:text-oo-burst leading-snug"
                >
                  {label}
                </a>
              </li>
            ))}
          </ul>
        </BtsCard>

        {/* Privacy / analytics (Phase 89) */}
        <BtsCard title="Privacy & analytics">
          <p className="text-[13.5px] leading-[1.75] text-oo-muted">
            OpenCheck counts visits with{" "}
            <a
              href="https://www.goatcounter.com/"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              GoatCounter
            </a>
            , an open-source, cookie-less analytics tool: no cookies, no
            fingerprinting, no cross-site tracking. The entities you look up
            stay private &mdash; recorded paths are rolled up to fixed buckets
            (for example every lookup counts as <code>/lookup</code>), so no
            Legal Entity Identifier, person name or query string is ever
            sent to analytics. Feature usage is counted as anonymous event
            names only.
          </p>
        </BtsCard>

      </div>
    </section>
  );
}

// ---------------------------------------------------------------------
// Source counter strip
// ---------------------------------------------------------------------


// ---------------------------------------------------------------------
// Small layout primitives — design system "eyebrow" labels & dividers
// ---------------------------------------------------------------------

/**
 * Small uppercase section heading per BO design system: 10–11px,
 * weight 600, letter-spacing 0.12em, muted grey, with a hairline
 * bottom border that lines up the section visually.
 */
function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <h2 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted border-b border-oo-rule pb-2 mb-4">
      {children}
    </h2>
  );
}

function ExampleLeiPicker({
  onPick,
  disabled,
}: {
  onPick: (lei: string) => void;
  disabled: boolean;
}) {
  return (
    <section className="mb-10">
      <SectionLabel>Try a curated example</SectionLabel>
      <ul
        className="grid gap-3"
        // 280px min keeps three subjects per row at desktop widths,
        // stacks on narrow viewports.
        style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 280px), 1fr))" }}
      >
        {EXAMPLE_LEIS.map((ex) => (
          <li key={ex.lei} className="relative">
            <button
              type="button"
              disabled={disabled}
              onClick={() => onPick(ex.lei)}
              className="w-full text-left bg-white border border-oo-rule rounded-oo p-4 transition-shadow hover:shadow-oo-card disabled:opacity-50"
            >
              <div className="font-head text-[14px] font-bold text-oo-ink leading-tight pr-6">
                {ex.name}
              </div>
              {ex.hint && (
                <div className="text-[12px] text-oo-muted mt-0.5">
                  {ex.hint}
                </div>
              )}
              {ex.signals && ex.signals.length > 0 && (
                <div className="flex flex-wrap gap-1 mt-2">
                  {ex.signals.map((sig) => (
                    <RiskChip
                      key={sig.code}
                      signal={{
                        code: sig.code,
                        confidence: sig.confidence,
                        source_id: "",
                        hit_id: "",
                        summary: RISK_PRESENTATION[sig.code]?.label ?? sig.code,
                        evidence: {},
                      }}
                      compact
                      interactive={false}
                    />
                  ))}
                </div>
              )}
            </button>
            {/* Phase 122: the Neo4j CSV download that used to sit here came
                off. It is a developer affordance on the app's headline entry
                point — an icon-only link, explained by a `title` no keyboard
                or touch user can read, offering a graph-database bundle to
                someone who has not yet run their first check. The same
                download lives in the export panel, where it is one of nine
                formats and is labelled. */}
          </li>
        ))}
      </ul>
    </section>
  );
}

const HOW_IT_WORKS_STEPS = [
  {
    num: "1",
    accent: "#3d30d4" as const,
    icon: <StepKeyIcon className="h-[15px] w-[15px]" />,
    title: "One ID, the whole world",
    body: (
      <>
        Paste a 20-character{" "}
        <a
          href="https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei"
          target="_blank"
          rel="noreferrer"
          className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
        >
          Legal Entity Identifier
        </a>{" "}
        — the single key to 3 million+ entities worldwide.
      </>
    ),
    badges: null,
  },
  {
    num: "2",
    accent: "#3d30d4" as const,
    icon: <StepBridgeIcon className="h-[15px] w-[15px]" />,
    title: "We bridge to the registers",
    body: (
      <>
        GLEIF maps the LEI to national company numbers and{" "}
        <a
          href="https://www.gleif.org/en/newsroom/blog/transforming-data-into-opportunities-metric-of-the-month-mapping-network"
          target="_blank"
          rel="noreferrer"
          className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
        >
          cross-references
        </a>{" "}
        — so you skip the manual lookups.
      </>
    ),
    badges: null,
  },
  {
    num: "3",
    accent: "#3d30d4" as const,
    icon: <StepNetworkIcon className="h-[15px] w-[15px]" />,
    title: "40 open sources, in parallel",
    body: (
      <>
        Each source is queried with the identifier it understands, and the
        results are normalised in line with the Beneficial Ownership Data
        Standard.
      </>
    ),
    badges: null,
  },
  {
    num: "4",
    accent: "#3d30d4" as const,
    icon: <StepShieldIcon className="h-[15px] w-[15px]" />,
    title: "Risk, explained and exportable",
    body: (
      <>
        Deterministic risk signals — sanctions, flagged jurisdictions, complex
        ownership and more — follow the EU AMLA's draft due-diligence standards,
        and a plain-English AI summary explains them with every statement linked
        to its source. Take it away as an accessible PDF or raw data — or copy
        the share link, and a live summary card appears wherever you paste it.
      </>
    ),
    badges: null,
  },
] as const;

/**
 * BatchInvite — the homepage's invitation to screen a list (Phase 166).
 *
 * It replaces ShareCardShowcase, the preview of the shareable summary card.
 * The share link is still advertised — one sentence in the last "How it
 * works" step — but the homepage's second section now promotes the thing a
 * compliance officer, a journalist or an EITI secretariat actually arrives
 * with: a list. Nothing in it is a claim about the engine, so nothing here
 * can go stale the way the rendered card could (see the hardcoded-claims
 * note in CLAUDE.md).
 */
function BatchInvite({ onOpen }: { onOpen: () => void }) {
  return (
    <section className="mb-10 bg-white border border-oo-rule rounded-oo p-7">
      <SectionLabel>Screen a list</SectionLabel>
      <div className="mt-2 flex flex-col md:flex-row md:items-center gap-5">
        <p className="text-oo-small text-oo-muted md:max-w-xl">
          Arrived with a counterparty list, a portfolio, or every licence-holder in a
          register? Paste up to 20 LEIs and get{" "}
          <span className="text-oo-ink font-medium">one table</span> — register status, the
          verdict sentence and the signal count for each company, every row linking to its
          full report — and{" "}
          <span className="text-oo-ink font-medium">one file</span> to take away.
        </p>
        <a
          href="/batch"
          onClick={(e) => {
            e.preventDefault();
            onOpen();
          }}
          className={buttonClasses("primary", "md", "shrink-0 self-start md:self-center")}
        >
          Screen a list of companies
        </a>
      </div>
    </section>
  );
}

function HowItWorks() {
  return (
    <section className="mb-10 bg-white border border-oo-rule rounded-oo p-7">
      <SectionLabel>How it works</SectionLabel>
      <ol className="mt-2 max-w-2xl">
        {HOW_IT_WORKS_STEPS.map((step, i) => {
          const isLast = i === HOW_IT_WORKS_STEPS.length - 1;
          return (
            <li key={step.num} className="flex gap-5">
              {/* Left rail — circle node + connector line */}
              <div className="flex flex-col items-center flex-shrink-0" style={{ width: 28 }}>
                <div
                  className="flex items-center justify-center rounded-full text-white flex-shrink-0"
                  style={{ width: 28, height: 28, background: step.accent }}
                  aria-hidden="true"
                >
                  {step.icon}
                </div>
                {!isLast && (
                  <div
                    className="w-px flex-1 mt-1"
                    style={{ background: "#e2e5ea", minHeight: 20 }}
                  />
                )}
              </div>

              {/* Right content */}
              <div className={isLast ? "pb-0" : "pb-6"} style={{ paddingTop: 3 }}>
                <p className="font-head font-bold text-[14px] text-oo-ink leading-snug">
                  <span className="sr-only">{`Step ${step.num}: `}</span>
                  {step.title}
                </p>
                <p className="text-[13px] leading-[1.65] text-oo-muted mt-1.5">
                  {step.body}
                </p>
              </div>
            </li>
          );
        })}
      </ol>
    </section>
  );
}



/** Human-readable label for each reconcile bridge key. */
const SCHEME_LABELS: Record<string, string> = {
  lei: "Legal Entity Identifier (LEI)",
  wikidata_qid: "Wikidata QID",
  gb_coh: "Companies House number",
  opensanctions_id: "OpenSanctions ID",
  name: "Name match",
};

/** Short display name for a source chip — the lead clause of the registry
 *  name ("EITI — Extractive Industries…" → "EITI"). */
function shortSourceName(sourceId: string, names: Record<string, string>): string {
  const full = names[sourceId] ?? sourceId;
  return full.split(" — ")[0].split(" (")[0].trim();
}

function CrossSourceIdentifiersTable({
  links,
  gleifMapped,
  sourceNames = {},
}: {
  links: CrossSourceLink[];
  gleifMapped: { scheme: string; value: string }[];
  sourceNames?: Record<string, string>;
}) {
  const hasRows = links.length > 0 || gleifMapped.length > 0;
  if (!hasRows) return null;

  return (
    <table className="w-full text-[13px] border-collapse table-fixed">
      <thead>
        <tr>
          <th className="text-left text-[10px] font-medium tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[32%]">
            Scheme
          </th>
          <th className="text-left text-[10px] font-medium tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[32%]">
            Value
          </th>
          <th className="text-right text-[10px] font-medium tracking-widest uppercase text-oo-muted pb-2 w-[36%]">
            Confirmed by
          </th>
        </tr>
      </thead>
      <tbody>
        {links.map((link, i) => (
          <tr key={`${link.key}:${link.key_value}:${i}`} className="border-t border-oo-rule">
            <td className="py-2 pr-3 text-oo-muted">
              {SCHEME_LABELS[link.key] ?? link.key}
            </td>
            <td className="py-2 pr-3 font-mono text-[12px] text-oo-ink break-all">
              {link.key_value}
            </td>
            <td className="py-2 text-right">
              <span className="inline-flex flex-wrap gap-1 justify-end">
                {link.hits.map((h) => (
                  <button
                    key={h.source_id}
                    type="button"
                    aria-label={`${sourceLabel(h.source_id, sourceNames)} — jump to this source's results`}
                    onClick={() =>
                      document
                        .getElementById(`source-${h.source_id}`)
                        ?.scrollIntoView({ behavior: "smooth", block: "start" })
                    }
                    className="text-[11px] bg-oo-bg border border-oo-rule rounded px-1.5 py-0.5 text-oo-muted hover:text-oo-ink hover:border-[#cfd6f5] transition-colors"
                  >
                    {shortSourceName(h.source_id, sourceNames)}
                  </button>
                ))}
              </span>
            </td>
          </tr>
        ))}
        {gleifMapped.map(({ scheme, value }) => (
          <tr key={scheme} className="border-t border-oo-rule">
            <td className="py-2 pr-3 text-oo-muted">{scheme}</td>
            <td className="py-2 pr-3 font-mono text-[12px] text-oo-ink break-all">{value}</td>
            <td className="py-2 text-right">
              <span className="inline-flex items-center gap-1 text-[11px] bg-blue-50 border border-blue-200 text-blue-700 rounded px-1.5 py-0.5">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="w-3 h-3"
                  aria-hidden="true"
                >
                  <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
                  <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
                </svg>
                Mapped by GLEIF
              </span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/**
 * Renders the name-only "likely same" entity candidates surfaced by the backend
 * reconciler (exact name + jurisdiction, no shared identifier). These are
 * **suggestions for a human to review**, never confirmed merges — the certain
 * matches already appear in the cross-source identifiers table above. Renders
 * nothing when there are no candidates.
 */
// How many possibly-same pairs are visible before the rest collapse behind
// the "Show more" toggle. Multi-source subjects (e.g. DNO ASA) can flag many
// pairs, which otherwise dominates the results page.
const POSSIBLY_SAME_PREVIEW_COUNT = 2;

/**
 * Failures in the panels that fetch outside the lookup pipeline.
 *
 * Separate from DegradedScreensNotice on purpose — see lib/panelErrors.ts for
 * why these must not be merged into `degraded_sources`. Same amber, same
 * closing principle, different sentence: a section that is not on screen
 * because its fetch failed must not be read as a section with nothing in it.
 */
function PanelErrorsNotice({ errors }: { errors: PanelError[] }) {
  if (errors.length === 0) return null;
  return (
    <section
      role="status"
      aria-label="Part of this report could not be loaded"
      className="mt-6 mb-8 rounded-oo border border-oo-warn-border bg-oo-warn-bg p-5"
    >
      <h2 className="font-head font-bold text-oo-body text-oo-warn-text">
        Part of this report could not be loaded
      </h2>
      <ul className="mt-2 space-y-1.5">
        {errors.map((e) => (
          <li key={e.panel} className="text-oo-meta text-oo-warn-text leading-[1.6]">
            <span className="font-semibold">{panelLabel(e.panel)}</span> — {e.detail}. You
            are not seeing {e.missing}.
          </li>
        ))}
      </ul>
      <p className="mt-2 text-oo-meta text-oo-warn-text">
        These sections are missing from the page, not empty. Their absence is not
        evidence of absence.
      </p>
    </section>
  );
}

/** Human phrasing for the closed degradation-reason vocabulary. */
const DEGRADED_REASON_LABELS: Record<string, string> = {
  upstream_error: "the upstream service errored",
  timeout: "the upstream service timed out",
  not_configured: "the required API credential is not configured",
  rate_limited: "the upstream service rate-limited the request",
};

/**
 * The mode's own sentence, as its panel card's first band.
 *
 * The strings have been on `MODE_TABS` since Phase 122 and rendered nowhere.
 * A tab labelled "QuickCheck" says what it is called, not what it does, and a
 * reader arriving on a shared link has no other way to find out which of the
 * four they are looking at. Three of the four panels used to state it
 * themselves — in their own coloured strip, in their own words, at a heading
 * level of their own choosing — which is three chances to disagree with the
 * tab above them; this is one.
 */
function ModeBlurb({
  mode,
  tabs,
}: {
  mode: CheckMode;
  tabs: { id: CheckMode; blurb: string }[];
}) {
  const blurb = tabs.find((t) => t.id === mode)?.blurb;
  if (!blurb) return null;
  return (
    <PanelSection>
      <p className="text-oo-small text-oo-muted">{blurb}</p>
    </PanelSection>
  );
}

/**
 * Warning box for degraded upstream screens (issue #50). Sits above the
 * risk panel and renders whenever the backend reports that a derived
 * check (related-party sanctions/PEP screening, ICIJ offshore-leaks
 * reconciliation) did not fully run — including when there are zero risk
 * signals, which is precisely the case that must not pass for a clean
 * screen. Details are counts only; the backend never sends the
 * related-party names that were being screened.
 */
function DegradedScreensNotice({
  degraded,
  sourceNames = {},
  onRetry,
}: {
  degraded: DegradedSource[];
  sourceNames?: Record<string, string>;
  /** Re-runs the lookup bypassing the replay cache; absent while streaming. */
  onRetry?: () => void;
}) {
  if (degraded.length === 0) return null;
  return (
    <section
      role="status"
      aria-label="Screening incomplete"
      className="mt-6 mb-8 rounded-oo border border-amber-300 bg-amber-50 p-5"
    >
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div className="flex items-start gap-3 min-w-0">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.75"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mt-0.5 h-4 w-4 shrink-0 text-amber-600"
            aria-hidden="true"
          >
            <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z" />
            <path d="M12 9v4" />
            <path d="M12 17h.01" />
          </svg>
          <div className="min-w-0">
            <p className="font-head font-bold text-[14px] text-amber-900">
              Screening incomplete — {degraded.length} check
              {degraded.length === 1 ? "" : "s"} did not fully run
            </p>
            <ul className="mt-2 space-y-1.5 text-[12.5px] leading-[1.6] text-amber-900">
              {degraded.map((d, i) => (
                <li key={`${d.source_id}:${d.check}:${i}`}>
                  <span className="font-semibold">
                    {d.source_id === "opencheck"
                      ? "OpenCheck"
                      : shortSourceName(d.source_id, sourceNames)}
                  </span>{" "}
                  — {d.detail}{" "}
                  <span className="text-amber-800">
                    ({DEGRADED_REASON_LABELS[d.reason] ?? d.reason})
                  </span>
                  {d.affected_signals.length > 0 && (
                    <span className="ml-1.5 inline-flex flex-wrap gap-1 align-middle">
                      {d.affected_signals.map((code) => (
                        <span
                          key={code}
                          className="text-[10px] font-semibold uppercase tracking-wide bg-white/70 border border-amber-300 rounded px-1.5 py-0.5 text-amber-900"
                        >
                          {RISK_PRESENTATION[code]?.label ??
                            code.replace(/_/g, " ")}
                        </span>
                      ))}
                    </span>
                  )}
                </li>
              ))}
            </ul>
            <p className="mt-2 text-[12px] text-amber-800">
              The absence of the signals above is not evidence of absence —
              an empty result here is not a clean screen.
            </p>
          </div>
        </div>
        {onRetry && (
          <button
            type="button"
            onClick={onRetry}
            className="shrink-0 rounded border border-amber-400 px-3 py-1.5 text-[12px] font-semibold text-amber-900 transition-colors hover:bg-amber-100"
          >
            Re-run screening
          </button>
        )}
      </div>
    </section>
  );
}

function PossiblySameTable({ pairs }: { pairs: PossiblySameEntity[] }) {
  const [expanded, setExpanded] = useState(false);
  if (pairs.length === 0) return null;
  const hiddenCount = pairs.length - POSSIBLY_SAME_PREVIEW_COUNT;
  const visible =
    expanded || hiddenCount <= 0
      ? pairs
      : pairs.slice(0, POSSIBLY_SAME_PREVIEW_COUNT);
  return (
    <>
      <p className="text-[12px] text-oo-muted mb-3">
        These records share an exact name and jurisdiction but no common
        identifier, so they are <em>likely</em> the same entity — flagged for
        review, not merged automatically.
      </p>
      <table className="w-full text-[13px] border-collapse table-fixed">
        <thead>
          <tr>
            {/* Narrower Records column and tighter letter-spacing on mobile:
                at tracking-widest the single words "Jurisdiction" and
                "Confidence" are wider than an 18% column on a phone, so they
                overflow their cells and collide. break-words lets them wrap
                rather than spill. */}
            <th className="text-left text-[10px] font-medium tracking-wide sm:tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[44%] sm:w-[64%] align-bottom">
              Records
            </th>
            <th className="text-left text-[10px] font-medium tracking-wide sm:tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[26%] sm:w-[18%] align-bottom break-words">
              Jurisdiction
            </th>
            <th className="text-right text-[10px] font-medium tracking-wide sm:tracking-widest uppercase text-oo-muted pb-2 w-[30%] sm:w-[18%] align-bottom break-words">
              Confidence
            </th>
          </tr>
        </thead>
        <tbody>
          {visible.map((p) => (
            <tr key={`${p.a}~${p.b}`} className="border-t border-oo-rule align-top">
              <td className="py-2 pr-3 text-oo-ink">
                <div className="break-words">
                  {p.a_name || p.a}
                  {p.a_source && (
                    <span className="ml-1.5 align-middle text-[10px] bg-oo-bg border border-oo-rule rounded px-1 py-0.5 text-oo-muted whitespace-nowrap">
                      {p.a_source}
                    </span>
                  )}
                </div>
                <div className="break-words text-oo-muted">
                  {p.b_name || p.b}
                  {p.b_source && (
                    <span className="ml-1.5 align-middle text-[10px] bg-oo-bg border border-oo-rule rounded px-1 py-0.5 text-oo-muted whitespace-nowrap">
                      {p.b_source}
                    </span>
                  )}
                </div>
              </td>
              <td className="py-2 pr-3 font-mono text-[12px] text-oo-muted">
                {p.jurisdiction || "—"}
              </td>
              <td className="py-2 text-right">
                <span className="inline-flex items-center gap-1 text-[11px] bg-amber-50 border border-amber-300 text-amber-800 rounded px-1.5 py-0.5">
                  likely same
                </span>
                {p.reason && (
                  <span className="block mt-1 text-[11px] text-oo-muted break-words">
                    {p.reason}
                  </span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {hiddenCount > 0 && (
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          aria-expanded={expanded}
          className="mt-3 text-[12px] font-medium text-oo-blue hover:text-oo-burst underline underline-offset-2"
        >
          {expanded
            ? "Show fewer"
            : `Show ${hiddenCount} more possible duplicate${hiddenCount === 1 ? "" : "s"}`}
        </button>
      )}
    </>
  );
}
