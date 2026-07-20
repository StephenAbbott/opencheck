import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import SearchLoadingGrid from "./components/SearchLoadingGrid";
import {
  BASE_URL,
  fetchSources,
  isValidLei,
  retryLookupSource,
  streamLookup,
  type BoAccessNotice,
  type BodsBreakdown,
  type BodsCountsEvent,
  type CrossSourceLink,
  type MeipMatch,
  type PossiblySameEntity,
  type RiskSignal,
  type SourceHit,
} from "./lib/api";
import {
  searchByNationalId,
  type GleifSearchResult,
} from "./lib/gleifNationalId";
import { COUNTRY_OPTIONS, RA_CODES, validateNationalId } from "./lib/raCodes";
import {
  OpenCheckIcon,
  GleifIcon,
  Neo4jIcon,
  StepKeyIcon,
  StepBridgeIcon,
  StepNetworkIcon,
  StepShieldIcon,
} from "./components/icons";
import { RiskChip, RISK_PRESENTATION, rank } from "./components/risk/RiskChip";
import { ExportPanel } from "./components/export/ExportPanel";
import { ChangelogPage } from "./components/ChangelogPage";
import { SubjectCard } from "./components/cdd/SubjectCard";
import { NarrativePanel } from "./components/cdd/NarrativePanel";
import {
  SourceBucketCard,
  SkeletonSourceCard,
  type SourceBucket,
} from "./components/cdd/SourceBucketCard";
import { EsgPanel } from "./components/cdd/EsgPanel";
import { MeipSignpost } from "./components/cdd/MeipSignpost";
import { SecuritiesSection } from "./components/cdd/SecuritiesSection";

// FullCheck (enhanced due diligence) view — lazy so Cytoscape/graph code only
// loads when a user switches into FullCheck mode.
const FullCheckPanel = lazy(() => import("./components/cdd/FullCheckPanel"));


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
  neo4jZipUrl?: string;
}

const _NEO4J_BASE =
  "https://github.com/StephenAbbott/opencheck/raw/main/data/demo/neo4j";

const EXAMPLE_LEIS: ExampleLei[] = [
  {
    lei: "213800LH1BZH3DI6G760",
    name: "BP P.L.C.",
    hint: "UK oil major",
    signals: [
      { code: "TRUST_OR_ARRANGEMENT", confidence: "high" },
      { code: "NON_EU_JURISDICTION", confidence: "high" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
    neo4jZipUrl: `${_NEO4J_BASE}/213800LH1BZH3DI6G760.zip`,
  },
  {
    lei: "253400JT3MQWNDKMJE44",
    name: "Rosneft",
    hint: "Russian state oil",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
      { code: "NON_EU_JURISDICTION", confidence: "high" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
    neo4jZipUrl: `${_NEO4J_BASE}/253400JT3MQWNDKMJE44.zip`,
  },
  {
    lei: "2138008KTNTDICZU8L25",
    name: "Bank Saderat PLC",
    hint: "Iran-linked UK bank",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
      { code: "NON_EU_JURISDICTION", confidence: "high" },
      { code: "RELATED_SANCTIONED", confidence: "high" },
    ],
    neo4jZipUrl: `${_NEO4J_BASE}/2138008KTNTDICZU8L25.zip`,
  },
  {
    lei: "2138002S3XGZ38WN5Q72",
    name: "Hornsea 1 Limited",
    hint: "UK offshore wind",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
    ],
    neo4jZipUrl: `${_NEO4J_BASE}/2138002S3XGZ38WN5Q72.zip`,
  },
  {
    lei: "213800E11LI1SCETU492",
    name: "Taqa Bratani Limited",
    hint: "UAE-owned UK oil & gas",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
      { code: "RELATED_SANCTIONS_LINKED", confidence: "high" },
    ],
    neo4jZipUrl: `${_NEO4J_BASE}/213800E11LI1SCETU492.zip`,
  },
  {
    lei: "213800AG2V6YE68H5N63",
    name: "Newcastle United FC",
    hint: "Saudi-owned football club",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
    ],
    neo4jZipUrl: `${_NEO4J_BASE}/213800AG2V6YE68H5N63.zip`,
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
  const searchPanelsCollapsed = !!streamingLei && !mobileSearchOpen;
  const [hits, setHits] = useState<SourceHit[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [crossSourceLinks, setCrossSourceLinks] = useState<CrossSourceLink[]>([]);
  const [possiblySame, setPossiblySame] = useState<PossiblySameEntity[]>([]);
  const [meip, setMeip] = useState<MeipMatch | null>(null);
  const [riskSignals, setRiskSignals] = useState<RiskSignal[]>([]);
  const [applicableSources, setApplicableSources] = useState<string[]>([]);
  const [completedSources, setCompletedSources] = useState<Set<string>>(new Set());
  const [streaming, setStreaming] = useState(false);
  // QuickCheck (subject screening, default) vs FullCheck (network EDD). Reset to
  // QuickCheck on each new lookup so the headline experience is always QuickCheck.
  const [mode, setMode] = useState<"quick" | "full">("quick");
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
  type View = "main" | "sources" | "behind" | "api" | "changelog";
  function pathToView(path: string): View {
    if (path === "/sources") return "sources";
    if (path === "/about") return "behind";
    if (path === "/api") return "api";
    if (path === "/changelog") return "changelog";
    return "main";
  }
  function viewToPath(v: View): string {
    if (v === "sources") return "/sources";
    if (v === "behind") return "/about";
    if (v === "api") return "/api";
    if (v === "changelog") return "/changelog";
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
      document.title = `${legalName} — OpenCheck`;
    } else if (view === "sources") {
      document.title = "Data Sources — OpenCheck";
    } else if (view === "behind") {
      document.title = "Behind the Scenes — OpenCheck";
    } else if (view === "api") {
      document.title = "API — OpenCheck";
    } else if (view === "changelog") {
      document.title = "Changelog — OpenCheck";
    } else {
      document.title = "OpenCheck";
    }
  }, [legalName, view]);

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
  const [searchMode, setSearchMode] = useState<"name" | "nationalId" | "lei">("name");
  // APG tabs keyboard pattern: Left/Right arrows (wrapping), Home and End move
  // both focus and selection across the search-mode tabs (roving tabindex).
  const SEARCH_TAB_ORDER = ["name", "nationalId", "lei"] as const;
  const SEARCH_TAB_IDS: Record<(typeof SEARCH_TAB_ORDER)[number], string> = {
    name: "tab-name",
    nationalId: "tab-national-id",
    lei: "tab-lei",
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
    { lei: string; refresh?: boolean }
  >({
    mutationFn: ({ lei, refresh }) =>
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
        setMode("quick");
        setHits([]);
        setErrors({});
        setCrossSourceLinks([]);
        setCrossSourceOpen(false);
        setPossiblySame([]);
        setMeip(null);
        setRiskSignals([]);
        setApplicableSources([]);
        setCompletedSources(new Set());
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
          onRiskSignals: (e) => setRiskSignals(e.signals),
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

  function lookupLei(rawLei: string, opts?: { refresh?: boolean }) {
    const lei = rawLei.trim().toUpperCase();
    setLeiInput(lei);
    setView("main");
    // Shareable URLs: reflect the lookup in ?lei= so refresh and copy/paste
    // re-run it (the backend replay cache makes repeats near-instant).
    const url = new URL(window.location.href);
    if (url.searchParams.get("lei") !== lei) {
      url.searchParams.set("lei", lei);
      window.history.pushState({}, "", url);
    }
    // Cancel any in-flight stream before starting a new one.
    cleanupRef.current?.();
    cleanupRef.current = null;
    lookupMutation.mutate({ lei, refresh: opts?.refresh });
  }

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
    const initial = fromUrl(new URLSearchParams(window.location.search).get("lei"));
    if (initial && isValidLei(initial)) lookupLei(initial);

    const onPopState = () => {
      // Handle non-main path views first (back/forward to /sources, /about etc.)
      const v = pathToView(window.location.pathname);
      if (v !== "main") {
        setView(v);
        return;
      }
      // Back on main — honour ?lei= if present, otherwise clear results.
      const lei = fromUrl(new URLSearchParams(window.location.search).get("lei"));
      if (lei && isValidLei(lei)) {
        lookupLei(lei);
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
  // matched across M sources") and the SubjectCard identifier badge.
  const crossLinkedSourceCount = useMemo(() => {
    const srcs = new Set<string>();
    for (const link of crossSourceLinks)
      for (const h of link.hits) srcs.add(h.source_id);
    return srcs.size;
  }, [crossSourceLinks]);

  // The cross-source identifiers box is collapsed by default but the
  // SubjectCard badge can pop it open — so its open state lives here
  // (controlled) rather than inside CollapsedSection.
  const [crossSourceOpen, setCrossSourceOpen] = useState(false);

  /** SubjectCard badge action: expand the cross-source identifiers box,
   *  scroll to it and flash it (same affordance as narrative citations). */
  const showCrossSourceIdentifiers = () => {
    setCrossSourceOpen(true);
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

  // The hero heading is the page's <h1> on the homepage; once results are on
  // screen the sr-only report heading below takes over as the <h1> (WCAG 1.3.1).
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
            <div className="flex items-center gap-4">
              <button
                type="button"
                onClick={() => {
                  // Click the title to return to a fresh homepage state.
                  cleanupRef.current?.();
                  cleanupRef.current = null;
                  navigate("main");
                  setStreamingLei(null);
                  setLegalName(null);
                  setHits([]);
                  setErrors({});
                  setCrossSourceLinks([]);
                  setCrossSourceOpen(false);
                  setPossiblySame([]);
                  setMeip(null);
                  setRiskSignals([]);
                  setApplicableSources([]);
                  setCompletedSources(new Set());
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
                }}
                aria-label="OpenCheck — back to homepage"
                className="flex items-center gap-2.5 hover:opacity-80 transition-opacity text-left"
              >
                <OpenCheckIcon className="h-7 w-auto flex-shrink-0" />
                <span className="font-head font-bold text-white leading-tight text-xl">
                  Open<span className="text-[#93c5fd]">Check</span>
                </span>
              </button>
              {sourcesQuery.data && (
                <span className="hidden sm:inline-flex items-center gap-1.5 text-[11px] text-white/70 font-mono">
                  <span className="text-white/70 font-semibold">{sourcesQuery.data.sources.filter(s => s.is_national_register).length}</span>
                  <span>national registers</span>
                  <span className="text-white/20" aria-hidden>·</span>
                  <span className="text-white/70 font-semibold">{sourcesQuery.data.sources.filter((s: { is_national_register: boolean }) => !s.is_national_register).length}</span>
                  <span>open sources</span>
                </span>
              )}
            </div>
            <nav aria-label="Site navigation" className="flex items-center gap-4">
              {view !== "main" ? (
                <button
                  type="button"
                  onClick={() => navigate("main")}
                  aria-label="Back to main page"
                  className="text-[12px] font-mono text-oo-light hover:text-white underline underline-offset-4 whitespace-nowrap"
                >
                  ← Back
                </button>
              ) : (
                <div className="flex items-center gap-4">
                  <a
                    href="/sources"
                    onClick={(e) => { e.preventDefault(); navigate("sources"); }}
                    aria-label="View data sources"
                    className="text-[12px] font-mono text-oo-light hover:text-white underline underline-offset-4 whitespace-nowrap"
                  >
                    Sources →
                  </a>
                  <a
                    href="/about"
                    onClick={(e) => { e.preventDefault(); navigate("behind"); }}
                    aria-label="Behind the scenes — how OpenCheck works"
                    className="hidden sm:inline text-[12px] font-mono text-oo-light hover:text-white underline underline-offset-4 whitespace-nowrap"
                  >
                    Behind the scenes →
                  </a>
                </div>
              )}
            </nav>
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
        {view === "main" && (
        <>
        {/* ── Search panel — two-tab design ── */}
        <div className="mb-3">
          <HeroHeading className="font-head font-bold text-oo-ink leading-tight text-[20px] sm:text-[26px]">
            Due diligence on <span className="text-oo-blue">3 million</span> companies, starting from a single ID
          </HeroHeading>
          <p className="text-[13px] sm:text-sm text-oo-muted leading-snug mt-2">
            With a Legal Entity Identifier, OpenCheck pulls open corporate data from 34 sources into one graph using the Beneficial Ownership Data Standard
          </p>
        </div>
        {!streamingLei && (
          <p className="text-[12px] text-oo-muted leading-snug mb-4 flex flex-wrap items-center gap-x-2 gap-y-1">
            <span className="text-[10px] font-semibold uppercase tracking-wide text-oo-blue border border-[#cfd6f5] bg-[#eef1fb] rounded-full px-1.5 py-0.5">
              New
            </span>
            <span className="text-oo-ink">Network screening</span>
            <span aria-hidden>·</span>
            <span className="text-oo-ink">Company timelines</span>
            <span aria-hidden>·</span>
            <span className="text-oo-ink">ESG data</span>
            <span className="text-oo-muted">— every claim links to its source.</span>
          </p>
        )}
        <div className="mb-4 bg-white border border-oo-rule rounded-oo overflow-hidden">
          {/* Tab bar */}
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
          </div>

          {/* Panels collapse on mobile once results are on screen — the tab
              bar stays as a landmark; the prompt row below reopens them. */}
          <div className={searchPanelsCollapsed ? "hidden sm:block" : ""}>

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
                    {nameSearchMutation.data.length} result{nameSearchMutation.data.length === 1 ? "" : "s"} — click to look up
                  </p>
                  <ul aria-label="Search results" className="divide-y divide-oo-rule border border-oo-rule rounded-oo overflow-hidden">
                    {nameSearchMutation.data.map((r) => (
                      <li key={r.lei}>
                        <button
                          type="button"
                          aria-label={`Look up ${r.legalName}, LEI ${r.lei}`}
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
                    { raCode: entry.raCode, id: q },
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
                    {nationalIdSearchMutation.isPending ? "Searching…" : "Look up"}
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
                    {nationalIdSearchMutation.data.length} results — click to look up
                  </p>
                  <ul aria-label="Search results" className="divide-y divide-oo-rule border border-oo-rule rounded-oo overflow-hidden">
                    {nationalIdSearchMutation.data.map((r) => (
                      <li key={r.lei}>
                        <button
                          type="button"
                          aria-label={`Look up ${r.legalName}, LEI ${r.lei}`}
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
                  {lookupMutation.isPending ? "Looking up…" : "Look up"}
                </button>
              </div>
            </form>
          )}

          </div>

          {searchPanelsCollapsed && (
            <button
              type="button"
              onClick={() => setMobileSearchOpen(true)}
              className="sm:hidden w-full flex items-center justify-center gap-2 px-4 py-2.5 text-[12px] font-medium text-oo-blue hover:bg-oo-bg transition-colors"
            >
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <circle cx="11" cy="11" r="7" />
                <path d="m21 21-4.3-4.3" />
              </svg>
              Search for a different entity
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

        {lookupMutation.isPending && (
          <SearchLoadingGrid sources={sourcesQuery.data?.sources ?? []} />
        )}

        {!streamingLei && !lookupMutation.isPending && !streaming && !lookupMutation.isError && !nameSearchMutation.data && !nameSearchMutation.isPending && !nationalIdSearchMutation.data && !nationalIdSearchMutation.isPending && (
          <>
            <ExampleLeiPicker onPick={lookupLei} disabled={lookupMutation.isPending || streaming} />
            <ShareCardShowcase />
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
          <SubjectCard
            lei={streamingLei}
            legalName={legalName}
            jurisdiction={subjectJurisdiction}
            signals={aggregatedCodes}
            screening={streaming}
            replayedAt={replayedAt}
            onRefresh={() => lookupLei(streamingLei, { refresh: true })}
            identifierSources={crossLinkedSourceCount}
            onShowIdentifiers={showCrossSourceIdentifiers}
          />
        )}

        {streamingLei && (
          <div className="mb-6 grid grid-cols-1 sm:grid-cols-2 gap-3" role="group" aria-label="Check mode">
            <button
              type="button"
              aria-pressed={mode === "quick"}
              onClick={() => setMode("quick")}
              className={`text-left rounded-oo border-2 p-4 transition-colors ${
                mode === "quick"
                  ? "border-oo-blue bg-[#eef1fb]"
                  : "border-oo-rule bg-white hover:border-[#cfd6f5]"
              }`}
            >
              <div className="flex items-center gap-2">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" className="text-oo-blue" aria-hidden="true"><path d="M13 3 4 14h7l-1 7 9-11h-7z" /></svg>
                <span className="font-head font-bold text-[15px] text-oo-ink">QuickCheck</span>
              </div>
              <p className="text-[12px] text-oo-muted mt-1 leading-[1.5]">
                Fast screening of this entity for immediate risks.
              </p>
            </button>
            <button
              type="button"
              aria-pressed={mode === "full"}
              onClick={() => setMode("full")}
              className={`text-left rounded-oo border-2 p-4 transition-colors ${
                mode === "full"
                  ? "border-oo-blue bg-[#eef1fb]"
                  : "border-oo-rule bg-white hover:border-[#cfd6f5]"
              }`}
            >
              <div className="flex items-center gap-2">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" className="text-oo-blue" aria-hidden="true"><circle cx="6" cy="6" r="2.3" /><circle cx="18" cy="6" r="2.3" /><circle cx="12" cy="18" r="2.3" /><path d="M8 7.5 10.7 15.6M16 7.5 13.3 15.6M8.5 6h7" /></svg>
                <span className="font-head font-bold text-[15px] text-oo-ink">FullCheck</span>
              </div>
              <p className="text-[12px] text-oo-muted mt-1 leading-[1.5]">
                Map the wider corporate network for enhanced due diligence.
              </p>
            </button>
          </div>
        )}

        {streamingLei && <NarrativePanel lei={streamingLei} legalName={legalName} />}

        {aggregatedCodes.length > 0 && (
          <section className="mb-8" id="risk-signals">
            <SectionLabel>Risk signals</SectionLabel>
            <div className="flex flex-wrap gap-2">
              {aggregatedCodes.map((sig) => (
                <RiskChip key={sig.code} signal={sig} />
              ))}
            </div>
            <p className="text-[12px] text-oo-muted mt-3">
              Select a chip for the rule that fired. Signals derived from
              open data; AMLA-aligned chips read BODS statements.
            </p>
          </section>
        )}

        {(crossSourceLinks.length > 0 || gleifMappedIds.length > 0) && (
          <CollapsedSection
            htmlId="cross-source-identifiers"
            label="Cross-source identifiers"
            open={crossSourceOpen}
            onToggle={setCrossSourceOpen}
            summary={
              crossLinkedSourceCount >= 2 ? (
                <>
                  <span className="font-semibold">
                    {crossSourceLinks.length + gleifMappedIds.length}{" "}
                    identifier
                    {crossSourceLinks.length + gleifMappedIds.length === 1
                      ? ""
                      : "s"}
                  </span>{" "}
                  matched across{" "}
                  <span className="font-semibold">
                    {crossLinkedSourceCount} sources
                  </span>
                </>
              ) : (
                <>
                  <span className="font-semibold">
                    {gleifMappedIds.length} identifier
                    {gleifMappedIds.length === 1 ? "" : "s"}
                  </span>{" "}
                  mapped by GLEIF
                </>
              )
            }
          >
            <CrossSourceIdentifiersTable
              links={crossSourceLinks}
              gleifMapped={gleifMappedIds}
              sourceNames={sourceNameIndex}
            />
          </CollapsedSection>
        )}

        {possiblySame.length > 0 && (
          <CollapsedSection
            htmlId="possibly-same"
            label="Possibly the same entity"
            summary={
              <>
                <span className="font-semibold">
                  {possiblySame.length} candidate pair
                  {possiblySame.length === 1 ? "" : "s"}
                </span>{" "}
                flagged for review — same name &amp; jurisdiction, no shared
                identifier
              </>
            }
          >
            <PossiblySameTable pairs={possiblySame} />
          </CollapsedSection>
        )}

        {mode === "full" && streamingLei ? (
          <Suspense fallback={<p className="text-[13px] text-oo-muted italic mb-8">Loading FullCheck…</p>}>
            <FullCheckPanel lei={streamingLei} legalName={legalName} signals={riskSignals} />
          </Suspense>
        ) : (
          <>
        {(cddBuckets.length > 0 || pendingCddSources.length > 0) && (
          <section className="mb-8">
            <SectionLabel>
              {totalHits} hit{totalHits === 1 ? "" : "s"} across{" "}
              {cddBuckets.length} source{cddBuckets.length === 1 ? "" : "s"}
              {pendingCddSources.length > 0 && (
                <span className="text-oo-blue font-normal ml-1.5">
                  · {pendingCddSources.length} pending…
                </span>
              )}
            </SectionLabel>
            {streamingLei && EXAMPLE_LEIS.some((e) => e.lei === streamingLei) && (
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
                    bodsCountMap={bodsCountMap}
                    bodsBreakdownMap={bodsBreakdownMap}
                    onRetry={b.error ? () => retrySource(b.sourceId) : undefined}
                    retrying={retryingSources.has(b.sourceId)}
                    footnote={
                      b.sourceId === "gleif" && gleifChildrenInfo && gleifChildrenInfo.total > 100
                        ? `Showing the first ${gleifChildrenInfo.fetched} of ${gleifChildrenInfo.total.toLocaleString()} direct subsidiaries in BODS statements (GLEIF Level 2)`
                        : undefined
                    }
                  />
                </div>
              ))}
              {pendingCddSources.map((id) => (
                <SkeletonSourceCard key={id} />
              ))}
            </div>
          </section>
        )}

        {streamingLei && <SecuritiesSection lei={streamingLei} />}

        {(esgBuckets.length > 0 || pendingEsgSources.length > 0) && (
          <EsgPanel buckets={esgBuckets} pendingCount={pendingEsgSources.length} bodsCountMap={bodsCountMap} bodsBreakdownMap={bodsBreakdownMap} />
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
          </>
        )}
        </>
        )}

        {view === "sources" && (
          <section>
            <SectionLabel>About the sources</SectionLabel>
            <p className="text-[14px] leading-[1.7] text-oo-muted mb-6 max-w-2xl">
              OpenCheck queries the open data sources below. GLEIF is
              the entry point — the LEI acts as a connector across the
              rest. Each source ships its data under its own license;
              non-commercial sources propagate that obligation through
              the export bundle.
            </p>
            {sourcesQuery.isLoading && (
              <p className="text-oo-muted">Loading…</p>
            )}
            {sourcesQuery.data && (
              <ul
                className="grid gap-6"
                // 480px min as per the BO design library card grid spec.
                style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 480px), 1fr))" }}
              >
                {[...sourcesQuery.data.sources]
                  .sort((a, b) => a.name.localeCompare(b.name))
                  .map((s, i) => (
                  <li
                    key={s.id}
                    className={`bg-white border rounded-oo p-6 text-sm transition-shadow hover:shadow-oo-card ${
                      s.category === "esg"
                        ? "border-emerald-200"
                        : "border-oo-rule"
                    }`}
                  >
                    <div className="flex items-baseline gap-3 mb-1 flex-wrap">
                      <span className="font-mono text-[11px] tracking-wider text-oo-blue">
                        {String(i + 1).padStart(2, "0")}
                      </span>
                      <a
                        href={s.homepage}
                        target="_blank"
                        rel="noreferrer"
                        className="font-head text-[17px] font-bold text-oo-ink leading-tight hover:underline underline-offset-2"
                      >
                        {s.name}
                      </a>
                      <div className="ml-auto flex items-center gap-2">
                        {s.category === "esg" && (
                          <span className="text-[10px] font-semibold uppercase tracking-wide bg-emerald-50 text-emerald-700 border border-emerald-200 rounded px-1.5 py-0.5">
                            ESG
                          </span>
                        )}
                        <LicenseChip license={s.license} />
                      </div>
                    </div>
                    {s.description && (
                      <p className="text-[13.5px] leading-[1.7] text-oo-muted mt-2">
                        {s.description}
                      </p>
                    )}
                    <p className="text-[11px] font-mono mt-3 text-oo-muted">
                      Supports: {s.supports.join(", ")} ·{" "}
                      {s.live_available ? "live ready" : "stub"}
                    </p>
                  </li>
                ))}
              </ul>
            )}
          </section>
        )}

        {view === "behind" && <BehindTheScenesPage />}

        {view === "api" && <ApiPage />}

        {view === "changelog" && <ChangelogPage />}
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
            relationships, cross-source links and risk signals.
          </ApiEndpoint>
          <ApiEndpoint path="/lookup-stream?lei=<LEI>">
            The same synthesis streamed as Server-Sent Events (<code className={mono}>gleif_done</code>,
            per-source <code className={mono}>hit</code> / <code className={mono}>source_error</code>,
            then <code className={mono}>done</code>) so a client can render progressively.
          </ApiEndpoint>
          <ApiEndpoint path="/lookup-source?lei=<LEI>&source_id=<id>">
            Re-run a single source for an existing lookup (the per-source “retry” in the UI).
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
            path="/export?lei=<LEI>&format=<zip|json|jsonl|xml|senzing|ftm>&subsidiaries=<bool>"
            params={[
              [
                "format",
                "zip ships bods.json + bods.jsonl + bods.xml + senzing.jsonl + ftm.jsonl + manifest.json + LICENSES.md; json / jsonl / xml return the statements only; senzing returns Senzing JSON entity records for entity resolution; ftm returns FollowTheMoney entities for OpenSanctions / OpenAleph workflows.",
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
            Look up a company by its LEI and get the unified BODS view:
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
        <BtsCard title="QuickCheck and FullCheck">
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

/**
 * Collapsed-by-default disclosure box for the reconciliation sections
 * (cross-source identifiers / possibly-same). The full tables pushed the
 * source results a long scroll below the fold for casual users, while the
 * LEI they mostly repeat is already on the SubjectCard — so each box now
 * renders as a one-line summary that expands in place. Kept in position
 * above the source cards so the "Confirmed by" jump chips still point
 * downward at their targets.
 */
function CollapsedSection({
  htmlId,
  label,
  summary,
  open: openProp,
  onToggle,
  children,
}: {
  /** id on the section wrapper — in-page anchors (e.g. the SubjectCard
   *  identifier badge) scroll here. */
  htmlId: string;
  label: string;
  summary: React.ReactNode;
  /** Controlled open state — omit to let the section manage its own. */
  open?: boolean;
  onToggle?: (open: boolean) => void;
  children: React.ReactNode;
}) {
  const [openState, setOpenState] = useState(false);
  const open = openProp ?? openState;
  return (
    <section
      id={htmlId}
      className="mb-8 bg-white border border-oo-rule rounded-oo scroll-mt-4"
    >
      <button
        type="button"
        aria-expanded={open}
        aria-controls={`${htmlId}-body`}
        onClick={() => {
          onToggle?.(!open);
          if (openProp === undefined) setOpenState(!open);
        }}
        className="w-full flex items-center justify-between gap-3 p-5 text-left group"
      >
        <span className="min-w-0">
          <span className="block text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted">
            {label}
          </span>
          <span className="block text-[13px] text-oo-ink mt-1">{summary}</span>
        </span>
        <svg
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
          className={`shrink-0 text-oo-muted transition-transform group-hover:text-oo-ink ${
            open ? "rotate-90" : ""
          }`}
        >
          <path d="m9 18 6-6-6-6" />
        </svg>
      </button>
      {open && (
        <div id={`${htmlId}-body`} className="px-5 pb-5">
          {children}
        </div>
      )}
    </section>
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
            {ex.neo4jZipUrl && (
              <a
                href={ex.neo4jZipUrl}
                target="_blank"
                rel="noopener noreferrer"
                title="Download Neo4j CSV bundle"
                className="absolute top-3 right-3 opacity-70 hover:opacity-100 transition-opacity"
                aria-label={`Download Neo4j CSV bundle for ${ex.name}`}
              >
                <Neo4jIcon />
              </a>
            )}
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
    title: "34 open sources, in parallel",
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
        to its source. Take it away as an accessible PDF or raw data.
      </>
    ),
    badges: null,
  },
] as const;

/**
 * ShareCardShowcase — homepage preview of the output: the live shareable
 * summary card every lookup generates. Shows what a result looks like before
 * the first query, and advertises the share-link feature. The image is a
 * committed render of the BP curated example (regenerate with
 * `opencheck.og_image.render_share_card` if the design changes).
 */
function ShareCardShowcase() {
  return (
    <section className="mb-10 bg-white border border-oo-rule rounded-oo p-7">
      <SectionLabel>Share what you find</SectionLabel>
      <div className="mt-2 flex flex-col lg:flex-row lg:items-center gap-6">
        <div className="lg:max-w-[300px] shrink-0">
          <p className="text-[13px] text-oo-muted leading-[1.65]">
            Every lookup generates a <span className="text-oo-ink font-medium">live summary card</span> —
            the entity, its risk-signal count, and the first flags in OpenCheck's
            risk colours. Copy the share link from any result and the card appears
            wherever you paste it: Slack, LinkedIn, X, WhatsApp.
          </p>
        </div>
        <img
          src="/share-card-example.png"
          width={1200}
          height={630}
          alt="Example shareable summary card for BP P.L.C. showing 3 risk signals: trust or arrangement, non-EU jurisdiction, and three or more ownership layers, with a prompt to visit opencheck.world for details"
          className="w-full max-w-[560px] h-auto rounded-oo border border-oo-rule shadow-oo-card"
          loading="lazy"
        />
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



// LicenseChip is also defined in SourceBucketCard for use inside DeepenBlock.
// This copy is used in the Sources list view inside App.
function LicenseChip({ license }: { license: string }) {
  const nc = license.toLowerCase().includes("nc");
  const classes = nc
    ? "bg-amber-50 text-amber-800 border-amber-200"
    : "bg-emerald-50 text-emerald-700 border-emerald-200";
  return (
    <span
      className={`text-[11px] border rounded px-1.5 py-0.5 font-mono ${classes}`}
    >
      {license}
    </span>
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
                    title={`${sourceNames[h.source_id] ?? h.source_id} — jump to this source's results`}
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
                GLEIF mapped
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
