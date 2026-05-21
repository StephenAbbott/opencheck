import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import BODSGraph from "./components/BODSGraph";
import SearchLoadingGrid from "./components/SearchLoadingGrid";
import {
  deepen,
  exportUrl,
  fetchSources,
  isValidLei,
  streamLookup,
  type CrossSourceLink,
  type DeepenResponse,
  type RiskSignal,
  type SourceHit,
} from "./lib/api";


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

interface SourceBucket {
  sourceId: string;
  sourceName: string;
  hits: SourceHit[];
  error?: string;
}

interface GleifSearchResult {
  lei: string;
  legalName: string;
  country: string;
  status: string;
}

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
}

const EXAMPLE_LEIS: ExampleLei[] = [
  {
    lei: "4OFD47D73QFJ1T1MOF29",
    name: "Daily Mail and General Trust",
    hint: "UK-listed media holding",
    signals: [
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
  },
  {
    lei: "213800LH1BZH3DI6G760",
    name: "BP P.L.C.",
    hint: "UK oil major",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
  },
  {
    lei: "253400JT3MQWNDKMJE44",
    name: "Rosneft",
    hint: "Russian state oil",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
      { code: "FATF_GREY_LIST", confidence: "high" },
    ],
  },
  {
    lei: "2138008KTNTDICZU8L25",
    name: "Bank Saderat PLC",
    hint: "Iran-linked UK bank",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
      { code: "FATF_BLACK_LIST", confidence: "high" },
    ],
  },
  {
    lei: "2138008RB4WDK7HYYS91",
    name: "Biffa PLC",
    hint: "UK waste management",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
      { code: "COMPLEX_CORPORATE_STRUCTURE", confidence: "high" },
    ],
  },
  {
    lei: "2138002S3XGZ38WN5Q72",
    name: "Hornsea 1 Limited",
    hint: "UK offshore wind",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
    ],
  },
  {
    lei: "213800DBE5Y9ZM58PN63",
    name: "Care UK Social Care",
    hint: "UK care provider",
    signals: [
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
  },
  {
    lei: "213800E11LI1SCETU492",
    name: "Taqa Bratani Limited",
    hint: "UAE-owned UK oil & gas",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
    ],
  },
  {
    lei: "213800AG2V6YE68H5N63",
    name: "Newcastle United FC",
    hint: "Saudi-owned football club",
    signals: [
      { code: "NON_EU_JURISDICTION", confidence: "high" },
      { code: "COMPLEX_OWNERSHIP_LAYERS", confidence: "medium" },
    ],
  },
];

/**
 * GLEIF LEI API search icon — pill-shaped search box with a magnifying
 * glass and a green cursor with click-spark lines. Matches the icon
 * shown on https://ai.gleif.org/connect-ai.
 */
function GleifIcon({ className, style }: { className?: string; style?: React.CSSProperties }) {
  return (
    <svg
      viewBox="0 0 88 40"
      className={className}
      style={style}
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
      focusable="false"
    >
      {/* Pill — white fill, dark teal border */}
      <rect x="1" y="4" width="62" height="32" rx="16" fill="white" stroke="#1b3d4f" strokeWidth="2.5" />
      {/* Magnifying glass ring */}
      <circle cx="19" cy="20" r="7" fill="none" stroke="#1b3d4f" strokeWidth="2.5" />
      {/* Magnifying glass handle */}
      <line x1="24" y1="25" x2="29" y2="30" stroke="#1b3d4f" strokeWidth="2.5" strokeLinecap="round" />
      {/* Three spark / click lines between pill and cursor */}
      <line x1="68" y1="10" x2="72" y2="7"  stroke="#34d399" strokeWidth="2" strokeLinecap="round" />
      <line x1="70" y1="18" x2="75" y2="18" stroke="#34d399" strokeWidth="2" strokeLinecap="round" />
      <line x1="68" y1="26" x2="72" y2="29" stroke="#34d399" strokeWidth="2" strokeLinecap="round" />
      {/* Arrow cursor — filled green */}
      <polygon points="77,12 77,34 81,27 86,35 88,33 83,25 88,25" fill="#34d399" />
    </svg>
  );
}

/**
 * OpenCheck magnifying-glass icon — white variant for use on the dark
 * navy header. Sized via className (e.g. ``h-9 w-auto``).
 */
function OpenCheckIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 200 200"
      className={className}
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
      focusable="false"
    >
      <defs>
        <clipPath id="oc-icon-lens">
          <circle cx="80" cy="80" r="63" />
        </clipPath>
      </defs>
      {/* Handle */}
      <line x1="127" y1="127" x2="186" y2="186" stroke="white" strokeWidth="14" strokeLinecap="round" />
      {/* Ring */}
      <circle cx="80" cy="80" r="70" fill="none" stroke="white" strokeWidth="13" />
      {/* Building silhouette */}
      <g clipPath="url(#oc-icon-lens)">
        <rect x="90" y="16" width="22" height="108" fill="white" />
        <rect x="108" y="42" width="18" height="82" fill="white" />
        {/* Windows */}
        <rect x="93" y="24" width="6" height="6" fill="#1e3a8a" />
        <rect x="103" y="24" width="6" height="6" fill="#1e3a8a" />
        <rect x="93" y="35" width="6" height="6" fill="#1e3a8a" />
        <rect x="103" y="35" width="6" height="6" fill="#1e3a8a" />
        <rect x="93" y="46" width="6" height="6" fill="#1e3a8a" />
        <rect x="103" y="46" width="6" height="6" fill="#1e3a8a" />
        <rect x="112" y="50" width="5" height="5" fill="#1e3a8a" />
        <rect x="112" y="61" width="5" height="5" fill="#1e3a8a" />
        {/* Door */}
        <rect x="96" y="94" width="10" height="30" fill="#1e3a8a" />
      </g>
      {/* Ownership network — edges */}
      <line x1="48" y1="28" x2="18" y2="76" stroke="#93c5fd" strokeWidth="4.5" strokeLinecap="round" />
      <line x1="18" y1="76" x2="48" y2="124" stroke="#93c5fd" strokeWidth="4.5" strokeLinecap="round" />
      <line x1="48" y1="28" x2="48" y2="124" stroke="#93c5fd" strokeWidth="4.5" strokeLinecap="round" />
      {/* Central arrow */}
      <polygon points="34,55 34,99 76,77" fill="white" />
      {/* Nodes */}
      <circle cx="48" cy="28" r="11" fill="#22c55e" />
      <circle cx="18" cy="76" r="11" fill="#3b82f6" />
      <circle cx="48" cy="124" r="11" fill="#7c3aed" />
    </svg>
  );
}

export default function App() {
  const [leiInput, setLeiInput] = useState("");
  const [looking, setLooking] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // --- Streaming lookup state ---
  // streamingLei is set once GLEIF resolves (replaces the old `result !== null` guard).
  const [streamingLei, setStreamingLei] = useState<string | null>(null);
  const [legalName, setLegalName] = useState<string | null>(null);
  const [hits, setHits] = useState<SourceHit[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [crossSourceLinks, setCrossSourceLinks] = useState<CrossSourceLink[]>([]);
  const [riskSignals, setRiskSignals] = useState<RiskSignal[]>([]);
  const [applicableSources, setApplicableSources] = useState<string[]>([]);
  const [completedSources, setCompletedSources] = useState<Set<string>>(new Set());
  const [streaming, setStreaming] = useState(false);

  // Cleanup ref — holds the SSE close function for the current in-flight stream.
  const cleanupRef = useRef<(() => void) | null>(null);

  // Close any open stream when the component unmounts.
  useEffect(() => () => { cleanupRef.current?.(); }, []);
  // ``main`` shows the LEI form + lookup result; ``sources`` shows the
  // source inventory page. Kept as state rather than a router so we
  // don't pull in react-router for two views.
  const [view, setView] = useState<"main" | "sources" | "behind">("main");

  // Two-mode search: "name" = GLEIF company-name search; "lei" = paste LEI.
  const [searchMode, setSearchMode] = useState<"name" | "lei">("name");
  const [nameQuery, setNameQuery] = useState("");
  const [nameResults, setNameResults] = useState<GleifSearchResult[] | null>(null);
  const [nameSearching, setNameSearching] = useState(false);
  const [nameError, setNameError] = useState<string | null>(null);

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchSources(),
  });

  function lookupLei(rawLei: string) {
    const lei = rawLei.trim().toUpperCase();
    setLeiInput(lei);
    setView("main");
    if (!isValidLei(lei)) {
      setError(
        "Enter a 20-character ISO 17442 LEI " +
          "(e.g. 213800LH1BZH3DI6G760)."
      );
      return;
    }

    // Cancel any in-flight stream from a previous lookup.
    cleanupRef.current?.();
    cleanupRef.current = null;

    // Reset all streaming state.
    setLooking(true);
    setError(null);
    setStreamingLei(null);
    setLegalName(null);
    setHits([]);
    setErrors({});
    setCrossSourceLinks([]);
    setRiskSignals([]);
    setApplicableSources([]);
    setCompletedSources(new Set());
    setStreaming(false);

    const cleanup = streamLookup(lei, {
      onGleifDone: (e) => {
        setStreamingLei(e.lei);
        setLegalName(e.legal_name);
        setLooking(false);
        setStreaming(true);
      },
      onSourcesApplicable: (e) => {
        setApplicableSources(e.source_ids);
      },
      onHit: (e) => {
        setHits((prev) => [...prev, e]);
      },
      onSourceCompleted: (e) => {
        setCompletedSources((prev) => new Set([...prev, e.source_id]));
      },
      onSourceError: (e) => {
        setErrors((prev) => ({ ...prev, [e.source_id]: e.error }));
        setCompletedSources((prev) => new Set([...prev, e.source_id]));
      },
      onCrossSourceLinks: (e) => {
        setCrossSourceLinks(e.links);
      },
      onRiskSignals: (e) => {
        setRiskSignals(e.signals);
      },
      onDone: (_e) => {
        setStreaming(false);
        cleanupRef.current = null;
      },
      onError: (detail) => {
        setError(detail);
        setLooking(false);
        setStreaming(false);
        cleanupRef.current = null;
      },
    });
    cleanupRef.current = cleanup;
  }

  async function runLookup(e: React.FormEvent) {
    e.preventDefault();
    await lookupLei(leiInput);
  }

  /**
   * Search GLEIF by company name using the public REST API.
   * On success, populates ``nameResults`` for the user to pick from.
   * After selection the standard ``lookupLei`` flow takes over.
   */
  async function searchByName(e: React.FormEvent) {
    e.preventDefault();
    const q = nameQuery.trim();
    if (!q) return;
    setNameSearching(true);
    setNameError(null);
    setNameResults(null);
    try {
      const url =
        `https://api.gleif.org/api/v1/lei-records` +
        `?filter[entity.legalName]=${encodeURIComponent(q)}` +
        `&page[size]=10`;
      const resp = await fetch(url, { headers: { Accept: "application/vnd.api+json" } });
      if (!resp.ok) throw new Error(`GLEIF API returned ${resp.status}`);
      const json = await resp.json();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const items: GleifSearchResult[] = (json.data ?? []).map((item: any) => {
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
        };
      });
      setNameResults(items);
      if (items.length === 0) {
        setNameError("No entities found. Try a shorter or different spelling.");
      }
    } catch (err) {
      setNameError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setNameSearching(false);
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

  // Index risk signals by source_id so each source card shows only the
  // signals attributed to that source — not all entity-level signals.
  const riskBySource = useMemo(() => {
    const out: Record<string, RiskSignal[]> = {};
    for (const sig of riskSignals) {
      (out[sig.source_id] = out[sig.source_id] ?? []).push(sig);
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
        className="relative overflow-hidden bg-oo-navy text-white px-6 sm:px-10 lg:px-16 py-10 sm:py-12"
        role="banner"
        style={{
          backgroundImage:
            "radial-gradient(circle 500px at calc(100% + 80px) -80px, rgba(61, 48, 212, 0.28), transparent)",
        }}
      >
        <div className="max-w-oo-page mx-auto relative">
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={() => {
                    // Click the title to return to a fresh homepage state.
                    cleanupRef.current?.();
                    cleanupRef.current = null;
                    setView("main");
                    setStreamingLei(null);
                    setLegalName(null);
                    setHits([]);
                    setErrors({});
                    setCrossSourceLinks([]);
                    setRiskSignals([]);
                    setApplicableSources([]);
                    setCompletedSources(new Set());
                    setStreaming(false);
                    setError(null);
                    setLooking(false);
                    setLeiInput("");
                    setNameQuery("");
                    setNameResults(null);
                    setNameError(null);
                    setSearchMode("name");
                  }}
                  aria-label="Back to homepage"
                  className="flex items-center gap-3 hover:opacity-80 transition-opacity text-left"
                >
                  <OpenCheckIcon className="h-[clamp(2rem,4vw,2.6rem)] w-auto flex-shrink-0" />
                  <span className="font-head font-bold text-white leading-tight text-[clamp(1.6rem,4vw,2.4rem)]">
                    Open<span className="text-[#93c5fd]">Check</span>
                  </span>
                </button>
                <span className="text-[11px] font-semibold tracking-oo-eyebrow uppercase bg-white/15 text-white/90 rounded px-2 py-0.5 border border-white/25">
                  Beta
                </span>
              </div>
            </div>
            <nav aria-label="Site navigation" className="flex items-center gap-5">
              {view !== "main" ? (
                <button
                  type="button"
                  onClick={() => setView("main")}
                  aria-label="Back to main page"
                  className="text-[12px] font-mono text-oo-light hover:text-white underline underline-offset-4 whitespace-nowrap"
                >
                  ← Back
                </button>
              ) : (
                <div className="flex flex-col sm:flex-row items-end sm:items-center gap-y-1 gap-x-4">
                  <button
                    type="button"
                    onClick={() => setView("sources")}
                    aria-label="View data sources"
                    className="text-[12px] font-mono text-oo-light hover:text-white underline underline-offset-4 whitespace-nowrap"
                  >
                    Sources →
                  </button>
                  <button
                    type="button"
                    onClick={() => setView("behind")}
                    aria-label="Behind the scenes — how OpenCheck works"
                    className="text-[12px] font-mono text-oo-light hover:text-white underline underline-offset-4 whitespace-nowrap"
                  >
                    Behind the scenes →
                  </button>
                </div>
              )}
            </nav>
          </div>
          <p className="mt-3 max-w-2xl text-[15px] font-light leading-[1.65] text-white/70">
            Customer due diligence risk checks driven by the Legal
            Entity Identifier (LEI) and open data — mapped to the
            Beneficial Ownership Data Standard.
          </p>
          <SourceCounter sources={sourcesQuery.data?.sources ?? null} />
        </div>
      </header>

      <main
        id="main-content"
        role="main"
        className="flex-1 px-6 sm:px-10 lg:px-16 py-12 max-w-oo-page mx-auto w-full"
      >
        {view === "main" && (
        <>
        {/* ── Search panel — two-tab design ── */}
        <div className="mb-8 bg-white border border-oo-rule rounded-oo overflow-hidden">
          {/* Tab bar */}
          <div role="tablist" aria-label="Search method" className="flex border-b border-oo-rule">
            <button
              type="button"
              role="tab"
              aria-selected={searchMode === "name"}
              aria-controls="panel-name"
              id="tab-name"
              onClick={() => setSearchMode("name")}
              className={`flex-1 flex items-center justify-center gap-2 px-4 py-3 text-[13px] font-medium transition-colors ${
                searchMode === "name"
                  ? "text-oo-ink border-b-2 border-oo-blue bg-white"
                  : "text-oo-muted bg-oo-bg hover:text-oo-ink"
              }`}
            >
              <GleifIcon className="flex-shrink-0" aria-hidden style={{ height: "1.15em", width: "auto" }} />
              Search by company name
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={searchMode === "lei"}
              aria-controls="panel-lei"
              id="tab-lei"
              onClick={() => setSearchMode("lei")}
              className={`flex-1 flex items-center justify-center gap-2 px-4 py-3 text-[13px] font-medium transition-colors border-l border-oo-rule ${
                searchMode === "lei"
                  ? "text-oo-ink border-b-2 border-oo-blue bg-white"
                  : "text-oo-muted bg-oo-bg hover:text-oo-ink"
              }`}
            >
              Paste an LEI
            </button>
          </div>

          {/* ── Name search panel ── */}
          {searchMode === "name" && (
            <div id="panel-name" role="tabpanel" aria-labelledby="tab-name" className="p-6">
              <form onSubmit={searchByName}>
                <label
                  htmlFor="name-input"
                  className="block text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2"
                >
                  Company name
                </label>
                <div className="flex gap-3">
                  <input
                    id="name-input"
                    type="search"
                    value={nameQuery}
                    onChange={(e) => setNameQuery(e.target.value)}
                    placeholder="e.g. Unilever PLC"
                    autoComplete="off"
                    aria-label="Company name"
                    aria-describedby="name-hint"
                    className="flex-1 border border-oo-rule rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                  />
                  <button
                    type="submit"
                    disabled={nameSearching || !nameQuery.trim()}
                    aria-busy={nameSearching}
                    className="bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                  >
                    {nameSearching ? "Searching…" : "Search"}
                  </button>
                </div>
                <p id="name-hint" className="text-[13px] leading-[1.7] text-oo-muted mt-3 max-w-2xl">
                  <GleifIcon className="inline-block align-middle mr-1.5" aria-hidden style={{ height: "1.2em", width: "auto" }} />
                  Powered by the{" "}
                  <a
                    href="https://www.gleif.org/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline hover:text-oo-ink transition-colors"
                  >
                    Global Legal Entity Identifier Foundation (GLEIF)
                  </a>{" "}
                  LEI registry via the{" "}
                  <a
                    href="https://mcp.gleif.org/gleif-api/mcp"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline hover:text-oo-ink transition-colors"
                  >
                    GLEIF MCP server
                  </a>
                  .
                </p>
              </form>

              <div aria-live="polite" aria-atomic="true">
              {nameError && (
                <div role="alert" className="mt-4 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
                  {nameError}
                </div>
              )}
              </div>

              {nameResults && nameResults.length > 0 && (
                <div className="mt-4" aria-live="polite">
                  <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-3">
                    {nameResults.length} result{nameResults.length === 1 ? "" : "s"} — click to look up
                  </p>
                  <ul aria-label="Search results" className="divide-y divide-oo-rule border border-oo-rule rounded-oo overflow-hidden">
                    {nameResults.map((r) => (
                      <li key={r.lei}>
                        <button
                          type="button"
                          aria-label={`Look up ${r.legalName}, LEI ${r.lei}`}
                          onClick={() => {
                            setNameResults(null);
                            setNameQuery("");
                            lookupLei(r.lei);
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
            <form onSubmit={runLookup} id="panel-lei" role="tabpanel" aria-labelledby="tab-lei" className="p-6">
              <label
                htmlFor="lei-input"
                className="block text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2"
              >
                Legal Entity Identifier
              </label>
              <div className="flex gap-3">
                <input
                  id="lei-input"
                  type="text"
                  value={leiInput}
                  onChange={(e) => setLeiInput(e.target.value)}
                  placeholder="e.g. 213800LH1BZH3DI6G760"
                  spellCheck={false}
                  autoComplete="off"
                  aria-label="Legal Entity Identifier (20 characters)"
                  aria-describedby="lei-hint"
                  pattern="[A-Za-z0-9]{20}"
                  inputMode="text"
                  className="flex-1 border border-oo-rule rounded px-3 py-2.5 font-mono uppercase tracking-wide focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                  maxLength={20}
                />
                <button
                  type="submit"
                  disabled={looking || !leiInput.trim()}
                  aria-busy={looking}
                  className="bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                >
                  {looking ? "Looking up…" : "Look up"}
                </button>
              </div>
              <p id="lei-hint" className="text-[13px] leading-[1.7] text-oo-muted mt-3 max-w-2xl">
                Enter a 20-character ISO 17442 LEI to query GLEIF and bridge to
                national company registries, OpenCorporates, OpenSanctions,
                OpenAleph, Wikidata, and OpenTender.
              </p>
            </form>
          )}
        </div>

        <div aria-live="assertive" aria-atomic="true">
          {error && (
            <div role="alert" className="mb-6 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
              {error}
            </div>
          )}
        </div>

        {looking && (
          <SearchLoadingGrid sources={sourcesQuery.data?.sources ?? []} />
        )}

        {!streamingLei && !looking && !streaming && !error && !nameResults && !nameSearching && (
          <>
            <ExampleLeiPicker onPick={lookupLei} disabled={looking || streaming} />
            <HowItWorks />
          </>
        )}

        {streamingLei && <SubjectCard lei={streamingLei} legalName={legalName} />}

        {aggregatedCodes.length > 0 && (
          <section className="mb-8">
            <SectionLabel>Risk signals</SectionLabel>
            <div className="flex flex-wrap gap-2">
              {aggregatedCodes.map((sig) => (
                <RiskChip key={sig.code} signal={sig} />
              ))}
            </div>
            <p className="text-[12px] text-oo-muted mt-3">
              Hover a chip for the rule that fired. Signals derived from
              open data; AMLA-aligned chips read BODS statements.
            </p>
          </section>
        )}

        {crossSourceLinks.length > 0 && (
          <section className="mb-8 bg-white border border-oo-rule rounded-oo p-5">
            <SectionLabel>Cross-source links</SectionLabel>
            <ul className="space-y-2">
              {crossSourceLinks.map((link, i) => (
                <CrossSourceLinkRow key={`${link.key}:${link.key_value}:${i}`} link={link} />
              ))}
            </ul>
          </section>
        )}

        {(cddBuckets.length > 0 || pendingCddSources.length > 0) && (
          <section className="mb-8">
            <SectionLabel>
              {totalHits} hit{totalHits === 1 ? "" : "s"} across{" "}
              {cddBuckets.length} source{cddBuckets.length === 1 ? "" : "s"}
              {pendingCddSources.length > 0 && (
                <span className="text-oo-blue/50 font-normal ml-1.5">
                  · {pendingCddSources.length} pending…
                </span>
              )}
            </SectionLabel>
            <div className="space-y-4">
              {cddBuckets.map((b) => (
                <SourceBucketCard
                  key={b.sourceId}
                  bucket={b}
                  riskByHit={riskByHit}
                  sourceSignals={riskBySource[b.sourceId] ?? []}
                />
              ))}
              {pendingCddSources.map((id) => (
                <SkeletonSourceCard key={id} />
              ))}
            </div>
          </section>
        )}

        {(esgBuckets.length > 0 || pendingEsgSources.length > 0) && (
          <EsgPanel buckets={esgBuckets} pendingCount={pendingEsgSources.length} />
        )}

        {streamingLei && !streaming && totalHits > 0 && (
          <ExportPanel
            lei={streamingLei}
            legalName={legalName}
            sourceLicenses={
              sourcesQuery.data
                ? Object.fromEntries(
                    sourcesQuery.data.sources.map((s) => [s.id, s.license])
                  )
                : {}
            }
            contributingSourceIds={[...cddBuckets, ...esgBuckets]
              .filter((b) => b.hits.some((h) => !h.is_stub))
              .map((b) => b.sourceId)}
          />
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
      </main>

      {/* GODIN ribbon — permanent attribution banner. */}
      <aside
        aria-label="GODIN — Global Open Data Integration Network"
        className="px-6 sm:px-10 lg:px-16 py-4 text-white/90 text-[13px] leading-[1.6]"
        style={{
          background:
            "linear-gradient(90deg, rgb(7, 116, 95) 0%, rgb(12, 213, 173) 100%)",
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
              onClick={() => setView("behind")}
              className="underline underline-offset-2 font-medium hover:text-white"
            >
              How it works →
            </button>
          </p>
        </div>
      </aside>

      <footer className="border-t border-oo-rule bg-white px-6 sm:px-10 lg:px-16 py-6 text-[12px] text-oo-muted">
        <div className="max-w-oo-page mx-auto text-center">
          <a
            href="https://github.com/StephenAbbott/opencheck"
            target="_blank"
            rel="noreferrer"
            className="font-mono text-oo-blue hover:text-oo-burst"
          >
            GitHub
          </a>{" "}
          ·{" "}
          <a
            href="https://github.com/StephenAbbott/opencheck?tab=License-1-ov-file"
            target="_blank"
            rel="noreferrer"
            className="font-mono text-oo-blue hover:text-oo-burst"
          >
            MIT license
          </a>{" "}
          · third-party data licensed per source — see{" "}
          <a
            href="https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md"
            target="_blank"
            rel="noreferrer"
            className="font-mono text-oo-blue hover:text-oo-burst"
          >
            ATTRIBUTIONS.md
          </a>
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
function useCountUp(target: number, duration = 800): number {
  const [value, setValue] = useState(0);
  const prev = useState(target)[0];
  // Re-run whenever target becomes non-zero (data loaded).
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (target === 0) return;
    const start = performance.now();
    const from = prev === target ? 0 : 0;
    const tick = (now: number) => {
      const t = Math.min((now - start) / duration, 1);
      // Ease-out cubic
      const eased = 1 - Math.pow(1 - t, 3);
      setValue(Math.round(from + (target - from) * eased));
      if (t < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }, [target, duration]);
  return value;
}

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

function SourceCounter({ sources }: { sources: { is_national_register: boolean }[] | null }) {
  const registerCount = sources ? sources.filter((s) => s.is_national_register).length : 0;
  const openCount = sources ? sources.filter((s) => !s.is_national_register).length : 0;

  const animatedRegisters = useCountUp(registerCount);
  const animatedOpen = useCountUp(openCount);

  if (!sources) return null;

  return (
    <div className="flex items-center gap-6 mt-5 pt-4 border-t border-white/10">
      <div className="flex items-baseline gap-2">
        <span className="font-mono font-bold text-[1.45rem] leading-none text-white tabular-nums">
          {animatedRegisters}
        </span>
        <span className="text-[11px] tracking-wide uppercase text-white/45">
          national registers
        </span>
      </div>
      <span className="text-white/15 text-[1.1rem]">·</span>
      <div className="flex items-baseline gap-2">
        <span className="font-mono font-bold text-[1.45rem] leading-none text-white tabular-nums">
          {animatedOpen}
        </span>
        <span className="text-[11px] tracking-wide uppercase text-white/45">
          open sources
        </span>
      </div>
    </div>
  );
}

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

// ---------------------------------------------------------------------
// Source bucket card
// ---------------------------------------------------------------------

function SourceBucketCard({
  bucket,
  riskByHit,
  sourceSignals = [],
}: {
  bucket: SourceBucket;
  riskByHit: Record<string, RiskSignal[]>;
  sourceSignals?: RiskSignal[];
}) {
  const stateLabel = bucket.error
    ? "error"
    : `${bucket.hits.length} result${bucket.hits.length === 1 ? "" : "s"}`;
  const stateColor = bucket.error
    ? "text-red-700"
    : "text-oo-muted";

  // Show only signals attributed to this specific source in the card header.
  // Entity-level signals across all sources are shown in the top-level
  // "Risk signals" strip above the source cards.
  const headerSignals = sourceSignals;

  return (
    <article className="bg-white border border-oo-rule rounded-oo">
      <header className="px-5 py-3 border-b border-oo-rule flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="font-head font-bold text-[15px] text-oo-ink">
            {bucket.sourceName}
          </h3>
          {headerSignals.length > 0 && (
            <div className="mt-1.5 flex flex-wrap gap-1">
              {headerSignals.map((sig, i) => (
                <RiskChip key={`${sig.code}-${i}`} signal={sig} compact />
              ))}
            </div>
          )}
        </div>
        <span className={`text-[11px] font-mono shrink-0 ${stateColor}`}>
          {stateLabel}
        </span>
      </header>
      {bucket.error && (
        <p className="px-5 py-3 text-[13px] text-red-700">{bucket.error}</p>
      )}
      {bucket.hits.length === 0 && !bucket.error && (
        <p className="px-5 py-3 text-[13px] text-oo-muted">No hits.</p>
      )}
      <ul className="divide-y divide-oo-rule">
        {bucket.hits.map((hit) => (
          <HitRow
            key={`${hit.source_id}:${hit.hit_id}`}
            hit={hit}
            riskSignals={riskByHit[`${hit.source_id}:${hit.hit_id}`] ?? []}
          />
        ))}
      </ul>
    </article>
  );
}

// ---------------------------------------------------------------------
// Skeleton source card — pulsing placeholder while a source is in flight
// ---------------------------------------------------------------------

function SkeletonSourceCard() {
  return (
    <article className="bg-white border border-oo-rule rounded-oo animate-pulse" aria-hidden>
      <header className="px-5 py-3 border-b border-oo-rule flex items-start justify-between gap-3">
        <div className="h-4 bg-oo-rule rounded w-44" />
        <div className="h-3 bg-oo-rule rounded w-12 mt-0.5" />
      </header>
      <div className="px-5 py-4 space-y-2.5">
        <div className="h-3 bg-oo-rule rounded w-3/4" />
        <div className="h-3 bg-oo-rule rounded w-1/2" />
        <div className="h-3 bg-oo-rule rounded w-2/3" />
      </div>
    </article>
  );
}

// ---------------------------------------------------------------------
// Subject card — top-of-page summary of the LEI lookup
// ---------------------------------------------------------------------

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
      <p className="text-[13px] leading-[1.7] text-oo-muted mb-4 max-w-2xl">
        Each subject has a pre-extracted Beneficial Ownership Data Standard
        (BODS) bundle on disk, so the lookup resolves entirely offline. Risk
        flags are pre-computed from the cached bundle. Use the search box above
        for any other LEI.
      </p>
      <ul
        className="grid gap-3"
        // 280px min keeps three subjects per row at desktop widths,
        // stacks on narrow viewports.
        style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 280px), 1fr))" }}
      >
        {EXAMPLE_LEIS.map((ex) => (
          <li key={ex.lei}>
            <button
              type="button"
              disabled={disabled}
              onClick={() => onPick(ex.lei)}
              className="w-full text-left bg-white border border-oo-rule rounded-oo p-4 transition-shadow hover:shadow-oo-card disabled:opacity-50"
            >
              <div className="font-head text-[14px] font-bold text-oo-ink leading-tight">
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
                    />
                  ))}
                </div>
              )}
              <div className="font-mono text-[10.5px] text-oo-blue mt-2 break-all">
                {ex.lei}
              </div>
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}

const HOW_IT_WORKS_STEPS = [
  {
    num: "1",
    accent: "#191d23" as const,
    title: "Paste a Legal Entity Identifier",
    body: (
      <>
        You supply a 20-character{" "}
        <a
          href="https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei"
          target="_blank"
          rel="noreferrer"
          className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
        >
          LEI
        </a>{" "}
        — OpenCheck's single entry point for any legal entity worldwide.
      </>
    ),
    badges: null,
  },
  {
    num: "2",
    accent: "#3d30d4" as const,
    title: "GLEIF bridges to national identifiers",
    body: (
      <>
        The LEI record carries{" "}
        <a
          href="https://www.gleif.org/en/newsroom/blog/transforming-data-into-opportunities-metric-of-the-month-mapping-network"
          target="_blank"
          rel="noreferrer"
          className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
        >
          registration data
        </a>{" "}
        that OpenCheck uses to derive bridging identifiers for each downstream
        source.
      </>
    ),
    badges: [
      "UK CH number",
      "OpenCorporates ID",
      "SIREN",
      "KvK number",
      "SE org number",
    ],
  },
  {
    num: "3",
    accent: "#3d30d4" as const,
    title: "Parallel queries to open sources",
    body: (
      <>
        Each source is queried using the right identifier for that dataset.
        Results are normalised into BODS statements.
      </>
    ),
    badges: [
      "GLEIF",
      "OpenSanctions",
      "OpenCorporates",
      "Companies House",
      "Bolagsverket",
      "OpenAleph",
      "Wikidata",
      "OpenTender",
    ],
  },
  {
    num: "4",
    accent: "#191d23" as const,
    title: "Risk signals + shareable BODS bundle",
    body: (
      <>
        Risk signals aligned with draft customer due diligence standards from
        the EU's Anti-Money Laundering Authority are computed deterministically
        across the assembled statements — checking for sanctions, flagged
        jurisdictions, complex corporate structures and more. The full bundle is
        one click away as JSON, JSONL or a ZIP with manifest and
        license notes.
      </>
    ),
    badges: null,
  },
] as const;

function HowItWorks() {
  return (
    <section className="mb-10 bg-white border border-oo-rule rounded-oo p-7">
      <SectionLabel>How it works</SectionLabel>
      <div className="mt-2 max-w-2xl">
        {HOW_IT_WORKS_STEPS.map((step, i) => {
          const isLast = i === HOW_IT_WORKS_STEPS.length - 1;
          return (
            <div key={step.num} className="flex gap-5">
              {/* Left rail — circle node + connector line */}
              <div className="flex flex-col items-center flex-shrink-0" style={{ width: 28 }}>
                <div
                  className="flex items-center justify-center rounded-full font-mono text-[11px] font-bold text-white flex-shrink-0"
                  style={{ width: 28, height: 28, background: step.accent }}
                >
                  {step.num}
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
                  {step.title}
                </p>
                <p className="text-[13px] leading-[1.65] text-oo-muted mt-1.5">
                  {step.body}
                </p>
                {step.badges && (
                  <div className="flex flex-wrap gap-1.5 mt-2.5">
                    {step.badges.map((b) => (
                      <span
                        key={b}
                        className="font-mono text-[10.5px] px-2 py-0.5 rounded border border-oo-rule bg-oo-bg text-oo-muted"
                      >
                        {b}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function SubjectCard({ lei, legalName }: { lei: string; legalName: string | null }) {
  return (
    <section className="mb-8 bg-white border border-oo-rule rounded-oo p-7 transition-shadow hover:shadow-oo-card">
      <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-blue">
        Subject
      </p>
      <h2 className="font-head font-bold text-oo-ink mt-2 leading-tight text-[clamp(1.25rem,2.5vw,1.6rem)]">
        {legalName || `LEI ${lei}`}
      </h2>
    </section>
  );
}

// ---------------------------------------------------------------------
// Hit row + drill-down
// ---------------------------------------------------------------------

function HitRow({
  hit,
  riskSignals,
}: {
  hit: SourceHit;
  riskSignals: RiskSignal[];
}) {
  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState<DeepenResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function toggle() {
    const next = !open;
    setOpen(next);
    if (next && !detail && !loading) {
      setLoading(true);
      setError(null);
      try {
        const data = await deepen(hit.source_id, hit.hit_id);
        setDetail(data);
      } catch (e) {
        setError(String(e));
      } finally {
        setLoading(false);
      }
    }
  }

  return (
    <li className="px-5 py-4">
      <div className="flex justify-between items-baseline gap-4">
        <div className="min-w-0">
          <div className="font-head font-bold text-[15px] text-oo-ink leading-snug">
            {hit.name}
            {hit.is_stub && (
              <span className="ml-2 text-[11px] font-mono bg-amber-50 text-amber-800 border border-amber-200 rounded px-1.5 py-0.5">
                stub
              </span>
            )}
          </div>
          <p className="text-[13px] text-oo-muted mt-1 leading-[1.6]">
            {hit.summary}
          </p>
          {Object.keys(hit.identifiers).length > 0 && (
            <p className="text-[11px] text-oo-muted mt-1.5 font-mono break-all">
              {Object.entries(hit.identifiers)
                .map(([k, v]) => `${k}=${v}`)
                .join(" · ")}
            </p>
          )}
          {riskSignals.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1">
              {riskSignals.map((sig, i) => (
                <RiskChip key={`${sig.code}-${i}`} signal={sig} compact />
              ))}
            </div>
          )}
        </div>
        <button
          onClick={toggle}
          className="text-[12px] font-mono text-oo-blue hover:text-oo-burst whitespace-nowrap"
        >
          {open ? "Hide" : "Go deeper →"}
        </button>
      </div>

      {open && (
        <div className="mt-4 bg-oo-bg rounded-oo p-4 text-[12px]">
          {loading && <p className="text-oo-muted">Fetching…</p>}
          {error && <p className="text-red-700">{error}</p>}
          {detail && <DeepenBlock detail={detail} />}
        </div>
      )}
    </li>
  );
}

// ---------------------------------------------------------------------
// BODS statement cards — field-level mapping view (Phase 4)
// ---------------------------------------------------------------------

type BODSStmt = Record<string, unknown>;

function stmtStr(obj: unknown, ...keys: string[]): string {
  let cur: unknown = obj;
  for (const k of keys) {
    if (cur == null || typeof cur !== "object") return "";
    cur = (cur as Record<string, unknown>)[k];
  }
  return typeof cur === "string" ? cur : "";
}

function stmtArr(obj: unknown, key: string): unknown[] {
  if (obj == null || typeof obj !== "object") return [];
  const v = (obj as Record<string, unknown>)[key];
  return Array.isArray(v) ? v : [];
}

/** Compact pill for a BODS identifier entry. */
function IdentifierPill({ id }: { id: unknown }) {
  const scheme = stmtStr(id, "schemeName") || stmtStr(id, "scheme");
  const value = stmtStr(id, "id");
  if (!value) return null;
  return (
    <span className="inline-flex items-center gap-1 font-mono text-[10px] bg-white border border-oo-rule rounded px-1.5 py-0.5">
      {scheme && <span className="text-oo-muted">{scheme}:</span>}
      <span className="text-oo-ink">{value}</span>
    </span>
  );
}

/** A single labelled field row inside a statement card. */
function FieldRow({
  label,
  value,
  mono,
}: {
  label: string;
  value: React.ReactNode;
  mono?: boolean;
}) {
  if (!value && value !== 0) return null;
  return (
    <div className="flex gap-2 items-baseline min-w-0">
      <span className="text-[10px] text-oo-muted font-semibold uppercase tracking-wide whitespace-nowrap w-28 shrink-0">
        {label}
      </span>
      <span
        className={`text-[11px] text-oo-ink break-words min-w-0 ${mono ? "font-mono" : ""}`}
      >
        {value}
      </span>
    </div>
  );
}

/** Card for a BODS entity statement. */
function EntityStatementCard({ stmt }: { stmt: BODSStmt }) {
  const rd = (stmt.recordDetails ?? {}) as Record<string, unknown>;
  const name = stmtStr(rd, "name");
  const entityType = stmtStr(rd, "entityType", "type");
  const jurisdiction = stmtStr(rd, "incorporatedInJurisdiction", "name");
  const jurisdictionCode = stmtStr(rd, "incorporatedInJurisdiction", "code");
  const foundingDate = stmtStr(rd, "foundingDate");
  const identifiers = stmtArr(rd, "identifiers");
  const addresses = stmtArr(rd, "addresses");
  const sourceDesc = stmtStr(stmt, "source", "description");
  const statementId = stmtStr(stmt, "statementId");

  return (
    <div className="rounded-oo border border-blue-200 bg-blue-50/40 overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-3 py-2 bg-blue-100/60 border-b border-blue-200">
        <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-blue-700">
          Entity
        </span>
        {entityType && (
          <span className="text-[10px] font-mono text-blue-600">{entityType}</span>
        )}
      </div>
      <div className="px-3 py-2.5 space-y-1.5">
        <FieldRow label="Name" value={name || <span className="text-oo-muted italic">unknown</span>} />
        {(jurisdiction || jurisdictionCode) && (
          <FieldRow
            label="Jurisdiction"
            value={[jurisdiction, jurisdictionCode].filter(Boolean).join(" · ")}
          />
        )}
        {foundingDate && <FieldRow label="Founded" value={foundingDate} mono />}
        {identifiers.length > 0 && (
          <FieldRow
            label="Identifiers"
            value={
              <span className="flex flex-wrap gap-1">
                {identifiers.map((id, i) => (
                  <IdentifierPill key={i} id={id} />
                ))}
              </span>
            }
          />
        )}
        {addresses.map((addr, i) => {
          const addrStr = stmtStr(addr, "address");
          const addrType = stmtStr(addr, "type");
          const addrCountry = stmtStr(addr, "country", "name");
          const full = [addrStr, addrCountry].filter(Boolean).join(", ");
          if (!full) return null;
          return (
            <FieldRow
              key={i}
              label={`Address${addrType ? ` (${addrType})` : ""}`}
              value={full}
            />
          );
        })}
        {sourceDesc && <FieldRow label="Source" value={sourceDesc} />}
        <details className="mt-1">
          <summary className="text-[10px] font-mono text-oo-muted cursor-pointer">
            {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
          </summary>
          <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
            {JSON.stringify(stmt, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  );
}

/** Card for a BODS person statement. */
function PersonStatementCard({ stmt }: { stmt: BODSStmt }) {
  const rd = (stmt.recordDetails ?? {}) as Record<string, unknown>;
  const names = stmtArr(rd, "names");
  const fullName =
    names.length > 0 ? stmtStr(names[0], "fullName") : "";
  const personType = stmtStr(rd, "personType");
  const birthDate = stmtStr(rd, "birthDate");
  const nationalities = stmtArr(rd, "nationalities");
  const identifiers = stmtArr(rd, "identifiers");
  const sourceDesc = stmtStr(stmt, "source", "description");
  const statementId = stmtStr(stmt, "statementId");

  return (
    <div className="rounded-oo border border-violet-200 bg-violet-50/40 overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-3 py-2 bg-violet-100/60 border-b border-violet-200">
        <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-violet-700">
          Person
        </span>
        {personType && (
          <span className="text-[10px] font-mono text-violet-600">{personType}</span>
        )}
      </div>
      <div className="px-3 py-2.5 space-y-1.5">
        <FieldRow label="Name" value={fullName || <span className="text-oo-muted italic">unknown</span>} />
        {birthDate && <FieldRow label="Born" value={birthDate} mono />}
        {nationalities.length > 0 && (
          <FieldRow
            label="Nationality"
            value={nationalities
              .map((n) => stmtStr(n, "name") || stmtStr(n, "code"))
              .filter(Boolean)
              .join(", ")}
          />
        )}
        {identifiers.length > 0 && (
          <FieldRow
            label="Identifiers"
            value={
              <span className="flex flex-wrap gap-1">
                {identifiers.map((id, i) => (
                  <IdentifierPill key={i} id={id} />
                ))}
              </span>
            }
          />
        )}
        {sourceDesc && <FieldRow label="Source" value={sourceDesc} />}
        <details className="mt-1">
          <summary className="text-[10px] font-mono text-oo-muted cursor-pointer">
            {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
          </summary>
          <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
            {JSON.stringify(stmt, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  );
}

/** Summarise a BODS interest entry as a short string. */
function describeInterest(interest: unknown): string {
  const type = stmtStr(interest, "type");
  const doi = stmtStr(interest, "directOrIndirect");
  const share = (interest as Record<string, unknown>)?.share as
    | Record<string, unknown>
    | undefined;
  let parts: string[] = [];
  if (type) parts.push(type);
  if (doi) parts.push(doi);
  if (share) {
    const exact = share.exact;
    const min = share.minimum;
    const max = share.maximum;
    if (exact != null) parts.push(`${exact}%`);
    else if (min != null && max != null) parts.push(`${min}–${max}%`);
    else if (min != null) parts.push(`≥${min}%`);
  }
  return parts.join(" · ");
}

/** Build a short name for a statement given a lookup map. */
function stmtLabel(
  id: string,
  lookup: Map<string, BODSStmt>
): string {
  const s = lookup.get(id);
  if (!s) return id.slice(0, 16) + "…";
  const rd = (s.recordDetails ?? {}) as Record<string, unknown>;
  if (s.recordType === "entity") return stmtStr(rd, "name") || id.slice(0, 16) + "…";
  if (s.recordType === "person") {
    const names = stmtArr(rd, "names");
    return (names.length > 0 ? stmtStr(names[0], "fullName") : "") || id.slice(0, 16) + "…";
  }
  return id.slice(0, 16) + "…";
}

/** Card for a BODS relationship (ownership/control) statement. */
function RelationshipStatementCard({
  stmt,
  lookup,
}: {
  stmt: BODSStmt;
  lookup: Map<string, BODSStmt>;
}) {
  const rd = (stmt.recordDetails ?? {}) as Record<string, unknown>;
  const subjectId = stmtStr(rd, "subject");
  const interestedPartyId = stmtStr(rd, "interestedParty");
  const interests = stmtArr(rd, "interests");
  const statementDate = stmtStr(stmt, "statementDate");
  const sourceDesc = stmtStr(stmt, "source", "description");
  const statementId = stmtStr(stmt, "statementId");

  return (
    <div className="rounded-oo border border-teal-200 bg-teal-50/40 overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-3 py-2 bg-teal-100/60 border-b border-teal-200">
        <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-teal-700">
          Ownership / Control
        </span>
        {statementDate && (
          <span className="text-[10px] font-mono text-teal-600">{statementDate}</span>
        )}
      </div>
      <div className="px-3 py-2.5 space-y-1.5">
        {subjectId && (
          <FieldRow
            label="Subject"
            value={stmtLabel(subjectId, lookup)}
          />
        )}
        {interestedPartyId && (
          <FieldRow
            label="Interested party"
            value={stmtLabel(interestedPartyId, lookup)}
          />
        )}
        {interests.length > 0 && (
          <FieldRow
            label="Interests"
            value={
              <span className="space-y-0.5 block">
                {interests.map((int, i) => {
                  const desc = describeInterest(int);
                  const details = stmtStr(int, "details");
                  return (
                    <span key={i} className="block">
                      {desc}
                      {details && (
                        <span className="text-oo-muted ml-1">({details})</span>
                      )}
                    </span>
                  );
                })}
              </span>
            }
          />
        )}
        {sourceDesc && <FieldRow label="Source" value={sourceDesc} />}
        <details className="mt-1">
          <summary className="text-[10px] font-mono text-oo-muted cursor-pointer">
            {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
          </summary>
          <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
            {JSON.stringify(stmt, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  );
}

/** Renders all BODS statements as structured cards with a collapse-to-JSON option. */
function BODSStatementCards({ statements }: { statements: BODSStmt[] }) {
  // Build lookup map (statementId → stmt) for relationship label resolution.
  const lookup = new Map<string, BODSStmt>();
  for (const s of statements) {
    const sid = stmtStr(s, "statementId");
    if (sid) lookup.set(sid, s);
  }

  return (
    <div className="space-y-2 mt-2">
      {statements.map((stmt, i) => {
        const type = stmtStr(stmt, "recordType");
        if (type === "entity")
          return <EntityStatementCard key={i} stmt={stmt} />;
        if (type === "person")
          return <PersonStatementCard key={i} stmt={stmt} />;
        if (type === "relationship")
          return (
            <RelationshipStatementCard key={i} stmt={stmt} lookup={lookup} />
          );
        // Fallback for unknown types
        return (
          <details key={i} className="text-[11px]">
            <summary className="font-mono text-oo-muted cursor-pointer">
              {type || "unknown"} statement
            </summary>
            <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
              {JSON.stringify(stmt, null, 2)}
            </pre>
          </details>
        );
      })}
    </div>
  );
}

function DeepenBlock({ detail }: { detail: DeepenResponse }) {
  return (
    <div className="space-y-4">
      {detail.license_notice && (
        <div className="bg-amber-50 border border-amber-200 text-amber-900 rounded-oo p-3">
          <div className="flex items-baseline justify-between gap-2">
            <span className="font-head font-bold text-[13px]">License notice</span>
            <LicenseChip license={detail.license} />
          </div>
          <p className="mt-1 leading-[1.6]">{detail.license_notice}</p>
        </div>
      )}
      {detail.bods.length > 0 && (
        <section>
          <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
            BODS · {detail.bods.length} statement
            {detail.bods.length === 1 ? "" : "s"}
          </h4>
          {detail.bods_issues.length > 0 && (
            <p className="text-amber-800 mb-2">
              {detail.bods_issues.length} validation issue
              {detail.bods_issues.length === 1 ? "" : "s"}
            </p>
          )}
          {/* Directed graph (via @openownership/bods-dagre). */}
          <BODSGraph statements={detail.bods} />
          {/* Structured statement cards — field-level mapping view. */}
          <BODSStatementCards statements={detail.bods as BODSStmt[]} />
          <details className="mt-3">
            <summary className="text-oo-muted cursor-pointer text-[11px] font-mono">
              Show raw JSON statements
            </summary>
            <pre className="mt-1 max-h-96 overflow-auto bg-white border border-oo-rule rounded-oo p-3 text-[10px]">
              {JSON.stringify(detail.bods, null, 2)}
            </pre>
          </details>
        </section>
      )}
      <section>
        <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
          Raw source payload
        </h4>
        <pre className="max-h-96 overflow-auto bg-white border border-oo-rule rounded-oo p-3 text-[10px]">
          {JSON.stringify(detail.raw, null, 2)}
        </pre>
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------
// Small bits
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// Risk chips and cross-source link row
// ---------------------------------------------------------------------

/**
 * Map a risk signal code to a colour palette + short display label.
 * Codes are stable strings from the backend ``opencheck.risk`` module.
 */
const RISK_PRESENTATION: Record<
  string,
  { label: string; classes: string }
> = {
  PEP: {
    label: "PEP",
    classes: "bg-violet-50 text-violet-700 border-violet-200",
  },
  SANCTIONED: {
    label: "Sanctioned",
    classes: "bg-rose-50 text-rose-700 border-rose-200",
  },
  OFFSHORE_LEAKS: {
    label: "Offshore leaks",
    classes: "bg-amber-50 text-amber-800 border-amber-200",
  },
  OPAQUE_OWNERSHIP: {
    label: "Opaque ownership",
    classes: "bg-slate-100 text-slate-700 border-slate-300",
  },
  // AMLA CDD RTS chips — distinct palette so reviewers can spot
  // BODS-derived signals at a glance.
  TRUST_OR_ARRANGEMENT: {
    label: "Trust / arrangement",
    classes: "bg-indigo-50 text-indigo-700 border-indigo-200",
  },
  NON_EU_JURISDICTION: {
    label: "Non-EU jurisdiction",
    classes: "bg-orange-50 text-orange-700 border-orange-200",
  },
  NOMINEE: {
    label: "Nominee",
    classes: "bg-fuchsia-50 text-fuchsia-700 border-fuchsia-200",
  },
  COMPLEX_OWNERSHIP_LAYERS: {
    label: "≥3 layers",
    classes: "bg-sky-50 text-sky-700 border-sky-200",
  },
  COMPLEX_CORPORATE_STRUCTURE: {
    label: "Complex corporate structure (AMLA)",
    classes: "bg-red-50 text-red-700 border-red-300 font-semibold",
  },
  POSSIBLE_OBFUSCATION: {
    label: "Possible obfuscation (advisory)",
    classes: "bg-yellow-50 text-yellow-800 border-yellow-300",
  },
  // Cross-source name match against OpenSanctions / EveryPolitician —
  // scoped to a related party inside the BODS bundle, not the subject.
  RELATED_PEP: {
    label: "Related PEP",
    classes: "bg-violet-50 text-violet-700 border-violet-300",
  },
  RELATED_SANCTIONED: {
    label: "Related sanctioned",
    classes: "bg-rose-50 text-rose-700 border-rose-300 font-semibold",
  },
  // FATF jurisdiction signals — derived from incorporatedInJurisdiction
  // codes on entity statements in the assembled BODS bundle.
  FATF_BLACK_LIST: {
    label: "FATF black list",
    classes: "bg-red-100 text-red-800 border-red-400 font-semibold",
  },
  FATF_GREY_LIST: {
    label: "FATF grey list",
    classes: "bg-orange-50 text-orange-800 border-orange-400",
  },
};

const CONFIDENCE_DOT: Record<string, string> = {
  high: "●",
  medium: "◐",
  low: "○",
};

function rank(confidence: string): number {
  return confidence === "high" ? 3 : confidence === "medium" ? 2 : 1;
}

function RiskChip({
  signal,
  compact = false,
}: {
  signal: RiskSignal;
  compact?: boolean;
}) {
  const presentation =
    RISK_PRESENTATION[signal.code] ?? {
      label: signal.code,
      classes: "bg-slate-100 text-slate-700 border-slate-200",
    };
  // Normal chips are deliberately larger so risk flags are hard to miss;
  // compact variant (inside hit rows) is slightly smaller but still readable.
  const padding = compact
    ? "px-2 py-0.5 text-[12px] font-medium"
    : "px-3 py-1 text-[13px] font-semibold";
  return (
    <span
      title={`${signal.summary}\n\nSource: ${signal.source_id}/${signal.hit_id}\nConfidence: ${signal.confidence}`}
      className={`inline-flex items-center gap-1.5 border rounded-full shadow-sm ${padding} ${presentation.classes}`}
    >
      <span aria-hidden className="text-[10px]">{CONFIDENCE_DOT[signal.confidence] ?? "•"}</span>
      <span>{presentation.label}</span>
    </span>
  );
}

// ---------------------------------------------------------------------
// Export panel
// ---------------------------------------------------------------------

/**
 * Download button + format selector that points at /export.
 *
 * Renders an in-place NC-license warning when any contributing source
 * carries a CC BY-NC clause — so users see the obligation BEFORE they
 * hit Download, not buried in LICENSES.md inside the zip.
 */
function ExportPanel({
  lei,
  legalName,
  sourceLicenses,
  contributingSourceIds,
}: {
  lei: string;
  legalName: string | null;
  sourceLicenses: Record<string, string>;
  contributingSourceIds: string[];
}) {
  const [format, setFormat] = useState<"zip" | "json" | "jsonl">("zip");

  const ncSources = contributingSourceIds.filter((id) =>
    (sourceLicenses[id] ?? "").toLowerCase().includes("nc")
  );

  const href = exportUrl(lei, format);

  return (
    <section className="mb-8 bg-white border border-oo-rule rounded-oo p-5">
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div className="min-w-0">
          <h2 className="font-head font-bold text-[15px] text-oo-ink">
            Download BODS bundle
          </h2>
          <p className="text-[13px] text-oo-muted mt-1 leading-[1.6]">
            Reproducible export for{" "}
            {legalName ? <span>{legalName} (</span> : null}
            <span className="font-mono">{lei}</span>
            {legalName ? <span>)</span> : null}. Includes BODS
            statements, manifest, and per-source license notes.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={format}
            onChange={(e) =>
              setFormat(e.target.value as "zip" | "json" | "jsonl")
            }
            className="border border-oo-rule rounded px-2 py-1.5 text-[13px] bg-white"
          >
            <option value="zip">ZIP (bods + manifest + licenses)</option>
            <option value="json">JSON (BODS array)</option>
            <option value="jsonl">JSONL (newline-delimited)</option>
          </select>
          <a
            href={href}
            // The `download` attr asks the browser to honour the
            // server's Content-Disposition filename rather than
            // opening the URL inline.
            download
            className="bg-oo-blue text-white text-[13px] font-medium rounded px-4 py-1.5 hover:bg-oo-burst transition-colors inline-block"
          >
            Download
          </a>
        </div>
      </div>
      {ncSources.length > 0 && (
        <p className="mt-3 text-[12px] bg-amber-50 border border-amber-200 text-amber-900 rounded-oo px-3 py-2 leading-[1.6]">
          <span className="font-head font-bold">License notice.</span>{" "}
          This bundle will include data from {ncSources.join(", ")} (CC
          BY-NC). The combined dataset inherits the non-commercial
          restriction — re-publication or commercial use is not
          permitted under the source license. See{" "}
          <span className="font-mono">LICENSES.md</span> inside the zip
          for details.
        </p>
      )}
    </section>
  );
}

// ---------------------------------------------------------------------
// ESG Panel — separate environmental / ESG data section
// ---------------------------------------------------------------------

/**
 * Leaf icon for the ESG panel header. Rendered inline in SVG so no icon
 * library dependency is needed.
 */
function LeafIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      className={className}
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
      focusable="false"
    >
      <path
        d="M12 2C6.5 2 2 7 2 12c0 1.8.5 3.5 1.4 4.9L12 22l8.6-5.1A10 10 0 0 0 22 12C22 7 17.5 2 12 2z"
        fill="currentColor"
        opacity="0.15"
      />
      <path
        d="M12 2C6.5 2 2 7 2 12c0 1.8.5 3.5 1.4 4.9L12 22l8.6-5.1A10 10 0 0 0 22 12C22 7 17.5 2 12 2z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinejoin="round"
      />
      <path
        d="M12 22V12M12 12C9 9 5 9 5 9"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
      />
    </svg>
  );
}

/**
 * Format a CO₂e tonnage figure for display.
 * ≥ 1 Mt → "X.X Mt"; ≥ 1 kt → "X,XXX kt"; otherwise "X,XXX t"
 */
function formatCo2e(tonnes: number): { value: string; unit: string } {
  if (tonnes >= 1_000_000) {
    return { value: (tonnes / 1_000_000).toFixed(1), unit: "Mt CO₂e" };
  }
  if (tonnes >= 1_000) {
    return { value: Math.round(tonnes / 1_000).toLocaleString(), unit: "kt CO₂e" };
  }
  return { value: Math.round(tonnes).toLocaleString(), unit: "t CO₂e" };
}

/**
 * Inline horizontal bar chart for sector emissions breakdown.
 * Max-bar width = 100%; each bar proportional to the top sector.
 */
function SectorBars({
  bySector,
  totalCo2e,
}: {
  bySector: Record<string, number>;
  totalCo2e: number;
}) {
  if (!bySector || Object.keys(bySector).length === 0) return null;
  const maxVal = Math.max(...Object.values(bySector));
  const sorted = Object.entries(bySector).sort((a, b) => b[1] - a[1]);
  return (
    <div className="mt-4 space-y-2">
      {sorted.map(([sector, value]) => {
        const pct = maxVal > 0 ? (value / maxVal) * 100 : 0;
        const shareOfTotal = totalCo2e > 0 ? ((value / totalCo2e) * 100).toFixed(0) : "—";
        const fmt = formatCo2e(value);
        return (
          <div key={sector}>
            <div className="flex items-baseline justify-between mb-1">
              <span className="text-[11px] text-emerald-900/70 capitalize font-medium">
                {sector.replace(/-/g, " ")}
              </span>
              <span className="text-[11px] font-mono text-emerald-900/60">
                {fmt.value} {fmt.unit} · {shareOfTotal}%
              </span>
            </div>
            <div className="h-1.5 rounded-full bg-emerald-100 overflow-hidden">
              <div
                className="h-full rounded-full bg-emerald-500 transition-all"
                style={{ width: `${pct}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}

/**
 * Card for a Climate TRACE / GEM hit. Shows:
 * - Entity name + GEM entity ID
 * - Total CO₂e in large type (from option 1 aesthetic)
 * - Year of estimate + unit
 * - Sector breakdown inline bar chart
 * - GEM parents (if any)
 * - Raw payload drill-down (same pattern as HitRow)
 */
function ClimateTRACECard({ hit }: { hit: SourceHit }) {
  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState<DeepenResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  // Pull emissions data out of the raw bundle the adapter returned.
  const raw = hit.raw as Record<string, unknown>;
  const emissions = (raw.emissions ?? {}) as {
    total_co2e_tonnes?: number;
    unit?: string;
    year?: number;
    by_sector?: Record<string, number>;
  };
  const parents = (raw.parents ?? []) as { entity_id: string; name: string }[];
  const totalCo2e = emissions.total_co2e_tonnes ?? 0;
  const bySector = emissions.by_sector ?? {};
  const year = emissions.year ?? 2024;
  const formatted = totalCo2e > 0 ? formatCo2e(totalCo2e) : null;

  async function toggle() {
    const next = !open;
    setOpen(next);
    if (next && !detail && !loading) {
      setLoading(true);
      setFetchError(null);
      try {
        const data = await deepen(hit.source_id, hit.hit_id);
        setDetail(data);
      } catch (e) {
        setFetchError(String(e));
      } finally {
        setLoading(false);
      }
    }
  }

  return (
    <div className="rounded-oo border border-emerald-200 bg-emerald-50/40 overflow-hidden">
      {/* Card header */}
      <div className="px-5 pt-4 pb-3 border-b border-emerald-200/60">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className="font-head font-bold text-[15px] text-emerald-950 leading-snug">
              {hit.name}
              {hit.is_stub && (
                <span className="ml-2 text-[11px] font-mono bg-amber-50 text-amber-800 border border-amber-200 rounded px-1.5 py-0.5">
                  stub
                </span>
              )}
            </div>
            <div className="text-[11px] font-mono text-emerald-700/70 mt-0.5">
              GEM entity {hit.identifiers.gem_entity_id}
            </div>
          </div>
          <button
            onClick={toggle}
            className="text-[12px] font-mono text-emerald-700 hover:text-emerald-900 whitespace-nowrap shrink-0"
          >
            {open ? "Hide" : "Go deeper →"}
          </button>
        </div>

        {/* Large CO₂e metric */}
        {formatted && (
          <div className="mt-4 flex items-end gap-3">
            <span className="font-head font-bold leading-none text-[2.6rem] text-emerald-800 tabular-nums">
              {formatted.value}
            </span>
            <div className="pb-1">
              <div className="text-[13px] font-semibold text-emerald-700">
                {formatted.unit}
              </div>
              <div className="text-[11px] text-emerald-600/70">
                {year} · direct assets
              </div>
            </div>
          </div>
        )}

        {!formatted && !hit.is_stub && (
          <p className="mt-3 text-[13px] text-emerald-700/60 italic">
            Emissions data not available for this entity.
          </p>
        )}

        {/* Sector breakdown */}
        {Object.keys(bySector).length > 0 && (
          <SectorBars bySector={bySector} totalCo2e={totalCo2e} />
        )}

        {/* GEM parents */}
        {parents.length > 0 && (
          <div className="mt-3 pt-3 border-t border-emerald-200/60">
            <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-700/60 mr-2">
              GEM parent{parents.length === 1 ? "" : "s"}
            </span>
            {parents.map((p) => (
              <span
                key={p.entity_id}
                className="inline-block text-[11px] font-mono text-emerald-900/70 bg-emerald-100 border border-emerald-200 rounded px-1.5 py-0.5 mr-1"
              >
                {p.name}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Drill-down */}
      {open && (
        <div className="px-5 py-4 bg-white/60 text-[12px]">
          {loading && <p className="text-emerald-700">Fetching…</p>}
          {fetchError && <p className="text-red-700">{fetchError}</p>}
          {detail && <DeepenBlock detail={detail} />}
        </div>
      )}
    </div>
  );
}

/**
 * Collapsible ESG panel. Sits below the CDD source cards.
 *
 * Any source adapter with ``category="esg"`` is routed here —
 * not just Climate TRACE. The visual language (green border,
 * leaf icon, "Environmental, Social, and Governance (ESG) Data" heading) signals clearly
 * that this is climate / ESG context, not a compliance check.
 */
function EsgPanel({
  buckets,
  pendingCount = 0,
}: {
  buckets: SourceBucket[];
  pendingCount?: number;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const hitCount = buckets.reduce((n, b) => n + b.hits.length, 0);

  return (
    <section className="mb-8">
      {/* Section divider */}
      <div className="flex items-center gap-3 mb-4">
        <div className="flex-1 h-px bg-emerald-200" />
        <div className="flex items-center gap-2 text-emerald-700">
          <LeafIcon className="w-4 h-4" />
          <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase">
            Environmental, Social, and Governance (ESG) Data
          </span>
        </div>
        <div className="flex-1 h-px bg-emerald-200" />
      </div>

      {/* Panel */}
      <div className="rounded-oo border border-emerald-200 bg-white overflow-hidden">
        {/* Panel header */}
        <button
          type="button"
          onClick={() => setCollapsed((c) => !c)}
          className="w-full flex items-center justify-between px-5 py-3 border-b border-emerald-200 bg-emerald-50/60 hover:bg-emerald-50 transition-colors text-left"
        >
          <div className="flex items-center gap-2.5">
            <LeafIcon className="w-4 h-4 text-emerald-600 shrink-0" />
            <div>
              <span className="font-head font-bold text-[14px] text-emerald-950">
                Environmental, Social, and Governance (ESG) Data
              </span>
              <span className="ml-2 text-[11px] font-mono text-emerald-600/70">
                {hitCount} result{hitCount === 1 ? "" : "s"} · {buckets.length} source{buckets.length === 1 ? "" : "s"}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            <span className="text-[11px] text-emerald-600/60 hidden sm:inline">
              ESG / climate risk · not a KYC source
            </span>
            <span className="text-[12px] font-mono text-emerald-700">
              {collapsed ? "Show ↓" : "Hide ↑"}
            </span>
          </div>
        </button>

        {!collapsed && (
          <div className="p-5 space-y-4">
            {/* Disclaimer */}
            <p className="text-[12px] leading-[1.65] text-emerald-800/70 bg-emerald-50 border border-emerald-200 rounded px-3 py-2">
              <span className="font-semibold">ESG context only.</span> Data
              from{" "}
              <a
                href="https://globalenergymonitor.org/"
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2 hover:text-emerald-900"
              >
                Global Energy Monitor
              </a>{" "}
              (CC BY 4.0) and{" "}
              <a
                href="https://climatetrace.org/"
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2 hover:text-emerald-900"
              >
                Climate TRACE
              </a>{" "}
              (CC BY 4.0). Emissions are satellite-derived estimates for
              directly owned assets — not a beneficial ownership or
              sanctions check.
            </p>

            {/* Cards — one per ESG source bucket */}
            {buckets.map((bucket) =>
              bucket.hits.map((hit) => (
                <ClimateTRACECard
                  key={`${hit.source_id}:${hit.hit_id}`}
                  hit={hit}
                />
              ))
            )}

            {/* Skeleton placeholders for in-flight ESG sources */}
            {Array.from({ length: pendingCount }).map((_, i) => (
              <SkeletonSourceCard key={`esg-pending-${i}`} />
            ))}

            {/* Error state */}
            {buckets
              .filter((b) => b.error && b.hits.length === 0)
              .map((b) => (
                <div
                  key={b.sourceId}
                  className="rounded-oo border border-red-200 bg-red-50 px-4 py-3 text-[13px] text-red-700"
                >
                  <span className="font-semibold">{b.sourceName}:</span>{" "}
                  {b.error}
                </div>
              ))}
          </div>
        )}
      </div>
    </section>
  );
}

function CrossSourceLinkRow({ link }: { link: CrossSourceLink }) {
  const confidenceClasses =
    link.confidence === "strong"
      ? "bg-emerald-50 text-emerald-700 border-emerald-200"
      : "bg-oo-bg text-oo-muted border-oo-rule";
  return (
    <li className="flex flex-wrap items-baseline gap-2 text-[13px]">
      <span
        className={`text-[11px] border rounded px-1.5 py-0.5 font-mono ${confidenceClasses}`}
      >
        {link.confidence}
      </span>
      <span className="font-mono text-oo-ink">
        {link.key} = {link.key_value}
      </span>
      <span className="text-oo-muted">→</span>
      <span className="text-oo-ink">
        {link.hits.map((h) => h.source_id).join(" · ")}
      </span>
      <span className="text-oo-muted italic">
        ({link.hits.map((h) => h.name).join(" / ")})
      </span>
    </li>
  );
}
