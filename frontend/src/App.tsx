import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import SearchLoadingGrid from "./components/SearchLoadingGrid";
import {
  fetchSources,
  isValidLei,
  streamLookup,
  type CrossSourceLink,
  type RiskSignal,
  type SourceHit,
} from "./lib/api";
import { OpenCheckIcon, GleifIcon } from "./components/icons";
import { RiskChip, RISK_PRESENTATION, rank } from "./components/risk/RiskChip";
import { ExportPanel } from "./components/export/ExportPanel";
import { SubjectCard } from "./components/cdd/SubjectCard";
import {
  SourceBucketCard,
  SkeletonSourceCard,
  type SourceBucket,
} from "./components/cdd/SourceBucketCard";
import { EsgPanel } from "./components/cdd/EsgPanel";


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


export default function App() {
  const [leiInput, setLeiInput] = useState("");

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

  // ── LEI lookup mutation ───────────────────────────────────────────────────
  // Opens the SSE stream for /lookup-stream. The mutation is considered
  // "pending" (i.e. showing the loading grid) until the backend emits the
  // gleif_done event confirming the entity; all subsequent streaming state
  // (hits, risk signals, cross-source links) is managed via useState below.
  const lookupMutation = useMutation<{ lei: string; legal_name: string | null }, Error, string>({
    mutationFn: (lei: string) =>
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
            setStreaming(true);
            resolve({ lei: e.lei, legal_name: e.legal_name });
          },
          onSourcesApplicable: (e) => setApplicableSources(e.source_ids),
          onHit: (e) => setHits((prev) => [...prev, e]),
          onSourceCompleted: (e) =>
            setCompletedSources((prev) => new Set([...prev, e.source_id])),
          onSourceError: (e) => {
            setErrors((prev) => ({ ...prev, [e.source_id]: e.error }));
            setCompletedSources((prev) => new Set([...prev, e.source_id]));
          },
          onCrossSourceLinks: (e) => setCrossSourceLinks(e.links),
          onRiskSignals: (e) => setRiskSignals(e.signals),
          onDone: () => {
            setStreaming(false);
            cleanupRef.current = null;
          },
          onError: (detail) => {
            setStreaming(false);
            cleanupRef.current = null;
            reject(new Error(detail));
          },
        });
        cleanupRef.current = cleanup;
      }),
  });

  function lookupLei(rawLei: string) {
    const lei = rawLei.trim().toUpperCase();
    setLeiInput(lei);
    setView("main");
    // Cancel any in-flight stream before starting a new one.
    cleanupRef.current?.();
    cleanupRef.current = null;
    lookupMutation.mutate(lei);
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
                    lookupMutation.reset();
                    nameSearchMutation.reset();
                    setLeiInput("");
                    setNameQuery("");
                    setSearchMode("name");
                  }}
                  aria-label="Back to homepage"
                  className="flex items-center gap-3 hover:opacity-80 transition-opacity text-left"
                >
                  <OpenCheckIcon className="h-[clamp(2rem,4vw,2.6rem)] w-auto flex-shrink-0" />
                  <span className="font-head font-bold text-white leading-tight text-[clamp(1.6rem,4vw,2.4rem)]">
                    Open<span className="text-[#93c5fd]">Check</span><span className="relative -top-2.5 ml-1 text-[8px] font-semibold tracking-oo-eyebrow uppercase bg-white/15 text-white/90 rounded px-1.5 py-0.5 border border-white/25 align-top">BETA</span>
                  </span>
                </button>
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
                    disabled={nameSearchMutation.isPending || !nameQuery.trim()}
                    aria-busy={nameSearchMutation.isPending}
                    className="bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                  >
                    {nameSearchMutation.isPending ? "Searching…" : "Search"}
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
                  disabled={lookupMutation.isPending || !leiInput.trim()}
                  aria-busy={lookupMutation.isPending}
                  className="bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                >
                  {lookupMutation.isPending ? "Looking up…" : "Look up"}
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
          {lookupMutation.isError && (
            <div role="alert" className="mb-6 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
              {lookupMutation.error?.message}
            </div>
          )}
        </div>

        {lookupMutation.isPending && (
          <SearchLoadingGrid sources={sourcesQuery.data?.sources ?? []} />
        )}

        {!streamingLei && !lookupMutation.isPending && !streaming && !lookupMutation.isError && !nameSearchMutation.data && !nameSearchMutation.isPending && (
          <>
            <ExampleLeiPicker onPick={lookupLei} disabled={lookupMutation.isPending || streaming} />
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

        {(crossSourceLinks.length > 0 || gleifMappedIds.length > 0) && (
          <section className="mb-8 bg-white border border-oo-rule rounded-oo p-5">
            <SectionLabel>Cross-source identifiers</SectionLabel>
            <CrossSourceIdentifiersTable
              links={crossSourceLinks}
              gleifMapped={gleifMappedIds}
            />
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
                <div key={b.sourceId}>
                  <SourceBucketCard
                    bucket={b}
                    riskByHit={riskByHit}
                    sourceSignals={riskBySource[b.sourceId] ?? []}
                  />
                  {b.sourceId === "gleif" && gleifChildrenInfo && (
                    <p className="text-[12px] text-oo-muted mt-2 px-1">
                      Showing {gleifChildrenInfo.fetched} of {gleifChildrenInfo.total.toLocaleString()} direct subsidiaries in BODS statements (GLEIF Level 2 — first page only)
                    </p>
                  )}
                </div>
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

function CrossSourceIdentifiersTable({
  links,
  gleifMapped,
}: {
  links: CrossSourceLink[];
  gleifMapped: { scheme: string; value: string }[];
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
                  <span
                    key={h.source_id}
                    className="text-[11px] bg-oo-bg border border-oo-rule rounded px-1.5 py-0.5 font-mono text-oo-muted"
                  >
                    {h.source_id}
                  </span>
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
