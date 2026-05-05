import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import BODSGraph from "./components/BODSGraph";
import {
  deepen,
  exportUrl,
  fetchSources,
  isValidLei,
  lookup,
  type CrossSourceLink,
  type DeepenResponse,
  type LookupResponse,
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
    signals: [],
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
  {
    lei: "213800BC4TEGCCQH9V07",
    name: "Melli Bank PLC",
    hint: "Iran-linked UK bank",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
      { code: "FATF_BLACK_LIST", confidence: "high" },
    ],
  },
];

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
  const [result, setResult] = useState<LookupResponse | null>(null);
  const [looking, setLooking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // ``main`` shows the LEI form + lookup result; ``sources`` shows the
  // source inventory page. Kept as state rather than a router so we
  // don't pull in react-router for two views.
  const [view, setView] = useState<"main" | "sources">("main");

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchSources(),
  });

  async function lookupLei(rawLei: string) {
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
    setLooking(true);
    setError(null);
    setResult(null);
    try {
      const data = await lookup(lei);
      setResult(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLooking(false);
    }
  }

  async function runLookup(e: React.FormEvent) {
    e.preventDefault();
    await lookupLei(leiInput);
  }

  // Group hits by source_id for the per-source bucket cards. With the
  // LEI flow the result arrives in one shot — no streaming state.
  const bucketList = useMemo<SourceBucket[]>(() => {
    if (!result) return [];
    const byId = new Map<string, SourceBucket>();
    const adapterIndex: Record<string, string> = sourcesQuery.data
      ? Object.fromEntries(
          sourcesQuery.data.sources.map((s) => [s.id, s.name])
        )
      : {};
    for (const hit of result.hits) {
      const existing = byId.get(hit.source_id);
      if (existing) {
        existing.hits.push(hit);
      } else {
        byId.set(hit.source_id, {
          sourceId: hit.source_id,
          sourceName: adapterIndex[hit.source_id] ?? hit.source_id,
          hits: [hit],
          error: result.errors[hit.source_id],
        });
      }
    }
    // Surface adapters that errored even when they returned no hits.
    for (const [source_id, errMsg] of Object.entries(result.errors)) {
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
  }, [result, sourcesQuery.data]);

  const totalHits = bucketList.reduce((n, b) => n + b.hits.length, 0);

  // Index risk signals by `${source_id}:${hit_id}` so hit rows can
  // pull their own chips without re-scanning the whole list.
  const riskByHit = useMemo(() => {
    const out: Record<string, RiskSignal[]> = {};
    for (const sig of result?.risk_signals ?? []) {
      const k = `${sig.source_id}:${sig.hit_id}`;
      (out[k] = out[k] ?? []).push(sig);
    }
    return out;
  }, [result]);

  // Index risk signals by source_id so each source card shows only the
  // signals attributed to that source — not all entity-level signals.
  const riskBySource = useMemo(() => {
    const out: Record<string, RiskSignal[]> = {};
    for (const sig of result?.risk_signals ?? []) {
      (out[sig.source_id] = out[sig.source_id] ?? []).push(sig);
    }
    return out;
  }, [result]);

  // Distinct codes — used for the top-level summary chip strip.
  const aggregatedCodes = useMemo(() => {
    const seen = new Map<string, RiskSignal>();
    for (const sig of result?.risk_signals ?? []) {
      const existing = seen.get(sig.code);
      if (!existing || rank(sig.confidence) > rank(existing.confidence)) {
        seen.set(sig.code, sig);
      }
    }
    return Array.from(seen.values());
  }, [result]);

  const crossSourceLinks: CrossSourceLink[] = result?.cross_source_links ?? [];

  return (
    <div className="min-h-screen flex flex-col bg-oo-bg">
      {/*
       * Header — full-width dark banner, BO design system.
       * Decorative blue radial gradient sits top-right (rgba 61,48,212,0.28)
       * fading to transparent. Inline style because Tailwind doesn't
       * have a clean utility for offset radial gradients.
       */}
      <header
        className="relative overflow-hidden bg-oo-navy text-white px-6 sm:px-10 lg:px-16 py-10 sm:py-12"
        style={{
          backgroundImage:
            "radial-gradient(circle 500px at calc(100% + 80px) -80px, rgba(61, 48, 212, 0.28), transparent)",
        }}
      >
        <div className="max-w-oo-page mx-auto relative">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-light">
                Customer due diligence
              </p>
              <div className="flex items-center gap-3 mt-2">
                <button
                  type="button"
                  onClick={() => {
                    // Click the title to return to a fresh homepage state.
                    setView("main");
                    setResult(null);
                    setError(null);
                    setLeiInput("");
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
            <nav>
              <button
                type="button"
                onClick={() => setView(view === "main" ? "sources" : "main")}
                className="text-[12px] font-mono text-oo-light hover:text-white underline underline-offset-4 whitespace-nowrap"
              >
                {view === "main" ? "About the sources →" : "← Back to lookup"}
              </button>
            </nav>
          </div>
          <p className="mt-3 max-w-2xl text-[15px] font-light leading-[1.65] text-white/70">
            Customer due diligence risk checks driven by the Legal
            Entity Identifier (LEI) and open data — mapped to version
            0.4 of the Beneficial Ownership Data Standard.
          </p>
        </div>
      </header>

      <main className="flex-1 px-6 sm:px-10 lg:px-16 py-12 max-w-oo-page mx-auto w-full">
        {view === "main" && (
        <>
        <form
          onSubmit={runLookup}
          className="mb-8 bg-white border border-oo-rule rounded-oo p-6"
        >
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
              className="flex-1 border border-oo-rule rounded px-3 py-2.5 font-mono uppercase tracking-wide focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
              maxLength={20}
            />
            <button
              type="submit"
              disabled={looking || !leiInput.trim()}
              className="bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
            >
              {looking ? "Looking up…" : "Look up"}
            </button>
          </div>
          <p className="text-[13px] leading-[1.7] text-oo-muted mt-3 max-w-2xl">
            <a
              href="https://search.gleif.org/#/search/"
              target="_blank"
              rel="noopener noreferrer"
              className="underline hover:text-oo-ink transition-colors"
            >
              Look up an entity
            </a>{" "}
            by its 20-character LEI. We query GLEIF first, then use the LEI to
            bridge to national company registries, OpenCorporates, OpenSanctions,
            OpenAleph, Wikidata, and OpenTender.
          </p>
        </form>

        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
            {error}
          </div>
        )}

        {!result && !looking && !error && (
          <>
            <ExampleLeiPicker onPick={lookupLei} disabled={looking} />
            <HowItWorks />
          </>
        )}

        {result && <SubjectCard result={result} />}

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
              open data; AMLA-aligned chips read BODS v0.4 statements.
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

        {bucketList.length > 0 && (
          <section className="mb-8">
            <SectionLabel>
              {totalHits} hit{totalHits === 1 ? "" : "s"} across{" "}
              {bucketList.length} source{bucketList.length === 1 ? "" : "s"}
            </SectionLabel>
            <div className="space-y-4">
              {bucketList.map((b) => (
                <SourceBucketCard
                  key={b.sourceId}
                  bucket={b}
                  riskByHit={riskByHit}
                  sourceSignals={riskBySource[b.sourceId] ?? []}
                />
              ))}
            </div>
          </section>
        )}

        {result && totalHits > 0 && (
          <ExportPanel
            lei={result.lei}
            legalName={result.legal_name}
            sourceLicenses={
              sourcesQuery.data
                ? Object.fromEntries(
                    sourcesQuery.data.sources.map((s) => [s.id, s.license])
                  )
                : {}
            }
            contributingSourceIds={bucketList
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
              OpenCheck queries the open-data sources below. GLEIF is
              the entry point — its Legal Entity Identifier (LEI) acts
              as a connector across the rest. Each source ships its
              data under its own license; non-commercial sources
              propagate that obligation through the export bundle.
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
                {sourcesQuery.data.sources.map((s, i) => (
                  <li
                    key={s.id}
                    className="bg-white border border-oo-rule rounded-oo p-6 text-sm transition-shadow hover:shadow-oo-card"
                  >
                    <div className="flex items-baseline gap-3 mb-1">
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
                      <span className="ml-auto">
                        <LicenseChip license={s.license} />
                      </span>
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
      </main>

      {/* GODIN ribbon — attribution banner. */}
      <aside
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
            title="Global Open Data Integration Network"
          >
            <img
              src="https://godin.gleif.org/images/512/14456540/GODINRGBColourWide.png"
              alt="GODIN"
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
            exists to enable.
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
            OpenCheck
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
        Each subject has a pre-extracted Open Ownership BODS bundle on
        disk, so the lookup resolves entirely offline. Risk flags are
        pre-computed from the cached bundle. Use the search box above
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
          ISO 17442 LEI
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
        Results are normalised into{" "}
        <a
          href="https://standard.openownership.org/en/0.4.0/"
          target="_blank"
          rel="noreferrer"
          className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
        >
          BODS v0.4
        </a>{" "}
        statements.
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
        AMLA CDD RTS–aligned risk signals are computed deterministically across
        the assembled statements — sanctions, FATF jurisdictions, complex
        corporate structures, and more. The full bundle is one click away as
        BODS v0.4 JSON, JSONL, or a ZIP with manifest and license notes.
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

function SubjectCard({ result }: { result: LookupResponse }) {
  return (
    <section className="mb-8 bg-white border border-oo-rule rounded-oo p-7 transition-shadow hover:shadow-oo-card">
      <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-blue">
        Subject
      </p>
      <h2 className="font-head font-bold text-oo-ink mt-2 leading-tight text-[clamp(1.25rem,2.5vw,1.6rem)]">
        {result.legal_name || `LEI ${result.lei}`}
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
            BODS v0.4 · {detail.bods.length} statement
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
          <details className="mt-2">
            <summary className="text-oo-muted cursor-pointer text-[11px] font-mono">
              Show JSON statements
            </summary>
            <pre className="mt-1 max-h-96 overflow-auto bg-white border border-oo-rule rounded-oo p-3">
              {JSON.stringify(detail.bods, null, 2)}
            </pre>
          </details>
        </section>
      )}
      <section>
        <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
          Raw source payload
        </h4>
        <pre className="max-h-96 overflow-auto bg-white border border-oo-rule rounded-oo p-3">
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
            {legalName ? <span>)</span> : null}. Includes BODS v0.4
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
