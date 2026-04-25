import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  deepen,
  fetchSources,
  streamSearch,
  type CrossSourceLink,
  type DeepenResponse,
  type RiskSignal,
  type SearchKind,
  type SourceHit,
} from "./lib/api";

/**
 * Phase 1 chat UI.
 *
 * - Single query input → SSE fan-out across adapters
 * - Progressive source cards (pending / streaming / complete / error)
 * - "Go deeper" drill-down on each hit: full raw payload + BODS v0.4 statements
 * - Source inventory panel with license chip per adapter
 */

type SourceState = "pending" | "streaming" | "complete" | "error";

interface SourceBucket {
  sourceId: string;
  sourceName: string;
  state: SourceState;
  hits: SourceHit[];
  error?: string;
}

export default function App() {
  const [query, setQuery] = useState("");
  const [kind, setKind] = useState<SearchKind>("entity");
  const [buckets, setBuckets] = useState<Record<string, SourceBucket>>({});
  const [crossSourceLinks, setCrossSourceLinks] = useState<CrossSourceLink[]>([]);
  const [riskSignals, setRiskSignals] = useState<RiskSignal[]>([]);
  const [running, setRunning] = useState(false);
  const cleanupRef = useRef<(() => void) | null>(null);

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchSources(),
  });

  useEffect(() => {
    return () => cleanupRef.current?.();
  }, []);

  function runSearch(e: React.FormEvent) {
    e.preventDefault();
    const q = query.trim();
    if (!q) return;

    cleanupRef.current?.();
    setBuckets({});
    setCrossSourceLinks([]);
    setRiskSignals([]);
    setRunning(true);

    cleanupRef.current = streamSearch(q, kind, {
      onSourceStarted: ({ source_id, source_name }) =>
        setBuckets((prev) => ({
          ...prev,
          [source_id]: {
            sourceId: source_id,
            sourceName: source_name,
            state: "streaming",
            hits: [],
          },
        })),
      onHit: (hit) =>
        setBuckets((prev) => {
          const bucket = prev[hit.source_id];
          if (!bucket) return prev;
          return {
            ...prev,
            [hit.source_id]: { ...bucket, hits: [...bucket.hits, hit] },
          };
        }),
      onSourceCompleted: ({ source_id }) =>
        setBuckets((prev) => {
          const bucket = prev[source_id];
          if (!bucket) return prev;
          return { ...prev, [source_id]: { ...bucket, state: "complete" } };
        }),
      onSourceError: ({ source_id, error }) =>
        setBuckets((prev) => {
          const bucket = prev[source_id];
          return {
            ...prev,
            [source_id]: {
              sourceId: source_id,
              sourceName: bucket?.sourceName ?? source_id,
              state: "error",
              hits: bucket?.hits ?? [],
              error,
            },
          };
        }),
      onCrossSourceLinks: ({ links }) => setCrossSourceLinks(links),
      onRiskSignals: ({ signals }) => setRiskSignals(signals),
      onDone: () => setRunning(false),
      onError: () => setRunning(false),
    });
  }

  const bucketList = useMemo(() => Object.values(buckets), [buckets]);
  const totalHits = bucketList.reduce((n, b) => n + b.hits.length, 0);

  // Index risk signals by `${source_id}:${hit_id}` so cards/rows can
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
      // Keep the highest-confidence signal per code for the summary tooltip.
      if (!existing || rank(sig.confidence) > rank(existing.confidence)) {
        seen.set(sig.code, sig);
      }
    }
    return Array.from(seen.values());
  }, [riskSignals]);

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-slate-200 bg-white px-6 py-4">
        <h1 className="text-xl font-semibold">OpenCheck</h1>
        <p className="text-sm text-slate-500">
          Chatbot-style corporate intelligence over open data · mapped to BODS v0.4
        </p>
      </header>

      <main className="flex-1 px-6 py-8 max-w-5xl mx-auto w-full">
        <form onSubmit={runSearch} className="flex gap-2 mb-6">
          <select
            value={kind}
            onChange={(e) => setKind(e.target.value as SearchKind)}
            className="border border-slate-300 rounded px-3 py-2 bg-white"
          >
            <option value="entity">Entity</option>
            <option value="person">Person</option>
          </select>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search a company or person (e.g. Rosneft, BP, Jane Smith)…"
            className="flex-1 border border-slate-300 rounded px-3 py-2"
          />
          <button
            type="submit"
            disabled={running}
            className="bg-slate-900 text-white rounded px-4 py-2 hover:bg-slate-700 disabled:opacity-50"
          >
            {running ? "Searching…" : "Search"}
          </button>
        </form>

        {aggregatedCodes.length > 0 && (
          <section className="mb-6">
            <h2 className="text-sm font-medium text-slate-500 uppercase tracking-wide mb-2">
              Risk signals
            </h2>
            <div className="flex flex-wrap gap-2">
              {aggregatedCodes.map((sig) => (
                <RiskChip key={sig.code} signal={sig} />
              ))}
            </div>
            <p className="text-xs text-slate-400 mt-2">
              Hover a chip for the rule that fired. Signals derived from open
              data; AMLA-aligned chips read BODS v0.4 statements.
            </p>
          </section>
        )}

        {crossSourceLinks.length > 0 && (
          <section className="mb-8 bg-white border border-slate-200 rounded p-4">
            <h2 className="text-sm font-medium text-slate-500 uppercase tracking-wide mb-2">
              Cross-source links
            </h2>
            <ul className="space-y-2">
              {crossSourceLinks.map((link, i) => (
                <CrossSourceLinkRow key={`${link.key}:${link.key_value}:${i}`} link={link} />
              ))}
            </ul>
          </section>
        )}

        {bucketList.length > 0 && (
          <section className="space-y-4 mb-10">
            <h2 className="text-sm font-medium text-slate-500 uppercase tracking-wide">
              {totalHits} hit{totalHits === 1 ? "" : "s"} across{" "}
              {bucketList.length} source{bucketList.length === 1 ? "" : "s"}
            </h2>
            {bucketList.map((b) => (
              <SourceBucketCard
                key={b.sourceId}
                bucket={b}
                riskByHit={riskByHit}
              />
            ))}
          </section>
        )}

        <section>
          <h2 className="text-sm font-medium text-slate-500 uppercase tracking-wide mb-3">
            Sources
          </h2>
          {sourcesQuery.isLoading && <p className="text-slate-500">Loading…</p>}
          {sourcesQuery.data && (
            <ul className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {sourcesQuery.data.sources.map((s) => (
                <li
                  key={s.id}
                  className="bg-white border border-slate-200 rounded p-3 text-sm"
                >
                  <div className="flex justify-between items-baseline">
                    <span className="font-medium">{s.name}</span>
                    <LicenseChip license={s.license} />
                  </div>
                  <p className="text-xs text-slate-500 mt-1">{s.attribution}</p>
                  <p className="text-xs text-slate-400 mt-1">
                    Supports: {s.supports.join(", ")} ·{" "}
                    {s.live_available ? "live ready" : "stub"}
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      </main>

      <footer className="border-t border-slate-200 bg-white px-6 py-3 text-xs text-slate-500">
        OpenCheck · MIT code · third-party data licensed per source — see
        ATTRIBUTIONS.md
      </footer>
    </div>
  );
}

// ---------------------------------------------------------------------
// Source bucket card
// ---------------------------------------------------------------------

function SourceBucketCard({
  bucket,
  riskByHit,
}: {
  bucket: SourceBucket;
  riskByHit: Record<string, RiskSignal[]>;
}) {
  const stateLabel = {
    pending: "queued",
    streaming: "searching…",
    complete: `${bucket.hits.length} result${bucket.hits.length === 1 ? "" : "s"}`,
    error: "error",
  }[bucket.state];

  const stateColor = {
    pending: "text-slate-400",
    streaming: "text-blue-600",
    complete: "text-slate-600",
    error: "text-red-600",
  }[bucket.state];

  return (
    <article className="bg-white border border-slate-200 rounded">
      <header className="px-4 py-2 border-b border-slate-100 flex items-baseline justify-between">
        <h3 className="font-medium">{bucket.sourceName}</h3>
        <span className={`text-xs ${stateColor}`}>{stateLabel}</span>
      </header>
      {bucket.error && (
        <p className="px-4 py-2 text-sm text-red-600">{bucket.error}</p>
      )}
      {bucket.hits.length === 0 && bucket.state === "complete" && (
        <p className="px-4 py-3 text-sm text-slate-400">No hits.</p>
      )}
      <ul className="divide-y divide-slate-100">
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
    <li className="px-4 py-3">
      <div className="flex justify-between items-baseline">
        <div>
          <div className="font-medium">
            {hit.name}
            {hit.is_stub && (
              <span className="ml-2 text-xs bg-amber-100 text-amber-800 rounded px-1">
                stub
              </span>
            )}
          </div>
          <p className="text-sm text-slate-600">{hit.summary}</p>
          {Object.keys(hit.identifiers).length > 0 && (
            <p className="text-xs text-slate-500 mt-1 font-mono">
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
          className="text-sm text-slate-700 hover:text-slate-900 underline underline-offset-2"
        >
          {open ? "Hide" : "Go deeper"}
        </button>
      </div>

      {open && (
        <div className="mt-3 bg-slate-50 rounded p-3 text-xs">
          {loading && <p className="text-slate-500">Fetching…</p>}
          {error && <p className="text-red-600">{error}</p>}
          {detail && <DeepenBlock detail={detail} />}
        </div>
      )}
    </li>
  );
}

function DeepenBlock({ detail }: { detail: DeepenResponse }) {
  return (
    <div className="space-y-3">
      {detail.license_notice && (
        <div className="bg-amber-50 border border-amber-200 text-amber-800 rounded p-2">
          <div className="flex items-baseline justify-between gap-2">
            <span className="font-medium">License notice</span>
            <LicenseChip license={detail.license} />
          </div>
          <p className="mt-1">{detail.license_notice}</p>
        </div>
      )}
      {detail.risk_signals.length > 0 && (
        <section>
          <h4 className="font-medium text-slate-700 mb-1">
            Risk signals (deepen-time)
          </h4>
          <div className="flex flex-wrap gap-1">
            {detail.risk_signals.map((sig, i) => (
              <RiskChip key={`${sig.code}-${i}`} signal={sig} />
            ))}
          </div>
        </section>
      )}
      {detail.bods.length > 0 && (
        <section>
          <h4 className="font-medium text-slate-700 mb-1">
            BODS v0.4 · {detail.bods.length} statement
            {detail.bods.length === 1 ? "" : "s"}
          </h4>
          {detail.bods_issues.length > 0 && (
            <p className="text-amber-700 mb-2">
              {detail.bods_issues.length} validation issue
              {detail.bods_issues.length === 1 ? "" : "s"}
            </p>
          )}
          <pre className="max-h-96 overflow-auto bg-white border border-slate-200 rounded p-2">
            {JSON.stringify(detail.bods, null, 2)}
          </pre>
        </section>
      )}
      <section>
        <h4 className="font-medium text-slate-700 mb-1">Raw source payload</h4>
        <pre className="max-h-96 overflow-auto bg-white border border-slate-200 rounded p-2">
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
    ? "bg-amber-50 text-amber-700 border-amber-200"
    : "bg-emerald-50 text-emerald-700 border-emerald-200";
  return (
    <span
      className={`text-xs border rounded px-1.5 py-0.5 font-mono ${classes}`}
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
  const padding = compact ? "px-1.5 py-0.5 text-[11px]" : "px-2 py-0.5 text-xs";
  return (
    <span
      title={`${signal.summary}\n\nSource: ${signal.source_id}/${signal.hit_id}\nConfidence: ${signal.confidence}`}
      className={`inline-flex items-baseline gap-1 border rounded ${padding} ${presentation.classes}`}
    >
      <span aria-hidden>{CONFIDENCE_DOT[signal.confidence] ?? "•"}</span>
      <span>{presentation.label}</span>
    </span>
  );
}

function CrossSourceLinkRow({ link }: { link: CrossSourceLink }) {
  const confidenceClasses =
    link.confidence === "strong"
      ? "bg-emerald-50 text-emerald-700 border-emerald-200"
      : "bg-slate-100 text-slate-600 border-slate-200";
  return (
    <li className="flex flex-wrap items-baseline gap-2 text-sm">
      <span
        className={`text-[11px] border rounded px-1.5 py-0.5 font-mono ${confidenceClasses}`}
      >
        {link.confidence}
      </span>
      <span className="font-mono text-slate-700">
        {link.key} = {link.key_value}
      </span>
      <span className="text-slate-400">→</span>
      <span className="text-slate-600">
        {link.hits.map((h) => h.source_id).join(" · ")}
      </span>
      <span className="text-slate-400 text-xs italic">
        ({link.hits.map((h) => h.name).join(" / ")})
      </span>
    </li>
  );
}
