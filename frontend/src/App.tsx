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

export default function App() {
  const [leiInput, setLeiInput] = useState("");
  const [result, setResult] = useState<LookupResponse | null>(null);
  const [looking, setLooking] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchSources(),
  });

  async function runLookup(e: React.FormEvent) {
    e.preventDefault();
    const lei = leiInput.trim().toUpperCase();
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
      setLeiInput(lei); // canonicalise the input on success
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLooking(false);
    }
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

  // Index risk signals by `${source_id}:${hit_id}` so cards/rows can
  // pull their own chips without re-scanning the whole list.
  const riskByHit = useMemo(() => {
    const out: Record<string, RiskSignal[]> = {};
    for (const sig of result?.risk_signals ?? []) {
      const k = `${sig.source_id}:${sig.hit_id}`;
      (out[k] = out[k] ?? []).push(sig);
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
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-slate-200 bg-white px-6 py-4">
        <h1 className="text-xl font-semibold">OpenCheck</h1>
        <p className="text-sm text-slate-500">
          Customer due diligence risk checks driven by the LEI and open
          data · mapped to BODS v0.4
        </p>
      </header>

      <main className="flex-1 px-6 py-8 max-w-5xl mx-auto w-full">
        <form onSubmit={runLookup} className="mb-6">
          <label
            htmlFor="lei-input"
            className="block text-xs font-medium text-slate-500 uppercase tracking-wide mb-1"
          >
            Legal Entity Identifier
          </label>
          <div className="flex gap-2">
            <input
              id="lei-input"
              type="text"
              value={leiInput}
              onChange={(e) => setLeiInput(e.target.value)}
              placeholder="e.g. 213800LH1BZH3DI6G760"
              spellCheck={false}
              autoComplete="off"
              className="flex-1 border border-slate-300 rounded px-3 py-2 font-mono uppercase"
              maxLength={20}
            />
            <button
              type="submit"
              disabled={looking || !leiInput.trim()}
              className="bg-slate-900 text-white rounded px-4 py-2 hover:bg-slate-700 disabled:opacity-50"
            >
              {looking ? "Looking up…" : "Look up"}
            </button>
          </div>
          <p className="text-xs text-slate-500 mt-1">
            Look up an entity by its 20-character LEI. We query GLEIF
            first, then use the LEI to bridge to Companies House,
            OpenSanctions, OpenAleph, Wikidata, OpenTender, and (soon)
            OpenCorporates.
          </p>
        </form>

        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 text-red-700 rounded p-3 text-sm">
            {error}
          </div>
        )}

        {result && <SubjectCard result={result} />}

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
                    <a
                      href={s.homepage}
                      target="_blank"
                      rel="noreferrer"
                      className="font-medium hover:underline underline-offset-2"
                    >
                      {s.name}
                    </a>
                    <LicenseChip license={s.license} />
                  </div>
                  {s.description && (
                    <p className="text-xs text-slate-600 mt-1">
                      {s.description}
                    </p>
                  )}
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
        <a
          href="https://github.com/StephenAbbott/opencheck"
          target="_blank"
          rel="noreferrer"
          className="underline underline-offset-2 hover:text-slate-700"
        >
          OpenCheck
        </a>{" "}
        ·{" "}
        <a
          href="https://github.com/StephenAbbott/opencheck?tab=License-1-ov-file"
          target="_blank"
          rel="noreferrer"
          className="underline underline-offset-2 hover:text-slate-700"
        >
          MIT license
        </a>{" "}
        · third-party data licensed per source — see{" "}
        <a
          href="https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md"
          target="_blank"
          rel="noreferrer"
          className="underline underline-offset-2 hover:text-slate-700"
        >
          ATTRIBUTIONS.md
        </a>
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
  const stateLabel = bucket.error
    ? "error"
    : `${bucket.hits.length} result${bucket.hits.length === 1 ? "" : "s"}`;
  const stateColor = bucket.error ? "text-red-600" : "text-slate-600";

  return (
    <article className="bg-white border border-slate-200 rounded">
      <header className="px-4 py-2 border-b border-slate-100 flex items-baseline justify-between">
        <h3 className="font-medium">{bucket.sourceName}</h3>
        <span className={`text-xs ${stateColor}`}>{stateLabel}</span>
      </header>
      {bucket.error && (
        <p className="px-4 py-2 text-sm text-red-600">{bucket.error}</p>
      )}
      {bucket.hits.length === 0 && !bucket.error && (
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
// Subject card — top-of-page summary of the LEI lookup
// ---------------------------------------------------------------------

function SubjectCard({ result }: { result: LookupResponse }) {
  const ids = Object.entries(result.derived_identifiers);
  return (
    <section className="mb-6 bg-white border border-slate-200 rounded p-5">
      <p className="text-xs font-medium text-slate-500 uppercase tracking-wide">
        Subject
      </p>
      <h2 className="text-xl font-semibold mt-1">
        {result.legal_name || `LEI ${result.lei}`}
      </h2>
      <p className="text-xs text-slate-500 font-mono mt-1">
        LEI {result.lei}
        {result.jurisdiction && ` · ${result.jurisdiction}`}
      </p>
      {ids.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-1.5">
          {ids.map(([k, v]) => (
            <span
              key={k}
              title={`${k} (derived via GLEIF + Wikidata for cross-source matching)`}
              className="inline-flex gap-1 text-xs border border-slate-200 rounded px-2 py-0.5 font-mono bg-slate-50"
            >
              <span className="text-slate-500">{k}=</span>
              <span className="text-slate-800">{v}</span>
            </span>
          ))}
        </div>
      )}
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
          {/* Directed graph (via @openownership/bods-dagre). */}
          <BODSGraph statements={detail.bods} />
          <details className="mt-2">
            <summary className="text-slate-500 cursor-pointer text-xs">
              Show JSON statements
            </summary>
            <pre className="mt-1 max-h-96 overflow-auto bg-white border border-slate-200 rounded p-2">
              {JSON.stringify(detail.bods, null, 2)}
            </pre>
          </details>
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
    <section className="mb-8 bg-slate-50 border border-slate-200 rounded p-4">
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div>
          <h2 className="text-sm font-medium text-slate-700">
            Download BODS bundle
          </h2>
          <p className="text-xs text-slate-500 mt-1">
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
            className="border border-slate-300 rounded px-2 py-1 text-sm bg-white"
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
            className="bg-slate-900 text-white text-sm rounded px-3 py-1.5 hover:bg-slate-700 inline-block"
          >
            Download
          </a>
        </div>
      </div>
      {ncSources.length > 0 && (
        <p className="mt-3 text-xs bg-amber-50 border border-amber-200 text-amber-800 rounded px-2 py-1.5">
          <span className="font-medium">License notice.</span> This bundle
          will include data from {ncSources.join(", ")} (CC BY-NC). The
          combined dataset inherits the non-commercial restriction —
          re-publication or commercial use is not permitted under the
          source license. See <span className="font-mono">LICENSES.md</span>{" "}
          inside the zip for details.
        </p>
      )}
    </section>
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
