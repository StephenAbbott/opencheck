import { useEffect, useState } from "react";
import { deepen } from "../../lib/api";
import type { BodsBreakdown, DeepenResponse, SourceHit } from "../../lib/api";
import { DeepenBlock, SkeletonSourceCard } from "./SourceBucketCard";
import type { SourceBucket } from "./SourceBucketCard";

// ---------------------------------------------------------------------
// LeafIcon — inline SVG for the ESG panel header
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// CO₂e formatting helpers
// ---------------------------------------------------------------------

function formatCo2e(tonnes: number): { value: string; unit: string } {
  if (tonnes >= 1_000_000) {
    return { value: (tonnes / 1_000_000).toFixed(1), unit: "Mt CO₂e" };
  }
  if (tonnes >= 1_000) {
    return { value: Math.round(tonnes / 1_000).toLocaleString(), unit: "kt CO₂e" };
  }
  return { value: Math.round(tonnes).toLocaleString(), unit: "t CO₂e" };
}

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

// ---------------------------------------------------------------------
// GEOT project portfolio (per-tracker ownership closure)
// ---------------------------------------------------------------------

type GeotTrackerCounts = [number, number, number]; // [live, operating, ≥50% controlled]

interface GeotProjects {
  total: GeotTrackerCounts;
  statuses: Record<string, number>;
  trackers: Record<string, GeotTrackerCounts>;
  meta: { release?: string; source?: string; control_threshold_pct?: number };
}

const TRACKER_LABELS: Record<string, string> = {
  coal_plant: "Coal plants",
  gas_plant: "Oil & gas plants",
  bioenergy: "Bioenergy plants",
  coal_mine: "Coal mines",
  iron_mine: "Iron ore mines",
  gas_pipeline: "Gas pipelines",
  oil_ngl_pipeline: "Oil & NGL pipelines",
  steel_plant: "Steel plants",
  cement: "Cement & concrete",
};

function TrackerTable({ projects }: { projects: GeotProjects }) {
  const rows = Object.entries(projects.trackers).sort((a, b) => b[1][0] - a[1][0]);
  if (rows.length === 0) return null;
  const maxLive = Math.max(...rows.map(([, v]) => v[0]));
  return (
    <div className="mt-4">
      <div className="grid grid-cols-[1fr_auto_auto_auto] gap-x-4 gap-y-1 items-center">
        <span />
        <span className="text-[10px] font-semibold uppercase tracking-oo-eyebrow text-emerald-700/50 text-right">
          Live
        </span>
        <span className="text-[10px] font-semibold uppercase tracking-oo-eyebrow text-emerald-700/50 text-right">
          Operating
        </span>
        <span className="text-[10px] font-semibold uppercase tracking-oo-eyebrow text-emerald-700/50 text-right">
          ≥50%
        </span>
        {rows.map(([tracker, [live, operating, controlled]]) => (
          <TrackerRow
            key={tracker}
            label={TRACKER_LABELS[tracker] ?? tracker.replace(/_/g, " ")}
            live={live}
            operating={operating}
            controlled={controlled}
            maxLive={maxLive}
          />
        ))}
      </div>
    </div>
  );
}

function TrackerRow({
  label,
  live,
  operating,
  controlled,
  maxLive,
}: {
  label: string;
  live: number;
  operating: number;
  controlled: number;
  maxLive: number;
}) {
  const pct = maxLive > 0 ? (live / maxLive) * 100 : 0;
  return (
    <>
      <div className="min-w-0">
        <div className="text-[11px] text-emerald-900/70 font-medium truncate">{label}</div>
        <div className="h-1 rounded-full bg-emerald-100 overflow-hidden mt-0.5">
          <div
            className="h-full rounded-full bg-emerald-400"
            style={{ width: `${pct}%` }}
          />
        </div>
      </div>
      <span className="text-[12px] font-mono tabular-nums text-emerald-900 text-right">
        {live.toLocaleString()}
      </span>
      <span className="text-[12px] font-mono tabular-nums text-emerald-800/70 text-right">
        {operating.toLocaleString()}
      </span>
      <span
        className={`text-[12px] font-mono tabular-nums text-right ${
          controlled > 0 ? "text-emerald-900 font-semibold" : "text-emerald-800/40"
        }`}
      >
        {controlled.toLocaleString()}
      </span>
    </>
  );
}

function statusFootnote(statuses: Record<string, number>): string | null {
  const parts: string[] = [];
  for (const key of ["mothballed", "retired", "cancelled"] as const) {
    const n = statuses[key] ?? 0;
    if (n > 0) parts.push(`${n.toLocaleString()} ${key}`);
  }
  return parts.length > 0 ? `plus ${parts.join(" · ")}` : null;
}

// ---------------------------------------------------------------------
// ClimateTRACECard — card for a Climate TRACE / GEM hit
// ---------------------------------------------------------------------

function ClimateTRACECard({
  hit,
  preloadedStmtCount,
  preloadedBreakdown,
}: {
  hit: SourceHit;
  preloadedStmtCount?: number;
  preloadedBreakdown?: BodsBreakdown;
}) {
  const [showDiagram,    setShowDiagram]    = useState(false);
  const [showStatements, setShowStatements] = useState(false);
  const [showJson,       setShowJson]       = useState(false);
  const [detail,     setDetail]     = useState<DeepenResponse | null>(null);
  const [loading,    setLoading]    = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  const raw = hit.raw as Record<string, unknown>;
  const emissions = (raw.emissions ?? {}) as {
    total_co2e_tonnes?: number;
    unit?: string;
    year?: number;
    by_sector?: Record<string, number>;
  };
  const parents = (raw.parents ?? []) as {
    entity_id: string;
    name: string;
    share?: number | null;
  }[];
  const projects = (raw.projects ?? null) as GeotProjects | null;
  const ownership = (raw.ownership ?? null) as {
    group_asset_count?: number;
    subsidiary_count?: number;
  } | null;
  const totalCo2e = emissions.total_co2e_tonnes ?? 0;
  const bySector = emissions.by_sector ?? {};
  const year = emissions.year ?? 2024;
  const formatted = totalCo2e > 0 ? formatCo2e(totalCo2e) : null;
  const [liveProjects, operatingProjects, controlledProjects] = projects?.total ?? [0, 0, 0];
  const footnote = projects ? statusFootnote(projects.statuses) : null;

  const anyOpen = showDiagram || showStatements || showJson;
  const stmtCount = detail?.bods.length ?? preloadedStmtCount ?? 0;
  const hasKnownCount = detail !== null || preloadedStmtCount !== undefined;

  // Graph-flavoured subtitle for the Visualise strip (matches the other source
  // cards): use the loaded detail, else the streamed entity/relationship split,
  // else a descriptive label.
  const breakdown: BodsBreakdown | undefined = detail
    ? {
        entities: detail.bods.filter((s) => (s as Record<string, unknown>).recordType === "entity").length,
        relationships: detail.bods.filter((s) => (s as Record<string, unknown>).recordType === "relationship").length,
      }
    : preloadedBreakdown;
  const graphMeta = breakdown
    ? `${breakdown.entities} ${breakdown.entities === 1 ? "entity" : "entities"} · ${breakdown.relationships} ${breakdown.relationships === 1 ? "relationship" : "relationships"}`
    : "Interactive ownership & control graph";
  // A single (or zero) statement is not a graph — hide the strip once known.
  const showGraphStrip = !hasKnownCount || stmtCount > 1;

  useEffect(() => {
    if (!showGraphStrip && showDiagram) setShowDiagram(false);
  }, [showGraphStrip, showDiagram]);

  async function ensureFetched() {
    if (detail || loading) return;
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

  function toggleDiagram()    { ensureFetched(); setShowDiagram(v    => !v); }
  function toggleStatements() { ensureFetched(); setShowStatements(v => !v); }
  function toggleJson()       { ensureFetched(); setShowJson(v       => !v); }

  return (
    <div className="rounded-oo border border-emerald-200 bg-emerald-50/40 overflow-hidden">
      <div className="px-5 pt-4 pb-3 border-b border-emerald-200/60">
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

        {/* Two-stat header: emissions | project portfolio */}
        {(formatted || liveProjects > 0) && (
          <div className="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-4">
            {formatted ? (
              <div>
                <div className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-700/50 mb-1">
                  Emissions ·{" "}
                  <a
                    href="https://climatetrace.org/"
                    target="_blank"
                    rel="noreferrer"
                    className="underline underline-offset-2 hover:text-emerald-900"
                  >
                    Climate TRACE
                  </a>
                </div>
                <div className="flex items-end gap-3">
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
              </div>
            ) : (
              <div className="hidden sm:block" />
            )}

            {liveProjects > 0 && (
              <div>
                <div className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-700/50 mb-1">
                  Projects ·{" "}
                  <a
                    href="https://globalenergymonitor.org/projects/global-energy-ownership-tracker"
                    target="_blank"
                    rel="noreferrer"
                    className="underline underline-offset-2 hover:text-emerald-900"
                  >
                    GEM Ownership Tracker
                  </a>
                </div>
                <div className="flex items-end gap-3">
                  <span className="font-head font-bold leading-none text-[2.6rem] text-emerald-800 tabular-nums">
                    {liveProjects.toLocaleString()}
                  </span>
                  <div className="pb-1">
                    <div className="text-[13px] font-semibold text-emerald-700">
                      live project{liveProjects === 1 ? "" : "s"}
                    </div>
                    <div className="text-[11px] text-emerald-600/70">
                      {operatingProjects.toLocaleString()} operating ·{" "}
                      {controlledProjects.toLocaleString()} ≥50% controlled
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {!formatted && liveProjects === 0 && !hit.is_stub && (
          <p className="mt-3 text-[13px] text-emerald-700/60 italic">
            Emissions and project data not available for this entity.
          </p>
        )}

        {projects && Object.keys(projects.trackers).length > 0 && (
          <>
            <TrackerTable projects={projects} />
            <div className="mt-1.5 flex items-baseline justify-between gap-2 flex-wrap">
              {footnote ? (
                <span className="text-[11px] text-emerald-700/50">{footnote}</span>
              ) : <span />}
              {projects.meta?.release && (
                <span className="text-[10px] text-emerald-700/40">
                  GEOT {projects.meta.release} · ≥50% effective share = controlled
                </span>
              )}
            </div>
          </>
        )}

        {ownership &&
          ((ownership.subsidiary_count ?? 0) > 0 ||
            (ownership.group_asset_count ?? 0) > 0) && (
          <div className="mt-2 text-[11px] text-emerald-800/60">
            Group reach: {(ownership.subsidiary_count ?? 0).toLocaleString()}{" "}
            subsidiaries · {(ownership.group_asset_count ?? 0).toLocaleString()}{" "}
            Climate TRACE assets group-wide
          </div>
        )}

        {Object.keys(bySector).length > 0 && (
          <SectorBars bySector={bySector} totalCo2e={totalCo2e} />
        )}

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
                {typeof p.share === "number" && (
                  <span className="text-emerald-700/60"> · {p.share}%</span>
                )}
              </span>
            ))}
          </div>
        )}

        {/* Visualise — primary invitation strip (emerald, to match the ESG card),
            aligned with the "Explore the ownership graph" CTA on the other source
            cards. Hidden when the source returns ≤ 1 statement. */}
        {showGraphStrip && (
        <button
          type="button"
          onClick={toggleDiagram}
          aria-pressed={showDiagram}
          className="mt-3 w-full flex items-center gap-3 rounded-oo border border-emerald-300 bg-emerald-100 px-3 py-2 text-left transition-colors hover:bg-emerald-200/70"
        >
          <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-emerald-600 text-white">
            <svg width="14" height="14" viewBox="0 0 12 12" fill="none" aria-hidden="true">
              <circle cx="6" cy="2.5" r="1.8" stroke="currentColor" strokeWidth="1.2"/>
              <circle cx="2" cy="9.5" r="1.8" stroke="currentColor" strokeWidth="1.2"/>
              <circle cx="10" cy="9.5" r="1.8" stroke="currentColor" strokeWidth="1.2"/>
              <line x1="6" y1="4.3" x2="2.8" y2="7.7" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
              <line x1="6" y1="4.3" x2="9.2" y2="7.7" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
            </svg>
          </span>
          <span className="min-w-0 flex-1">
            <span className="block text-[13px] font-semibold text-emerald-900 leading-tight">
              {showDiagram ? "Hide ownership graph" : "Explore the ownership graph"}
            </span>
            <span className="block text-[11px] font-mono text-emerald-700/80 truncate">
              {graphMeta}
            </span>
          </span>
          <svg width="14" height="14" viewBox="0 0 12 12" fill="none" aria-hidden="true"
            className={`shrink-0 text-emerald-700 transition-transform ${showDiagram ? "rotate-90" : ""}`}>
            <path d="M4.5 2.5 L8 6 L4.5 9.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>
        )}

        {/* Secondary drill-downs — quieter than the graph CTA. */}
        <div className={`flex flex-wrap gap-4 text-[11px] font-mono ${showGraphStrip ? "mt-2" : "mt-3"}`}>
          <button type="button" onClick={toggleStatements} aria-pressed={showStatements}
            className={`hover:underline ${showStatements ? "text-emerald-800" : "text-emerald-700/70 hover:text-emerald-900"}`}>
            {showStatements ? "Hide statements" : (
              hasKnownCount ? `${stmtCount} statement${stmtCount === 1 ? "" : "s"}` : "Statements"
            )}
          </button>
          <button type="button" onClick={toggleJson} aria-pressed={showJson}
            className={`hover:underline ${showJson ? "text-emerald-800" : "text-emerald-700/70 hover:text-emerald-900"}`}>
            {showJson ? "Hide JSON" : "Raw JSON"}
          </button>
        </div>
      </div>

      {anyOpen && (
        <div className="px-5 py-4 bg-white/60 text-[12px]">
          {loading && <p className="text-emerald-700">Fetching…</p>}
          {fetchError && <p className="text-red-700">{fetchError}</p>}
          {detail && (
            <DeepenBlock
              detail={detail}
              showDiagram={showDiagram}
              showStatements={showStatements}
              showJson={showJson}
            />
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------
// EsgPanel — collapsible ESG section
// ---------------------------------------------------------------------

export function EsgPanel({
  buckets,
  pendingCount = 0,
  bodsCountMap = {},
  bodsBreakdownMap = {},
}: {
  buckets: SourceBucket[];
  pendingCount?: number;
  bodsCountMap?: Record<string, number>;
  bodsBreakdownMap?: Record<string, BodsBreakdown>;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const hitCount = buckets.reduce((n, b) => n + b.hits.length, 0);

  return (
    <section className="mb-8">
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

      <div className="rounded-oo border border-emerald-200 bg-white overflow-hidden">
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

            {buckets.map((bucket) =>
              bucket.hits.map((hit) => (
                <ClimateTRACECard
                  key={`${hit.source_id}:${hit.hit_id}`}
                  hit={hit}
                  preloadedStmtCount={bodsCountMap[`${hit.source_id}:${hit.hit_id}`]}
                  preloadedBreakdown={bodsBreakdownMap[`${hit.source_id}:${hit.hit_id}`]}
                />
              ))
            )}

            {Array.from({ length: pendingCount }).map((_, i) => (
              <SkeletonSourceCard key={`esg-pending-${i}`} />
            ))}

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
