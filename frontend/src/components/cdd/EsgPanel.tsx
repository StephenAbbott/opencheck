import { useEffect, useId, useState } from "react";
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
              <span className="text-[11px] text-emerald-900 capitalize font-medium">
                {sector.replace(/-/g, " ")}
              </span>
              <span className="text-[11px] font-mono text-emerald-900">
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
      <table className="w-full border-collapse">
        <caption className="sr-only">Asset tracker counts by status</caption>
        <thead>
          <tr>
            <th scope="col"><span className="sr-only">Tracker</span></th>
            <th scope="col" className="pl-4 text-[10px] font-semibold uppercase tracking-oo-eyebrow text-emerald-800 text-right">
              Live
            </th>
            <th scope="col" className="pl-4 text-[10px] font-semibold uppercase tracking-oo-eyebrow text-emerald-800 text-right">
              Operating
            </th>
            <th scope="col" className="pl-4 text-[10px] font-semibold uppercase tracking-oo-eyebrow text-emerald-800 text-right">
              ≥50%
            </th>
          </tr>
        </thead>
        <tbody>
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
        </tbody>
      </table>
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
    <tr>
      <th scope="row" className="w-full max-w-0 pt-1 align-middle text-left font-normal">
        <div className="min-w-0">
          <div className="text-[11px] text-emerald-900 font-medium truncate">{label}</div>
          <div className="h-1 rounded-full bg-emerald-100 overflow-hidden mt-0.5">
            <div
              className="h-full rounded-full bg-emerald-400"
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>
      </th>
      <td className="pl-4 pt-1 align-middle text-[12px] font-mono tabular-nums text-emerald-900 text-right">
        {live.toLocaleString()}
      </td>
      <td className="pl-4 pt-1 align-middle text-[12px] font-mono tabular-nums text-emerald-800 text-right">
        {operating.toLocaleString()}
      </td>
      <td
        className={`pl-4 pt-1 align-middle text-[12px] font-mono tabular-nums text-right ${
          controlled > 0 ? "text-emerald-900 font-semibold" : "text-emerald-800"
        }`}
      >
        {controlled.toLocaleString()}
      </td>
    </tr>
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
// EitiCard — payments-to-governments card for an EITI hit
// ---------------------------------------------------------------------

interface EitiRevenueYear {
  year: string | null;
  organisation_id: string;
  total_usd: number;
  rows: { label: string | null; revenue: number | null; currency: string | null; gfs_label: string | null }[];
}

interface EitiBundle {
  country: string;
  identification: string;
  entity_name: string | null;
  organisations: { id: string; year: string | null; label: string | null }[];
  revenue_years: EitiRevenueYear[];
  streams: Record<string, number>;
  total_usd: number;
  years: string[];
}

function formatUsd(v: number): string {
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `$${Math.round(v / 1_000).toLocaleString()}k`;
  return `$${Math.round(v).toLocaleString()}`;
}

function EitiCard({ hit }: { hit: SourceHit }) {
  const raw = hit.raw as unknown as EitiBundle;
  const streams = Object.entries(raw.streams ?? {}).slice(0, 6);
  const maxStream = streams.length ? Math.max(...streams.map(([, v]) => v)) : 0;
  const years = raw.years ?? [];

  return (
    <div className="rounded-oo border border-emerald-200 bg-emerald-50/40 overflow-hidden">
      <div className="px-5 pt-4 pb-4 border-b border-emerald-200/60">
        <SourceTag sourceId="eiti" />
        <h3 className="font-head font-bold text-[15px] text-emerald-950 leading-snug">
          {hit.name}
        </h3>
        <div className="text-[11px] font-mono text-emerald-800 mt-0.5">
          {raw.country} · national ID {raw.identification}
        </div>

        <div className="mt-4">
          <div className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mb-1">
            Payments to governments ·{" "}
            <a
              href="https://eiti.org/open-data"
              target="_blank"
              rel="noreferrer"
              className="underline underline-offset-2 hover:text-emerald-900"
            >
              EITI
              <span className="sr-only"> (opens in new tab)</span>
            </a>
          </div>
          <div className="flex items-end gap-3">
            <span className="font-head font-bold leading-none text-[2.6rem] text-emerald-800 tabular-nums">
              {raw.total_usd > 0 ? formatUsd(raw.total_usd) : years.length.toLocaleString()}
            </span>
            <div className="pb-1">
              <div className="text-[13px] font-semibold text-emerald-700">
                {raw.total_usd > 0 ? "USD disclosed" : `reporting year${years.length === 1 ? "" : "s"}`}
              </div>
              <div className="text-[11px] text-emerald-700">
                {years.length > 0 &&
                  (years.length > 1
                    ? `${years[years.length - 1]}–${years[0]} · ${years.length} reporting years`
                    : `reported ${years[0]}`)}
              </div>
            </div>
          </div>
        </div>

        {streams.length > 0 && (
          <div className="mt-4 space-y-2">
            {streams.map(([label, value]) => (
              <div key={label}>
                <div className="flex items-baseline justify-between mb-1">
                  <span className="text-[11px] text-emerald-900 font-medium">{label}</span>
                  <span className="text-[11px] font-mono text-emerald-900">{formatUsd(value)}</span>
                </div>
                <div className="h-1.5 rounded-full bg-emerald-100 overflow-hidden">
                  <div
                    className="h-full rounded-full bg-emerald-500"
                    style={{ width: `${maxStream > 0 ? (value / maxStream) * 100 : 0}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        )}

        <p className="mt-3 text-[10px] text-emerald-800">
          GFS-classified fiscal disclosures under the EITI Standard · EITI
          International Secretariat, eiti.org
        </p>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// WikirateCard — community-researched ESG metric answers
// ---------------------------------------------------------------------

interface WikirateAnswer {
  metric_designer: string | null;
  metric_name: string | null;
  year: number | null;
  value: unknown;
  answer_url: string | null;
}

interface WikirateBundle {
  card_id: number;
  name: string;
  wikirate_url: string;
  matched_by: string;
  identifiers: Record<string, unknown>;
  total_answers: number;
  latest_answers: WikirateAnswer[];
}

function formatAnswerValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") {
    return Math.abs(value) >= 1000 ? value.toLocaleString() : String(value);
  }
  if (Array.isArray(value)) return value.slice(0, 3).join(", ");
  const text = String(value);
  return text.length > 60 ? `${text.slice(0, 57)}…` : text;
}

function WikirateCard({ hit }: { hit: SourceHit }) {
  const raw = hit.raw as unknown as WikirateBundle;
  const answers = (raw.latest_answers ?? []).slice(0, 6);
  const total = raw.total_answers ?? 0;

  return (
    <div className="rounded-oo border border-emerald-200 bg-emerald-50/40 overflow-hidden">
      <div className="px-5 pt-4 pb-4">
        <SourceTag sourceId="wikirate" />
        <h3 className="font-head font-bold text-[15px] text-emerald-950 leading-snug">
          {hit.name}
        </h3>
        <div className="text-[11px] font-mono text-emerald-800 mt-0.5">
          {raw.matched_by === "wikidata_qid" ? "Wikidata match" : "LEI match"} ·
          card ~{raw.card_id}
        </div>

        <div className="mt-4">
          <div className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mb-1">
            ESG data points ·{" "}
            <a
              href={raw.wikirate_url}
              target="_blank"
              rel="noreferrer"
              className="underline underline-offset-2 hover:text-emerald-900"
            >
              Wikirate
              <span className="sr-only"> (opens in new tab)</span>
            </a>
          </div>
          <div className="flex items-end gap-3">
            <span className="font-head font-bold leading-none text-[2.6rem] text-emerald-800 tabular-nums">
              {total.toLocaleString()}
            </span>
            <div className="pb-1">
              <div className="text-[13px] font-semibold text-emerald-700">
                metric answers
              </div>
              <div className="text-[11px] text-emerald-700">
                community-researched · latest value per metric below
              </div>
            </div>
          </div>
        </div>

        {answers.length > 0 && (
          <div className="mt-4 space-y-1.5">
            {answers.map((a, i) => (
              <div
                key={`${a.metric_name}-${i}`}
                className="flex items-baseline justify-between gap-3"
              >
                <span className="text-[11px] text-emerald-900 font-medium min-w-0 truncate">
                  {a.answer_url ? (
                    <a
                      href={a.answer_url}
                      target="_blank"
                      rel="noreferrer"
                      className="underline decoration-dotted underline-offset-2"
                    >
                      {a.metric_name ?? "Metric"}
                      <span className="sr-only"> (opens in new tab)</span>
                    </a>
                  ) : (
                    a.metric_name ?? "Metric"
                  )}
                  {a.metric_designer && (
                    <span className="text-emerald-800"> · {a.metric_designer}</span>
                  )}
                </span>
                <span className="text-[11px] font-mono text-emerald-900 shrink-0">
                  {a.year && <span className="text-emerald-800">{a.year} · </span>}
                  {formatAnswerValue(a.value)}
                </span>
              </div>
            ))}
          </div>
        )}

        <a
          href={raw.wikirate_url}
          target="_blank"
          rel="noreferrer"
          className="mt-3 inline-block text-[12px] font-semibold text-emerald-800 underline underline-offset-2 hover:text-emerald-950"
        >
          View all {total.toLocaleString()} data points on wikirate.org →
          <span className="sr-only"> (opens in new tab)</span>
        </a>

        <p className="mt-3 text-[10px] text-emerald-800">
          Open ESG metric answers researched by the Wikirate community ·
          Wikirate.org, CC BY 4.0
        </p>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// Source attribution — every ESG card and tile names its data origin
// ---------------------------------------------------------------------

const ESG_SOURCE_META: Record<
  string,
  { org: string; href: string; licence: string }
> = {
  climatetrace: {
    org: "Global Energy Monitor · Climate TRACE",
    href: "https://climatetrace.org/",
    licence: "CC BY 4.0",
  },
  eiti: {
    org: "EITI International Secretariat",
    href: "https://eiti.org/",
    licence: "open data, attribution",
  },
  wikirate: {
    org: "Wikirate",
    href: "https://wikirate.org/",
    licence: "CC BY 4.0",
  },
};

function SourceTag({ sourceId }: { sourceId: string }) {
  const meta = ESG_SOURCE_META[sourceId];
  if (!meta) return null;
  return (
    <div className="mb-2 -mt-0.5 flex items-center gap-1.5">
      <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-500" />
      <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-800">
        Data from{" "}
        <a
          href={meta.href}
          target="_blank"
          rel="noreferrer"
          className="underline underline-offset-2 hover:text-emerald-900"
        >
          {meta.org}
          <span className="sr-only"> (opens in new tab)</span>
        </a>{" "}
        · {meta.licence}
      </span>
    </div>
  );
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
  const panelId = useId();

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
        <SourceTag sourceId="climatetrace" />
        <h3 className="font-head font-bold text-[15px] text-emerald-950 leading-snug">
          {hit.name}
          {hit.is_stub && (
            <span className="ml-2 text-[11px] font-mono bg-amber-50 text-amber-800 border border-amber-200 rounded px-1.5 py-0.5">
              stub
            </span>
          )}
        </h3>
        <div className="text-[11px] font-mono text-emerald-800 mt-0.5">
          GEM entity {hit.identifiers.gem_entity_id}
        </div>

        {/* Two-stat header: emissions | project portfolio */}
        {(formatted || liveProjects > 0) && (
          <div className="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-4">
            {formatted ? (
              <div>
                <div className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mb-1">
                  Emissions ·{" "}
                  <a
                    href="https://climatetrace.org/"
                    target="_blank"
                    rel="noreferrer"
                    className="underline underline-offset-2 hover:text-emerald-900"
                  >
                    Climate TRACE
                    <span className="sr-only"> (opens in new tab)</span>
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
                    <div className="text-[11px] text-emerald-700">
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
                <div className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mb-1">
                  Projects ·{" "}
                  <a
                    href="https://globalenergymonitor.org/projects/global-energy-ownership-tracker"
                    target="_blank"
                    rel="noreferrer"
                    className="underline underline-offset-2 hover:text-emerald-900"
                  >
                    GEM Ownership Tracker
                    <span className="sr-only"> (opens in new tab)</span>
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
                    <div className="text-[11px] text-emerald-700">
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
          <p className="mt-3 text-[13px] text-emerald-800 italic">
            Emissions and project data not available for this entity.
          </p>
        )}

        {projects && Object.keys(projects.trackers).length > 0 && (
          <>
            <TrackerTable projects={projects} />
            <div className="mt-1.5 flex items-baseline justify-between gap-2 flex-wrap">
              {footnote ? (
                <span className="text-[11px] text-emerald-800">{footnote}</span>
              ) : <span />}
              {projects.meta?.release && (
                <span className="text-[10px] text-emerald-800">
                  GEOT {projects.meta.release} · ≥50% effective share = controlled
                </span>
              )}
            </div>
          </>
        )}

        {ownership &&
          ((ownership.subsidiary_count ?? 0) > 0 ||
            (ownership.group_asset_count ?? 0) > 0) && (
          <div className="mt-2 text-[11px] text-emerald-800">
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
            <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mr-2">
              GEM parent{parents.length === 1 ? "" : "s"}
            </span>
            {parents.map((p) => (
              <span
                key={p.entity_id}
                className="inline-block text-[11px] font-mono text-emerald-900 bg-emerald-100 border border-emerald-200 rounded px-1.5 py-0.5 mr-1"
              >
                {p.name}
                {typeof p.share === "number" && (
                  <span className="text-emerald-800"> · {p.share}%</span>
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
          aria-expanded={showDiagram}
          aria-controls={panelId}
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
            <span className="block text-[11px] font-mono text-emerald-700 truncate">
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
          <button type="button" onClick={toggleStatements} aria-expanded={showStatements} aria-controls={panelId}
            className={`hover:underline ${showStatements ? "text-emerald-800" : "text-emerald-800 hover:text-emerald-900"}`}>
            {showStatements ? "Hide statements" : (
              hasKnownCount ? `${stmtCount} statement${stmtCount === 1 ? "" : "s"}` : "Statements"
            )}
          </button>
          <button type="button" onClick={toggleJson} aria-expanded={showJson} aria-controls={panelId}
            className={`hover:underline ${showJson ? "text-emerald-800" : "text-emerald-800 hover:text-emerald-900"}`}>
            {showJson ? "Hide JSON" : "Raw JSON"}
          </button>
        </div>
      </div>

      {anyOpen && (
        <div id={panelId} className="px-5 py-4 bg-white/60 text-[12px]">
          {loading && <p className="text-emerald-700" role="status">Fetching…</p>}
          {fetchError && <p className="text-red-700" role="alert">{fetchError}</p>}
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
// Summary tiles — one per ESG hit; the full card expands on click
// ---------------------------------------------------------------------

function tileStats(hit: SourceHit): { stat: string; unit: string; sub: string } {
  if (hit.source_id === "eiti") {
    const raw = hit.raw as unknown as EitiBundle;
    const years = raw.years ?? [];
    const span =
      years.length > 1
        ? `${years[years.length - 1]}–${years[0]}`
        : years[0] ?? "";
    if ((raw.total_usd ?? 0) > 0) {
      return {
        stat: formatUsd(raw.total_usd),
        unit: "to governments",
        sub: span ? `USD disclosed · ${span}` : "USD disclosed",
      };
    }
    return {
      stat: years.length.toLocaleString(),
      unit: `reporting year${years.length === 1 ? "" : "s"}`,
      sub: span,
    };
  }
  if (hit.source_id === "wikirate") {
    const raw = hit.raw as unknown as WikirateBundle;
    return {
      stat: (raw.total_answers ?? 0).toLocaleString(),
      unit: "ESG data points",
      sub: "community-researched metrics",
    };
  }
  // climatetrace / GEM
  const raw = hit.raw as Record<string, unknown>;
  const emissions = (raw.emissions ?? {}) as { total_co2e_tonnes?: number; year?: number };
  const projects = (raw.projects ?? null) as GeotProjects | null;
  const totalCo2e = emissions.total_co2e_tonnes ?? 0;
  const [liveProjects] = projects?.total ?? [0];
  if (totalCo2e > 0) {
    const fmt = formatCo2e(totalCo2e);
    return {
      stat: fmt.value,
      unit: fmt.unit,
      sub:
        liveProjects > 0
          ? `${emissions.year ?? 2024} · ${liveProjects.toLocaleString()} live projects`
          : `${emissions.year ?? 2024} · direct assets`,
    };
  }
  if (liveProjects > 0) {
    return {
      stat: liveProjects.toLocaleString(),
      unit: `live project${liveProjects === 1 ? "" : "s"}`,
      sub: "GEM Ownership Tracker",
    };
  }
  return { stat: "—", unit: "", sub: "no emissions or project data" };
}

function EsgTile({
  hit,
  expanded,
  onToggle,
}: {
  hit: SourceHit;
  expanded: boolean;
  onToggle: () => void;
}) {
  const meta = ESG_SOURCE_META[hit.source_id];
  const { stat, unit, sub } = tileStats(hit);
  return (
    <button
      type="button"
      onClick={onToggle}
      aria-expanded={expanded}
      className={`text-left rounded-oo border px-4 py-3 transition-colors ${
        expanded
          ? "border-emerald-400 bg-emerald-100/70"
          : "border-emerald-200 bg-emerald-50/40 hover:bg-emerald-100/50"
      }`}
    >
      <div className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-emerald-800 mb-1.5">
        {meta?.org ?? hit.source_id}
      </div>
      <div className="flex items-baseline gap-2">
        <span className="font-head font-bold leading-none text-[1.7rem] text-emerald-800 tabular-nums">
          {stat}
        </span>
        <span className="text-[12px] font-semibold text-emerald-700">{unit}</span>
      </div>
      <div className="mt-1 flex items-center justify-between gap-2">
        <span className="text-[11px] text-emerald-700 truncate">{sub}</span>
        <span className="flex items-center gap-1 shrink-0 text-[11px] font-mono text-emerald-700">
          {expanded ? "Hide" : "Detail"}
          <svg
            width="11" height="11" viewBox="0 0 12 12" fill="none" aria-hidden="true"
            className={`transition-transform ${expanded ? "rotate-90" : ""}`}
          >
            <path d="M4.5 2.5 L8 6 L4.5 9.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </span>
      </div>
    </button>
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
  const bodyId = useId();
  // Per-hit expansion for the summary tiles. Unset entries fall back to the
  // default rule: a lone ESG hit shows its full card without an extra click;
  // two or more start as tiles only.
  const [expandedMap, setExpandedMap] = useState<Record<string, boolean>>({});
  const allHits = buckets.flatMap((b) => b.hits);
  const hitCount = allHits.length;
  const isExpanded = (key: string) => expandedMap[key] ?? hitCount === 1;
  const toggle = (key: string) =>
    setExpandedMap((m) => ({ ...m, [key]: !isExpanded(key) }));

  return (
    <section className="mb-8">
      <div className="flex items-center gap-3 mb-4">
        <div className="flex-1 h-px bg-emerald-200" />
        <div className="flex items-center gap-2 text-emerald-700">
          <LeafIcon className="w-4 h-4" />
          <h2 className="text-[10px] font-semibold tracking-oo-eyebrow uppercase">
            Environmental, Social, and Governance (ESG) Data
          </h2>
        </div>
        <div className="flex-1 h-px bg-emerald-200" />
      </div>

      <div className="rounded-oo border border-emerald-200 bg-white overflow-hidden">
        <button
          type="button"
          onClick={() => setCollapsed((c) => !c)}
          aria-expanded={!collapsed}
          aria-controls={bodyId}
          className="w-full flex items-center justify-between px-5 py-3 border-b border-emerald-200 bg-emerald-50/60 hover:bg-emerald-50 transition-colors text-left"
        >
          <div className="flex items-center gap-2.5">
            <LeafIcon className="w-4 h-4 text-emerald-600 shrink-0" />
            <div>
              <span className="font-head font-bold text-[14px] text-emerald-950">
                Environmental, Social, and Governance (ESG) Data
              </span>
              <span className="ml-2 text-[11px] font-mono text-emerald-700">
                {hitCount} result{hitCount === 1 ? "" : "s"} · {buckets.length} source{buckets.length === 1 ? "" : "s"}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            <span className="text-[11px] text-emerald-700 hidden sm:inline">
              ESG / climate risk · not a KYC source
            </span>
            <span className="text-[12px] font-mono text-emerald-700">
              {collapsed ? "Show ↓" : "Hide ↑"}
            </span>
          </div>
        </button>

        {!collapsed && (
          <div id={bodyId} className="p-5 space-y-4">
            <p className="text-[12px] leading-[1.65] text-emerald-800 bg-emerald-50 border border-emerald-200 rounded px-3 py-2">
              <span className="font-semibold">ESG context only.</span> Data
              from{" "}
              <a
                href="https://globalenergymonitor.org/"
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2 hover:text-emerald-900"
              >
                Global Energy Monitor
                <span className="sr-only"> (opens in new tab)</span>
              </a>{" "}
              (CC BY 4.0),{" "}
              <a
                href="https://climatetrace.org/"
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2 hover:text-emerald-900"
              >
                Climate TRACE
                <span className="sr-only"> (opens in new tab)</span>
              </a>{" "}
              (CC BY 4.0), the{" "}
              <a
                href="https://eiti.org/"
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2 hover:text-emerald-900"
              >
                EITI
                <span className="sr-only"> (opens in new tab)</span>
              </a>{" "}
              (open data, attribution) and{" "}
              <a
                href="https://wikirate.org/"
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2 hover:text-emerald-900"
              >
                Wikirate
                <span className="sr-only"> (opens in new tab)</span>
              </a>{" "}
              (CC BY 4.0). Emissions are satellite-derived estimates for
              directly owned assets; payments to governments are company
              disclosures under the EITI Standard; Wikirate metrics are
              community-researched — not a beneficial ownership or
              sanctions check.
            </p>

            {/* Summary tiles — one per hit, each naming its data origin.
                Clicking a tile expands the full card below the grid. */}
            {allHits.length > 0 && (
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {allHits.map((hit) => {
                  const key = `${hit.source_id}:${hit.hit_id}`;
                  return (
                    <EsgTile
                      key={key}
                      hit={hit}
                      expanded={isExpanded(key)}
                      onToggle={() => toggle(key)}
                    />
                  );
                })}
              </div>
            )}

            {allHits
              .filter((hit) => isExpanded(`${hit.source_id}:${hit.hit_id}`))
              .map((hit) =>
                hit.source_id === "eiti" ? (
                  <EitiCard key={`${hit.source_id}:${hit.hit_id}`} hit={hit} />
                ) : hit.source_id === "wikirate" ? (
                  <WikirateCard key={`${hit.source_id}:${hit.hit_id}`} hit={hit} />
                ) : (
                  <ClimateTRACECard
                    key={`${hit.source_id}:${hit.hit_id}`}
                    hit={hit}
                    preloadedStmtCount={bodsCountMap[`${hit.source_id}:${hit.hit_id}`]}
                    preloadedBreakdown={bodsBreakdownMap[`${hit.source_id}:${hit.hit_id}`]}
                  />
                )
              )}

            {Array.from({ length: pendingCount }).map((_, i) => (
              <SkeletonSourceCard key={`esg-pending-${i}`} />
            ))}

            {buckets
              .filter((b) => b.error && b.hits.length === 0)
              .map((b) => (
                <div
                  key={b.sourceId}
                  role="alert"
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
