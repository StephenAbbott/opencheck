import { useState } from "react";
import { deepen } from "../../lib/api";
import type { DeepenResponse, SourceHit } from "../../lib/api";
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
// ClimateTRACECard — card for a Climate TRACE / GEM hit
// ---------------------------------------------------------------------

function ClimateTRACECard({ hit }: { hit: SourceHit }) {
  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState<DeepenResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

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
              </span>
            ))}
          </div>
        )}
      </div>

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

// ---------------------------------------------------------------------
// EsgPanel — collapsible ESG section
// ---------------------------------------------------------------------

export function EsgPanel({
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
