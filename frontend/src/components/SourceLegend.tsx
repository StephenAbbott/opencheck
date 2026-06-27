/**
 * SourceLegend — "which sources built this network" provenance strip (FullCheck).
 *
 * The reconciled network merges every source's view of a company onto one node,
 * so a node many sources agree on reads as corroborated. This legend names the
 * sources contributing to the graph and turns each into a highlight toggle:
 * click one to spotlight the nodes/edges it asserts and dim the rest (highlight,
 * never hide — the network context stays). The count on each chip is how many
 * nodes that source asserts.
 */

export interface SourceCount {
  source: string;
  count: number;
}

export function SourceLegend({
  sources,
  active,
  corroboratedCount,
  onToggle,
}: {
  sources: SourceCount[];
  active: string | null;
  corroboratedCount: number;
  onToggle: (source: string) => void;
}) {
  return (
    <div className="mb-2 rounded-oo border border-oo-rule bg-oo-bg px-3 py-2">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[11px] font-semibold uppercase tracking-oo-eyebrow text-oo-blue">
          Sources in this network
        </span>
        {corroboratedCount > 0 && (
          <span className="text-[11px] text-oo-muted">
            {corroboratedCount} {corroboratedCount === 1 ? "node" : "nodes"} corroborated by ≥2 sources
          </span>
        )}
      </div>
      <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
        {sources.map(({ source, count }) => {
          const on = active === source;
          return (
            <button
              key={source}
              type="button"
              aria-pressed={on}
              onClick={() => onToggle(source)}
              title={`Highlight everything ${source} asserts`}
              className={`flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] transition-colors ${
                on
                  ? "border-[#1565c0] bg-[#e8f0fb] text-[#1565c0] font-semibold"
                  : "border-oo-rule bg-white text-oo-ink hover:border-[#1565c0] hover:text-[#1565c0]"
              }`}
            >
              <span className="truncate max-w-[16rem]">{source}</span>
              <span className={on ? "text-[#1565c0]" : "text-oo-muted"}>{count}</span>
            </button>
          );
        })}
        {active && (
          <button
            type="button"
            onClick={() => onToggle(active)}
            className="text-[11px] font-mono text-oo-blue hover:underline ml-1"
          >
            clear
          </button>
        )}
      </div>
    </div>
  );
}
