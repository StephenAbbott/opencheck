import { lazy, Suspense, useState } from "react";
import { getSubsidiaries } from "../../lib/api";
import type { SubsidiariesResponse, SubsidiaryChild } from "../../lib/api";

// BodsGraphExplorer pulls in Cytoscape — load it only when a small network is
// actually rendered as a graph (large networks degrade to a table + export).
const BodsGraphExplorer = lazy(() => import("../BodsGraphExplorer"));

// ---------------------------------------------------------------------
// Relation badge — direct / ultimate / both
// ---------------------------------------------------------------------

function RelationBadge({ relation }: { relation: SubsidiaryChild["relation"] }) {
  const map = {
    direct: { label: "Direct", classes: "bg-blue-50 text-blue-700 border-blue-200" },
    ultimate: { label: "Ultimate", classes: "bg-sky-50 text-sky-700 border-sky-200" },
    both: { label: "Direct + ultimate", classes: "bg-indigo-50 text-indigo-700 border-indigo-200" },
  } as const;
  const m = map[relation];
  return (
    <span
      className={`text-[10px] font-semibold rounded px-1.5 py-0.5 border ${m.classes}`}
      title={
        relation === "both"
          ? "Both a direct and an ultimate child (GLEIF Level 2 accounting consolidation)"
          : relation === "ultimate"
            ? "Ultimate (indirect) child — consolidated by the group head"
            : "Direct child — directly consolidated"
      }
    >
      {m.label}
    </span>
  );
}

// ---------------------------------------------------------------------
// Children table — direct-first-then-tail ordering
// ---------------------------------------------------------------------

const RELATION_ORDER: Record<SubsidiaryChild["relation"], number> = {
  direct: 0,
  both: 1,
  ultimate: 2,
};

function orderChildren(children: SubsidiaryChild[]): SubsidiaryChild[] {
  return [...children].sort((a, b) => {
    const r = RELATION_ORDER[a.relation] - RELATION_ORDER[b.relation];
    if (r !== 0) return r;
    return (a.name || a.lei).localeCompare(b.name || b.lei);
  });
}

function ChildrenTable({ children }: { children: SubsidiaryChild[] }) {
  return (
    <ul className="mt-2 divide-y divide-oo-rule rounded-oo border border-oo-rule bg-white">
      {orderChildren(children).map((c) => (
        <li
          key={`${c.lei}-${c.relation}`}
          className="flex items-start justify-between gap-3 px-3 py-2"
        >
          <div className="min-w-0">
            <div className="text-[12px] text-oo-ink leading-snug">
              {c.link ? (
                <a href={c.link} target="_blank" rel="noopener noreferrer" className="hover:underline">
                  {c.name || c.lei}
                </a>
              ) : (
                c.name || c.lei
              )}
            </div>
            <div className="mt-0.5 font-mono text-[10px] text-oo-muted">
              {c.lei}
              {c.jurisdiction ? ` · ${c.jurisdiction}` : ""}
              {c.status ? ` · ${c.status}` : ""}
            </div>
          </div>
          <div className="shrink-0">
            <RelationBadge relation={c.relation} />
          </div>
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------
// Summary header — counts + jurisdiction spread
// ---------------------------------------------------------------------

function SummaryStats({ data }: { data: SubsidiariesResponse }) {
  return (
    <>
      <p className="mt-1 text-[12px] text-oo-ink leading-[1.6]">
        <strong>{data.direct_total}</strong> direct{" "}
        {data.direct_total === 1 ? "child" : "children"} ·{" "}
        <strong>{data.ultimate_total}</strong> ultimate{" "}
        {data.ultimate_total === 1 ? "child" : "children"}
        <span className="text-oo-muted">
          {" "}
          ({data.distinct_fetched} distinct{data.indirect_only > 0 ? `, ${data.indirect_only} indirect-only` : ""})
        </span>
      </p>
      {data.jurisdictions.length > 0 && (
        <div className="mt-1.5 flex flex-wrap items-center gap-1">
          {data.jurisdictions.slice(0, 12).map((j) => (
            <span
              key={j.code}
              className="text-[10px] font-mono rounded px-1.5 py-0.5 border border-oo-rule bg-oo-bg text-oo-muted"
            >
              {j.code} {j.count}
            </span>
          ))}
        </div>
      )}
      {data.truncated && (
        <p className="mt-1.5 text-[11px] text-oo-muted leading-[1.5]">
          Showing {data.distinct_fetched} of ~{data.node_estimate} entities — a sample of a large
          network (counts are exact from GLEIF; the child list is capped).
        </p>
      )}
    </>
  );
}

// ---------------------------------------------------------------------
// Invitation strip — shown before the lookup and when collapsed again
// ---------------------------------------------------------------------

function InvitationStrip({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="mt-3 w-full flex items-center gap-3 rounded-oo border border-[#c7cdf0] bg-[#eef1fb] px-3 py-2 text-left transition-colors hover:bg-[#e6eafb]"
    >
      <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-[#3d30d4] text-white">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden="true">
          <rect x="5" y="1.5" width="4" height="3" rx="0.6" stroke="currentColor" strokeWidth="1.1" />
          <rect x="1.5" y="9.5" width="4" height="3" rx="0.6" stroke="currentColor" strokeWidth="1.1" />
          <rect x="8.5" y="9.5" width="4" height="3" rx="0.6" stroke="currentColor" strokeWidth="1.1" />
          <path d="M7 4.5 V7 M7 7 H3.5 V9.5 M7 7 H10.5 V9.5" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" />
        </svg>
      </span>
      <span className="min-w-0 flex-1">
        <span className="block text-[13px] font-semibold text-[#2a2382] leading-tight">
          Reveal subsidiary network
        </span>
        <span className="block text-[11px] text-[#5b54a8]">
          GLEIF Level 2 direct &amp; ultimate children, mapped to BODS · live lookup
        </span>
      </span>
    </button>
  );
}

// ---------------------------------------------------------------------
// SubsidiaryNetwork — lazy GLEIF children reveal (graph or table + export)
// ---------------------------------------------------------------------

export function SubsidiaryNetwork({ lei, entityName }: { lei: string; entityName?: string }) {
  const [data, setData] = useState<SubsidiariesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState(false);

  // BODS statements for the graph / export — fetched on demand (format=bods).
  const [bods, setBods] = useState<Record<string, unknown>[] | null>(null);
  const [bodsLoading, setBodsLoading] = useState(false);
  const [showGraph, setShowGraph] = useState(false);

  async function run() {
    if (loading || data) return;
    setLoading(true);
    setError(null);
    try {
      setData(await getSubsidiaries(lei, "summary"));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  async function loadBods(): Promise<Record<string, unknown>[] | null> {
    if (bods) return bods;
    setBodsLoading(true);
    try {
      const full = await getSubsidiaries(lei, "bods");
      const stmts = full.bods ?? [];
      setBods(stmts);
      return stmts;
    } catch (e) {
      setError(String(e));
      return null;
    } finally {
      setBodsLoading(false);
    }
  }

  async function revealGraph() {
    const stmts = await loadBods();
    if (stmts) setShowGraph(true);
  }

  async function downloadBods() {
    const stmts = await loadBods();
    if (!stmts) return;
    const blob = new Blob([JSON.stringify(stmts, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `subsidiaries-${lei}.bods.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // Layer 1 — invitation (nothing fires until clicked).
  if (!data && !loading && !error) {
    return <InvitationStrip onClick={run} />;
  }

  // Collapsed after viewing — back to the invitation strip; re-opening keeps the
  // already-fetched data (no second lookup).
  if (data && collapsed) {
    return <InvitationStrip onClick={() => setCollapsed(false)} />;
  }

  const isGraphMode = data?.render_mode === "graph";

  return (
    <section className="mt-3 rounded-oo border border-oo-rule bg-oo-bg p-3">
      {loading && <p className="text-[12px] text-oo-muted">Fetching the GLEIF subsidiary network…</p>}
      {error && <p className="text-[12px] text-red-700">{error}</p>}

      {data && !data.available && (
        <div className="flex items-start justify-between gap-3">
          <p className="text-[12px] text-oo-muted leading-[1.6]">
            No subsidiary network published{data.reason ? ` (${data.reason})` : ""}.
          </p>
          <button
            type="button"
            onClick={() => setCollapsed(true)}
            className="shrink-0 text-[11px] font-mono text-oo-blue hover:underline"
          >
            Hide
          </button>
        </div>
      )}

      {data && data.available && (
        <>
          <div className="flex items-baseline justify-between gap-2">
            <h4 className="font-head font-bold text-[13px] text-oo-ink">Subsidiary network</h4>
            <button
              type="button"
              onClick={() => setCollapsed(true)}
              className="shrink-0 text-[11px] font-mono text-oo-blue hover:underline"
            >
              Hide
            </button>
          </div>

          <SummaryStats data={data} />

          {/* Action row — reveal graph (small networks) or export BODS. */}
          <div className="mt-2.5 flex flex-wrap items-center gap-2">
            {isGraphMode && !showGraph && (
              <button
                type="button"
                onClick={revealGraph}
                disabled={bodsLoading}
                className="rounded-oo border border-oo-rule bg-white px-2.5 py-1 text-[11px] font-semibold text-oo-ink transition-colors hover:bg-oo-bg disabled:opacity-50"
              >
                {bodsLoading ? "Building graph…" : "Show network graph"}
              </button>
            )}
            {isGraphMode && showGraph && (
              <button
                type="button"
                onClick={() => setShowGraph(false)}
                className="rounded-oo border border-oo-rule bg-white px-2.5 py-1 text-[11px] font-semibold text-oo-ink transition-colors hover:bg-oo-bg"
              >
                Hide graph
              </button>
            )}
            <button
              type="button"
              onClick={downloadBods}
              disabled={bodsLoading}
              className="rounded-oo border border-oo-rule bg-white px-2.5 py-1 text-[11px] font-semibold text-oo-ink transition-colors hover:bg-oo-bg disabled:opacity-50"
            >
              {bodsLoading ? "Preparing…" : "Download BODS"}
            </button>
          </div>

          {!isGraphMode && (
            <p className="mt-2 text-[11px] text-oo-muted leading-[1.5]">
              Large network ({data.node_estimate} entities) — shown as a table to stay readable.
              Direct children first, then the indirect (ultimate-only) tail. Use the BODS export to
              render it in your own graph tooling.
            </p>
          )}

          {/* Small network rendered as an interactive BODS graph. */}
          {isGraphMode && showGraph && bods && (
            <div className="mt-2">
              <Suspense fallback={<p className="text-[12px] text-oo-muted">Loading graph…</p>}>
                <BodsGraphExplorer statements={bods} entityName={entityName} direction="subsidiaries" fullCheck />
              </Suspense>
            </div>
          )}

          {/* Children list — always available (direct-first-then-tail). */}
          {data.children.length > 0 ? (
            <ChildrenTable children={data.children} />
          ) : (
            <p className="mt-2 text-[12px] text-oo-muted">No children to list.</p>
          )}
        </>
      )}
    </section>
  );
}
