import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { getSubsidiaries } from "../../lib/api";
import type { RiskSignal, SubsidiariesResponse, SubsidiaryChild } from "../../lib/api";
import { scopeCrossSourceSignals } from "../../lib/signalScope";
import { describeFetchFailure, panelError, type PanelError, type PanelId } from "../../lib/panelErrors";
import InvitationStrip from "../ui/InvitationStrip";

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
    <span className={`text-oo-meta font-semibold rounded px-1.5 py-0.5 border ${m.classes}`}>
      {m.label}
      {/* GLEIF Level 2 is accounting consolidation, not shareholding. That
          caveat is the whole reason this badge is not called "owns", and until
          Phase 124 it lived only in a `title` — invisible to keyboard and
          touch, on the one label most likely to be misread as ownership. */}
      <span className="sr-only">
        {relation === "both"
          ? " — reported as both a direct and an ultimate consolidating relationship"
          : relation === "ultimate"
            ? " — consolidated by the group head, not directly"
            : " — directly consolidated"}
      </span>
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

const SUBSIDIARY_ICON = (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden="true">
    <rect x="5" y="1.5" width="4" height="3" rx="0.6" stroke="currentColor" strokeWidth="1.1" />
    <rect x="1.5" y="9.5" width="4" height="3" rx="0.6" stroke="currentColor" strokeWidth="1.1" />
    <rect x="8.5" y="9.5" width="4" height="3" rx="0.6" stroke="currentColor" strokeWidth="1.1" />
    <path d="M7 4.5 V7 M7 7 H3.5 V9.5 M7 7 H10.5 V9.5" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" />
  </svg>
);

// ---------------------------------------------------------------------
// SubsidiaryNetwork — lazy GLEIF children reveal (graph or table + export)
// ---------------------------------------------------------------------

export function SubsidiaryNetwork({
  lei,
  entityName,
  signals = [],
  onError,
  onRecovered,
}: {
  lei: string;
  entityName?: string;
  /** Report a fetch failure to the report-level notice — these panels sit
   *  outside `_lookup_pipeline`, so nothing else knows they failed. */
  onError?: (e: PanelError) => void;
  onRecovered?: (panel: PanelId) => void;
  /** The lookup's top-level risk signals. This graph is built from a
   *  separately-fetched GLEIF children bundle, so — like the source cards —
   *  it saw no cross-source findings at all until Phase 109. */
  signals?: RiskSignal[];
}) {
  const [data, setData] = useState<SubsidiariesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  // Collapsing unmounts the panel and put focus on <body>; the strip that
  // replaces it takes focus instead. NzAssociations already did this — the
  // duplicated component is why the fix only landed in one of the two.
  const stripRef = useRef<HTMLButtonElement>(null);
  const wantStripFocus = useRef(false);
  useEffect(() => {
    if (collapsed && wantStripFocus.current) {
      stripRef.current?.focus();
      wantStripFocus.current = false;
    }
  }, [collapsed]);
  const collapse = () => { wantStripFocus.current = true; setCollapsed(true); };

  // BODS statements for the graph / export — fetched on demand (format=bods).
  const [bods, setBods] = useState<Record<string, unknown>[] | null>(null);
  const [bodsLoading, setBodsLoading] = useState(false);
  const [showGraph, setShowGraph] = useState(false);

  // Only the cross-source signals that land on a statement in the fetched
  // subsidiaries bundle — see lib/signalScope.ts.
  const graphSignals = useMemo(() => scopeCrossSourceSignals(signals, bods ?? []), [signals, bods]);

  async function run() {
    if (loading || data) return;
    setLoading(true);
    setError(null);
    try {
      setData(await getSubsidiaries(lei, "summary"));
      onRecovered?.("subsidiaries");
    } catch (e) {
      // Was `String(e)` — the reader saw "Error: 500 Internal Server Error".
      setError(describeFetchFailure(e));
      onError?.(panelError("subsidiaries", e));
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
      setError(describeFetchFailure(e));
      onError?.(panelError("subsidiaries", e));
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
    return (
      <InvitationStrip
        title="Reveal subsidiary network"
        detail="GLEIF Level 2 direct and ultimate children, mapped to BODS · live lookup"
        icon={SUBSIDIARY_ICON}
        onClick={run}
        buttonRef={stripRef}
      />
    );
  }

  // Collapsed after viewing — back to the invitation strip; re-opening keeps the
  // already-fetched data (no second lookup).
  if (data && collapsed) {
    return (
      <InvitationStrip
        title="Reveal subsidiary network"
        detail="GLEIF Level 2 direct and ultimate children, mapped to BODS · already fetched"
        icon={SUBSIDIARY_ICON}
        onClick={() => setCollapsed(false)}
        buttonRef={stripRef}
      />
    );
  }

  const isGraphMode = data?.render_mode === "graph";

  return (
    <section className="mt-3 rounded-oo border border-oo-rule bg-oo-bg p-3">
      {loading && <p className="text-[12px] text-oo-muted">Fetching the GLEIF subsidiary network…</p>}
      {error && (
        // role="alert" so it is announced, matching NzAssociations and
        // FullCheckPanel. Amber, not red: an upstream that did not answer is
        // incomplete, not a failure the reader caused. And a retry, because
        // once `error` was truthy the invitation strip's early return no
        // longer fired and the only way back was a page reload.
        <div role="alert" className="flex flex-wrap items-center justify-between gap-2">
          <p className="text-oo-meta text-oo-warn-text">
            The subsidiary network could not be fetched — {error}. This is not a
            finding that the entity has none.
          </p>
          <button
            type="button"
            onClick={() => { setError(null); setData(null); run(); }}
            className="shrink-0 rounded border border-oo-warn-border px-2.5 py-1 text-oo-meta font-semibold text-oo-warn-text hover:bg-oo-warn-bg"
          >
            Try again
          </button>
        </div>
      )}

      {data && !data.available && (
        <div className="flex items-start justify-between gap-3">
          <p className="text-[12px] text-oo-muted leading-[1.6]">
            No subsidiary network published{data.reason ? ` (${data.reason})` : ""}.
          </p>
          <button
            type="button"
            onClick={collapse}
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
              onClick={collapse}
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
                <BodsGraphExplorer statements={bods} signals={graphSignals} entityName={entityName} direction="subsidiaries" fullCheck />
              </Suspense>
            </div>
          )}

          {/* Children list — the view when there is no diagram (Phase 124).
              It used to render underneath the graph as well, so a revealed
              network showed the diagram, the diagram's own text equivalent and
              this list: three renderings of one set of statements. The
              explorer's "Read as text" disclosure covers the graph case, and
              covers it better, because it nests. */}
          {!(isGraphMode && showGraph && bods) &&
            (data.children.length > 0 ? (
              <ChildrenTable children={data.children} />
            ) : (
              <p className="mt-2 text-oo-meta text-oo-muted">No children to list.</p>
            ))}
        </>
      )}
    </section>
  );
}
