/**
 * BodsGraphExplorer — the dual-pane explorer for a BODS ownership structure.
 *
 * Owns the state shared between the two panes — `collapsed` (which nodes are
 * collapsed) and `selectedId` (the focused node) — and renders the visual
 * graph (BODSGraph) alongside the accessible tabular tree (BodsTree). A view
 * toggle switches between split / graph-only / tree-only; the layout is
 * side-by-side on wide screens and stacked on narrow ones.
 *
 * This is the component SourceBucketCard mounts; the panes themselves are
 * controlled, so selecting or expanding in one is reflected in the other.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import BODSGraph from "./BODSGraph";
import BodsTree from "./BodsTree";
import { bodsToGraph, autoCollapse, buildTree, type GraphModel } from "../lib/bodsGraph";
import type { RiskSignal } from "../lib/api";

type ViewMode = "split" | "graph" | "tree";

const VIEW_OPTIONS: { value: ViewMode; label: string }[] = [
  { value: "split", label: "Split" },
  { value: "graph", label: "Graph" },
  { value: "tree", label: "Tree" },
];

export default function BodsGraphExplorer({
  statements,
  signals = [],
  entityName,
}: {
  statements: unknown[];
  signals?: RiskSignal[];
  entityName?: string;
}) {
  const model: GraphModel = useMemo(
    () => bodsToGraph(statements as Record<string, unknown>[]),
    [statements]
  );

  const [collapsed, setCollapsed] = useState<Set<string>>(() => autoCollapse(model));
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [view, setView] = useState<ViewMode>("split");
  const prevModelRef = useRef<GraphModel | null>(null);

  // Reset shared state when the statement bundle changes (not on first mount).
  useEffect(() => {
    if (prevModelRef.current && prevModelRef.current !== model) {
      setCollapsed(autoCollapse(model));
      setSelectedId(null);
    }
    prevModelRef.current = model;
  }, [model]);

  const rows = useMemo(() => buildTree(model, collapsed), [model, collapsed]);

  function toggleCollapse(id: string) {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  if (model.nodes.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  return (
    <div>
      {/* View toggle */}
      <div className="flex items-center gap-1 mb-1.5" role="group" aria-label="Visualisation view">
        {VIEW_OPTIONS.map((opt) => (
          <button
            key={opt.value}
            type="button"
            aria-pressed={view === opt.value}
            onClick={() => setView(opt.value)}
            className={`text-[11px] px-2 py-0.5 rounded-full border ${
              view === opt.value
                ? "bg-[#e8f0fb] border-[#1565c0] text-[#1565c0] font-medium"
                : "border-oo-rule text-oo-muted hover:text-oo-blue"
            }`}
          >
            {opt.label}
          </button>
        ))}
      </div>

      <div className={`flex flex-col gap-2 ${view === "split" ? "lg:flex-row" : ""}`}>
        {view !== "tree" && (
          <div className="flex-1 min-w-0">
            <BODSGraph
              model={model}
              signals={signals}
              entityName={entityName}
              collapsed={collapsed}
              onCollapsedChange={setCollapsed}
              selectedId={selectedId}
              onSelect={setSelectedId}
            />
          </div>
        )}
        {view !== "graph" && (
          <div className={view === "split" ? "w-full lg:w-96 lg:max-w-[44%] flex-shrink-0" : "w-full"}>
            <BodsTree
              rows={rows}
              selectedId={selectedId}
              onSelect={setSelectedId}
              onToggleCollapse={toggleCollapse}
              entityName={entityName}
            />
          </div>
        )}
      </div>
    </div>
  );
}
