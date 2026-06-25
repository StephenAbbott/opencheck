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
 *
 * SPIKE — progressive discovery: selecting a corporate node reveals an action
 * to resolve its owners one hop deeper (live, via /expand). The newly fetched
 * BODS is merged into the local statement set and the graph re-derives. Person
 * nodes are terminal; nodes without an LEI are an honest dead-end (the case
 * bulk register data would cover). Driven off `selectedId`, so the intricate
 * Cytoscape event/overlay code is untouched.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import BODSGraph from "./BODSGraph";
import BodsTree from "./BodsTree";
import { bodsToGraph, autoCollapse, buildTree, type GraphModel } from "../lib/bodsGraph";
import { expandNode, type RiskSignal } from "../lib/api";
import { isEntityStatement, subjectLei, mergeStatements } from "../lib/expand";

type Stmt = Record<string, unknown>;
type ViewMode = "split" | "graph" | "tree";

const VIEW_OPTIONS: { value: ViewMode; label: string }[] = [
  { value: "split", label: "Split" },
  { value: "graph", label: "Graph" },
  { value: "tree", label: "Tree" },
];

// SPIKE guard: cap live hops per session so a runaway click-fest can't fan out
// the whole register.
const MAX_EXPANSIONS = 12;

export default function BodsGraphExplorer({
  statements,
  signals = [],
  entityName,
}: {
  statements: unknown[];
  signals?: RiskSignal[];
  entityName?: string;
}) {
  // SPIKE: owners revealed via progressive discovery, merged onto the base set.
  const [extra, setExtra] = useState<Stmt[]>([]);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [expanding, setExpanding] = useState<string | null>(null);
  const [expandNote, setExpandNote] = useState<string | null>(null);

  const baseModel: GraphModel = useMemo(
    () => bodsToGraph(statements as Stmt[]),
    [statements]
  );
  const allStatements: Stmt[] = useMemo(
    () => mergeStatements(statements as Stmt[], extra),
    [statements, extra]
  );
  const model: GraphModel = useMemo(() => bodsToGraph(allStatements), [allStatements]);

  const [collapsed, setCollapsed] = useState<Set<string>>(() => autoCollapse(baseModel));
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [view, setView] = useState<ViewMode>("split");
  const prevStatementsRef = useRef<unknown[]>(statements);

  // Reset everything when the underlying subject changes (a new lookup), but NOT
  // when we expand (which only grows `extra`). Keying on the base `statements`
  // prop — not the derived model — is what keeps expansion from self-resetting.
  useEffect(() => {
    if (prevStatementsRef.current !== statements) {
      setExtra([]);
      setExpandedIds(new Set());
      setSelectedId(null);
      setExpandNote(null);
      setCollapsed(autoCollapse(baseModel));
      prevStatementsRef.current = statements;
    }
  }, [statements, baseModel]);

  const rows = useMemo(() => buildTree(model, collapsed), [model, collapsed]);

  // Citation chips in the narrative panel dispatch `oc:cite` with the statement
  // they reference; if it lives in this graph, focus it (expanding it first so a
  // collapsed node still becomes visible).
  useEffect(() => {
    function onCite(ev: Event) {
      const sid = (ev as CustomEvent<{ statementId?: string | null }>).detail?.statementId;
      if (!sid) return;
      if (!model.nodes.some((n) => n.id === sid)) return;
      setCollapsed((prev) => {
        if (!prev.has(sid)) return prev;
        const next = new Set(prev);
        next.delete(sid);
        return next;
      });
      setSelectedId(sid);
    }
    window.addEventListener("oc:cite", onCite as EventListener);
    return () => window.removeEventListener("oc:cite", onCite as EventListener);
  }, [model]);

  function toggleCollapse(id: string) {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  // ── SPIKE: reveal-owners action for the selected node ──────────────────────
  const selectedStmt = selectedId
    ? allStatements.find((s) => s.statementId === selectedId)
    : undefined;
  const selectedNode = selectedId ? model.nodes.find((n) => n.id === selectedId) : undefined;
  const selectedLabel = selectedNode?.label ?? "this company";

  async function revealOwners() {
    if (!selectedId || !isEntityStatement(selectedStmt)) return;
    if (expandedIds.has(selectedId)) return;
    if (expandedIds.size >= MAX_EXPANSIONS) {
      setExpandNote("Reached the expansion limit for this session.");
      return;
    }
    const lei = subjectLei(selectedStmt);
    if (!lei) {
      setExpandNote(
        "This company has no LEI, so its owners can't be resolved live — this is the case bulk register data would cover."
      );
      return;
    }
    setExpanding(selectedId);
    setExpandNote(null);
    try {
      const res = await expandNode(lei, selectedId);
      const added = res.bods as Stmt[];
      setExtra((prev) => mergeStatements(prev, added));
      setExpandedIds((prev) => new Set(prev).add(selectedId));
      const newOwners = added.filter((s) => s.recordType === "relationship").length;
      setExpandNote(
        newOwners > 0
          ? null
          : "No further owners disclosed for this entity (a terminal node)."
      );
    } catch (e) {
      setExpandNote(`Couldn't expand: ${(e as Error).message}`);
    } finally {
      setExpanding(null);
    }
  }

  if (model.nodes.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  const expandControl = (() => {
    if (!selectedId || !selectedNode) {
      return (
        <span className="text-oo-muted">
          Select a company node to reveal its owners a layer deeper.
        </span>
      );
    }
    if (!isEntityStatement(selectedStmt)) {
      return (
        <span className="text-oo-muted">
          People are terminal in an ownership chain — nothing to expand.
        </span>
      );
    }
    if (expandedIds.has(selectedId)) {
      return <span className="text-oo-muted">Owners revealed for {selectedLabel}.</span>;
    }
    return (
      <button
        type="button"
        onClick={revealOwners}
        disabled={expanding !== null}
        className="text-[12px] px-2.5 py-1 rounded-full border border-[#1565c0] text-[#1565c0] hover:bg-[#e8f0fb] disabled:opacity-50"
      >
        {expanding === selectedId ? "Revealing…" : `▸ Reveal owners of ${selectedLabel}`}
      </button>
    );
  })();

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

      {/* SPIKE: progressive-discovery control */}
      <div className="mb-2 flex items-center gap-2 flex-wrap text-[12px]">{expandControl}</div>
      {expandNote && (
        <p className="mb-2 text-[12px] text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1 leading-[1.5]">
          {expandNote}
        </p>
      )}

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
