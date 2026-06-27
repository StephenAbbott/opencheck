/**
 * BodsGraphExplorer — the dual-pane explorer for a BODS ownership structure.
 *
 * Owns the state shared between the two panes — `collapsed` (which nodes are
 * collapsed) and `selectedId` (the focused node) — and renders the visual
 * graph (BODSGraph) alongside the accessible tabular tree (BodsTree). A view
 * toggle switches between split / graph-only / tree-only.
 *
 * Progressive discovery ("Add next layer"): a single action takes the current
 * *frontier* (LEI-bearing entity nodes at the growing edge of the graph) and
 * resolves every one a hop deeper at once, live, via /expand-layer. The mounting
 * view sets the direction — an ownership graph digs up (owners), a subsidiary
 * tree digs down (children). Fetched layers are merged into the local statement
 * set (deduped by statementId) and the graph re-derives, so new nodes render one
 * rank further out in the graph's existing direction rather than spawning a
 * floating cluster. People are terminal; nodes without an LEI are skipped (the
 * bulk-data case). Driven off the derived model, so the Cytoscape event/overlay
 * code is untouched. This is the owner/subsidiary traversal foundation that
 * FullCheck's network exploration will build on.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import BODSGraph from "./BODSGraph";
import BodsTree from "./BodsTree";
import { bodsToGraph, autoCollapse, buildTree, type GraphModel } from "../lib/bodsGraph";
import { expandLayer, type RiskSignal } from "../lib/api";
import { frontierAnchors, mergeStatements, type ExpandDirection } from "../lib/expand";

type Stmt = Record<string, unknown>;
type ViewMode = "split" | "graph" | "tree";

const VIEW_OPTIONS: { value: ViewMode; label: string }[] = [
  { value: "split", label: "Split" },
  { value: "graph", label: "Graph" },
  { value: "tree", label: "Tree" },
];

// Guard: cap how many anchors we'll expand across a session so a runaway
// click-fest can't fan out the whole register (the server also caps each batch).
const MAX_EXPANDED = 60;

export default function BodsGraphExplorer({
  statements,
  signals = [],
  entityName,
  direction = "owners",
}: {
  statements: unknown[];
  signals?: RiskSignal[];
  entityName?: string;
  /** Which way "Add next layer" digs: an ownership graph goes up (owners), a
   *  subsidiary tree goes down (children). The mounting view sets this. */
  direction?: ExpandDirection;
}) {
  // Layers revealed via progressive discovery, merged onto the base statement set.
  const [extra, setExtra] = useState<Stmt[]>([]);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [expanding, setExpanding] = useState(false);
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
  // they reference; if it lives in this graph, focus it (expanding a collapsed
  // node first so it becomes visible).
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

  // ── "Add next layer" over the whole frontier (direction set by the view) ───
  const frontier = useMemo(
    () => frontierAnchors(allStatements, model.edges, expandedIds, direction),
    [allStatements, model.edges, expandedIds, direction]
  );
  const noun = direction === "subsidiaries" ? "subsidiaries" : "owners/controllers";
  const helperText =
    direction === "subsidiaries"
      ? "Resolves the next layer of subsidiaries for frontier companies which have an LEI. Chains which end with people can't be explored further"
      : "Resolves the next layer of ownership for frontier companies which have an LEI. Chains which end with people can't be explored further";

  async function addNextLayer() {
    if (!frontier.length || expanding) return;
    if (expandedIds.size >= MAX_EXPANDED) {
      setExpandNote("Reached the expansion limit for this session.");
      return;
    }
    setExpanding(true);
    setExpandNote(null);
    try {
      const res = await expandLayer(frontier, direction);
      setExtra((prev) => mergeStatements(prev, res.bods as Stmt[]));
      setExpandedIds((prev) => {
        const next = new Set(prev);
        frontier.forEach((f) => next.add(f.anchor));
        return next;
      });
      const newRels = (res.bods as Stmt[]).filter((s) => s.recordType === "relationship").length;
      const parts: string[] = [];
      if (newRels === 0) parts.push(`No further ${noun} disclosed for the current frontier.`);
      if (res.truncated) parts.push(`Only the first ${res.count} were expanded (frontier was larger).`);
      setExpandNote(parts.join(" ") || null);
    } catch (e) {
      setExpandNote(`Couldn't add layer: ${(e as Error).message}`);
    } finally {
      setExpanding(false);
    }
  }

  if (model.nodes.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }

  const frontierLabel = frontier.length === 1 ? "company" : "companies";

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

      {/* Prominent "Add next layer" control */}
      <div className="mb-2 flex items-center gap-3 flex-wrap">
        <button
          type="button"
          onClick={addNextLayer}
          disabled={expanding || frontier.length === 0}
          className="bg-oo-blue text-white text-[13px] font-medium rounded px-4 py-1.5 hover:bg-oo-burst transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {expanding
            ? "Adding layer…"
            : frontier.length === 0
              ? `No further ${noun} to reveal`
              : `▸ Add next layer — ${frontier.length} ${frontierLabel}`}
        </button>
        <span className="text-[11px] text-oo-muted leading-[1.5] max-w-md">
          {helperText}
        </span>
      </div>
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
