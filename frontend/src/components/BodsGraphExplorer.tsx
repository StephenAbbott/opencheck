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
import {
  expandLayer,
  downloadNetwork,
  type RiskSignal,
  type NetworkExportFormat,
} from "../lib/api";
import {
  dedupeFrontier,
  frontierAnchors,
  mergeStatements,
  mergeSignals,
  signalsBeyond,
  type ExpandDirection,
} from "../lib/expand";
import { reconcileBods, remapSignals, possiblySameAs } from "../lib/reconcile";
import { RiskChip } from "./risk/RiskChip";
import { SourceLegend } from "./SourceLegend";

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

// FullCheck eager run: stop expanding once the network reaches this many company
// nodes — also the graph's comfortable render ceiling.
const FULLCHECK_NODE_CAP = 150;

export default function BodsGraphExplorer({
  statements,
  signals = [],
  entityName,
  direction = "owners",
  fullCheck = false,
}: {
  statements: unknown[];
  signals?: RiskSignal[];
  entityName?: string;
  /** Which way "Add next layer" digs: an ownership graph goes up (owners), a
   *  subsidiary tree goes down (children). The mounting view sets this. */
  direction?: ExpandDirection;
  /** FullCheck mode: also show a "Run FullCheck" control that eagerly expands the
   *  network to a chosen depth budget (Phase 1 eager traversal). */
  fullCheck?: boolean;
}) {
  // Layers revealed via progressive discovery, merged onto the base statement set.
  const [extra, setExtra] = useState<Stmt[]>([]);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [expanding, setExpanding] = useState(false);
  const [expandNote, setExpandNote] = useState<string | null>(null);
  // Risk signals discovered while expanding (each hop's sub-lookup screens the
  // expanded entity) — the network-wide risk beyond the subject's own screening.
  const [discoveredSignals, setDiscoveredSignals] = useState<RiskSignal[]>([]);
  // FullCheck eager-run controls (driven by runFullCheck below).
  const [depthBudget, setDepthBudget] = useState(2);
  const [running, setRunning] = useState(false);
  const [runProgress, setRunProgress] = useState<string | null>(null);
  const cancelRef = useRef(false);
  // FullCheck network export.
  const [exportFormat, setExportFormat] = useState<NetworkExportFormat>("zip");
  const [exporting, setExporting] = useState(false);

  // FullCheck reconciliation (display transform): merge per-source duplicate
  // nodes that share an LEI/company number into one node, keyed by a stable
  // identifier-derived id, stamping each surviving statement with `_sources`.
  // Applied ONLY to the rendered model — the frontier/expansion bookkeeping
  // below keeps running on the raw statements, so the working traversal is
  // untouched. QuickCheck panels (fullCheck=false) render the raw model.
  const allStatements: Stmt[] = useMemo(
    () => mergeStatements(statements as Stmt[], extra),
    [statements, extra]
  );
  const recon = useMemo(
    () => (fullCheck ? reconcileBods(allStatements) : null),
    [fullCheck, allStatements]
  );
  const baseModel: GraphModel = useMemo(
    () =>
      bodsToGraph(
        fullCheck ? reconcileBods(statements as Stmt[]).statements : (statements as Stmt[])
      ),
    [statements, fullCheck]
  );
  const model: GraphModel = useMemo(
    () => bodsToGraph((recon?.statements ?? allStatements) as Stmt[]),
    [recon, allStatements]
  );
  // FullCheck: name-only "likely same" candidates (post-reconciliation) → dashed
  // review edges. Empty for QuickCheck (recon === null).
  const sameAs = useMemo(() => (recon ? possiblySameAs(recon.statements) : []), [recon]);
  // Frontier/expansion run on the RAW (unreconciled) edges so the live traversal
  // keys are unchanged; for QuickCheck raw == display.
  const rawEdges = useMemo(
    () => (recon ? bodsToGraph(allStatements).edges : model.edges),
    [recon, allStatements, model]
  );

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
      setDiscoveredSignals([]);
      setRunProgress(null);
      setHighlightSource(null);
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
  // The frontier is computed on the RAW statements (so the live traversal keys
  // are unchanged). In FullCheck the display is reconciled, so a raw frontier
  // can list several per-source duplicates of the same entity — inflating the
  // "Add next layer — N" count above the visible node count and re-fetching the
  // same company. Dedupe by canonical id (via the reconcile remap) so the count
  // matches what the user sees and each entity is expanded once.
  const frontier = useMemo(() => {
    const raw = frontierAnchors(allStatements, rawEdges, expandedIds, direction);
    return recon ? dedupeFrontier(raw, recon.remap) : raw;
  }, [allStatements, rawEdges, expandedIds, direction, recon]);
  const noun = direction === "subsidiaries" ? "subsidiaries" : "owners/controllers";
  const helperText =
    direction === "subsidiaries"
      ? "Resolves the next layer of subsidiaries for frontier companies which have an LEI. Chains which end with people can't be explored further"
      : "Resolves the next layer of ownership for frontier companies which have an LEI. Chains which end with people can't be explored further";

  // Subject signals (QuickCheck, from the prop) + everything discovered while
  // expanding = the network-wide risk; `additionalSignals` is the diff.
  const networkSignals = useMemo(
    () => mergeSignals(signals, discoveredSignals),
    [signals, discoveredSignals]
  );
  const additionalSignals = useMemo(
    () => signalsBeyond(signals, discoveredSignals),
    [signals, discoveredSignals]
  );

  // ── FullCheck provenance: source legend + highlight-by-source ──────────────
  const [highlightSource, setHighlightSource] = useState<string | null>(null);
  // Distinct sources across the reconciled display, with how many nodes each
  // asserts (corroboration count) — drives the legend chips.
  const networkSources = useMemo(() => {
    if (!fullCheck) return [];
    const counts = new Map<string, number>();
    for (const n of model.nodes)
      for (const s of n.sources) counts.set(s, (counts.get(s) ?? 0) + 1);
    return [...counts.entries()]
      .map(([source, count]) => ({ source, count }))
      .sort((a, b) => b.count - a.count || a.source.localeCompare(b.source));
  }, [fullCheck, model]);
  // Nodes corroborated by ≥2 sources — the EDD confidence signal.
  const corroboratedCount = useMemo(
    () => (fullCheck ? model.nodes.filter((n) => n.sources.length > 1).length : 0),
    [fullCheck, model]
  );
  // Drop a stale highlight if its source left the network (e.g. after a reset).
  useEffect(() => {
    if (highlightSource && !networkSources.some((s) => s.source === highlightSource)) {
      setHighlightSource(null);
    }
  }, [networkSources, highlightSource]);
  // Risk-signal evidence ids follow the merged node so BOVS overlays still land.
  const displaySignals = useMemo(
    () => (recon ? remapSignals(networkSignals, recon.remap) : networkSignals),
    [recon, networkSignals]
  );

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
      setDiscoveredSignals((prev) => mergeSignals(prev, res.risk_signals));
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

  // ── FullCheck: eagerly expand the network to a depth budget ────────────────
  async function runFullCheck() {
    if (running) return;
    setRunning(true);
    cancelRef.current = false;
    setExpandNote(null);
    setRunProgress("Starting FullCheck…");
    try {
      // Accumulate locally: React state updates aren't visible within this loop,
      // so each layer recomputes the frontier from the local `working` set and
      // also pushes to `extra` for progressive rendering.
      let working = allStatements;
      const expanded = new Set(expandedIds);
      let stop = "";
      for (let d = 0; d < depthBudget; d++) {
        if (cancelRef.current) { stop = "cancelled"; break; }
        const front = frontierAnchors(working, bodsToGraph(working).edges, expanded, direction);
        if (front.length === 0) {
          stop = "frontier exhausted (no further LEI-bearing companies)";
          break;
        }
        const entities = working.filter((s) => (s as Stmt).recordType === "entity").length;
        if (entities >= FULLCHECK_NODE_CAP || expanded.size >= MAX_EXPANDED) {
          stop = `node cap reached (${entities} companies)`;
          break;
        }
        setRunProgress(
          `Layer ${d + 1} of ${depthBudget} — expanding ${front.length} ${front.length === 1 ? "company" : "companies"}…`
        );
        const res = await expandLayer(front, direction);
        working = mergeStatements(working, res.bods as Stmt[]);
        // Only the anchors the server actually processed (it caps each batch).
        res.expanded.forEach((a) => expanded.add(a));
        setExtra((prev) => mergeStatements(prev, res.bods as Stmt[]));
        setDiscoveredSignals((prev) => mergeSignals(prev, res.risk_signals));
        setExpandedIds(new Set(expanded));
      }
      if (cancelRef.current) setRunProgress("FullCheck cancelled.");
      else setRunProgress(`FullCheck complete — ${stop || `reached the depth budget (${depthBudget})`}.`);
    } catch (e) {
      setRunProgress(`FullCheck failed: ${(e as Error).message}`);
    } finally {
      setRunning(false);
      cancelRef.current = false;
    }
  }

  async function exportNetwork() {
    if (exporting) return;
    setExporting(true);
    setExpandNote(null);
    try {
      await downloadNetwork(allStatements, exportFormat, entityName ?? undefined);
    } catch (e) {
      setExpandNote(`Export failed: ${(e as Error).message}`);
    } finally {
      setExporting(false);
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

      {/* FullCheck: eager "Run" to a depth budget */}
      {fullCheck && (
        <div className="mb-2 rounded-oo border border-[#1565c0] bg-[#f3f7fe] px-3 py-2">
          <div className="flex items-center gap-3 flex-wrap">
            <button
              type="button"
              onClick={running ? () => { cancelRef.current = true; } : runFullCheck}
              disabled={!running && frontier.length === 0}
              className="bg-oo-blue text-white text-[13px] font-semibold rounded px-4 py-1.5 hover:bg-oo-burst transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {running ? "Cancel" : "▸ Run FullCheck"}
            </button>
            <label className="text-[12px] text-oo-ink flex items-center gap-1.5">
              Depth
              <input
                type="number"
                min={1}
                max={5}
                value={depthBudget}
                disabled={running}
                onChange={(e) =>
                  setDepthBudget(Math.max(1, Math.min(5, Number(e.target.value) || 1)))
                }
                className="w-12 border border-oo-rule rounded px-1.5 py-0.5 text-[12px]"
              />
            </label>
            <span className="text-[11px] text-oo-muted leading-[1.5] max-w-sm">
              Builds the wider {direction === "subsidiaries" ? "subsidiary" : "ownership"} network to
              the chosen depth (LEI-bearing companies; capped at {FULLCHECK_NODE_CAP}).
            </span>
          </div>
          {runProgress && (
            <p className="mt-1.5 text-[12px] text-[#1565c0]" aria-live="polite">
              {runProgress}
            </p>
          )}
        </div>
      )}

      {/* FullCheck: risk-first — network risk + QuickCheck-vs-FullCheck diff */}
      {fullCheck && (
        <div className="mb-2 rounded-oo border border-oo-rule bg-white px-3 py-2">
          <div className="text-[11px] font-semibold uppercase tracking-oo-eyebrow text-oo-blue mb-1">
            Network risk
          </div>
          <p className="text-[13px] text-oo-ink leading-[1.5]">
            QuickCheck flagged <strong>{signals.length}</strong> signal
            {signals.length === 1 ? "" : "s"} on the subject.
            {discoveredSignals.length > 0 ? (
              <>
                {" "}FullCheck surfaced <strong>{additionalSignals.length}</strong> more
                across the wider network.
              </>
            ) : (
              <> Run FullCheck to screen the wider network for risk.</>
            )}
          </p>
          {additionalSignals.length > 0 && (
            <div className="mt-1.5 flex flex-wrap gap-1.5">
              {additionalSignals.map((s, i) => (
                <RiskChip key={i} signal={s} compact />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Prominent "Add next layer" control — FullCheck only (QuickCheck graph
          panels are view-only). */}
      {fullCheck && (
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
      )}
      {expandNote && (
        <p role="status" className="mb-2 text-[12px] text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1 leading-[1.5]">
          {expandNote}
        </p>
      )}

      <div className={`flex flex-col gap-2 ${view === "split" ? "lg:flex-row" : ""}`}>
        {view !== "tree" && (
          <div className="flex-1 min-w-0">
            {fullCheck && networkSources.length > 0 && (
              <SourceLegend
                sources={networkSources}
                active={highlightSource}
                corroboratedCount={corroboratedCount}
                onToggle={(s) => setHighlightSource((cur) => (cur === s ? null : s))}
              />
            )}
            {fullCheck && sameAs.length > 0 && (
              <p className="mb-1.5 text-[11px] text-[#b45309] leading-[1.5]">
                <span className="font-semibold">{sameAs.length}</span> dashed “likely same”{" "}
                {sameAs.length === 1 ? "link" : "links"}: same name + jurisdiction, no shared
                identifier — review before treating as one entity (not auto-merged).
              </p>
            )}
            <BODSGraph
              model={model}
              signals={displaySignals}
              entityName={entityName}
              collapsed={collapsed}
              onCollapsedChange={setCollapsed}
              selectedId={selectedId}
              onSelect={setSelectedId}
              highlightSource={highlightSource}
              sameAs={sameAs}
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

      {fullCheck && (
        <div className="mt-3 flex items-center gap-2 flex-wrap border-t border-oo-rule pt-3">
          <span className="text-[12px] text-oo-muted">Export network</span>
          <select
            value={exportFormat}
            aria-label="Export network format"
            onChange={(e) => setExportFormat(e.target.value as NetworkExportFormat)}
            className="border border-oo-rule rounded px-2 py-1 text-[12px] bg-white"
          >
            <option value="zip">ZIP (all formats + licences)</option>
            <option value="json">BODS · JSON</option>
            <option value="jsonl">BODS · JSONL</option>
            <option value="xml">BODS · XML</option>
            <option value="senzing">Senzing JSON</option>
            <option value="ftm">FollowTheMoney (Aleph)</option>
            <option value="cypher">Neo4j · Cypher</option>
            <option value="rdf">RDF · TriG</option>
            <option value="gql">BigQuery · GQL (zip)</option>
            <option value="amlai">Google AML AI (zip)</option>
          </select>
          <button
            type="button"
            onClick={exportNetwork}
            disabled={exporting}
            className="bg-oo-blue text-white text-[12px] font-medium rounded px-3 py-1 hover:bg-oo-burst transition-colors disabled:opacity-50"
          >
            {exporting ? "Exporting…" : "Download"}
          </button>
        </div>
      )}
    </div>
  );
}
