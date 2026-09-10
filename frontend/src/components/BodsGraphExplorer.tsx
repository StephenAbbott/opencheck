/**
 * BodsGraphExplorer — one diagram, one text equivalent (Phase 124).
 *
 * Owns the state shared between the canvas and its equivalent — `collapsed`
 * (which nodes are collapsed) and `selectedId` (the focused node) — and renders
 * the diagram (BODSGraph) with the accessible tree (BodsTree) underneath it.
 *
 * **The tree is a fallback, not a peer view.** v1 had a Split / Graph / Tree
 * switch here, BODSGraph had its own "View as table" toggle inside the Graph
 * pane, and SubsidiaryNetwork rendered a children list beneath both — so in
 * Split mode with the table on, a reader saw two different tables of the same
 * statements side by side plus a third list below. There is now one canvas, one
 * text equivalent, and the equivalent is always available rather than being a
 * mode you can end up in instead of the diagram.
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

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import BODSGraph from "./BODSGraph";
import BodsTree from "./BodsTree";
import { bodsToGraph, autoCollapse, buildTree, type GraphModel } from "../lib/bodsGraph";
import {
  expandLayer,
  fetchExpandSchemes,
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
import {
  canonicalStatementId,
  reconcileBods,
  remapSignals,
  possiblySameAs,
} from "../lib/reconcile";
import { buildSignalMap } from "../lib/signalScope";
import { independentCount } from "../lib/lineage";
import { riskFindingCount } from "../lib/signalKind";
import { RiskChip } from "./risk/RiskChip";
import { SourceLegend } from "./SourceLegend";
import {
  layerControl,
  networkRiskSentence,
  runHelper,
  summaryParts,
} from "../lib/fullCheckHeader";
import { buttonClasses } from "./ui";

type Stmt = Record<string, unknown>;

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
  focusStatementId = null,
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
  /** Phase 200: a statement to select once the graph has it — how a board row
   *  on the History tab reaches the person it names. A prop rather than the
   *  `oc:cite` event because the arrival order differs: a citation chip fires
   *  while the graph is already mounted, whereas this target is known before
   *  the panel has even fetched its statements, so an event would land in the
   *  gap and be lost. Raw statement ids are fine — resolved through the
   *  reconcile remap below, same as a citation. */
  focusStatementId?: string | null;
}) {
  // Layers revealed via progressive discovery, merged onto the base statement set.
  const [extra, setExtra] = useState<Stmt[]>([]);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [expanding, setExpanding] = useState(false);
  const [expandNote, setExpandNote] = useState<string | null>(null);
  // Risk signals discovered while expanding (each hop's sub-lookup screens the
  // expanded entity) — the network-wide risk beyond the subject's own screening.
  const [discoveredSignals, setDiscoveredSignals] = useState<RiskSignal[]>([]);
  // Phase 182: the identifier schemes the server can hop on without an LEI
  // (a GB-COH company number → Companies House). Loaded once; until it
  // arrives, or if it never does, the frontier is LEI-only as before.
  const [hopSchemes, setHopSchemes] = useState<ReadonlySet<string>>(() => new Set());
  useEffect(() => {
    let live = true;
    fetchExpandSchemes().then((schemes) => {
      if (live) setHopSchemes(schemes);
    });
    return () => {
      live = false;
    };
  }, []);
  // FullCheck eager-run controls (driven by runFullCheck below).
  const [depthBudget, setDepthBudget] = useState(2);
  const [running, setRunning] = useState(false);
  const [runProgress, setRunProgress] = useState<string | null>(null);
  const cancelRef = useRef(false);
  // How the network reached its current size — the two header states turn on
  // this. `runDepth` accumulates the layers eager runs actually COMPLETED (not
  // the budget they were given: a run that stops at the node cap has expanded
  // one layer, not three, and the summary must not claim otherwise).
  const [runDepth, setRunDepth] = useState<number | null>(null);
  const [manualLayers, setManualLayers] = useState(0);
  // "Go deeper" re-opens the run control over an expanded network.
  const [showRunControls, setShowRunControls] = useState(false);
  const runButtonRef = useRef<HTMLButtonElement>(null);
  const goDeeperRef = useRef<HTMLButtonElement>(null);
  // A run started from the button that is about to unmount: focus has to be
  // put somewhere deliberate when the header collapses, or it falls to <body>
  // — the same bug `selectMode` fixed for the mode tabs.
  const returnFocusRef = useRef(false);
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
      setRunDepth(null);
      setManualLayers(0);
      setShowRunControls(false);
      setHighlightSource(null);
      setCollapsed(autoCollapse(baseModel));
      prevStatementsRef.current = statements;
    }
  }, [statements, baseModel]);

  const rows = useMemo(() => buildTree(model, collapsed), [model, collapsed]);

  /**
   * Select a statement by id, expanding a collapsed ancestor first so it is
   * actually visible. Returns whether the graph had it.
   *
   * The id is resolved through the reconcile remap before it is looked up.
   * Phase 195 merges people across registers into one canonical node, so a
   * caller holding the Companies House statement id for a director who is
   * also in OpenCorporates is holding an id no node carries any more. Until
   * Phase 200 this step was missing and such a citation silently focused
   * nothing — worse than an error, because the click appeared to work.
   */
  const focusStatement = useCallback(
    (rawId: string | null | undefined): boolean => {
      if (!rawId) return false;
      const sid = canonicalStatementId(rawId, recon?.remap);
      if (!model.nodes.some((n) => n.id === sid)) return false;
      setCollapsed((prev) => {
        if (!prev.has(sid)) return prev;
        const next = new Set(prev);
        next.delete(sid);
        return next;
      });
      setSelectedId(sid);
      return true;
    },
    [model, recon],
  );

  // Citation chips in the narrative panel dispatch `oc:cite` with the statement
  // they reference; if it lives in this graph, focus it.
  useEffect(() => {
    function onCite(ev: Event) {
      focusStatement(
        (ev as CustomEvent<{ statementId?: string | null }>).detail?.statementId,
      );
    }
    window.addEventListener("oc:cite", onCite as EventListener);
    return () => window.removeEventListener("oc:cite", onCite as EventListener);
  }, [focusStatement]);

  // Phase 200: a focus target that arrived with the page rather than from a
  // click. Runs whenever the model changes as well as when the target does,
  // because the statements are still being fetched when the prop first
  // arrives — the node appears a beat later. Focusing an already-focused
  // node is a no-op, so re-running is harmless; what it buys is that the
  // link works on a cold load, which is the case that matters (someone
  // opened a shared URL).
  const focusedRef = useRef<string | null>(null);
  useEffect(() => {
    if (!focusStatementId) return;
    if (focusedRef.current === focusStatementId) return;
    if (focusStatement(focusStatementId)) focusedRef.current = focusStatementId;
  }, [focusStatementId, focusStatement]);

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
    const raw = frontierAnchors(allStatements, rawEdges, expandedIds, direction, hopSchemes);
    return recon ? dedupeFrontier(raw, recon.remap) : raw;
  }, [allStatements, rawEdges, expandedIds, direction, recon, hopSchemes]);
  const noun = direction === "subsidiaries" ? "subsidiaries" : "owners and controllers";
  const registerHops = hopSchemes.size > 0;

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
  const subjectRiskCount = useMemo(() => riskFindingCount(signals), [signals]);

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
  // Nodes corroborated by ≥2 INDEPENDENT sources — the EDD confidence signal.
  // `n.sources` lists every source that asserted the node (provenance, kept
  // in full for the legend and the highlight toggles); corroboration is the
  // lineage-collapsed count, so Companies House + OpenCorporates is one.
  const corroboratedCount = useMemo(
    () => (fullCheck ? model.nodes.filter((n) => independentCount(n.sources) > 1).length : 0),
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
  // The same node→signals map the canvas draws badges from, so the text
  // equivalent cannot silently omit a party the diagram has flagged.
  const signalMap = useMemo(() => buildSignalMap(displaySignals), [displaySignals]);

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
      setManualLayers((n) => n + 1);
      const newRels = (res.bods as Stmt[]).filter((s) => s.recordType === "relationship").length;
      const parts: string[] = [];
      if (newRels === 0) parts.push(`No further ${noun} disclosed for the companies at the edge of the network.`);
      if (res.truncated) parts.push(`Only the first ${res.count} were expanded — there were more.`);
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
    returnFocusRef.current = true;
    setRunProgress("Starting FullCheck…");
    let completed = 0;
    try {
      // Accumulate locally: React state updates aren't visible within this loop,
      // so each layer recomputes the frontier from the local `working` set and
      // also pushes to `extra` for progressive rendering.
      let working = allStatements;
      const expanded = new Set(expandedIds);
      let stop = "";
      for (let d = 0; d < depthBudget; d++) {
        if (cancelRef.current) { stop = "cancelled"; break; }
        const front = frontierAnchors(
          working, bodsToGraph(working).edges, expanded, direction, hopSchemes
        );
        if (front.length === 0) {
          stop = registerHops && direction === "owners"
            ? "there was nothing further to expand (no more companies with an LEI or a readable company number)"
            : "there was nothing further to expand (no more companies with an LEI)";
          break;
        }
        const entities = working.filter((s) => (s as Stmt).recordType === "entity").length;
        if (entities >= FULLCHECK_NODE_CAP || expanded.size >= MAX_EXPANDED) {
          stop = `the size limit was reached (${entities} companies)`;
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
        completed += 1;
      }
      if (completed > 0) setRunDepth((prev) => (prev ?? 0) + completed);
      setShowRunControls(false);
      if (cancelRef.current) setRunProgress("FullCheck cancelled.");
      else setRunProgress(`FullCheck complete — ${stop || `reached the depth budget (${depthBudget})`}.`);
    } catch (e) {
      setRunProgress(`FullCheck failed: ${(e as Error).message}`);
    } finally {
      setRunning(false);
      cancelRef.current = false;
    }
  }

  /** Back to the network the subject's own lookup produced. */
  function resetNetwork() {
    setExtra([]);
    setExpandedIds(new Set());
    setDiscoveredSignals([]);
    setRunProgress(null);
    setRunDepth(null);
    setManualLayers(0);
    setExpandNote(null);
    setHighlightSource(null);
    setShowRunControls(false);
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

  // ── The header's two states ──────────────────────────────────────
  // A network that has been expanded at all is a network with a result in it,
  // so the control that produced it collapses to a summary of what it did.
  const hasRun = expandedIds.size > 0;
  const personCount = useMemo(
    () =>
      model.nodes.filter((n) => n.recordType === "person" || n.recordType === "personStatement")
        .length,
    [model]
  );
  const summary = useMemo(
    () =>
      summaryParts(
        {
          companies: model.nodes.length - personCount,
          people: personCount,
          sources: networkSources.length,
          corroborated: corroboratedCount,
        },
        { runDepth, manualLayers }
      ),
    [model, personCount, networkSources, corroboratedCount, runDepth, manualLayers]
  );
  const layer = useMemo(
    () => layerControl({ frontier: frontier.length, noun, busy: expanding }),
    [frontier, noun, expanding]
  );

  // Focus follows the control that vanished. Collapsing the run box unmounts
  // the button the user just pressed; without this, focus lands on <body> and a
  // keyboard user restarts from the top of the document — the same failure
  // `selectMode` fixed for the mode tabs.
  useEffect(() => {
    if (running || !hasRun || showRunControls || !returnFocusRef.current) return;
    returnFocusRef.current = false;
    goDeeperRef.current?.focus();
  }, [running, hasRun, showRunControls]);
  useEffect(() => {
    if (showRunControls) runButtonRef.current?.focus();
  }, [showRunControls]);

  if (model.nodes.length === 0) {
    return <p className="text-xs text-oo-muted italic">No BODS statements to visualise.</p>;
  }


  return (
    <div>
      {/* ── FullCheck header, state 1 of 2: the run control ────────────────
          One primary action, one line of help. The full-width "Add next layer"
          button that used to sit below this one is now in the canvas toolbar:
          two blue buttons ran the same expansion at different budgets, with
          nothing on screen saying which was the bigger one.

          It stays mounted for the whole run, because the first layer landing is
          what flips `hasRun` — collapsing on that would take Cancel and the
          progress line away mid-traversal. */}
      {fullCheck && (!hasRun || showRunControls || running) && (
        <div className="mb-2 rounded-oo border border-oo-blue bg-oo-soft px-3 py-2">
          <div className="flex items-center gap-3 flex-wrap">
            <button
              type="button"
              ref={runButtonRef}
              onClick={running ? () => { cancelRef.current = true; } : runFullCheck}
              disabled={!running && frontier.length === 0}
              className={buttonClasses("primary", "sm")}
            >
              {running ? "Cancel" : hasRun ? "▸ Go deeper" : "▸ Run FullCheck"}
            </button>
            <label className="text-oo-meta text-oo-ink flex items-center gap-1.5">
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
                className="w-12 border border-oo-rule rounded px-1.5 py-0.5 text-oo-meta"
              />
            </label>
            <span className="text-oo-meta text-oo-muted max-w-sm">
              {runHelper({ direction, registerHops, cap: FULLCHECK_NODE_CAP })}
            </span>
            {hasRun && !running && (
              <button
                type="button"
                onClick={() => {
                  returnFocusRef.current = true;
                  setShowRunControls(false);
                }}
                className={buttonClasses("ghost", "sm", "ml-auto")}
              >
                Hide
              </button>
            )}
          </div>
          {runProgress && (
            <p className="mt-1.5 text-oo-meta text-oo-blue" aria-live="polite">
              {runProgress}
            </p>
          )}
        </div>
      )}

      {/* ── FullCheck header, state 2 of 2: what was built ──────────────
          The control has answered its own question, so it collapses to a line
          stating what was run and what it reached. The run's stop reason stays
          on it: that sentence is how a reader learns the traversal hit the node
          cap rather than running out of network to walk. */}
      {fullCheck && hasRun && !running && !showRunControls && (
        <div
          role="status"
          aria-label="FullCheck run summary"
          className="mb-2 rounded-oo border border-oo-rule bg-white px-3 py-2"
        >
          <div className="flex items-center gap-2 flex-wrap text-oo-meta">
            <span className="font-semibold text-oo-ink">FullCheck</span>
            {summary.map((part) => (
              <span key={part} className="flex items-center gap-2">
                <span aria-hidden="true" className="text-oo-rule">·</span>
                <span className="text-oo-muted">{part}</span>
              </span>
            ))}
            <span className="ml-auto flex items-center gap-1">
              <button
                type="button"
                ref={goDeeperRef}
                onClick={() => setShowRunControls(true)}
                className={buttonClasses("ghost", "sm")}
              >
                Go deeper
              </button>
              <button type="button" onClick={resetNetwork} className={buttonClasses("ghost", "sm")}>
                Reset
              </button>
            </span>
          </div>
          {runProgress && <p className="mt-1 text-oo-meta text-oo-blue">{runProgress}</p>}
        </div>
      )}

      {/* FullCheck: risk-first — network risk + QuickCheck-vs-FullCheck diff.
          Both counts are distinct FINDINGS (`riskFindingCount`), not raw
          signals: the related-party rules emit several signals per hit, so a raw
          count overstates, and the verdict strip counts the same way — one
          function, so the two lines cannot disagree. The wording, and its zero
          branches, live in `lib/fullCheckHeader`. */}
      {fullCheck && (
        <div className="mb-2 rounded-oo border border-oo-rule bg-white px-3 py-2">
          <div className="text-oo-meta font-semibold uppercase tracking-oo-eyebrow text-oo-blue mb-1">
            Network risk
          </div>
          <p className="text-oo-small text-oo-ink">
            {networkRiskSentence({
              subject: subjectRiskCount,
              additional: riskFindingCount(additionalSignals),
              hasRun,
            })}
          </p>
          {additionalSignals.length > 0 && (
            <div className="mt-1.5 flex flex-wrap gap-1.5">
              {additionalSignals.map((sig, i) => (
                <RiskChip key={i} signal={sig} compact />
              ))}
            </div>
          )}
        </div>
      )}

      {expandNote && (
        <p role="status" className="mb-2 text-[12px] text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1 leading-[1.5]">
          {expandNote}
        </p>
      )}

      <div className="flex flex-col gap-2">
        <div className="min-w-0">
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
              layer={fullCheck ? layer : undefined}
              onAddLayer={fullCheck ? addNextLayer : undefined}
            />
            {/* Provenance sits UNDER the canvas it describes. Above it, on a
                network nobody had asked to expand yet, it was four source chips
                and a corroboration count standing between the reader and the
                diagram — describing a graph they had not seen. */}
            {fullCheck && networkSources.length > 0 && (
              <div className="mt-2">
                <SourceLegend
                  sources={networkSources}
                  active={highlightSource}
                  corroboratedCount={corroboratedCount}
                  onToggle={(src) => setHighlightSource((cur) => (cur === src ? null : src))}
                />
              </div>
            )}
        </div>

        {/* Text equivalent of the canvas (WCAG 1.1.1 / 1.3.1 / 2.1.1). Open by
            default is wrong — it doubles the height of every source card — but
            hidden behind a mode switch was worse, because a reader could land
            in the equivalent *instead of* the diagram. A disclosure that names
            its row count is available on every mount and costs nothing shut. */}
        <details className="rounded-oo border border-oo-rule bg-white">
          <summary className="cursor-pointer px-3 py-1.5 text-oo-meta text-oo-blue hover:bg-oo-soft rounded-oo">
            Read as text — {rows.length} {rows.length === 1 ? "row" : "rows"}, keyboard navigable
          </summary>
          <div className="border-t border-oo-rule">
            <BodsTree
              rows={rows}
              selectedId={selectedId}
              onSelect={setSelectedId}
              onToggleCollapse={toggleCollapse}
              entityName={entityName}
              signalsByNode={signalMap}
            />
          </div>
        </details>
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
