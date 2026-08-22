/**
 * BodsTree — the text equivalent of the ownership graph (Phase 124).
 *
 * The Cytoscape canvas in BODSGraph is `role="img"`: a screen-reader user
 * perceives only its accessible name, and nodes and edges are pointer-only.
 * This is the same structure as a WAI-ARIA `tree` whose rows are laid out as a
 * table (indented name + signals + interest + jurisdiction), keyboard-navigable
 * throughout, satisfying WCAG 2.1 SC 1.1.1 / 1.3.1 / 2.1.1 for every mount of
 * the graph. It shares `collapsed` and `selectedId` with the canvas, so
 * expanding or selecting in one is reflected in the other.
 *
 * **It is the only text equivalent.** Phase 124 retired
 * `BodsRelationshipTable`, which rendered the same GraphModel as a flat
 * relationship table from inside BODSGraph's own "View as table" toggle. With
 * the explorer's Split/Graph/Tree switch on top of that, one report could show
 * two different tables of the same statements side by side plus a children list
 * underneath. The tree won because it is navigable and it nests, which is the
 * shape of the data; the two things the table did better — risk-signal labels
 * per party, and naming parties with no reported relationships — moved here
 * rather than being dropped with it.
 */

import { useEffect, useRef, useState } from "react";
import type { TreeRow } from "../lib/bodsGraph";
import type { RiskSignal } from "../lib/api";
import { RISK_PRESENTATION } from "./risk/RiskChip";

function typeLabel(recordType: string): string {
  return recordType === "person" || recordType === "personStatement" ? "Person" : "Entity";
}

/** Country code as text from the flag URL ("/flags/gb.svg" → "GB") — the tree
 *  row only carries the flag image URL (see flagUrl() in lib/bodsGraph.ts). */
function jurisdictionCode(flagUrl: string): string {
  const m = flagUrl.match(/\/([a-z0-9]{2,3})\.svg$/i);
  return m ? m[1].toUpperCase() : "";
}

/** Human label for a risk-signal code, falling back to the code de-underscored.
 *  Carried over from BodsRelationshipTable: the canvas draws 1–3 character
 *  badges ("RSC", "≥3"), which are unreadable as text, so the equivalent must
 *  spell the signal out. */
function signalLabel(code: string): string {
  return RISK_PRESENTATION[code]?.label ?? code.replace(/_/g, " ");
}

export default function BodsTree({
  rows,
  selectedId,
  onSelect,
  onToggleCollapse,
  entityName,
  signalsByNode,
}: {
  rows: TreeRow[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onToggleCollapse: (id: string) => void;
  entityName?: string;
  /** Node id → signals scoped to that node (`buildSignalMap`, the same map the
   *  canvas badges read). Without it a row would say nothing about a party the
   *  graph has drawn a risk badge on — the equivalent would be incomplete. */
  signalsByNode?: Map<string, RiskSignal[]>;
}) {
  const [active, setActive] = useState(0);
  const rowEls = useRef<(HTMLDivElement | null)[]>([]);
  const wantFocus = useRef(false);

  // Keep `active` in range as the visible rows change (expand/collapse).
  useEffect(() => {
    if (active > rows.length - 1) setActive(Math.max(0, rows.length - 1));
  }, [rows.length, active]);

  // Reflect an external selection (e.g. a graph node click): move the active
  // row to the first occurrence of that node and scroll it into view.
  useEffect(() => {
    if (!selectedId) return;
    const idx = rows.findIndex((r) => r.id === selectedId);
    if (idx >= 0) setActive(idx);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId]);

  // Move focus / scroll the active row into view after it changes.
  useEffect(() => {
    const el = rowEls.current[active];
    if (!el) return;
    if (wantFocus.current) { el.focus(); wantFocus.current = false; }
    else el.scrollIntoView({ block: "nearest" });
  }, [active, rows]);

  function moveTo(idx: number) {
    wantFocus.current = true;
    setActive(Math.max(0, Math.min(rows.length - 1, idx)));
  }

  function onKeyDown(e: React.KeyboardEvent, i: number) {
    const row = rows[i];
    switch (e.key) {
      case "ArrowDown": e.preventDefault(); moveTo(i + 1); break;
      case "ArrowUp":   e.preventDefault(); moveTo(i - 1); break;
      case "Home":      e.preventDefault(); moveTo(0); break;
      case "End":       e.preventDefault(); moveTo(rows.length - 1); break;
      case "ArrowRight":
        e.preventDefault();
        if (row.hasChildren && !row.isRepeat && row.collapsed) onToggleCollapse(row.id);
        else moveTo(i + 1);
        break;
      case "ArrowLeft":
        e.preventDefault();
        if (row.hasChildren && !row.isRepeat && !row.collapsed) onToggleCollapse(row.id);
        else {
          // move to the parent: nearest previous row at a shallower depth
          for (let j = i - 1; j >= 0; j--) {
            if (rows[j].depth < row.depth) { moveTo(j); break; }
          }
        }
        break;
      case "Enter":
      case " ":
        e.preventDefault();
        onSelect(row.id);
        break;
    }
  }

  return (
    <div
      role="tree"
      aria-label={entityName ? `Ownership tree for ${entityName}` : "Ownership tree"}
      className="text-oo-meta border border-oo-rule rounded-oo overflow-auto bg-white"
      style={{ maxHeight: 460 }}
    >
      {rows.map((row, i) => {
        const expandable = row.hasChildren && !row.isRepeat;
        const isSelected = row.id === selectedId;
        return (
          <div
            key={row.rowKey}
            ref={(el) => { rowEls.current[i] = el; }}
            role="treeitem"
            aria-level={row.depth + 1}
            aria-selected={isSelected}
            aria-expanded={expandable ? !row.collapsed : undefined}
            tabIndex={i === active ? 0 : -1}
            onKeyDown={(e) => onKeyDown(e, i)}
            onClick={() => { setActive(i); onSelect(row.id); }}
            className={`flex items-center gap-1.5 px-2 py-1 border-b border-oo-rule/60 cursor-pointer outline-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-oo-blue focus-visible:-outline-offset-2 ${
              isSelected ? "bg-oo-soft" : "hover:bg-oo-bg"
            }`}
          >
            {/* Name cell — indentation + caret + icon + label */}
            <span className="flex items-center gap-1 min-w-0 flex-1" style={{ paddingLeft: row.depth * 14 }}>
              {expandable ? (
                <button
                  type="button"
                  tabIndex={-1}
                  aria-label={row.collapsed ? "Expand" : "Collapse"}
                  className="font-mono w-4 flex-shrink-0 text-oo-blue"
                  onClick={(e) => { e.stopPropagation(); onToggleCollapse(row.id); }}
                >
                  {row.collapsed ? "▸" : "▾"}
                </button>
              ) : (
                <span className="w-4 flex-shrink-0" />
              )}
              <span className="truncate">{row.label}</span>
              {/* Identifiers (LEI etc.) — `title` alone is mouse-only; expose
                  the same text to screen readers (WCAG 1.1.1). */}
              {row.identifiers.length > 0 && (
                <span className="sr-only">{row.identifiers.join(" · ")}</span>
              )}
              {row.isRepeat && (
                <span className="flex-shrink-0 text-oo-muted">
                  <span aria-hidden="true">↑</span>
                  <span className="sr-only">shown in full above</span>
                </span>
              )}
              {expandable && row.collapsed && (
                <span className="flex-shrink-0 text-oo-muted">({row.childCount})</span>
              )}
            </span>

            {/* Signal cell — the text equivalent of the canvas risk badges.
                Spelled out, not the badge's 1–3 character abbreviation. */}
            {(signalsByNode?.get(row.id)?.length ?? 0) > 0 && (
              <span className="flex-shrink-0 flex flex-wrap items-center gap-1">
                {[...new Set(signalsByNode!.get(row.id)!.map((s) => signalLabel(s.code)))].map(
                  (l) => (
                    <span
                      key={l}
                      className="text-oo-meta font-medium text-oo-ink border border-oo-rule rounded-full px-1.5"
                    >
                      {l}
                    </span>
                  )
                )}
              </span>
            )}

            {/* Interest cell */}
            {row.interestLabel && (
              <span className="flex-shrink-0 text-oo-meta text-oo-muted truncate max-w-[40%]">
                {row.interestLabel.split("\n")[0]}
              </span>
            )}

            {/* Jurisdiction cell — flag is decorative; the code is the text
                equivalent (the model only carries the flag URL, whose filename
                is the lowercased jurisdiction code). */}
            <span className="flex-shrink-0 flex items-center gap-1 min-w-[1.25rem]">
              {row.flagUrl && (
                <>
                  <img src={row.flagUrl} alt="" className="inline-block w-4 h-3 object-cover align-middle border border-black/10" />
                  <span className="text-oo-meta text-oo-muted">{jurisdictionCode(row.flagUrl)}</span>
                </>
              )}
            </span>

            {/* Type cell */}
            <span className="flex-shrink-0 w-14 text-oo-meta text-oo-muted uppercase tracking-wide text-right">
              {typeLabel(row.recordType)}
            </span>
          </div>
        );
      })}
    </div>
  );
}
