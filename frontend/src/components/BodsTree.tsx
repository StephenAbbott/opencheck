/**
 * BodsTree — an accessible, tabular tree view of a BODS ownership structure.
 *
 * It is the keyboard- and screen-reader-friendly counterpart to the canvas
 * graph (which is only `role="img"`): a WAI-ARIA `tree` whose rows are laid
 * out as a table (indented name + interest + jurisdiction). It shares the
 * `collapsed` and `selectedId` state with the graph, so expanding/collapsing
 * or selecting in one pane is reflected in the other.
 */

import { useEffect, useRef, useState } from "react";
import type { TreeRow } from "../lib/bodsGraph";

function typeLabel(recordType: string): string {
  return recordType === "person" || recordType === "personStatement" ? "Person" : "Entity";
}

export default function BodsTree({
  rows,
  selectedId,
  onSelect,
  onToggleCollapse,
  entityName,
}: {
  rows: TreeRow[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onToggleCollapse: (id: string) => void;
  entityName?: string;
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
      className="text-xs border border-oo-rule rounded-oo overflow-auto bg-white"
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
            className={`flex items-center gap-1.5 px-2 py-1 border-b border-oo-rule/60 cursor-pointer outline-none ${
              isSelected ? "bg-[#e8f0fb]" : "hover:bg-oo-bg"
            }`}
            title={row.identifiers.length ? row.identifiers.join(" · ") : undefined}
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
              {row.isRepeat && (
                <span className="flex-shrink-0 text-oo-muted" title="Shown in full above">↑</span>
              )}
              {expandable && row.collapsed && (
                <span className="flex-shrink-0 text-oo-muted">({row.childCount})</span>
              )}
            </span>

            {/* Interest cell */}
            {row.interestLabel && (
              <span className="flex-shrink-0 text-[11px] text-oo-muted truncate max-w-[40%]">
                {row.interestLabel.split("\n")[0]}
              </span>
            )}

            {/* Jurisdiction flag cell */}
            <span className="flex-shrink-0 w-5 text-center">
              {row.flagUrl
                ? <img src={row.flagUrl} alt="" className="inline-block w-4 h-3 object-cover align-middle border border-black/10" />
                : null}
            </span>

            {/* Type cell */}
            <span className="flex-shrink-0 w-12 text-[10px] text-oo-muted uppercase tracking-wide text-right">
              {typeLabel(row.recordType)}
            </span>
          </div>
        );
      })}
    </div>
  );
}
