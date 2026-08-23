/**
 * RowList — the list that stays on the row (Phase 129).
 *
 * Most sources answer with a fact about the subject, and the finding sentence
 * is the whole answer; their records belong behind the `Data` disclosure. A
 * few answer with a *list* — TED's award notices, the OpenAleph collections a
 * name appears in — and for those the list **is** the finding. Putting it in
 * the drawer hides the answer behind a click and leaves the row saying only
 * that there is one.
 *
 * So: title on the left, its own metadata on the right, thin rules between,
 * a few rows and then a count. Deliberately not a table — these are not
 * columns of comparable values, and a table header over two cells is chrome
 * that says nothing.
 *
 * The cap is the point. An unbounded list on a row buries every source below
 * it, which is what a person-heavy graph did to the OpenAleph card before the
 * preview cap existed.
 */

import type { ReactNode } from "react";
import { ActionChip } from "./ActionChip";

export interface RowListItem {
  key: string;
  /** The thing itself — usually a link to the record. */
  title: ReactNode;
  /** What it is: a date, a buyer, a count. Right-aligned, never wrapped
   *  below the title on wide viewports, so the column scans. */
  meta?: ReactNode;
}

export function RowList({
  items,
  total,
  expanded,
  onToggle,
  controls,
  moreLabel,
  footnote,
}: {
  items: RowListItem[];
  /** How many there are in all — the "+N more" is computed from this, so a
   *  caller that shows everything cannot accidentally offer to show more. */
  total: number;
  expanded: boolean;
  onToggle: () => void;
  controls: string;
  /** Noun for the count: "notice", "collection". Pluralised here. */
  moreLabel: string;
  footnote?: ReactNode;
}) {
  if (items.length === 0) return null;
  const hidden = Math.max(0, total - items.length);
  return (
    <div className="mt-3 border-t border-oo-rule pt-2.5">
      <ul id={controls} className="flex flex-col">
        {items.map((item) => (
          <li
            key={item.key}
            className="flex items-baseline justify-between gap-4 border-b border-oo-bg py-1.5 text-oo-small"
          >
            <span className="min-w-0">{item.title}</span>
            {item.meta ? (
              <span className="shrink-0 text-oo-muted">{item.meta}</span>
            ) : null}
          </li>
        ))}
      </ul>
      {(hidden > 0 || expanded) && (
        <div className="mt-2.5">
          <ActionChip onClick={onToggle} expanded={expanded} controls={controls}>
            {expanded
              ? "Show fewer"
              : `+${hidden} more ${moreLabel}${hidden === 1 ? "" : "s"}`}
          </ActionChip>
        </div>
      )}
      {footnote ? (
        <p className="mt-2 text-oo-meta text-oo-muted leading-[1.5] max-w-[80ch]">
          {footnote}
        </p>
      ) : null}
    </div>
  );
}
