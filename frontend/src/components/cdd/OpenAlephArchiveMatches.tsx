/**
 * Archive matches — OpenAleph (informational percolation matches, Phase 97).
 *
 * Related-party names found in OpenAleph collections whose topics map to no
 * RELATED_* risk code (leak/court collections, poi, corp.disqual). Rendered
 * directly underneath the OpenAleph source card so the matches sit with the
 * source they came from; falls back to a standalone placement when OpenAleph
 * produced no card at all (screening can match even when the subject lookup
 * found nothing).
 *
 * Name-derived — never rendered as a risk signal and never treated as
 * identifier corroboration.
 *
 * Long lists are capped at PREVIEW_COUNT entries with a "Show all" toggle —
 * a person-heavy graph (e.g. Eli Lilly's board) can percolate into dozens of
 * archive matches, and an unbounded list buried the sources that follow.
 */

import { useId, useState } from "react";
import { groupIdentical, repeatNote } from "../../lib/hitGroups";
import { topicLabel } from "../../lib/vocab";
import { ActionChip, Chip, SectionHeading } from "../ui";
import type { OpenAlephScreeningMatch } from "../../lib/api";

/** Matches shown before the "Show all" toggle expands the list. */
export const PREVIEW_COUNT = 10;

/**
 * The slice of matches to render for the current expanded state.
 * Pure — unit-tested in OpenAlephArchiveMatches.test.ts.
 */
export function visibleArchiveMatches<T>(matches: T[], expanded: boolean): T[] {
  if (expanded || matches.length <= PREVIEW_COUNT) return matches;
  return matches.slice(0, PREVIEW_COUNT);
}

/**
 * What an archive row says: who matched, what kind of party they are, the
 * name that matched if it differs, the collection, and the topics. Not
 * `entity_id` — two OpenAleph records for the same person in the same
 * collection are exactly the case this collapses.
 */
export function archiveRowKey(m: OpenAlephScreeningMatch): string {
  return [
    m.search_name,
    m.kind,
    m.matched_name ?? "",
    m.collection ?? "",
    [...new Set(m.topics)].sort().join(","),
  ].join("|");
}

export function OpenAlephArchiveMatches({
  matches,
  standalone = false,
}: {
  matches: OpenAlephScreeningMatch[];
  /** No source card to sit inside (OpenAleph produced no bucket), so carry
   *  the card chrome instead of the band's top rule — which against a card's
   *  own border would draw a doubled line. */
  standalone?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  // The control expands the list, not the wrapper that contains the control.
  const listId = useId();
  if (matches.length === 0) return null;
  // Two records for one person in one collection rendered as two identical
  // rows on a live BP lookup — the same defect as the source rows above, and
  // grouped by the same rule: what the row says, not what identifies it.
  const grouped = groupIdentical(matches, archiveRowKey);
  const visible = visibleArchiveMatches(grouped, expanded);
  const hiddenCount = grouped.length - visible.length;

  return (
    <div
      id="openaleph-screening"
      className={`px-5 py-3.5 ${
        standalone
          ? "bg-white border border-oo-rule rounded-oo"
          : "border-t border-oo-rule"
      }`}
    >
      <div className="flex items-baseline justify-between gap-3 flex-wrap mb-2">
        {/* h3 standing alone, h4 nested under a source card's own h3 — the
            level has to follow where the block actually sits. */}
        <SectionHeading as={standalone ? "h3" : "h4"}>Archive matches</SectionHeading>
        <p className="text-oo-small text-oo-muted">
          {matches.length} name {matches.length === 1 ? "match" : "matches"} · no risk signal
        </p>
      </div>
      <ul id={listId} className="flex flex-col gap-1.5">
        {visible.map(({ lead: m, count }) => (
          <li
            key={`${m.statement_id}:${m.entity_id}`}
            className="text-oo-small text-oo-ink leading-[1.6]"
          >
            <span className="font-bold">{m.search_name}</span>{" "}
            <span className="text-oo-muted">
              {m.kind === "person" ? "(related party)" : "(related entity)"}{" "}
              matched
              {m.matched_name &&
              m.matched_name.toLowerCase() !== m.search_name.toLowerCase()
                ? ` ‘${m.matched_name}’`
                : ""}{" "}
              in{" "}
            </span>
            {m.url ? (
              <a
                href={m.url}
                target="_blank"
                rel="noreferrer"
                className="text-oo-blue hover:underline underline-offset-2"
              >
                {m.collection || "an OpenAleph collection"}
                <span className="sr-only"> (opens in new tab)</span>
              </a>
            ) : (
              <span>{m.collection || "an OpenAleph collection"}</span>
            )}
            {repeatNote(count) && (
              <span className="text-oo-muted"> ({repeatNote(count)})</span>
            )}
            {m.topics.length > 0 && (
              <>
                {" "}
                {[...new Set(m.topics.map(topicLabel))].map((t) => (
                  <Chip key={t} tone="context" size="sm">
                    {t}
                  </Chip>
                ))}
              </>
            )}
          </li>
        ))}
      </ul>
      {(hiddenCount > 0 || expanded) && matches.length > PREVIEW_COUNT && (
        <div className="mt-2.5">
          <ActionChip
            onClick={() => setExpanded((prev) => !prev)}
            expanded={expanded}
            controls={listId}
          >
            {expanded ? "Show fewer" : `Show all ${grouped.length} matches`}
          </ActionChip>
        </div>
      )}
      <p className="text-oo-meta text-oo-muted mt-3 leading-[1.5] max-w-[80ch]">
        Informational only — name matches from OpenAleph collections that map
        to no risk signal. A name match is never treated as identifier
        confirmation.
      </p>
    </div>
  );
}
