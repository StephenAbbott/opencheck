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

import { useState } from "react";
import type { OpenAlephScreeningMatch } from "../../lib/api";

/** Matches shown before the "Show all" toggle expands the list. */
export const PREVIEW_COUNT = 10;

/**
 * The slice of matches to render for the current expanded state.
 * Pure — unit-tested in OpenAlephArchiveMatches.test.ts.
 */
export function visibleArchiveMatches(
  matches: OpenAlephScreeningMatch[],
  expanded: boolean
): OpenAlephScreeningMatch[] {
  if (expanded || matches.length <= PREVIEW_COUNT) return matches;
  return matches.slice(0, PREVIEW_COUNT);
}

export function OpenAlephArchiveMatches({
  matches,
}: {
  matches: OpenAlephScreeningMatch[];
}) {
  const [expanded, setExpanded] = useState(false);
  if (matches.length === 0) return null;
  const visible = visibleArchiveMatches(matches, expanded);
  const hiddenCount = matches.length - visible.length;

  return (
    <div
      id="openaleph-screening"
      className="mt-2 rounded-oo border border-oo-rule bg-white px-4 py-3"
    >
      <p className="text-[11px] font-semibold uppercase tracking-[0.08em] text-oo-muted mb-2">
        Archive matches — OpenAleph
      </p>
      <ul className="space-y-1.5">
        {visible.map((m) => (
          <li
            key={`${m.statement_id}:${m.entity_id}`}
            className="text-[13px] text-oo-ink leading-[1.6]"
          >
            <span className="font-semibold">{m.search_name}</span>{" "}
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
              </a>
            ) : (
              <span>{m.collection || "an OpenAleph collection"}</span>
            )}
            {m.topics.length > 0 && (
              <span className="text-oo-muted"> · {m.topics.join(", ")}</span>
            )}
          </li>
        ))}
      </ul>
      {(hiddenCount > 0 || expanded) && matches.length > PREVIEW_COUNT && (
        <button
          type="button"
          className="mt-2 text-[12px] font-medium text-oo-blue hover:underline underline-offset-2"
          onClick={() => setExpanded((prev) => !prev)}
        >
          {expanded
            ? "Show fewer"
            : `Show all ${matches.length} matches`}
        </button>
      )}
      <p className="text-[12px] text-oo-muted mt-3">
        Informational only — name matches from OpenAleph collections that map
        to no risk signal. A name match is never treated as identifier
        confirmation.
      </p>
    </div>
  );
}
