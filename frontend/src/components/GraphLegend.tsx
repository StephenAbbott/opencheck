/**
 * GraphLegend — what the marks on the diagram mean (Phase 124).
 *
 * Generated from `lib/graphStyle.ts`, the same values the Cytoscape stylesheet
 * draws with, and scoped to the graph in front of the reader. The v1 legend was
 * a hand-written list of six chips beside a vocabulary of thirty badges, three
 * of which it happened to cover; the rest were explained only by a `title`
 * carrying the raw signal code.
 *
 * Every entry carries a **non-colour cue** as well as a colour — solid, dotted
 * or dashed for edges, the badge mark itself for signals — so the legend does
 * not fail WCAG 1.4.1 on the same page as the diagram it explains.
 */

import { useMemo, useState } from "react";
import type { RiskSignal } from "../lib/api";
import { EDGE_STYLE, buildGraphLegend } from "../lib/graphStyle";
import { RISK_PRESENTATION } from "./risk/RiskChip";
import { IdentityTick } from "./ui/IdentityTick";

function signalName(code: string): string {
  return RISK_PRESENTATION[code]?.label ?? code.replace(/_/g, " ");
}

/** A short rule in the edge's own colour and dash pattern. */
function EdgeRule({ color, dash }: { color: string; dash: "solid" | "dotted" | "dashed" }) {
  return (
    <span
      aria-hidden="true"
      className="inline-block w-3.5 flex-shrink-0"
      style={
        dash === "solid"
          ? { borderTop: `2px solid ${color}` }
          : { borderTop: `1.5px ${dash} ${color}` }
      }
    />
  );
}

export default function GraphLegend({
  edgeCategories,
  signalsByNode,
  hasPeople,
  hasCollapsed,
  hasIdentityVerified = false,
  hasEnded = false,
  hasClusters = false,
}: {
  edgeCategories: Iterable<string>;
  signalsByNode: Map<string, RiskSignal[]>;
  hasPeople: boolean;
  hasCollapsed: boolean;
  hasIdentityVerified?: boolean;
  hasEnded?: boolean;
  hasClusters?: boolean;
}) {
  const legend = useMemo(
    () =>
      buildGraphLegend({
        edgeCategories,
        signalsByNode,
        hasPeople,
        hasCollapsed,
        hasIdentityVerified,
        hasEnded,
        hasClusters,
        signalName,
      }),
    [edgeCategories, signalsByNode, hasPeople, hasCollapsed, hasIdentityVerified, hasEnded, hasClusters]
  );
  // Signals can run to a dozen entries on a big FullCheck network, so they sit
  // behind a count the reader can close. They start OPEN (Phase 243): shut, a
  // node read "RP / RCS / N / Ex" with nothing on screen to say what the
  // letters meant until the reader found "Show 4 signal marks" (Opus 5.5
  // check, D-H3).
  const [showSignals, setShowSignals] = useState(true);

  const total =
    legend.edges.length + legend.edgeModifiers.length + legend.nodes.length + legend.signals.length;
  if (total === 0) return null;

  return (
    <div className="px-3 pb-2">
      <div className="flex flex-wrap items-center gap-1.5">
        {legend.edges.map((e) => (
          <span
            key={e.key}
            className="flex items-center gap-1.5 text-oo-meta font-medium px-2 py-0.5 rounded-full border"
            style={{ background: e.style.tint, borderColor: e.style.color, color: e.style.textColor }}
          >
            <EdgeRule color={e.style.color} dash={e.style.dash} />
            {e.name}
            <span className="sr-only"> — {e.meaning}</span>
          </span>
        ))}

        {/* Phase 219 / 243 — the modifier chip shows the mark itself: a
            lighter rule ending in a hollow arrowhead, beside words that say
            what it means. */}
        {legend.edgeModifiers.map((m) => (
          <span
            key={m.key}
            className="flex items-center gap-1.5 text-oo-meta font-medium px-2 py-0.5 rounded-full border border-oo-rule bg-white text-oo-muted"
          >
            <svg aria-hidden="true" width="18" height="8" viewBox="0 0 18 8" className="flex-shrink-0">
              <line x1="0" y1="4" x2="11" y2="4" stroke={EDGE_STYLE.unknown.endedColor} strokeWidth="2" />
              <path d="M 11 1 L 17 4 L 11 7 z" fill="white" stroke={EDGE_STYLE.unknown.endedColor} strokeWidth="1.2" />
            </svg>
            {m.name}
            <span className="sr-only"> — {m.meaning}</span>
          </span>
        ))}

        {legend.nodes.map((n) => (
          <span
            key={n.key}
            className="flex items-center gap-1.5 text-oo-meta font-medium px-2 py-0.5 rounded-full border border-oo-rule bg-white text-oo-muted"
          >
            {n.key === "person" ? (
              <span
                aria-hidden="true"
                className="inline-block h-3 w-3 rounded-full border border-dashed border-oo-navy flex-shrink-0"
              />
            ) : n.key === "identityVerified" ? (
              <IdentityTick />
            ) : n.key === "cluster" ? (
              <span
                aria-hidden="true"
                className="inline-block h-3 w-4 rounded-sm border-2 border-double border-oo-blue bg-oo-soft flex-shrink-0"
              />
            ) : (
              <span
                aria-hidden="true"
                className="inline-block h-3 w-3 rounded-full border-2 border-oo-blue flex-shrink-0"
              />
            )}
            {n.name}
            <span className="sr-only"> — {n.meaning}</span>
          </span>
        ))}

        {legend.signals.length > 0 && (
          <button
            type="button"
            aria-expanded={showSignals}
            onClick={() => setShowSignals((v) => !v)}
            className="text-oo-meta font-medium px-2 py-0.5 rounded-full border border-oo-rule bg-white text-oo-blue hover:bg-oo-soft"
          >
            {showSignals ? "Hide" : "Show"} {legend.signals.length} signal mark
            {legend.signals.length === 1 ? "" : "s"}
          </button>
        )}
      </div>

      {showSignals && legend.signals.length > 0 && (
        <dl className="mt-1.5 flex flex-wrap gap-x-3 gap-y-1">
          {legend.signals.map((s) => (
            <div key={s.key} className="flex items-center gap-1.5">
              <dt
                className="inline-flex h-4 min-w-[1rem] items-center justify-center rounded-full border px-1 text-oo-meta font-semibold"
                style={{ background: s.style.bg, borderColor: s.style.border, color: s.style.text }}
              >
                {s.badge}
              </dt>
              <dd className="text-oo-meta text-oo-muted">{s.name}</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  );
}
