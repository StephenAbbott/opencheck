import { useState } from "react";
import type { PossiblySameEntity } from "../../lib/api";

/* Moved out of App.tsx in Phase 246, unchanged. */

// How many possibly-same pairs are visible before the rest collapse behind
// the "Show more" toggle. Multi-source subjects (e.g. DNO ASA) can flag many
// pairs, which otherwise dominates the results page.
const POSSIBLY_SAME_PREVIEW_COUNT = 2;

/**
 * Renders the name-only "likely same" entity candidates surfaced by the backend
 * reconciler (exact name + jurisdiction, no shared identifier). These are
 * **suggestions for a human to review**, never confirmed merges — the certain
 * matches already appear in the cross-source identifiers table above. Renders
 * nothing when there are no candidates.
 */
export function PossiblySameTable({ pairs }: { pairs: PossiblySameEntity[] }) {
  const [expanded, setExpanded] = useState(false);
  if (pairs.length === 0) return null;
  const hiddenCount = pairs.length - POSSIBLY_SAME_PREVIEW_COUNT;
  const visible =
    expanded || hiddenCount <= 0
      ? pairs
      : pairs.slice(0, POSSIBLY_SAME_PREVIEW_COUNT);
  return (
    <>
      <p className="text-oo-meta text-oo-muted mb-3">
        These records share an exact name and jurisdiction but no common
        identifier, so they are <em>likely</em> the same entity — flagged for
        review, not merged automatically.
      </p>
      <table className="w-full text-oo-small border-collapse table-fixed">
        <thead>
          <tr>
            {/* Narrower Records column and tighter letter-spacing on mobile:
                at tracking-widest the single words "Jurisdiction" and
                "Confidence" are wider than an 18% column on a phone, so they
                overflow their cells and collide. break-words lets them wrap
                rather than spill. */}
            <th className="text-left text-[10px] font-medium tracking-wide sm:tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[44%] sm:w-[64%] align-bottom">
              Records
            </th>
            <th className="text-left text-[10px] font-medium tracking-wide sm:tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[26%] sm:w-[18%] align-bottom break-words">
              Jurisdiction
            </th>
            <th className="text-right text-[10px] font-medium tracking-wide sm:tracking-widest uppercase text-oo-muted pb-2 w-[30%] sm:w-[18%] align-bottom break-words">
              Confidence
            </th>
          </tr>
        </thead>
        <tbody>
          {visible.map((p) => (
            <tr key={`${p.a}~${p.b}`} className="border-t border-oo-rule align-top">
              <td className="py-2 pr-3 text-oo-ink">
                <div className="break-words">
                  {p.a_name || p.a}
                  {p.a_source && (
                    <span className="ml-1.5 align-middle text-[10px] bg-oo-bg border border-oo-rule rounded px-1 py-0.5 text-oo-muted whitespace-nowrap">
                      {p.a_source}
                    </span>
                  )}
                </div>
                <div className="break-words text-oo-muted">
                  {p.b_name || p.b}
                  {p.b_source && (
                    <span className="ml-1.5 align-middle text-[10px] bg-oo-bg border border-oo-rule rounded px-1 py-0.5 text-oo-muted whitespace-nowrap">
                      {p.b_source}
                    </span>
                  )}
                </div>
              </td>
              <td className="py-2 pr-3 font-mono text-oo-meta text-oo-muted">
                {p.jurisdiction || "—"}
              </td>
              <td className="py-2 text-right">
                <span className="inline-flex items-center gap-1 text-[11px] bg-amber-50 border border-amber-300 text-amber-800 rounded px-1.5 py-0.5">
                  likely same
                </span>
                {p.reason && (
                  <span className="block mt-1 text-[11px] text-oo-muted break-words">
                    {p.reason}
                  </span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {hiddenCount > 0 && (
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          aria-expanded={expanded}
          className="mt-3 text-oo-meta font-medium text-oo-blue hover:text-oo-burst underline underline-offset-2"
        >
          {expanded
            ? "Show fewer"
            : `Show ${hiddenCount} more possible duplicate${hiddenCount === 1 ? "" : "s"}`}
        </button>
      )}
    </>
  );
}
