import type { CrossSourceLink } from "../../lib/api";
import { scrollBehavior } from "../../lib/motion";
import { sourceLabel } from "../../lib/vocab";

/*
 * The identifier table in the report's "Is this the right company?" band.
 * Moved out of App.tsx in Phase 246, unchanged.
 */

/** Human-readable label for each reconcile bridge key. */
const SCHEME_LABELS: Record<string, string> = {
  lei: "Legal Entity Identifier (LEI)",
  wikidata_qid: "Wikidata QID",
  gb_coh: "Companies House number",
  opensanctions_id: "OpenSanctions ID",
  name: "Name match",
};

/** Short display name for a source chip — the lead clause of the registry
 *  name ("EITI — Extractive Industries…" → "EITI"). */
export function shortSourceName(sourceId: string, names: Record<string, string>): string {
  const full = names[sourceId] ?? sourceId;
  return full.split(" — ")[0].split(" (")[0].trim();
}

export function CrossSourceIdentifiersTable({
  links,
  gleifMapped,
  sourceNames = {},
}: {
  links: CrossSourceLink[];
  gleifMapped: { scheme: string; value: string }[];
  sourceNames?: Record<string, string>;
}) {
  const hasRows = links.length > 0 || gleifMapped.length > 0;
  if (!hasRows) return null;

  return (
    <table className="w-full text-oo-small border-collapse table-fixed">
      <thead>
        <tr>
          <th className="text-left text-[10px] font-medium tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[32%]">
            Scheme
          </th>
          <th className="text-left text-[10px] font-medium tracking-widest uppercase text-oo-muted pb-2 pr-3 w-[32%]">
            Value
          </th>
          <th className="text-right text-[10px] font-medium tracking-widest uppercase text-oo-muted pb-2 w-[36%]">
            Confirmed by
          </th>
        </tr>
      </thead>
      <tbody>
        {links.map((link, i) => (
          <tr key={`${link.key}:${link.key_value}:${i}`} className="border-t border-oo-rule">
            <td className="py-2 pr-3 text-oo-muted">
              {SCHEME_LABELS[link.key] ?? link.key}
            </td>
            <td className="py-2 pr-3 font-mono text-oo-meta text-oo-ink break-all">
              {link.key_value}
            </td>
            <td className="py-2 text-right">
              <span className="inline-flex flex-wrap gap-1 justify-end">
                {link.hits.map((h) => (
                  <button
                    key={h.source_id}
                    type="button"
                    aria-label={`${sourceLabel(h.source_id, sourceNames)} — jump to this source's results`}
                    onClick={() =>
                      document
                        .getElementById(`source-${h.source_id}`)
                        ?.scrollIntoView({ behavior: scrollBehavior(), block: "start" })
                    }
                    className="text-[11px] bg-oo-bg border border-oo-rule rounded px-1.5 py-0.5 text-oo-muted hover:text-oo-ink hover:border-oo-softBorder transition-colors"
                  >
                    {shortSourceName(h.source_id, sourceNames)}
                  </button>
                ))}
              </span>
            </td>
          </tr>
        ))}
        {gleifMapped.map(({ scheme, value }) => (
          <tr key={scheme} className="border-t border-oo-rule">
            <td className="py-2 pr-3 text-oo-muted">{scheme}</td>
            <td className="py-2 pr-3 font-mono text-oo-meta text-oo-ink break-all">{value}</td>
            <td className="py-2 text-right">
              <span className="inline-flex items-center gap-1 text-[11px] bg-blue-50 border border-blue-200 text-blue-700 rounded px-1.5 py-0.5">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="w-3 h-3"
                  aria-hidden="true"
                >
                  <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
                  <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
                </svg>
                Mapped by GLEIF
              </span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
