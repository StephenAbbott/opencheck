/**
 * IntegratedOwnershipList — the FullCheck "Integrated ownership" band
 * (Phase 339).
 *
 * Every party above the subject, person or company, with how much of the
 * subject it holds directly and through the companies it holds — one figure
 * per source, side by side, never added (`lib/integratedOwnership.ts`). The
 * figure is context: no threshold, no highlighting, nothing here feeds a risk
 * signal or the verdict. The method is named in plain words beneath the
 * table; the credit to T-Rank is in the annotation and the docs.
 *
 * Live, the panel recomputes the payload as the network expands. On a saved
 * report it shows the payload frozen with the run, and reads no clock.
 */

import type { Holder, IntegratedOwnershipPayload } from "../../lib/integratedOwnership";
import { METHOD_DESCRIPTION, routeWords } from "../../lib/integratedOwnership";

const TH = "text-left text-oo-meta font-medium tracking-wide uppercase text-oo-muted pb-2 pr-3";

function rowsOf(h: Holder): { figure: string; source: string; route: string }[] {
  if (h.status !== "current") return [{ figure: h.note ?? "", source: h.sources.join("; "), route: "" }];
  return h.figures.map((f) => ({ figure: f.text, source: f.source, route: routeWords(f) }));
}

export function IntegratedOwnershipList({
  payload,
  expanding = false,
  readOnly = false,
}: {
  payload: IntegratedOwnershipPayload | null;
  expanding?: boolean;
  readOnly?: boolean;
}) {
  const holders = payload?.holders ?? [];
  return (
    <div data-testid="integrated-ownership">
      {holders.length === 0 ? (
        <p className="text-oo-small text-oo-muted">
          No shareholding is stated on any path to this company in the network as drawn.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-oo-small border-collapse">
            <caption className="sr-only">Integrated ownership of the company, by holder and source</caption>
            <thead>
              <tr>
                <th scope="col" className={TH}>Holder</th>
                <th scope="col" className={TH}>Integrated ownership</th>
                <th scope="col" className={TH}>Source</th>
                <th scope="col" className={`${TH} pr-0`}>Route</th>
              </tr>
            </thead>
            <tbody>
              {holders.flatMap((h) =>
                rowsOf(h).map((r, i) => (
                  <tr key={`${h.party}:${i}`} className="border-t border-oo-rule align-top">
                    <td className="py-2 pr-3 text-oo-ink">
                      {i === 0 ? (
                        <>
                          {h.name}
                          {h.kind === "person" && <span className="text-oo-muted"> · person</span>}
                        </>
                      ) : (
                        <span className="sr-only">{h.name}</span>
                      )}
                    </td>
                    <td className={`py-2 pr-3 ${h.status === "current" ? "text-oo-ink tabular-nums" : "text-oo-muted italic"}`}>
                      {r.figure}
                    </td>
                    <td className="py-2 pr-3 text-oo-muted">{r.source}</td>
                    <td className="py-2 text-oo-muted">{r.route}</td>
                  </tr>
                )),
              )}
            </tbody>
          </table>
        </div>
      )}
      <p className="mt-3 text-oo-meta text-oo-muted leading-[1.6] max-w-[82ch]">
        {payload?.description ?? METHOD_DESCRIPTION}
        {payload?.as_of && readOnly ? ` Computed from the sources as they stood on ${payload.as_of}.` : ""}
      </p>
      {expanding && (
        <p role="status" className="mt-1 text-oo-meta text-oo-muted italic">
          Updating as the network expands…
        </p>
      )}
    </div>
  );
}
