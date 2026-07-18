/**
 * BodsRelationshipTable — a text-equivalent view of the ownership graph.
 *
 * The Cytoscape canvas in BODSGraph is `role="img"`: screen-reader users
 * perceive only its accessible name, and nodes/edges are pointer-only. This
 * component renders the SAME GraphModel as a real HTML table — one row per
 * relationship (Interested party | Interest | Subject, mirroring the PDF
 * export) — so every mount of the graph has an always-available fallback
 * (WCAG 2.1 SC 1.1.1 / 1.3.1 / 2.1.1). It is static content: no handlers,
 * nothing to focus, nothing pointer-only.
 */

import type { GraphModel, GraphNode } from "../lib/bodsGraph";
import type { RiskSignal } from "../lib/api";
import { RISK_PRESENTATION } from "./risk/RiskChip";

/** "person" | "entity" from a BODS record/statement type. */
function kindLabel(recordType: string): string {
  return recordType === "person" || recordType === "personStatement" ? "person" : "entity";
}

/** Country code as TEXT, derived from the flag URL ("/flags/gb.svg" → "GB") —
 *  the GraphModel carries only the flag image URL, whose filename is the
 *  lowercased jurisdiction code (see flagUrl() in lib/bodsGraph.ts). */
export function jurisdictionCodeFromFlagUrl(flagUrl?: string): string | null {
  const m = flagUrl?.match(/\/([a-z0-9]{2,3})\.svg$/i);
  return m ? m[1].toUpperCase() : null;
}

/** Human label for a risk-signal code (falls back to the code itself). */
function signalLabel(code: string): string {
  return RISK_PRESENTATION[code]?.label ?? code.replace(/_/g, " ");
}

/** Party cell contents: name + kind + jurisdiction code + risk-signal labels. */
function PartyCell({
  node,
  signals,
}: {
  node: GraphNode | undefined;
  signals?: RiskSignal[];
}) {
  if (!node) return <span className="text-oo-muted italic">Unknown party</span>;
  const code = jurisdictionCodeFromFlagUrl(node.flagUrl);
  const sigLabels = [...new Set((signals ?? []).map((s) => signalLabel(s.code)))];
  return (
    <span className="inline-flex items-baseline gap-1.5 flex-wrap">
      <span className="text-oo-ink">{node.label}</span>
      <span className="text-[10px] text-oo-muted uppercase tracking-wide">
        {kindLabel(node.recordType)}
      </span>
      {code && <span className="text-[10px] text-oo-muted">{code}</span>}
      {sigLabels.map((l) => (
        <span
          key={l}
          className="text-[10px] font-medium text-oo-ink border border-oo-rule rounded-full px-1.5"
        >
          {l}
        </span>
      ))}
    </span>
  );
}

export default function BodsRelationshipTable({
  model,
  signalsByNode,
  entityName,
}: {
  /** The SAME model BODSGraph renders (built by lib/bodsGraph.ts bodsToGraph). */
  model: GraphModel;
  /** Node id → risk signals scoped to that node (buildSignalMap in BODSGraph). */
  signalsByNode?: Map<string, RiskSignal[]>;
  entityName?: string;
}) {
  if (model.nodes.length === 0) {
    return (
      <p className="text-xs text-oo-muted italic">
        No ownership or control relationships reported; entity record only.
      </p>
    );
  }

  const byId = new Map(model.nodes.map((n) => [n.id, n] as const));
  const connected = new Set<string>();
  for (const e of model.edges) {
    connected.add(e.source);
    connected.add(e.target);
  }
  const isolated = model.nodes.filter((n) => !connected.has(n.id));

  return (
    <div className="text-xs">
      {model.edges.length > 0 ? (
        <table className="w-full border-collapse">
          <caption className="text-left text-[11px] text-oo-muted pb-1.5">
            Ownership and control relationships{entityName ? ` for ${entityName}` : ""} —{" "}
            {model.edges.length} relationship{model.edges.length === 1 ? "" : "s"} across{" "}
            {model.nodes.length} part{model.nodes.length === 1 ? "y" : "ies"}
          </caption>
          <thead>
            <tr className="border-b border-oo-rule text-left">
              <th scope="col" className="py-1 pr-3 font-semibold text-oo-ink">Interested party</th>
              <th scope="col" className="py-1 pr-3 font-semibold text-oo-ink">Interest</th>
              <th scope="col" className="py-1 font-semibold text-oo-ink">Subject</th>
            </tr>
          </thead>
          <tbody>
            {model.edges.map((e) => (
              <tr key={e.id} className="border-b border-oo-rule/60 align-top">
                <td className="py-1 pr-3">
                  <PartyCell node={byId.get(e.source)} signals={signalsByNode?.get(e.source)} />
                </td>
                <td className="py-1 pr-3">
                  {e.label ? (
                    <span className="text-oo-ink">{e.label.split("\n").join("; ")}</span>
                  ) : (
                    <span className="text-oo-muted">{e.category}</span>
                  )}
                </td>
                <td className="py-1">
                  <PartyCell node={byId.get(e.target)} signals={signalsByNode?.get(e.target)} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-xs text-oo-muted italic">
          No ownership or control relationships reported; entity record only.
        </p>
      )}

      {isolated.length > 0 && (
        <p className="pt-2 text-[11px] text-oo-muted">
          <span className="font-semibold">Parties with no reported relationships:</span>{" "}
          {isolated.map((n, i) => {
            const code = jurisdictionCodeFromFlagUrl(n.flagUrl);
            return (
              <span key={n.id}>
                {i > 0 && ", "}
                <span className="text-oo-ink">{n.label}</span> ({kindLabel(n.recordType)}
                {code ? `, ${code}` : ""})
              </span>
            );
          })}
        </p>
      )}
    </div>
  );
}
