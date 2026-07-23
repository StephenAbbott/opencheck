/**
 * ClusterGroup — wraps the PersonCards the clusterer believes MAY be one
 * person into a single review group, without merging them. Cluster UI ticket,
 * Phase 2.
 *
 * Non-invasive: it renders its children (the existing PersonCards, untouched)
 * inside a bordered group with a confidence chip and a per-pair evidence list.
 * Every card inside still screens independently and keeps its own statementIds.
 *
 * Confidence is about IDENTITY LIKELIHOOD, never risk — kept visually distinct
 * from RiskChip AND from the solid-violet action buttons (it is a tinted status
 * chip with a status dot, not a solid pill). High = the system is fairly sure
 * (shared identifier, or a near-exact name corroborated by birth year +
 * nationality/company). Medium = plausible, needs a human (name variant,
 * missing/conflicting birth year).
 */

import type { ReactNode } from "react";
import type { PersonCluster } from "../../lib/clusterPeople";

export function ClusterGroup({
  cluster,
  nameByKey,
  children,
}: {
  cluster: PersonCluster;
  /** Map from person key → display name, for the evidence lines. */
  nameByKey: Record<string, string>;
  /** The member PersonCards, already rendered by the panel. */
  children: ReactNode;
}) {
  const high = cluster.confidence === "high";
  const headingId = `cluster-${cluster.keys
    .join("-")
    .replace(/[^a-z0-9]/gi, "-")}`;
  return (
    <div
      className="rounded-oo border border-violet-300 border-l-[3px] border-l-violet-500 bg-violet-50/40 p-3"
      role="group"
      aria-labelledby={headingId}
    >
      <div className="mb-2 flex items-start justify-between gap-3">
        <p
          id={headingId}
          className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-violet-800"
        >
          Possibly one person — {cluster.size} records · review
        </p>
        <ConfidenceChip confidence={cluster.confidence} />
      </div>

      <ul className="mb-3 list-none p-0 m-0 space-y-1">
        {cluster.pairs.map((pair, i) => (
          <li
            key={i}
            className="text-[11px] leading-[1.6] text-violet-900 rounded-oo border border-violet-200 bg-white px-2.5 py-1.5"
          >
            <span className="font-medium">{nameByKey[pair.aKey] ?? "record"}</span>
            <span className="text-violet-500" aria-hidden="true">
              {" ↔ "}
            </span>
            <span className="sr-only"> and </span>
            <span className="font-medium">{nameByKey[pair.bKey] ?? "record"}</span>
            {" — "}
            {pair.evidence}
          </li>
        ))}
      </ul>

      <div className="space-y-4">{children}</div>

      <p className="mt-2 text-[11px] leading-[1.5] text-violet-800">
        {high
          ? "Grouped as a strong candidate — verify before treating as one individual. Each record is still listed and screened separately, not combined."
          : "Grouped for review — the evidence is suggestive, not conclusive. Each record is still listed and screened separately, not combined."}
      </p>
    </div>
  );
}

function ConfidenceChip({ confidence }: { confidence: "high" | "medium" }) {
  const high = confidence === "high";
  return (
    <span
      className={
        "shrink-0 inline-flex items-center gap-1 rounded-oo border px-2 py-0.5 text-[10px] font-semibold tracking-oo-eyebrow uppercase " +
        (high
          ? "border-violet-400 bg-violet-100 text-violet-900"
          : "border-amber-300 bg-amber-50 text-amber-800")
      }
    >
      <span
        aria-hidden="true"
        className={
          "h-1.5 w-1.5 rounded-full " + (high ? "bg-violet-600" : "bg-amber-500")
        }
      />
      {high ? "High confidence" : "Medium · review"}
    </span>
  );
}
