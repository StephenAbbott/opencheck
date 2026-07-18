/**
 * FullCheckPanel — the FullCheck (enhanced due diligence) view.
 *
 * QuickCheck screens the subject; FullCheck maps the wider corporate network
 * connected to it. This panel fetches the subject's merged BODS (one /lookup,
 * replay-cached) and renders a single **unified** network graph — distinct from
 * QuickCheck's per-source panels — with the "Run FullCheck" control that eagerly
 * expands owners/controllers to a depth budget (Phase 1). Network-wide risk
 * surfacing + the QuickCheck-vs-FullCheck comparison come in Phase 2.
 */

import { useEffect, useState } from "react";
import { lookup, type RiskSignal } from "../../lib/api";
import BodsGraphExplorer from "../BodsGraphExplorer";
import { SubsidiaryNetwork } from "./SubsidiaryNetwork";

type Stmt = Record<string, unknown>;

export default function FullCheckPanel({
  lei,
  legalName,
  signals = [],
}: {
  lei: string;
  legalName: string | null;
  signals?: RiskSignal[];
}) {
  const [statements, setStatements] = useState<Stmt[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setStatements(null);
    setError(null);
    lookup(lei)
      .then((r) => {
        if (!cancelled) setStatements(r.bods as Stmt[]);
      })
      .catch((e) => {
        if (!cancelled) setError((e as Error).message);
      });
    return () => {
      cancelled = true;
    };
  }, [lei]);

  return (
    <section className="mb-8" aria-label="FullCheck — enhanced due diligence">
      <div className="mb-3 rounded-oo border border-[#1565c0] bg-[#eef4fe] px-4 py-3">
        <h3 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-blue mb-1">
          FullCheck · Enhanced due diligence
        </h3>
        <p className="text-[13px] text-oo-ink leading-[1.6]">
          The wider corporate network connected to{" "}
          <span className="font-medium">{legalName ?? lei}</span>. Run FullCheck to
          expand owners and controllers layer by layer, then explore the whole
          network in one graph.
        </p>
      </div>

      {error && (
        <p role="alert" className="text-[13px] text-red-700 bg-red-50 border border-red-200 rounded-oo px-3 py-2">
          Couldn't load the network: {error}
        </p>
      )}
      {!statements && !error && (
        <p role="status" className="text-[13px] text-oo-muted italic">Loading the network…</p>
      )}
      {statements && (
        <div className="bg-white border border-oo-rule rounded-oo p-4">
          <BodsGraphExplorer
            statements={statements}
            signals={signals}
            entityName={legalName ?? undefined}
            direction="owners"
            fullCheck
          />
        </div>
      )}

      <div className="mt-4">
        <SubsidiaryNetwork lei={lei} entityName={legalName ?? undefined} />
      </div>
    </section>
  );
}
