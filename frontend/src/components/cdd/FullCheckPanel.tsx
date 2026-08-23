/**
 * FullCheckPanel — the FullCheck (enhanced due diligence) view.
 *
 * QuickCheck screens the subject; FullCheck maps the wider corporate network
 * connected to it. This panel fetches the subject's merged BODS (one /lookup,
 * replay-cached) and renders a single **unified** network graph — distinct from
 * QuickCheck's per-source panels — with the "Run FullCheck" control that eagerly
 * expands owners/controllers to a depth budget.
 *
 * **It renders bands, not cards** (Phase 128). The tabpanel above it owns the
 * `PanelCard`, exactly as QuickCheck's does, so the tab strip claims what is
 * beneath it. Before that this panel drew three detached objects on the grey
 * page — a blue blurb strip, a white graph card, a grey subsidiary card — and
 * the tab connected to none of them.
 *
 * The blurb strip is gone rather than restyled: it said in four lines what
 * `MODE_TABS[1].blurb` says in one, and the tabpanel now renders that blurb as
 * the card's first band, the same way every mode does. A panel that
 * reintroduces its own title is a card inside a card again.
 */

import { useEffect, useState } from "react";
import { lookup, type RiskSignal } from "../../lib/api";
import BodsGraphExplorer from "../BodsGraphExplorer";
import PanelSection from "../ui/PanelSection";
import { SubsidiaryNetwork } from "./SubsidiaryNetwork";
import type { PanelError, PanelId } from "../../lib/panelErrors";

type Stmt = Record<string, unknown>;

export default function FullCheckPanel({
  lei,
  legalName,
  signals = [],
  onPanelError,
  onPanelRecovered,
}: {
  lei: string;
  legalName: string | null;
  signals?: RiskSignal[];
  /** Forwarded to SubsidiaryNetwork so a /subsidiaries failure reaches the
   *  report-level notice — this panel is mounted inside a tab, so nothing
   *  above it would otherwise learn that the fetch failed. */
  onPanelError?: (e: PanelError) => void;
  onPanelRecovered?: (panel: PanelId) => void;
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
    <>
      <PanelSection
        title="Ownership network"
        aside="Drag to move, scroll to zoom — everything here is in the table too"
      >
        {error && (
          <p
            role="alert"
            className="text-oo-small text-oo-warn-text bg-oo-warn-bg border border-oo-warn-border rounded-oo px-3 py-2"
          >
            Couldn&rsquo;t load the network: {error}
          </p>
        )}
        {!statements && !error && (
          <p role="status" className="text-oo-small text-oo-muted italic">
            Loading the network…
          </p>
        )}
        {statements && (
          <BodsGraphExplorer
            statements={statements}
            signals={signals}
            entityName={legalName ?? undefined}
            direction="owners"
            fullCheck
          />
        )}
      </PanelSection>

      <PanelSection title="Subsidiary network">
        <SubsidiaryNetwork
          lei={lei}
          entityName={legalName ?? undefined}
          signals={signals}
          onError={onPanelError}
          onRecovered={onPanelRecovered}
          bare
        />
      </PanelSection>
    </>
  );
}
