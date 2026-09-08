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
 *
 * Phase 185 took the "Subsidiary network" band to its own tab. FullCheck
 * follows the chain upwards — who owns this company — and the band that
 * followed it downwards sat behind a strip at the bottom where the tab could
 * not claim it and no URL could reach it. One line here says where it went.
 */

import { useEffect, useState } from "react";
import { lookup, type RiskSignal } from "../../lib/api";
import { subsidiaryHref } from "../../lib/subsidiariesMode";
import BodsGraphExplorer from "../BodsGraphExplorer";
import PanelSection from "../ui/PanelSection";
import type { PanelError, PanelId } from "../../lib/panelErrors";

type Stmt = Record<string, unknown>;

export default function FullCheckPanel({
  lei,
  legalName,
  signals = [],
  onOpenSubsidiaries,
}: {
  lei: string;
  legalName: string | null;
  signals?: RiskSignal[];
  /** Switch to the Subsidiaries tab in place. The pointer is a real link to
   *  `?mode=subsidiaries` so it survives a right-click; the handler makes a
   *  plain click a tab switch rather than a reload. */
  onOpenSubsidiaries?: () => void;
  /** Kept on the signature so App's call site is unchanged; the band that
   *  used them moved to the Subsidiaries tab in Phase 185. */
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
        // Only once there is a graph. "Everything here is in the table too"
        // printed beside "Couldn't load the network" describes something that
        // is not on the page.
        aside={
          statements
            ? "Drag to move, scroll to zoom — everything here is in the table too"
            : undefined
        }
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
          <>
            <p className="mb-3 text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">
              The wider corporate network connected to{" "}
              <span className="font-medium text-oo-ink">{legalName ?? lei}</span>.
              Run FullCheck to expand owners and controllers layer by layer.
            </p>
            <BodsGraphExplorer
              statements={statements}
              signals={signals}
              entityName={legalName ?? undefined}
              direction="owners"
              fullCheck
            />
          </>
        )}
      </PanelSection>

      <PanelSection title="Subsidiaries">
        <p className="text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">
          FullCheck follows the chain upwards. What this company owns — GLEIF&rsquo;s
          Level 2 network and every other list OpenCheck holds — has its own tab:{" "}
          <a
            href={subsidiaryHref(lei)}
            onClick={(e) => {
              if (!onOpenSubsidiaries) return;
              e.preventDefault();
              onOpenSubsidiaries();
            }}
            className="text-oo-blue hover:underline"
          >
            open Subsidiaries
          </a>
          .
        </p>
      </PanelSection>
    </>
  );
}
