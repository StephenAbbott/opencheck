/**
 * "Companies on the New Zealand register" — one director's or shareholder's
 * other companies, on their BackgroundCheck card. Fetched only when asked for,
 * listed, never drawn as graph nodes; absent from a saved report (the list is
 * read today). The Estonian control beside it has the same shape — the
 * difference is that this register searches by name, so every row carries how
 * far the filed address agrees (`MatchConfidenceChip`).
 *
 * Until 19 Oct 2026 this was QuickCheck's company-wide "Director & shareholder
 * associations" panel on the New Zealand source card.
 */

import { useId, useState } from "react";

import { nzPersonAssociations, type NzPersonAssociationsResponse } from "../../lib/api";
import {
  NZ_ATTRIBUTION,
  NZ_MATCH_CAVEAT,
  nzCompaniesSummary,
  nzCompanyRoles,
  nzTruncatedNote,
  nzWithheldNote,
} from "../../lib/nzPerson";
import { Button } from "../ui";
import MatchConfidenceChip from "../ui/MatchConfidenceChip";
import { useSavedReport } from "./savedReportContext";

export function NzPersonCompanies({
  companyNumber,
  filedName,
  name,
}: {
  companyNumber: string;
  /** The name as the NZ filing gives it — what the register is asked about. */
  filedName: string;
  /** The card's display name, for the control's accessible name. */
  name?: string;
}) {
  const savedReport = useSavedReport();
  const [data, setData] = useState<NzPersonAssociationsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const panelId = useId();

  if (savedReport) return null;

  async function toggle() {
    if (open) {
      setOpen(false);
      return;
    }
    setOpen(true);
    if (data || loading) return;
    setLoading(true);
    setError(null);
    try {
      setData(await nzPersonAssociations(companyNumber, filedName));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  const rows = data?.person?.companies ?? [];
  const truncated = data ? nzTruncatedNote(data) : null;
  const withheld = data ? nzWithheldNote(data) : null;
  return (
    <div className="mt-1.5">
      <Button
        variant="secondary"
        size="sm"
        onClick={toggle}
        aria-expanded={open}
        aria-controls={panelId}
        aria-label={`Companies on the New Zealand register for ${name ?? filedName}`}
      >
        {open ? "Hide companies on the register" : "Companies on the New Zealand register"}
      </Button>
      <div id={panelId} hidden={!open}>
        {loading && (
          <p className="text-oo-meta text-oo-muted mt-1.5" role="status">
            Searching the New Zealand register…
          </p>
        )}
        {error && (
          <p className="text-oo-meta text-oo-warn-text mt-1.5" role="alert">
            The New Zealand register did not answer: {error}
          </p>
        )}
        {data && (
          <div className="mt-1.5 rounded-oo border border-oo-rule bg-oo-bg px-3 py-2">
            <p className="text-oo-meta text-oo-ink mb-1.5">{nzCompaniesSummary(data)}</p>
            {rows.length > 0 && (
              <ul className="space-y-1.5 list-none p-0 m-0">
                {rows.map((c) => (
                  <li key={c.number} className="text-oo-meta leading-[1.5] text-oo-ink">
                    {c.link ? (
                      <a
                        href={c.link}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="font-medium text-oo-blue underline hover:no-underline"
                      >
                        {c.name || `Company ${c.number}`}
                      </a>
                    ) : (
                      <span className="font-medium">{c.name || `Company ${c.number}`}</span>
                    )}
                    <span className="font-mono text-oo-muted"> {c.number}</span>
                    <div className="mt-0.5 flex flex-wrap items-center gap-1.5">
                      <span className="text-oo-muted">{nzCompanyRoles(c)}</span>
                      <MatchConfidenceChip confidence={c.confidence} basis={c.basis} />
                    </div>
                  </li>
                ))}
              </ul>
            )}
            {withheld && <p className="text-oo-meta text-oo-muted mt-1.5">{withheld}</p>}
            {truncated && <p className="text-oo-meta text-oo-muted mt-1.5">{truncated}</p>}
            {data.available && (
              <p className="text-oo-meta text-oo-muted mt-1.5">{NZ_MATCH_CAVEAT}</p>
            )}
            <p className="text-oo-meta text-oo-muted mt-0.5">{NZ_ATTRIBUTION}</p>
          </div>
        )}
      </div>
    </div>
  );
}
