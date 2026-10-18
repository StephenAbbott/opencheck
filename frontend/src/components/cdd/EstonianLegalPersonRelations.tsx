/**
 * "Every company on the Estonian register" — the companies a legal person has
 * held a role in, current, former and deleted, read from RIK's REST service
 * when asked for (the legal-person-relations ticket). Listed, never drawn as
 * graph nodes; not on a saved report, because the list is read today.
 */

import { useId, useState } from "react";

import { legalPersonRelations, type LegalPersonRelationsResponse } from "../../lib/api";
import { registerCompanyUrl } from "../../lib/estonianPerson";
import { companyStatusNote, relationLine, relationsSummary } from "../../lib/legalPersonRelations";
import { Button } from "../ui";
import { useSavedReport } from "./savedReportContext";

export function EstonianLegalPersonRelations({
  code,
  country = "EST",
  name,
  label = "Every company on the Estonian register",
}: {
  code: string;
  country?: string;
  name?: string;
  label?: string;
}) {
  const savedReport = useSavedReport();
  const [data, setData] = useState<LegalPersonRelationsResponse | null>(null);
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
      setData(await legalPersonRelations(code, country));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="mt-1.5">
      <Button
        variant="secondary"
        size="sm"
        onClick={toggle}
        aria-expanded={open}
        aria-controls={panelId}
        aria-label={`${label} for ${name ?? "this company"}`}
      >
        {open ? "Hide the register's list" : label}
      </Button>
      <div id={panelId} hidden={!open}>
        {loading && (
          <p className="text-oo-meta text-oo-muted mt-1.5" role="status">
            Asking the e-Business Register…
          </p>
        )}
        {error && (
          <p className="text-oo-meta text-oo-warn-text mt-1.5" role="alert">
            The register did not answer: {error}. This is not a finding that there are none.
          </p>
        )}
        {data && (
          <div className="mt-1.5 rounded-oo border border-oo-rule bg-oo-bg px-3 py-2">
            <p className="text-oo-meta text-oo-ink mb-1.5">{relationsSummary(data)}</p>
            {data.companies.length > 0 && (
              <ul className="space-y-1.5 list-none p-0 m-0">
                {data.companies.map((c, i) => {
                  const url = c.code ? registerCompanyUrl(c.code) : null;
                  const note = companyStatusNote(c);
                  return (
                    <li key={`${c.code ?? c.name}-${i}`} className="text-oo-meta leading-[1.5] text-oo-ink">
                      {url ? (
                        <a href={url} target="_blank" rel="noopener noreferrer" className="font-medium text-oo-blue underline hover:no-underline">
                          {c.name}
                        </a>
                      ) : (
                        <span className="font-medium">{c.name}</span>
                      )}
                      {c.code && <span className="font-mono text-oo-muted"> {c.code}</span>}
                      {c.legal_form && <span className="text-oo-muted"> · {c.legal_form}</span>}
                      {note && <span className="text-oo-muted"> · {note}</span>}
                      <ul className="list-none p-0 m-0">
                        {c.relations.map((r, j) => (
                          <li key={j} className="text-oo-muted">
                            {relationLine(r)}
                            {r.ended && <span> (ended)</span>}
                          </li>
                        ))}
                      </ul>
                    </li>
                  );
                })}
              </ul>
            )}
            <p className="text-oo-meta text-oo-muted mt-1.5">{data.caveat}</p>
            <p className="text-oo-meta text-oo-muted mt-0.5">{data.attribution}</p>
          </div>
        )}
      </div>
    </div>
  );
}
