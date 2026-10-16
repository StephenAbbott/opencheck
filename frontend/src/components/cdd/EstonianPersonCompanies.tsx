/**
 * "Companies on the Estonian register" — one person's positions across every
 * company whose current lists name them, keyed on the register's pseudonymous
 * person key (`isikukood_hash`). Fetched only when asked for, listed, never
 * drawn as graph nodes (Stephen, 16 Oct 2026). Not on a saved report: the
 * list is read today, and a saved report shows what was saved.
 */

import { useId, useState } from "react";

import { estonianPersonCompanies, type PersonAppointmentsResponse } from "../../lib/api";
import {
  REGISTER_LIST_LABEL,
  companiesSummary,
  groupByCompany,
  positionLine,
  registerCompanyUrl,
} from "../../lib/estonianPerson";
import { Button } from "../ui";
import { useSavedReport } from "./savedReportContext";

export function EstonianPersonCompanies({ personHash, name }: { personHash: string; name?: string }) {
  const savedReport = useSavedReport();
  const [data, setData] = useState<PersonAppointmentsResponse | null>(null);
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
      setData(await estonianPersonCompanies(personHash));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  const companies = data ? groupByCompany(data.appointments) : [];
  return (
    <div className="mt-1.5">
      <Button
        variant="secondary"
        size="sm"
        onClick={toggle}
        aria-expanded={open}
        aria-controls={panelId}
        aria-label={`Companies on the Estonian register for ${name ?? "this person"}`}
      >
        {open ? "Hide companies on the register" : "Companies on the Estonian register"}
      </Button>
      <div id={panelId} hidden={!open}>
        {loading && <p className="text-oo-meta text-oo-muted mt-1.5" role="status">Reading the register's index…</p>}
        {error && (
          <p className="text-oo-meta text-oo-warn-text mt-1.5" role="alert">
            The register's index did not answer: {error}
          </p>
        )}
        {data && (
          <div className="mt-1.5 rounded-oo border border-oo-rule bg-oo-bg px-3 py-2">
            <p className="text-oo-meta text-oo-ink mb-1.5">{companiesSummary(data)}</p>
            {companies.length > 0 && (
              <ul className="space-y-1.5 list-none p-0 m-0">
                {companies.map((c) => {
                  const url = registerCompanyUrl(c.code);
                  return (
                    <li key={c.code} className="text-oo-meta leading-[1.5] text-oo-ink">
                      {url ? (
                        <a href={url} target="_blank" rel="noopener noreferrer" className="font-medium text-oo-blue underline hover:no-underline">
                          {c.name}
                        </a>
                      ) : (
                        <span className="font-medium">{c.name}</span>
                      )}
                      <span className="font-mono text-oo-muted"> {c.code}</span>
                      {c.status && c.status !== "Entered in the register" && (
                        <span className="text-oo-muted"> · {c.status}</span>
                      )}
                      <ul className="list-none p-0 m-0">
                        {c.positions.map((p, i) => (
                          <li key={i} className="text-oo-muted">
                            {positionLine(p)}
                            {p.register_list && (
                              <span> ({REGISTER_LIST_LABEL[p.register_list] ?? p.register_list})</span>
                            )}
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
