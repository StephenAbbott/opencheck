/**
 * EstmaCard — payments to governments reported under Canada's Extractive
 * Sector Transparency Measures Act, on the Climate & ESG tab.
 *
 * Blocks, in the order a reader needs them: the latest report (total of its
 * payee lines, by payment type, by country, the largest payees and
 * projects, a link to the report itself); earlier years, one line each;
 * the entities whose payments this one's report includes.
 *
 * ## What this card must never do
 *
 * **Add the payee and project views together.** They are the same payments.
 * The total is the payee lines' sum and is labelled as OpenCheck's sum.
 *
 * **Add two reports together**, even for one year.
 *
 * **Say an entity paid nothing** because its payments are filed in another
 * entity's report, or read that arrangement as ownership.
 *
 * **Present the LEI as ESTMA's.** The file carries only the ESTMA ID; the
 * LEI is a reviewed name match, graded on `MatchConfidenceChip`.
 */
import type { SourceHit } from "../../lib/api";
import {
  formatCad,
  ownReports,
  periodLine,
  type EstmaBundle,
  type EstmaEntity,
  type EstmaLine,
  type EstmaReport,
} from "../../lib/estma";
import MatchConfidenceChip from "../ui/MatchConfidenceChip";
import { ESG_SOURCE_META } from "./esgSources";

function Eyebrow({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-oo-esg-text mb-1.5">
      {children}
    </div>
  );
}

function ExternalLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noreferrer" className="underline underline-offset-2 hover:text-oo-esg-strong">
      {children}
      <span className="sr-only"> (opens in new tab)</span>
    </a>
  );
}

function Lines({ label, lines, count }: { label: string; lines: EstmaLine[]; count: number }) {
  if (lines.length === 0) return null;
  return (
    <div>
      <div className="text-oo-meta font-semibold text-oo-esg-text">{label}</div>
      <ul className="mt-0.5 space-y-0.5">
        {lines.map((l, i) => (
          <li key={`${l.name}:${l.country_code}:${i}`} className="flex items-baseline justify-between gap-3 text-oo-meta text-oo-esg-strong">
            <span>
              {l.name}
              {l.country_name ? <span className="text-oo-esg-text"> · {l.country_name}</span> : null}
            </span>
            <span className="font-mono">{formatCad(l.total_cad)}</span>
          </li>
        ))}
      </ul>
      {count > lines.length && (
        <p className="text-oo-meta text-oo-esg-text">
          {lines.length} of {count} shown; the report has the rest.
        </p>
      )}
    </div>
  );
}

function LatestReport({ report }: { report: EstmaReport }) {
  const period = periodLine(report);
  return (
    <section aria-label={`ESTMA report for ${report.year}`} className="space-y-2">
      <div className="flex items-baseline justify-between gap-3">
        <span className="text-oo-small font-semibold text-oo-esg-strong">
          {report.year}
          {report.report_type_label ? (
            <span className="font-normal text-oo-esg-text"> · {report.report_type_label}</span>
          ) : null}
        </span>
        <span className="text-oo-small font-mono text-oo-esg-strong">{formatCad(report.payee_total_cad ?? 0)}</span>
      </div>
      <p className="text-oo-meta text-oo-esg-text">
        {period ? `Reporting period ${period}. ` : ""}
        Total is OpenCheck's sum of the {report.payee_count ?? 0} payee line
        {report.payee_count === 1 ? "" : "s"}; the same payments are also filed by project, shown below and not
        added.{" "}
        {report.url ? <ExternalLink href={report.url}>Read the report</ExternalLink> : null}
      </p>
      {(report.by_payment_type ?? []).length > 0 && (
        <div className="text-oo-meta text-oo-esg-strong">
          {(report.by_payment_type ?? []).map((t) => `${t.label ?? t.code} ${formatCad(t.cad)}`).join(" · ")}
        </div>
      )}
      {(report.by_country ?? []).length > 0 && (
        <div className="text-oo-meta text-oo-esg-text">
          {(report.by_country ?? [])
            .map((c) => `${c.name ?? c.code ?? "Country not stated"} ${formatCad(c.cad)}`)
            .join(" · ")}
        </div>
      )}
      <Lines label="Largest payees" lines={report.payees ?? []} count={report.payee_count ?? 0} />
      <Lines label="By project (the same payments)" lines={report.projects ?? []} count={report.project_count ?? 0} />
    </section>
  );
}

function EntityBlock({ entity }: { entity: EstmaEntity }) {
  const own = ownReports(entity);
  const latestYear = entity.reports[0]?.year;
  const latest = own.filter((r) => r.year === latestYear);
  const earlier = own.filter((r) => r.year !== latestYear);
  const included = entity.reports.filter((r) => r.included_in);
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-oo-small font-semibold text-oo-esg-strong">{entity.name}</span>
        <span className="text-oo-meta font-mono text-oo-esg-text">ESTMA {entity.estma_id}</span>
      </div>

      {latest.length > 1 && (
        <p className="text-oo-meta text-oo-esg-text">
          {latest.length} reports were filed for {latestYear} (a full year and a stub period, or a refiled report).
          They are shown apart and never added together.
        </p>
      )}
      {latest.map((r) => (
        <LatestReport key={`${r.year}:${r.url}:${r.report_type}:${r.period_end}`} report={r} />
      ))}

      {earlier.length > 0 && (
        <section aria-label="Earlier ESTMA reports">
          <Eyebrow>Earlier reports</Eyebrow>
          <ul className="space-y-1">
            {earlier.map((r) => (
              <li
                key={`${r.year}:${r.url}:${r.report_type}:${r.period_end}`}
                className="flex items-baseline justify-between gap-3 text-oo-small text-oo-esg-strong"
              >
                <span>
                  <span className="font-semibold">{r.year}</span>
                  {r.report_type_label ? <span className="text-oo-esg-text"> · {r.report_type_label}</span> : null}
                  {r.url ? (
                    <span className="text-oo-meta">
                      {" · "}
                      <ExternalLink href={r.url}>report</ExternalLink>
                    </span>
                  ) : null}
                </span>
                <span className="font-mono">{formatCad(r.payee_total_cad ?? 0)}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {included.length > 0 && (
        <section aria-label="Payments filed in another entity's report">
          <Eyebrow>Filed in another entity's report</Eyebrow>
          <ul className="space-y-1">
            {included.map((r) => (
              <li key={`${r.year}:${r.url}:${r.included_in?.estma_id}`} className="text-oo-small text-oo-esg-strong">
                <span className="font-semibold">{r.year}</span> · in the report of{" "}
                {r.included_in?.name ?? r.included_in?.estma_id}
                {r.url ? (
                  <span className="text-oo-meta">
                    {" · "}
                    <ExternalLink href={r.url}>report</ExternalLink>
                  </span>
                ) : null}
              </li>
            ))}
          </ul>
          <p className="mt-1 text-oo-meta text-oo-esg-text">
            This entity's payments are included in that report, not separated out. It is a reporting arrangement
            under the Act, not a statement of ownership.
          </p>
        </section>
      )}

      {entity.includes && entity.includes.entities.length > 0 && (
        <section aria-label="Entities included in this report">
          <Eyebrow>Its {entity.includes.year} report also covers</Eyebrow>
          <p className="text-oo-small text-oo-esg-strong">
            {entity.includes.entities.map((e) => e.name).join(", ")}
          </p>
          <p className="mt-1 text-oo-meta text-oo-esg-text">
            Entities that file their payments in this report — a reporting arrangement, not a list of subsidiaries.
          </p>
        </section>
      )}
    </div>
  );
}

export function EstmaCard({ hit }: { hit: SourceHit }) {
  const raw = hit.raw as unknown as EstmaBundle;
  const meta = ESG_SOURCE_META.estma;
  const entities = raw.entities ?? [];
  const first = entities[0];
  const gleifName = (raw.gleif_legal_name || hit.name || "").trim();

  return (
    <div className="rounded-oo border border-oo-esg-border bg-oo-esg-bg/40 overflow-hidden">
      <div className="px-5 pt-4 pb-4 space-y-4">
        <div>
          <div className="mb-2 -mt-0.5 flex items-center gap-1.5">
            <span className="inline-block w-1.5 h-1.5 rounded-full bg-oo-esg-accent" />
            <span className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-oo-esg-text">
              Data from <ExternalLink href={raw.dataset_page || meta.href}>{meta.org}</ExternalLink> ·{" "}
              <ExternalLink href={raw.licence_url || "https://www.statcan.gc.ca/en/reference/licence"}>
                {meta.licence}
              </ExternalLink>
            </span>
          </div>
          <h3 className="font-head font-bold text-oo-lead text-oo-esg-strong leading-snug">
            Payments to governments (ESTMA)
          </h3>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            <MatchConfidenceChip confidence="medium" basis={first?.match.basis} />
          </div>
        </div>

        <p className="-mt-2 text-oo-meta text-oo-esg-text">
          The ESTMA data carries no LEI or business number. OpenCheck linked it to {gleifName || "this LEI"} through a
          name crosswalk reviewed by hand.
        </p>

        {entities.length > 1 && (
          <p className="text-oo-meta text-oo-esg-text">
            ESTMA holds this company under {entities.length} entity IDs (for instance before and after a change of
            name). Each is shown with its own reports.
          </p>
        )}

        {entities.map((e) => (
          <EntityBlock key={e.estma_id} entity={e} />
        ))}

        <p className="text-oo-meta text-oo-esg-text">
          Source: Statistics Canada, Extractive Sector Transparency Measures Act data, collected by Natural Resources
          Canada{raw.cut ? `, published ${raw.cut}` : ""}. Amounts are in Canadian dollars as reported. A company's own
          disclosure under Canadian law — not a beneficial ownership or sanctions check.
        </p>
      </div>
    </div>
  );
}
