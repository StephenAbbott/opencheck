/**
 * EitiZambiaCard — the Zambia EITI data portal, on the Climate & ESG tab.
 *
 * Blocks, in the order a reader needs them: a WARMA water-permit offence when
 * there is one; ZRA tax receipts by year; the payments the company reported
 * for EITI reconciliation; employment; mining rights.
 *
 * ## What this card must never do
 *
 * **Add tables together.** ZRA receipts are one table per payment year (the
 * portal's tables overlap), and the reconciliation report is the company's own
 * disclosure, shown apart from ZRA's receipts and never reconciled with them.
 *
 * **Show a total it does not have.** A year whose table publishes no usable
 * amounts shows a payment count and says the amounts are not published.
 *
 * **Present the LEI as the portal's.** The portal keys on the ZRA TPIN and
 * GLEIF on the PACRA number; the LEI is a name match, graded on
 * `MatchConfidenceChip` with its basis.
 *
 * **Read a licence share as a company share.** "(100%)" is the holder's share
 * of a mining right, not of the company.
 */
import type { SourceHit } from "../../lib/api";
import {
  datasetName,
  formatUsd,
  formatZmw,
  matchBasis,
  womenShare,
  type EitiZambiaBundle,
} from "../../lib/eitiZambia";
import MatchConfidenceChip from "../ui/MatchConfidenceChip";
import { ESG_SOURCE_META } from "./esgSources";

function Eyebrow({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-oo-esg-text mb-1.5">
      {children}
    </div>
  );
}

function DatasetLink({ bundle, code }: { bundle: EitiZambiaBundle; code: string }) {
  const url = bundle.datasets?.[code]?.url;
  const name = datasetName(bundle, code);
  if (!url) return <span>{name}</span>;
  return (
    <a href={url} target="_blank" rel="noreferrer" className="underline underline-offset-2 hover:text-oo-esg-strong">
      {name}
      <span className="sr-only"> (opens in new tab)</span>
    </a>
  );
}

export function EitiZambiaCard({ hit }: { hit: SourceHit }) {
  const raw = hit.raw as unknown as EitiZambiaBundle;
  const meta = ESG_SOURCE_META.eiti_zambia;
  const tpin = raw.tpins?.[0];
  const gleifName = (raw.gleif_legal_name || hit.name || "").trim();
  const otherNames = (raw.names_as_filed ?? []).filter(
    (n) => n.trim().toUpperCase() !== gleifName.toUpperCase(),
  );
  const offences = raw.water_offences ?? [];
  const tax = raw.zra_tax ?? [];
  const recon = raw.eiti_reconciliation ?? [];
  const employment = raw.employment ?? [];
  const licences = raw.licences ?? [];

  return (
    <div className="rounded-oo border border-oo-esg-border bg-oo-esg-bg/40 overflow-hidden">
      <div className="px-5 pt-4 pb-4 space-y-4">
        <div>
          <div className="mb-2 -mt-0.5 flex items-center gap-1.5">
            <span className="inline-block w-1.5 h-1.5 rounded-full bg-oo-esg-accent" />
            <span className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-oo-esg-text">
              Data from{" "}
              <a href={meta.href} target="_blank" rel="noreferrer" className="underline underline-offset-2 hover:text-oo-esg-strong">
                {meta.org}
                <span className="sr-only"> (opens in new tab)</span>
              </a>{" "}
              ·{" "}
              <a
                href={raw.licence_url || "https://eiti.org/sites/default/files/attachments/zambia_open_data_policy.pdf"}
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2 hover:text-oo-esg-strong"
              >
                {meta.licence}
                <span className="sr-only"> (opens in new tab)</span>
              </a>
            </span>
          </div>
          <h3 className="font-head font-bold text-oo-lead text-oo-esg-strong leading-snug">{gleifName}</h3>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            {tpin && <span className="text-oo-meta font-mono text-oo-esg-text">ZM-TPIN {tpin}</span>}
            <MatchConfidenceChip confidence="medium" basis={matchBasis(raw.match?.method)} />
          </div>
          {otherNames.length > 0 && (
            <p className="mt-2 text-oo-small text-oo-esg-text">
              Filed on the portal as {otherNames.map((n) => `“${n}”`).join(", ")}.
            </p>
          )}
        </div>

        {offences.length > 0 && (
          <section aria-label="WARMA water-permit offences">
            <Eyebrow>Water Resources Management Authority (WARMA) offences list</Eyebrow>
            <ul className="space-y-1">
              {offences.map((o, i) => (
                <li key={i} className="text-oo-small text-oo-esg-strong">
                  <span className="font-semibold">{o.offence}</span>
                  {o.activity ? ` · ${o.activity}` : ""}
                  {o.permit ? ` · permit: ${o.permit}` : ""}
                  <span className="text-oo-esg-text"> — listed as “{o.name_as_filed}”</span>
                </li>
              ))}
            </ul>
          </section>
        )}

        {tax.length > 0 && (
          <section aria-label="ZRA tax receipts">
            <Eyebrow>Tax receipts recorded by the Zambia Revenue Authority</Eyebrow>
            <ul className="space-y-2">
              {tax.map((t) => (
                <li key={`${t.year}:${t.dataset}`}>
                  <div className="flex items-baseline justify-between gap-3">
                    <span className="text-oo-small font-semibold text-oo-esg-strong">{t.year}</span>
                    <span className="text-oo-small font-mono text-oo-esg-strong">
                      {t.amounts_summed && t.total_zmw != null
                        ? formatZmw(t.total_zmw)
                        : `${t.payments.toLocaleString()} payment${t.payments === 1 ? "" : "s"} · amounts not published`}
                    </span>
                  </div>
                  <div className="text-oo-meta text-oo-esg-text">
                    {t.by_tax_type
                      .slice(0, 3)
                      .map((x) => (x.zmw != null ? `${x.tax_type} ${formatZmw(x.zmw)}` : `${x.tax_type} ×${x.payments}`))
                      .join(" · ")}
                    {" · "}
                    <DatasetLink bundle={raw} code={t.dataset} />
                  </div>
                </li>
              ))}
            </ul>
            <p className="mt-2 text-oo-meta text-oo-esg-text">
              One ZRA table per payment year; the portal's tables overlap, so years are never added
              across tables.
            </p>
          </section>
        )}

        {recon.length > 0 && (
          <section aria-label="EITI reconciliation payments">
            <Eyebrow>Payments the company reported for EITI reconciliation</Eyebrow>
            <ul className="space-y-1">
              {recon.map((r) => (
                <li key={r.year} className="text-oo-small text-oo-esg-strong">
                  <span className="font-semibold">{r.year}</span>{" "}
                  <span className="font-mono">
                    {formatZmw(r.total_zmw)}
                    {r.total_usd > 0 ? ` + ${formatUsd(r.total_usd)}` : ""}
                  </span>
                  <span className="text-oo-meta text-oo-esg-text">
                    {" · "}
                    {r.by_receiving_entity.slice(0, 3).map((e) => e.entity).join(", ")}
                  </span>
                </li>
              ))}
            </ul>
            <p className="mt-2 text-oo-meta text-oo-esg-text">
              The company's own disclosure (<DatasetLink bundle={raw} code="payment-report" />), shown apart
              from ZRA's receipts and not reconciled with them.
            </p>
          </section>
        )}

        {employment.length > 0 && (
          <section aria-label="Employment">
            <Eyebrow>Employment</Eyebrow>
            <ul className="space-y-1">
              {employment.map((e) => (
                <li key={e.year} className="text-oo-small text-oo-esg-strong">
                  <span className="font-semibold">{e.year}</span>{" "}
                  {e.employees != null ? `${e.employees.toLocaleString()} employees` : "employees not stated"}
                  {e.expatriate != null ? ` (${e.expatriate.toLocaleString()} expatriate)` : ""}
                  {womenShare(e.women_share) ? ` · ${womenShare(e.women_share)} women` : ""}
                </li>
              ))}
            </ul>
          </section>
        )}

        {licences.length > 0 && (
          <section aria-label="Mining rights">
            <Eyebrow>Mining rights in the cadastre (2023 – Q2 2025)</Eyebrow>
            <div className="overflow-x-auto">
              <table className="w-full text-oo-meta text-oo-esg-strong">
                <thead>
                  <tr className="text-left text-oo-esg-text">
                    <th className="pr-3 font-semibold">Licence</th>
                    <th className="pr-3 font-semibold">Status</th>
                    <th className="pr-3 font-semibold">Location</th>
                    <th className="font-semibold">Holder's share</th>
                  </tr>
                </thead>
                <tbody>
                  {licences.map((l) => (
                    <tr key={l.code} className="align-top">
                      <td className="pr-3 font-mono">
                        {l.code}
                        {l.type ? ` · ${l.type}` : ""}
                      </td>
                      <td className="pr-3">{l.status}</td>
                      <td className="pr-3">{l.location}</td>
                      <td>{l.holder_share_pct != null ? `${l.holder_share_pct}%` : "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="mt-2 text-oo-meta text-oo-esg-text">
              A holder's share is a share of the licence, not of the company.
            </p>
          </section>
        )}

        <p className="text-oo-meta text-oo-esg-text">
          Zambia EITI (ZEITI) portal · data from the Zambia Revenue Authority, the Ministry of Mines and
          Minerals Development and WARMA. The portal keys on the ZRA TPIN and GLEIF on the PACRA number, so
          OpenCheck links them by name.
        </p>
      </div>
    </div>
  );
}
