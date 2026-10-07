/**
 * Zambia EITI portal (`eiti_zambia`) — the values behind its card and tile.
 *
 * Two rules the card leans on, kept here where they can be tested:
 *
 * - **ZRA receipts are one table per year.** The portal's tables overlap (two
 *   of the 2023 tables are the same rows), so the backend takes one per
 *   payment year and never sums across them. A year whose table publishes no
 *   usable amounts carries a payment count instead, and the card says so
 *   rather than showing a total it does not have.
 * - **The LEI is a name match.** The portal keys on the ZRA TPIN; GLEIF files
 *   the PACRA number. The match is graded "medium" with the basis spelled out.
 */

export interface ZambiaTaxType {
  tax_type: string;
  zmw?: number;
  payments?: number;
}

export interface ZambiaTaxYear {
  year: string;
  dataset: string;
  payments: number;
  amounts_summed: boolean;
  total_zmw?: number;
  by_tax_type: ZambiaTaxType[];
}

export interface ZambiaReconciliationYear {
  year: string;
  total_zmw: number;
  total_usd: number;
  lines: number;
  by_receiving_entity: { entity: string; zmw: number; usd: number }[];
}

export interface ZambiaEmployment {
  year: string;
  employees: number | null;
  domestic: number | null;
  expatriate: number | null;
  women_share: number | null;
}

export interface ZambiaLicence {
  code: string;
  type: string | null;
  status: string | null;
  commodities: string | null;
  area: string | null;
  location: string | null;
  grant_date: string | null;
  expiry_date: string | null;
  holder_as_filed: string;
  holder_share_pct: number | null;
}

export interface ZambiaOffence {
  name_as_filed: string;
  offence: string | null;
  activity: string | null;
  permit: string | null;
}

export interface EitiZambiaBundle {
  lei: string;
  gleif_legal_name: string;
  tpins: string[];
  names_as_filed: string[];
  match: { method: string; confidence: string; aliases: string[] };
  zra_tax: ZambiaTaxYear[];
  eiti_reconciliation: ZambiaReconciliationYear[];
  employment: ZambiaEmployment[];
  licences: ZambiaLicence[];
  water_offences: ZambiaOffence[];
  datasets: Record<string, { name?: string; url?: string; last_updated?: string }>;
  portal_url?: string;
  licence_url?: string;
}

/** Kwacha, compact — `ZMW 6.4bn`, `ZMW 12.3m`, `ZMW 54,395`. */
export function formatZmw(v: number): string {
  if (v >= 1e9) return `ZMW ${(v / 1e9).toFixed(1)}bn`;
  if (v >= 1e6) return `ZMW ${(v / 1e6).toFixed(1)}m`;
  return `ZMW ${Math.round(v).toLocaleString()}`;
}

/** US dollars, compact, for the reconciliation report's USD column. */
export function formatUsd(v: number): string {
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  return `$${Math.round(v).toLocaleString()}`;
}

/** What the "Possible match" chip rests on, in words. */
export function matchBasis(method: string | null | undefined): string {
  return method === "tpin_via_name"
    ? "GLEIF legal name matches the name ZRA files with this TPIN"
    : "GLEIF legal name matches the licence holder in the cadastre";
}

/** The dataset's display name, falling back to its code. */
export function datasetName(bundle: EitiZambiaBundle, code: string): string {
  return bundle.datasets?.[code]?.name || code;
}

/** The most recent ZRA year that carries a kwacha total, if any. */
export function latestSummedYear(bundle: EitiZambiaBundle): ZambiaTaxYear | null {
  return (bundle.zra_tax ?? []).find((t) => t.amounts_summed && (t.total_zmw ?? 0) > 0) ?? null;
}

/** The summary tile: the latest ZRA total when there is one; otherwise the
 *  payment count, then the rights count — always in the same voice. */
export function zambiaTile(bundle: EitiZambiaBundle): { stat: string; unit: string; sub: string } {
  const summed = latestSummedYear(bundle);
  if (summed) {
    return { stat: formatZmw(summed.total_zmw ?? 0), unit: "ZRA tax receipts", sub: summed.year };
  }
  const tax = bundle.zra_tax ?? [];
  if (tax.length > 0) {
    const t = tax[0];
    return {
      stat: t.payments.toLocaleString(),
      unit: `ZRA tax payment${t.payments === 1 ? "" : "s"}`,
      sub: `${t.year} · amounts not published`,
    };
  }
  const n = (bundle.licences ?? []).length;
  return { stat: n.toLocaleString(), unit: `mining right${n === 1 ? "" : "s"}`, sub: "in the cadastre" };
}

/** A women-employed share as filed (0.083 → "8.3%"); null stays absent. */
export function womenShare(share: number | null | undefined): string | null {
  if (share == null) return null;
  return `${Math.round(share * 1000) / 10}%`;
}
