/**
 * ESTMA (`estma`) — payments to governments reported under Canada's
 * Extractive Sector Transparency Measures Act, as Statistics Canada
 * republishes them. The values behind its card and tile.
 *
 * Rules the card leans on, kept here where they can be tested:
 *
 * - **One payment, two views.** ESTMA files every payment by payee and again
 *   by project. A report's total is the sum of its payee lines
 *   (`payee_total_cad`, OpenCheck's arithmetic, labelled as such); the
 *   project lines are the same money seen another way and are never added.
 * - **Reports are never added together** — a stub period beside a full
 *   year, or a refiled report, stays its own row.
 * - **"Filed in another entity's report" is not "paid nothing"**, and not
 *   ownership: it is a reporting arrangement under the Act.
 * - **The LEI is a reviewed name match**; the file carries only the ESTMA ID.
 */

export interface EstmaAmountByType {
  code: string;
  label: string | null;
  cad: number;
}

export interface EstmaLine {
  name: string;
  country_code: string | null;
  country_name: string | null;
  total_cad: number;
  by_payment_type: EstmaAmountByType[];
}

export interface EstmaReport {
  year: string;
  period_start: string | null;
  period_end: string | null;
  report_type: string | null;
  report_type_label: string | null;
  url: string | null;
  included_in: { estma_id: string; name: string | null } | null;
  payee_total_cad?: number;
  payee_count?: number;
  project_count?: number;
  by_payment_type?: EstmaAmountByType[];
  by_country?: { code: string | null; name: string | null; cad: number; payees: number }[];
  payees?: EstmaLine[];
  projects?: EstmaLine[];
}

export interface EstmaEntity {
  estma_id: string;
  name: string;
  reports_payments: boolean;
  reports: EstmaReport[];
  includes: { year: string; entities: { estma_id: string; name: string }[] } | null;
  match: { method: string; basis: string; gleif_name: string; note: string };
}

export interface EstmaBundle {
  lei: string;
  gleif_legal_name: string;
  entities: EstmaEntity[];
  latest_year: string | null;
  cut?: string | null;
  dataset_url?: string;
  dataset_page?: string;
  nrcan_url?: string;
  licence_url?: string;
}

/** Canadian dollars, compact — `CAD 4.6bn`, `CAD 12.3m`, `CAD 54,395`. */
export function formatCad(v: number): string {
  const a = Math.abs(v);
  const sign = v < 0 ? "−" : "";
  if (a >= 1e9) return `${sign}CAD ${(a / 1e9).toFixed(1)}bn`;
  if (a >= 1e6) return `${sign}CAD ${(a / 1e6).toFixed(1)}m`;
  return `${sign}CAD ${Math.round(a).toLocaleString("en-GB")}`;
}

/** The reports an entity filed itself (not ones it is included in). */
export function ownReports(entity: EstmaEntity): EstmaReport[] {
  return entity.reports.filter((r) => !r.included_in);
}

/** The reporting period, as filed: `1 Jan 2025 – 31 Dec 2025`. */
export function periodLine(report: EstmaReport): string | null {
  const fmt = (d: string | null) => {
    if (!d) return null;
    const dt = new Date(`${d}T00:00:00Z`);
    if (Number.isNaN(dt.getTime())) return d;
    return dt.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
  };
  const a = fmt(report.period_start);
  const b = fmt(report.period_end);
  return a && b ? `${a} – ${b}` : a || b;
}

/** The summary tile: the latest report's payee total, said as a sum of its
 *  payee lines; for an entity filed in another's report, whose report. */
export function estmaTile(bundle: EstmaBundle): { stat: string; unit: string; sub: string } {
  const entity = bundle.entities[0];
  const latest = entity?.reports[0];
  if (!entity || !latest) return { stat: "—", unit: "ESTMA reports", sub: "" };
  const own = ownReports(entity).filter((r) => r.year === latest.year);
  if (own.length > 0) {
    const r = own[0];
    return {
      stat: formatCad(r.payee_total_cad ?? 0),
      unit: "to governments",
      sub:
        own.length > 1
          ? `${latest.year} · ${own.length} reports, first shown`
          : `${latest.year} · sum of payee lines`,
    };
  }
  return {
    stat: latest.year,
    unit: "payments filed by another entity",
    sub: `in the report of ${latest.included_in?.name ?? latest.included_in?.estma_id ?? "another entity"}`,
  };
}

/**
 * Said once above the ESG tiles when a company has both an `eiti` card and
 * an `estma` card: two disclosures of payments to governments that overlap
 * in subject, not in figures. Null when only one is present.
 */
export function estmaAndEitiNote(sourceIds: readonly string[]): string | null {
  const ids = new Set(sourceIds);
  if (!ids.has("estma") || !(ids.has("eiti") || ids.has("eiti_zambia"))) return null;
  return (
    "Two disclosures of payments to governments cover this company. ESTMA reports are " +
    "filed under Canadian law, in Canadian dollars, for the company's own reporting " +
    "periods; EITI data is reported country by country under the EITI Standard. " +
    "Different reports, currencies and periods, so the figures are not added together " +
    "or compared line by line."
  );
}
