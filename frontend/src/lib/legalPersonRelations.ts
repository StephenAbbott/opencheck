/**
 * Every company a legal person has held a role in on the Estonian
 * e-Business Register — the values layer for
 * `components/cdd/EstonianLegalPersonRelations.tsx`.
 *
 * Stephen's decisions (17 Oct 2026, the legal-person-relations ticket):
 * legal persons only; fetched on click, never on every lookup; current
 * holdings shown beside the former and deleted ones; listed, never drawn.
 */

import type { LegalPersonCompany, LegalPersonRelation, LegalPersonRelationsResponse } from "./api";
import { humanDate } from "./identityVerification";

const EE_CODE = /^\d{8}$/;
const ISO_DAY = /^\d{4}-\d{2}-\d{2}$/;

/** The Estonian registry code an entity statement carries as an
 *  `EE-ARIREGISTER` identifier — the key the relations list hangs off. */
export function estonianRegistryCodeOf(stmt: unknown): string | null {
  const rd = (stmt as { recordDetails?: { identifiers?: unknown } } | null)?.recordDetails;
  const ids = Array.isArray(rd?.identifiers) ? rd.identifiers : [];
  for (const id of ids as Array<{ scheme?: unknown; id?: unknown }>) {
    if (id?.scheme === "EE-ARIREGISTER" && typeof id.id === "string" && EE_CODE.test(id.id.trim())) {
      return id.id.trim();
    }
  }
  return null;
}

/** "Shareholder (osanik) · 9 Oct 2009 – 7 Feb 2012 · 55%" — the role as the
 *  register words it, its dates (an open one reads "since"), and the share. */
export function relationLine(rel: LegalPersonRelation): string {
  const parts: string[] = [rel.role || "Listed"];
  const since = rel.since && ISO_DAY.test(rel.since) ? humanDate(rel.since) : null;
  const until = rel.until && ISO_DAY.test(rel.until) ? humanDate(rel.until) : null;
  if (since && until) parts.push(`${since} – ${until}`);
  else if (until) parts.push(`until ${until}`);
  else if (since) parts.push(rel.founder ? `at founding, ${since}` : `since ${since}`);
  if (rel.share_percent) parts.push(`${rel.share_percent}%`);
  else if (rel.share_size) parts.push(`${rel.share_size}${rel.share_currency ? ` ${rel.share_currency}` : ""}`);
  return parts.join(" · ");
}

/** The company's status beside its name: only an ended or ending one is
 *  said — "Deleted" in the register's own word. */
export function companyStatusNote(c: LegalPersonCompany): string | null {
  if (!c.status || c.status_code === "R") return null;
  return c.status;
}

/** The sentence above the list. A list that was not read is never "none". */
export function relationsSummary(data: LegalPersonRelationsResponse): string {
  if (data.status === "not_configured") {
    return "The e-Business Register's REST service is not configured on this server, so this list could not be read.";
  }
  if (data.status === "unavailable") {
    return `The e-Business Register did not answer: ${data.detail ?? "the request was refused"}. This is not a finding that there are none.`;
  }
  const n = data.total;
  if (n === 0) return "The register records this legal person in no other company.";
  const deleted = data.companies.filter((c) => c.status_code === "K").length;
  const current = data.companies.filter((c) => c.current).length;
  const companies = n === 1 ? "one company" : `${n} companies`;
  const parts = [`Recorded in ${companies}`];
  const detail: string[] = [];
  if (current) detail.push(`${current} with a current role`);
  if (deleted) detail.push(`${deleted} deleted`);
  const head = parts.join("") + (detail.length ? ` (${detail.join(", ")})` : "");
  const day =
    data.retrieved_at && ISO_DAY.test(data.retrieved_at.slice(0, 10))
      ? `, read on ${humanDate(data.retrieved_at.slice(0, 10))}`
      : "";
  const cut = data.truncated ? ` The first ${data.companies.length} are shown.` : "";
  return `${head}${day}.${cut}`;
}
