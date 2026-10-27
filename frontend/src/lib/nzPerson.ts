/**
 * A director's or shareholder's other companies on the New Zealand register —
 * the values layer for `components/cdd/NzPersonCompanies.tsx`.
 *
 * Moved from QuickCheck's company-wide "Director & shareholder associations"
 * panel to the BackgroundCheck card (19 Oct 2026), beside the Estonian
 * register's "Companies on the register" control. The difference that stays:
 * Estonia keys a person on the register's own person key, so its list is
 * exact; New Zealand's Role Search is keyed on a name, so every company here
 * is a *match*, graded by how far the filed address agrees.
 */

import type { NzAssociatedCompany, NzPersonAssociationsResponse } from "./api";

/** "Director" / "Shareholder" — the register's two role kinds. */
export function nzRoleLabel(role: string): string {
  if (role === "director") return "Director";
  if (role === "shareholder") return "Shareholder";
  return role;
}

/** "Director · Shareholder 50%" — a company's row, as the register gives it. */
export function nzCompanyRoles(c: NzAssociatedCompany): string {
  return c.roles
    .map((r) =>
      r === "shareholder" && c.share_percentage != null
        ? `${nzRoleLabel(r)} ${c.share_percentage}%`
        : nzRoleLabel(r),
    )
    .join(" · ");
}

function companies(n: number): string {
  return n === 1 ? "one other company" : `${n} other companies`;
}

/** The sentence above the list. Absence is said in the same voice, and a
 *  name-only match is never counted as the same person. */
export function nzCompaniesSummary(data: NzPersonAssociationsResponse): string {
  if (!data.available || !data.person) {
    return `The New Zealand register could not be searched for this person${
      data.reason ? ` (${data.reason})` : ""
    }.`;
  }
  const p = data.person;
  if (p.other_company_count === 0) {
    return "The New Zealand register names no other current company under this name.";
  }
  const parts: string[] = [];
  if (p.address_match_count > 0) {
    parts.push(`${p.address_match_count} with a matching address`);
  }
  if (p.name_only_count > 0) {
    parts.push(`${p.name_only_count} on the name alone`);
  }
  return `Under this name the New Zealand register lists ${companies(p.other_company_count)}: ${parts.join(", ")}.`;
}

/** The caveat under the list — what a match does and does not say. */
export const NZ_MATCH_CAVEAT =
  "The register searches by name, so each company is a match, not an identity: " +
  "a matching filed address makes it stronger; a name-only match may be a " +
  "different person who shares the name. From 18 November 2026 a director may " +
  "publish an alternative address instead of their home address — often an " +
  "accountant's or agent's office shared by all of their clients. The name " +
  "search then publishes no address for them, so it is read from each " +
  "company's own record, and a match on an alternative address is never graded " +
  "above medium. For review, not a determination.";

function companiesWord(n: number): string {
  return n === 1 ? "one company" : `${n} companies`;
}

/** What happened where the name search withheld the address. Null when it
 *  withheld none, or on an answer recorded before the field existed. */
export function nzWithheldNote(data: NzPersonAssociationsResponse): string | null {
  const p = data.person;
  if (!p || !p.address_withheld_count) return null;
  const parts: string[] = [];
  if (p.withheld_checked) {
    parts.push(`${companiesWord(p.withheld_checked)} checked against the company's own record`);
  }
  if (p.withheld_not_checked) {
    parts.push(`${companiesWord(p.withheld_not_checked)} not checked`);
  }
  if (p.withheld_check_failed) {
    parts.push(`${companiesWord(p.withheld_check_failed)} whose record could not be read`);
  }
  const lead =
    p.address_withheld_count === 1
      ? "For one company the name search publishes no address for this person"
      : `For ${p.address_withheld_count} companies the name search publishes no address for this person`;
  const tail =
    p.withheld_not_checked || p.withheld_check_failed
      ? " Those not checked are counted on the name alone."
      : "";
  return `${lead}: ${parts.join(", ")}.${tail}`;
}

/** When the register holds more role records under the name than were read. */
export function nzTruncatedNote(data: NzPersonAssociationsResponse): string | null {
  const p = data.person;
  if (!p || !p.truncated) return null;
  return `${p.total_records_under_name} role records exist under this name; only the first were read, and a common name may mix several people.`;
}

export const NZ_ATTRIBUTION = "New Zealand Companies Office — Companies Entity Role Search";

/** Said on QuickCheck's New Zealand source card, where the company-wide panel
 *  used to be, so a reader who looks there finds where it went. */
export const NZ_PEOPLE_NOTE =
  "Each director's and shareholder's other companies on the New Zealand register are on their card in the BackgroundCheck tab.";
