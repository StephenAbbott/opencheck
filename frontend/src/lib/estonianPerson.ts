/**
 * A person's companies on the Estonian e-Business Register, keyed on the
 * register's pseudonymous person key (`isikukood_hash`) — the values layer for
 * `components/cdd/EstonianPersonCompanies.tsx`.
 *
 * Stephen's decisions (16 Oct 2026): the key only, never a name search; the
 * fields the register publishes (company, role, since, share), no addresses;
 * listed on demand, never drawn as graph nodes.
 */

import type { AppointmentItem, PersonAppointmentsResponse } from "./api";
import { personIdentifierNotes } from "./annotations";
import { humanDate } from "./identityVerification";

export const EE_PERSON_SCHEME = "EE-ARIREGISTER-HASH";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** The register's person key from a grouped person's identifier keys
 *  (`backgroundCheck.ts` lowercases them as `scheme:value`). */
export function estonianPersonHash(identifierKeys: readonly string[]): string | null {
  const prefix = `${EE_PERSON_SCHEME.toLowerCase()}:`;
  for (const key of identifierKeys) {
    if (!key.startsWith(prefix)) continue;
    const value = key.slice(prefix.length);
    if (UUID.test(value)) return value.toLowerCase();
  }
  return null;
}

/** The register's person key carried by one BODS person statement, as the
 *  Phase 255 `identifying` annotation — never `identifiers`. */
export function estonianPersonHashOf(stmt: unknown): string | null {
  for (const note of personIdentifierNotes(stmt)) {
    if (note.scheme === EE_PERSON_SCHEME && UUID.test(note.id)) return note.id.toLowerCase();
  }
  return null;
}

/** Which of the company's lists the person is on, in words. */
export const REGISTER_LIST_LABEL: Record<string, string> = {
  registry_card: "registry card",
  shareholders: "shareholders",
  beneficial_owners: "beneficial owners",
  off_card: "outside the registry card",
};

/** "Shareholder (osanik) · since 10 March 2010 · 60.00%" — the role as the
 *  register words it, the date it gives, and the share where the list
 *  states one (the percentage, else the share's value). */
export function positionLine(item: AppointmentItem): string {
  const parts: string[] = [item.role ?? "Listed"];
  if (item.appointed_on && /^\d{4}-\d{2}-\d{2}$/.test(item.appointed_on)) {
    parts.push(`since ${humanDate(item.appointed_on)}`);
  }
  if (item.share_percent) {
    parts.push(`${item.share_percent}%`);
  } else if (item.share_size) {
    parts.push(`${item.share_size}${item.share_currency ? ` ${item.share_currency}` : ""}`);
  }
  return parts.join(" · ");
}

/** One company and the person's rows on it, in the order the server gave. */
export interface CompanyPositions {
  code: string;
  name: string;
  status: string | null;
  legalForm: string | null;
  positions: AppointmentItem[];
}

export function groupByCompany(items: readonly AppointmentItem[]): CompanyPositions[] {
  const out: CompanyPositions[] = [];
  const byCode = new Map<string, CompanyPositions>();
  for (const item of items) {
    const code = item.company_number ?? item.company_name;
    let entry = byCode.get(code);
    if (!entry) {
      entry = {
        code,
        name: item.company_name,
        status: item.company_status ?? null,
        legalForm: item.company_legal_form ?? null,
        positions: [],
      };
      byCode.set(code, entry);
      out.push(entry);
    }
    entry.positions.push(item);
  }
  return out;
}

/** The sentence above the list. Absence is said in the same voice. */
export function companiesSummary(data: PersonAppointmentsResponse): string {
  const n = data.company_count ?? 0;
  const cut = data.as_of && /^\d{4}-\d{2}-\d{2}$/.test(data.as_of) ? ` as of ${humanDate(data.as_of)}` : "";
  if (data.is_stub) {
    return "The register's index is not loaded on this server, so the person's other companies could not be listed.";
  }
  if (n === 0) {
    return `The register's current lists name this person key on no company${cut}.`;
  }
  const companies = n === 1 ? "one company" : `${n} companies`;
  const shown = data.truncated ? ` The first ${data.appointments.length} of ${data.total_results} entries are shown.` : "";
  return `Named on the current lists of ${companies} in the e-Business Register${cut}, this one included.${shown}`;
}

/** The register's own page for a company. */
export function registerCompanyUrl(code: string): string | null {
  return /^\d{8}$/.test(code) ? `https://ariregister.rik.ee/eng/company/${code}` : null;
}
