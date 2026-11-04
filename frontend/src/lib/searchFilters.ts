/**
 * Search filters for the Company name tab (the advanced-search-filters ticket).
 *
 * Pure: values, words and the GLEIF query — the website searches GLEIF from
 * the browser, so the backend's `opencheck/search_filters.py` is mirrored
 * here and `backend/tests/test_search_filters.py` parses this file for the
 * words, the scan sizes and the parameter name. Stephen's decisions
 * (3 Nov 2026):
 *
 * - The filters are hidden behind "More filters" and, once set, always
 *   visible as removable chips.
 * - Legal form waits for a jurisdiction: ISO 20275 defines forms country by
 *   country (and state by state in the US, province by province in Canada).
 * - The jurisdiction filter is exact. GLEIF's `entity.jurisdiction` filter
 *   matches each part of a code, so `PA` also returns Pennsylvania (`US-PA`);
 *   for the clashing codes the search scans and keeps exact rows, and the
 *   total is GLEIF's minus the clashing sub-divisions'.
 */

import jurisdictionData from "./gleifJurisdictions.json";

export interface EntityFilters {
  jurisdiction?: string;
  /** ELF codes; `["8888", "9999"]` is "No ELF code". */
  legalForm?: string[];
  entityStatus?: string;
  registrationStatus?: string[];
  category?: string[];
}

/** A legal form as `/elf-forms` returns it. */
export interface LegalForm {
  code: string;
  name: string;
  abbreviation: string | null;
  abbreviations: string[];
  names: string[];
  subdivision: string | null;
  issued_leis: number | null;
}

export interface LegalFormList {
  jurisdiction: string;
  jurisdiction_name: string;
  forms: LegalForm[];
  has_subdivision_forms: boolean;
  counts_available: boolean;
  elf_version: string;
}

export const ENTITY_STATUS_WORDS: Record<string, string> = {
  ACTIVE: "Active",
  INACTIVE: "Inactive",
};

export const REGISTRATION_STATUS_WORDS: Record<string, string> = {
  ISSUED: "Issued",
  LAPSED: "Lapsed",
  RETIRED: "Retired",
  MERGED: "Merged",
  ANNULLED: "Annulled",
  DUPLICATE: "Duplicate",
  PENDING_TRANSFER: "Pending transfer",
  PENDING_ARCHIVAL: "Pending archival",
};

export const CATEGORY_WORDS: Record<string, string> = {
  GENERAL: "General",
  BRANCH: "Branch",
  FUND: "Fund",
  SOLE_PROPRIETOR: "Sole proprietor",
  RESIDENT_GOVERNMENT_ENTITY: "Government entity",
  INTERNATIONAL_ORGANIZATION: "International organisation",
};

/** ELF placeholders: 8888 "not yet in the list", 9999 "no ELF code". */
export const PLACEHOLDER_FORMS = ["8888", "9999"];
export const NO_ELF_CODE_LABEL = "No ELF code";

/** How far the exact-jurisdiction scan reads before it stops and says so. */
export const EXACT_SCAN_PAGE = 200;
export const EXACT_SCAN_MAX_PAGES = 5;

interface JurisdictionData {
  jurisdictions: { code: string; name: string }[];
  collisions: Record<string, string[]>;
}
const DATA = jurisdictionData as JurisdictionData;

const NAMES: Record<string, string> = Object.fromEntries(
  DATA.jurisdictions.map((j) => [j.code, j.name]),
);

export function jurisdictionName(code: string): string {
  return NAMES[code] ?? code;
}

/** Country-level jurisdictions for the picker, by name. GLEIF's "XX"
 *  (unknown) is not something to search for. */
export const COUNTRY_JURISDICTIONS = DATA.jurisdictions
  .filter((j) => !j.code.includes("-") && j.code !== "XX")
  .sort((a, b) => a.name.localeCompare(b.name));

/** A country's own sub-divisions (US states, Canadian provinces, UAE
 *  emirates…), by name. */
export function subdivisionsOf(country: string): { code: string; name: string }[] {
  return DATA.jurisdictions
    .filter((j) => j.code.startsWith(`${country}-`))
    .sort((a, b) => a.name.localeCompare(b.name));
}

/** The sub-divisions GLEIF's filter wrongly includes for a country code. */
export function clashingSubdivisions(code: string): string[] {
  return DATA.collisions[code] ?? [];
}

/** A record's jurisdiction is the wanted one or one of its own sub-divisions:
 *  `US-DE` matches `US`, never `DE`. */
export function jurisdictionMatches(value: string | undefined | null, wanted: string): boolean {
  if (!value) return false;
  const v = value.toUpperCase();
  const w = wanted.toUpperCase();
  return v === w || v.startsWith(`${w}-`);
}

export function isPlaceholderForm(codes: string[] | undefined): boolean {
  return !!codes?.length && codes.every((c) => PLACEHOLDER_FORMS.includes(c));
}

/** Whether GLEIF's jurisdiction filter would return records from elsewhere.
 *  A real legal-form code belongs to one country, so it excludes them; the
 *  placeholders are used everywhere. */
export function needsExactJurisdiction(f: EntityFilters): boolean {
  if (!f.jurisdiction || clashingSubdivisions(f.jurisdiction).length === 0) return false;
  return !f.legalForm?.length || f.legalForm.some((c) => PLACEHOLDER_FORMS.includes(c));
}

export function activeFilterCount(f: EntityFilters): number {
  return (
    (f.jurisdiction ? 1 : 0) +
    (f.legalForm?.length ? 1 : 0) +
    (f.entityStatus ? 1 : 0) +
    (f.registrationStatus?.length ? 1 : 0) +
    (f.category?.length ? 1 : 0)
  );
}

/** The GLEIF `lei-records` filter parameters. `entity.legalForm`, never
 *  `entity.legalForm.id`, which GLEIF refuses with HTTP 400. */
export function gleifFilterParams(f: EntityFilters): [string, string][] {
  const out: [string, string][] = [];
  if (f.jurisdiction) out.push(["filter[entity.jurisdiction]", f.jurisdiction]);
  if (f.legalForm?.length) out.push(["filter[entity.legalForm]", f.legalForm.join(",")]);
  if (f.entityStatus) out.push(["filter[entity.status]", f.entityStatus]);
  if (f.registrationStatus?.length)
    out.push(["filter[registration.status]", f.registrationStatus.join(",")]);
  if (f.category?.length) out.push(["filter[entity.category]", f.category.join(",")]);
  return out;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function recordMatches(item: any, f: EntityFilters): boolean {
  const entity = item?.attributes?.entity ?? {};
  const reg = item?.attributes?.registration ?? {};
  if (f.jurisdiction && !jurisdictionMatches(entity.jurisdiction, f.jurisdiction)) return false;
  if (f.legalForm?.length && !f.legalForm.includes(String(entity.legalForm?.id ?? "").toUpperCase()))
    return false;
  if (f.entityStatus && String(entity.status ?? "").toUpperCase() !== f.entityStatus) return false;
  if (
    f.registrationStatus?.length &&
    !f.registrationStatus.includes(String(reg.status ?? "").toUpperCase())
  )
    return false;
  if (f.category?.length && !f.category.includes(String(entity.category ?? "").toUpperCase()))
    return false;
  return true;
}

/** The label a legal form wears in a chip, a field and a result row. */
export function legalFormLabel(form: Pick<LegalForm, "abbreviation" | "name">): string {
  return form.abbreviation ? `${form.abbreviation} · ${form.name}` : form.name;
}

/** The short label for a result row: the abbreviation where there is one. */
export function legalFormShort(form: Pick<LegalForm, "abbreviation" | "name">): string {
  return form.abbreviation ?? form.name;
}

export interface FilterChip {
  key: keyof EntityFilters;
  label: string;
  removeLabel: string;
}

/** One chip per set filter, in the panel's order. `forms` is the loaded list
 *  for the jurisdiction, used for the legal form's words. */
export function filterChips(f: EntityFilters, forms: LegalForm[] = []): FilterChip[] {
  const chips: FilterChip[] = [];
  if (f.jurisdiction) {
    const name = jurisdictionName(f.jurisdiction);
    chips.push({ key: "jurisdiction", label: name, removeLabel: `Remove jurisdiction filter: ${name}` });
  }
  if (f.legalForm?.length) {
    let label: string;
    if (isPlaceholderForm(f.legalForm)) label = NO_ELF_CODE_LABEL;
    else {
      const form = forms.find((x) => x.code === f.legalForm![0]);
      label = form ? legalFormLabel(form) : f.legalForm.join(", ");
    }
    chips.push({ key: "legalForm", label, removeLabel: `Remove legal form filter: ${label}` });
  }
  if (f.entityStatus) {
    const word = (ENTITY_STATUS_WORDS[f.entityStatus] ?? f.entityStatus).toLowerCase();
    chips.push({
      key: "entityStatus",
      label: `Company ${word}`,
      removeLabel: `Remove company status filter: ${word}`,
    });
  }
  if (f.registrationStatus?.length) {
    const words = f.registrationStatus.map((s) => (REGISTRATION_STATUS_WORDS[s] ?? s).toLowerCase());
    chips.push({
      key: "registrationStatus",
      label: `LEI ${words.join(" or ")}`,
      removeLabel: `Remove LEI registration filter: ${words.join(" or ")}`,
    });
  }
  if (f.category?.length) {
    const words = f.category.map((c) => CATEGORY_WORDS[c] ?? c);
    chips.push({
      key: "category",
      label: words.join(" or "),
      removeLabel: `Remove category filter: ${words.join(" or ")}`,
    });
  }
  return chips;
}

export function withoutFilter(f: EntityFilters, key: keyof EntityFilters): EntityFilters {
  const next = { ...f };
  delete next[key];
  // A legal form belongs to its jurisdiction: removing the jurisdiction
  // removes the form too, or the search would keep a German form with no
  // German scope on screen.
  if (key === "jurisdiction") delete next.legalForm;
  return next;
}

/** Filter a legal-form list by what was typed: abbreviation, any name in any
 *  language, or the code. Abbreviation matches first, then the list's order. */
export function matchLegalForms(forms: LegalForm[], typed: string): LegalForm[] {
  const needle = fold(typed);
  if (!needle) return forms;
  const scored: [number, number, LegalForm][] = [];
  forms.forEach((form, index) => {
    const abbrs = form.abbreviations.map(fold);
    let score = -1;
    if (fold(form.code) === needle || abbrs.includes(needle)) score = 0;
    else if (abbrs.some((a) => a.startsWith(needle))) score = 1;
    else if (form.names.some((n) => fold(n).includes(needle))) score = 2;
    else if (abbrs.some((a) => a.includes(needle))) score = 3;
    if (score >= 0) scored.push([score, index, form]);
  });
  return scored.sort((a, b) => a[0] - b[0] || a[1] - b[1]).map((s) => s[2]);
}

function fold(text: string): string {
  return text
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[\s.]+/g, "")
    .toLowerCase();
}

export interface CountLineInput {
  shown: number;
  total: number | null;
  /** The exact-jurisdiction scan stopped at its limit before finding enough. */
  scanCapped?: boolean;
  recordsChecked?: number;
  rawTotal?: number | null;
}

/** "First 10 of 48,503" — the count says how many matched, never just how
 *  many are shown. (The v1 line read "10 results" for 48,503 matches.) */
export function countLine(input: CountLineInput): string {
  const { shown, total, scanCapped, recordsChecked, rawTotal } = input;
  if (scanCapped && recordsChecked) {
    return (
      `Showing the ${shown.toLocaleString("en-GB")} found in the first ` +
      `${recordsChecked.toLocaleString("en-GB")} of ${(rawTotal ?? recordsChecked).toLocaleString("en-GB")} ` +
      "GLEIF records checked"
    );
  }
  if (total === null) return `${shown.toLocaleString("en-GB")} shown`;
  if (total <= shown) return total === 1 ? "1 GLEIF record" : `${total.toLocaleString("en-GB")} GLEIF records`;
  return `First ${shown.toLocaleString("en-GB")} of ${total.toLocaleString("en-GB")} GLEIF records`;
}

/** What a scan that stopped early leaves unchecked, in words — the other
 *  places sharing the code, so the reader knows why it is slow going. */
export function scanCappedNote(jurisdiction: string): string {
  const places = clashingSubdivisions(jurisdiction).map(jurisdictionName);
  return (
    `GLEIF's ${jurisdiction} filter also returns ${places.join(" and ")}, ` +
    "which OpenCheck sets aside. Keep looking to check more records."
  );
}
