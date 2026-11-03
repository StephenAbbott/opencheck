/**
 * The Company name tab's GLEIF search, with the advanced-search filters.
 *
 * The browser calls GLEIF directly (it always has). Two things are new:
 *
 * - **The true total.** GLEIF's `meta.pagination.total`, so the count line
 *   can say "First 10 of 48,503" instead of "10 results".
 * - **An exact jurisdiction.** For a code GLEIF also matches inside a
 *   sub-division (`PA` → Pennsylvania, `DE` → Delaware), the total is GLEIF's
 *   minus each clashing sub-division's under the same filters, and the rows
 *   are scanned 200 at a time, keeping exact ones, up to 1,000 records per
 *   request. A scan that stops early says so and can be continued.
 */

import { parseOtherNames, type GleifSearchResult } from "./gleifNationalId";
import {
  EXACT_SCAN_MAX_PAGES,
  EXACT_SCAN_PAGE,
  clashingSubdivisions,
  gleifFilterParams,
  needsExactJurisdiction,
  recordMatches,
  type EntityFilters,
} from "./searchFilters";

export const GLEIF_API = "https://api.gleif.org/api/v1/lei-records";
export const PAGE_SIZE = 10;

/** Where the next "Show the next 10" picks up. */
export interface SearchCursor {
  /** GLEIF page number to read next. */
  page: number;
  /** Page size the cursor counts in. */
  size: number;
  /** Rows of that page already used (an exact scan can stop mid-page). */
  offset: number;
}

export interface NameSearchResult {
  results: GleifSearchResult[];
  /** Records that match the filters exactly — GLEIF's count, corrected. */
  total: number | null;
  /** GLEIF's own count before the jurisdiction correction. */
  rawTotal: number | null;
  /** The exact scan stopped at its limit before it had a full page. */
  scanCapped: boolean;
  /** Records the exact scan read in this request. */
  recordsChecked: number;
  /** Null when there is nothing more to show. */
  next: SearchCursor | null;
}

type Fetch = (url: string, init?: RequestInit) => Promise<Response>;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function parseNameRecord(item: any): GleifSearchResult {
  const attrs = item.attributes ?? {};
  const entity = attrs.entity ?? {};
  const reg = attrs.registration ?? {};
  return {
    lei: attrs.lei as string,
    legalName: (entity.legalName?.name as string) ?? (entity.legalName as string) ?? attrs.lei,
    country: entity.legalAddress?.country ?? "—",
    status: reg.status ?? "—",
    otherNames: parseOtherNames(entity.otherNames),
    jurisdiction: entity.jurisdiction ?? undefined,
    legalForm: entity.legalForm?.id ?? undefined,
    entityStatus: entity.status ?? undefined,
  };
}

export function nameSearchUrl(
  q: string,
  filters: EntityFilters,
  size: number,
  page: number,
  jurisdictionOverride?: string,
): string {
  const parts = [`filter[entity.names]=${encodeURIComponent(q)}`];
  for (const [key, value] of gleifFilterParams(filters)) {
    const v = key === "filter[entity.jurisdiction]" && jurisdictionOverride ? jurisdictionOverride : value;
    parts.push(`${key}=${encodeURIComponent(v).replace(/%2C/g, ",")}`);
  }
  parts.push(`page[size]=${size}`, `page[number]=${page}`);
  return `${GLEIF_API}?${parts.join("&")}`;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function getJson(fetchImpl: Fetch, url: string): Promise<any> {
  const resp = await fetchImpl(url, { headers: { Accept: "application/vnd.api+json" } });
  if (!resp.ok) throw new Error(`GLEIF API returned ${resp.status}`);
  return resp.json();
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function totalOf(json: any): number | null {
  const t = json?.meta?.pagination?.total;
  return typeof t === "number" ? t : null;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function lastPageOf(json: any, page: number): number {
  const last = json?.meta?.pagination?.lastPage;
  return typeof last === "number" ? last : page;
}

/** The exact total: GLEIF's count minus each clashing sub-division's. */
async function exactTotal(
  fetchImpl: Fetch,
  q: string,
  filters: EntityFilters,
  rawTotal: number | null,
): Promise<number | null> {
  if (rawTotal === null) return null;
  const others = await Promise.all(
    clashingSubdivisions(filters.jurisdiction!).map((sub) =>
      getJson(fetchImpl, nameSearchUrl(q, filters, 1, 1, sub)).then(totalOf),
    ),
  );
  if (others.some((n) => n === null)) return null;
  return Math.max(0, rawTotal - others.reduce<number>((a, n) => a + (n ?? 0), 0));
}

/**
 * One page of results. `cursor` continues a previous search ("Show the next
 * 10"); its absence starts one, and only a first page computes the totals.
 */
export async function searchGleifByName(
  q: string,
  filters: EntityFilters,
  cursor: SearchCursor | null = null,
  fetchImpl: Fetch = (url, init) => fetch(url, init),
): Promise<NameSearchResult> {
  if (!needsExactJurisdiction(filters)) {
    const page = cursor?.page ?? 1;
    const json = await getJson(fetchImpl, nameSearchUrl(q, filters, PAGE_SIZE, page));
    const total = totalOf(json);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rows = (json.data ?? []).filter((item: any) => recordMatches(item, filters)).map(parseNameRecord);
    const more = page < lastPageOf(json, page);
    return {
      results: rows,
      total,
      rawTotal: total,
      scanCapped: false,
      recordsChecked: (json.data ?? []).length,
      next: more ? { page: page + 1, size: PAGE_SIZE, offset: 0 } : null,
    };
  }

  // The exact scan.
  let page = cursor?.page ?? 1;
  let offset = cursor?.offset ?? 0;
  const results: GleifSearchResult[] = [];
  let rawTotal: number | null = null;
  let recordsChecked = 0;
  let lastPage = page;
  let next: SearchCursor | null = null;
  for (let read = 0; read < EXACT_SCAN_MAX_PAGES; read += 1) {
    const json = await getJson(fetchImpl, nameSearchUrl(q, filters, EXACT_SCAN_PAGE, page));
    if (rawTotal === null) rawTotal = totalOf(json);
    lastPage = lastPageOf(json, page);
    const data = json.data ?? [];
    let i = offset;
    for (; i < data.length && results.length < PAGE_SIZE; i += 1) {
      recordsChecked += 1;
      if (recordMatches(data[i], filters)) results.push(parseNameRecord(data[i]));
    }
    if (results.length >= PAGE_SIZE) {
      next = i < data.length ? { page, size: EXACT_SCAN_PAGE, offset: i } : page < lastPage ? { page: page + 1, size: EXACT_SCAN_PAGE, offset: 0 } : null;
      break;
    }
    offset = 0;
    if (page >= lastPage) {
      next = null;
      break;
    }
    page += 1;
    next = { page, size: EXACT_SCAN_PAGE, offset: 0 };
  }
  const scanCapped = results.length < PAGE_SIZE && next !== null;
  const total = cursor ? null : await exactTotal(fetchImpl, q, filters, rawTotal);
  return { results, total, rawTotal, scanCapped, recordsChecked, next };
}
