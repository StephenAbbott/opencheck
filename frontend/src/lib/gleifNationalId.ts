/**
 * GLEIF reverse lookup: national registration ID → LEI.
 *
 * A national ID may appear in three different fields on a GLEIF LEI record.
 * We query all three filter endpoints in parallel and deduplicate by LEI
 * to ensure we don't miss entities where the ID sits in a non-primary field.
 *
 * Always pass the RA code as a second filter to avoid false matches from
 * coincidental ID collisions across different national registries.
 *
 * Reference: https://documenter.getpostman.com/view/7679680/SVYrrxuU
 */

import { nationalIdQuery } from "./raCodes";

export interface GleifSearchResult {
  lei: string;
  legalName: string;
  country: string;
  status: string;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function parseRecord(item: any): GleifSearchResult {
  const attrs = item.attributes ?? {};
  const entity = attrs.entity ?? {};
  const reg = attrs.registration ?? {};
  return {
    lei: attrs.lei as string,
    legalName:
      (entity.legalName?.name as string) ??
      (entity.legalName as string) ??
      attrs.lei,
    country: entity.legalAddress?.country ?? "—",
    status: reg.status ?? "—",
  };
}

async function gleifFilter(
  filterParams: Record<string, string>,
): Promise<GleifSearchResult[]> {
  const params = new URLSearchParams({
    ...filterParams,
    "page[size]": "10",
  });
  try {
    const resp = await fetch(
      `https://api.gleif.org/api/v1/lei-records?${params.toString()}`,
      { headers: { Accept: "application/vnd.api+json" } },
    );
    if (!resp.ok) return [];
    const json = await resp.json();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (json.data ?? []).map((item: any) => parseRecord(item));
  } catch {
    return [];
  }
}

/**
 * Query all three GLEIF registration-ID filter fields in parallel,
 * deduplicate by LEI, and return a list of matching entities.
 *
 * @param raCode  GLEIF Registration Authority code, e.g. "RA000585"
 * @param registrationId  The national company registration number
 */
export async function searchByNationalId(
  raCode: string,
  registrationId: string,
): Promise<GleifSearchResult[]> {
  // Every spelling and every authority that files the same number (France:
  // three authorities, two spellings — Phase 265). Comma = OR in GLEIF filters.
  const { values, raCodes } = nationalIdQuery(raCode, registrationId);
  const id = values.join(",");
  const scope: Record<string, string> = raCodes.length
    ? { "filter[entity.registeredAt]": raCodes.join(",") }
    : {};

  const [r1, r2, r3] = await Promise.all([
    gleifFilter({ "filter[entity.registeredAs]": id, ...scope }),
    gleifFilter({ "filter[registration.validatedAs]": id, ...scope }),
    gleifFilter({
      "filter[registration.otherValidationAuthorities.validatedAs]": id,
      ...scope,
    }),
  ]);

  // Deduplicate by LEI — the same entity can appear in multiple filter results.
  const seen = new Set<string>();
  const results: GleifSearchResult[] = [];
  for (const record of [...r1, ...r2, ...r3]) {
    if (!seen.has(record.lei)) {
      seen.add(record.lei);
      results.push(record);
    }
  }
  return results;
}
