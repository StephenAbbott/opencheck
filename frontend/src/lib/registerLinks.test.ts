/**
 * Phase 296 — the frontend half of the one register-link table.
 *
 * backend/tests/test_register_links.py parses REGISTER_LINKS out of this
 * module's source and fails if it differs from the backend table, so these
 * tests pin what only the frontend does with it: the same URLs from the same
 * GLEIF samples (opened against the live registers on 6 Oct 2026), the
 * hit-level fallback, and "no address means no link".
 */
import { describe, expect, it } from "vitest";
import {
  REGISTER_LINKS,
  hitRegisterUrl,
  registerHistoryUrl,
  registerRecordUrl,
} from "./registerLinks";

describe("registerRecordUrl", () => {
  it.each([
    ["companies_house", "03751777", "https://find-and-update.company-information.service.gov.uk/company/03751777"],
    ["kvk", "83235035", "https://www.kvk.nl/bestellen/?kvknummer=83235035"],
    ["zefix", "CHE112229805", "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE112229805"],
    ["ariregister", "16905950", "https://ariregister.rik.ee/eng/company/16905950"],
    ["nz_companies", "4767319", "https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/4767319"],
    ["nz_companies", "9429039305992", "https://www.nzbn.govt.nz/mynzbn/nzbndetails/9429039305992/"],
    ["brreg", "932862581", "https://virksomhet.brreg.no/nb/oppslag/enheter/932862581"],
    ["prh", "2798110-1", "https://tietopalvelu.ytj.fi/yritys/2798110-1"],
    ["ur_latvia", "40203552355", "https://info.ur.gov.lv/#/legal-entity/40203552355"],
    ["ares", "24505285", "https://ares.gov.cz/ekonomicke-subjekty?ico=24505285"],
    ["inpi", "327048260", "https://data.inpi.fr/entreprises/327048260"],
  ])("%s %s", (sourceId, number, url) => {
    expect(registerRecordUrl(sourceId, number)).toBe(url);
  });

  it("compacts a displayed number before matching", () => {
    expect(registerRecordUrl("zefix", "CHE-469.102.316")).toBe(
      "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE469102316",
    );
  });

  it.each([
    ["corporations_canada", "1719748"],
    ["companies_house", "3751777"],
    ["ny_dos", "49779"],
    ["firmenbuch", "355240m"],
    ["sudreg_croatia", "030020445"],
    ["kvk", ""],
    ["kvk", null],
  ])("gives no link for %s %s", (sourceId, number) => {
    expect(registerRecordUrl(sourceId, number as string | null)).toBeNull();
  });

  it("never points a register at a search page", () => {
    for (const entry of Object.values(REGISTER_LINKS)) {
      for (const [, template] of [...entry.record, ...(entry.history ?? [])]) {
        expect(template).not.toMatch(/zoeken|\/search\?|[?&]q=/);
      }
    }
  });
});

describe("registerHistoryUrl", () => {
  it("uses the history override, else the record page", () => {
    expect(registerHistoryUrl("companies_house", "00358949")).toBe(
      "https://find-and-update.company-information.service.gov.uk/company/00358949/filing-history",
    );
    expect(registerHistoryUrl("ny_dos", "49779")).toBe(
      "https://data.ny.gov/resource/63wc-4exh.json?corpid_num=49779",
    );
    expect(registerHistoryUrl("cvr_denmark", "24256790")).toBe(
      "https://datacvr.virk.dk/enhed/virksomhed/24256790",
    );
  });
});

describe("hitRegisterUrl", () => {
  it("prefers the number under the register's identifier key", () => {
    expect(
      hitRegisterUrl("nz_companies", {
        hit_id: "9429041026625",
        identifiers: { nz_company_number: "2288120", nzbn: "9429041026625" },
      }),
    ).toBe("https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/2288120");
  });

  it("falls back to the hit id when it has the register's shape", () => {
    expect(hitRegisterUrl("kvk", { hit_id: "83235035", identifiers: {} })).toBe(
      "https://www.kvk.nl/bestellen/?kvknummer=83235035",
    );
  });

  it("gives nothing for a source without an entry or a number of the wrong shape", () => {
    expect(hitRegisterUrl("opensanctions", { hit_id: "NK-abc", identifiers: {} })).toBeNull();
    expect(hitRegisterUrl("kvk", { hit_id: "kvk-search-1", identifiers: {} })).toBeNull();
  });
});
