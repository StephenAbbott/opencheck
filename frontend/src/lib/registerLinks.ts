/**
 * How each national register addresses a company's own page — the frontend
 * mirror of `backend/opencheck/register_links.py` (Phase 296).
 *
 * One table for every register link OpenCheck draws: the source cards
 * (`sourceEntityUrl` in SourceBucketCard.tsx), the History tab (`recordUrl` in
 * historyMode.ts) and, through the backend, the subject header's "Register
 * record" line. Before Phase 296 those were three separate copies, and four of
 * the source-card links opened a page that did not show the company.
 *
 * `record` is an ordered list of `[pattern, template]`: the first pattern the
 * register number fully matches picks the template (`{id}` is substituted). A
 * number that matches none gets no link — never a guessed one. `history`
 * overrides `record` on the History tab. Patterns are anchored with `^…$` and
 * use only syntax Python's `re` and JavaScript's `RegExp` read the same way.
 *
 * The block between the BEGIN/END markers is parsed as JSON by
 * backend/tests/test_register_links.py, which fails if it differs from the
 * backend table — keep it JSON-shaped (double quotes, no trailing commas, no
 * comments inside it). Why each template is what it is, and how each was
 * verified, is written beside its entry in the backend module.
 */

export interface RegisterLinkEntry {
  /** The register's name as the subject header shows it. */
  register: string;
  /** The derived key the register's number is stored under (hit.identifiers). */
  identifierKey: string;
  record: [string, string][];
  history?: [string, string][];
}

// BEGIN REGISTER_LINKS
export const REGISTER_LINKS: Record<string, RegisterLinkEntry> = {
  "companies_house": {
    "register": "Companies House",
    "identifierKey": "gb_coh",
    "record": [
      ["^[A-Z0-9]{8}$", "https://find-and-update.company-information.service.gov.uk/company/{id}"]
    ],
    "history": [
      ["^[A-Z0-9]{8}$", "https://find-and-update.company-information.service.gov.uk/company/{id}/filing-history"]
    ]
  },
  "kvk": {
    "register": "KvK Handelsregister",
    "identifierKey": "kvk_number",
    "record": [
      ["^\\d{8}$", "https://www.kvk.nl/bestellen/?kvknummer={id}"]
    ]
  },
  "zefix": {
    "register": "Swiss UID register (FSO)",
    "identifierKey": "che_uid",
    "record": [
      ["^CHE\\d{9}$", "https://www.uid.admin.ch/Detail.aspx?uid_id={id}"]
    ]
  },
  "inpi": {
    "register": "INPI (Registre national des entreprises)",
    "identifierKey": "siren",
    "record": [
      ["^\\d{9}$", "https://data.inpi.fr/entreprises/{id}"]
    ]
  },
  "ariregister": {
    "register": "e-Business Register (RIK)",
    "identifierKey": "ee_registry_code",
    "record": [
      ["^\\d{8}$", "https://ariregister.rik.ee/eng/company/{id}"]
    ]
  },
  "nz_companies": {
    "register": "NZ Companies Register",
    "identifierKey": "nz_company_number",
    "record": [
      ["^94\\d{11}$", "https://www.nzbn.govt.nz/mynzbn/nzbndetails/{id}/"],
      ["^\\d{1,8}$", "https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/{id}"]
    ]
  },
  "brreg": {
    "register": "Brønnøysundregistrene",
    "identifierKey": "no_orgnr",
    "record": [
      ["^\\d{9}$", "https://virksomhet.brreg.no/nb/oppslag/enheter/{id}"]
    ]
  },
  "prh": {
    "register": "YTJ (PRH)",
    "identifierKey": "fi_ytunnus",
    "record": [
      ["^\\d{7}-\\d$", "https://tietopalvelu.ytj.fi/yritys/{id}"]
    ]
  },
  "ur_latvia": {
    "register": "Latvian Register of Enterprises",
    "identifierKey": "lv_regcode",
    "record": [
      ["^\\d{11}$", "https://info.ur.gov.lv/#/legal-entity/{id}"]
    ]
  },
  "ares": {
    "register": "ARES",
    "identifierKey": "cz_ico",
    "record": [
      ["^\\d{8}$", "https://ares.gov.cz/ekonomicke-subjekty?ico={id}"]
    ]
  },
  "bce_belgium": {
    "register": "Crossroads Bank for Enterprises (KBO/BCE)",
    "identifierKey": "be_enterprise_number",
    "record": [
      ["^\\d{10}$", "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={id}"]
    ]
  },
  "corporations_canada": {
    "register": "Corporations Canada",
    "identifierKey": "ca_corp_id",
    "record": [
      ["^\\d{8}$", "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html?corpId={id}"]
    ]
  },
  "abr_australia": {
    "register": "ABN Lookup",
    "identifierKey": "au_abn",
    "record": [
      ["^\\d{11}$", "https://abr.business.gov.au/ABN/View?abn={id}"]
    ]
  },
  "cro": {
    "register": "CRO",
    "identifierKey": "ie_crn",
    "record": [
      ["^\\d{1,7}$", "https://core.cro.ie/company/{id}"]
    ]
  },
  "cvr_denmark": {
    "register": "CVR",
    "identifierKey": "dk_cvr",
    "record": [
      ["^\\d{8}$", "https://datacvr.virk.dk/enhed/virksomhed/{id}"]
    ]
  },
  "jar_lithuania": {
    "register": "Register of Legal Entities (JAR)",
    "identifierKey": "lt_code",
    "record": [
      ["^\\d{9}$", "https://www.registrucentras.lt/jar/p/index.php?kod={id}"]
    ]
  },
  "ny_dos": {
    "register": "NY Department of State",
    "identifierKey": "us_ny_dos_id",
    "record": [],
    "history": [
      ["^\\d+$", "https://data.ny.gov/resource/63wc-4exh.json?corpid_num={id}"]
    ]
  }
};
// END REGISTER_LINKS

/** The number as given, then with spaces, dots and hyphens removed. */
function candidates(identifier: string): string[] {
  const raw = (identifier ?? "").trim();
  const compact = raw.replace(/[\s.\-]/g, "");
  return compact !== raw ? [raw, compact] : [raw];
}

function pick(rules: [string, string][], identifier: string): string | null {
  for (const candidate of candidates(identifier)) {
    if (!candidate) continue;
    for (const [pattern, template] of rules) {
      if (new RegExp(pattern).test(candidate)) return template.replace("{id}", candidate);
    }
  }
  return null;
}

/** The register's own page for a company, or null when there is no address
 *  for it (no entry, no per-company page, or a number of the wrong shape). */
export function registerRecordUrl(sourceId: string, identifier: string | null | undefined): string | null {
  const entry = REGISTER_LINKS[sourceId];
  if (!entry || !identifier) return null;
  return pick(entry.record, identifier);
}

/** The page the History tab links a register's dated rows to. */
export function registerHistoryUrl(sourceId: string, identifier: string | null | undefined): string | null {
  const entry = REGISTER_LINKS[sourceId];
  if (!entry || !identifier) return null;
  return pick(entry.history ?? entry.record, identifier);
}

/** The `register_record` the anchor event carries (Phase 296). */
export interface RegisterRecord {
  source_id: string;
  register: string;
  identifier: string;
  url: string;
  ra_code?: string;
}

export const REGISTER_RECORD_LABEL = "Register record";

export const REGISTER_RECORD_EXPLANATION =
  "The company's own page on its home register, built from the registration " +
  "authority and registration number on its LEI record. It does not depend on " +
  "OpenCheck having reached that register in this check, so it is here even " +
  "when the register's card above is degraded or was not queried.";

/** A source card's register link: the number the hit's `identifiers` carry
 *  under the register's key, else the hit id — and only when the register has
 *  an entry and the number has the shape it addresses. */
export function hitRegisterUrl(
  sourceId: string,
  hit: { hit_id: string; identifiers?: Record<string, string> | null },
): string | null {
  const entry = REGISTER_LINKS[sourceId];
  if (!entry) return null;
  return (
    registerRecordUrl(sourceId, hit.identifiers?.[entry.identifierKey]) ??
    registerRecordUrl(sourceId, hit.hit_id)
  );
}
