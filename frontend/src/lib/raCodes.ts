/**
 * GLEIF Registration Authority codes for the 21 countries that have
 * OpenCheck adapters. Used to scope the national-ID reverse lookup to
 * a single registry and avoid false matches from coincidental ID
 * collisions across registries.
 *
 * filter[entity.registeredAt]=<raCode> is the GLEIF API parameter.
 * Reference: https://www.gleif.org/en/about-lei/code-lists/gleif-registration-authorities-list
 *
 * Every code re-verified 2026-08-28 against live GLEIF records — reading
 * registeredAt.id off real entities, not off the RA catalogue, because the
 * catalogue only says which authorities exist, not which one GLEIF actually
 * files a country's companies under. Eleven of the twenty were wrong, and
 * each pointed at a real but different authority, so the country picker
 * scoped the reverse lookup to a registry the company was not registered at
 * and returned no match. Norway pointed at India's MCA (RA000394) and Sweden
 * at Singapore's ACRA (RA000523); both had already been corrected in the
 * backend and in CLAUDE.md without this file being updated.
 *
 * Keep in step with _RA_BY_COUNTRY in backend/opencheck/routers/lookup.py —
 * backend/tests/test_ra_codes.py parses THIS FILE and fails if they diverge.
 */

export interface RaEntry {
  raCode: string;
  countryName: string;
  /** Short label for the input field, e.g. "Companies House number" */
  idLabel: string;
  /** Placeholder value shown in the text input */
  placeholder: string;
  /** One-line format hint shown below the input */
  formatHint: string;
  /**
   * Optional regex used for client-side format validation.
   * Applied to the trimmed input value. Absence means "no strict check"
   * (used for countries with variable-length or complex ID formats).
   * Validation is advisory — a mismatch shows a warning but never blocks
   * submission, since GLEIF may store the ID in a normalised form.
   */
  formatPattern?: RegExp;
  /**
   * Countries whose companies are filed under more than one GLEIF authority,
   * where the registration number's prefix says which. Rules are tried in
   * order; a number matching none of them keeps `raCode`.
   *
   * Companies House is the only one OpenCheck covers. This matters more than
   * it looks: the GB format hint below explicitly invites an `SC` or `NI`
   * number, and until Phase 141 every one of them was scoped to the England &
   * Wales authority, which is the one registry those companies are guaranteed
   * *not* to be in. The query returned nothing, and nothing is what an absent
   * company also returns.
   *
   * Mirrors SUB_REGISTRIES in backend/opencheck/ra_codes.py —
   * backend/tests/test_ra_codes.py parses this file and fails if they diverge.
   */
  subRegistries?: { prefixes: string[]; raCode: string; label: string }[];
}

export const RA_CODES: Record<string, RaEntry> = {
  GB: {
    raCode: "RA000585",
    countryName: "United Kingdom",
    idLabel: "Companies House number",
    placeholder: "02000048",
    formatHint: "8 characters — digits or two-letter prefix (OC, SC, NI…) + 6 digits",
    // 8 pure digits OR two uppercase letters + 6 digits (total 8 chars).
    formatPattern: /^(?:\d{8}|[A-Z]{2}\d{6})$/i,
    subRegistries: [
      // Scottish limited companies are SC; Scottish limited partnerships and
      // qualifying partnerships are SO and SF.
      { prefixes: ["SC", "SO", "SF"], raCode: "RA000587", label: "Companies House — Scotland" },
      // Northern Irish companies are NI; NC and R0 are older registrations
      // carried over from the pre-2009 Belfast registry.
      { prefixes: ["NI", "NC", "R0"], raCode: "RA000586", label: "Companies House — Northern Ireland" },
    ],
  },
  NL: {
    raCode: "RA000463",
    countryName: "Netherlands",
    idLabel: "KvK number",
    placeholder: "34362985",
    formatHint: "8 digits",
    formatPattern: /^\d{8}$/,
  },
  NO: {
    raCode: "RA000472",
    countryName: "Norway",
    idLabel: "Organisation number (orgnr)",
    placeholder: "923609016",
    formatHint: "9 digits",
    formatPattern: /^\d{9}$/,
  },
  NZ: {
    raCode: "RA000466",
    countryName: "New Zealand",
    idLabel: "Company number",
    placeholder: "1166320",
    formatHint: "Companies Register number (digits)",
    formatPattern: /^\d{1,9}$/,
  },
  DK: {
    raCode: "RA000170",
    countryName: "Denmark",
    idLabel: "CVR number",
    placeholder: "36213728",
    formatHint: "8 digits",
    formatPattern: /^\d{8}$/,
  },
  SE: {
    raCode: "RA000544",
    countryName: "Sweden",
    idLabel: "Organisation number",
    placeholder: "5560985801",
    formatHint: "10 digits, optionally written as NNNNNN-NNNN",
    // 10 pure digits OR NNNNNN-NNNN (with dash).
    formatPattern: /^\d{10}$|^\d{6}-\d{4}$/,
  },
  FR: {
    raCode: "RA000189",
    countryName: "France",
    idLabel: "SIREN number",
    placeholder: "542107651",
    formatHint: "9 digits",
    formatPattern: /^\d{9}$/,
  },
  BE: {
    raCode: "RA000025",
    countryName: "Belgium",
    idLabel: "CBE / KBO number",
    placeholder: "0403838524",
    formatHint: "10 digits",
    formatPattern: /^\d{10}$/,
  },
  BR: {
    raCode: "RA000681",
    countryName: "Brazil",
    idLabel: "CNPJ",
    placeholder: "33.000.167/0001-01",
    formatHint: "14 digits (CNPJ), with or without punctuation",
    // 14 plain digits OR the punctuated XX.XXX.XXX/XXXX-XX form.
    formatPattern: /^\d{14}$|^\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}$/,
  },
  IE: {
    raCode: "RA000402",
    countryName: "Ireland",
    idLabel: "CRO number",
    placeholder: "012345",
    formatHint: "Up to 6 digits",
    formatPattern: /^\d{1,6}$/,
  },
  PL: {
    raCode: "RA000484",
    countryName: "Poland",
    idLabel: "KRS number",
    placeholder: "0000037171",
    formatHint: "10 digits",
    formatPattern: /^\d{10}$/,
  },
  AT: {
    raCode: "RA000017",
    countryName: "Austria",
    idLabel: "Firmenbuchnummer",
    placeholder: "FN123456a",
    formatHint: "FN + digits + letter suffix (e.g. FN 237338 p or FN123456a)",
    // Optional "FN" prefix + optional space + digits + optional space + single letter suffix.
    formatPattern: /^(?:FN\s*)?\d+\s*[a-z]?$/i,
  },
  EE: {
    raCode: "RA000181",
    countryName: "Estonia",
    idLabel: "Registration code",
    placeholder: "10138896",
    formatHint: "8 digits",
    formatPattern: /^\d{8}$/,
  },
  LV: {
    raCode: "RA000423",
    countryName: "Latvia",
    idLabel: "Registration number",
    placeholder: "40003571815",
    formatHint: "11 digits",
    formatPattern: /^\d{11}$/,
  },
  LT: {
    raCode: "RA000430",
    countryName: "Lithuania",
    idLabel: "JAR code",
    placeholder: "302511363",
    formatHint: "9 digits",
    formatPattern: /^\d{9}$/,
  },
  MT: {
    raCode: "RA000443",
    countryName: "Malta",
    idLabel: "Registration number (C-number)",
    placeholder: "C 113927",
    formatHint: "Letter prefix + number, e.g. C 12345",
    // Short letter prefix (commonly C) + digits, with or without a space.
    formatPattern: /^[A-Z]{1,3}\s*\d+$/i,
  },
  SK: {
    raCode: "RA000526",
    countryName: "Slovakia",
    idLabel: "IČO number",
    placeholder: "31320155",
    formatHint: "8 digits",
    formatPattern: /^\d{8}$/,
  },
  HR: {
    raCode: "RA000156",
    countryName: "Croatia",
    idLabel: "OIB",
    placeholder: "30420566661",
    formatHint: "11 digits",
    formatPattern: /^\d{11}$/,
  },
  SG: {
    raCode: "RA000523",
    countryName: "Singapore",
    idLabel: "UEN",
    placeholder: "196700240H",
    formatHint: "9–10 alphanumeric characters",
    // Local companies: 9 digits + check letter. Foreign/other: various.
    // Accept 9-10 alphanumeric chars as the common denominator.
    formatPattern: /^[A-Z0-9]{9,10}$/i,
  },
  CA: {
    raCode: "RA000072",
    countryName: "Canada",
    idLabel: "Corporation number",
    placeholder: "1234567",
    formatHint: "7–9 digits (federal corporations)",
    formatPattern: /^\d{7,9}$/,
  },
  GR: {
    raCode: "RA000685",
    countryName: "Greece",
    idLabel: "Αριθμός ΓΕΜΗ (GEMI number)",
    placeholder: "160228803000",
    // ΓΕΜΗ numbers are 9-12 digits. GLEIF stores some zero-padded to 12 and
    // some not, and the ΓΕΜΗ API accepts either, so do not normalise here.
    formatHint: "9–12 digits",
    formatPattern: /^\d{9,12}$/,
  },
};

/**
 * Alphabetical by country name. UK is the default selected value (set in
 * App.tsx state) but sits in its natural A–Z position in the list.
 *
 * Derived from RA_CODES rather than hand-listed: the hand-listed version had
 * silently omitted New Zealand, so a country with a correct RA code, a format
 * hint and a working backend mapping could not be picked at all. Greece was
 * missing from the file entirely, having been added to the backend map when
 * the ΓΕΜΗ adapter shipped. Neither produced an error — an option that does
 * not exist simply is not there to notice.
 */
export const COUNTRY_OPTIONS: { code: string; entry: RaEntry }[] = Object.entries(RA_CODES)
  .map(([code, entry]) => ({ code, entry }))
  .sort((a, b) => a.entry.countryName.localeCompare(b.entry.countryName, "en"));

/**
 * Returns true if `value` is empty, the country has no pattern defined,
 * or `value` matches the country's formatPattern.
 *
 * Validation is advisory — callers should warn but not block submission.
 */
export function validateNationalId(countryCode: string, value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) return true;
  const pattern = RA_CODES[countryCode]?.formatPattern;
  if (!pattern) return true;
  return pattern.test(trimmed);
}

/**
 * The RA code a reverse lookup should be scoped to — the country's authority,
 * refined by the registration number where the country has sub-registries.
 *
 * Callers should always pass the number they have. `RA_CODES[c].raCode` alone
 * answers "which registry is this country's", which is not the same question
 * as "which registry is this company in", and for GB the two differ for every
 * Scottish and Northern Irish company.
 *
 * Returns "" for an unknown country, which leaves the GLEIF query unscoped —
 * a wider search rather than a wrong one.
 */
export function raCodeFor(countryCode: string, value = ""): string {
  const entry = RA_CODES[countryCode];
  if (!entry) return "";
  const trimmed = value.trim().toUpperCase();
  if (trimmed) {
    for (const rule of entry.subRegistries ?? []) {
      if (rule.prefixes.some((p) => trimmed.startsWith(p))) return rule.raCode;
    }
  }
  return entry.raCode;
}
