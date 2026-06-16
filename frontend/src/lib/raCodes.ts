/**
 * GLEIF Registration Authority codes for the 17 countries that have
 * OpenCheck adapters. Used to scope the national-ID reverse lookup to
 * a single registry and avoid false matches from coincidental ID
 * collisions across registries.
 *
 * filter[entity.registeredAt]=<raCode> is the GLEIF API parameter.
 * Reference: https://www.gleif.org/en/about-lei/code-lists/gleif-registration-authorities-list
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
    raCode: "RA000394",
    countryName: "Norway",
    idLabel: "Organisation number (orgnr)",
    placeholder: "923609016",
    formatHint: "9 digits",
    formatPattern: /^\d{9}$/,
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
    raCode: "RA000523",
    countryName: "Sweden",
    idLabel: "Organisation number",
    placeholder: "5560985801",
    formatHint: "10 digits, optionally written as NNNNNN-NNNN",
    // 10 pure digits OR NNNNNN-NNNN (with dash).
    formatPattern: /^\d{10}$|^\d{6}-\d{4}$/,
  },
  FR: {
    raCode: "RA000580",
    countryName: "France",
    idLabel: "SIREN number",
    placeholder: "542107651",
    formatHint: "9 digits",
    formatPattern: /^\d{9}$/,
  },
  BE: {
    raCode: "RA000143",
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
    raCode: "RA000215",
    countryName: "Ireland",
    idLabel: "CRO number",
    placeholder: "012345",
    formatHint: "Up to 6 digits",
    formatPattern: /^\d{1,6}$/,
  },
  PL: {
    raCode: "RA000439",
    countryName: "Poland",
    idLabel: "KRS number",
    placeholder: "0000037171",
    formatHint: "10 digits",
    formatPattern: /^\d{10}$/,
  },
  AT: {
    raCode: "RA000128",
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
    raCode: "RA000327",
    countryName: "Latvia",
    idLabel: "Registration number",
    placeholder: "40003571815",
    formatHint: "11 digits",
    formatPattern: /^\d{11}$/,
  },
  LT: {
    raCode: "RA000330",
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
    raCode: "RA000476",
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
    raCode: "RA000509",
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
};

/**
 * Alphabetical by country name. UK is the default selected value (set in
 * App.tsx state) but sits in its natural A–Z position in the list.
 */
export const COUNTRY_OPTIONS: { code: string; entry: RaEntry }[] = [
  "AT", "BE", "BR", "CA", "HR", "DK", "EE", "FR", "IE", "LV", "LT",
  "MT", "NL", "NO", "PL", "SG", "SK", "SE", "GB",
].map((code) => ({ code, entry: RA_CODES[code] }));

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
