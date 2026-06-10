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
}

export const RA_CODES: Record<string, RaEntry> = {
  GB: {
    raCode: "RA000585",
    countryName: "United Kingdom",
    idLabel: "Companies House number",
    placeholder: "02000048",
    formatHint: "8 characters — digits or OC/SC/NI prefix + digits",
  },
  NL: {
    raCode: "RA000463",
    countryName: "Netherlands",
    idLabel: "KvK number",
    placeholder: "34362985",
    formatHint: "8 digits",
  },
  NO: {
    raCode: "RA000394",
    countryName: "Norway",
    idLabel: "Organisation number (orgnr)",
    placeholder: "923609016",
    formatHint: "9 digits",
  },
  DK: {
    raCode: "RA000170",
    countryName: "Denmark",
    idLabel: "CVR number",
    placeholder: "36213728",
    formatHint: "8 digits",
  },
  SE: {
    raCode: "RA000523",
    countryName: "Sweden",
    idLabel: "Organisation number",
    placeholder: "5560985801",
    formatHint: "10 digits (format: NNNNNN-NNNN or 10 digits without dash)",
  },
  FR: {
    raCode: "RA000580",
    countryName: "France",
    idLabel: "SIREN number",
    placeholder: "542107651",
    formatHint: "9 digits",
  },
  BE: {
    raCode: "RA000143",
    countryName: "Belgium",
    idLabel: "CBE / KBO number",
    placeholder: "0403838524",
    formatHint: "10 digits",
  },
  IE: {
    raCode: "RA000215",
    countryName: "Ireland",
    idLabel: "CRO number",
    placeholder: "012345",
    formatHint: "Up to 6 digits",
  },
  PL: {
    raCode: "RA000439",
    countryName: "Poland",
    idLabel: "KRS number",
    placeholder: "0000037171",
    formatHint: "10 digits",
  },
  AT: {
    raCode: "RA000128",
    countryName: "Austria",
    idLabel: "Firmenbuchnummer",
    placeholder: "FN123456a",
    formatHint: "FN + digits + letter suffix (e.g. FN 237338 p)",
  },
  EE: {
    raCode: "RA000181",
    countryName: "Estonia",
    idLabel: "Registration code",
    placeholder: "10138896",
    formatHint: "8 digits",
  },
  LV: {
    raCode: "RA000327",
    countryName: "Latvia",
    idLabel: "Registration number",
    placeholder: "40003571815",
    formatHint: "11 digits",
  },
  LT: {
    raCode: "RA000330",
    countryName: "Lithuania",
    idLabel: "JAR code",
    placeholder: "302511363",
    formatHint: "9 digits",
  },
  SK: {
    raCode: "RA000476",
    countryName: "Slovakia",
    idLabel: "IČO number",
    placeholder: "31320155",
    formatHint: "8 digits",
  },
  HR: {
    raCode: "RA000156",
    countryName: "Croatia",
    idLabel: "OIB",
    placeholder: "30420566661",
    formatHint: "11 digits",
  },
  SG: {
    raCode: "RA000509",
    countryName: "Singapore",
    idLabel: "UEN",
    placeholder: "196700240H",
    formatHint: "9–10 characters",
  },
  CA: {
    raCode: "RA000072",
    countryName: "Canada",
    idLabel: "Corporation number",
    placeholder: "1234567",
    formatHint: "7–9 digits (federal corporations)",
  },
};

/**
 * Ordered list for the country picker dropdown.
 * UK/NL/NO/FR/DK first (richest adapter coverage), then the rest alphabetically.
 */
export const COUNTRY_OPTIONS: { code: string; entry: RaEntry }[] = [
  "GB", "NL", "NO", "FR", "DK", "BE", "SE", "PL", "IE", "AT",
  "EE", "LV", "LT", "SK", "HR", "SG", "CA",
].map((code) => ({ code, entry: RA_CODES[code] }));
