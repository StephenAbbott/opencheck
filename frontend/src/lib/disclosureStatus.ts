/**
 * What each source says about the subject's owners — the values layer for the
 * `disclosure_status` lookup event (backend `opencheck/disclosure.py`).
 *
 * One normalised value per source, for what it covers. Stephen's decisions
 * (3 Nov 2026): it describes and raises nothing; every value a source supports
 * is listed (no precedence rule); the chip sits on QuickCheck source cards;
 * sources that are not ownership sources are named on one line, not chipped.
 *
 * `VALUE_WORDS` and `COVERS_WORDS` are parsed by
 * `backend/tests/test_disclosure_status.py` — change both sides together.
 */

export type DisclosureValue =
  | "declared"
  | "none_exist"
  | "exempt"
  | "not_provided"
  | "none_filed"
  | "restricted"
  | "unreachable"
  | "no_record"
  | "not_covered";

export type DisclosureCovers = "beneficial_owners" | "shareholders" | "parent";

export interface DisclosureEntry {
  value: DisclosureValue;
  covers: DisclosureCovers | null;
  /** Distinct parties, on a `declared` entry. */
  count?: number;
  /** The source's own words: a PSC statement, a GLEIF exception meaning. */
  details?: string[];
  /** The BODS `unspecifiedReason` or GLEIF exception code behind the value. */
  reasons?: string[];
}

export interface SourceDisclosure {
  values: DisclosureValue[];
  entries: DisclosureEntry[];
  /** True for a source whose job includes owners. */
  ownership_source: boolean;
}

export interface DisclosureStatusPayload {
  sources: Record<string, SourceDisclosure>;
  not_covered: string[];
  as_of: string;
  subject_ids?: string[];
}

export const VALUE_WORDS: Record<DisclosureValue, string> = {
  declared: "Declared",
  none_exist: "Source says none exist",
  exempt: "Exempt",
  not_provided: "Not provided",
  none_filed: "None on record",
  restricted: "Not public",
  unreachable: "Not read",
  no_record: "No record of the company",
  not_covered: "Not an ownership source",
};

export const COVERS_WORDS: Record<DisclosureCovers, string> = {
  beneficial_owners: "Beneficial owners",
  shareholders: "Shareholders",
  parent: "Parent",
};

/** "Beneficial owners: Declared (3)". */
export function entryLabel(entry: DisclosureEntry): string {
  const words = VALUE_WORDS[entry.value] ?? entry.value;
  const count = entry.count ? ` (${entry.count})` : "";
  const head = entry.covers ? `${COVERS_WORDS[entry.covers]}: ` : "";
  return `${head}${words}${count}`;
}

/**
 * The entries a source card chips. Nothing for a source whose only value is
 * `not_covered` (it is named on the one line instead) or `no_record` (that
 * card is not drawn — the "Answered with no record" list says it). A card
 * already saying "Did not answer" gets no "Not read" chip on top; a partial
 * read or an unread beneficial-owner tier keeps it, because it says what was
 * not read.
 */
export function chipEntries(
  status: SourceDisclosure | undefined,
  opts: { cardFailed?: boolean } = {},
): DisclosureEntry[] {
  if (!status) return [];
  return status.entries.filter((e) => {
    if (e.value === "not_covered" || e.value === "no_record") return false;
    if (e.value === "unreachable" && opts.cardFailed) return false;
    return true;
  });
}

/** `warn` for what was not read — incomplete, not failed; `neutral` for
 *  everything the source said. Never `risk`: this is a description. */
export function entryTone(entry: DisclosureEntry): "neutral" | "warn" {
  return entry.value === "unreachable" ? "warn" : "neutral";
}

/** The source's own words behind its values, once each. */
export function entryDetails(entries: DisclosureEntry[]): string[] {
  const out: string[] = [];
  for (const e of entries) {
    for (const d of e.details ?? []) {
      const line = `${e.covers ? COVERS_WORDS[e.covers] : ""}${e.covers ? " — " : ""}${d}`;
      if (!out.includes(line)) out.push(line);
    }
  }
  return out;
}

/** "Not ownership sources: OpenSanctions, TED." — or null. */
export function notCoveredLine(
  payload: DisclosureStatusPayload | null | undefined,
  label: (id: string) => string,
): string | null {
  const ids = payload?.not_covered ?? [];
  if (ids.length === 0) return null;
  return `Not ownership sources: ${ids.map(label).join(", ")}.`;
}
