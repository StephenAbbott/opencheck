/**
 * Former legal names (Phase 309) — the values layer.
 *
 * The web picker searches GLEIF on `entity.names` (every name a
 * record carries) rather than `entity.legalName`, so a renamed company is
 * found under the name it had — "Barrick Gold Corporation" finds BARRICK
 * MINING CORPORATION through its PREVIOUS_LEGAL_NAME — and the row must then
 * say why it matched. The subject's own former names are not here: they are
 * the "Former names" row of the "Is this the right company?" band
 * (`subjectProfile.formerNamesRow`) — Phase 311 took the short form off the
 * subject card, where it pushed the mode tabs below the fold on a phone.
 * In `lib/` because every sentence here is a claim the suite pins.
 */

/** A GLEIF other name as the picker carries it. */
export interface OtherName {
  name: string;
  type: string;
}

export const FORMERLY_LABEL = "Formerly";


/** Lower-cased, diacritics folded, punctuation dropped, one space between
 *  tokens — enough to compare a query with a name a register filed. */
export function nameKey(text: string): string {
  return text
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function tokens(text: string): string[] {
  return nameKey(text).split(" ").filter(Boolean);
}

/** True when every token of `query` appears in `name` — the picker's
 *  reading of "the query matched this name". */
export function queryMatches(query: string, name: string): boolean {
  const q = tokens(query);
  if (q.length === 0) return false;
  const n = new Set(tokens(name));
  return q.every((t) => n.has(t));
}

/** The former legal name the query matched when the current legal name did
 *  not, or null. Only GLEIF's PREVIOUS_LEGAL_NAME type counts — a trading
 *  name or a translation is never called former. */
export function matchedFormerName(
  query: string,
  legalName: string,
  otherNames: OtherName[] | undefined,
): string | null {
  if (!otherNames || otherNames.length === 0) return null;
  if (queryMatches(query, legalName)) return null;
  for (const other of otherNames) {
    if (other.type !== "PREVIOUS_LEGAL_NAME") continue;
    if (queryMatches(query, other.name)) return other.name;
  }
  return null;
}

/** "Formerly Barrick Gold Corporation — matched your search" for the row. */
export function formerlyMatchedLine(name: string): string {
  return `${FORMERLY_LABEL} ${name} — matched your search`;
}
