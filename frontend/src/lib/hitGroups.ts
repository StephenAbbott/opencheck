/**
 * Rows a reader cannot tell apart, collapsed to one (Phase 133).
 *
 * A live BP lookup put four rows on the OpenAleph card reading, in full:
 *
 *     Bp P.L.C.
 *     Indexed in Companies House (UK) Persons with Significant Control.
 *     collection: Companies House (UK) PSC · Company · FtM match score 107 ·
 *     identifier corroborated
 *
 * — four times over, identical to the pixel. They are not duplicates. They are
 * four distinct FollowTheMoney entities: BP is named as a person with
 * significant control of four different UK companies, and OpenAleph holds one
 * PSC record for each. The thing that distinguishes them — *which* company —
 * is not on the record. It survives only inside the upstream entity id
 * (`gb-coh-psc-00593645-…`), and reading a company number out of an
 * undocumented id convention to print beside a name would be asserting who BP
 * controls on the strength of a string format that upstream never promised.
 * So OpenCheck does not say it.
 *
 * What it can say is how many there are. One row per distinct rendering, with
 * the count — the count being exactly the information the repetition carried,
 * as with the citation chips two phases earlier.
 *
 * **Grouping is by what the row renders**, not by any identifier: the defect
 * is visual sameness, and any key narrower than "everything the reader sees"
 * leaves rows that still look identical. Any key wider — the source id, say —
 * would collapse rows that differ.
 *
 * The lead is the first in adapter order, which for every adapter that can
 * produce a group is the best-scoring record. The drawer on the collapsed row
 * therefore opens the closest match, and the row says how many others there
 * are so the drawer is not silently standing in for all of them.
 */

import type { SourceHit } from "./api";

export interface Group<T> {
  /** The item whose row is rendered, and whose drawer opens. */
  lead: T;
  /** How many items render identically, including the lead. */
  count: number;
  /** The others, in order — kept so a caller can count or list them. */
  rest: T[];
}

export type HitGroup = Group<SourceHit>;

/**
 * A retrieval score, as it appears in an OpenAleph summary fragment.
 *
 * `FtM match score 107` and `FtM match score 106` are the same answer to the
 * reader's question. It is a BM25 relevance number on no published scale —
 * two of BP's four PSC records scored 107 and one scored 106, which is noise
 * that happens to be printed, and it kept one row out of its own group. Match
 * *strength*, where OpenCheck asserts it, is `ui/MatchConfidenceChip`; this is
 * not that.
 */
const RETRIEVAL_SCORE = /\s*\u00b7?\s*FtM match score\s+[\d.]+/gi;

/**
 * What the reader can use to tell one row from another.
 *
 * Everything the row renders, less the retrieval score — the defect is rows
 * that say the same thing, and a key narrower than "what the row says" would
 * collapse rows that differ. The score is excluded because it is not part of
 * what the row says about the name: it is metadata about how the record was
 * found, and no reader can calibrate a one-point difference in it.
 */
function displayKey(hit: SourceHit): string {
  return [hit.name ?? "", hit.finding ?? "", hit.summary ?? ""]
    .join(" ")
    .replace(RETRIEVAL_SCORE, "");
}

/**
 * Collapse anything that renders identically, preserving first-seen order.
 *
 * Generic because the defect is not specific to source rows: the same BP
 * lookup showed one related party twice in the archive-matches list, for the
 * same reason — two upstream records the row has no way to distinguish.
 */
export function groupIdentical<T>(
  items: T[],
  key: (item: T) => string
): Group<T>[] {
  const byKey = new Map<string, Group<T>>();
  const order: string[] = [];
  for (const item of items) {
    const k = key(item);
    const existing = byKey.get(k);
    if (existing) {
      existing.count += 1;
      existing.rest.push(item);
      continue;
    }
    byKey.set(k, { lead: item, count: 1, rest: [] });
    order.push(k);
  }
  return order.map((k) => byKey.get(k)!);
}

/** Group a source card's rows, preserving first-seen order. */
export function groupHitsForDisplay(hits: SourceHit[]): HitGroup[] {
  return groupIdentical(hits, displayKey);
}

/**
 * The sentence a collapsed row carries.
 *
 * It states two things and infers neither: that the source holds this many
 * records which say the same thing about this name, and that what is open
 * below is one of them. "records", because a FollowTheMoney entity is a
 * record — not a "result", which is the word for a row.
 */
export function siblingNote(count: number): string | null {
  if (count <= 1) return null;
  return `${count} records here carry this name and say the same thing about it; the data below is the closest match.`;
}

/** The same, for a list whose rows have no drawer under them. */
export function repeatNote(count: number): string | null {
  if (count <= 1) return null;
  return `${count} records`;
}
