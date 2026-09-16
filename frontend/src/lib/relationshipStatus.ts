/**
 * Has this relationship ended? (Phase 219)
 *
 * BODS v0.4 says a relationship has ended in two places, and a publisher may
 * use either or both:
 *
 *   - the **record**: `recordStatus: "closed"` on the relationship statement;
 *   - each **interest**: an `endDate`.
 *
 * The Open Ownership UK PSC extract OpenCheck's demo graph is built from has
 * both on 78 relationships, and six closed relationships with no `endDate` at
 * all — so reading one of the two misses a case the other catches. CAC
 * Nigeria's INACTIVE rows are the live example of the second shape: closed,
 * with no cessation date published.
 *
 * Until this module the diagrams read neither, and a ceased PSC drew exactly
 * like a current one. BOVS has no rule for historical relationships, but its
 * completeness rule ("no Party may be omitted") keeps them on the diagram and
 * its relevance rule allows less relevant parts to be drawn "through tinting
 * or transparency" — which is what the canvas does with the answer here.
 *
 * `backend/opencheck/bods/lifecycle.py` states the same rule for the PDF
 * diagram. The two are pinned by parallel tests over the same cases, not by a
 * shared file; if you change one, change the other.
 */

export interface EndableInterest {
  endDate?: string;
}

/** Today as `YYYY-MM-DD`, UTC — the form BODS dates compare in. */
export function todayIso(now: Date = new Date()): string {
  return now.toISOString().slice(0, 10);
}

/** The date part of a BODS date or date-time, or undefined when unusable. */
function isoDay(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const m = value.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : undefined;
}

/**
 * An interest has ended when its record is closed, or when its `endDate` is on
 * or before `asOf`. A future `endDate` is a scheduled end, not an ended one —
 * saying "ended" about it would be untrue today.
 */
export function interestEnded(
  interest: EndableInterest,
  recordClosed: boolean,
  asOf: string = todayIso()
): boolean {
  if (recordClosed) return true;
  const end = isoDay(interest.endDate);
  return end !== undefined && end <= asOf;
}

export interface RelationshipLifecycle {
  /** Every interest has ended, or the record is closed. */
  ended: boolean;
  /** The latest `endDate` among the ended interests, when any was published.
   *  Undefined for a closed record that names no date — never invented. */
  endedOn?: string;
}

/**
 * The lifecycle of one relationship statement (or of several pooled onto one
 * edge — pass all their interests and whether *every* record was closed).
 *
 * A relationship with no interests is ended only when its record is closed:
 * there is nothing else to read.
 */
export function relationshipLifecycle(
  interests: EndableInterest[],
  recordClosed: boolean,
  asOf: string = todayIso()
): RelationshipLifecycle {
  const ended =
    recordClosed ||
    (interests.length > 0 && interests.every((i) => interestEnded(i, false, asOf)));
  if (!ended) return { ended: false };
  let endedOn: string | undefined;
  for (const i of interests) {
    const end = isoDay(i.endDate);
    if (end !== undefined && end <= asOf && (endedOn === undefined || end > endedOn)) endedOn = end;
  }
  return endedOn ? { ended: true, endedOn } : { ended: true };
}

/** Is this statement's record closed? */
export function recordClosed(stmt: Record<string, unknown>): boolean {
  return stmt.recordStatus === "closed";
}

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

/**
 * "ended 30 November 2024", or "ended" when no date was published. Parsed by
 * hand: `new Date("2024-11-30")` is midnight UTC, which a negative-offset
 * timezone renders as the day before.
 */
export function endedPhrase(endedOn?: string): string {
  if (!endedOn) return "ended";
  const [y, m, d] = endedOn.split("-").map(Number);
  if (!y || !m || !d || m > 12) return "ended";
  return `ended ${d} ${MONTHS[m - 1]} ${y}`;
}

/**
 * The tree's interest cell (Phase 219): the edge label's first line, and for
 * an ended edge the "ended <date>" phrase after it. An ended edge with no
 * interest label of its own already reads as the phrase alone, so the phrase
 * is not said twice.
 */
export function interestCellText(label: string | undefined, ended: boolean, endedOn?: string): string {
  const first = (label ?? "").split("\n")[0];
  if (!ended) return first;
  const phrase = endedPhrase(endedOn);
  return !first || first === phrase ? phrase : `${first} · ${phrase}`;
}
