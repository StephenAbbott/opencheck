/**
 * Ownership and control relationships that no source dates.
 *
 * Mirrors backend `opencheck/bods/undated.py` (the undated-relationships
 * ticket, 11 Nov 2026 — Rosneft's FullCheck drew BP Russian Investments as a
 * current ~20% owner four years after BP announced its exit, and none of the
 * 120 ownership relationships in that lookup carried a date). A missing date
 * means "not stated": OpenCheck still reads the relationship as current, and
 * says beside it that no date was given. `backend/tests/test_undated_relationships.py`
 * parses this file.
 */
import type { Interest } from "./bodsGraph";

/** Interest types that name an appointment, not a holding. */
export const ROLE_INTEREST_TYPES = ["seniorManagingOfficial", "boardMember", "boardChair"];

/** The line the canvas and the tree print beside an undated edge. */
export const UNDATED_LABEL = "no date given";

function isHolding(i: Interest): boolean {
  if (ROLE_INTEREST_TYPES.includes(i.type ?? "")) return false;
  const position = i.opencheckOfficerPosition;
  return !(typeof position === "string" && position.trim());
}

/**
 * True when a current edge's interests hold or control something and none
 * carries a `startDate` or an `endDate`. Pass the *current* interests of an
 * edge; an ended edge already says "ended" and is never undated.
 */
export function isUndatedInterests(interests: Interest[]): boolean {
  if (!interests.some(isHolding)) return false;
  return !interests.some((i) => Boolean(i.startDate) || Boolean(i.endDate));
}

/** The statement-level rule, as `bods/undated.py::is_undated` states it: a
 *  relationship, not closed, whose interests are undated holdings. */
export function isUndatedRelationship(s: {
  recordType?: unknown;
  recordStatus?: unknown;
  recordDetails?: unknown;
}): boolean {
  if (s.recordType !== "relationship" || s.recordStatus === "closed") return false;
  const interests = ((s.recordDetails as { interests?: unknown } | undefined)?.interests ?? []) as Interest[];
  return Array.isArray(interests) && isUndatedInterests(interests.filter((i) => i && typeof i === "object"));
}
