/**
 * Leads for a retired LEI that names no successor (Phase 308) — the values
 * layer. `components/cdd/RetiredLeiLeads.tsx` renders what is decided here,
 * in `lib/` because the frontend suite is logic-only and every sentence
 * below is a claim worth pinning.
 *
 * Three-quarters of INACTIVE LEI records end in a dissolution or a
 * liquidation with no successor named, so for most retired LEIs the Phase
 * 307 follow-forward row says "GLEIF names no successor on this LEI record"
 * and stops. The block beneath it offers what GLEIF's own data can still
 * say — the parent it last filed, and active records with a similar name —
 * as leads, never as a successor: the heading says so, every candidate
 * wears a "Name only" chip, and nothing here is ever asserted, exported or
 * saved.
 */

import type { LeadCandidate, LeadParent, LeadsResponse, SubjectProfile } from "./api";

/** The block renders only when the profile says the company has ended and
 *  GLEIF names no successor. A live company, a company with a successor or
 *  a duplicate registration never asks for leads. */
export function leadsEligible(profile: SubjectProfile | null | undefined): boolean {
  if (!profile) return false;
  if (profile.register_status?.liveness !== "terminal") return false;
  return profile.lei_successor?.relation === "none";
}

export const LEADS_HEADING = "Where to look next — leads, not a successor";

/** The one sentence under the heading. The server's `note` is the same
 *  claim in the API; this is the page's. */
export const LEADS_INTRO =
  "GLEIF names no successor on this record. What follows is not asserted: a similar name is a name match only, and the parent is what GLEIF last filed, which may itself have ended.";

export const PARENT_LABEL = "Parent GLEIF last filed";
export const CANDIDATES_LABEL = "Active LEI records with a similar name";
export const OPEN_LABEL = "Open";

/** "GLEIF found no active record with a similar name" — said in words,
 *  never an empty list, because an empty list reads as "nothing to see". */
export const NO_CANDIDATES = "GLEIF holds no active LEI record with a similar name.";
export const NO_PARENT = "GLEIF holds no parent relationship for this record.";

/** Why GLEIF could not be asked, by `gleif_unavailable_reason`. Mirrors the
 *  securities panel's vocabulary: three reasons, and a failure never reads
 *  as "nothing similar exists". */
export const UNAVAILABLE_LINES: Record<string, string> = {
  held_for_lookups: "GLEIF was not searched: the request budget is being held for lookups. Try again in a minute.",
  rate_limited: "GLEIF was not searched: it is limiting requests. Try again in a minute.",
  unreachable: "GLEIF could not be searched for a similar name.",
};

export function unavailableLine(reason: string | null | undefined): string | null {
  if (!reason) return null;
  return UNAVAILABLE_LINES[reason] ?? UNAVAILABLE_LINES.unreachable;
}

/** Match-tier words for the candidate line, from `search_rank.MATCH_TIERS`. */
const TIER_WORDS: Record<string, string> = {
  exact: "same name",
  same_name: "same name, different legal form",
  all_tokens: "every word of the name",
  distinctive_tokens: "the distinctive words of the name",
};

export function tierWords(match: string): string {
  return TIER_WORDS[match] ?? "a similar name";
}

/** GLEIF's other-name types, in words. */
const NAME_TYPE_WORDS: Record<string, string> = {
  PREVIOUS_LEGAL_NAME: "former legal name",
  TRADING_OR_OPERATING_NAME: "trading or operating name",
  ALTERNATIVE_LANGUAGE_LEGAL_NAME: "legal name in another language",
};

export function nameTypeWords(type: string | null | undefined): string {
  if (!type) return "other name";
  return NAME_TYPE_WORDS[type] ?? type.toLowerCase().replace(/_/g, " ");
}

/** "ACCESS BANK PLC · NG · LEI issued — same name", or, when the match
 *  rests on another name GLEIF files, "… — every word of the name, as its
 *  former legal name “Barrick Gold Corporation”". */
export function candidateLine(c: LeadCandidate): string {
  const bits = [c.name];
  if (c.jurisdiction) bits.push(c.jurisdiction);
  if (c.registration_status) bits.push(`LEI ${c.registration_status.toLowerCase()}`);
  const via = c.matched_name ? `, as its ${nameTypeWords(c.matched_name_type)} “${c.matched_name}”` : "";
  return `${bits.join(" · ")} — ${tierWords(c.match)}${via}`;
}

/** "BARRICK MINING CORPORATION (0O4K…) · relationship retired" — the parent
 *  as GLEIF last filed it, with the relationship's own standing said. */
export function parentLine(p: LeadParent): string {
  const head = p.name ? `${p.name} (${p.lei})` : p.lei;
  const status: string[] = [];
  if (p.entity_status) status.push(`entity ${p.entity_status.toLowerCase()}`);
  if (!p.standing) status.push("relationship no longer served by GLEIF");
  return status.length ? `${head} · ${status.join(" · ")}` : head;
}

export function lookupHref(lei: string): string {
  return `/?lei=${lei}`;
}

/** The lines the block shows, in order, once the response is in. Pure, so
 *  the test can pin what a reader sees for each shape of answer. */
export function leadsLines(r: LeadsResponse): {
  parent: string;
  parentHref: string | null;
  candidates: { line: string; href: string }[];
  candidatesNote: string | null;
} {
  const parent = r.parent ? parentLine(r.parent) : NO_PARENT;
  const unavailable = unavailableLine(r.gleif_unavailable_reason);
  return {
    parent,
    parentHref: r.parent ? lookupHref(r.parent.lei) : null,
    candidates: r.candidates.map((c) => ({ line: candidateLine(c), href: lookupHref(c.lei) })),
    candidatesNote: unavailable ?? (r.candidates.length === 0 ? NO_CANDIDATES : null),
  };
}
