/**
 * The subject's profile, as the page reads it (Phase 154).
 *
 * `opencheck/subject_profile.py` assembles the four facts — legal form,
 * register status, founding date, registered address — from the subject's own
 * entity statements; this module decides how they are *said*. It is in `lib/`
 * because the frontend suite is logic-only: the chip's wording, its tone and
 * the row text are the claims, and a claim that lives only inside JSX cannot
 * be pinned.
 *
 * Two placements, one rule each:
 *
 * - **The status chip** on the subject card carries register status alone. Of
 *   the four facts it is the one that changes a reading — a dissolved company
 *   with an ACTIVE LEI is the Phase 151 case — and it has to be met before the
 *   verdict, not found on a structured-records card. Its tone is `neutral` for
 *   a live register status, because a status is a fact with no valence
 *   (`ui/Chip`); `warn` while a terminal process is under way; and `terminal`
 *   — a dark, un-tinted chip — when the register says the company has ended.
 *   Never `risk`: dissolved is a fact about the company, not a finding against
 *   it. An absent status renders no chip: absence is not "active".
 * - **The profile rows** lead the "Is this the right company?" band, which the
 *   LEI badge already opens and which is the identity enquiry: legal form,
 *   founding date and address are answers to *which* company, not *what did
 *   you find*.
 */

import type { SubjectProfile, SubjectProfileFact, SubjectProfileStatus } from "./api";
import { sourceLabel, sourceList } from "./vocab";

export type StatusChipTone = "neutral" | "warn" | "terminal";

export interface StatusChip {
  label: string;
  tone: StatusChipTone;
  /** For assistive technology and the row: the same claim, in full. */
  detail: string;
}

/** "5 Feb 2002" from an ISO date; a bare year or year-month stays as written. */
export function formatProfileDate(iso: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return iso;
  const d = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
}

const STATUS_WORD: Record<SubjectProfileStatus["liveness"], string> = {
  live: "Active",
  pending: "Terminal process under way",
  terminal: "Dissolved",
};

const STATUS_TONE: Record<SubjectProfileStatus["liveness"], StatusChipTone> = {
  live: "neutral",
  pending: "warn",
  terminal: "terminal",
};

export function statusChip(
  profile: SubjectProfile | null | undefined,
  names?: Record<string, string>,
): StatusChip | null {
  const status = profile?.register_status;
  if (!status) return null;
  const source = sourceLabel(status.source_id, names);
  const word = STATUS_WORD[status.liveness];
  const since =
    status.since && status.liveness !== "live" ? ` since ${formatProfileDate(status.since)}` : "";
  return {
    label: `${word} · ${source}`,
    tone: STATUS_TONE[status.liveness],
    detail: `${source} records this company as ${word.toLowerCase()}${since}.`,
  };
}

export interface ProfileRow {
  label: string;
  value: string;
  /** "Source: Companies House and GLEIF" — or, when only one source states
   *  it, "Source: GLEIF". Never a count: two sources that copy each other
   *  would read as two. */
  sources: string;
}

function factRow(
  label: string,
  fact: SubjectProfileFact | null | undefined,
  format: (v: string) => string,
  names?: Record<string, string>,
): ProfileRow | null {
  if (!fact || !fact.value) return null;
  return {
    label,
    value: format(fact.value),
    sources: `Source: ${sourceList(fact.sources, names)}`,
  };
}

/** The rows for the identity band, in reading order. A fact no source
 *  stated is simply absent — the band says what is known, not what is not. */
export function profileRows(
  profile: SubjectProfile | null | undefined,
  names?: Record<string, string>,
): ProfileRow[] {
  if (!profile) return [];
  const rows: (ProfileRow | null)[] = [
    factRow("Legal form", profile.legal_form, (v) => v, names),
    profile.register_status
      ? {
          label: "Register status",
          value:
            STATUS_WORD[profile.register_status.liveness] +
            (profile.register_status.since && profile.register_status.liveness !== "live"
              ? ` since ${formatProfileDate(profile.register_status.since)}`
              : "") +
            (profile.register_status.raw &&
            profile.register_status.raw.toLowerCase() !==
              STATUS_WORD[profile.register_status.liveness].toLowerCase()
              ? ` — register status: “${profile.register_status.raw}”`
              : ""),
          sources: `Source: ${sourceList(profile.register_status.sources, names)}`,
        }
      : null,
    factRow("Incorporated", profile.founding_date, formatProfileDate, names),
    factRow("Registered address", profile.registered_address, (v) => v, names),
  ];
  return rows.filter((r): r is ProfileRow => r !== null);
}
