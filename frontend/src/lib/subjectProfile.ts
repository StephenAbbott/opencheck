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

import type {
  LeiRegistration,
  LeiSuccessor,
  SubjectProfile,
  SubjectProfileFact,
  SubjectProfileStatus,
} from "./api";
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
  declared: "Last declared active",
};

const STATUS_TONE: Record<SubjectProfileStatus["liveness"], StatusChipTone> = {
  live: "neutral",
  pending: "warn",
  terminal: "terminal",
  declared: "neutral",
};

/** Phase 291: GLEIF's ACTIVE on an LEI no issuer maintains. The server's
 *  sentence when the payload carries one (`opencheck/lei_registration.py`
 *  `declared_sentence`); otherwise the same claim built here, for a payload
 *  that kept only liveness and date. Never "live", never "dissolved". */
export function declaredSentence(status: Pick<SubjectProfileStatus, "raw" | "since" | "sentence">): string {
  if (status.sentence) return status.sentence;
  const word = status.raw || "ACTIVE";
  const head = status.since
    ? `GLEIF holds the entity status as ${word}, last declared to the LEI issuer before ${formatProfileDate(status.since)}; the LEI has not been renewed since, so no issuer has re-checked it.`
    : `GLEIF holds the entity status as ${word}, as last declared to the LEI issuer; no issuer re-checks it.`;
  return `${head} This is not a current register reading.`;
}

export function statusChip(
  profile: SubjectProfile | null | undefined,
  names?: Record<string, string>,
): StatusChip | null {
  const status = profile?.register_status;
  if (!status) return null;
  const source = sourceLabel(status.source_id, names);
  const word = STATUS_WORD[status.liveness];
  if (status.liveness === "declared") {
    const before = status.since ? ` (before ${formatProfileDate(status.since)})` : "";
    return {
      label: `${word}${before} · ${source}`,
      tone: STATUS_TONE.declared,
      detail: declaredSentence(status),
    };
  }
  const since =
    status.since && status.liveness !== "live" ? ` since ${formatProfileDate(status.since)}` : "";
  return {
    label: `${word} · ${source}`,
    tone: STATUS_TONE[status.liveness],
    detail: `${source} records this company as ${word.toLowerCase()}${since}.`,
  };
}

// ---------------------------------------------------------------------------
// The LEI record's own status (Phase 242)
// ---------------------------------------------------------------------------
//
// GLEIF's `registration.status` says whether the LEI *record* is kept up, not
// whether the company exists — that is register status, above. So it gets its
// own chip beside the LEI, in the `context` tone (a fact about the record, not
// a finding, and visibly a different claim from the neutral register chip),
// and only for a status other than ISSUED (Stephen, 24 Sept 2026). The row in
// the identity band always states it. Mirrors `opencheck/lei_registration.py`.

/** GLEIF LEI-CDF RegistrationStatus → the word a reader sees. */
export const LEI_REGISTRATION_LABEL: Record<string, string> = {
  ISSUED: "Issued",
  LAPSED: "Lapsed",
  RETIRED: "Retired",
  MERGED: "Merged",
  ANNULLED: "Annulled",
  DUPLICATE: "Duplicate",
  CANCELLED: "Cancelled",
  TRANSFERRED: "Transferred",
  PENDING_TRANSFER: "Pending transfer",
  PENDING_ARCHIVAL: "Pending archival",
  PENDING_VALIDATION: "Pending validation",
};

/** The clause every non-ISSUED statement ends on — the reading this exists
 *  to stop is "lapsed LEI = dissolved company". */
export const LEI_NOT_ENTITY_STATUS = "the LEI record's status, not the company's";

export function leiRegistrationLabel(status: string): string {
  const s = status.toUpperCase();
  return (
    LEI_REGISTRATION_LABEL[s] ??
    s.charAt(0) + s.slice(1).toLowerCase().replace(/_/g, " ")
  );
}

export interface LeiRegistrationChip {
  label: string;
  tone: "context";
  /** The full claim — the server's frozen sentence when there is one. */
  detail: string;
}

/** The chip beside the LEI, or null for ISSUED and for no status at all.
 *  "LEI lapsed since 19 Oct 2017" — a lapse takes effect on the renewal date
 *  that was missed, the one date GLEIF publishes for it. */
export function leiRegistrationChip(
  reg: Pick<LeiRegistration, "status"> & Partial<LeiRegistration> | null | undefined,
): LeiRegistrationChip | null {
  if (!reg || !reg.status) return null;
  const status = reg.status.toUpperCase();
  if (status === "ISSUED") return null;
  const word = leiRegistrationLabel(status).toLowerCase();
  const since = status === "LAPSED" && reg.since ? ` since ${formatProfileDate(reg.since)}` : "";
  return {
    label: `LEI ${word}${since}`,
    tone: "context",
    detail:
      reg.sentence ||
      `GLEIF records this LEI as ${word}${since}. This is ${LEI_NOT_ENTITY_STATUS}.`,
  };
}

/** The identity-band row value — mirrors `lei_registration.short_line`, plus
 *  the not-the-company clause where the status is not ISSUED. */
export function leiRegistrationLine(reg: LeiRegistration | null | undefined): string | null {
  if (!reg || !reg.status) return null;
  const status = reg.status.toUpperCase();
  const label = reg.label || leiRegistrationLabel(status);
  const renewal = reg.next_renewal_date;
  let line = label;
  if (status === "LAPSED" && renewal) line = `${label} — renewal was due ${formatProfileDate(renewal)}`;
  else if (status === "ISSUED" && renewal) line = `${label} — renews ${formatProfileDate(renewal)}`;
  return status === "ISSUED" ? line : `${line} · ${LEI_NOT_ENTITY_STATUS}`;
}

export interface ProfileRow {
  label: string;
  value: string;
  /** "Source: Companies House and GLEIF" — or, when only one source states
   *  it, "Source: GLEIF". Never a count: two sources that copy each other
   *  would read as two. */
  sources: string;
  /** Phase 307: a lookup the row points forward to, with its link text —
   *  only the successor row carries one. */
  href?: string;
  hrefLabel?: string;
}

// ---------------------------------------------------------------------------
// The successor GLEIF names (Phase 307)
// ---------------------------------------------------------------------------
//
// A retired LEI's record can name the entity that took its place; the band
// says what GLEIF named, in GLEIF's own sentence, and offers one link: the
// record a reader should open next — the END of a followed chain (2,734
// INACTIVE records name a successor that is itself retired), never an
// intermediate hop. A successor named by name alone gets no link: matching
// it would be OpenCheck's assertion. Mirrors `opencheck/lei_successor.py`.

/** Label for the follow-forward link, shared with GEM's banner wording. */
export const SUCCESSOR_FOLLOW_LABEL = "Open the successor's record";

/** Where the successor row links: the last record of a followed chain, else
 *  the one LEI GLEIF named; null for a name only or several successors. */
export function leiSuccessorHref(succ: LeiSuccessor | null | undefined): string | null {
  if (!succ || !succ.named || succ.named.length === 0) return null;
  const last = succ.chain && succ.chain.length > 0 ? succ.chain[succ.chain.length - 1] : null;
  if (last?.lei) return `/?lei=${last.lei}`;
  if (succ.named.length === 1 && succ.named[0].lei) return `/?lei=${succ.named[0].lei}`;
  return null;
}

/** The identity-band row for a named successor, or null when GLEIF names
 *  none. The value is the server's frozen sentence. */
export function leiSuccessorRow(
  succ: LeiSuccessor | null | undefined,
  names?: Record<string, string>,
): ProfileRow | null {
  if (!succ || !succ.sentence) return null;
  // Phase 308: an ended company GLEIF names no successor for still gets the
  // row — the sentence stops a reader looking for a link GLEIF never filed.
  if ((!succ.named || succ.named.length === 0) && succ.relation !== "none") return null;
  const href = leiSuccessorHref(succ);
  return {
    label: succ.relation === "duplicate" ? "Duplicate LEI" : "Successor",
    value: succ.sentence,
    sources: `Source: ${sourceList([succ.source_id || "gleif"], names)}`,
    ...(href ? { href, hrefLabel: SUCCESSOR_FOLLOW_LABEL } : {}),
  };
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
    profile.register_status?.liveness === "declared"
      ? {
          label: "Register status",
          value: declaredSentence(profile.register_status),
          sources: `Source: ${sourceList(profile.register_status.sources, names)}`,
        }
      : profile.register_status
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
    profile.lei_registration && leiRegistrationLine(profile.lei_registration)
      ? {
          label: "LEI registration",
          value: leiRegistrationLine(profile.lei_registration) as string,
          sources: `Source: ${sourceList([profile.lei_registration.source_id || "gleif"], names)}`,
        }
      : null,
    leiSuccessorRow(profile.lei_successor, names),
    factRow("Incorporated", profile.founding_date, formatProfileDate, names),
    factRow("Registered address", profile.registered_address, (v) => v, names),
  ];
  return rows.filter((r): r is ProfileRow => r !== null);
}
