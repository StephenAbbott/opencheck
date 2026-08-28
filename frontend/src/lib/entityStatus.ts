/**
 * Entity-status banner for the GEM / Climate TRACE ESG card (Phase 142).
 *
 * The August 2026 Global Energy Ownership Tracker release records whether an
 * entity has been dissolved or amalgamated, the successor it merged into, and
 * whether it is a joint venture. The adapter surfaces that as
 * `raw.entity_status` on the climatetrace hit; this module turns it into the
 * banner the EsgPanel renders, so the wording and the follow-forward rules are
 * pinned by the logic-only test suite rather than living inline in JSX.
 *
 * Two rules worth stating:
 * - The banner is context, not a risk signal. A dissolved entity reads in the
 *   same voice as an amalgamated one; nothing here styles or scores risk —
 *   that is the signals layer's job, and climatetrace is an ESG-category
 *   source on purpose.
 * - The follow-forward link exists only when GEM's successor resolves to an
 *   LEI. It points at the app's shareable `/?lei=` form, so following the
 *   ownership trail forward is a plain lookup of the successor.
 */

export interface GemEntityStatus {
  status?: string;
  merged_into?: string;
  merged_into_name?: string;
  merged_into_lei?: string;
  urls?: string[];
  jv?: boolean;
}

export interface EntityStatusBanner {
  /** One sentence attributing the record to Global Energy Monitor. */
  text: string;
  /** Lookup href for the successor, when its LEI is known. */
  followHref: string | null;
  /** Evidence links from GEM's Entity Status Data Source URL column. */
  urls: string[];
}

/** Label for the follow-forward link — GEM's own framing of the feature. */
export const FOLLOW_FORWARD_LABEL = "Follow the ownership trail forward";

/** Chip text for a joint venture — a structural observation (tone context). */
export const JOINT_VENTURE_LABEL = "Joint venture";

const LEI_SHAPE = /^[A-Z0-9]{20}$/;

function successorLei(status: GemEntityStatus): string | null {
  const lei = (status.merged_into_lei ?? "").trim().toUpperCase();
  return LEI_SHAPE.test(lei) ? lei : null;
}

function evidenceUrls(status: GemEntityStatus): string[] {
  return (status.urls ?? []).filter(
    (u) => typeof u === "string" && /^https?:\/\//i.test(u.trim()),
  );
}

/**
 * Banner content for a climatetrace hit's `raw.entity_status`, or null when
 * the entity is not status-flagged (active entities and pre-August-2026 data
 * carry no banner; a bare joint-venture flag is a chip, not a banner).
 */
export function entityStatusBanner(raw: unknown): EntityStatusBanner | null {
  const status = (raw ?? null) as GemEntityStatus | null;
  if (!status) return null;

  if (status.status === "amalgamated" || status.status === "dissolved") {
    const successor =
      (status.merged_into_name ?? "").trim() || (status.merged_into ?? "").trim();
    const text =
      status.status === "amalgamated"
        ? `Global Energy Monitor records this entity as amalgamated into ${
            successor || "another entity"
          }.`
        : successor
          ? `Global Energy Monitor records this entity as dissolved; its record points forward to ${successor}.`
          : "Global Energy Monitor records this entity as dissolved.";
    const lei = successorLei(status);
    return {
      text,
      followHref: lei ? `/?lei=${lei}` : null,
      urls: evidenceUrls(status),
    };
  }
  return null;
}

/** True when GEM flags the entity as a joint venture. */
export function isJointVenture(raw: unknown): boolean {
  return Boolean((raw as GemEntityStatus | null)?.jv);
}
