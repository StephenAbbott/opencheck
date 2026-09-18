/**
 * knowability — the values behind the "What can be known" band (Phase 224).
 *
 * The sentence is the backend's (`opencheck/knowability.py`, on the
 * `knowability` lookup event) and is rendered verbatim: the page, the PDF and
 * the MCP summary must not disagree, and a sentence rebuilt here would be a
 * second copy that drifts. What this module does is the reader-facing framing
 * around it — the heading, the provenance badge, and the per-field list the
 * ⓘ disclosure opens — as pure functions so the component stays markup-only
 * and the values are pinned by `knowability.test.ts`.
 *
 * Two rules carried over from the backend module, because a values layer is
 * where they would quietly break:
 *
 * 1. **Describe, never judge.** The badge tones are `context` and `neutral`
 *    only. "Unverified draft" is a fact about the row, not a warning about
 *    the company; "restricted" is a fact about the register, not a finding.
 *    Nothing here may pick `risk` or `warn`.
 * 2. **A saved report reads no clock.** Dates shown are the ones on the
 *    statement (`last_verified`, `as_of`), formatted — never `new Date()`.
 */

import type { KnowabilityStatement } from "./api";
import type { ChipTone } from "../components/ui/Chip";

export interface KnowabilityRow {
  label: string;
  value: string;
}

export interface KnowabilityBadge {
  label: string;
  tone: Extract<ChipTone, "context" | "neutral">;
}

export interface KnowabilityView {
  /** "What can be known" — one heading, fixed, so the strip's outline is stable. */
  heading: string;
  /** The jurisdiction's name — "United Kingdom", or the bare code when
   *  pycountry did not know it. Rendered beside the heading, never folded
   *  into a phrase ("in United Kingdom" needs an article the data lacks). */
  subheading: string;
  /** The server-built sentence(s), verbatim. */
  sentence: string;
  badge: KnowabilityBadge;
  /** Per-field rows for the disclosure, empty values already dropped. */
  rows: KnowabilityRow[];
  /** Source URLs Stephen recorded for the row. */
  sources: { url: string; title: string }[];
  /** "Rendered as of 18 Sep 2026" — the day the sentence was judged against,
   *  which on a saved report is the day of the run. Null when unknown. */
  asOfLine: string | null;
}

const ACCESS_LABEL: Record<string, string> = {
  public: "Public",
  public_with_registration_or_justification: "Public after registration or a stated reason",
  legitimate_interest: "Authorities, obliged entities, and others on legitimate interest",
  restricted_no_lia_route_yet:
    "Authorities and obliged entities; legitimate-interest route in law, not yet open",
  authorities_and_obliged_entities_only: "Authorities and obliged entities only",
  no_register: "No central register",
  in_progress: "Register being set up",
};

const REPORTING_BASIS_LABEL: Record<string, string> = {
  first_qualifying_link: "first qualifying link (not the ultimate person)",
  ultimate_natural_person: "ultimate natural person",
  unknown: "not recorded",
};

const VERIFICATION_LABEL: Record<string, string> = {
  self_declared: "self-declared filings",
  identity_verified: "identities verified",
  cross_checked_against_registers: "cross-checked against other registers",
  unknown: "not recorded",
};

/** ISO date → "18 Sep 2026", UTC so the day never shifts with the viewer's zone. */
export function formatKnowabilityDate(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const d = new Date(`${iso.slice(0, 10)}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
}

function humanise(code: string): string {
  return code.replace(/_/g, " ");
}

function list(v: unknown): string | null {
  if (!Array.isArray(v) || v.length === 0) return null;
  return v.map((x) => humanise(String(x))).join(", ");
}

function text(v: unknown): string | null {
  if (typeof v !== "string") return null;
  const t = v.trim();
  return t ? t : null;
}

export function knowabilityBadge(st: KnowabilityStatement): KnowabilityBadge {
  if (st.stated_absence) return { label: "No register notes held", tone: "neutral" };
  const day = formatKnowabilityDate(st.last_verified);
  if (st.review_status === "verified" && day) return { label: `Checked ${day}`, tone: "context" };
  return { label: "Unverified draft", tone: "neutral" };
}

/** The per-field list behind the sentence. Empty fields are dropped rather
 *  than shown as "—": the sentence already said what is known, and a list of
 *  dashes reads as "nothing recorded" for the whole jurisdiction. */
export function knowabilityRows(st: KnowabilityStatement): KnowabilityRow[] {
  if (st.stated_absence) return [];
  const f = st.fields ?? {};
  const rows: (KnowabilityRow | null)[] = [];
  const push = (label: string, value: string | null | undefined) => {
    rows.push(value ? { label, value } : null);
  };

  push("Beneficial ownership register", text(f.register));
  push("Who can see it", st.access ? ACCESS_LABEL[st.access] ?? humanise(st.access) : null);
  push("Current arrangement since", formatKnowabilityDate(st.access_since));
  push("Next change announced", formatKnowabilityDate(st.next_change_expected));
  push("Threshold wording", text(f.threshold_wording));
  push("Fields published", list(f.fields_published));
  const basis = text(f.reporting_basis);
  push("Reporting basis", basis ? REPORTING_BASIS_LABEL[basis] ?? humanise(basis) : null);
  push("Covers", list(f.covers));
  const verification = text(f.verification);
  const verificationNote = text(f.verification_note);
  push(
    "Verification of filings",
    verification
      ? `${VERIFICATION_LABEL[verification] ?? humanise(verification)}${
          verificationNote ? ` — ${verificationNote}` : ""
        }`
      : verificationNote,
  );
  push("6AMLD legitimate-interest access", text(f.amld6_lia));
  push("6AMLD details", text(f.amld6_details));
  push("BORIS interconnection", text(f.boris));
  if (f.amlr_alignment_watch === true) {
    push("AMLR alignment watch", 'threshold wording must move to "25 % or more" by 10 July 2027');
  }
  push("Pending changes", text(f.pending_changes));

  const company = (f.company_register ?? null) as Record<string, unknown> | null;
  if (company) {
    const name = text(company.name);
    const publishes = list(company.publishes);
    const isPublic = text(company.public);
    const parts = [
      name,
      isPublic ? `public: ${isPublic.toLowerCase()}` : null,
      publishes ? `publishes ${publishes}` : null,
    ].filter(Boolean);
    push("Company register", parts.length ? parts.join("; ") : null);
  }

  const reads = st.opencheck_reads ?? [];
  push(
    "OpenCheck reads",
    reads.length
      ? reads
          .map(
            (r) =>
              `${r.name}${r.reads_beneficial_owners ? " (beneficial owners)" : ""}${
                r.requires_api_key ? " (API key)" : ""
              }`,
          )
          .join("; ")
      : `no ${st.name} register`,
  );

  const fatf = (f.fatf ?? null) as Record<string, unknown> | null;
  if (fatf) {
    const body = text(fatf.body);
    const onsite = formatKnowabilityDate(text(fatf.onsite));
    const plenary = formatKnowabilityDate(text(fatf.plenary));
    const parts = [body, onsite ? `onsite ${onsite}` : null, plenary ? `plenary ${plenary}` : null].filter(
      Boolean,
    );
    push("Next FATF / FSRB assessment", parts.length ? parts.join("; ") : null);
  }

  push("Notes", text(f.notes));
  return rows.filter((r): r is KnowabilityRow => r !== null);
}

export function knowabilityView(st: KnowabilityStatement): KnowabilityView {
  const asOf = formatKnowabilityDate(st.as_of);
  return {
    heading: "What can be known",
    subheading: st.name,
    sentence: st.sentence,
    badge: knowabilityBadge(st),
    rows: knowabilityRows(st),
    sources: (st.sources ?? []).map((s) => ({
      url: s.url,
      title: s.title?.trim() || s.url.replace(/^https?:\/\//, "").replace(/\/$/, ""),
    })),
    asOfLine: asOf ? `Rendered as of ${asOf}` : null,
  };
}
