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

/** The per-field list behind the sentence — the public subset.
 *
 * The jurisdiction table carries far more than a reader of one company's
 * check needs (threshold wording, reporting basis, BORIS, FATF dates,
 * pending changes, notes…): those are Stephen's working notes, maintained in
 * Notion, and on the results page they buried the two facts the band exists
 * to state. What a reader sees is: the register, who can see it, and — for
 * EU/EEA countries only, where the 6AMLD question means something — whether
 * the legitimate-interest route exists in law. Sources and the as-of line are
 * rendered by the caller from `KnowabilityView`.
 *
 * Empty fields are dropped rather than shown as "—": a list of dashes reads
 * as "nothing recorded" for the whole jurisdiction. */
export function knowabilityRows(st: KnowabilityStatement): KnowabilityRow[] {
  if (st.stated_absence) return [];
  const f = st.fields ?? {};
  const rows: KnowabilityRow[] = [];
  const push = (label: string, value: string | null | undefined) => {
    if (value) rows.push({ label, value });
  };

  push("Beneficial ownership register", text(f.register));
  push("Who can see it", st.access ? ACCESS_LABEL[st.access] ?? humanise(st.access) : null);
  if (f.eu_eea === true) {
    const lia = text(f.amld6_lia);
    // Yes / No only — "Not applicable" says nothing inside the EU/EEA.
    push("6AMLD legitimate-interest access", lia === "Yes" || lia === "No" ? lia : null);
  }
  return rows;
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
