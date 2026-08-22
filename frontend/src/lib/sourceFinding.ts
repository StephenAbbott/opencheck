import type { SourceHit } from "./api";

/**
 * What a source row leads with (Phase 122).
 *
 * v1 rows led with the source's `summary`, which is an identifier fragment
 * — "GB · registered entity", "SG-UEN 12345 · live". True, but it answers
 * "what is this record called?" rather than "what did this source say?".
 * The backend now builds a `finding` sentence per adapter
 * (`opencheck/findings.py`), and the row leads with that where it exists.
 *
 * The fallback chain is the point of this module: `finding` → `summary` →
 * nothing. Roughly thirty adapters have no template yet, and a row for one
 * of them must look exactly like v1 rather than empty or broken, so the
 * seven that do can land one at a time.
 */
export interface RowFinding {
  /** The sentence, or the fragment when there is no sentence. */
  lead: string;
  /** The fragment, when it is playing second fiddle to a sentence. */
  sub: string | null;
}

export function rowFinding(hit: Pick<SourceHit, "summary" | "finding">): RowFinding | null {
  const finding = (hit.finding ?? "").trim();
  const summary = (hit.summary ?? "").trim();

  if (finding && summary && finding !== summary) return { lead: finding, sub: summary };
  if (finding) return { lead: finding, sub: null };
  if (summary) return { lead: summary, sub: null };
  return null;
}
