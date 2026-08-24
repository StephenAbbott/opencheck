/**
 * Citation chips, one per source rather than one per fact (Phase 132).
 *
 * The summary's evidence list renders a chip for every evidence id a claim
 * cites. On a claim like "GLEIF Level 2 data records over 90 direct
 * accounting-consolidation subsidiaries" that is twenty-eight facts, all from
 * GLEIF — twenty-eight chips reading "GLEIF", which on a phone stack one per
 * line into a column of identical pills several screens tall. Raised from
 * production against Shell.
 *
 * Twenty-eight chips also said nothing twenty-eight times: every one of them
 * scrolls to the same source card and highlights the same diagram. The count
 * is the only new information in the repetition, so the count is what the
 * grouped chip carries.
 *
 * Grouping is by **what the chip says and does** — its kind and its label —
 * not by `sourceId`. A `gap` has no source id at all and they must not
 * collapse into one "Limitation" chip with the others; two sources that
 * happen to share a display name would be a registry bug, not something to
 * paper over here.
 */

export interface CiteLike {
  id: string;
  kind: "fact" | "risk" | "gap";
  label: string;
  sourceId: string | null;
  statementId: string | null;
  confidence: string | null;
}

export interface CiteGroup<T extends CiteLike = CiteLike> {
  /** Stable key for React, and the grouping identity. */
  key: string;
  label: string;
  kind: T["kind"];
  /** How many citations this chip stands for. 1 means an ungrouped chip. */
  count: number;
  /**
   * The **weakest** confidence in the group, or null when none carries one.
   *
   * Not the strongest and not the first: the glyph is a claim about the whole
   * group, and one high-confidence fact among twenty-seven medium ones must
   * not lend the group a ● it has not earned. `ui/Chip`'s legend defines these
   * levels, and this is the reading that agrees with it.
   */
  confidence: string | null;
  /** The first citation, which is what activating the chip acts on — they all
   *  scroll to the same card. Kept whole so callers can reach the rest. */
  cites: T[];
}

const CONFIDENCE_RANK: Record<string, number> = { high: 3, medium: 2, low: 1 };

function weakest(a: string | null, b: string | null): string | null {
  if (a === null) return b;
  if (b === null) return a;
  return (CONFIDENCE_RANK[b] ?? 0) < (CONFIDENCE_RANK[a] ?? 0) ? b : a;
}

/** Group a claim's citations for display, preserving first-seen order. */
export function groupCitations<T extends CiteLike>(cites: T[]): CiteGroup<T>[] {
  const byKey = new Map<string, CiteGroup<T>>();
  for (const cite of cites) {
    const key = `${cite.kind}:${cite.label}`;
    const existing = byKey.get(key);
    if (existing) {
      existing.count += 1;
      existing.cites.push(cite);
      existing.confidence = weakest(existing.confidence, cite.confidence);
      continue;
    }
    byKey.set(key, {
      key,
      label: cite.label,
      kind: cite.kind,
      count: 1,
      confidence: cite.confidence,
      cites: [cite],
    });
  }
  return [...byKey.values()];
}

/**
 * What a grouped chip says out loud: "GLEIF, 28 records".
 *
 * "records", because that is what the facts behind a citation are, and
 * because the count is otherwise a bare number beside a source name — which
 * reads as a percentage, a rank, or a year, depending on the reader.
 */
export function citeGroupDescription(group: CiteGroup): string {
  if (group.count <= 1) return group.label;
  return `${group.label}, ${group.count} records`;
}
