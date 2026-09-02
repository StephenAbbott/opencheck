import type { CrossSourceLink } from "./api";
import { independentCount } from "./lineage";

/**
 * Distinct sources that independently publish the subject's LEI.
 *
 * This is the count behind the SubjectCard "LEI confirmed by N sources"
 * badge. The badge renders next to the LEI, so its number must be scoped to
 * the LEI: counting every source participating in *any* cross-source link
 * (Wikidata QID, Companies House number, OpenSanctions id, name matches…)
 * overstates what the badge visually claims. Only links whose bridge key is
 * the LEI — and whose value is the subject's own LEI, not a related entity's
 * — contribute.
 *
 * The badge and the "Is this the right company?" band therefore carry
 * different numbers on purpose — on BP, 6 and 8 — and until Phase 133 the
 * badge said "Identifier confirmed by", which gave a reader no way to tell
 * that the two were answers to different questions. It names the LEI now.
 *
 * The reconciler only emits an "lei" link when ≥2 sources share the value,
 * and per CLAUDE.md a source's hit only carries `lei` when that source
 * publishes or validates it. "Independently" is then a lineage question:
 * OpenSanctions' company record IS GLEIF's, so GLEIF + OpenSanctions is one
 * confirmation, not two. The count is therefore of independent origins
 * (`lineage.ts`), never of participating hits.
 */
export function countLeiConfirmingSources(
  links: CrossSourceLink[],
  lei: string,
): number {
  const target = lei.trim().toUpperCase();
  if (!target) return 0;
  const sources = new Set<string>();
  for (const link of links) {
    if (link.key !== "lei") continue;
    if (link.key_value.trim().toUpperCase() !== target) continue;
    for (const hit of link.hits) sources.add(hit.source_id);
  }
  return independentCount([...sources]);
}
