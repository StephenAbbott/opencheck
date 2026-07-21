import type { CrossSourceLink } from "./api";

/**
 * Distinct sources that independently publish the subject's LEI.
 *
 * This is the count behind the SubjectCard "Identifier confirmed by N
 * sources" badge. The badge renders next to the LEI, so its number must be
 * scoped to the LEI: counting every source participating in *any*
 * cross-source link (Wikidata QID, Companies House number, OpenSanctions id,
 * name matches…) overstates what the badge visually claims. Only links whose
 * bridge key is the LEI — and whose value is the subject's own LEI, not a
 * related entity's — contribute.
 *
 * The reconciler only emits an "lei" link when ≥2 sources share the value,
 * and per CLAUDE.md a source's hit only carries `lei` when that source
 * independently publishes or validates it — so every source counted here is
 * a genuine, independent confirmation of the LEI.
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
  return sources.size;
}
