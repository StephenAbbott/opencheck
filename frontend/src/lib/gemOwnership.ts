/**
 * The GEM ownership chips on the Climate TRACE / GEM ESG card (Phase 199).
 *
 * The adapter hands the card two lists, and they overlap:
 *
 * - `raw.parents` — the "Gem parents" column of the entity's own row. GEM
 *   fills it with the top of the group *within GEM's universe*, so a group's
 *   top entity names itself (5,132 rows; PT Pertamina (Persero) showed
 *   "GEM parent: PT Pertamina (Persero) PT · 100%").
 * - `raw.owners` — the entity's direct owners from GEM's relationships CSV,
 *   with the precise share.
 *
 * The chips follow the BODS mapper's two structural rules: an entity is never
 * its own parent or owner, and a party that is both a parent and a direct
 * owner is shown once, as a direct owner. The card is a list of names, so it
 * does not apply the mapper's refusal to make a person-typed party a node
 * (none in the July 2026 release besides GEM's placeholders, which it shows).
 */

export interface GemPartyChip {
  /** GEM entity ID — stable React key. */
  id: string;
  name: string;
  /** Percentage as GEM published it, or null when GEM gives none. */
  share: number | null;
}

interface RawParty {
  entity_id?: unknown;
  name?: unknown;
  share?: unknown;
}

function toChips(list: unknown, selfId: string): GemPartyChip[] {
  if (!Array.isArray(list)) return [];
  const seen = new Set<string>();
  const chips: GemPartyChip[] = [];
  for (const item of list as RawParty[]) {
    const id = typeof item?.entity_id === "string" ? item.entity_id.trim() : "";
    if (!id || id === selfId || seen.has(id)) continue;
    seen.add(id);
    const name = typeof item.name === "string" && item.name.trim() ? item.name.trim() : id;
    chips.push({ id, name, share: typeof item.share === "number" ? item.share : null });
  }
  return chips;
}

/** Direct owners, then the parents GEM names that are not already among them. */
export function gemOwnershipChips(raw: Record<string, unknown>): {
  owners: GemPartyChip[];
  parents: GemPartyChip[];
} {
  const selfId = typeof raw.entity_id === "string" ? raw.entity_id.trim() : "";
  const owners = toChips(raw.owners, selfId);
  const ownerIds = new Set(owners.map((o) => o.id));
  const parents = toChips(raw.parents, selfId).filter((p) => !ownerIds.has(p.id));
  return { owners, parents };
}

/** "GEM direct owner" / "GEM direct owners" — the row's eyebrow. */
export function gemChipHeading(kind: "owners" | "parents", count: number): string {
  const noun = kind === "owners" ? "GEM direct owner" : "GEM parent";
  return count === 1 ? noun : `${noun}s`;
}
