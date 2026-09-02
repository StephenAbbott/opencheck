/**
 * lineage — which source republishes which (browser copy).
 *
 * Several sources are copies of each other: OpenCorporates mirrors the
 * national registers, OpenAleph's register collections are Companies House
 * and GLEIF, OpenSanctions' company records are GLEIF and UK PSC mirrors,
 * EveryPolitician reads OpenSanctions. Wherever the page counts "sources that
 * agree" as corroboration — the LEI-confirmation badge, the FullCheck
 * network's ≥2-sources glyph, the reconciliation box headline — the count has
 * to be of INDEPENDENT origins, or a company Companies House and
 * OpenCorporates both describe reads as twice as certain as it is.
 *
 * The table is `lineage.json`, GENERATED from the backend's declarations by
 * `backend/scripts/gen_lineage.py` (a backend test fails when it is stale);
 * never edit the JSON by hand. It carries `ancestors` (transitive upstreams
 * per source id) and `descriptions` (source id → the `source.description`
 * label the BODS mapper stamps on statements — the only handle the network
 * has on a statement's origin), so this module accepts either ids or labels.
 *
 * Mirror of `opencheck.sources.lineage.independent_sources` — same rules:
 *   - a source whose upstream is also present is dropped in favour of it;
 *   - two survivors sharing an ancestor (OpenCorporates and OpenAleph, both
 *     mirroring Companies House) count once between them;
 *   - anything not in the table is original — under-claim, never crash.
 */

import table from "./lineage.json";

type LineageTable = {
  derived_from: Record<string, string[]>;
  ancestors: Record<string, string[]>;
  descriptions: Record<string, string>;
};

const LINEAGE = table as LineageTable;

/** `source.description` label → source id, for statements' provenance. */
const ID_BY_DESCRIPTION: Record<string, string> = Object.fromEntries(
  Object.entries(LINEAGE.descriptions).map(([id, label]) => [label, id])
);

/** Resolve an adapter id or a mapper description label to an id. Unknown
 *  strings pass through unchanged and behave as original sources. */
export function toSourceId(idOrLabel: string): string {
  const s = idOrLabel.trim();
  if (!s) return "";
  if (s in LINEAGE.descriptions || s in LINEAGE.ancestors) return s;
  return ID_BY_DESCRIPTION[s] ?? s;
}

export function ancestorsOf(idOrLabel: string): ReadonlySet<string> {
  return new Set(LINEAGE.ancestors[toSourceId(idOrLabel)] ?? []);
}

/** True when neither source republishes the other (and they are not two
 *  mirrors of a shared upstream). A source is never independent of itself. */
export function independent(a: string, b: string): boolean {
  const ia = toSourceId(a);
  const ib = toSourceId(b);
  if (ia === ib) return false;
  const aa = ancestorsOf(ia);
  const ab = ancestorsOf(ib);
  if (aa.has(ib) || ab.has(ia)) return false;
  for (const x of aa) if (ab.has(x)) return false;
  return true;
}

/** Collapse ids/labels to their independent origins (sorted, deduped source
 *  ids). `.length` is the corroboration count. */
export function independentSources(sources: readonly string[]): string[] {
  const ids = [...new Set(sources.map(toSourceId).filter(Boolean))].sort();
  const present = new Set(ids);
  const survivors = ids.filter((s) => ![...ancestorsOf(s)].some((up) => present.has(up)));
  const parent = new Map<string, string>(survivors.map((s) => [s, s]));
  const find = (x: string): string => {
    let cur = x;
    while (parent.get(cur) !== cur) cur = parent.get(cur)!;
    return cur;
  };
  for (let i = 0; i < survivors.length; i++) {
    for (let j = i + 1; j < survivors.length; j++) {
      const a = survivors[i];
      const b = survivors[j];
      const aa = ancestorsOf(a);
      let shared = false;
      for (const x of ancestorsOf(b)) if (aa.has(x)) { shared = true; break; }
      if (!shared) continue;
      const ra = find(a);
      const rb = find(b);
      if (ra !== rb) parent.set(ra < rb ? rb : ra, ra < rb ? ra : rb);
    }
  }
  return [...new Set(survivors.map(find))].sort();
}

export function independentCount(sources: readonly string[]): number {
  return independentSources(sources).length;
}
