/**
 * One word per concept (Phase 124).
 *
 * The Phase 122 audit's finding was that a glossary pass would do more for
 * comprehension than any visual change, and the Phase 124 sweep found the
 * collisions were concrete rather than stylistic:
 *
 * - The same array was called **results** on one line of `SourceBucketCard` and
 *   **hits** 54 lines later; elsewhere the same thing is a *match*.
 * - **statements** meant BODS records in eleven places and narrative claims in
 *   three, and both render on the QuickCheck page at once.
 * - Four verbs for one action: *Search*, *Look up*, *Screen*, *Run background
 *   check* — one button even said "Look up" idle and "Searching…" pending.
 * - **`signpost · not in graph`** and **`context · not in graph`** are the same
 *   statement about the same kind of data, in two nouns, one of which leaks an
 *   internal field name and the other of which collides with `Chip`'s `context`
 *   tone.
 * - Raw `source_id` slugs reached users as prose — "companies_house and
 *   opensanctions describe the…" — although a display-name map already existed
 *   and was used correctly two files away.
 *
 * These live in `lib/` because the frontend suite is logic-only (no jsdom): a
 * term that is only a string literal inside JSX cannot be pinned, and unpinned
 * is how the four verbs happened. Adding a synonym to `BANNED_SYNONYMS` fails
 * the build until the caller is switched to the canonical term.
 *
 * Acronyms are the other half. `expandOnFirstUse` is deliberately *not* a
 * tooltip: the expansion goes in the sentence the first time a surface says the
 * word, because a reader who does not know what BODS is will not hover a word
 * they cannot read.
 */

// ---------------------------------------------------------------------------
// Canonical terms
// ---------------------------------------------------------------------------

/** What a source returned about the subject. Never "hit" — that is the field
 *  name (`bucket.hits`), not the word for a reader. */
export function resultCount(n: number): string {
  return `${n} result${n === 1 ? "" : "s"}`;
}

/** A BODS record. Always qualified, because the narrative panel calls its own
 *  claims "statements" too and the two render on one page. */
export function bodsRecordCount(n: number): string {
  return `${n} BODS record${n === 1 ? "" : "s"}`;
}

/** The one verb for starting a check on a company. */
export const LOOKUP_VERB = "Search";

/** The one verb for starting a check on a person. Distinct from `LOOKUP_VERB`
 *  on purpose — it is a different action against a different index — but it is
 *  now the *only* word for it, replacing "Screen person" / "Run background
 *  check" / "Screening…". */
export const PERSON_VERB = "Run BackgroundCheck";

/** Data a source publishes that OpenCheck does not map into the BODS graph.
 *  Replaces both "signpost · not in graph" and "context · not in graph". */
export const NOT_IN_GRAPH = "published elsewhere · not in the graph";

/** Terms that must not reappear. Each maps to what to say instead. Pinned by
 *  `vocab.test.ts` so the fix cannot silently regress. */
export const BANNED_SYNONYMS: Record<string, string> = {
  hit: "result",
  hits: "results",
  signpost: NOT_IN_GRAPH,
  stub: "placeholder data",
  "look up": LOOKUP_VERB,
  screen: PERSON_VERB,
  "frontier companies": "companies at the edge of the network so far",
  "node cap": "size limit",
  "person-capable source": "source that holds people",
};

// ---------------------------------------------------------------------------
// Acronyms
// ---------------------------------------------------------------------------

export const ACRONYMS: Record<string, string> = {
  BODS: "Beneficial Ownership Data Standard",
  LEI: "Legal Entity Identifier",
  PEP: "politically exposed person",
  PSC: "person with significant control",
  UBO: "ultimate beneficial owner",
  FATF: "Financial Action Task Force",
  AMLA: "the EU Anti-Money Laundering Authority",
  GLEIF: "Global Legal Entity Identifier Foundation",
  OGL: "Open Government Licence",
  RA: "registration authority",
};

/**
 * `BODS` → `BODS (Beneficial Ownership Data Standard)` the first time a surface
 * uses it, and bare afterwards.
 *
 * `seen` is the caller's per-surface set, so "first use" means first on this
 * page rather than first in the session — a reader who lands directly on a
 * report has not read the homepage.
 */
export function expandOnFirstUse(term: string, seen: Set<string>): string {
  const expansion = ACRONYMS[term];
  if (!expansion || seen.has(term)) return term;
  seen.add(term);
  return `${term} (${expansion})`;
}

// ---------------------------------------------------------------------------
// Source names
// ---------------------------------------------------------------------------

/**
 * A source's display name, from the map the lookup response carries.
 *
 * Four call sites bypassed the map and rendered the raw slug — `via
 * companies_house`, `Source: opensanctions/Q123`, and worst,
 * `companies_house and opensanctions describe the…` as English prose. The
 * fallback prettifies rather than showing snake_case: an unmapped source is a
 * missing registry entry, not a reason to show a reader a variable name.
 */
/** Segments of a source id that are abbreviations, so they uppercase rather
 *  than title-case. An explicit set, not a length rule: "≤3 characters means an
 *  abbreviation" turns `brand_new_source` into "Brand NEW Source". */
const ID_ABBREVIATIONS = new Set([
  "eu", "uk", "us", "ted", "cvr", "kvk", "cro", "ur", "jar", "rpo", "rpvs",
  "bce", "krs", "cac", "acra", "inpi", "sec", "bods", "gleif", "lei", "psc",
  "esg", "gem", "icij", "amla", "fatf", "oc", "ftm", "rdf", "api",
]);

export function sourceLabel(sourceId: string, names?: Record<string, string>): string {
  const mapped = names?.[sourceId];
  if (mapped) return mapped;
  return sourceId
    .split(/[_-]/)
    .filter(Boolean)
    .map((w) =>
      ID_ABBREVIATIONS.has(w.toLowerCase()) ? w.toUpperCase() : w[0].toUpperCase() + w.slice(1)
    )
    .join(" ");
}

/** "Companies House, OpenSanctions and Wikidata" — an English list, never a
 *  `.join(" and ")` over raw slugs. */
export function sourceList(ids: string[], names?: Record<string, string>): string {
  const labels = [...new Set(ids)].map((id) => sourceLabel(id, names));
  if (labels.length === 0) return "";
  if (labels.length === 1) return labels[0];
  return `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}`;
}

// ---------------------------------------------------------------------------
// OpenAleph topics
// ---------------------------------------------------------------------------

/**
 * OpenAleph topic ids reached the archive-matches list verbatim — a reader saw
 * `corp.disqual`, `poi`, `crime.fin`. These are FollowTheMoney topic slugs; the
 * mapping is upstream's own vocabulary, so it is a translation and not an
 * inference. An unmapped id is prettified rather than hidden: the fact that a
 * collection was tagged is still information.
 */
export const OPENALEPH_TOPIC: Record<string, string> = {
  poi: "person of interest",
  "corp.disqual": "disqualified director",
  "crime.fin": "financial crime",
  "crime.theft": "theft",
  "crime.war": "war crimes",
  "crime.boss": "organised crime",
  "crime.terror": "terrorism",
  "crime.traffick": "trafficking",
  "role.pep": "politically exposed person",
  "role.rca": "relative or close associate",
  "role.judge": "judiciary",
  "role.diplo": "diplomatic service",
  "role.oligarch": "oligarch",
  debarment: "debarred from public contracts",
  sanction: "sanctions listing",
  "sanction.linked": "linked to a sanctions listing",
  "sanction.counter": "counter-sanctions listing",
  export_control: "export control listing",
  wanted: "wanted by law enforcement",
  asset__frozen: "frozen asset",
};

export function topicLabel(topic: string): string {
  return OPENALEPH_TOPIC[topic] ?? topic.replace(/[._]/g, " ");
}

/** The topics on one match, as readable text. */
export function topicList(topics: string[]): string {
  return [...new Set(topics.map(topicLabel))].join(", ");
}
