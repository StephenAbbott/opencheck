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

/**
 * What the diagram chip on a source row says, from the graph's own split.
 *
 * It said "**7 parties**", counted from `breakdown.entities` alone — so every
 * natural person the source disclosed was standing in the diagram and missing
 * from the number that described it. A reader who opened a UK Companies House
 * row saw seven in the label and eleven nodes on screen.
 *
 * Two decisions, both narrower than they look:
 *
 * - **"entities", not "parties".** A party is whatever the graph holds, and
 *   using it for a count that excludes half of them is the overclaim. BODS
 *   itself splits `entity` from `person`, and so does the label.
 * - **People are named when there are any.** Suppressing the clause at zero
 *   keeps the common case short, and "· 0 people" reads as a finding about
 *   disclosure rather than as an empty count.
 */
export function graphPartiesLabel(entities: number, persons = 0): string {
  const e = `${entities} ${entities === 1 ? "entity" : "entities"}`;
  if (persons <= 0) return e;
  return `${e} · ${persons} ${persons === 1 ? "person" : "people"}`;
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

/**
 * **Exact labels** that must not reappear, mapped to what to render instead.
 * Enforced by `scripts/lint-design-system.mjs` on the same ratchet as the token
 * and type-scale rules.
 *
 * These are phrases, not words, and that distinction was learned the hard way.
 * The first version banned single words — `screen`, `hit`, `stub` — and every
 * one of them turned out to have legitimate uses the moment a lint was pointed
 * at them: "an empty result here is not a clean screen", "a fast screen of the
 * subject", the SSE event literally named `"hit"`, and `Liveness = "stub"`.
 * Banning the word would have forced rewrites of correct English to satisfy a
 * rule aimed at something else.
 *
 * What actually regressed was a **label a reader sees**: a button that said
 * "Screen person" while another said "Run background check" for the same
 * action, and an empty state that said "No hits." forty lines from one that
 * said "3 results". So the rule bans those strings and leaves the language
 * alone.
 */
export const BANNED_SYNONYMS: Record<string, string> = {
  "No hits.": "No results.",
  "Screen person": PERSON_VERB,
  "Run background check": PERSON_VERB,
  "Look up": LOOKUP_VERB,
  "Looking up…": "Searching…",
  "signpost · not in graph": NOT_IN_GRAPH,
  "context · not in graph": NOT_IN_GRAPH,
  "frontier companies": "companies at the edge of the network so far",
  "node cap reached": "the size limit was reached",
  "person-capable source": "source that holds people",
  "GLEIF mapped": "Mapped by GLEIF",
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

/**
 * The registry's own names, published once so a component that has a source id
 * but no map still says the source's name.
 *
 * The map arrives on `/sources` and was threaded down as a `sourceNames` prop.
 * Threading is fine where it reaches; the problem is where it does not. A risk
 * chip's expanded evidence read "Source: Opensanctions", the ESG cards and the
 * source legend the same way — the registry calls it **OpenSanctions**, and
 * `everypolitician` prettifies to "Everypolitician", `climatetrace` to
 * "Climatetrace" against a registry name of "Global Energy Monitor / Climate
 * TRACE". The prettifier is a fallback for a source the registry does not
 * know, and using it for one the registry *does* know invents a brand name —
 * the defect `sourceList` had with "Openfigi".
 *
 * Module-scoped for the same reason the "as filed" toggle is: it is one fact
 * about the deployment, not per-component state, and every card must agree.
 * An explicit prop still wins, so nothing that already threads the map
 * changes behaviour.
 */
let REGISTRY_NAMES: Record<string, string> = {};

/** Publish the registry's names. Called once when `/sources` resolves. */
export function setSourceNames(names: Record<string, string>): void {
  REGISTRY_NAMES = { ...names };
}

/** The names currently published — for tests and for callers that need the
 *  whole map rather than one label. */
export function getSourceNames(): Record<string, string> {
  return REGISTRY_NAMES;
}

export function sourceLabel(sourceId: string, names?: Record<string, string>): string {
  const mapped = names?.[sourceId] ?? REGISTRY_NAMES[sourceId];
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
