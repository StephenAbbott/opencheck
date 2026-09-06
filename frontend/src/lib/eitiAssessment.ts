/**
 * The EITI Company Assessment, as values (Phase 11, plan B3).
 *
 * Everything on the `eiti_assessment` card that can be wrong about *what the
 * data says* lives here rather than in the markup, because the two claims this
 * source makes are both easy to overstate by accident:
 *
 * 1. **A disclosure assessment is not a finding about the company.** EITI's
 *    expectation 6 asks whether a supporting company discloses its beneficial
 *    owners. A result of "Not available" means EITI did not assess it — and
 *    rendering that in a risk tone would convert a gap in EITI's coverage into
 *    an allegation. `boDisclosureTone` is the one place that mapping exists,
 *    and `eitiAssessment.test.ts` pins the absent cases to non-risk tones.
 * 2. **A name in EITI's subsidiary list is not a matched company.** EITI
 *    publishes 1,230 parent→child rows as free text with no identifier. The
 *    cross-reference below compares *names* and says so; it never claims the
 *    two records are the same company.
 *
 * ## Built for more than one comparison list
 *
 * `crossReference` takes N lists, not one. Today the only list is the GLEIF
 * Level 2 children OpenCheck already fetches; B4 adds OECD-UNSD MEIP once its
 * BODS release lands, and that must be an added column rather than a rewrite of
 * this module and the card above it.
 */

import { compareKey } from "./vocab";

export const BO_EXPECTATION = "exp_6";
export const SUBSIDIARY_EXPECTATION = "exp_2";

// ---------------------------------------------------------------------------
// The bundle, as the adapter builds it
// ---------------------------------------------------------------------------

export interface EitiExpectation {
  label?: string | null;
  result?: string | null;
  response?: string | null;
  url?: string | null;
  comment?: string | null;
  bo_disclosure?: string | null;
  bo_url?: string | null;
  bo_disclosure_url?: string | null;
  stock_exchange?: string | null;
  stock_url?: string | null;
}

export interface EitiDeclaredSubsidiary {
  name: string;
  country?: string | null;
  years?: string[];
  /** EITI's own note on where the row came from ("Source: Company feedback"). */
  source?: string | null;
}

export interface EitiAssessmentMatch {
  method?: string | null;
  reviewed?: boolean;
  /** The GLEIF legal name of the LEI this record is keyed on. */
  gleif_legal_name?: string | null;
}

export interface EitiAssessmentBundle {
  lei: string;
  name: string;
  hq_country?: string | null;
  hq_city?: string | null;
  sectors?: string[];
  company_type?: string | null;
  business_activity?: string | null;
  match?: EitiAssessmentMatch;
  assessments?: Record<string, Record<string, EitiExpectation>>;
  subsidiaries?: EitiDeclaredSubsidiary[];
  source_snapshot?: string | null;
}

// ---------------------------------------------------------------------------
// Reading the bundle
// ---------------------------------------------------------------------------

/** The most recent assessment year present, or null.
 *
 *  Ordered by (length, value) rather than numerically, matching
 *  `sources/eiti_assessment.py::latest_year` — the two must not disagree about
 *  which year the finding sentence and the card are describing. */
export function latestAssessmentYear(bundle: EitiAssessmentBundle | null): string | null {
  const years = Object.keys(bundle?.assessments ?? {}).filter((y) => y.trim());
  if (!years.length) return null;
  return years.sort((a, b) => (a.length - b.length) || a.localeCompare(b)).at(-1) ?? null;
}

/** All assessment years, newest first. */
export function assessmentYears(bundle: EitiAssessmentBundle | null): string[] {
  return Object.keys(bundle?.assessments ?? {})
    .filter((y) => y.trim())
    .sort((a, b) => (b.length - a.length) || b.localeCompare(a));
}

export function expectation(
  bundle: EitiAssessmentBundle | null,
  shorthand: string,
  year?: string | null,
): EitiExpectation | null {
  const y = year ?? latestAssessmentYear(bundle);
  if (!y) return null;
  return bundle?.assessments?.[y]?.[shorthand] ?? null;
}

// ---------------------------------------------------------------------------
// Block 1 — the beneficial ownership disclosure assessment
// ---------------------------------------------------------------------------

/** The chip tones this card may use. Deliberately narrower than `ChipTone`:
 *  `accent` is an OpenCheck affordance and `context` is a structural
 *  observation, and neither is what an assessment result is. */
export type BoTone = "ok" | "warn" | "risk" | "neutral";

export interface BoDisclosure {
  /** The assessment year this describes. */
  year: string;
  /** EITI's verbatim result, e.g. "Expectation partially met". */
  result: string;
  /** What the company told EITI ("Yes" / "No"), when EITI recorded it. */
  response: string | null;
  tone: BoTone;
  /** The chip label — what the row asserts, in a reader's words. */
  label: string;
  /** EITI's own commentary on the result, verbatim. */
  comment: string | null;
}

/**
 * EITI's result → the tone and label the chip carries.
 *
 * **"Not available" is never a risk tone.** It is a statement about EITI's
 * assessment, not about the company, and the counter-sanctions lesson is that
 * a missing check must not be rendered as an adverse one. Anything unrecognised
 * lands in the same place for the same reason: an unknown result is not a
 * finding.
 */
const BO_DISPLAY: Record<string, { tone: BoTone; label: string }> = {
  "expectation met": { tone: "ok", label: "Discloses beneficial ownership" },
  "expectation partially met": { tone: "warn", label: "Partially discloses" },
  "expectation not met": { tone: "risk", label: "Does not disclose" },
  "not available": { tone: "neutral", label: "Not assessed" },
  "not applicable": { tone: "neutral", label: "Not assessed" },
};

export function boDisclosureTone(result: string | null | undefined): BoTone {
  return BO_DISPLAY[(result ?? "").trim().toLowerCase()]?.tone ?? "neutral";
}

export function boDisclosureLabel(result: string | null | undefined): string {
  const known = BO_DISPLAY[(result ?? "").trim().toLowerCase()];
  if (known) return known.label;
  const raw = (result ?? "").trim();
  // An unrecognised result still reports that EITI reached one, rather than
  // guessing at what it meant or silently dropping it.
  return raw ? `EITI recorded: ${raw}` : "Not assessed";
}

export function boDisclosure(bundle: EitiAssessmentBundle | null): BoDisclosure | null {
  const year = latestAssessmentYear(bundle);
  if (!year) return null;
  const exp = expectation(bundle, BO_EXPECTATION, year);
  const result = (exp?.result ?? "").trim();
  return {
    year,
    result,
    response: (exp?.response ?? "").trim() || null,
    tone: boDisclosureTone(result),
    label: boDisclosureLabel(result),
    comment: (exp?.comment ?? "").trim() || null,
  };
}

export interface DisclosureLink {
  url: string;
  /** The year whose assessment carried the link — often *not* the year on the
   *  chip. EITI populated these for 2023 and published none for 2025, so a card
   *  that showed a 2023 link under a 2025 heading would misdate the evidence. */
  year: string;
}

/** The best available link to the company's own beneficial ownership
 *  disclosure, falling back through earlier assessment years.
 *
 *  Field order matches `sources/eiti_assessment.py::bo_disclosure_url`. */
export function disclosureLink(bundle: EitiAssessmentBundle | null): DisclosureLink | null {
  for (const year of assessmentYears(bundle)) {
    const exp = bundle?.assessments?.[year]?.[BO_EXPECTATION] ?? {};
    for (const field of ["bo_url", "bo_disclosure_url", "url"] as const) {
      const value = (exp[field] ?? "").trim();
      if (value) return { url: value, year };
    }
  }
  return null;
}

/**
 * The advocacy sentence, assembled from what the row actually says.
 *
 * Every clause is conditional on evidence being present, because the point of
 * the sentence is that a *document* is not structured data — and claiming a
 * document exists where EITI published no link would be the same overstatement
 * in the other direction. Where EITI recorded a disclosure and published no
 * link, the card says exactly that rather than showing nothing.
 */
export function disclosureSentence(
  bundle: EitiAssessmentBundle | null,
): string | null {
  const bo = boDisclosure(bundle);
  if (!bo) return null;
  const name = (bundle?.name ?? "").trim() || "This company";
  const link = disclosureLink(bundle);
  const parts: string[] = [];

  const response = (bo.response ?? "").toLowerCase();
  if (response === "yes") {
    parts.push(`${name} told EITI it discloses its beneficial owners.`);
  } else if (response === "no") {
    parts.push(`${name} told EITI it does not disclose its beneficial owners.`);
  }

  const verdict: Record<string, string> = {
    "expectation met": `EITI assessed this as met for ${bo.year}.`,
    "expectation partially met": `EITI assessed this as partially met for ${bo.year}.`,
    "expectation not met": `EITI assessed this as not met for ${bo.year}.`,
  };
  const key = bo.result.toLowerCase();
  if (verdict[key]) {
    parts.push(verdict[key]);
  } else {
    parts.push(
      `EITI did not assess beneficial ownership disclosure for ${bo.year}.`,
    );
  }

  if (link) {
    parts.push(
      link.year === bo.year
        ? "The disclosure is a document, not structured data — it cannot be queried, matched, or kept current."
        : `The disclosure EITI links to was published with the ${link.year} assessment. It is a document, not structured data — it cannot be queried, matched, or kept current.`,
    );
  } else if (verdict[key]) {
    parts.push(
      `EITI records a disclosure but did not publish the link for ${bo.year}.`,
    );
  }

  return parts.join(" ");
}

// ---------------------------------------------------------------------------
// Block 3 — cross-referencing the declared names against lists we already hold
// ---------------------------------------------------------------------------

/**
 * One list to compare EITI's declared subsidiaries against.
 *
 * `available: false` is not "the list is empty" — it is "we could not ask", and
 * the two must never render the same way. A failed `/subsidiaries` fetch that
 * showed every row as "EITI only" would report a fetch failure as a finding
 * about the company, which is the Phase 124 honest-failure rule.
 */
export interface ComparisonList {
  id: string;
  /** What a reader should call this list. */
  label: string;
  /** What it measures — why a difference is not a discrepancy. */
  measures: string;
  names: string[];
  /** Identifiers this list carries, keyed by `compareKey(name)`. GLEIF children
   *  carry an LEI; B4's MEIP rows will too. Absent lists simply omit it. */
  leiByKey?: Record<string, string>;
  available: boolean;
}

export interface CrossRefRow {
  name: string;
  country: string | null;
  years: string[];
  /** ids of the comparison lists that carry this name. */
  alsoIn: string[];
  /** An LEI a comparison list holds for this name. Name-derived, so it may be
   *  linked and chipped — never asserted as an identifier, never mapped. */
  lei: string | null;
}

export interface CrossRefListSummary {
  id: string;
  label: string;
  measures: string;
  available: boolean;
  /** How many entries the comparison list itself holds. */
  listed: number;
  /** Declared names that also appear in it. */
  overlap: number;
  /** Declared names it does not carry. */
  eitiOnly: number;
  /** Its own entries that EITI does not declare — the direction that gets
   *  hidden if you only ever count from the EITI side. */
  onlyInList: number;
}

export interface CrossReference {
  declared: number;
  rows: CrossRefRow[];
  lists: CrossRefListSummary[];
}

export function crossReference(
  declared: EitiDeclaredSubsidiary[],
  lists: ComparisonList[],
): CrossReference {
  const declaredKeys = new Map<string, EitiDeclaredSubsidiary>();
  for (const sub of declared) {
    const key = compareKey(sub.name);
    if (key && !declaredKeys.has(key)) declaredKeys.set(key, sub);
  }

  const keysByList = new Map<string, Set<string>>();
  for (const list of lists) {
    keysByList.set(
      list.id,
      new Set(list.available ? list.names.map(compareKey).filter(Boolean) : []),
    );
  }

  const rows: CrossRefRow[] = declared.map((sub) => {
    const key = compareKey(sub.name);
    const alsoIn = lists
      .filter((l) => l.available && keysByList.get(l.id)?.has(key))
      .map((l) => l.id);
    const lei =
      lists
        .map((l) => (l.available ? l.leiByKey?.[key] : undefined))
        .find((v) => v) ?? null;
    return {
      name: sub.name,
      country: (sub.country ?? "").trim() || null,
      years: sub.years ?? [],
      alsoIn,
      lei,
    };
  });

  const summaries: CrossRefListSummary[] = lists.map((list) => {
    const keys = keysByList.get(list.id) ?? new Set<string>();
    const overlap = [...declaredKeys.keys()].filter((k) => keys.has(k)).length;
    return {
      id: list.id,
      label: list.label,
      measures: list.measures,
      available: list.available,
      listed: list.available ? keys.size : 0,
      overlap: list.available ? overlap : 0,
      eitiOnly: list.available ? declaredKeys.size - overlap : 0,
      onlyInList: list.available ? keys.size - overlap : 0,
    };
  });

  return { declared: declared.length, rows, lists: summaries };
}

// ---------------------------------------------------------------------------
// How the LEI on this record was arrived at
// ---------------------------------------------------------------------------

/** The builder's resolution method → a *match strength*, never a corroboration
 *  grade. Unknown methods read as the weakest, because over-stating a match is
 *  the failure that matters — `Teck` passed as a plain exact name match and was
 *  an unrelated German company. */
export function methodMatchLevel(method: string | null | undefined): "high" | "medium" | "low" {
  const m = (method ?? "").trim().toLowerCase();
  if (m === "published_lei") return "high";
  if (m === "gleif_name_exact") return "medium";
  return "low";
}

export function methodBasis(method: string | null | undefined): string {
  const m = (method ?? "").trim().toLowerCase();
  if (m === "published_lei") return "EITI publishes this LEI itself; checked by hand";
  if (m === "gleif_name_exact") return "exact name match in GLEIF, checked by hand";
  return "chosen by hand from a GLEIF shortlist";
}

// ---------------------------------------------------------------------------
// The summary tile
// ---------------------------------------------------------------------------

/** The tile above the card: the declared-subsidiary count when there is one,
 *  and the same voice when there is not — an em dash and a sentence, rather
 *  than a tile that quietly does not appear. */
export function assessmentTile(bundle: EitiAssessmentBundle | null): {
  stat: string;
  unit: string;
  sub: string;
} {
  const year = latestAssessmentYear(bundle);
  const subs = bundle?.subsidiaries ?? [];
  if (subs.length) {
    return {
      stat: subs.length.toLocaleString(),
      unit: subs.length === 1 ? "declared subsidiary" : "declared subsidiaries",
      sub: year ? `EITI Company Assessment ${year}` : "EITI Company Assessment",
    };
  }
  return {
    stat: "—",
    unit: "",
    sub: year ? `assessed ${year} · no subsidiary list filed` : "no assessment year recorded",
  };
}
