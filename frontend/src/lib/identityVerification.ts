/**
 * Companies House identity verification — reading the BODS annotation (Phase 203).
 *
 * Companies House is verifying the identity of every director and individual
 * PSC (transition to November 2026). The backend
 * (`backend/opencheck/bods/identity_verification.py`) turns the register's
 * `identity_verification_details` into a `commenting` annotation on the person
 * statement, carrying a structured `identityVerification` object. This module
 * reads that annotation — **the tick is drawn from the BODS output, never from
 * the raw register payload**, so what the page shows and what the export
 * says cannot disagree.
 *
 * Two rules, pinned by `identityVerification.test.ts`:
 *
 * - **The tick only ever asserts a positive fact.** No annotation means "this
 *   data carries no Companies House verification" — for a person who came from
 *   OpenCorporates or Wikidata that is simply no data, and until November a
 *   director without a statement is not in breach. Nothing here renders an
 *   "unverified" state (Stephen, 10 Sept 2026).
 * - **The words say what the register said.** For the direct route Companies
 *   House publishes no verification date and names no provider; the sentence
 *   gives neither.
 */

export const IDENTITY_VERIFICATION_KEY = "identityVerification";

export const ROUTE_ACSP = "authorisedCorporateServiceProvider";
export const ROUTE_COMPANIES_HOUSE = "companiesHouse";
export type VerificationRoute = typeof ROUTE_ACSP | typeof ROUTE_COMPANIES_HOUSE;

/** The label the tick stands for — visible in the legend, read by screen readers. */
export const IDENTITY_VERIFIED_LABEL = "Identity verified with Companies House";

export interface IdentityVerification {
  route: VerificationRoute;
  verifierName?: string;
  supervisors: string[];
  identityVerifiedOn?: string;
  firstStatementOn?: string;
}

type Rec = Record<string, unknown>;

function rec(v: unknown): Rec | undefined {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Rec) : undefined;
}

function isoDate(v: unknown): string | undefined {
  return typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : undefined;
}

/**
 * The verification a person statement carries, or null.
 *
 * Reads only the annotation pinned at `/recordDetails` — the person-level one.
 * The role-level annotations sit on relationship statements, pinned at
 * `/recordDetails/interestedParty`, and are not a statement about this record.
 */
export function readIdentityVerification(stmt: unknown): IdentityVerification | null {
  const s = rec(stmt);
  if (!s) return null;
  const annotations = Array.isArray(s.annotations) ? s.annotations : [];
  for (const raw of annotations) {
    const a = rec(raw);
    if (!a || a.statementPointerTarget !== "/recordDetails") continue;
    const iv = rec(a[IDENTITY_VERIFICATION_KEY]);
    if (!iv || iv.status !== "verified") continue;
    const route = iv.route;
    if (route !== ROUTE_ACSP && route !== ROUTE_COMPANIES_HOUSE) continue;
    const verifier = rec(iv.verifiedBy);
    const supervisors = Array.isArray(verifier?.antiMoneyLaunderingSupervisoryBodies)
      ? (verifier!.antiMoneyLaunderingSupervisoryBodies as unknown[]).filter(
          (x): x is string => typeof x === "string" && x.length > 0
        )
      : [];
    const verifierName =
      typeof verifier?.name === "string" && verifier.name ? verifier.name : undefined;
    return {
      route,
      ...(route === ROUTE_ACSP && verifierName ? { verifierName } : {}),
      supervisors: route === ROUTE_ACSP ? supervisors : [],
      ...(isoDate(iv.identityVerifiedOn) ? { identityVerifiedOn: isoDate(iv.identityVerifiedOn) } : {}),
      ...(isoDate(iv.firstVerificationStatementOn)
        ? { firstStatementOn: isoDate(iv.firstVerificationStatementOn) }
        : {}),
    };
  }
  return null;
}

export function isIdentityVerified(stmt: unknown): boolean {
  return readIdentityVerification(stmt) !== null;
}

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

/** "2025-07-28" → "28 July 2025". Parsed by hand: `new Date()` would shift a
 *  bare date across midnight in a negative-offset timezone. */
export function humanDate(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  return `${d} ${MONTHS[m - 1]} ${y}`;
}

/**
 * The sentence shown beside a person, e.g.
 * "Identity verified with Companies House through DE PINNA LLP ACSP on 28 July 2025."
 * Never a date or a provider the register did not publish.
 */
export function identityVerificationSentence(v: IdentityVerification): string {
  if (v.route === ROUTE_ACSP) {
    const by = v.verifierName ? ` through ${v.verifierName}` : " through an authorised corporate service provider";
    const on = v.identityVerifiedOn ? ` on ${humanDate(v.identityVerifiedOn)}` : "";
    return `${IDENTITY_VERIFIED_LABEL}${by}${on}.`;
  }
  return `${IDENTITY_VERIFIED_LABEL}; the register names no service provider.`;
}
