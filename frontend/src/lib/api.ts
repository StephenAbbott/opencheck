/**
 * Thin typed client for the OpenCheck backend.
 *
 * Phase 1 surface: /health, /sources, /search, /stream (SSE), /deepen.
 * Phase 2 surface: /lookup-stream (SSE, LEI-anchored progressive lookup).
 */

import { trackEvent } from "./analytics";
import type { Liveness, SourceLiveness } from "../components/cdd/LivenessBadge";

export type { Liveness, SourceLiveness };

export type SearchKind = "entity" | "person";

/** EU/EEA beneficial-ownership access notice for a national register, computed
 *  by the backend from the country's `restricted_from` date and today. */
export interface BoAccessNotice {
  status: "restricted" | "becoming_restricted";
  country_code: string;
  country_name: string;
  /** ISO date the restriction takes effect — only set for `becoming_restricted`. */
  effective_date: string | null;
  access_url: string | null;
}

export interface SourceInfo {
  id: string;
  name: string;
  homepage: string;
  description: string;
  license: string;
  attribution: string;
  supports: SearchKind[];
  requires_api_key: boolean;
  live_available: boolean;
  /** "cdd" = customer due diligence / compliance; "esg" = environmental, social and governance. */
  category: "cdd" | "esg";
  /** True for official national company / BO registers (e.g. Companies House, Bolagsverket). */
  is_national_register: boolean;
  /** ISO 3166-1 alpha-2 code for national registers; null for global sources. */
  country?: string | null;
  /** EU/EEA beneficial-ownership access notice, or null when unrestricted. */
  bo_access?: BoAccessNotice | null;
}

export interface SourceHit {
  source_id: string;
  hit_id: string;
  kind: SearchKind;
  name: string;
  /** Identifier fragment ("GB · registered entity") — what the search-result
   *  rows, the share card and og_image.py have always consumed. */
  summary: string;
  /** One plain-English sentence saying what this source actually said about
   *  the subject, built by the adapter from fields it already parsed
   *  (`opencheck/findings.py`). Absent for sources with no template yet, and
   *  for payloads recorded before Phase 122 — the row falls back to
   *  `summary`, so an un-migrated source looks like v1 rather than broken. */
  finding?: string | null;
  identifiers: Record<string, string>;
  raw: Record<string, unknown>;
  is_stub: boolean;
  /** How current this payload is — see LivenessBadge. Defaults to "stub" so a
   *  source that declares nothing under-claims rather than over-claims. */
  liveness?: Liveness;
  /** When OpenCheck actually obtained the payload; null when nothing was
   *  fetched (stub and curated data). */
  retrieved_at?: string | null;
}

export interface CrossSourceLink {
  key: string;
  key_value: string;
  confidence: "strong" | "possible";
  /** Independent origins among `hits` after source lineage (GLEIF +
   *  OpenSanctions sharing an LEI is one). Absent on payloads recorded before
   *  Phase 150; the client recomputes from `lineage.ts` regardless. */
  independent_source_count?: number;
  hits: { source_id: string; hit_id: string; name: string }[];
}

/** A name-only "likely same" entity candidate (same name + jurisdiction, no
 *  shared identifier) — a human-review suggestion, never a confirmed match.
 *  `a`/`b` are entity BODS statementIds; `a_name`/`b_name`/`jurisdiction` are
 *  carried so the report can render the pair without the BODS bundle. */
export interface PossiblySameEntity {
  a: string;
  b: string;
  reason: string;
  a_name: string;
  b_name: string;
  jurisdiction: string;
  /** Which source asserted each record — context for the human review. */
  a_source?: string;
  b_source?: string;
}

/** One identifier surfaced by the MEIP signpost. `corroborated` = GLEIF also
 *  publishes this identifier for the LEI. */
export interface MeipIdentifier {
  scheme: string; // "lei" | "opencorporates" | "permid" | "capiq"
  label: string;
  value: string;
  corroborated: boolean;
}

/** OECD-UNSD MEIP signpost match for the subject LEI. Not mapped to BODS — a
 *  pointer to the richer MEIP dataset on the OECD site. */
export interface MeipMatch {
  mode: "subsidiary" | "mne_head";
  lei: string;
  name: string;
  iso3: string;
  parent_mne: string;
  immediate_parent: string | null;
  alt_names: string[];
  address: string;
  identifiers: MeipIdentifier[];
  subsidiaries_total: number | null;
  subsidiaries_with_lei: number | null;
  source_url: string;
}

/** A single risk signal — see backend opencheck/risk.py for the rule list. */
export interface RiskSignal {
  code: string;
  confidence: "high" | "medium" | "low";
  summary: string;
  source_id: string;
  hit_id: string;
  evidence: Record<string, unknown>;
  /** `"risk"` (default) or `"context"`. A *context* signal is a structural
   * observation, not an adverse finding — show it, but never present it as
   * a risk or count it in "N risk signals". Optional so cached responses
   * predating the field still parse; treat a missing value as `"risk"`. */
  kind?: "risk" | "context";
}

/** One derived risk check that did not fully run for this lookup (issue
 * #50). An empty risk_signals list alongside a non-empty degraded list is
 * NOT a clean screen — the affected signals were never fully screened.
 * Carries counts only, never related-party names. */
export interface DegradedSource {
  /** Upstream adapter id ("opensanctions", "icij", ...); "opencheck" when
   *  the failure happened before reaching any upstream. */
  source_id: string;
  /** Which derived check degraded ("cross_source_names", "icij_offshore_leaks"). */
  check: string;
  /** Risk codes whose absence is now unreliable. */
  affected_signals: string[];
  /** Human-readable failure summary — counts only. */
  detail: string;
  reason: "upstream_error" | "timeout" | "not_configured" | "rate_limited";
}

/** One informational related-party match from OpenAleph percolation
 * (Phase 96): an attributed, similarity-gated hit whose topics map to no
 * RELATED_* risk code — leak/court collections, poi, corp.disqual.
 * Name-derived — never identifier corroboration. */
export interface OpenAlephScreeningMatch {
  /** BODS statementId of the related party the match attributes to. */
  statement_id: string;
  /** The related-party name that was screened. */
  search_name: string;
  kind: "person" | "entity";
  /** The OpenAleph record's own (closest) name. */
  matched_name: string;
  entity_id: string;
  /** Collection label, e.g. "Russian Oligarch Database". */
  collection: string;
  /** Public OpenAleph UI link for the matched record. */
  url: string;
  topics: string[];
  /** The exact phrase of ours the percolator fired on. */
  surface_form: string;
  percolator_match: string[];
  score: number;
}

export interface SearchResponse {
  query: string;
  kind: SearchKind;
  hits: SourceHit[];
  errors: Record<string, string>;
  cross_source_links: CrossSourceLink[];
  risk_signals: RiskSignal[];
}

export interface LookupResponse {
  lei: string;
  legal_name: string | null;
  jurisdiction: string | null;
  derived_identifiers: Record<string, string>;
  query: string;
  kind: SearchKind;
  hits: SourceHit[];
  errors: Record<string, string>;
  cross_source_links: CrossSourceLink[];
  possibly_same_entities: PossiblySameEntity[];
  meip: MeipMatch | null;
  risk_signals: RiskSignal[];
  degraded_sources: DegradedSource[];
  openaleph_screening?: OpenAlephScreeningMatch[];
  /** How current each source's payload is, keyed by source_id. Sibling to
   *  degraded_sources: data that is not current must not read as live. */
  source_liveness?: Record<string, SourceLiveness>;
  /** How big the mapped graph is. Absent on payloads recorded before this
   *  field existed, which is why every consumer treats it as optional. */
  graph_shape?: GraphShape;
  /** One deterministic sentence stating what the check found — built from
   *  the signals and degradations by the backend (`opencheck/verdict.py`),
   *  never by a model, so the page, the PDF, the share card and the API
   *  cannot disagree. Absent on payloads recorded before Phase 122. */
  verdict?: string | null;
  /** What the registers say the subject *is* (Phase 154) — see
   *  `SubjectProfile`. Absent on payloads recorded before this field existed. */
  subject_profile?: SubjectProfile | null;
  bods: Record<string, unknown>[];
  bods_issues: string[];
  license_notices: { source_id: string; hit_id: string; notice: string }[];
}

/** One profile fact and who states it. `sources` are the adapter ids whose
 *  value agrees with `value`; `independent_sources` counts them through the
 *  lineage table (OpenCorporates republishing Companies House is one). */
export interface SubjectProfileFact {
  value: string;
  sources: string[];
  independent_sources: number;
  other_values: { source_id: string; value: string }[];
}

export interface SubjectProfileStatus {
  liveness: "live" | "pending" | "terminal";
  since: string | null;
  raw: string | null;
  /** The source whose status is shown — the register before GLEIF. */
  source_id: string;
  sources: string[];
  independent_sources: number;
  other_values: { source_id: string; value: string }[];
}

/** The subject's profile, assembled by `opencheck/subject_profile.py` from
 *  the subject's own entity statements: facts, never findings. */
export interface SubjectProfile {
  legal_form: SubjectProfileFact | null;
  register_status: SubjectProfileStatus | null;
  founding_date: SubjectProfileFact | null;
  registered_address: (SubjectProfileFact & { country: string }) | null;
  jurisdiction: string | null;
  statement_ids: string[];
}

export interface SubjectProfileEvent {
  profile: SubjectProfile | null;
}

/** `graph_shape` on the `risk_signals` event: the size of the ownership-and-
 *  control graph this check produced, deduplicated by statementId. `depth` is
 *  the longest chain the risk layer measured, or null when it measured none —
 *  never 0, which would render as a flat graph. */
export interface GraphShape {
  companies: number;
  people: number;
  relationships: number;
  depth: number | null;
}

export interface DeepenResponse {
  source_id: string;
  hit_id: string;
  raw: Record<string, unknown>;
  bods: Record<string, unknown>[];
  bods_issues: string[];
  license: string;
  license_notice: string | null;
  risk_signals: RiskSignal[];
}

// In dev the Vite dev-server proxy (vite.config.ts) intercepts these paths
// server-side and forwards them to the backend, so the browser only ever
// sees the same origin — which means relative URLs work from any device
// (phones, VMs, etc.) without CORS issues.
//
// In production (static build on Render / any CDN) there is no proxy, so we
// bake in the absolute backend URL at build time via the VITE_API_BASE_URL
// environment variable.  The Render dashboard sets this to the backend
// service URL (e.g. https://api.opencheck.world).
export const BASE_URL: string = import.meta.env.DEV
  ? ""
  : ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "");

/**
 * Build a URL to the /export endpoint that browsers can hit directly
 * via an <a download> link. The backend's Content-Disposition header
 * carries the canonical filename — we just hand the browser the URL.
 *
 * The LEI-anchored export reuses the same /export endpoint with the
 * ``lei`` parameter; backend dispatches to the LEI synthesis path
 * (not the free-text /report one).
 */
/** Exactly `_EXPORT_FORMATS` in `backend/opencheck/routers/export.py`. The
 *  backend rejects anything outside that set with a 400, so the picker must
 *  not be able to name one — an export chip that produces an error is worse
 *  than an absent format. */
export type ExportFormat =
  | "json"
  | "jsonl"
  | "zip"
  | "xml"
  | "csv"
  | "xlsx"
  | "cypher"
  | "senzing"
  | "ftm"
  | "gql"
  | "amlai"
  | "rdf";

/**
 * Every export format, once, in the order the picker shows them.
 *
 * Built from a `Record<ExportFormat, ...>` rather than written out as an
 * array, so adding a format to the type without adding it here fails the
 * build. Two surfaces read it — the download picker and the API reference on
 * the About page — and the API reference is where the list drifted before
 * (Phase 81 fixed it once; it drifted again by two the moment csv and cypher
 * landed).
 */
const EXPORT_FORMAT_ORDER: Record<ExportFormat, number> = {
  json: 0,
  zip: 1,
  csv: 2,
  xlsx: 3,
  jsonl: 4,
  xml: 5,
  ftm: 6,
  cypher: 7,
  rdf: 8,
  senzing: 9,
  gql: 10,
  amlai: 11,
};

export const EXPORT_FORMATS: readonly ExportFormat[] = (
  Object.keys(EXPORT_FORMAT_ORDER) as ExportFormat[]
).sort((a, b) => EXPORT_FORMAT_ORDER[a] - EXPORT_FORMAT_ORDER[b]);

export function exportUrl(
  lei: string,
  format: ExportFormat,
  opts?: { subsidiaries?: boolean }
): string {
  const params = new URLSearchParams({ lei, format });
  if (opts?.subsidiaries) params.set("subsidiaries", "true");
  return `${BASE_URL}/export?${params.toString()}`;
}

/** Progressive discovery: resolve one corporate node a hop deeper. Returns the
 * new layer as BODS statements, with the looked-up entity's identity remapped
 * onto `anchor` so it stitches onto the existing graph node. */
export interface ExpandResponse {
  lei: string;
  anchor: string;
  bods: Record<string, unknown>[];
}

export async function expandNode(
  lei: string,
  anchor: string
): Promise<ExpandResponse> {
  trackEvent("graph_expand"); // feature event; no subject identifiers recorded
  const params = new URLSearchParams({ lei, anchor });
  return getJson<ExpandResponse>(`/expand?${params.toString()}`);
}

/** Batch ("add next layer"): go one hop deeper on the whole frontier at once.
 * Each item is a (lei, anchor) pair; the server fans out concurrently and
 * returns the merged, de-duplicated layer. */
export interface ExpandLayerResponse {
  bods: Record<string, unknown>[];
  /** Risk signals the per-hop sub-lookups screened for the expanded entities,
   *  with statement-id evidence remapped onto each anchor. Drives FullCheck's
   *  network-wide risk + the QuickCheck-vs-FullCheck comparison. */
  risk_signals: RiskSignal[];
  expanded: string[];
  count: number;
  truncated: boolean;
}

export type NetworkExportFormat =
  | "json"
  | "jsonl"
  | "xml"
  | "senzing"
  | "ftm"
  | "cypher"
  | "gql"
  | "amlai"
  | "rdf"
  | "zip";

/** Export a client-assembled FullCheck network (BODS) in the chosen format and
 * trigger a browser download. Reuses the server's Senzing / XML / Cypher /
 * licensing machinery via POST /export-network. */
export async function downloadNetwork(
  bods: Record<string, unknown>[],
  format: NetworkExportFormat,
  slug?: string
): Promise<void> {
  const r = await fetch(`${BASE_URL}/export-network`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ bods, format, slug }),
  });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} — /export-network`);
  const blob = await r.blob();
  const cd = r.headers.get("content-disposition") ?? "";
  const m = /filename="?([^"]+)"?/.exec(cd);
  const filename = m ? m[1] : `opencheck-network.${format === "zip" ? "zip" : "txt"}`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export async function expandLayer(
  items: { lei: string; anchor: string }[],
  direction: "owners" | "subsidiaries" = "owners"
): Promise<ExpandLayerResponse> {
  trackEvent("graph_expand"); // feature event; no subject identifiers recorded
  const r = await fetch(`${BASE_URL}/expand-layer`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items, direction }),
  });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} — /expand-layer`);
  return (await r.json()) as ExpandLayerResponse;
}

async function getJson<T>(path: string): Promise<T> {
  const r = await fetch(`${BASE_URL}${path}`);
  if (!r.ok) {
    throw new Error(`${r.status} ${r.statusText} — ${path}`);
  }
  return (await r.json()) as T;
}

export function fetchSources(): Promise<{ sources: SourceInfo[] }> {
  return getJson("/sources");
}

// --- Source health (Phase 161) ---------------------------------------------

export type SourceHealthStatus = "ok" | "degraded" | "fail" | "skipped";

/** One source's row from the last weekly sweep, as `/source-health` shapes it. */
export interface SourceHealthRow {
  status: SourceHealthStatus;
  /** Why it is not `ok` — already credential-redacted by the sweep. */
  reason: string;
  /** A defect the probe knowingly asserts around, printed rather than hidden. */
  known_gap: string;
  liveness: "live" | "cached" | "snapshot" | "curated" | "stub" | null;
  retrieved_at: string | null;
  latency_ms: number | null;
  attempts: number;
  /** Entity + person + relationship statements the probe subject mapped to. */
  statement_total: number | null;
  /** Week-over-week collapse, when the sweep reported one. */
  statement_collapse: Record<string, { was: number; now: number }> | null;
  /** Oldest first; the last entry is this sweep. */
  history: SourceHealthStatus[];
}

export type SourceHealthReport =
  | {
      available: true;
      generated_at: string;
      compared_against: string | null;
      registry_size: number | null;
      probed: number | null;
      counts: Record<SourceHealthStatus, number>;
      /** `generated_at` of each sweep in the history, oldest first. */
      sweeps: string[];
      sources: Record<string, SourceHealthRow>;
      /** Set when the asset could not be re-read and this is the last good copy. */
      stale?: boolean;
    }
  | { available: false; reason: string };

export function fetchSourceHealth(): Promise<SourceHealthReport> {
  return getJson("/source-health");
}

// --- Licensing compatibility matrix (the export "licensing assistant") -------

export interface LicenseTerms {
  license: string;
  name: string;
  url: string | null;
  commercial_use: "yes" | "no" | "conditional";
  attribution_required: boolean;
  share_alike: boolean;
  redistribution: "yes" | "no" | "conditional";
  color: "green" | "amber" | "red";
  summary: string;
}

export interface SourceLicensing {
  source_id: string;
  name: string;
  license: string;
  terms: LicenseTerms;
}

export interface LicenseAssessment {
  commercial_use: "yes" | "no" | "conditional";
  attribution_required: boolean;
  share_alike: boolean;
  color: "green" | "amber" | "red";
  headline: string;
  warnings: string[];
  per_source: SourceLicensing[];
  disclaimer: string;
}

export interface LicenseMatrix {
  disclaimer: string;
  sources: SourceLicensing[];
  licenses: LicenseTerms[];
  assessment?: LicenseAssessment;
}

/** Licensing matrix; pass contributing source ids to also get a combined
 * commercial-use assessment for the current result. */
export function getLicenseMatrix(sourceIds?: string[]): Promise<LicenseMatrix> {
  const params = new URLSearchParams();
  if (sourceIds && sourceIds.length > 0) params.set("sources", sourceIds.join(","));
  const qs = params.toString();
  return getJson(`/license-matrix${qs ? `?${qs}` : ""}`);
}

/**
 * Drive the LEI-anchored lookup: GLEIF → cross-source bridges →
 * unified subject view. Throws an Error with the backend's detail
 * message when the LEI is malformed (400) or unknown to GLEIF (404).
 */
// ---------------------------------------------------------------------
// Securities — /securities (GLEIF ISINs + OpenFIGI typing + OpenSanctions)
// ---------------------------------------------------------------------

export interface Security {
  isin: string;
  type: string | null;
  name: string | null;
  ticker: string | null;
  exchange: string | null;
  sanctioned: boolean;
  regimes?: string[];
  opensanctions_id?: string | null;
}

export interface SecuritiesResponse {
  lei: string;
  available: boolean;
  total: number;
  page: number;
  page_size: number;
  securities: Security[];
  sanctioned: Security[];
  /** Phase 145: false when GLEIF could not be queried (rate-limited or down).
   *  The sanctioned overlay is a local index on the backend and still runs,
   *  so `sanctioned` is trustworthy even when `total`/`securities` are not. */
  isin_list_available: boolean;
  sources: string[];
  license_notices: { source_id: string; notice: string }[];
}

/** Fetch one page of securities (ISINs) for an LEI, with the sanctioned subset. */
export async function getSecurities(lei: string, page = 1): Promise<SecuritiesResponse> {
  const params = new URLSearchParams({ lei, page: String(page) });
  const r = await fetch(`${BASE_URL}/securities?${params.toString()}`);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return (await r.json()) as SecuritiesResponse;
}

// ---------------------------------------------------------------------
// History / Time Machine — /history (GLEIF change log + Companies House)
// ---------------------------------------------------------------------

export interface HistoryEntry {
  change_type: string;
  label: string;
  tier: number; // 1 = ownership/control, 2 = identity/status
  record_type: string; // "entity" | "relationship"
  date: string | null;
  date_basis: string; // "effective" | "recorded" | "snapshot_window"
  date_confidence: string; // "high" | "medium" | "low"
  value_old: string | null;
  value_new: string | null;
  sources: string[];
  corroborating_sources: string[];
  counterparty: string | null;
  interest_start_date: string | null;
  interest_end_date: string | null;
  boosted: boolean;
}

export interface HistoryRawChange {
  source_id: string;
  record_type: string;
  raw_change_type: string;
  raw_field: string | null;
  value_old: string | null;
  value_new: string | null;
  change_type: string | null;
  tier: number;
  event_date: string | null;
  date_basis: string;
}

export interface HistoryResponse {
  lei: string;
  company_number: string | null;
  available: boolean;
  sources: string[];
  notable_count: number;
  notable: HistoryEntry[];
  events: HistoryRawChange[];
  /** Phase 146: false when GLEIF refused that call (rate-limited or down).
   *  An empty timeline alongside either false is "could not check", not
   *  "checked, nothing there" — the two used to be indistinguishable. */
  gleif_record_available: boolean;
  gleif_events_available: boolean;
  /** The GLEIF record failed and no cached one stood in, so the registry
   *  history sources could not be attempted at all. */
  registry_sources_blocked: boolean;
  /** "live" | "cached" | null — where `company_number` came from. */
  company_number_basis: string | null;
}

/** Fetch the Time Machine timeline for an LEI (notable changes, GLEIF + CH). */
export async function getHistory(
  lei: string,
  includeNoise = false,
): Promise<HistoryResponse> {
  const params = new URLSearchParams({ lei });
  if (includeNoise) params.set("include_noise", "true");
  const r = await fetch(`${BASE_URL}/history?${params.toString()}`);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return (await r.json()) as HistoryResponse;
}

// ---------------------------------------------------------------------
// NZ associations — /nz-associations (director/shareholder cross-company links)
// ---------------------------------------------------------------------

export interface NzAssociatedCompany {
  number: string;
  name: string | null;
  nzbn: string | null;
  roles: string[]; // "director" | "shareholder"
  share_percentage: number | null;
  confidence: string; // "high" | "medium"
  basis: string;
  link: string | null;
}

export interface NzPersonAssociations {
  name: string;
  role_here: string[];
  other_company_count: number;
  high_confidence_count: number;
  /** High + medium — corroborated by a matching registered address. */
  address_match_count: number;
  /** Low — name matches but no address corroboration. */
  name_only_count: number;
  as_director: number;
  as_shareholder: number;
  total_records_under_name: number;
  truncated: boolean;
  companies: NzAssociatedCompany[];
}

export interface NzAssociationsResponse {
  company_number: string;
  available: boolean;
  reason: string | null;
  subject_name: string | null;
  checked: number;
  not_checked: number;
  people: NzPersonAssociations[];
}

/** Director/shareholder cross-company associations for an NZ company. */
export async function getNzAssociations(
  companyNumber: string,
): Promise<NzAssociationsResponse> {
  const params = new URLSearchParams({ company_number: companyNumber });
  const r = await fetch(`${BASE_URL}/nz-associations?${params.toString()}`);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return (await r.json()) as NzAssociationsResponse;
}

// ---------------------------------------------------------------------
// Subsidiary network — /subsidiaries (GLEIF direct + ultimate children)
// ---------------------------------------------------------------------

export interface SubsidiaryChild {
  lei: string;
  name: string | null;
  jurisdiction: string | null;
  status: string | null;
  relation: "direct" | "ultimate" | "both";
  link: string | null;
}

export interface SubsidiaryJurisdiction {
  code: string;
  count: number;
}

export interface SubsidiariesResponse {
  lei: string;
  available: boolean;
  reason: string | null;
  /** Phase 146: false when GLEIF refused both relation calls and no snapshot
   *  stood in — an empty `children` list is then not a finding about the
   *  entity, and the panel must say so instead of rendering "no network". */
  children_available: boolean;
  direct_available: boolean;
  ultimate_available: boolean;
  /** Direct children served from OpenCheck's Golden Copy snapshot, not live. */
  snapshot_fallback: boolean;
  snapshot_date: string | null;
  /** One sentence naming what GLEIF did not answer; null when it answered. */
  degraded_detail: string | null;
  direct_total: number;
  ultimate_total: number;
  distinct_fetched: number;
  indirect_only: number;
  node_estimate: number;
  render_mode: "graph" | "table";
  truncated: boolean;
  jurisdictions: SubsidiaryJurisdiction[];
  children: SubsidiaryChild[];
  bods: Record<string, unknown>[] | null;
}

/** GLEIF subsidiary network (direct + ultimate children) for a subject LEI.
 *  `format: "bods"` additionally returns the BODS statements for the graph. */
export async function getSubsidiaries(
  lei: string,
  format: "summary" | "bods" = "summary",
): Promise<SubsidiariesResponse> {
  const params = new URLSearchParams({ lei });
  if (format === "bods") params.set("format", "bods");
  const r = await fetch(`${BASE_URL}/subsidiaries?${params.toString()}`);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return (await r.json()) as SubsidiariesResponse;
}

// --- BackgroundCheck (person screening) — SPIKE feat/background-check -------

/** One hit from /person-check, scored against the queried name. */
export interface PersonMatch {
  hit: SourceHit;
  name_score: number;
  birth_year_compatible: boolean;
  strong: boolean;
}

/** Per-source outcome for a person check — powers the honest
 * "what was checked" footer (a no-hit source is still shown). */
export interface PersonCheckSource {
  source_id: string;
  name: string;
  license: string;
  attribution: string;
  homepage: string;
  live: boolean;
  hit_count: number;
  error: string | null;
}

export interface PersonCheckResponse {
  query: string;
  birth_year: number | null;
  matches: PersonMatch[];
  risk_signals: RiskSignal[];
  weak_match_count: number;
  sources: PersonCheckSource[];
  caveats: string[];
  /** Identifier-backed links between strong matches only (shared Q-ID /
   * OpenSanctions id) — the person-world cross-source panel. */
  cross_source_links: CrossSourceLink[];
}

/** One Companies House appointment held by an officer. */
export interface AppointmentItem {
  company_name: string;
  company_number: string | null;
  company_status: string | null;
  role: string | null;
  appointed_on: string | null;
  resigned_on: string | null;
}

export interface PersonAppointmentsResponse {
  officer_id: string;
  name: string | null;
  birth_date: string | null;
  is_stub: boolean;
  total_results: number | null;
  active_count: number;
  appointments: AppointmentItem[];
  bods: Record<string, unknown>[];
  attribution: string;
  caveat: string;
}

export async function personAppointments(
  officerId: string
): Promise<PersonAppointmentsResponse> {
  const params = new URLSearchParams({ officer_id: officerId });
  return getJson(`/person-appointments?${params.toString()}`);
}

/** One political position held (EveryPolitician / OpenSanctions PEPs). */
export interface PositionItem {
  label: string;
  country: string | null;
  start_date: string | null;
  end_date: string | null;
  current: boolean;
}

export interface PersonPositionsResponse {
  entity_id: string;
  name: string | null;
  is_stub: boolean;
  positions: PositionItem[];
  wikidata_qid: string | null;
  countries: string[];
  source_url: string;
  attribution: string;
  maintenance_note: string;
  caveat: string;
}

export async function personPositions(
  entityId: string
): Promise<PersonPositionsResponse> {
  const params = new URLSearchParams({ entity_id: entityId });
  return getJson(`/person-positions?${params.toString()}`);
}

export async function personCheck(
  name: string,
  birthYear?: number
): Promise<PersonCheckResponse> {
  const params = new URLSearchParams({ name });
  if (birthYear) params.set("birth_year", String(birthYear));
  return getJson(`/person-check?${params.toString()}`);
}

export async function lookup(lei: string): Promise<LookupResponse> {
  const params = new URLSearchParams({ lei });
  const r = await fetch(`${BASE_URL}/lookup?${params.toString()}`);
  if (!r.ok) {
    let detail = `${r.status} ${r.statusText}`;
    try {
      const body = await r.json();
      if (body?.detail) detail = body.detail;
    } catch {
      /* fall through */
    }
    throw new Error(detail);
  }
  return (await r.json()) as LookupResponse;
}

// ---------------------------------------------------------------------
// Narrative summary — /narrative
// ---------------------------------------------------------------------

/** One atomic, evidenced statement the narrative may draw on. */
export interface NarrativeFact {
  id: string;
  statement: string;
  source_name: string;
  source_id: string | null;
  source_url: string | null;
  bods_statement_ids: string[];
  confidence: "high" | "medium" | "low";
}

export interface NarrativeRisk {
  id: string;
  code: string;
  label: string;
  confidence: "high" | "medium" | "low";
  rationale: string;
  source_name: string;
  source_id: string | null;
  fact_ids: string[];
}

export interface NarrativeGap {
  id: string;
  statement: string;
}

export interface EvidencePacket {
  subject_name: string;
  lei: string | null;
  jurisdiction: string | null;
  subject_confidence: "identifier-confirmed" | "name-matched" | string;
  identifiers: Record<string, string>;
  facts: NarrativeFact[];
  risks: NarrativeRisk[];
  sources_consulted: { source_id: string; name: string; license: string; homepage: string | null }[];
  gaps: NarrativeGap[];
}

/** One claim the model made, each grounded in packet evidence ids (f/r/g). */
export interface NarrativeClaim {
  id: string;
  text: string;
  fact_ids: string[];
  confidence: "high" | "medium" | "low";
}

export interface NarrativeResponse {
  lei: string | null;
  subject_name: string;
  summary: string;
  claims: NarrativeClaim[];
  limitations: string[];
  overall_confidence: "high" | "medium" | "low";
  model: string;
  prompt_version: string;
  /** Deterministic id of this exact narrative — dispositions are keyed to it.
   *  Optional: pre-baked curated narratives predate the field. */
  run_id?: string;
  generated_at?: string;
  packet: EvidencePacket;
  validation_ok: boolean;
  dropped_claims: NarrativeClaim[];
  validation_issues: string[];
  /** Packet gap ids no surviving claim cited ("clear fallbacks, not silent gaps"). */
  uncited_gaps?: string[];
}

// ---------------------------------------------------------------------
// Analyst dispositions — /narrative/dispositions
// ---------------------------------------------------------------------

export type DispositionStatus = "accepted" | "disputed" | "needs_review";

export interface ClaimDisposition {
  claim_id: string;
  status: DispositionStatus;
  comment: string | null;
  decided_at?: string | null;
}

export interface DispositionRecord {
  lei: string;
  run_id: string;
  prompt_version: string;
  model: string;
  reviewer: string | null;
  dispositions: ClaimDisposition[];
  /** The analyst has read the summary as a whole. Never derived from the
   *  claim decisions: "I have read this" is a weaker statement than "I accept
   *  every sentence in it". Bound to `run_id`, so a regenerate clears it. */
  reviewed?: boolean;
  reviewed_at?: string | null;
  updated_at?: string | null;
}

/**
 * Persist the analyst's claim dispositions for one narrative run (whole-sheet
 * overwrite; timestamps are stamped server-side). Returns the stored record.
 */
export async function putDispositions(
  lei: string,
  runId: string,
  dispositions: { claim_id: string; status: DispositionStatus; comment: string | null }[],
  meta: { prompt_version?: string; model?: string; reviewed?: boolean } = {},
): Promise<DispositionRecord> {
  const r = await fetch(`${BASE_URL}/narrative/dispositions`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      lei,
      run_id: runId,
      prompt_version: meta.prompt_version ?? "",
      model: meta.model ?? "",
      dispositions,
      // Always sent, never omitted: the sheet is a whole-sheet overwrite, so
      // leaving it out on a withdrawal would make "reviewed" impossible to
      // clear.
      reviewed: meta.reviewed ?? false,
    }),
  });
  if (!r.ok) {
    let detail = `${r.status} ${r.statusText}`;
    try {
      const body = await r.json();
      if (body?.detail) detail = body.detail;
    } catch {
      /* fall through */
    }
    throw new Error(detail);
  }
  return (await r.json()) as DispositionRecord;
}

/** Fetch the stored disposition sheet for a narrative run, or null when none exists. */
export async function getDispositions(
  lei: string,
  runId: string,
): Promise<DispositionRecord | null> {
  const params = new URLSearchParams({ lei, run_id: runId });
  const r = await fetch(`${BASE_URL}/narrative/dispositions?${params.toString()}`);
  if (r.status === 404) return null;
  if (!r.ok) return null; // hydration is best-effort — the panel still works without it
  return (await r.json()) as DispositionRecord;
}

/**
 * Fetch a grounded narrative summary for a resolved LEI. Every claim cites
 * evidence in `packet`; nothing is asserted beyond OpenCheck's own data.
 * Throws with the backend detail on 404 (disabled) / 503 (no key) / 5xx.
 */
export async function fetchNarrative(
  lei: string,
  deepenTop = 5,
): Promise<NarrativeResponse> {
  const params = new URLSearchParams({ lei, deepen_top: String(deepenTop) });
  const r = await fetch(`${BASE_URL}/narrative?${params.toString()}`);
  if (!r.ok) {
    let detail = `${r.status} ${r.statusText}`;
    try {
      const body = await r.json();
      if (body?.detail) detail = body.detail;
    } catch {
      /* fall through */
    }
    throw new Error(detail);
  }
  return (await r.json()) as NarrativeResponse;
}

/**
 * Look for a pre-baked narrative for a curated example, served as a static
 * file from the frontend's own origin (`/curated-narratives/<lei>.json`). These
 * are generated offline so curated examples show an instant, cited summary with
 * no model call. Returns null when there's no cached file (the normal case for
 * live lookups), so the panel falls back to the on-demand "Generate" button.
 */
export async function fetchCuratedNarrative(
  lei: string,
): Promise<NarrativeResponse | null> {
  try {
    const r = await fetch(`/curated-narratives/${encodeURIComponent(lei)}.json`, {
      headers: { Accept: "application/json" },
    });
    if (!r.ok) return null;
    const ct = r.headers.get("Content-Type") ?? "";
    if (!ct.includes("json")) return null; // a SPA 404 may return index.html
    return (await r.json()) as NarrativeResponse;
  } catch {
    return null;
  }
}

/**
 * POST a report-export request and trigger the browser download of the
 * response. Shared by the PDF and Markdown report downloads — same request
 * body, different route/extension. Throws with the backend detail on failure.
 */
async function downloadReport(
  path: "/export/pdf" | "/export/markdown",
  fallbackExt: "pdf" | "md",
  lei: string,
  narrative?: NarrativeResponse | null,
  dispositions?: DispositionRecord | null,
): Promise<void> {
  const r = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      lei,
      narrative: narrative ?? null,
      dispositions: dispositions ?? null,
    }),
  });
  if (!r.ok) {
    let detail = `${r.status} ${r.statusText}`;
    try {
      const body = await r.json();
      if (body?.detail) detail = body.detail;
    } catch {
      /* fall through */
    }
    throw new Error(detail);
  }
  const blob = await r.blob();
  const cd = r.headers.get("Content-Disposition") ?? "";
  const filename =
    /filename="?([^"]+)"?/.exec(cd)?.[1] ?? `opencheck-${lei}.${fallbackExt}`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/**
 * Download an accessible (tagged) PDF report for an LEI. POSTs to /export/pdf;
 * the already-generated narrative (if any) is sent so it can be embedded without
 * a fresh model call. Triggers the browser download and resolves when done.
 * Throws with the backend detail on failure (e.g. 503 if PDF is unavailable).
 */
export async function downloadReportPdf(
  lei: string,
  narrative?: NarrativeResponse | null,
  dispositions?: DispositionRecord | null,
): Promise<void> {
  return downloadReport("/export/pdf", "pdf", lei, narrative, dispositions);
}

/**
 * Download the due-diligence report as portable Markdown. Same request body
 * and embedded narrative/dispositions as the PDF, but always available — the
 * backend needs no PDF toolchain for this route.
 */
export async function downloadReportMarkdown(
  lei: string,
  narrative?: NarrativeResponse | null,
  dispositions?: DispositionRecord | null,
): Promise<void> {
  return downloadReport("/export/markdown", "md", lei, narrative, dispositions);
}

/** ISO 17442 LEI: 20-char alphanumeric. */
export const LEI_PATTERN = /^[A-Z0-9]{20}$/;
export function isValidLei(lei: string): boolean {
  return LEI_PATTERN.test(lei.trim().toUpperCase());
}

export function search(
  q: string,
  kind: SearchKind = "entity"
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q, kind });
  return getJson(`/search?${params.toString()}`);
}

export function deepen(
  source: string,
  hitId: string
): Promise<DeepenResponse> {
  const params = new URLSearchParams({ source, hit_id: hitId });
  return getJson(`/deepen?${params.toString()}`);
}

// ---------------------------------------------------------------------
// SSE — /stream
// ---------------------------------------------------------------------

export interface SourceStartedEvent {
  source_id: string;
  source_name: string;
}

export interface SourceCompletedEvent {
  source_id: string;
  hit_count: number;
}

export interface SourceErrorEvent {
  source_id: string;
  error: string;
  /** "schema_changed" when the source API structure changed; "fetch_error" otherwise. */
  error_type?: "schema_changed" | "fetch_error";
}

export interface DoneEvent {
  query: string;
  kind: SearchKind;
}

export interface CrossSourceLinksEvent {
  links: CrossSourceLink[];
}

export interface PossiblySameEntitiesEvent {
  pairs: PossiblySameEntity[];
}

export interface MeipEvent {
  match: MeipMatch | null;
}

export interface RiskSignalsEvent {
  signals: RiskSignal[];
  /** Derived checks that did not fully run — empty/absent when clean. */
  degraded_sources?: DegradedSource[];
  /** Informational OpenAleph percolation matches — empty/absent when none. */
  openaleph_screening?: OpenAlephScreeningMatch[];
  /** Per-source currency, keyed by source_id. */
  source_liveness?: Record<string, SourceLiveness>;
  /** How big the mapped graph is — see `GraphShape`. Rides on this event
   *  rather than `done` so the verdict strip's three columns all come from
   *  one payload and cannot disagree about the same run. */
  graph_shape?: GraphShape;
  /** One deterministic sentence stating what the check found — built from
   *  the signals and degradations by the backend (`opencheck/verdict.py`),
   *  never by a model, so the page, the PDF, the share card and the API
   *  cannot disagree. Absent on payloads recorded before Phase 122. */
  verdict?: string | null;
}

export type StreamHandlers = {
  onSourceStarted?: (e: SourceStartedEvent) => void;
  onHit?: (e: SourceHit) => void;
  onSourceCompleted?: (e: SourceCompletedEvent) => void;
  onSourceError?: (e: SourceErrorEvent) => void;
  onCrossSourceLinks?: (e: CrossSourceLinksEvent) => void;
  onRiskSignals?: (e: RiskSignalsEvent) => void;
  onDone?: (e: DoneEvent) => void;
  onError?: (err: Event) => void;
};

/**
 * Subscribe to the SSE search stream. Returns a cleanup function.
 */
export function streamSearch(
  q: string,
  kind: SearchKind,
  handlers: StreamHandlers
): () => void {
  const params = new URLSearchParams({ q, kind });
  const es = new EventSource(`${BASE_URL}/stream?${params.toString()}`);

  const safeParse = <T>(raw: string): T | null => {
    try {
      return JSON.parse(raw) as T;
    } catch {
      return null;
    }
  };

  es.addEventListener("source_started", (ev) => {
    const data = safeParse<SourceStartedEvent>((ev as MessageEvent).data);
    if (data) handlers.onSourceStarted?.(data);
  });
  es.addEventListener("hit", (ev) => {
    const data = safeParse<SourceHit>((ev as MessageEvent).data);
    if (data) handlers.onHit?.(data);
  });
  es.addEventListener("source_completed", (ev) => {
    const data = safeParse<SourceCompletedEvent>((ev as MessageEvent).data);
    if (data) handlers.onSourceCompleted?.(data);
  });
  es.addEventListener("source_error", (ev) => {
    const data = safeParse<SourceErrorEvent>((ev as MessageEvent).data);
    if (data) handlers.onSourceError?.(data);
  });
  es.addEventListener("cross_source_links", (ev) => {
    const data = safeParse<CrossSourceLinksEvent>((ev as MessageEvent).data);
    if (data) handlers.onCrossSourceLinks?.(data);
  });
  es.addEventListener("risk_signals", (ev) => {
    const data = safeParse<RiskSignalsEvent>((ev as MessageEvent).data);
    if (data) handlers.onRiskSignals?.(data);
  });
  es.addEventListener("done", (ev) => {
    const data = safeParse<DoneEvent>((ev as MessageEvent).data);
    if (data) handlers.onDone?.(data);
    es.close();
  });
  es.onerror = (err) => {
    handlers.onError?.(err);
    es.close();
  };

  return () => es.close();
}

// ---------------------------------------------------------------------
// SSE — /lookup-stream
// ---------------------------------------------------------------------

/** Emitted once GLEIF has resolved the LEI and derived cross-register IDs. */
export interface LookupGleifDoneEvent {
  lei: string;
  legal_name: string | null;
  jurisdiction: string | null;
  derived_identifiers: Record<string, string>;
}

/** Emitted right after gleif_done; lists every source_id that will be queried.
 *  Use this to render skeleton cards before any hits arrive. */
export interface LookupSourcesApplicableEvent {
  source_ids: string[];
}

/** Emitted when all sources have completed and post-processing is done. */
export interface LookupStreamDoneEvent {
  lei: string;
  bods_issues: string[];
  license_notices: { source_id: string; hit_id: string; notice: string }[];
}

/** Fatal error before streaming could start (e.g. invalid / unknown LEI). */
export interface LookupStreamErrorEvent {
  detail: string;
}

/** Entity / person / relationship split for a single deepened hit's BODS
 *  graph.
 *
 *  `persons` is counted separately because the diagram chip is labelled by
 *  the entity figure alone. Calling that total "parties" hid every natural
 *  person the source disclosed behind a number that excluded them — the
 *  reader saw "7 parties" over a diagram containing eleven nodes.
 *
 *  Optional: payloads recorded before the backend split it out carry only
 *  entities and relationships. */
export interface BodsBreakdown {
  entities: number;
  persons?: number;
  relationships: number;
}

/**
 * Emitted after the deepen batch completes.
 * counts maps "source_id:hit_id" → number of BODS statements for that hit.
 * breakdown maps the same key → entity / relationship split (for the graph CTA).
 */
export interface BodsCountsEvent {
  counts: Record<string, number>;
  breakdown?: Record<string, BodsBreakdown>;
}

/**
 * Emitted first (before any result) when the stream is served from the
 * backend's short-lived replay cache instead of a fresh run.
 */
export interface ReplayedEvent {
  /** Wall-clock UTC ISO 8601 completion time of the original run. */
  fetched_at: string;
  age_seconds: number;
}

export type LookupStreamHandlers = {
  onReplayed?: (e: ReplayedEvent) => void;
  onGleifDone?: (e: LookupGleifDoneEvent) => void;
  onSourcesApplicable?: (e: LookupSourcesApplicableEvent) => void;
  onSourceStarted?: (e: SourceStartedEvent) => void;
  onHit?: (e: SourceHit) => void;
  onSourceCompleted?: (e: SourceCompletedEvent) => void;
  onSourceError?: (e: SourceErrorEvent) => void;
  onCrossSourceLinks?: (e: CrossSourceLinksEvent) => void;
  onPossiblySame?: (e: PossiblySameEntitiesEvent) => void;
  onMeip?: (e: MeipEvent) => void;
  onRiskSignals?: (e: RiskSignalsEvent) => void;
  onBodsCounts?: (e: BodsCountsEvent) => void;
  onSubjectProfile?: (e: SubjectProfileEvent) => void;
  onDone?: (e: LookupStreamDoneEvent) => void;
  /** Called on both backend "error" events and EventSource network errors. */
  onError?: (detail: string) => void;
};

/**
 * Subscribe to the /lookup-stream SSE endpoint.
 * Returns a cleanup function that closes the connection.
 *
 * Event sequence:
 *   source_started (gleif) → gleif_done → hit (gleif) → source_completed (gleif)
 *   → sources_applicable → source_started* → {hit, source_completed}* (unordered)
 *   → cross_source_links → risk_signals → done
 */
export function streamLookup(
  lei: string,
  handlers: LookupStreamHandlers,
  deepen_top = 5,
  refresh = false,
): () => void {
  const params = new URLSearchParams({ lei, deepen_top: String(deepen_top) });
  if (refresh) params.set("refresh", "true");
  const es = new EventSource(`${BASE_URL}/lookup-stream?${params.toString()}`);

  const safeParse = <T>(raw: string): T | null => {
    try {
      return JSON.parse(raw) as T;
    } catch {
      return null;
    }
  };

  es.addEventListener("error", (ev) => {
    // Backend emitted an "error" event (e.g. invalid LEI, GLEIF not found).
    const data = safeParse<LookupStreamErrorEvent>((ev as MessageEvent).data);
    handlers.onError?.(data?.detail ?? "Unknown error");
    es.close();
  });
  es.addEventListener("replayed", (ev) => {
    const data = safeParse<ReplayedEvent>((ev as MessageEvent).data);
    if (data) handlers.onReplayed?.(data);
  });
  es.addEventListener("gleif_done", (ev) => {
    const data = safeParse<LookupGleifDoneEvent>((ev as MessageEvent).data);
    if (data) handlers.onGleifDone?.(data);
  });
  es.addEventListener("sources_applicable", (ev) => {
    const data = safeParse<LookupSourcesApplicableEvent>((ev as MessageEvent).data);
    if (data) handlers.onSourcesApplicable?.(data);
  });
  es.addEventListener("source_started", (ev) => {
    const data = safeParse<SourceStartedEvent>((ev as MessageEvent).data);
    if (data) handlers.onSourceStarted?.(data);
  });
  es.addEventListener("hit", (ev) => {
    const data = safeParse<SourceHit>((ev as MessageEvent).data);
    if (data) handlers.onHit?.(data);
  });
  es.addEventListener("source_completed", (ev) => {
    const data = safeParse<SourceCompletedEvent>((ev as MessageEvent).data);
    if (data) handlers.onSourceCompleted?.(data);
  });
  es.addEventListener("source_error", (ev) => {
    const data = safeParse<SourceErrorEvent>((ev as MessageEvent).data);
    if (data) handlers.onSourceError?.(data);
  });
  es.addEventListener("cross_source_links", (ev) => {
    const data = safeParse<CrossSourceLinksEvent>((ev as MessageEvent).data);
    if (data) handlers.onCrossSourceLinks?.(data);
  });
  es.addEventListener("possibly_same_entities", (ev) => {
    const data = safeParse<PossiblySameEntitiesEvent>((ev as MessageEvent).data);
    if (data) handlers.onPossiblySame?.(data);
  });
  es.addEventListener("meip", (ev) => {
    const data = safeParse<MeipEvent>((ev as MessageEvent).data);
    if (data) handlers.onMeip?.(data);
  });
  es.addEventListener("risk_signals", (ev) => {
    const data = safeParse<RiskSignalsEvent>((ev as MessageEvent).data);
    if (data) handlers.onRiskSignals?.(data);
  });
  es.addEventListener("bods_counts", (ev) => {
    const data = safeParse<BodsCountsEvent>((ev as MessageEvent).data);
    if (data) handlers.onBodsCounts?.(data);
  });
  es.addEventListener("subject_profile", (ev) => {
    const data = safeParse<SubjectProfileEvent>((ev as MessageEvent).data);
    if (data) handlers.onSubjectProfile?.(data);
  });
  es.addEventListener("done", (ev) => {
    const data = safeParse<LookupStreamDoneEvent>((ev as MessageEvent).data);
    if (data) handlers.onDone?.(data);
    es.close();
  });
  es.onerror = () => {
    // Network-level error (connection dropped, CORS failure, etc.)
    handlers.onError?.("Connection error");
    es.close();
  };

  return () => es.close();
}

// ---------------------------------------------------------------------
// Batch screening — /batch-stream (Phase 164 backend, Phase 166 page)
// ---------------------------------------------------------------------

/** One screened company, as `mcp.shaping.shape_batch_row` reduces it. */
export interface BatchRow {
  lei: string;
  legal_name: string | null;
  jurisdiction: string | null;
  register_status: {
    liveness: "live" | "pending" | "terminal";
    since?: string | null;
    raw?: string | null;
    source_id?: string;
  } | null;
  verdict: string | null;
  risk_count: number;
  context_count: number;
  risk_codes: string[];
  context_codes: string[];
  /** The GLEIF anchor is counted in both figures (Phase 156). */
  coverage: {
    applicable: number;
    answered: number;
    applicable_ids: string[];
    answered_ids: string[];
  };
  /** A screening check did not fully run — never a clean row. */
  degraded: boolean;
  degraded_sources: string[];
  licensing: { commercial_use: boolean; headline: string } | null;
  replayed: boolean;
  report_url: string;
}

/** A company that could not be screened at all — a row, not an exception. */
export interface BatchFailedRow {
  lei: string;
  status: number;
  reason: string;
  /** 503 — the shared upstream budget was momentarily spent. */
  retryable: boolean;
  degraded: true;
}

export interface BatchStartEvent {
  accepted: string[];
  rejected: { token: string; reason: string }[];
  overflow: number;
  cap: number;
  concurrency: number;
}

export interface BatchDoneEvent {
  requested: number;
  done: number;
  failed: number;
}

export type BatchStreamHandlers = {
  onStart?: (e: BatchStartEvent) => void;
  onRow?: (row: BatchRow) => void;
  onRowFailed?: (row: BatchFailedRow) => void;
  onDone?: (e: BatchDoneEvent) => void;
  /** HTTP refusal (422 nothing valid, 403 bot gate, 429 heavy tier) or a dropped connection. */
  onError?: (detail: string) => void;
};

/**
 * Subscribe to `/batch-stream`. Rows arrive in completion order, not paste
 * order — the page keeps a running placeholder per accepted LEI and fills
 * each in as its event lands. Returns a cleanup that aborts the request;
 * the backend cancels its in-flight pipelines when the client goes away.
 *
 * This one uses `fetch` rather than `EventSource`, deliberately: the route
 * sits on the heavy rate tier (a few batches a minute), so a 429 is an
 * ordinary outcome here, and an `EventSource` cannot say *why* it failed —
 * it fires `onerror` with no status. A reader who has just been told to
 * wait a minute needs those words, not "connection error".
 */
export function streamBatch(
  leis: string[],
  handlers: BatchStreamHandlers,
  deepen_top = 5,
  refresh = false,
): () => void {
  const params = new URLSearchParams({ leis: leis.join(","), deepen_top: String(deepen_top) });
  if (refresh) params.set("refresh", "true");
  const controller = new AbortController();

  const dispatch = (event: string, raw: string) => {
    let data: unknown;
    try {
      data = JSON.parse(raw);
    } catch {
      return;
    }
    if (event === "batch_start") handlers.onStart?.(data as BatchStartEvent);
    else if (event === "row_done") handlers.onRow?.(data as BatchRow);
    else if (event === "row_failed") handlers.onRowFailed?.(data as BatchFailedRow);
    else if (event === "batch_done") handlers.onDone?.(data as BatchDoneEvent);
  };

  (async () => {
    let finished = false;
    try {
      const res = await fetch(`${BASE_URL}/batch-stream?${params.toString()}`, {
        headers: { Accept: "text/event-stream" },
        signal: controller.signal,
      });
      if (!res.ok || !res.body) {
        let detail = `The batch could not start (HTTP ${res.status}).`;
        try {
          const body = (await res.json()) as { detail?: unknown };
          const d = body?.detail;
          if (typeof d === "string") detail = d;
          else if (d && typeof d === "object" && "message" in d)
            detail = String((d as { message: unknown }).message);
        } catch {
          /* keep the status line */
        }
        if (res.status === 429) detail = `${detail} Batches are limited to a few a minute — try again shortly.`;
        handlers.onError?.(detail);
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let event = "";
      let dataLines: string[] = [];
      const flush = () => {
        if (event && dataLines.length) dispatch(event, dataLines.join("\n"));
        event = "";
        dataLines = [];
      };
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let nl: number;
        while ((nl = buffer.indexOf("\n")) >= 0) {
          const line = buffer.slice(0, nl).replace(/\r$/, "");
          buffer = buffer.slice(nl + 1);
          if (line === "") {
            if (event === "batch_done") finished = true;
            flush();
          } else if (line.startsWith("event:")) event = line.slice(6).trim();
          else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
          // ":" comment lines (keep-alives) and other fields are ignored.
        }
      }
      flush();
      if (!finished) handlers.onError?.("The connection dropped before the batch finished.");
    } catch (err) {
      if ((err as { name?: string })?.name === "AbortError") return;
      handlers.onError?.("The batch could not start or the connection dropped.");
    }
  })();

  return () => controller.abort();
}

/**
 * Fetch the combined export for a screened list (Phase 167): one zip with
 * every row's BODS statements de-duplicated, `rows.csv`, a manifest and a
 * `LICENSES.md` over the union of sources. The backend re-runs the batch,
 * which inside the replay window — the case when the table has just been
 * shown — costs nothing upstream. Resolves to the blob and the filename
 * the server chose; rejects with the server's own words on 4xx/5xx.
 */
export async function fetchBatchExport(
  leis: string[],
  deepen_top = 5,
): Promise<{ blob: Blob; filename: string; failed: number }> {
  const params = new URLSearchParams({ leis: leis.join(","), deepen_top: String(deepen_top) });
  const res = await fetch(`${BASE_URL}/batch-export?${params.toString()}`);
  if (!res.ok) {
    let detail = `The export could not be built (HTTP ${res.status}).`;
    try {
      const body = (await res.json()) as { detail?: unknown };
      const d = body?.detail;
      if (typeof d === "string") detail = d;
      else if (d && typeof d === "object" && "message" in d)
        detail = String((d as { message: unknown }).message);
    } catch {
      /* keep the status line */
    }
    if (res.status === 429) detail = `${detail} Exports are limited to a few a minute — try again shortly.`;
    throw new Error(detail);
  }
  const disposition = res.headers.get("content-disposition") ?? "";
  const m = /filename="([^"]+)"/.exec(disposition);
  return {
    blob: await res.blob(),
    filename: m?.[1] ?? "opencheck-batch.zip",
    failed: Number(res.headers.get("x-opencheck-batch-failed") ?? "0") || 0,
  };
}

// ---------------------------------------------------------------------
// Per-source retry — /lookup-source
// ---------------------------------------------------------------------

export interface LookupSourceResponse {
  lei: string;
  source_id: string;
  hits: SourceHit[];
  error: string | null;
}

/**
 * Re-run a single source for an existing lookup (per-source retry button).
 * Also invalidates the backend's replay cache for the LEI.
 */
export async function retryLookupSource(
  lei: string,
  sourceId: string,
): Promise<LookupSourceResponse> {
  const params = new URLSearchParams({ lei, source_id: sourceId });
  const resp = await fetch(`${BASE_URL}/lookup-source?${params.toString()}`);
  if (!resp.ok) {
    const detail = await resp
      .json()
      .then((b) => b.detail as string)
      .catch(() => `HTTP ${resp.status}`);
    throw new Error(detail);
  }
  return (await resp.json()) as LookupSourceResponse;
}
