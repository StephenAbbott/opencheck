/**
 * Thin typed client for the OpenCheck backend.
 *
 * Phase 1 surface: /health, /sources, /search, /stream (SSE), /deepen.
 * Phase 2 surface: /lookup-stream (SSE, LEI-anchored progressive lookup).
 */

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
  summary: string;
  identifiers: Record<string, string>;
  raw: Record<string, unknown>;
  is_stub: boolean;
}

export interface CrossSourceLink {
  key: string;
  key_value: string;
  confidence: "strong" | "possible";
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
  bods: Record<string, unknown>[];
  bods_issues: string[];
  license_notices: { source_id: string; hit_id: string; notice: string }[];
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
export function exportUrl(
  lei: string,
  format: "json" | "jsonl" | "zip" | "xml" | "senzing" | "ftm",
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
  meta: { prompt_version?: string; model?: string } = {},
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
  const r = await fetch(`${BASE_URL}/export/pdf`, {
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
  const filename = /filename="?([^"]+)"?/.exec(cd)?.[1] ?? `opencheck-${lei}.pdf`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
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

/** Entity / relationship split for a single deepened hit's BODS graph. */
export interface BodsBreakdown {
  entities: number;
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
