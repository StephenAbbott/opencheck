/**
 * Thin typed client for the OpenCheck backend.
 *
 * Phase 1 surface: /health, /sources, /search, /stream (SSE), /deepen.
 * Phase 2 surface: /lookup-stream (SSE, LEI-anchored progressive lookup).
 */

export type SearchKind = "entity" | "person";

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
// service URL (e.g. https://opencheck-api.onrender.com).
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
  format: "json" | "jsonl" | "zip" | "xml"
): string {
  const params = new URLSearchParams({ lei, format });
  return `${BASE_URL}/export?${params.toString()}`;
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

/**
 * Drive the LEI-anchored lookup: GLEIF → cross-source bridges →
 * unified subject view. Throws an Error with the backend's detail
 * message when the LEI is malformed (400) or unknown to GLEIF (404).
 */
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

export type LookupStreamHandlers = {
  onGleifDone?: (e: LookupGleifDoneEvent) => void;
  onSourcesApplicable?: (e: LookupSourcesApplicableEvent) => void;
  onSourceStarted?: (e: SourceStartedEvent) => void;
  onHit?: (e: SourceHit) => void;
  onSourceCompleted?: (e: SourceCompletedEvent) => void;
  onSourceError?: (e: SourceErrorEvent) => void;
  onCrossSourceLinks?: (e: CrossSourceLinksEvent) => void;
  onRiskSignals?: (e: RiskSignalsEvent) => void;
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
): () => void {
  const params = new URLSearchParams({ lei, deepen_top: String(deepen_top) });
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
  es.addEventListener("risk_signals", (ev) => {
    const data = safeParse<RiskSignalsEvent>((ev as MessageEvent).data);
    if (data) handlers.onRiskSignals?.(data);
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
