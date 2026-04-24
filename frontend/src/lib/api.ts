/**
 * Thin typed client for the OpenCheck backend.
 *
 * Phase 0 surface only: /health, /sources, /search.
 */

export type SearchKind = "entity" | "person";

export interface SourceInfo {
  id: string;
  name: string;
  homepage: string;
  license: string;
  attribution: string;
  supports: SearchKind[];
  requires_api_key: boolean;
  live_available: boolean;
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

export interface SearchResponse {
  query: string;
  kind: SearchKind;
  hits: SourceHit[];
  errors: Record<string, string>;
}

const BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ??
  "http://localhost:8000";

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

export function search(
  q: string,
  kind: SearchKind = "entity"
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q, kind });
  return getJson(`/search?${params.toString()}`);
}
