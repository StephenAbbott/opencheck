import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchSources, search, type SearchKind, type SearchResponse } from "./lib/api";

/**
 * Phase 0 chat stub.
 *
 * Single input → fans out to the backend /search endpoint → lists the
 * stub hits grouped by source. The richer chat UX (streaming, source
 * cards, "Go deeper", BODS graph) lands in Phase 1+.
 */
export default function App() {
  const [query, setQuery] = useState("");
  const [kind, setKind] = useState<SearchKind>("entity");
  const [submitted, setSubmitted] = useState<{ q: string; kind: SearchKind } | null>(null);

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchSources(),
  });

  const searchQuery = useQuery<SearchResponse>({
    queryKey: ["search", submitted?.q, submitted?.kind],
    queryFn: () => search(submitted!.q, submitted!.kind),
    enabled: !!submitted,
  });

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (query.trim()) {
      setSubmitted({ q: query.trim(), kind });
    }
  }

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-slate-200 bg-white px-6 py-4">
        <h1 className="text-xl font-semibold">OpenCheck</h1>
        <p className="text-sm text-slate-500">
          Chatbot-style corporate intelligence over open data · Phase 0 stub
        </p>
      </header>

      <main className="flex-1 px-6 py-8 max-w-4xl mx-auto w-full">
        <form onSubmit={handleSubmit} className="flex gap-2 mb-6">
          <select
            value={kind}
            onChange={(e) => setKind(e.target.value as SearchKind)}
            className="border border-slate-300 rounded px-3 py-2 bg-white"
          >
            <option value="entity">Entity</option>
            <option value="person">Person</option>
          </select>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search a company or person (e.g. Rosneft)…"
            className="flex-1 border border-slate-300 rounded px-3 py-2"
          />
          <button
            type="submit"
            className="bg-slate-900 text-white rounded px-4 py-2 hover:bg-slate-700"
          >
            Search
          </button>
        </form>

        {submitted && searchQuery.isLoading && (
          <p className="text-slate-500">Searching…</p>
        )}

        {searchQuery.isError && (
          <p className="text-red-600">Search failed: {String(searchQuery.error)}</p>
        )}

        {searchQuery.data && (
          <section className="space-y-3">
            <h2 className="text-lg font-medium">
              {searchQuery.data.hits.length} stub hit
              {searchQuery.data.hits.length === 1 ? "" : "s"} for
              <span className="font-mono"> "{searchQuery.data.query}"</span>
            </h2>
            <ul className="space-y-2">
              {searchQuery.data.hits.map((hit) => (
                <li
                  key={`${hit.source_id}:${hit.hit_id}`}
                  className="bg-white border border-slate-200 rounded p-4"
                >
                  <div className="flex justify-between items-baseline">
                    <h3 className="font-medium">{hit.name}</h3>
                    <span className="text-xs text-slate-500 font-mono">
                      {hit.source_id}
                    </span>
                  </div>
                  <p className="text-sm text-slate-600 mt-1">{hit.summary}</p>
                  {Object.keys(hit.identifiers).length > 0 && (
                    <p className="text-xs text-slate-500 mt-2 font-mono">
                      {Object.entries(hit.identifiers)
                        .map(([k, v]) => `${k}=${v}`)
                        .join(" · ")}
                    </p>
                  )}
                </li>
              ))}
            </ul>
          </section>
        )}

        <section className="mt-10">
          <h2 className="text-sm font-medium text-slate-500 uppercase tracking-wide mb-3">
            Sources
          </h2>
          {sourcesQuery.isLoading && <p className="text-slate-500">Loading…</p>}
          {sourcesQuery.data && (
            <ul className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {sourcesQuery.data.sources.map((s) => (
                <li
                  key={s.id}
                  className="bg-white border border-slate-200 rounded p-3 text-sm"
                >
                  <div className="flex justify-between">
                    <span className="font-medium">{s.name}</span>
                    <span className="text-xs text-slate-500">{s.license}</span>
                  </div>
                  <p className="text-xs text-slate-500 mt-1">{s.attribution}</p>
                  <p className="text-xs text-slate-400 mt-1">
                    Supports: {s.supports.join(", ")} ·{" "}
                    {s.live_available ? "live ready" : "stub"}
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      </main>

      <footer className="border-t border-slate-200 bg-white px-6 py-3 text-xs text-slate-500">
        OpenCheck · MIT code · third-party data licensed per source — see
        ATTRIBUTIONS.md
      </footer>
    </div>
  );
}
