import { useEffect, useId, useMemo, useState } from "react";
import { getSecurities, type Security, type SecuritiesResponse } from "../../lib/api";
import { NOT_IN_GRAPH, sourceList } from "../../lib/vocab";
import { ActionChip } from "../ui";
import PanelSection from "../ui/PanelSection";
import {
  panelError,
  securitiesOverlayUnavailable,
  securitiesPageUnavailable,
  type PanelError,
  type PanelId,
} from "../../lib/panelErrors";

// Entity-level "Securities" section: ISINs linked to the LEI, combining GLEIF
// (count + list), OpenFIGI (security type) and OpenSanctions (sanctioned subset).
// Design — sanctions-first: any sanctioned securities sit in a red banner that is
// always visible; the long tail (an issuer can have tens of thousands of ISINs)
// is a count behind a "Browse all" drawer with search + type filter.

function RegimeChip({ label }: { label: string }) {
  return (
    <span className="font-mono text-[10px] rounded px-1.5 py-0.5 bg-rose-100 text-rose-800 border border-rose-200">
      {label}
    </span>
  );
}

function SecRow({ s, danger }: { s: Security; danger?: boolean }) {
  // Regimes are company-level (identical across the entity's ISINs), so they're
  // shown once in the banner header — not per row. Rows stay narrow (ISIN +
  // type), which also keeps the box within the viewport on mobile.
  return (
    <li
      className={`flex items-center gap-2 px-2.5 py-1.5 rounded min-w-0 ${
        danger ? "bg-rose-50/60" : "border-b border-oo-rule/60 last:border-b-0"
      }`}
    >
      <span className="font-mono text-[12px] text-oo-ink shrink-0">{s.isin}</span>
      {danger && (
        <span className="text-[10px] rounded-full px-1.5 py-0.5 bg-rose-100 text-rose-800 shrink-0">
          sanctioned
        </span>
      )}
      <span className="text-[11px] text-oo-muted truncate min-w-0">
        {[s.type, s.exchange].filter(Boolean).join(" · ")}
        {s.name ? ` — ${s.name}` : ""}
      </span>
    </li>
  );
}

export function SecuritiesSection({
  lei,
  onError,
  onRecovered,
  sourceNames,
}: {
  lei: string;
  /** The lookup's source_id → display-name map. Without it `sourceLabel`
   *  prettifies the slug, which turns `openfigi` into "Openfigi" — a brand
   *  name OpenCheck has invented. Raw slugs were worse; an invented casing
   *  is still a claim about how someone spells their own name. */
  sourceNames?: Record<string, string>;
  /** Report a fetch failure upward. The section renders nothing on failure —
   *  which is the bug — so somebody outside it has to say so. */
  onError?: (e: PanelError) => void;
  /** Clear a previously reported failure once a fetch succeeds. */
  onRecovered?: (panel: PanelId) => void;
}) {
  const [meta, setMeta] = useState<SecuritiesResponse | null>(null);
  const [loaded, setLoaded] = useState<Security[]>([]);
  const [page, setPage] = useState(1);
  const [loadingMore, setLoadingMore] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [showAllSanctioned, setShowAllSanctioned] = useState(false);
  const [showAllRegimes, setShowAllRegimes] = useState(false);
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState<string>("All");
  const uid = useId();
  const regimesId = `${uid}-regimes`;
  const sanctionedListId = `${uid}-sanctioned`;
  const drawerId = `${uid}-drawer`;

  useEffect(() => {
    let cancelled = false;
    setMeta(null);
    setLoaded([]);
    setPage(1);
    setExpanded(false);
    setShowAllSanctioned(false);
    setShowAllRegimes(false);
    setQuery("");
    setTypeFilter("All");
    getSecurities(lei, 1)
      .then((r) => {
        if (cancelled) return;
        // Phase 145: GLEIF being unavailable no longer 500s — the response
        // says so (`isin_list_available: false`) with the sanctioned overlay
        // still applied. When the overlay did not run either (no index on
        // this deployment), the response holds nothing trustworthy, so
        // report it as the panel failure the 500 used to be.
        if (r.isin_list_available === false && !r.sources.includes("opensanctions")) {
          onError?.(securitiesOverlayUnavailable());
          return;
        }
        onRecovered?.("securities");
        setMeta(r);
        setLoaded(r.securities);
      })
      .catch((e) => {
        if (cancelled) return;
        // Phase 124: this used to swallow the rejection with no state write,
        // so the section returned null and a transport failure was
        // indistinguishable from "this entity has no securities" — including
        // for the sanctioned-securities banner this component's own docstring
        // calls always-visible.
        onError?.(panelError("securities", e));
      });
    return () => {
      cancelled = true;
    };
  }, [lei]);

  const types = useMemo(() => {
    const set = new Set<string>();
    for (const s of loaded) if (s.type) set.add(s.type);
    return ["All", ...Array.from(set).sort()];
  }, [loaded]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return loaded.filter((s) => {
      if (typeFilter !== "All" && s.type !== typeFilter) return false;
      if (!q) return true;
      return (
        s.isin.toLowerCase().includes(q) ||
        (s.name ?? "").toLowerCase().includes(q) ||
        (s.ticker ?? "").toLowerCase().includes(q)
      );
    });
  }, [loaded, query, typeFilter]);

  async function loadMore() {
    if (!meta || loadingMore) return;
    setLoadingMore(true);
    try {
      const next = page + 1;
      const r = await getSecurities(lei, next);
      if (r.isin_list_available === false) {
        // A degraded 200 mid-browse (GLEIF started rate-limiting between
        // pages): appending its empty page would read as a no-op click —
        // the exact Phase 124 bug. The banner above is still standing, so
        // the report names the pages as missing, not the whole check.
        onError?.(securitiesPageUnavailable());
        return;
      }
      setLoaded((prev) => [...prev, ...r.securities]);
      setPage(next);
    } catch (e) {
      // A failed page used to read as a no-op click: the button un-disabled
      // and the "Showing N of M" counter kept its old numerator.
      onError?.(panelError("securities", e));
    } finally {
      setLoadingMore(false);
    }
  }

  // Render nothing until we have a usable result (offline/curated demos return
  // available:false; entities with no securities and nothing sanctioned are hidden).
  if (!meta || !meta.available) return null;
  const sanctioned = meta.sanctioned;
  // Degraded (Phase 145): the ISIN count/page is missing but the sanctions
  // overlay ran — the section stays visible to say so, even with nothing
  // sanctioned, because hiding it would read as "checked and clean list".
  const isinListDown = meta.isin_list_available === false;
  if (!isinListDown && meta.total === 0 && sanctioned.length === 0) return null;

  const ncNotice = meta.license_notices.find((n) => n.source_id === "opensanctions");
  // Regimes are company-level — collect the union once for the banner header.
  const sanctionedRegimes = Array.from(
    new Set(sanctioned.flatMap((s) => s.regimes ?? [])),
  );

  return (
    <PanelSection title="Securities" aside={NOT_IN_GRAPH}>
      <div>
        {/* Sanctions-first banner */}
        {sanctioned.length > 0 && (
          <div className="mb-3 rounded-oo border border-rose-200 bg-rose-50 px-4 py-3">
            <div className="flex items-center gap-2 mb-2">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#be123c" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
                <line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" />
              </svg>
              <span className="font-bold text-[13px] text-rose-700">
                {sanctioned.length} sanctioned secur{sanctioned.length === 1 ? "ity" : "ities"}
              </span>
            </div>
            {sanctionedRegimes.length > 0 && (
              <div id={regimesId} className="flex flex-wrap items-center gap-1 mb-2">
                {(showAllRegimes ? sanctionedRegimes : sanctionedRegimes.slice(0, 4)).map((r) => (
                  <RegimeChip key={r} label={r} />
                ))}
                {sanctionedRegimes.length > 4 && (
                  <button
                    type="button"
                    onClick={() => setShowAllRegimes((v) => !v)}
                    aria-expanded={showAllRegimes}
                    aria-controls={regimesId}
                    className="font-mono text-[10px] rounded px-1.5 py-0.5 text-rose-700 hover:underline"
                  >
                    {showAllRegimes ? "show fewer" : `+${sanctionedRegimes.length - 4} more`}
                  </button>
                )}
              </div>
            )}
            <ul id={sanctionedListId} className="space-y-1">
              {(showAllSanctioned ? sanctioned : sanctioned.slice(0, 2)).map((s) => (
                <SecRow key={s.isin} s={s} danger />
              ))}
            </ul>
            {sanctioned.length > 2 && (
              <button
                type="button"
                onClick={() => setShowAllSanctioned((v) => !v)}
                aria-expanded={showAllSanctioned}
                aria-controls={sanctionedListId}
                className="mt-1.5 text-[11px] font-semibold text-rose-700 hover:underline"
              >
                {showAllSanctioned ? "Hide" : `Show all ${sanctioned.length}`}
              </button>
            )}
          </div>
        )}

        {/* Summary + browse toggle. The "not in the graph" caveat moved to the
            band's split-head, where it qualifies the section rather than
            trailing the count as a mono pill. When the ISIN list is down the
            count is unknown, and "0 securities mapped" would be a false claim
            — say which part is missing instead (the sanctions overlay above,
            or the sentence below when it found nothing, covers what did run). */}
        {isinListDown ? (
          <div className="rounded-oo border border-amber-200 bg-amber-50 px-3 py-2 text-oo-small text-amber-900">
            The GLEIF ISIN list could not be fetched (GLEIF is rate-limiting or
            unreachable), so how many securities are mapped to this LEI is
            unknown right now — try again in a minute.
            {sanctioned.length === 0 && (
              <>
                {" "}The sanctioned-securities check did run, from OpenCheck's
                local OpenSanctions index: it records no sanctioned securities
                for this entity.
              </>
            )}
          </div>
        ) : (
          <div className="flex items-center gap-2.5 flex-wrap">
            <span className="text-oo-small text-oo-ink">
              <span className="font-semibold">{meta.total.toLocaleString()}</span>{" "}
              secur{meta.total === 1 ? "ity" : "ities"} mapped to this LEI
            </span>
            {meta.total > 0 && (
              <ActionChip
                onClick={() => setExpanded((v) => !v)}
                expanded={expanded}
                controls={drawerId}
              >
                {expanded ? "Hide" : "Browse all"}
              </ActionChip>
            )}
          </div>
        )}
        <div className="text-oo-meta text-oo-muted mt-1.5">
          {sourceList(meta.sources, sourceNames)}
        </div>

        {/* Drawer */}
        {expanded && meta.total > 0 && (
          <div id={drawerId} className="mt-3 border-t border-oo-rule pt-3">
            <div className="flex items-center gap-2 mb-2 flex-wrap">
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search ISIN, name, ticker…"
                aria-label="Search securities by ISIN, name or ticker"
                className="flex-1 min-w-[160px] text-[12px] font-mono border border-oo-rule rounded px-2 py-1"
              />
              {types.map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setTypeFilter(t)}
                  aria-pressed={typeFilter === t}
                  className={`text-[11px] rounded-full px-2.5 py-0.5 border ${
                    typeFilter === t
                      ? "bg-[#e8e6fb] border-[#c9c2f4] text-oo-blue font-semibold"
                      : "border-oo-rule text-oo-muted hover:text-oo-ink"
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
            {filtered.length > 0 ? (
              <ul>
                {filtered.map((s) => (
                  <SecRow key={s.isin} s={s} danger={s.sanctioned} />
                ))}
              </ul>
            ) : (
              <p className="text-[12px] text-oo-muted py-2">No matching securities on the loaded pages.</p>
            )}
            <div className="mt-2 flex items-center gap-3 text-[11px] text-oo-muted font-mono">
              <span role="status">
                Showing {loaded.length.toLocaleString()} of {meta.total.toLocaleString()}
              </span>
              {loaded.length < meta.total && (
                <button
                  type="button"
                  onClick={loadMore}
                  disabled={loadingMore}
                  className="text-oo-blue hover:underline disabled:opacity-50"
                >
                  {loadingMore ? "Loading…" : "Load more"}
                </button>
              )}
            </div>
          </div>
        )}

        {ncNotice && (
          <p className="text-[10px] text-amber-800 mt-3 leading-[1.5]">
            Sanctioned-securities data: {ncNotice.notice}
          </p>
        )}
      </div>
    </PanelSection>
  );
}
