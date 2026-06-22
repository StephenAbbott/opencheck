import { useEffect, useMemo, useState } from "react";
import { getHistory } from "../../lib/api";
import type { HistoryEntry, HistoryRawChange, HistoryResponse } from "../../lib/api";

// ---------------------------------------------------------------------
// Source presentation — chips + links back to GLEIF / Companies House
// ---------------------------------------------------------------------

const SOURCE_LABEL: Record<string, string> = {
  gleif: "GLEIF",
  companies_house: "Companies House",
};

function sourceUrl(
  sourceId: string,
  lei: string,
  companyNumber: string | null,
): string | null {
  if (sourceId === "gleif") return `https://search.gleif.org/#/record/${lei}`;
  if (sourceId === "companies_house" && companyNumber)
    return `https://find-and-update.company-information.service.gov.uk/company/${companyNumber}/filing-history`;
  return null;
}

function SourceChip({
  sourceId,
  lei,
  companyNumber,
}: {
  sourceId: string;
  lei: string;
  companyNumber: string | null;
}) {
  const label = SOURCE_LABEL[sourceId] ?? sourceId;
  const url = sourceUrl(sourceId, lei, companyNumber);
  const classes =
    "inline-flex items-center gap-0.5 text-[10px] font-mono rounded px-1.5 py-0.5 border";
  const palette =
    sourceId === "gleif"
      ? "bg-blue-50 text-blue-700 border-blue-200"
      : "bg-teal-50 text-teal-700 border-teal-200";
  if (!url)
    return <span className={`${classes} ${palette}`}>{label}</span>;
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className={`${classes} ${palette} hover:underline`}
    >
      {label}
      <span aria-hidden>↗</span>
    </a>
  );
}

// ---------------------------------------------------------------------
// Date-basis honesty label
// ---------------------------------------------------------------------

function basisLabel(basis: string): string {
  if (basis === "effective") return "as filed";
  if (basis === "recorded") return "as recorded by GLEIF";
  if (basis === "snapshot_window") return "approximate";
  return "";
}

// ---------------------------------------------------------------------
// Tier accent — teal = ownership/control (T1), blue = identity/status (T2)
// ---------------------------------------------------------------------

function tierAccent(tier: number) {
  if (tier === 1)
    return { dot: "bg-teal-500", card: "border-teal-200 bg-teal-50/40" };
  return { dot: "bg-oo-blue", card: "border-blue-200 bg-blue-50/40" };
}

// ---------------------------------------------------------------------
// Notable entry row
// ---------------------------------------------------------------------

function NotableRow({
  entry,
  lei,
  companyNumber,
}: {
  entry: HistoryEntry;
  lei: string;
  companyNumber: string | null;
}) {
  const accent = tierAccent(entry.tier);
  const transition =
    entry.value_old || entry.value_new
      ? `${entry.value_old ?? "—"} → ${entry.value_new ?? "—"}`
      : null;
  // For ownership changes the economic interest start matters more than the
  // (possibly lagged) recorded date.
  const interest =
    entry.interest_start_date && entry.tier === 1
      ? `from ${entry.interest_start_date}${entry.interest_end_date ? ` to ${entry.interest_end_date}` : ""}`
      : null;

  return (
    <li className="relative pl-8 pb-5 last:pb-0">
      <span
        className={`absolute left-[3px] top-1.5 h-3 w-3 rounded-full ring-2 ring-white ${accent.dot}`}
        aria-hidden
      />
      <div className={`rounded-oo border px-3 py-2 ${accent.card}`}>
        <div className="flex items-start justify-between gap-2">
          <span className="text-[12px] font-mono text-oo-ink">
            {entry.date ?? "date unknown"}
            <span className="ml-1.5 text-[10px] text-oo-muted">
              {basisLabel(entry.date_basis)}
            </span>
          </span>
          <span className="flex flex-wrap items-center justify-end gap-1 shrink-0">
            {entry.sources.map((s) => (
              <SourceChip
                key={s}
                sourceId={s}
                lei={lei}
                companyNumber={companyNumber}
              />
            ))}
          </span>
        </div>
        <div className="mt-1 font-head font-bold text-[13px] text-oo-ink leading-snug">
          {entry.label}
          {entry.boosted && (
            <span className="ml-2 text-[10px] font-mono bg-amber-50 text-amber-800 border border-amber-200 rounded px-1 py-0.5">
              flagged
            </span>
          )}
        </div>
        {transition && (
          <div className="mt-0.5 text-[12px] text-oo-ink break-words">
            {transition}
          </div>
        )}
        {entry.counterparty && (
          <div className="mt-0.5 text-[11px] font-mono text-oo-muted break-words">
            {entry.counterparty}
            {interest && <span className="ml-1">· {interest}</span>}
          </div>
        )}
        {!entry.counterparty && interest && (
          <div className="mt-0.5 text-[11px] font-mono text-oo-muted">{interest}</div>
        )}
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------
// Administrative-noise row (muted, on the same rail)
// ---------------------------------------------------------------------

function fieldLabel(raw: HistoryRawChange): string {
  const f = (raw.raw_field ?? "").split("/").pop() ?? raw.raw_change_type;
  return f.replace(/^(lei:|rr:)/, "");
}

function NoiseRow({ raw }: { raw: HistoryRawChange }) {
  const transition =
    raw.value_old || raw.value_new
      ? `${raw.value_old ?? "—"} → ${raw.value_new ?? "—"}`
      : raw.raw_change_type;
  return (
    <li className="relative pl-8 pb-3 last:pb-0">
      <span
        className="absolute left-[5px] top-1.5 h-2 w-2 rounded-full bg-oo-rule ring-2 ring-white"
        aria-hidden
      />
      <div className="text-[11px] text-oo-muted">
        <span className="font-mono">{raw.event_date ?? "—"}</span>
        <span className="mx-1.5">·</span>
        <span className="font-mono">{fieldLabel(raw)}</span>
        <span className="mx-1.5">·</span>
        <span className="font-mono break-words">{transition}</span>
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------
// HistoryTimeline — vertical rail of notable changes (Time Machine)
// ---------------------------------------------------------------------

type Row =
  | { kind: "notable"; date: string; entry: HistoryEntry }
  | { kind: "noise"; date: string; raw: HistoryRawChange };

export function HistoryTimeline({
  lei,
  entityName,
}: {
  lei: string;
  entityName?: string;
}) {
  const [data, setData] = useState<HistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showNoise, setShowNoise] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    getHistory(lei, true)
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [lei]);

  const noiseEvents = useMemo(
    () => (data?.events ?? []).filter((e) => e.tier === 3),
    [data],
  );

  const rows = useMemo<Row[]>(() => {
    if (!data) return [];
    const out: Row[] = data.notable.map((entry) => ({
      kind: "notable",
      date: entry.date ?? "9999-12-31",
      entry,
    }));
    if (showNoise) {
      for (const raw of noiseEvents)
        out.push({ kind: "noise", date: raw.event_date ?? "9999-12-31", raw });
    }
    out.sort((a, b) => a.date.localeCompare(b.date));
    return out;
  }, [data, showNoise, noiseEvents]);

  return (
    <section className="mt-3 bg-white border border-oo-rule rounded-oo overflow-hidden">
      <header className="px-5 py-3 border-b border-oo-rule flex items-baseline justify-between gap-3">
        <h3 className="font-head font-bold text-[14px] text-oo-ink">
          Timeline{entityName ? ` — ${entityName}` : ""}
        </h3>
        {data && (
          <span className="text-[11px] font-mono text-oo-muted shrink-0">
            {data.sources.map((s) => SOURCE_LABEL[s] ?? s).join(" · ") || "no sources"}
          </span>
        )}
      </header>

      <div className="px-5 py-4">
        {loading && <p className="text-[13px] text-oo-muted">Loading change history…</p>}
        {error && <p className="text-[13px] text-red-700">{error}</p>}

        {!loading && !error && data && data.notable.length === 0 && (
          <p className="text-[13px] text-oo-muted leading-[1.6]">
            No notable ownership or identity changes found in the available
            history{data.available ? "" : " (live history not available)"}.
          </p>
        )}

        {!loading && !error && data && data.notable.length > 0 && (
          <>
            <ol className="relative">
              <span
                className="absolute left-[8px] top-1 bottom-1 w-px bg-oo-rule"
                aria-hidden
              />
              {rows.map((row, i) =>
                row.kind === "notable" ? (
                  <NotableRow
                    key={`n-${i}`}
                    entry={row.entry}
                    lei={lei}
                    companyNumber={data.company_number}
                  />
                ) : (
                  <NoiseRow key={`x-${i}`} raw={row.raw} />
                ),
              )}
            </ol>

            {noiseEvents.length > 0 && (
              <button
                type="button"
                onClick={() => setShowNoise((v) => !v)}
                aria-pressed={showNoise}
                className="mt-3 text-[11px] font-mono text-oo-muted hover:text-oo-ink hover:underline"
              >
                {showNoise
                  ? "Hide administrative changes"
                  : `Show administrative changes (${noiseEvents.length})`}
              </button>
            )}
          </>
        )}
      </div>
    </section>
  );
}
