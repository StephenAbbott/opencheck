/**
 * HistoryTimeline — the vertical rail of dated changes (Phase 190).
 *
 * Presentational only. It is handed a fetched `HistoryResponse` and draws it;
 * the fetch, the coverage sentence and the bands belong to `HistoryPanel`, and
 * the ordering, labels, links and notices belong to `lib/historyMode.ts`.
 *
 * Before Phase 190 this component fetched for itself and was mounted by
 * `SourceBucketCard` behind a per-source "Changes over time" button — five
 * copies of one entity-wide timeline, one under each source that emits
 * history, none of them addressable. Splitting the fetch out is what lets the
 * tab ask once and say something true about the answer above the rail.
 */

import type { HistoryEntry, HistoryRawChange, HistoryResponse } from "../../lib/api";
import {
  basisLabel,
  historySourceLabel,
  recordUrl,
  type Row,
} from "../../lib/historyMode";

// ---------------------------------------------------------------------
// Source chip — names the register and links back to its record
// ---------------------------------------------------------------------

function SourceChip({
  sourceId,
  lei,
  registryNumbers,
}: {
  sourceId: string;
  lei: string;
  registryNumbers: Record<string, string>;
}) {
  const label = historySourceLabel(sourceId);
  const url = recordUrl(sourceId, lei, registryNumbers);
  const classes =
    "inline-flex items-center gap-0.5 text-oo-meta font-mono rounded px-1.5 py-0.5 border";
  const palette =
    sourceId === "gleif"
      ? "bg-blue-50 text-blue-700 border-blue-200"
      : sourceId === "nz_companies"
        ? "bg-emerald-50 text-emerald-700 border-emerald-200"
        : sourceId === "ariregister"
          ? "bg-violet-50 text-violet-700 border-violet-200"
          : sourceId === "cvr_denmark"
            ? "bg-rose-50 text-rose-700 border-rose-200"
            : "bg-teal-50 text-teal-700 border-teal-200";
  if (!url) return <span className={`${classes} ${palette}`}>{label}</span>;
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className={`${classes} ${palette} hover:underline`}
    >
      {label}
      <span aria-hidden>↗</span>
      <span className="sr-only"> — open the record in this register (opens in new tab)</span>
    </a>
  );
}

// ---------------------------------------------------------------------
// Tier accent — teal = ownership/control (T1), blue = identity/status (T2)
// ---------------------------------------------------------------------

function tierAccent(tier: number) {
  if (tier === 1) return { dot: "bg-teal-500", card: "border-teal-200 bg-teal-50/40" };
  return { dot: "bg-oo-blue", card: "border-blue-200 bg-blue-50/40" };
}

// ---------------------------------------------------------------------
// Notable entry row
// ---------------------------------------------------------------------

function NotableRow({
  entry,
  lei,
  registryNumbers,
}: {
  entry: HistoryEntry;
  lei: string;
  registryNumbers: Record<string, string>;
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
          <span className="text-oo-small font-mono text-oo-ink">
            {entry.date ?? "date unknown"}
            <span className="ml-1.5 text-oo-meta text-oo-muted">
              {basisLabel(entry.date_basis)}
            </span>
          </span>
          <span className="flex flex-wrap items-center gap-1 justify-end shrink-0">
            {entry.sources.map((s) => (
              <SourceChip key={s} sourceId={s} lei={lei} registryNumbers={registryNumbers} />
            ))}
          </span>
        </div>
        <p className="mt-1 font-head font-bold text-oo-small text-oo-ink">
          {entry.label}
          {entry.boosted && (
            <span className="ml-2 rounded bg-amber-100 px-1.5 py-0.5 text-oo-meta font-normal text-amber-800">
              flagged
            </span>
          )}
        </p>
        {transition && (
          <p className="mt-0.5 font-mono text-oo-meta text-oo-muted break-words">
            {transition}
          </p>
        )}
        {entry.counterparty && (
          <p className="mt-0.5 font-mono text-oo-meta text-oo-muted break-words">
            {entry.counterparty}
          </p>
        )}
        {interest && (
          <p className="mt-0.5 text-oo-meta text-oo-muted">{interest}</p>
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
      <div className="text-oo-meta text-oo-muted">
        <span className="font-mono">{raw.event_date ?? "—"}</span>
        <span className="mx-1.5">·</span>
        <span className="font-mono">{historySourceLabel(raw.source_id)}</span>
        <span className="mx-1.5">·</span>
        <span className="font-mono">{fieldLabel(raw)}</span>
        <span className="mx-1.5">·</span>
        <span className="font-mono break-words">{transition}</span>
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------
// Board-change row (Phase 194) — who joined, who left, and who it was
//
// Between the notable rows and the administrative ones in weight, because
// that is where it sits in meaning: a board change is a real change to the
// company that is not a change in who owns it. The dot is `oo.graph.role`,
// the colour the network already draws a `seniorManagingOfficial` edge in,
// so the same relationship wears the same colour on both surfaces. The name
// is the row's subject and is set in the ink colour; where the register
// published none, the row says so rather than showing an empty space.
// ---------------------------------------------------------------------

function BoardRow({ raw }: { raw: HistoryRawChange }) {
  return (
    <li className="relative pl-8 pb-3 last:pb-0">
      <span
        className="absolute left-[4px] top-1.5 h-2.5 w-2.5 rounded-full bg-oo-graph-role ring-2 ring-white"
        aria-hidden
      />
      <div className="text-oo-meta">
        <span className="font-mono text-oo-muted">{raw.event_date ?? "—"}</span>
        <span className="mx-1.5 text-oo-muted">·</span>
        <span className="font-mono text-oo-muted">{historySourceLabel(raw.source_id)}</span>
        <span className="mx-1.5 text-oo-muted">·</span>
        <span className="text-oo-ink font-semibold">
          {raw.label ?? raw.raw_change_type}
        </span>
        <span className="mx-1.5 text-oo-muted">·</span>
        {raw.counterparty ? (
          <span className="text-oo-ink break-words">{raw.counterparty}</span>
        ) : (
          <span className="text-oo-muted italic">no name on the filing</span>
        )}
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------
// The rail
// ---------------------------------------------------------------------

export function HistoryTimeline({
  rows,
  lei,
  data,
}: {
  rows: Row[];
  lei: string;
  data: HistoryResponse;
}) {
  return (
    <ol className="relative" data-testid="history-rail">
      <span className="absolute left-[8px] top-1 bottom-1 w-px bg-oo-rule" aria-hidden />
      {rows.map((row, i) =>
        row.kind === "notable" ? (
          <NotableRow
            key={`n-${i}`}
            entry={row.entry}
            lei={lei}
            registryNumbers={data.registry_numbers ?? {}}
          />
        ) : row.kind === "board" ? (
          <BoardRow key={`b-${i}`} raw={row.raw} />
        ) : (
          <NoiseRow key={`x-${i}`} raw={row.raw} />
        ),
      )}
    </ol>
  );
}

export default HistoryTimeline;
