/**
 * `/batch` — screen a list of companies (Phase 166).
 *
 * Everything in OpenCheck was one LEI at a time; the people who most need
 * it arrive with a list. This page is the front of the Phase 164 loop: a
 * paste box, one request, one table, one file. It is its own view rather
 * than a fifth search tab — the mode strip already carries four and became
 * a 2×2 grid on phones in Phase 157; four is the ceiling.
 *
 * What the page promises, and where each promise is pinned:
 *
 * - **What a paste means** — separators, check digits, duplicates, the cap —
 *   is decided in `lib/batch.ts` (`parseLeiPaste`) before a request is
 *   spent, and said under the box as it is typed. Rejected tokens are
 *   listed with their reason; nothing is silently dropped.
 * - **What it costs** — `costLine` — is said before the button, in minutes.
 * - **Three row states.** *Running* rows hold their place in paste order;
 *   *done* rows carry the register status, the verdict sentence, the risk
 *   and context counts and the Phase 156 coverage figures; *degraded*
 *   rows — a check that did not fully run, or a company that could not be
 *   screened at all — sort to the top and are never rendered as clean.
 *   The sort is a sort, not a grade (Phase 132).
 * - **The CSV** (`rowsToCsv`) has every screened LEI, failed ones with
 *   their reason, so the file never reads as a list of the ones that worked.
 *
 * Phones get a card list, not a squeezed table (the Phase 157 principle).
 */

import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";

import {
  streamBatch,
  type BatchDoneEvent,
  type BatchFailedRow,
  type BatchRow,
  type BatchStartEvent,
} from "../lib/api";
import {
  BATCH_CAP,
  costLine,
  csvFilename,
  parseLeiPaste,
  pasteSummary,
  rowCoverage,
  rowState,
  rowsToCsv,
  sortRows,
  type TableRow,
} from "../lib/batch";
import { statusChip } from "../lib/subjectProfile";
import { Button, Chip, SectionLabel, sectionLabelClasses } from "./ui";
import { RISK_PRESENTATION } from "./risk/RiskChip";

const PLACEHOLDER = "213800LH1BZH3DI6G760\n529900RWC8ZYB066JF16\n335800TYLGG93MM7PR89";

type Phase = "idle" | "running" | "done" | "error";

export default function BatchPage({
  registryTotal,
  sourceNames,
  onOpen,
}: {
  /** Registry size from `/sources`, or null until loaded — for the coverage cell. */
  registryTotal: number | null;
  /** id → display name, for the status chip's source. */
  sourceNames?: Record<string, string>;
  /** Open one company's report in the app rather than by full page load. */
  onOpen?: (lei: string) => void;
}) {
  const [text, setText] = useState("");
  const [phase, setPhase] = useState<Phase>("idle");
  const [rows, setRows] = useState<TableRow[]>([]);
  const [start, setStart] = useState<BatchStartEvent | null>(null);
  const [summary, setSummary] = useState<BatchDoneEvent | null>(null);
  const [error, setError] = useState<string | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);
  const textareaId = useId();
  const statusId = useId();

  const parsed = useMemo(() => parseLeiPaste(text), [text]);
  const canRun = parsed.leis.length > 0 && phase !== "running";

  useEffect(() => () => cleanupRef.current?.(), []);

  const run = useCallback(() => {
    if (!canRun) return;
    cleanupRef.current?.();
    const leis = parsed.leis;
    setRows(leis.map((lei) => ({ state: "running", lei })));
    setStart(null);
    setSummary(null);
    setError(null);
    setPhase("running");
    cleanupRef.current = streamBatch(leis, {
      onStart: (e) => setStart(e),
      onRow: (row: BatchRow) =>
        setRows((prev) =>
          prev.map((r) => (r.lei === row.lei ? { state: rowState(row), lei: row.lei, row } : r)),
        ),
      onRowFailed: (failed: BatchFailedRow) =>
        setRows((prev) =>
          prev.map((r) => (r.lei === failed.lei ? { state: "degraded", lei: failed.lei, failed } : r)),
        ),
      onDone: (e) => {
        setSummary(e);
        setPhase("done");
      },
      onError: (detail) => {
        setError(detail);
        setPhase((p) => (p === "running" ? "error" : p));
      },
    });
  }, [canRun, parsed.leis]);

  const sorted = useMemo(() => sortRows(rows), [rows]);
  const finished = rows.filter((r) => r.state !== "running").length;
  const degradedCount = rows.filter((r) => r.state === "degraded").length;

  const download = useCallback(() => {
    const csv = rowsToCsv(rows, window.location.origin);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = csvFilename();
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }, [rows]);

  return (
    <div className="max-w-oo-page mx-auto">
      <section className="mb-6 bg-white border border-oo-rule rounded-oo p-7">
        <h1 className={sectionLabelClasses("muted")}>Screen a list</h1>
        <p className="mt-2 max-w-2xl text-oo-small text-oo-muted">
          Paste up to {BATCH_CAP} Legal Entity Identifiers — one per line, or separated by
          commas or spaces, straight from a spreadsheet column. Each company runs through the
          same check as a single lookup, and every row links to its full report.
        </p>

        <label htmlFor={textareaId} className="sr-only">
          LEIs to screen
        </label>
        <textarea
          id={textareaId}
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder={PLACEHOLDER}
          rows={6}
          spellCheck={false}
          autoCapitalize="characters"
          aria-describedby={statusId}
          className="mt-4 w-full rounded-oo border border-oo-rule bg-oo-bg px-3 py-2 font-mono text-oo-small text-oo-ink placeholder:text-oo-muted/60 focus:outline-none focus:ring-2 focus:ring-oo-blue"
        />

        <div id={statusId} className="mt-2 text-oo-meta text-oo-muted" aria-live="polite">
          {text.trim() ? pasteSummary(parsed) : `Up to ${BATCH_CAP} at a time.`}
          {parsed.leis.length > 0 && <span className="block mt-1">{costLine(parsed.leis.length)}</span>}
        </div>

        {parsed.rejected.length > 0 && (
          <ul className="mt-3 space-y-1 text-oo-meta" aria-label="Values that will not be screened">
            {parsed.rejected.map((r, i) => (
              <li key={`${r.token}-${i}`} className="flex flex-wrap items-baseline gap-x-2">
                <code className="font-mono text-oo-ink break-all">{r.token}</code>
                <span className="text-oo-warn-text">{r.reason}</span>
              </li>
            ))}
          </ul>
        )}

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <Button variant="primary" onClick={run} disabled={!canRun}>
            {phase === "running" ? "Screening…" : "Screen these companies"}
          </Button>
          {rows.length > 0 && (
            <Button variant="secondary" onClick={download} disabled={phase === "running"}>
              Download table (CSV)
            </Button>
          )}
        </div>
      </section>

      {error && (
        <div
          role="alert"
          className="mb-6 rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3 text-oo-small text-oo-warn-text"
        >
          {error}
        </div>
      )}

      {rows.length > 0 && (
        <section className="bg-white border border-oo-rule rounded-oo p-7">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <SectionLabel as="h2">Results</SectionLabel>
            <p className="text-oo-meta text-oo-muted" role="status">
              {phase === "running"
                ? `${finished} of ${rows.length} screened · ${start?.concurrency ?? 2} at a time`
                : summary
                  ? `${summary.done} screened${summary.failed ? `, ${summary.failed} could not be checked` : ""}${
                      degradedCount ? ` · ${degradedCount} ${degradedCount === 1 ? "row needs" : "rows need"} a closer look` : ""
                    }`
                  : null}
            </p>
          </div>
          {start && start.overflow > 0 && (
            <p className="mt-1 text-oo-meta text-oo-warn-text">
              {start.overflow} more valid {start.overflow === 1 ? "LEI was" : "LEIs were"} beyond the
              cap of {start.cap} and not screened — run them as a second list.
            </p>
          )}
          {degradedCount > 0 && (
            <p className="mt-1 text-oo-meta text-oo-muted">
              Rows marked <span className="font-medium text-oo-warn-text">not fully checked</span> are
              listed first: a screening check did not run for them, and the absence of a finding there
              is not a clean result.
            </p>
          )}

          {/* Desktop: a table. */}
          <div className="mt-4 hidden md:block overflow-x-auto">
            <table className="w-full border-collapse text-oo-small">
              <caption className="sr-only">Screening results, one company per row</caption>
              <thead>
                <tr className="text-left text-oo-meta font-semibold uppercase tracking-oo-eyebrow text-oo-muted">
                  <th scope="col" className="py-2 pr-3">Company</th>
                  <th scope="col" className="py-2 pr-3">Status</th>
                  <th scope="col" className="py-2 pr-3">What the check found</th>
                  <th scope="col" className="py-2 pr-3">Signals</th>
                  <th scope="col" className="py-2 pr-3">Coverage</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((r) => (
                  <ResultRow key={r.lei} r={r} registryTotal={registryTotal} sourceNames={sourceNames} onOpen={onOpen} />
                ))}
              </tbody>
            </table>
          </div>

          {/* Phones: cards. */}
          <ul className="mt-4 md:hidden space-y-3" aria-label="Screening results">
            {sorted.map((r) => (
              <li key={r.lei} className="rounded-oo border border-oo-rule p-3">
                <ResultCard r={r} registryTotal={registryTotal} sourceNames={sourceNames} onOpen={onOpen} />
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------

function CompanyLink({
  lei,
  name,
  href,
  onOpen,
}: {
  lei: string;
  name: string | null;
  href: string;
  onOpen?: (lei: string) => void;
}) {
  return (
    <a
      href={href}
      onClick={(e) => {
        if (!onOpen) return;
        e.preventDefault();
        onOpen(lei);
      }}
      className="font-head font-bold text-oo-ink underline-offset-2 hover:underline hover:text-oo-blue"
    >
      {name || lei}
    </a>
  );
}

function StateChip({ r }: { r: TableRow }) {
  if (r.state === "running")
    return (
      <Chip tone="neutral" size="sm">
        <span aria-hidden="true">Screening…</span>
        <span className="sr-only">Still screening</span>
      </Chip>
    );
  if ("failed" in r)
    return (
      <Chip tone="warn" size="sm" title={r.failed.reason}>
        Not checked
      </Chip>
    );
  if (r.state === "degraded")
    return (
      <Chip tone="warn" size="sm" title={`Did not fully run: ${r.row.degraded_sources.join(", ")}`}>
        Not fully checked
      </Chip>
    );
  return null;
}

function RegisterStatus({
  row,
  sourceNames,
}: {
  row: BatchRow;
  sourceNames?: Record<string, string>;
}) {
  const chip = statusChip(
    row.register_status
      ? {
          register_status: {
            liveness: row.register_status.liveness,
            since: row.register_status.since ?? null,
            raw: row.register_status.raw ?? null,
            source_id: row.register_status.source_id ?? "",
            sources: [],
            independent_sources: 1,
            other_values: [],
          },
          legal_form: null,
          founding_date: null,
          registered_address: null,
          jurisdiction: row.jurisdiction,
          statement_ids: [],
        }
      : null,
    sourceNames,
  );
  if (!chip) return <span className="text-oo-muted">—</span>;
  const classes =
    chip.tone === "terminal"
      ? "inline-flex items-center rounded-full border font-body px-2.5 py-0.5 text-oo-meta bg-oo-navy border-oo-navy text-white font-medium"
      : undefined;
  return classes ? (
    <span className={classes}>
      <span aria-hidden="true">{chip.label}</span>
      <span className="sr-only">{chip.detail}</span>
    </span>
  ) : (
    <Chip tone={chip.tone === "warn" ? "warn" : "neutral"} size="sm">
      <span aria-hidden="true">{chip.label}</span>
      <span className="sr-only">{chip.detail}</span>
    </Chip>
  );
}

/** Risk findings as chips by name (the Phase 160 rule: a chip's name is its
 *  label), the structural count as one neutral chip after them. */
function SignalChips({ row }: { row: BatchRow }) {
  if (row.risk_count === 0 && row.context_count === 0)
    return <span className="text-oo-muted">none</span>;
  const shown = row.risk_codes.slice(0, 3);
  const more = row.risk_codes.length - shown.length;
  return (
    <span className="flex flex-wrap gap-1">
      {shown.map((code) => (
        <Chip key={code} tone="risk" size="sm">
          {RISK_PRESENTATION[code]?.label ?? code}
        </Chip>
      ))}
      {more > 0 && (
        <Chip tone="risk" size="sm">
          +{more} more
        </Chip>
      )}
      {row.context_count > 0 && (
        <Chip tone="context" size="sm" title={row.context_codes.join(", ")}>
          {row.context_count} structural
        </Chip>
      )}
    </span>
  );
}

function Coverage({ row, registryTotal }: { row: BatchRow; registryTotal: number | null }) {
  const c = rowCoverage(row, registryTotal);
  return (
    <span title={c.detail}>
      <span aria-hidden="true">{c.aside}</span>
      <span className="sr-only">{c.detail}</span>
    </span>
  );
}

function ResultRow({
  r,
  registryTotal,
  sourceNames,
  onOpen,
}: {
  r: TableRow;
  registryTotal: number | null;
  sourceNames?: Record<string, string>;
  onOpen?: (lei: string) => void;
}) {
  const href = "row" in r ? r.row.report_url : `/?lei=${r.lei}`;
  const name = "row" in r ? r.row.legal_name : null;
  const tint = r.state === "degraded" ? "bg-oo-warn-bg/40" : "";
  return (
    <tr className={`border-t border-oo-rule align-top ${tint}`}>
      <td className="py-2.5 pr-3">
        <CompanyLink lei={r.lei} name={name} href={href} onOpen={onOpen} />
        {name && (
          <div className="font-mono text-oo-meta text-oo-muted">
            {r.lei}
            {"row" in r && r.row.jurisdiction ? ` · ${r.row.jurisdiction}` : ""}
          </div>
        )}
      </td>
      <td className="py-2.5 pr-3">
        <span className="flex flex-wrap gap-1">
          <StateChip r={r} />
          {"row" in r && <RegisterStatus row={r.row} sourceNames={sourceNames} />}
        </span>
      </td>
      <td className="py-2.5 pr-3 text-oo-ink max-w-md">
        {"failed" in r ? (
          <span className="text-oo-warn-text">{r.failed.reason}</span>
        ) : "row" in r ? (
          r.row.verdict ?? <span className="text-oo-muted">—</span>
        ) : (
          <span className="text-oo-muted">—</span>
        )}
      </td>
      <td className="py-2.5 pr-3">{"row" in r ? <SignalChips row={r.row} /> : null}</td>
      <td className="py-2.5 pr-3 text-oo-muted whitespace-nowrap">
        {"row" in r ? <Coverage row={r.row} registryTotal={registryTotal} /> : null}
      </td>
    </tr>
  );
}

function ResultCard({
  r,
  registryTotal,
  sourceNames,
  onOpen,
}: {
  r: TableRow;
  registryTotal: number | null;
  sourceNames?: Record<string, string>;
  onOpen?: (lei: string) => void;
}) {
  const href = "row" in r ? r.row.report_url : `/?lei=${r.lei}`;
  const name = "row" in r ? r.row.legal_name : null;
  return (
    <div className="text-oo-small">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <CompanyLink lei={r.lei} name={name} href={href} onOpen={onOpen} />
        <StateChip r={r} />
      </div>
      {name && (
        <div className="font-mono text-oo-meta text-oo-muted">
          {r.lei}
          {"row" in r && r.row.jurisdiction ? ` · ${r.row.jurisdiction}` : ""}
        </div>
      )}
      {"failed" in r && <p className="mt-2 text-oo-warn-text">{r.failed.reason}</p>}
      {"row" in r && (
        <>
          <div className="mt-2 flex flex-wrap gap-1">
            <RegisterStatus row={r.row} sourceNames={sourceNames} />
            <SignalChips row={r.row} />
          </div>
          {r.row.verdict && <p className="mt-2 text-oo-ink">{r.row.verdict}</p>}
          <p className="mt-1 text-oo-meta text-oo-muted">
            <Coverage row={r.row} registryTotal={registryTotal} />
          </p>
        </>
      )}
    </div>
  );
}
