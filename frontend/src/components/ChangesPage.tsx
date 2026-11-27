/**
 * `/changes` — what changed at GLEIF or OpenSanctions on one UTC day (the
 * "Daily deltas for OpenCheck" ticket, step C; the mock is on the ticket).
 *
 * The page lists the larger changes and offers the whole day as files. It is
 * a reading of the two publishers' deltas, graded by OpenCheck, and nothing
 * more: no check has been run on any company on it, and the page says so at
 * the top. "Run a check" is the reader's choice.
 *
 * Ownership and sanctions lead and are open; the ceased, the ceased on an
 * already-lapsed LEI and the record changes are folded; new LEIs and
 * note-only changes are counted, not listed — they are in the day's file.
 * An OpenSanctions row is a reference only (entity id, topic codes and a
 * link): OpenSanctions data is CC BY-NC 4.0.
 *
 * The day is in the address (`?day=`), like a report's `?lei=`; without one
 * the page shows the newest complete day.
 */

import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { fetchDailyChangesDay, fetchDailyChangesIndex } from "../lib/api";
import {
  BAND_LABELS,
  BAND_SENTENCES,
  bandCount,
  changeLines,
  chooseDay,
  countedNotListed,
  dayFromSearch,
  fileUrls,
  formatBytes,
  formatCount,
  longDay,
  neighbours,
  reasonBreakdown,
  recordLink,
  rowChip,
  rowsIn,
  sourceLine,
  STRIP,
  type DailyDay,
  type DailyRow,
} from "../lib/dailyChanges";
import { Button, buttonClasses, Chip, SectionLabel } from "./ui";

/** How many headline rows show before "Show the other N". */
const HEADLINE_SHOWN = 5;
/** How many rows a folded section opens at a time. */
const PAGE = 20;

function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

function ChangeRow({ row, onOpen }: { row: DailyRow; onOpen: (lei: string) => void }) {
  const chip = rowChip(row);
  const lines = changeLines(row);
  const link = recordLink(row);
  return (
    <li className="py-4 border-t border-oo-rule first:border-t-0">
      <div className="flex flex-wrap items-center gap-2 mb-1">
        <Chip tone={chip.tone}>{chip.label}</Chip>
        <span className="text-oo-meta text-oo-muted font-mono">{sourceLine(row)}</span>
      </div>
      <p className="font-head font-bold text-oo-lead text-oo-ink mb-1">{row.name ?? row.lei}</p>
      {lines.length > 0 && (
        <dl className="text-oo-small text-oo-ink mb-2 space-y-1">
          {lines.map((line, i) => (
            <div key={i}>
              <dt className="font-semibold">{line.what}</dt>
              {line.before !== undefined && (
                <dd className="m-0 text-oo-muted break-words">
                  Before <span className="font-mono">{line.before}</span> · Now{" "}
                  <span className="font-mono text-oo-ink">{line.now}</span>
                </dd>
              )}
            </div>
          ))}
        </dl>
      )}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <span className="text-oo-meta text-oo-muted font-mono">
          {row.jurisdiction ? `${row.jurisdiction} · ` : ""}LEI {row.lei}
        </span>
        <Button size="sm" variant="secondary" onClick={() => onOpen(row.lei)}>
          Run a check
        </Button>
        <a
          href={link.href}
          target="_blank"
          rel="noreferrer"
          className="text-oo-small text-oo-blue underline hover:text-oo-burst"
        >
          {link.label}
          <span className="sr-only"> (opens in new tab)</span>
        </a>
      </div>
    </li>
  );
}

function RowsShown({
  rows,
  first,
  step,
  onOpen,
}: {
  rows: DailyRow[];
  first: number;
  /** Rows each press adds; `null` shows every remaining row at once. */
  step: number | null;
  onOpen: (lei: string) => void;
}) {
  const [shown, setShown] = useState(first);
  useEffect(() => setShown(first), [rows, first]);
  const rest = rows.length - shown;
  return (
    <>
      <ul className="list-none p-0 m-0">
        {rows.slice(0, shown).map((row) => (
          <ChangeRow key={`${row.lei}-${row.source}-${row.as_of}`} row={row} onOpen={onOpen} />
        ))}
      </ul>
      {rest > 0 && (
        <Button
          size="sm"
          variant="ghost"
          className="mt-2"
          onClick={() => setShown((n) => n + (step ?? rest))}
        >
          {step === null || rest <= step
            ? `Show the other ${formatCount(rest)}`
            : `Show ${formatCount(step)} more`}
        </Button>
      )}
    </>
  );
}

function Breakdown({ rows }: { rows: DailyRow[] }) {
  const parts = reasonBreakdown(rows);
  if (!parts.length) return null;
  return (
    <p className="text-oo-meta text-oo-muted mb-2">
      {parts.map((p) => `${p.label} ${formatCount(p.n)}`).join(" · ")}
    </p>
  );
}

function FoldedBand({
  band,
  day,
  onOpen,
}: {
  band: "ceased" | "ceased_lapsed" | "record";
  day: DailyDay;
  onOpen: (lei: string) => void;
}) {
  const rows = rowsIn(day, band);
  const n = bandCount(day, band);
  if (!n) return null;
  return (
    <details className="bg-white border border-oo-rule rounded-oo px-5 py-3 mb-3">
      <summary className="cursor-pointer font-head font-bold text-oo-lead text-oo-ink">
        {BAND_LABELS[band]} · {formatCount(n)}
      </summary>
      <p className="text-oo-small text-oo-muted mt-2 mb-1">{BAND_SENTENCES[band]}</p>
      <Breakdown rows={rows} />
      <RowsShown rows={rows} first={PAGE} step={PAGE} onOpen={onOpen} />
    </details>
  );
}

function DownloadAside({ day, file }: { day: string; file?: { bytes: number | null; statements: number | null } }) {
  const urls = fileUrls(day, typeof window === "undefined" ? "" : window.location.origin);
  const size = formatBytes(file?.bytes);
  return (
    <aside aria-labelledby="changes-download" className="min-w-0 bg-white border border-oo-rule rounded-oo p-5 space-y-4">
      <div>
        <h3 id="changes-download" className="font-head font-bold text-oo-lead text-oo-ink mb-1">
          Download this day
        </h3>
        <p className="text-oo-small text-oo-muted">One file per day, kept for thirty days.</p>
      </div>
      <div className="flex flex-col gap-2">
        {file ? (
          <a href={urls.bods} className={buttonClasses("primary", "md")} download>
            BODS v0.4, JSON Lines
          </a>
        ) : (
          <p className="text-oo-small text-oo-muted">The BODS file for this day has not been written.</p>
        )}
        {file && (size || file.statements != null) && (
          <p className="text-oo-meta text-oo-muted font-mono">
            {[size, file.statements != null ? `${formatCount(file.statements)} statements` : null]
              .filter(Boolean)
              .join(" · ")}
          </p>
        )}
        <a href={urls.csv} className={buttonClasses("secondary", "sm")} download>
          List as CSV
        </a>
        <a href={urls.json} className={buttonClasses("secondary", "sm")}>
          List as JSON
        </a>
      </div>
      <div>
        <SectionLabel as="h4" className="mb-1">Fetch it every day</SectionLabel>
        <p className="text-oo-small text-oo-muted mb-2">
          The address follows the date. The file for a day is final once that day has ended in UTC.
        </p>
        <pre className="bg-oo-bg border border-oo-rule rounded-oo p-2 text-oo-meta overflow-x-auto">
          <code>curl -O {urls.bods}</code>
        </pre>
        <p className="text-oo-small text-oo-muted my-2">For the newest day, without working out the date:</p>
        <pre className="bg-oo-bg border border-oo-rule rounded-oo p-2 text-oo-meta overflow-x-auto">
          <code>curl -OJL {urls.latest}</code>
        </pre>
      </div>
      <div>
        <SectionLabel as="h4" className="mb-1">Use it</SectionLabel>
        <ol className="list-decimal pl-5 text-oo-small text-oo-ink space-y-1">
          <li>
            Each line is one BODS v0.4 statement: the company as GLEIF records it after the day's
            deltas, and its parent relationships.
          </li>
          <li>
            A statement's <code className="font-mono">recordId</code> stays the same across days.
            Replace what you hold under that id with the new statement.
          </li>
          <li>
            A parent relationship that GLEIF removed arrives as a closed record. Keep it. It is
            history, not a deletion.
          </li>
        </ol>
      </div>
      <div>
        <SectionLabel as="h4" className="mb-1">Licences</SectionLabel>
        <p className="text-oo-small text-oo-muted">
          GLEIF data is CC0 and free to reuse. OpenSanctions rows carry only a record id, topic
          codes and a link, because OpenSanctions data is CC BY-NC 4.0. Follow the link for the
          record itself. OpenSanctions is not in the BODS file.
        </p>
      </div>
    </aside>
  );
}

export default function ChangesPage({
  onOpen,
  onOpenWatchlist,
}: {
  onOpen: (lei: string) => void;
  onOpenWatchlist: () => void;
}) {
  const [asked, setAsked] = useState<string | null>(() =>
    typeof window === "undefined" ? null : dayFromSearch(window.location.search),
  );
  useEffect(() => {
    const onPop = () => setAsked(dayFromSearch(window.location.search));
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const indexQuery = useQuery({
    queryKey: ["daily-changes"],
    queryFn: fetchDailyChangesIndex,
    staleTime: 5 * 60 * 1000,
    retry: false,
  });
  const index = indexQuery.data;
  const day = useMemo(() => (index ? chooseDay(index, asked, todayUtc()) : null), [index, asked]);
  const dayQuery = useQuery({
    queryKey: ["daily-changes", day],
    queryFn: () => fetchDailyChangesDay(day as string),
    enabled: !!day,
    staleTime: 5 * 60 * 1000,
  });
  const data = dayQuery.data;

  const goTo = (d: string) => {
    const url = new URL(window.location.href);
    url.searchParams.set("day", d);
    window.history.pushState({}, "", url.toString());
    setAsked(d);
  };

  const near = index && day ? neighbours(index, day) : { older: null, newer: null };
  const file = index?.files?.find((f) => f.day === day);
  const headline = data ? rowsIn(data, "headline") : [];

  return (
    <section aria-labelledby="changes-heading">
      <SectionLabel as="p" className="mb-2">Daily changes</SectionLabel>
      <h2
        id="changes-heading"
        className="font-head font-bold text-[clamp(1.35rem,3vw,1.8rem)] text-oo-ink mb-2 leading-tight"
      >
        {day ? `What changed on ${longDay(day)}` : "What changed"}
      </h2>
      <p className="text-oo-body leading-relaxed text-oo-muted mb-4 max-w-2xl">
        Companies with a Legal Entity Identifier whose record at GLEIF or OpenSanctions changed
        that day. Each row says what the source changed. OpenCheck has not run a check on any of
        them.
      </p>

      {indexQuery.isLoading && <p role="status" className="text-oo-small text-oo-muted">Loading the days held…</p>}
      {indexQuery.isError && (
        <div role="alert" className="rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3 text-oo-small text-oo-warn-text">
          The daily changes list is not available on this server right now.
        </div>
      )}
      {index && !day && (
        <p role="status" className="text-oo-small text-oo-muted">No day has been recorded yet.</p>
      )}

      {index && day && (
        <>
          <nav aria-label="Choose a day" className="flex flex-wrap items-center gap-2 mb-5">
            <Button size="sm" variant="secondary" disabled={!near.older} onClick={() => near.older && goTo(near.older)}>
              Earlier day
            </Button>
            <label className="text-oo-small text-oo-muted flex items-center gap-2">
              Day
              <select
                value={day}
                onChange={(e) => goTo(e.target.value)}
                className="border border-oo-rule rounded-oo px-2 min-h-[36px] text-oo-small text-oo-ink bg-white"
              >
                {index.days.map((d) => (
                  <option key={d.day} value={d.day}>
                    {longDay(d.day)}
                    {d.day >= todayUtc() ? " (so far)" : ""}
                  </option>
                ))}
              </select>
            </label>
            <Button size="sm" variant="secondary" disabled={!near.newer} onClick={() => near.newer && goTo(near.newer)}>
              Later day
            </Button>
            {data && !data.complete && (
              <Chip tone="warn">Today, still being recorded</Chip>
            )}
          </nav>

          {dayQuery.isLoading && <p role="status" className="text-oo-small text-oo-muted">Loading {longDay(day)}…</p>}
          {dayQuery.isError && (
            <div role="alert" className="rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3 text-oo-small text-oo-warn-text mb-4">
              This day could not be loaded. <Button size="sm" variant="warnGhost" onClick={() => dayQuery.refetch()}>Try again</Button>
            </div>
          )}

          {data && (
            <div className="grid gap-6 grid-cols-[minmax(0,1fr)] lg:grid-cols-[minmax(0,1fr)_20rem]">
              {/* An explicit single column below lg: an implicit grid track
                  is `auto`, which takes the min-content width of its widest
                  item (the curl lines in the download box) and pushed every
                  row 111px past a phone's edge. */}
              <div className="min-w-0">
                <dl className="flex flex-wrap gap-x-6 gap-y-2 mb-6">
                  {STRIP.map(({ band: b, label }) => (
                    <div key={b} className="flex items-baseline gap-1.5">
                      <dd className="font-head font-bold text-oo-head text-oo-ink m-0">
                        {formatCount(bandCount(data, b))}
                      </dd>
                      <dt className="text-oo-small text-oo-muted">{label}</dt>
                    </div>
                  ))}
                </dl>

                <section aria-labelledby="band-headline" className="bg-white border border-oo-rule rounded-oo px-5 py-4 mb-4">
                  <h3 id="band-headline" className="font-head font-bold text-oo-lead text-oo-ink">
                    {BAND_LABELS.headline}
                  </h3>
                  <p className="text-oo-small text-oo-muted mt-1 mb-1">
                    {formatCount(headline.length)} {headline.length === 1 ? "company" : "companies"}.{" "}
                    {BAND_SENTENCES.headline}
                  </p>
                  <Breakdown rows={headline} />
                  {headline.length ? (
                    <RowsShown rows={headline} first={HEADLINE_SHOWN} step={null} onOpen={onOpen} />
                  ) : (
                    <p className="text-oo-small text-oo-ink">No ownership or sanctions change was recorded on this day.</p>
                  )}
                </section>

                <FoldedBand band="ceased" day={data} onOpen={onOpen} />
                <FoldedBand band="ceased_lapsed" day={data} onOpen={onOpen} />
                <FoldedBand band="record" day={data} onOpen={onOpen} />

                <section aria-labelledby="counted" className="border border-oo-rule rounded-oo px-5 py-4 mt-4">
                  <h3 id="counted" className="font-head font-bold text-oo-lead text-oo-ink mb-2">
                    Counted, and not listed on this page
                  </h3>
                  <dl className="space-y-1">
                    {countedNotListed(data).map((c) => (
                      <div key={c.key} className="flex items-baseline gap-2">
                        <dt className="font-head font-bold text-oo-body text-oo-ink min-w-[4rem]">{formatCount(c.n)}</dt>
                        <dd className="m-0 text-oo-small text-oo-muted">{c.sentence}</dd>
                      </div>
                    ))}
                  </dl>
                </section>

                <section className="bg-oo-soft border border-oo-softBorder rounded-oo px-5 py-4 mt-4">
                  <h3 className="font-head font-bold text-oo-lead text-oo-ink mb-1">Following particular companies?</h3>
                  <p className="text-oo-small text-oo-ink mb-2">
                    A watchlist re-runs the full check when one of your companies changes, and tells you
                    what the check found.
                  </p>
                  <a
                    href="/watchlist"
                    onClick={(e) => {
                      e.preventDefault();
                      onOpenWatchlist();
                    }}
                    className={buttonClasses("secondary", "sm")}
                  >
                    Set up a watchlist
                  </a>
                </section>

                <p className="text-oo-meta text-oo-muted mt-4">
                  Changes are read from GLEIF's Golden Copy deltas as they are published, and from
                  OpenSanctions as it publishes.
                </p>
              </div>
              <DownloadAside day={day} file={file} />
            </div>
          )}
        </>
      )}
    </section>
  );
}
