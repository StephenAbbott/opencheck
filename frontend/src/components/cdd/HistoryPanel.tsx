/**
 * HistoryPanel — the History tab (Phase 190).
 *
 * How this company's records changed, merged across every register OpenCheck
 * holds a change log for: GLEIF's field-modification stream, Companies House
 * filing history, the New Zealand Companies Office, the Estonian
 * e-Äriregister and Danish CVR. One axis, newest first, each dated row
 * carrying the register that recorded it and a link back to the record.
 *
 * The merge itself is not new — `timeline/assemble.py` has clustered
 * cross-source identity changes and ranked their dates since Phase 146. What
 * is new is that it has a home. Until now it was reachable only by pressing
 * "Changes over time" on a source card, which mounted the same entity-wide
 * timeline under each of the five sources that emit history: up to five
 * identical copies of one panel, none of them addressable, and each one
 * looking like that source's own history when it was every source's.
 *
 * Two things the tab is careful to say, because a timeline that says neither
 * invites the wrong reading:
 *
 * - **A short history is usually a register's silence, not a quiet company.**
 *   Most registers publish no change log at all, and those that do keep
 *   changes of name, address and status far better than changes of ownership
 *   — the asymmetry FATF R.24/R.25 exists to close.
 * - **Dates from different registers mean different things.** A Companies
 *   House date is when a change took effect; a GLEIF date is when GLEIF
 *   noticed. The rail labels each one rather than flattening them.
 *
 * Renders bands, not cards (the Phase 128 rule): the tabpanel owns the
 * `PanelCard` and this supplies its `PanelSection`s.
 */

import { useEffect, useMemo, useState } from "react";
import { getHistory, type HistoryResponse } from "../../lib/api";
import {
  boardChangesOf,
  boardChangesSummary,
  boardUncheckedNotice,
  buildTimelineRows,
  corroboratedCount,
  filingsTruncatedNotice,
  historyDegradedNotice,
  HISTORY_CAVEAT,
  historySentence,
  historySourceLabel,
  noiseEventsOf,
  silentRegisters,
  VISIBLE_ROWS,
} from "../../lib/historyMode";
import { describeFetchFailure, panelError, type PanelError, type PanelId } from "../../lib/panelErrors";
import { Button } from "../ui/Button";
import PanelSection from "../ui/PanelSection";
import { HistoryTimeline } from "./HistoryTimeline";

/** "3 registers · 10 changes · 3 corroborated" */
function coverageAside(data: HistoryResponse | null, loading: boolean): string {
  if (loading) return "Reading the change logs…";
  if (!data) return "";
  const n = data.sources.length;
  const parts = [`${n} ${n === 1 ? "register" : "registers"}`];
  parts.push(`${data.notable.length.toLocaleString()} notable`);
  const agreed = corroboratedCount(data.notable);
  if (agreed > 0) parts.push(`${agreed.toLocaleString()} corroborated`);
  return parts.join(" · ");
}

export default function HistoryPanel({
  lei,
  legalName,
  onPanelError,
  onPanelRecovered,
}: {
  lei: string;
  legalName: string | null;
  /** `/history` sits outside `_lookup_pipeline`, so nothing else learns it
   *  failed — the same channel `/securities` and `/subsidiaries` report on. */
  onPanelError?: (e: PanelError) => void;
  onPanelRecovered?: (panel: PanelId) => void;
}) {
  const name = legalName ?? lei;
  const [data, setData] = useState<HistoryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);
  const [showNoise, setShowNoise] = useState(false);
  const [showBoard, setShowBoard] = useState(false);

  useEffect(() => {
    let live = true;
    setData(null);
    setError(null);
    setShowAll(false);
    setShowNoise(false);
    setShowBoard(false);
    // `true`: the raw tier-3 stream rides along on the first fetch, so the
    // full-timeline toggle is instant rather than a second round trip.
    getHistory(lei, true)
      .then((d) => {
        if (!live) return;
        setData(d);
        onPanelRecovered?.("history");
      })
      .catch((e) => {
        if (!live) return;
        setError(describeFetchFailure(e));
        onPanelError?.(panelError("history", e));
      });
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lei]);

  const loading = !data && !error;
  const degraded = useMemo(() => (data ? historyDegradedNotice(data) : null), [data]);
  const noiseEvents = useMemo(() => (data ? noiseEventsOf(data) : []), [data]);
  const boardEvents = useMemo(() => (data ? boardChangesOf(data) : []), [data]);
  const boardSummary = useMemo(() => boardChangesSummary(boardEvents), [boardEvents]);
  const truncated = useMemo(() => (data ? filingsTruncatedNotice(data) : null), [data]);
  const boardUnchecked = useMemo(() => (data ? boardUncheckedNotice(data) : null), [data]);
  const allRows = useMemo(
    () => (data ? buildTimelineRows(data, showNoise, showBoard) : []),
    [data, showNoise, showBoard],
  );
  const rows = showAll ? allRows : allRows.slice(0, VISIBLE_ROWS);
  const silent = useMemo(() => (data ? silentRegisters(data) : []), [data]);

  return (
    <>
      <PanelSection title="What the registers keep" aside={coverageAside(data, loading)}>
        {error && (
          <p
            role="alert"
            className="mb-2 text-oo-small text-oo-warn-text bg-oo-warn-bg border border-oo-warn-border rounded-oo px-3 py-2"
          >
            The change history could not be fetched — {error}. This is not a finding that
            nothing about {name} has changed.
          </p>
        )}
        {loading ? (
          <p role="status" className="text-oo-small text-oo-muted italic">
            Reading what each register records about changes to {name}…
          </p>
        ) : data ? (
          <>
            <p className="text-oo-body text-oo-ink leading-[1.6] max-w-[82ch]">
              {historySentence(data, name)}
            </p>
            <p className="mt-1.5 text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">
              {HISTORY_CAVEAT}
            </p>
            {/* Phase 146's honesty flags: an upstream that refused is not an
                entity that never changed. Amber, above the pills. */}
            {degraded && (
              <div
                role="status"
                className="mt-3 rounded-oo border border-oo-warn-border bg-oo-warn-bg px-3 py-2 text-oo-meta text-oo-warn-text leading-[1.5]"
              >
                {degraded}
              </div>
            )}
            {/* Phase 194: the register holds more than this view read, and it
                answers newest-first — so what is missing is the oldest end.
                Stated where it qualifies every row below, not inside the one
                stream that made it worth saying. */}
            {truncated && (
              <div
                role="status"
                className="mt-3 rounded-oo border border-oo-warn-border bg-oo-warn-bg px-3 py-2 text-oo-meta text-oo-warn-text leading-[1.5]"
              >
                {truncated}
              </div>
            )}
            {/* Phase 198: the board stream has its own fetch now, so it has
                its own way to be missing. An unread officers list is not a
                company that never changed its board. */}
            {boardUnchecked && (
              <div
                role="status"
                className="mt-3 rounded-oo border border-oo-warn-border bg-oo-warn-bg px-3 py-2 text-oo-meta text-oo-warn-text leading-[1.5]"
              >
                {boardUnchecked}
              </div>
            )}
            {data.sources.length > 0 && (
              <ul className="mt-3 flex flex-wrap gap-2" aria-label="Registers with a change log">
                {data.sources.map((s) => (
                  <li
                    key={s}
                    className="rounded-oo border border-oo-rule bg-oo-bg px-2.5 py-1.5 text-oo-meta text-oo-ink"
                  >
                    <span className="font-semibold">{historySourceLabel(s)}</span>
                    <span className="text-oo-muted">
                      {" "}
                      · {data.notable.filter((e) => e.sources.includes(s)).length.toLocaleString()}{" "}
                      notable
                    </span>
                  </li>
                ))}
              </ul>
            )}
            {silent.length > 0 && (
              <ul className="mt-2 space-y-0.5" aria-label="Registers with no change log for this company">
                {silent.map((s) => (
                  <li key={s} className="text-oo-meta text-oo-muted">
                    {historySourceLabel(s)} — holds this company, and published no change
                    history for it here.
                  </li>
                ))}
              </ul>
            )}
          </>
        ) : null}
      </PanelSection>

      {data && data.notable.length > 0 && (
        <PanelSection
          title="Changes over time"
          aside="most recent first · one axis across every register"
        >
          <HistoryTimeline rows={rows} lei={lei} data={data} />

          <div className="mt-3 flex flex-wrap items-center gap-2">
            {allRows.length > VISIBLE_ROWS && (
              <Button
                variant="secondary"
                aria-expanded={showAll}
                onClick={() => setShowAll((v) => !v)}
              >
                {showAll
                  ? `Show the first ${VISIBLE_ROWS}`
                  : `Show all ${allRows.length.toLocaleString()} rows`}
              </Button>
            )}
            {boardEvents.length > 0 && (
              // Board turnover is its own stream: not notable, because an
              // appointment is not a beneficial-ownership change, and not
              // administrative either. A director's own particulars changing
              // is, and stays with the noise.
              <Button
                variant="ghost"
                size="sm"
                aria-pressed={showBoard}
                onClick={() => {
                  setShowBoard((v) => !v);
                  setShowAll(false);
                }}
              >
                {showBoard
                  ? "Hide board changes"
                  : `Add the ${boardEvents.length.toLocaleString()} board changes`}
              </Button>
            )}
            {noiseEvents.length > 0 && (
              // The administrative stream is the larger half by an order of
              // magnitude — a thousand Companies House confirmation statements
              // and GLEIF renewal-date bumps for one company — so it stays
              // behind a control, and behind the row cap once revealed.
              <Button
                variant="ghost"
                size="sm"
                aria-pressed={showNoise}
                onClick={() => {
                  setShowNoise((v) => !v);
                  setShowAll(false);
                }}
              >
                {showNoise
                  ? "Hide administrative changes"
                  : `Add the ${noiseEvents.length.toLocaleString()} administrative changes`}
              </Button>
            )}
          </div>
          {showBoard && boardSummary && (
            // Phase 198 retired the sentence that used to sit here about
            // Companies House naming an officer only from the electronic era.
            // It was true of the filing history this stream used to be read
            // from, and is not true of the officers list it is read from now,
            // which names everyone it lists. What replaces it is the trade
            // that change made: names and a link to the person, against a
            // shorter reach back.
            <p className="mt-2 text-oo-meta text-oo-muted leading-[1.5] max-w-[82ch]">
              {boardSummary} These come from the register's officers list rather than
              its filings, so each one names the officer — but the register keeps
              officer records for a shorter period than it keeps filings, and an
              appointment older than that shows in the administrative stream instead.
            </p>
          )}
          {showNoise && (
            <p className="mt-2 text-oo-meta text-oo-muted leading-[1.5] max-w-[82ch]">
              Administrative filings are shown greyed: confirmation statements, renewal
              dates and field-precision fixes. Filtering them out is what makes the rest a
              timeline — but they are the register's own record, so they are suppressed,
              never dropped.
            </p>
          )}
        </PanelSection>
      )}
    </>
  );
}
