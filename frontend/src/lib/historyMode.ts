/**
 * The History tab's values layer (Phase 190).
 *
 * `/history` merges every register OpenCheck holds a change log for onto one
 * axis — GLEIF, Companies House, the New Zealand Companies Office, the
 * Estonian e-Äriregister and Danish CVR. This module holds what the tab
 * *says* about that merge: which register a row came from, how to get back to
 * the record that published it, and the sentence that opens the tab.
 *
 * It lives in `lib/` because the frontend's node suite is logic-only: a
 * sentence that exists only as a literal inside JSX cannot be pinned, which is
 * how four verbs for two actions happened (see `lib/vocab.ts`).
 *
 * Two claims this module is careful about, both of the same kind the
 * Subsidiaries tab makes:
 *
 * - **Publishing a change log is rare.** Five of the registers OpenCheck reads
 *   expose entity history; the rest answer only about now. A change missing
 *   from this timeline is far more often a register that keeps no history than
 *   a change that did not happen, and the tab has to say so rather than let
 *   silence read as evidence.
 * - **Knowing the company is not the same as answering.** `registry_numbers`
 *   says a register addresses the company by some number; `sources` says it
 *   returned events. A register that holds the company and published nothing
 *   is a different statement from one that was never asked, and both differ
 *   again from one that refused.
 */

import type { HistoryEntry, HistoryRawChange, HistoryResponse } from "./api";

/** Every source that can emit change events, in the order the tab names them. */
export const HISTORY_SOURCES = [
  "gleif",
  "companies_house",
  "nz_companies",
  "ariregister",
  "cvr_denmark",
] as const;

export type HistorySourceId = (typeof HISTORY_SOURCES)[number];

/**
 * How each register is named to a reader.
 *
 * `cvr_denmark` was missing here until Phase 190, so a Danish row chipped as
 * the raw slug — invisible until a Danish company with history was looked at,
 * which is the failure mode of a label map that is not derived from anything.
 */
export const HISTORY_SOURCE_LABEL: Record<string, string> = {
  gleif: "GLEIF",
  companies_house: "Companies House",
  nz_companies: "Companies Office (NZ)",
  ariregister: "e-Äriregister (EE)",
  cvr_denmark: "CVR (DK)",
};

export function historySourceLabel(sourceId: string): string {
  return HISTORY_SOURCE_LABEL[sourceId] ?? sourceId;
}

/** Rows shown before the timeline collapses behind a control. */
export const VISIBLE_ROWS = 10;

/**
 * The public record a row came from, or `null` when the register publishes no
 * addressable page for it.
 *
 * GLEIF is addressed by the LEI the caller already holds; the four national
 * registers each need their own number, which is why `/history` grew
 * `registry_numbers` in Phase 190. Before that only GLEIF and Companies House
 * could be linked and the other three rendered as plain text — a timeline that
 * shows a change and cannot say where to read it is doing half its job.
 */
export function recordUrl(
  sourceId: string,
  lei: string,
  registryNumbers: Record<string, string> = {},
): string | null {
  const n = registryNumbers[sourceId];
  switch (sourceId) {
    case "gleif":
      return `https://search.gleif.org/#/record/${encodeURIComponent(lei)}`;
    case "companies_house":
      return n
        ? `https://find-and-update.company-information.service.gov.uk/company/${encodeURIComponent(n)}/filing-history`
        : null;
    case "nz_companies":
      // The company number GLEIF publishes for RA000466 addresses the record
      // directly. It must NOT be handed to the register's `/search?q=` path:
      // that page runs its query in the browser, so the server returns an
      // empty search form with HTTP 200 — a link that looks like it worked
      // and shows the reader nothing about the company. Verified against
      // Fonterra Commodities Limited (2288120), whose record this URL
      // returns and whose search URL does not mention it at all.
      return n
        ? `https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/${encodeURIComponent(n)}`
        : null;
    case "ariregister":
      return n ? `https://ariregister.rik.ee/eng/company/${encodeURIComponent(n)}` : null;
    case "cvr_denmark":
      return n ? `https://datacvr.virk.dk/enhed/virksomhed/${encodeURIComponent(n)}` : null;
    default:
      return null;
  }
}

/** Earliest and latest dated year across entries, or `null` when none is dated. */
export function datedSpan(entries: HistoryEntry[]): { from: string; to: string } | null {
  const dates = entries.map((e) => e.date).filter((d): d is string => !!d).sort();
  if (dates.length === 0) return null;
  return { from: dates[0].slice(0, 4), to: dates[dates.length - 1].slice(0, 4) };
}

/** Entries a second register also recorded — the merge's own evidence. */
export function corroboratedCount(entries: HistoryEntry[]): number {
  return entries.filter((e) => e.sources.length > 1).length;
}

/**
 * Registers that hold this company but returned no events.
 *
 * Read from `registry_numbers` minus `sources`. A register here was asked and
 * had nothing to say, or could not be asked because OpenCheck has no key for
 * it — either way the tab names it rather than leaving the reader to assume
 * the timeline is the whole of the company's recorded history.
 */
export function silentRegisters(data: HistoryResponse): string[] {
  const answered = new Set(data.sources);
  return Object.keys(data.registry_numbers ?? {})
    .filter((id) => !answered.has(id))
    .sort();
}

function numberWord(n: number): string {
  return ["No", "One", "Two", "Three", "Four", "Five"][n] ?? String(n);
}

/**
 * The tab's opening sentence, built from the numbers rather than asserted.
 *
 * It has to carry the advocacy point without overstating it: history is
 * scarce, the registers that keep it disagree about what a date means, and an
 * empty timeline is a statement about record-keeping rather than about the
 * company.
 */
export function historySentence(data: HistoryResponse, name: string): string {
  const n = data.sources.length;
  const entries = data.notable ?? [];
  const absence =
    "That is an absence of records, not a finding that nothing changed.";

  if (n === 0 || entries.length === 0) {
    if (n === 0) {
      return `No register OpenCheck can ask publishes a change log for ${name}. ${absence}`;
    }
    const which = n === 1 ? "One register holds" : `${numberWord(n)} registers hold`;
    const it = n === 1 ? "it records" : "none of them records";
    return `${which} a change log for ${name}, and ${it} a notable change to its ownership or identity. ${absence}`;
  }

  const registers =
    n === 1 ? "One register publishes" : `${numberWord(n)} registers publish`;
  const span = datedSpan(entries);
  const changes = `${entries.length.toLocaleString()} notable ${entries.length === 1 ? "change" : "changes"}`;
  const opening = span
    ? `${registers} a change log for ${name} — ${changes}${span.from === span.to ? ` in ${span.from}` : ` between ${span.from} and ${span.to}`}`
    : `${registers} a change log for ${name} — ${changes}`;

  const agreed = corroboratedCount(entries);
  const tail =
    n === 1
      ? "No second register keeps a history to compare it against."
      : agreed === 0
        ? "No change is recorded by more than one of them: they log different things, and a date one calls effective another calls the day it noticed."
        : `${agreed.toLocaleString()} of them ${agreed === 1 ? "is" : "are"} recorded by more than one register, which is how a filing date and the day a register noticed can be told apart.`;

  return `${opening}. ${tail}`;
}

/**
 * The standing caveat under the sentence — true of every entity, so it is not
 * built from the numbers.
 *
 * FATF Recommendations 24 and 25 ask registers to keep beneficial-ownership
 * records for at least five years after a company is struck off, and most keep
 * far better records of corporate changes than of ownership changes. The
 * asymmetry is the thing an investigator most needs to know before reading a
 * short timeline as a quiet company.
 */
export const HISTORY_CAVEAT =
  "Most registers publish no history at all, and those that do keep changes of name, address and status far better than changes of ownership. What is missing here is usually a register that keeps no record, not a change that did not happen.";

/**
 * The label under a date, saying what that date actually means.
 *
 * "as recorded by GLEIF" names GLEIF rather than "the register" because GLEIF
 * is the only emitter that produces a `recorded` basis — the four national
 * registers all publish real effective dates. And because a cluster's shown
 * date is its most authoritative one (effective outranks recorded), a row
 * still reading `recorded` is one no register dated for us. Generalising the
 * wording would lose that, for no source it would become true of.
 */
export function basisLabel(basis: string): string {
  if (basis === "effective") return "as filed";
  if (basis === "recorded") return "as recorded by GLEIF";
  if (basis === "snapshot_window") return "approximate";
  return "";
}

// --------------------------------------------------------------------------
// Rail rows — moved here from the component in Phase 190
// --------------------------------------------------------------------------

export type Row =
  | { kind: "notable"; date: string; entry: HistoryEntry }
  | { kind: "noise"; date: string; raw: HistoryRawChange }
  | { kind: "board"; date: string; raw: HistoryRawChange };

/** Tier-3 (administrative noise) raw changes, used by the full-timeline toggle. */
export function noiseEventsOf(data: HistoryResponse): HistoryRawChange[] {
  return (data.events ?? []).filter((e) => e.tier === 3);
}

/** Sentinel date for undated rows so they always sort to the bottom. */
const _UNDATED = "9999-12-31";

/**
 * The rail rows: notable entries always; noise rows when toggled on. Sorted
 * newest-first (reverse chronological); undated rows pinned last.
 *
 * The endpoint returns events oldest-first — that is the right order for an
 * audit trail and the wrong one for a reader, who arrives asking what changed
 * most recently.
 */
export function buildTimelineRows(
  data: HistoryResponse,
  showNoise: boolean,
  showBoard = false,
): Row[] {
  const out: Row[] = data.notable.map((entry) => ({
    kind: "notable",
    date: entry.date ?? _UNDATED,
    entry,
  }));
  if (showNoise) {
    for (const raw of noiseEventsOf(data))
      out.push({ kind: "noise", date: raw.event_date ?? _UNDATED, raw });
  }
  if (showBoard) {
    for (const raw of boardChangesOf(data))
      out.push({ kind: "board", date: raw.event_date ?? _UNDATED, raw });
  }
  out.sort((a, b) => {
    const aMissing = a.date === _UNDATED;
    const bMissing = b.date === _UNDATED;
    if (aMissing !== bMissing) return aMissing ? 1 : -1; // undated last
    return b.date.localeCompare(a.date); // newest first
  });
  return out;
}

/**
 * What to tell the reader when GLEIF would not answer, or `null`.
 *
 * The Time Machine's silent failure was the worst of the three GLEIF panels:
 * a 429 emptied the change log AND — because the Companies House / NZ /
 * Estonia / Denmark branches gate on a registry number that only the GLEIF
 * record carries — stopped every other source from being attempted, leaving a
 * near-empty timeline that read as "checked, nothing happened". These
 * sentences say which of those two things actually occurred.
 */
export function historyDegradedNotice(data: HistoryResponse): string | null {
  const recordDown = data.gleif_record_available === false;
  const eventsDown = data.gleif_events_available === false;
  if (!recordDown && !eventsDown) return null;

  const parts: string[] = [];
  if (eventsDown && recordDown) {
    parts.push(
      "GLEIF is rate-limiting or unreachable, so this entity's change history could not be checked.",
    );
  } else if (eventsDown) {
    parts.push(
      "GLEIF did not return its change log, so any GLEIF-recorded changes are missing from this timeline.",
    );
  } else {
    parts.push("GLEIF did not return this entity's record.");
  }
  if (data.registry_sources_blocked) {
    parts.push(
      "The company-registry histories (Companies House, NZ, Estonia, Denmark) could not be attempted either — the registry number they need comes from that record.",
    );
  } else if (recordDown && data.company_number_basis === "cached") {
    parts.push(
      "The company-registry histories were still attempted, using a registry number from OpenCheck's cached copy of the GLEIF record.",
    );
  }
  parts.push("What is shown is not a finding that nothing changed.");
  return parts.join(" ");
}

// --------------------------------------------------------------------------
// Board changes — Phase 194
//
// Who joined and who left, as its own opt-in stream. Not `notable`: an
// appointment is not a beneficial-ownership change, and on Lloyds Bank PLC
// there are 195 of them against 10 notable rows, so promoting them would bury
// the thing the tab leads on.
//
// Phase 198 changed where these come from. They were read out of the filing
// history, which cost nothing extra but published only a name — 75 of 246
// rows named anyone at all, and none of them could be pointed at a person.
// They are read from the register's officers list now: fewer rows (195, and
// the pre-1992 tail is gone, because Companies House holds officer records
// from 1992 for this company while its filings go back to 1986), but every
// one names the officer and carries the id the graph keys a person on. The
// officer filings did not disappear; they are typed rows in the
// administrative stream, where the ones that are not turnover always were.
// --------------------------------------------------------------------------

/** Tier-4 (board turnover) raw changes: appointments and resignations. */
export function boardChangesOf(data: HistoryResponse): HistoryRawChange[] {
  return (data.events ?? []).filter((e) => e.tier === 4);
}

/**
 * What the board stream says about itself, or `null` when there is nothing to
 * open. Built from the rows, never from a literal: the counts, the span, and
 * how many of them name anybody.
 *
 * The naming clause survives Phase 198 even though the officers list names
 * everyone it lists, because the register does not key every officer it
 * holds, and a stream that silently dropped the unnamed ones would be a
 * quieter lie than one that counts them.
 */
export function boardChangesSummary(rows: HistoryRawChange[]): string | null {
  if (rows.length === 0) return null;

  const dates = rows
    .map((r) => r.event_date)
    .filter((d): d is string => Boolean(d))
    .sort();
  const named = rows.filter((r) => Boolean(r.counterparty)).length;

  const count =
    rows.length === 1
      ? "1 appointment or resignation"
      : `${rows.length.toLocaleString()} appointments and resignations`;
  const span =
    dates.length === 0
      ? null
      : dates[0].slice(0, 4) === dates[dates.length - 1].slice(0, 4)
        ? `in ${dates[0].slice(0, 4)}`
        : `between ${dates[0].slice(0, 4)} and ${dates[dates.length - 1].slice(0, 4)}`;

  let naming: string;
  if (named === rows.length)
    naming = rows.length === 1 ? "naming the officer" : "each naming the officer";
  else if (named === 0) naming = "none of which name the officer";
  else if (named === 1) naming = "1 of which names the officer";
  else naming = `${named.toLocaleString()} of which name the officer`;

  const head = [count, span].filter(Boolean).join(" ");
  return `${head} — ${naming}.`;
}

/**
 * What to tell the reader when the register holds more filings than the fetch
 * read, or `null`.
 *
 * Companies House answers filing-history newest-first, so a truncated history
 * is missing its oldest end. Saying nothing would let a 1,000-filing view read
 * as a complete one, which is the defect Phase 192 fixed on the officers list.
 * Since Phase 198 this bounds the administrative stream only — the board
 * stream is read from the officers list, which is fetched whole.
 */
export function filingsTruncatedNotice(data: HistoryResponse): string | null {
  if (!data.filings_truncated) return null;
  return (
    "Companies House holds more filings than this view reads. Its history is " +
    "returned newest first, so the filings not shown are the oldest — this " +
    "timeline starts later than the company does."
  );
}

/**
 * What to tell the reader when the board stream is empty for a company the
 * officers list was never read for, or `null`.
 *
 * Phase 198. Every other emptiness on this tab already says which kind it is
 * — `gleif_record_available` for the change log, `registry_sources_blocked`
 * for the registry sources. The board stream had no such flag because it fell
 * out of a fetch that was happening anyway; now it has its own call, and a
 * call that did not happen must not look like a company with no board.
 */
export function boardUncheckedNotice(data: HistoryResponse): string | null {
  if (data.officers_available !== false) return null;
  if ((data.events ?? []).some((e) => e.tier === 4)) return null;
  return (
    "The register's officers list was not read for this company, so there is " +
    "no board history to show — which is not the same as a company with no " +
    "board changes."
  );
}

/**
 * The BODS person statement a board row points at, or `null` when the row
 * identifies nobody.
 *
 * Two separate conditions, and the second is the one that is easy to forget:
 * the register may key an officer OpenCheck does not draw. The graph holds
 * *serving managing officials* — a resigned director, or a secretary, is on
 * this stream and not in that graph — so `known` is the caller's set of
 * statement ids and a row outside it stays plain text. Linking to a node that
 * is not there would be worse than not linking.
 */
export function boardRowPersonId(
  row: HistoryRawChange,
  known: ReadonlySet<string>,
): string | null {
  const sid = row.party_statement_id;
  if (!sid) return null;
  return known.has(sid) ? sid : null;
}

/**
 * Where a linkable board row goes: the FullCheck network, focused on that
 * person.
 *
 * A real href rather than a click handler alone, so the row survives a
 * right-click, a middle-click and a copied link — the modes are URL-addressed
 * (`?mode=`) precisely so a view can be shared, and a control that only works
 * on a plain left-click would be the one part of the tab that is not.
 */
export function boardRowPersonHref(lei: string, statementId: string): string {
  const params = new URLSearchParams({
    lei,
    mode: "full",
    focus: statementId,
  });
  return `/?${params.toString()}`;
}

/**
 * How many rows in this stream can reach a person, and the sentence that says
 * why the rest cannot — or `null` when the question does not arise.
 *
 * This exists because the honest number is small. On Lloyds Bank PLC 15 of
 * 195 board rows link: the graph draws the officers who are *serving*, and
 * the board stream is mostly people who have left. Fifteen scattered links in
 * a stream of otherwise identical rows read as broken unless the page says
 * plainly what makes a row linkable — which is why the affordance is labelled
 * and counted rather than left as a bare underline on a name.
 */
export function boardLinkSummary(
  rows: HistoryRawChange[],
  known: ReadonlySet<string>,
): string | null {
  if (rows.length === 0 || known.size === 0) return null;
  const linked = rows.filter((r) => boardRowPersonId(r, known)).length;
  if (linked === 0) return null;
  const count =
    linked === 1
      ? "One row reaches a person in the network"
      : `${linked.toLocaleString()} of these rows reach a person in the network`;
  return (
    `${count}: the graph draws the officers currently serving, so an ` +
    "appointment links where that person is still on the board. The rest name " +
    "an officer the register no longer lists as serving, and there is no node " +
    "to point at."
  );
}
