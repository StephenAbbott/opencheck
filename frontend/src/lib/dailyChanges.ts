/**
 * The `/changes` page's values layer — what changed at GLEIF or
 * OpenSanctions on one UTC day (the "Daily deltas for OpenCheck" ticket).
 *
 * The page reads `GET /daily-changes` (the days held, their counts and the
 * day files) and `GET /daily-changes/{day}.json` (one day's rows). Nothing on
 * the page runs a check: each row says what the publisher changed, and "Run a
 * check" is the reader's choice.
 *
 * `BAND_LABELS` and the keys of `REASON_WORDS` mirror `BAND_WORDS` and
 * `REASON_BAND` in `backend/opencheck/daily_changes.py`;
 * `backend/tests/test_daily_changes_file.py` parses this file and fails when
 * either side moves alone.
 */

import { BASE_URL } from "./api";
import { topicList } from "./vocab";

export type Band = "headline" | "ceased" | "ceased_lapsed" | "record" | "note" | "new";
export type ChangeSource = "gleif" | "opensanctions";

export interface DailyChange {
  field?: string;
  old?: unknown;
  new?: unknown;
  topics_added?: string[];
  topics_removed?: string[];
  datasets_added?: string[];
}

export interface DailyRow {
  lei: string;
  name: string | null;
  jurisdiction: string | null;
  source: ChangeSource;
  band: Band;
  reasons: string[];
  changes: DailyChange[];
  as_of: string;
  ref?: { opensanctions_id: string; url: string };
}

export interface DailyFile {
  day: string;
  bods: string;
  csv: string;
  json: string;
  bytes: number | null;
  statements: number | null;
  leis: number | null;
}

export interface DailyDaySummary {
  day: string;
  bands: Partial<Record<Band, number>>;
  sources: Partial<Record<ChangeSource, Record<string, number>>>;
}

export interface DailyIndex {
  available: boolean;
  what_this_is?: string;
  days: DailyDaySummary[];
  files?: DailyFile[];
}

export interface DailyDay {
  day: string;
  complete: boolean;
  counts: Partial<Record<Band, number>>;
  sources: Partial<Record<ChangeSource, Record<string, number>>>;
  rows: DailyRow[];
  opensanctions_notice: string;
  /** OpenSanctions' title for each dataset the rows name, from its
   *  catalogue; a dataset missing here is shown by its code. */
  dataset_titles?: Record<string, string>;
}

export const BAND_LABELS: Record<Band, string> = {
  headline: "Ownership and sanctions",
  ceased: "Ceased",
  ceased_lapsed: "Ceased, on an LEI that had already lapsed",
  record: "Record changed",
  note: "Note only",
  new: "New LEI",
};

/** What each band's section says under its heading. */
export const BAND_SENTENCES: Record<"headline" | "ceased" | "ceased_lapsed" | "record", string> = {
  headline:
    "A parent changed, a successor or merger was recorded, or OpenSanctions added or changed a listing.",
  ceased:
    "GLEIF now records the company as inactive, or recorded an ending such as a dissolution, liquidation or bankruptcy.",
  ceased_lapsed:
    "These companies ended some time ago and their LEI had already lapsed. GLEIF's record caught up on this day.",
  record: "The name, legal form, jurisdiction, address country or status changed.",
};

/** One short phrase per reason, for the chip on a row. */
export const REASON_WORDS: Record<string, string> = {
  parent_changed: "Parent changed",
  successor_named: "Successor named",
  merger_event: "Merger recorded",
  entity_inactive: "Now inactive",
  terminal_event: "Ending recorded",
  legal_name: "Legal name",
  legal_form: "Legal form",
  jurisdiction: "Jurisdiction",
  address_country: "Address country",
  entity_status: "Entity status",
  event_completed: "Event completed",
  terminal_event_in_progress: "Ending in progress",
  reporting_exception: "Reporting exception",
  register_identifier: "Register number",
  registration_status: "LEI registration status",
  classification: "Classification",
  creation_date: "Creation date",
  expiration: "Expiration",
  event_other: "Other event",
  new_lei: "New LEI",
  os_topic_added: "Listing added",
  os_list_added: "Added to a list",
  os_new_listing: "New listing",
  os_topic_removed: "Listing removed",
  os_dataset_added: "New dataset",
  os_removed: "Record removed",
};

const FIELD_WORDS: Record<string, string> = {
  direct_parent_lei: "Direct parent",
  ultimate_parent_lei: "Ultimate parent",
  successors: "Successor",
  legal_name: "Legal name",
  legal_form: "Legal form",
  jurisdiction: "Jurisdiction",
  legal_address_country: "Legal address country",
  hq_address_country: "Headquarters country",
  entity_status: "Entity status",
  registration_status: "LEI registration status",
  direct_exception: "Direct parent exception",
  ultimate_exception: "Ultimate parent exception",
  registered_as: "Register number",
  registered_at: "Register",
  validated_at: "Validated at",
  category: "Category",
  sub_category: "Sub-category",
  conformity_flag: "Conformity flag",
  creation_date: "Creation date",
  expiration_date: "Expiration date",
  expiration_reason: "Expiration reason",
};

export type RowTone = "risk" | "context" | "neutral";

/** The chip a row leads with. An OpenSanctions listing is a finding about the
 *  company and wears the risk tone; a GLEIF change is a fact about the record. */
export function rowChip(row: DailyRow): { label: string; tone: RowTone } {
  const first = row.reasons[0] ?? "";
  const label = REASON_WORDS[first] ?? BAND_LABELS[row.band];
  if (row.source === "opensanctions" && row.band === "headline") return { label, tone: "risk" };
  if (row.band === "headline") return { label, tone: "context" };
  return { label, tone: "neutral" };
}

function valueText(v: unknown): string {
  if (v === null || v === undefined || v === "") return "none";
  if (Array.isArray(v)) {
    if (!v.length) return "none";
    return v
      .map((x) =>
        typeof x === "object" && x !== null
          ? String((x as Record<string, unknown>).lei ?? (x as Record<string, unknown>).name ?? JSON.stringify(x))
          : String(x),
      )
      .join(", ");
  }
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export interface ChangeLine {
  what: string;
  before?: string;
  now?: string;
}

/** Each change on a row as a line the page can print: what, before, now. */
export function changeLines(row: DailyRow, datasetTitles: Record<string, string> = {}): ChangeLine[] {
  const out: ChangeLine[] = [];
  for (const c of row.changes) {
    if (c.field === "corporate_events") {
      const events = Array.isArray(c.new) ? (c.new as Record<string, unknown>[]) : [];
      for (const e of events) {
        const type = String(e.type ?? "event").toLowerCase().replace(/_/g, " ");
        const status = String(e.status ?? "").toLowerCase().replace(/_/g, " ");
        out.push({ what: `Legal entity event recorded: ${type}${status ? ` (${status})` : ""}` });
      }
      continue;
    }
    if (c.field) {
      out.push({
        what: FIELD_WORDS[c.field] ?? c.field.replace(/_/g, " "),
        before: valueText(c.old),
        now: valueText(c.new),
      });
      continue;
    }
    if (c.topics_added?.length) out.push({ what: `OpenSanctions topics added: ${topicList(c.topics_added)}` });
    if (c.topics_removed?.length) out.push({ what: `OpenSanctions topics removed: ${topicList(c.topics_removed)}` });
    if (c.datasets_added?.length) {
      // The list's own name, as OpenSanctions titles it; its code only when
      // the catalogue gave no title (Steve, 27 Nov 2026).
      const names = c.datasets_added.map((d) => datasetTitles[d] ?? d);
      out.push({ what: `${names.length === 1 ? "Added to" : "Added to these lists"}: ${names.join("; ")}` });
    }
  }
  return out;
}

/** The record a row's change is on: GLEIF's page for the LEI, or the
 *  OpenSanctions record it references. */
export function recordLink(row: DailyRow): { href: string; label: string } {
  if (row.source === "opensanctions" && row.ref?.url) {
    return { href: row.ref.url, label: "OpenSanctions record" };
  }
  return { href: `https://search.gleif.org/#/record/${row.lei}`, label: "GLEIF record" };
}

/** Where a row's change came from, and when that publisher stamped it. */
export function sourceLine(row: DailyRow): string {
  if (row.source === "opensanctions") return "OpenSanctions";
  const time = /\d{2}:\d{2}/.exec(row.as_of)?.[0];
  return time ? `GLEIF · ${time} UTC delta` : "GLEIF";
}

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

/** `2026-11-12` → `12 November 2026`. */
export function longDay(day: string): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day);
  if (!m) return day;
  return `${Number(m[3])} ${MONTHS[Number(m[2]) - 1]} ${m[1]}`;
}

/** The day a page address names (`?day=`), when it is well formed. */
export function dayFromSearch(search: string): string | null {
  const day = new URLSearchParams(search).get("day");
  return day && /^\d{4}-\d{2}-\d{2}$/.test(day) ? day : null;
}

/** The day to show: the one asked for if held, else the newest complete day,
 *  else the newest. */
export function chooseDay(index: DailyIndex, asked: string | null, today: string): string | null {
  const days = index.days.map((d) => d.day);
  if (asked && days.includes(asked)) return asked;
  return days.find((d) => d < today) ?? days[0] ?? null;
}

/** The days either side of `day`, in calendar order (older, newer). */
export function neighbours(index: DailyIndex, day: string): { older: string | null; newer: string | null } {
  const days = index.days.map((d) => d.day).sort();
  const i = days.indexOf(day);
  return {
    older: i > 0 ? days[i - 1] : null,
    newer: i >= 0 && i < days.length - 1 ? days[i + 1] : null,
  };
}

/** A count's numbers, from the day's counters: GLEIF and OpenSanctions together. */
export function bandCount(day: DailyDay, band: Band): number {
  return day.counts[band] ?? 0;
}

/** What is counted and not listed: new LEIs and note-only changes (both in
 *  the day's file), renewals with nothing else changed, and sole proprietors. */
export function countedNotListed(day: DailyDay): { key: string; n: number; sentence: string }[] {
  const g = day.sources.gleif ?? {};
  return [
    { key: "new", n: bandCount(day, "new"), sentence: "new LEIs. They are in the day's file." },
    {
      key: "note",
      n: bandCount(day, "note"),
      sentence: "smaller changes, such as a reporting exception or a re-keyed register number. They are in the day's file.",
    },
    { key: "churn", n: g.churn ?? 0, sentence: "renewals with nothing else changed. Counted only." },
    {
      key: "sole_proprietor",
      n: g.sole_proprietor ?? 0,
      sentence: "sole proprietors, left out because the legal name is a person's.",
    },
  ];
}

/** Rows of one band. Within the headline band, OpenSanctions listings come
 *  first: there are few of them and they are the ones a reader acts on. */
export function rowsIn(day: DailyDay, band: Band): DailyRow[] {
  const rows = day.rows.filter((r) => r.band === band);
  if (band !== "headline") return rows;
  return [...rows.filter((r) => r.source === "opensanctions"), ...rows.filter((r) => r.source !== "opensanctions")];
}

/** The counts strip under the day's heading. */
export const STRIP: { band: Band; label: string }[] = [
  { band: "headline", label: "ownership and sanctions" },
  { band: "ceased", label: "ceased" },
  { band: "record", label: "record changed" },
  { band: "new", label: "new LEIs" },
];

/** The reason counts in one band, most frequent first: "Parent changed 31". */
export function reasonBreakdown(rows: DailyRow[]): { reason: string; label: string; n: number }[] {
  const counts = new Map<string, number>();
  for (const r of rows) for (const reason of r.reasons) counts.set(reason, (counts.get(reason) ?? 0) + 1);
  return [...counts.entries()]
    .map(([reason, n]) => ({ reason, label: REASON_WORDS[reason] ?? reason, n }))
    .sort((a, b) => b.n - a.n || a.label.localeCompare(b.label));
}

/** Absolute addresses of a day's files, for the links and the curl lines. */
export function fileUrls(day: string, origin: string): { bods: string; csv: string; json: string; latest: string } {
  const base = BASE_URL || origin;
  return {
    bods: `${base}/changes/${day}.jsonl.gz`,
    csv: `${base}/changes/${day}.csv`,
    json: `${base}/daily-changes/${day}.json?band=all`,
    latest: `${base}/changes/latest.jsonl.gz`,
  };
}

/** `1234567` → `1.2 MB`. */
export function formatBytes(n: number | null | undefined): string | null {
  if (n === null || n === undefined) return null;
  if (n < 1024) return `${n} bytes`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(0)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

export function formatCount(n: number): string {
  return n.toLocaleString("en-GB");
}
