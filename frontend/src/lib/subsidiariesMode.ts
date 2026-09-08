/**
 * The Subsidiaries tab's values layer (Phase 185).
 *
 * Four lists can describe what one company owns — GLEIF Level 2, OECD-UNSD
 * MEIP, EITI's declared list and Global Energy Monitor's ownership graph —
 * and they disagree, because none of them measures the same thing. The tab
 * keeps them apart and says what each measures. What it *does* try to settle
 * is which rows are companies OpenCheck can open: a row carries an LEI when
 * its own source publishes one, and otherwise a name-derived match is offered
 * from the LEI-keyed lists, chipped as a name match and never asserted.
 *
 * Everything here is pure so it runs in the node suite. The comparison key is
 * `compareKey` from `lib/vocab.ts`, the same one the EITI card used.
 */

import type { DeclaredSource, DeclaredSourceId, SubsidiaryChild } from "./api";
import { compareKey } from "./vocab";

export type ListId = DeclaredSourceId | "gleif";

/** The Subsidiaries tab of a company's report. */
export function subsidiaryHref(lei: string): string {
  return `/?lei=${encodeURIComponent(lei)}&mode=subsidiaries`;
}

export const LIST_LABEL: Record<ListId, string> = {
  gleif: "GLEIF Level 2",
  meip: "OECD-UNSD MEIP",
  eiti_assessment: "EITI",
  climatetrace: "Global Energy Monitor",
};

/** One row of any list, after resolution. */
export interface ResolvedRow {
  name: string;
  /** The LEI this row's own source publishes, if any. */
  lei: string | null;
  /** An LEI another list holds for the same name — a name match, never an
   *  identifier. `from` names which list supplied it. */
  matched: { lei: string; from: ListId } | null;
  country: string | null;
  relation: string | null;
  percent: number | null;
  years: string[];
  via: string | null;
  /** The other lists that carry this name (by comparison key). */
  alsoIn: ListId[];
}

export interface ResolvedList {
  id: ListId;
  label: string;
  rows: ResolvedRow[];
  /** Rows whose own source publishes an LEI. */
  withLei: number;
  /** Rows that gained a name-derived LEI from another list. */
  matched: number;
  /** Rows OpenCheck can open — either of the above. */
  openable: number;
}

export interface Coverage {
  lists: ResolvedList[];
  /** Distinct comparison keys across every list — the union. */
  distinctNames: number;
  /** Keys that appear in two or more lists. */
  agreedNames: number;
  /** Rows across every list that can be opened in OpenCheck. */
  openable: number;
  listed: number;
}

function keyIndex(rows: { name: string; lei: string | null }[]): Map<string, string | null> {
  const out = new Map<string, string | null>();
  for (const r of rows) {
    const k = compareKey(r.name);
    if (!k) continue;
    // Keep an LEI if any row with this key carries one.
    if (!out.has(k) || (!out.get(k) && r.lei)) out.set(k, r.lei ?? null);
  }
  return out;
}

/**
 * Resolve every list against every other. `gleif` may be null when the GLEIF
 * network has not been fetched (or was refused) — then no name match is
 * drawn from it, and it is absent from `lists` rather than shown empty, so a
 * failed fetch never reads as "GLEIF holds none of these".
 */
export function resolveLists(
  declared: DeclaredSource[],
  gleif: SubsidiaryChild[] | null,
): Coverage {
  type Raw = { id: ListId; label: string; rows: { name: string; lei: string | null; extra: Partial<ResolvedRow> }[] };
  const raw: Raw[] = [];
  if (gleif) {
    raw.push({
      id: "gleif",
      label: LIST_LABEL.gleif,
      rows: gleif.map((c) => ({
        name: c.name || c.lei,
        lei: c.lei,
        extra: { country: c.jurisdiction, relation: c.relation },
      })),
    });
  }
  for (const src of declared) {
    if (!src.covered || !src.available) continue;
    raw.push({
      id: src.id,
      label: LIST_LABEL[src.id] ?? src.label,
      rows: src.rows.map((r) => ({
        name: r.name,
        lei: r.lei,
        extra: {
          country: r.country,
          relation: r.relation,
          percent: r.percent,
          years: r.years,
          via: r.via,
        },
      })),
    });
  }

  const indexes = new Map<ListId, Map<string, string | null>>();
  for (const list of raw) indexes.set(list.id, keyIndex(list.rows));

  const lists: ResolvedList[] = raw.map((list) => {
    const rows: ResolvedRow[] = list.rows.map((r) => {
      const k = compareKey(r.name);
      const alsoIn: ListId[] = [];
      let matched: ResolvedRow["matched"] = null;
      for (const other of raw) {
        if (other.id === list.id || !k) continue;
        const idx = indexes.get(other.id)!;
        if (!idx.has(k)) continue;
        alsoIn.push(other.id);
        const lei = idx.get(k);
        if (!r.lei && !matched && lei) matched = { lei, from: other.id };
      }
      return {
        name: r.name,
        lei: r.lei,
        matched,
        country: r.extra.country ?? null,
        relation: r.extra.relation ?? null,
        percent: r.extra.percent ?? null,
        years: r.extra.years ?? [],
        via: r.extra.via ?? null,
        alsoIn,
      };
    });
    const withLei = rows.filter((r) => r.lei).length;
    const matchedN = rows.filter((r) => !r.lei && r.matched).length;
    return { id: list.id, label: list.label, rows, withLei, matched: matchedN, openable: withLei + matchedN };
  });

  const seen = new Map<string, number>();
  for (const idx of indexes.values()) for (const k of idx.keys()) seen.set(k, (seen.get(k) ?? 0) + 1);
  const agreedNames = [...seen.values()].filter((n) => n >= 2).length;

  return {
    lists,
    distinctNames: seen.size,
    agreedNames,
    openable: lists.reduce((n, l) => n + l.openable, 0),
    listed: lists.reduce((n, l) => n + l.rows.length, 0),
  };
}

/** Rows a reader should see first: those OpenCheck can open, then by name. */
export function orderRows(rows: ResolvedRow[]): ResolvedRow[] {
  return [...rows].sort((a, b) => {
    const oa = a.lei || a.matched ? 0 : 1;
    const ob = b.lei || b.matched ? 0 : 1;
    if (oa !== ob) return oa - ob;
    if ((b.percent ?? -1) !== (a.percent ?? -1)) return (b.percent ?? -1) - (a.percent ?? -1);
    return a.name.localeCompare(b.name);
  });
}

/**
 * The tab's opening sentence. It is the advocacy point, so it is built from
 * the numbers rather than asserted: how many sources answered, how many names
 * that came to, and how few of those names any two sources share.
 */
export function coverageSentence(cov: Coverage, name: string): string {
  const n = cov.lists.length;
  if (n === 0) {
    return `None of the sources checked publishes a subsidiary list for ${name}. That is an absence of records, not a finding that it owns nothing.`;
  }
  // Covered but empty — a leaf company three registers know and none records
  // as owning anything (A/S Norske Shell, found on production). "Three
  // sources list what it owns — 0 names across 0 rows" is arithmetic, not a
  // sentence, and the disagreement clause that follows has nothing to be
  // about. Say the absence in the same voice as the presence.
  if (cov.listed === 0) {
    const covers = n === 1 ? "One source covers" : `${numberWord(n)} sources cover`;
    const lists = n === 1 ? "it lists nothing" : "none of them lists anything";
    return `${covers} ${name}, and ${lists} it owns. That is an absence of records, not a finding that it owns nothing.`;
  }
  const sources = n === 1 ? "One source lists" : `${numberWord(n)} sources list`;
  const opening = `${sources} what ${name} owns — ${cov.distinctNames.toLocaleString()} distinct ${cov.distinctNames === 1 ? "name" : "names"} across ${cov.listed.toLocaleString()} ${cov.listed === 1 ? "row" : "rows"}`;
  if (n === 1) {
    return `${opening}. No second source is available to compare it against.`;
  }
  const agree =
    cov.agreedNames === 0
      ? "no name appears in more than one of them"
      : `only ${cov.agreedNames.toLocaleString()} ${cov.agreedNames === 1 ? "name appears" : "names appear"} in more than one of them`;
  return `${opening}, and ${agree}. That is not because any list is wrong: each measures something different, and no public source publishes the whole picture.`;
}

/** Which rows can be opened, as one sentence for the coverage strip. */
export function openableSentence(cov: Coverage): string {
  if (cov.listed === 0) return "";
  if (cov.openable === 0) {
    return "None of these rows carries an LEI, so none can be opened in OpenCheck yet.";
  }
  const matched = cov.lists.reduce((n, l) => n + l.matched, 0);
  const base = `${cov.openable.toLocaleString()} of ${cov.listed.toLocaleString()} rows can be opened in OpenCheck`;
  return matched > 0
    ? `${base} — ${matched.toLocaleString()} of them by a name match to another list, marked as such.`
    : `${base}.`;
}

function numberWord(n: number): string {
  return ["Zero", "One", "Two", "Three", "Four", "Five"][n] ?? String(n);
}
