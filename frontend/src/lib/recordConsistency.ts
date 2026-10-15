/**
 * The per-field rows behind a `RECORD_CONSISTENCY` signal (Phase 337).
 *
 * The backend's `evidence.items` is the list of comparisons that passed the
 * measured gate (`consistency_allowlist.py`), each with the two sources, the
 * two values as compared and — `said` — the two values as the register wrote
 * them ("ACTIVE", "dissolved (since 3 Apr 2019)"). This turns them into rows
 * a table can render, with the field named in the words the subject card
 * already uses. Pure, so the shape is pinned by the logic-only suite.
 *
 * Nothing here scopes to a graph node: the decision on the plan is panel and
 * export only, and `signalScope.ts` stays `RELATED_*`-only.
 */

import type { RiskSignal } from "./api";

/** What each compared field is called. Mirrors `FIELD_NOUNS` in
 *  `backend/opencheck/consistency.py` (a backend test parses this file). */
export const FIELD_LABELS: Record<string, string> = {
  liveness: "Register status",
  jurisdiction: "Jurisdiction",
  founding_date: "Incorporation date",
  identifier_clash: "Register number",
};

export interface RecordConsistencyRow {
  key: string;
  field: string;
  fieldLabel: string;
  relation: "agree" | "disagree";
  sourceA: string;
  sourceB: string;
  valueA: string;
  valueB: string;
}

function asString(v: unknown): string {
  if (v === null || v === undefined) return "—";
  return String(v);
}

/** Rows for the table; `[]` for any other signal or an evidence with none. */
export function recordConsistencyRows(signal: RiskSignal): RecordConsistencyRow[] {
  if (signal.code !== "RECORD_CONSISTENCY") return [];
  const e = (signal.evidence ?? {}) as Record<string, unknown>;
  const items = Array.isArray(e.items) ? e.items : [];
  const rows: RecordConsistencyRow[] = [];
  items.forEach((raw, i) => {
    if (!raw || typeof raw !== "object") return;
    const item = raw as Record<string, unknown>;
    const field = typeof item.field === "string" ? item.field : "";
    const relation = item.relation === "disagree" ? "disagree" : item.relation === "agree" ? "agree" : null;
    const sources = Array.isArray(item.sources) ? item.sources : [];
    if (!field || !relation || sources.length !== 2) return;
    const said = Array.isArray(item.said) && item.said.length === 2 ? item.said : null;
    const values = Array.isArray(item.values) && item.values.length === 2 ? item.values : ["", ""];
    const [va, vb] = said ?? values;
    rows.push({
      key: `${field}:${i}`,
      field,
      fieldLabel: FIELD_LABELS[field] ?? field.replace(/_/g, " "),
      relation,
      sourceA: String(sources[0]),
      sourceB: String(sources[1]),
      valueA: asString(va),
      valueB: asString(vb),
    });
  });
  // Disagreements first — they are what the sentence named — then agreements
  // in field order, so the table reads the way the sentence does.
  const order = Object.keys(FIELD_LABELS);
  return rows.sort((a, b) => {
    if (a.relation !== b.relation) return a.relation === "disagree" ? -1 : 1;
    return order.indexOf(a.field) - order.indexOf(b.field);
  });
}

/**
 * The fields the signal says the sources agree on / differ on, as the
 * backend listed them — for callers that want the words without the rows.
 */
export function recordConsistencyFields(signal: RiskSignal): {
  agreed: string[];
  disagreed: string[];
} {
  const e = (signal.evidence ?? {}) as Record<string, unknown>;
  const pick = (k: string) =>
    Array.isArray(e[k]) ? (e[k] as unknown[]).filter((x): x is string => typeof x === "string") : [];
  return { agreed: pick("agreed_fields"), disagreed: pick("disagreed_fields") };
}
