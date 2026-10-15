/**
 * The rows behind a sanctions-ownership chip (Phase 342).
 *
 * `SANCTIONS_OWNERSHIP_TEST` (risk) and `SANCTIONS_OWNERSHIP_TEST_POSSIBLE`
 * (context) carry the whole computation in `evidence.regimes`: one entry per
 * regime (OFAC, EU, UK), each with one row per source that has a designated
 * holder. This turns them into table rows, side by side, never summed across
 * sources — the backend (`opencheck/sanctions_ownership.py`) does the
 * arithmetic and this file only reads it. Pure, so the shape is pinned by
 * the logic-only suite.
 */

import type { RiskSignal } from "./api";

export const SANCTIONS_OWNERSHIP_CODES = new Set([
  "SANCTIONS_OWNERSHIP_TEST",
  "SANCTIONS_OWNERSHIP_TEST_POSSIBLE",
]);

/** Mirrors `RESULT_WORDS` in `backend/opencheck/sanctions_ownership.py`. */
export const RESULT_WORDS: Record<string, string> = {
  meets: "Meets the ownership test",
  may_meet: "May meet the ownership test",
  not_met: "Does not meet the ownership test",
  no_designated_holder: "No party designated under this regime holds shares",
};

export interface SanctionsOwnershipRow {
  key: string;
  regime: string;
  rule: string;
  result: string;
  resultWords: string;
  ifConfirmed: boolean;
  holding: string;
  holders: string;
  source: string;
}

type Obj = Record<string, unknown>;
const obj = (v: unknown): Obj => (v && typeof v === "object" ? (v as Obj) : {});
const str = (v: unknown): string => (typeof v === "string" ? v : "");
const arr = (v: unknown): unknown[] => (Array.isArray(v) ? v : []);

function holderWords(h: Obj): string {
  const name = str(h.name);
  const stake = str(h.stake);
  if (h.designated === true) return `${name} ${stake}`.trim();
  const by = arr(h.held_by)
    .map((b) => `${str(obj(b).name)} ${str(obj(b).stake)}`.trim())
    .filter(Boolean)
    .join(", ");
  return `${name} ${stake} (caught company${by ? `: held by ${by}` : ""})`.trim();
}

/** The run's sanctions-ownership signal, if any. */
export function sanctionsOwnershipSignal(signals: RiskSignal[] | undefined): RiskSignal | null {
  return (signals ?? []).find((s) => SANCTIONS_OWNERSHIP_CODES.has(s.code)) ?? null;
}

/** Rows for the table; `[]` for any other signal. */
export function sanctionsOwnershipRows(signal: RiskSignal | null): SanctionsOwnershipRow[] {
  if (!signal || !SANCTIONS_OWNERSHIP_CODES.has(signal.code)) return [];
  const rows: SanctionsOwnershipRow[] = [];
  for (const raw of arr(obj(signal.evidence).regimes)) {
    const r = obj(raw);
    const regime = str(r.label) || str(r.regime);
    const rule = str(r.rule);
    const sources = arr(r.sources);
    if (sources.length === 0) {
      const result = "no_designated_holder";
      rows.push({
        key: `${regime}:none`, regime, rule, result, resultWords: RESULT_WORDS[result],
        ifConfirmed: false, holding: "", holders: "", source: "",
      });
      continue;
    }
    sources.forEach((s, i) => {
      const row = obj(s);
      const result = str(row.result);
      rows.push({
        key: `${regime}:${str(row.source_id)}:${i}`,
        regime,
        rule,
        result,
        resultWords: RESULT_WORDS[result] ?? result,
        ifConfirmed: row.if_confirmed === true,
        holding: str(obj(row.figure).text),
        holders: arr(row.holders).map((h) => holderWords(obj(h))).join("; "),
        source: str(row.source),
      });
    });
  }
  return rows;
}

/** The how-it-is-computed and not-computed lines printed under the table. */
export function sanctionsOwnershipNotes(signal: RiskSignal | null): string[] {
  if (!signal || !SANCTIONS_OWNERSHIP_CODES.has(signal.code)) return [];
  const e = obj(signal.evidence);
  return [str(e.description), str(e.not_computed)].filter(Boolean);
}

/** One line for the FullCheck integrated-ownership band: each regime's
 *  result, side by side. Empty when the run has no such signal. */
export function sanctionsOwnershipBandNote(signals: RiskSignal[] | undefined): string {
  const sig = sanctionsOwnershipSignal(signals);
  if (!sig) return "";
  const parts = arr(obj(sig.evidence).regimes).map((raw) => {
    const r = obj(raw);
    const result = str(r.result);
    const words: Record<string, string> = {
      meets: "meets",
      may_meet: "may meet",
      not_met: "not met",
      no_designated_holder: "no designated holder",
    };
    const confirmed = r.if_confirmed === true && result !== "meets" ? " (would meet if a name-only match were confirmed)" : "";
    return `${str(r.label) || str(r.regime)}: ${words[result] ?? result}${confirmed}`;
  });
  return (
    `Sanctions ownership test, computed separately — ${parts.join("; ")}. ` +
    "Not the figures above: the test counts a company caught by the rule at its whole holding, " +
    "not multiplied. Ownership only; see Risk signals for the regimes, rules and holders."
  );
}
