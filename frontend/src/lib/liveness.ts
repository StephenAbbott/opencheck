/**
 * liveness — read a register's view of whether an entity is still live.
 *
 * Phase 151 made every adapter write the register's status through one path
 * (`backend/opencheck/bods/liveness.py`): `recordDetails.dissolutionDate` only
 * for a real date, and a `commenting` annotation on `/recordDetails` whose
 * description follows a fixed grammar —
 *
 *   "<Source> records this entity as <active | in a terminal process |
 *    dissolved>[ since <YYYY-MM-DD>][ — register status: “<raw>”]."
 *
 * This is the browser-side reader. It mirrors `read_register_status`
 * exactly: writer and reader share one grammar, pinned by tests on both
 * sides, so the class is read back without a second copy of every register's
 * vocabulary. A bare `dissolutionDate` with no annotation (a bulk dataset, or
 * a payload cached before Phase 151) reads as `terminal`.
 */

import { annotationsAt, type BODSAnnotation } from "./annotations";

export type LivenessClass = "live" | "pending" | "terminal";

export interface RegisterStatus {
  /** The source label as the mapper stamped it (`source.description`). */
  source: string;
  liveness: LivenessClass;
  /** ISO date the status took effect, when the register gave one. */
  since: string | null;
  /** The register's own status label, verbatim. */
  raw: string | null;
}

const WORD_CLASS: Record<string, LivenessClass> = {
  active: "live",
  "in a terminal process": "pending",
  dissolved: "terminal",
};

const DESCRIPTION_RE =
  /^([\s\S]+?) records this entity as (active|in a terminal process|dissolved)(?: since (\d{4}-\d{2}-\d{2}))?(?: — register status: “([\s\S]*)”)?\.$/;

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

function isStatusAnnotation(a: BODSAnnotation): boolean {
  return a.motivation === "commenting" && DESCRIPTION_RE.test(a.description ?? "");
}

export function readRegisterStatus(stmt: unknown): RegisterStatus | null {
  if (stmt == null || typeof stmt !== "object") return null;
  for (const a of annotationsAt(stmt, "recordDetails")) {
    if (!isStatusAnnotation(a)) continue;
    const m = DESCRIPTION_RE.exec(a.description ?? "")!;
    return {
      source: m[1],
      liveness: WORD_CLASS[m[2]],
      since: m[3] ?? null,
      raw: m[4] ?? null,
    };
  }
  const rd = (stmt as Record<string, unknown>).recordDetails as Record<string, unknown> | undefined;
  const dissolution = rd?.dissolutionDate;
  if (typeof dissolution === "string" && ISO_DATE.test(dissolution)) {
    const src = (stmt as Record<string, unknown>).source as Record<string, unknown> | undefined;
    return {
      source: String(src?.description ?? ""),
      liveness: "terminal",
      since: dissolution,
      raw: null,
    };
  }
  return null;
}

/** Short label for a status row: what the register said, in its own words
 *  where it gave them. Live statuses are not labelled — absence of a terminal
 *  status is not a finding, and "active" on every card would be noise. */
export function registerStatusLabel(status: RegisterStatus): string | null {
  if (status.liveness === "live") return null;
  const head = status.liveness === "terminal" ? "Dissolved" : "Terminal process under way";
  const parts = [head];
  if (status.raw && status.raw.toLowerCase() !== head.toLowerCase()) parts.push(`“${status.raw}”`);
  if (status.since) parts.push(`since ${status.since}`);
  return parts.join(" · ");
}
