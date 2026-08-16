/**
 * signalScope — which BODS statements does a risk signal belong to, and
 * which signals belong on *this* bundle's graph.
 *
 * Two things live here that used to live apart, and drifting apart was the
 * bug (Phase 109):
 *
 * 1. `signalStatementIds()` — the evidence→statementId mapping. It was
 *    inline in `BODSGraph.tsx`'s `buildSignalMap()`, which also imports
 *    Cytoscape, so nothing else could reuse or cheaply test it.
 * 2. `scopeCrossSourceSignals()` — the filter that decides which of the
 *    lookup's top-level signals may badge a node in a *per-source* bundle.
 *
 * If those two read `evidence` differently, the filter silently drops
 * signals the graph would have badged — the same class of failure as the
 * one this module exists to fix. So the filter is built on the mapping,
 * and `BODSGraph` imports `buildSignalMap` from here rather than defining
 * its own.
 *
 * Why the filter is needed at all: `RELATED_*` signals are computed late in
 * the backend's `_lookup_pipeline` against the **merged** bundle and ride on
 * the top-level `risk_signals` event. A source card's graph is fed a
 * `DeepenResponse`, which carries only that source's own findings — so a
 * cross-source signal structurally could not reach it, and a node the risk
 * panel calls sanctions-linked rendered unbadged, i.e. as if checked and
 * clean. In a due-diligence tool that is the wrong inference to invite.
 */

import type { RiskSignal } from "./api";

/** A BODS statement, as it arrives over the wire. */
type Stmt = Record<string, unknown>;

/**
 * Every BODS statementId this signal's evidence points at.
 *
 * The five shapes (documented in CLAUDE.md, "Signal→BODS node mapping"):
 *   - `evidence.statement_id`            — SANCTIONED, PEP
 *   - `evidence.subject_statement_id`    — RELATED_SANCTIONED, RELATED_PEP, …
 *   - `evidence.matches[].statement_id`  — TRUST_OR_ARRANGEMENT, NOMINEE, AMLA
 *   - `evidence.jurisdictions[].statement_id` — FATF_*, NON_EU_JURISDICTION
 *   - `evidence.longest_path[]`          — COMPLEX_OWNERSHIP_LAYERS
 *
 * Order is not meaningful and duplicates are not removed: callers either
 * build a multimap (`buildSignalMap`) or test for intersection.
 */
export function signalStatementIds(sig: RiskSignal): string[] {
  const ev = (sig.evidence ?? {}) as Record<string, unknown>;
  const ids: string[] = [];

  if (typeof ev.statement_id === "string") ids.push(ev.statement_id);
  if (typeof ev.subject_statement_id === "string") ids.push(ev.subject_statement_id);

  for (const key of ["matches", "jurisdictions"] as const) {
    const arr = ev[key];
    if (!Array.isArray(arr)) continue;
    for (const item of arr) {
      if (item && typeof item === "object") {
        const id = (item as Record<string, unknown>).statement_id;
        if (typeof id === "string") ids.push(id);
      }
    }
  }

  if (Array.isArray(ev.longest_path)) {
    for (const id of ev.longest_path) {
      if (typeof id === "string") ids.push(id);
    }
  }

  return ids.filter(Boolean);
}

/**
 * Build a map from BODS statementId → RiskSignal[] from each signal's evidence.
 *
 * Moved here from BODSGraph.tsx (Phase 109) so the badge machinery and the
 * scoping filter cannot disagree about what a signal's evidence points at.
 */
export function buildSignalMap(signals: RiskSignal[]): Map<string, RiskSignal[]> {
  const map = new Map<string, RiskSignal[]>();
  for (const sig of signals) {
    for (const id of signalStatementIds(sig)) {
      const bucket = map.get(id);
      if (bucket) bucket.push(sig);
      else map.set(id, [sig]);
    }
  }
  return map;
}

/**
 * True for the cross-source screening codes — the ones assessed against the
 * merged bundle rather than produced by a single source's own `/deepen`.
 *
 * Deliberately a prefix test rather than an allow-list: `RELATED_PEP`,
 * `RELATED_SANCTIONED`, `RELATED_SANCTIONS_LINKED`,
 * `RELATED_SANCTIONS_CONTROLLED`, `RELATED_COUNTER_SANCTIONED` and
 * `RELATED_DEBARMENT` all qualify today, and a new `RELATED_*` code should
 * be included the day it is added, not the day someone remembers this list.
 */
export function isCrossSourceSignal(sig: RiskSignal): boolean {
  return typeof sig.code === "string" && sig.code.startsWith("RELATED_");
}

/** The `statementId`s present in a BODS bundle. */
export function statementIdsIn(statements: Stmt[]): Set<string> {
  const ids = new Set<string>();
  for (const stmt of statements ?? []) {
    const id = (stmt as Record<string, unknown>)?.statementId;
    if (typeof id === "string" && id) ids.add(id);
  }
  return ids;
}

/**
 * The cross-source (`RELATED_*`) signals that belong on a graph rendering
 * `statements` — i.e. those whose evidence lands on a statement this bundle
 * actually contains.
 *
 * Scoped narrowly on purpose. Subject-level codes are NOT included, and that
 * is a decision rather than an omission: `COMPLEX_OWNERSHIP_LAYERS` carries
 * `evidence.longest_path[]` computed over the merged graph, and a single
 * source's bundle typically holds only a fragment of that path — badging it
 * here would assert a structural claim that is not true of the graph on
 * screen. `FATF_*` / `NON_EU_JURISDICTION` have the same problem via
 * `jurisdictions[]`. `SANCTIONED` / `PEP` already reach a source card on its
 * own `DeepenResponse`. Widening past `RELATED_*` needs its own think.
 *
 * Filtering (rather than passing the whole top-level list and letting
 * non-matching ids fall on the floor) is what makes the intent legible: the
 * signals handed to the graph are exactly the ones that can badge a node.
 */
export function scopeCrossSourceSignals(
  signals: RiskSignal[] | undefined,
  statements: Stmt[] | undefined,
): RiskSignal[] {
  if (!signals?.length || !statements?.length) return [];
  const present = statementIdsIn(statements);
  if (present.size === 0) return [];
  return signals.filter(
    (sig) => isCrossSourceSignal(sig) && signalStatementIds(sig).some((id) => present.has(id)),
  );
}
