/**
 * integratedOwnership — how much of the subject each party above it holds
 * (Phase 339), for the FullCheck "Integrated ownership" band.
 *
 * The mirror of `backend/opencheck/integrated_ownership.py`. The lookup
 * freezes the backend's payload into the `integrated_ownership` event; this
 * file recomputes it in the browser as FullCheck expands the network, so the
 * band grows with the graph. Both run the case table
 * `integratedOwnershipCases.json`; `tests/test_frontend_parity.py` compares
 * the excluded-source lists and the status words.
 *
 * The rules, each Stephen's decision on the ticket (15 Oct 2026):
 * shareholding interests only (a voting-rights path is listed, without a
 * figure); one figure per source, never added across sources; GLEIF and MEIP
 * are not ownership; an ended holding is "formerly", with no figure; no
 * threshold — context, never a signal.
 */

import { refIndex, resolveRef } from "./bodsRefs";
import { interestEnded, recordClosed } from "./relationshipStatus";
import { REASON_WORDS, figureText, integratedOwnership, shareBounds, type Edge } from "./ownershipMath";

type Stmt = Record<string, unknown>;

export const METHOD = "integrated_ownership";
export const METHOD_URL = "https://docs.trank.no/white_papers";

/** The plain-words description the page prints beside the figures — the
 *  backend's `METHOD_DESCRIPTION`, word for word (parity-tested). */
export const METHOD_DESCRIPTION =
  "How much of the company each party holds, directly and through the " +
  "companies it holds: shares are multiplied along each path and added " +
  "across paths, and a company's own shares are given back to its other " +
  "holders. Each source is computed on its own; figures from two sources " +
  "are listed side by side, never added. Shareholdings only — voting " +
  "rights, roles and control are not shares. For a person it is arithmetic " +
  "on what was filed, not a determination of beneficial ownership. Context, " +
  "not a finding.";
export const METHOD_CREDIT =
  "Method: integrated ownership as published by T-Rank (https://docs.trank.no/white_papers).";

/** GLEIF Level 2 is accounting consolidation; MEIP is group membership. */
export const EXCLUDED_SOURCES = new Set(["gleif", "bods_gleif", "meip"]);
export const EXCLUDED_DESCRIPTIONS = new Set([
  "GLEIF",
  "GLEIF — Global LEI Foundation (BODS bulk dataset)",
  "OECD-UNSD Multinational Enterprise Information Platform (MEIP)",
  "OECD-UNSD Multinational Enterprise Information Platform (MEIP), 'Group Register' sheet.",
]);

export const SHAREHOLDING = "shareholding";
export const VOTING_RIGHTS = "votingRights";

export type HolderStatus = "current" | "voting_rights_only" | "formerly";

export const STATUS_WORDS: Record<Exclude<HolderStatus, "current">, string> = {
  voting_rights_only: "Voting rights, not shares, on this path",
  formerly: "Formerly — every path to the company has ended",
};

export interface Figure {
  source_id: string;
  source: string;
  minimum?: number;
  maximum?: number;
  exact?: number;
  incomplete?: boolean;
  reason?: string;
  via?: string[];
  direct: boolean;
  via_names: string[];
  text: string;
}

export interface Holder {
  party: string;
  name: string;
  kind: "person" | "entity";
  figures: Figure[];
  status: HolderStatus;
  sources: string[];
  note?: string;
}

export interface IntegratedOwnershipPayload {
  method: string;
  as_of: string;
  description?: string;
  credit?: string;
  holders: Holder[];
  subject_ids?: string[];
}

const SUBJECT = "\u0000subject";

const rd = (s: Stmt): Stmt => (s.recordDetails ?? {}) as Stmt;

export function sourceKey(s: Stmt): [string, string] {
  const src = (s.source ?? {}) as Stmt;
  const desc = String(src.description ?? "").trim();
  const sid = String(src.opencheckSourceId ?? "").trim();
  return [sid || desc, desc || sid];
}

function excluded(s: Stmt): boolean {
  const src = (s.source ?? {}) as Stmt;
  return (
    EXCLUDED_SOURCES.has(String(src.opencheckSourceId ?? "").trim()) ||
    EXCLUDED_DESCRIPTIONS.has(String(src.description ?? "").trim())
  );
}

export function partyName(s: Stmt | undefined): string {
  if (!s) return "";
  const d = rd(s);
  if (typeof d.name === "string" && d.name.trim()) return d.name.trim();
  for (const n of (d.names ?? []) as Stmt[]) {
    const full = String(n?.fullName ?? "").trim();
    if (full) return full;
  }
  return "";
}

type EdgeKind = ["share", [number, number | null] | null] | ["voting", null] | ["ended", null] | null;

function edgeKind(s: Stmt, asOf: string): EdgeKind {
  const closed = recordClosed(s);
  const kinds = ((rd(s).interests ?? []) as Stmt[]).filter(
    (i) => i && (i.type === SHAREHOLDING || i.type === VOTING_RIGHTS),
  );
  if (kinds.length === 0) return null;
  const currentShare = kinds.filter((i) => i.type === SHAREHOLDING && !interestEnded(i, closed, asOf));
  if (currentShare.length) {
    let lo: number | null = null;
    let hi: number | null = null;
    let stated = false;
    let unstated = false;
    for (const i of currentShare) {
      const b = shareBounds(i.share);
      if (b === null) {
        unstated = true;
        continue;
      }
      stated = true;
      lo = lo === null ? b[0] : Math.max(lo, b[0]);
      if (b[1] === null) unstated = true;
      else hi = hi === null ? b[1] : Math.max(hi, b[1]);
    }
    if (!stated) return ["share", null];
    return ["share", [lo ?? 0, unstated ? null : hi]];
  }
  if (kinds.some((i) => i.type === VOTING_RIGHTS && !interestEnded(i, closed, asOf))) return ["voting", null];
  return ["ended", null];
}

function upstream(edges: [string, string][], target: string): Set<string> {
  const owners = new Map<string, Set<string>>();
  for (const [a, b] of edges) (owners.get(b) ?? owners.set(b, new Set()).get(b)!).add(a);
  const seen = new Set<string>();
  const stack = [target];
  while (stack.length) {
    const n = stack.pop()!;
    for (const a of owners.get(n) ?? []) {
      if (!seen.has(a)) {
        seen.add(a);
        stack.push(a);
      }
    }
  }
  seen.delete(target);
  return seen;
}

const byCodePoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

export function figureWords(fig: { minimum?: number; maximum?: number; reason?: string }): string {
  const text = figureText(fig);
  if (text) return text;
  if (fig.reason && REASON_WORDS[fig.reason]) return `No figure — ${REASON_WORDS[fig.reason]}`;
  return "No figure";
}

/** The route in words: directly, through A and B, or both. */
export function routeWords(fig: { direct?: boolean; via_names?: string[] }): string {
  const names = (fig.via_names ?? []).filter(Boolean);
  if (!names.length) return fig.direct ? "directly" : "";
  const through = `through ${names.join(", ")}`;
  return fig.direct ? `directly and ${through}` : through;
}

/** The payload, as `integrated_ownership.compute` builds it. `canon` maps a
 *  statementId to its reconciled node (FullCheck's `reconcileBods` remap). */
export function computeIntegratedOwnership(
  statements: Stmt[],
  subjectIds: Iterable<string>,
  asOf: string,
  canon: Record<string, string> = {},
): IntegratedOwnershipPayload {
  const c = (x: string) => canon[x] ?? x;
  const index = refIndex(statements);
  const subjects = new Set([...subjectIds].filter(Boolean).map(c));
  const byCanon = new Map<string, Stmt>();
  for (const s of statements) {
    if ((s.recordType === "entity" || s.recordType === "person") && typeof s.statementId === "string") {
      const cid = c(s.statementId);
      const prev = byCanon.get(cid);
      if (!prev || (!partyName(prev) && partyName(s))) byCanon.set(cid, s);
    }
  }
  const node = (raw: unknown): string => {
    const sid = resolveRef(raw, index);
    if (!sid) return "";
    const n = c(sid);
    return subjects.has(n) ? SUBJECT : n;
  };

  const share = new Map<string, Edge[]>();
  const voting = new Map<string, [string, string][]>();
  const ended = new Map<string, [string, string][]>();
  const labels = new Map<string, string>();
  const push = <T,>(m: Map<string, T[]>, k: string, v: T) => (m.get(k) ?? m.set(k, []).get(k)!).push(v);
  for (const s of statements) {
    if (s.recordType !== "relationship" || excluded(s)) continue;
    const d = rd(s);
    const owned = node(d.subject);
    const owner = node(d.interestedParty);
    if (!owned || !owner) continue;
    const kind = edgeKind(s, asOf);
    if (!kind) continue;
    const [k, label] = sourceKey(s);
    if (!labels.has(k)) labels.set(k, label);
    if (kind[0] === "share") {
      const b = kind[1];
      push(share, k, b === null ? { owner, owned, low: 0, high: null } : { owner, owned, low: b[0], high: b[1] });
    } else if (kind[0] === "voting") push(voting, k, [owner, owned] as [string, string]);
    else push(ended, k, [owner, owned] as [string, string]);
  }

  type Work = Holder & { _voting: string[]; _ended: string[] };
  const holders = new Map<string, Work>();
  const holder = (pid: string): Work => {
    let h = holders.get(pid);
    if (!h) {
      const st = byCanon.get(pid);
      h = {
        party: pid,
        name: partyName(st) || pid,
        kind: st?.recordType === "person" ? "person" : "entity",
        figures: [],
        status: "current",
        sources: [],
        _voting: [],
        _ended: [],
      };
      holders.set(pid, h);
    }
    return h;
  };

  const keys = [...new Set([...share.keys(), ...voting.keys(), ...ended.keys()])].sort(byCodePoint);
  for (const k of keys) {
    const sedges = share.get(k) ?? [];
    const spairs = sedges.map((e) => [e.owner, e.owned] as [string, string]);
    const v = voting.get(k) ?? [];
    const withShares = upstream(spairs, SUBJECT);
    const withVotes = upstream([...spairs, ...v], SUBJECT);
    const withEnded = upstream([...spairs, ...v, ...(ended.get(k) ?? [])], SUBJECT);
    for (const pid of [...withShares].sort(byCodePoint)) {
      const r = integratedOwnership(sedges, [pid], SUBJECT);
      const via = (r.via ?? []).filter((x) => x !== SUBJECT);
      const fig: Figure = {
        source_id: k,
        source: labels.get(k)!,
        ...(r.minimum !== undefined ? { minimum: r.minimum } : {}),
        ...(r.maximum !== undefined ? { maximum: r.maximum } : {}),
        ...(r.exact !== undefined ? { exact: r.exact } : {}),
        ...(r.incomplete ? { incomplete: true } : {}),
        ...(r.reason ? { reason: r.reason } : {}),
        ...(via.length ? { via } : {}),
        direct: sedges.some((e) => e.owner === pid && e.owned === SUBJECT),
        via_names: via.map((x) => partyName(byCanon.get(x)) || x),
        text: "",
      };
      fig.text = figureWords(fig);
      holder(pid).figures.push(fig);
    }
    for (const pid of [...withVotes].filter((p) => !withShares.has(p)).sort(byCodePoint)) holder(pid)._voting.push(k);
    for (const pid of [...withEnded].filter((p) => !withVotes.has(p)).sort(byCodePoint)) holder(pid)._ended.push(k);
  }

  const out: Holder[] = [];
  for (const h of holders.values()) {
    const { _voting, _ended, ...rest } = h;
    const row: Holder = rest;
    if (row.figures.length) {
      row.status = "current";
      row.figures.sort(
        (a, b) => -(a.minimum ?? 0) + (b.minimum ?? 0) || byCodePoint(a.source, b.source) || byCodePoint(a.source_id, b.source_id),
      );
      row.sources = row.figures.map((f) => f.source);
    } else if (_voting.length) {
      row.status = "voting_rights_only";
      row.sources = [...new Set(_voting.map((k) => labels.get(k)!))].sort(byCodePoint);
    } else {
      row.status = "formerly";
      row.sources = [...new Set(_ended.map((k) => labels.get(k)!))].sort(byCodePoint);
    }
    if (row.status !== "current") row.note = STATUS_WORDS[row.status];
    out.push(row);
  }
  const rank: Record<HolderStatus, number> = { current: 0, voting_rights_only: 1, formerly: 2 };
  const top = (h: Holder) => (h.figures.length ? -Math.max(...h.figures.map((f) => f.minimum ?? 0)) : 0);
  out.sort(
    (a, b) =>
      rank[a.status] - rank[b.status] ||
      top(a) - top(b) ||
      byCodePoint(a.name.toLowerCase(), b.name.toLowerCase()) ||
      byCodePoint(a.party, b.party),
  );
  return { method: METHOD, as_of: asOf, description: METHOD_DESCRIPTION, credit: METHOD_CREDIT, holders: out };
}

/** The subject's statementIds in a network: every entity statement carrying
 *  the looked-up LEI (one per source before reconciliation). */
export function subjectIdsFor(statements: Stmt[], lei: string, leiOf: (s: Stmt) => string | null): string[] {
  const want = lei.trim().toUpperCase();
  return statements
    .filter((s) => s.recordType === "entity" && typeof s.statementId === "string" && leiOf(s)?.toUpperCase() === want)
    .map((s) => s.statementId as string);
}

/** One holder in a few words for a tree row: "24% (OpenSanctions); 50.1%
 *  (Wikidata)", or the status note for a holder with no figure. */
export function ownershipCellText(h: Holder): string {
  if (h.status !== "current") return h.status === "formerly" ? "formerly held shares" : "voting rights, not shares";
  return h.figures.map((f) => `${f.text} (${f.source})`).join("; ");
}

/** Node id → the tree-row text, for every holder in a payload. */
export function ownershipByNode(payload: IntegratedOwnershipPayload | null | undefined): Map<string, string> {
  const out = new Map<string, string>();
  for (const h of payload?.holders ?? []) out.set(h.party, ownershipCellText(h));
  return out;
}
