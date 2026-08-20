import { describe, it, expect } from "vitest";

import {
  buildSignalMap,
  isCrossSourceSignal,
  scopeCrossSourceSignals,
  signalStatementIds,
  statementIdsIn,
} from "./signalScope";
import type { RiskSignal } from "./api";

type Stmt = Record<string, unknown>;

function stmt(id: string): Stmt {
  return {
    statementId: id,
    recordType: "entity",
    recordDetails: { entityType: { type: "registeredEntity" }, name: id },
  };
}

function signal(code: string, evidence: Record<string, unknown>): RiskSignal {
  return {
    code,
    confidence: "high",
    summary: `${code} summary`,
    source_id: "opensanctions",
    hit_id: "hit-1",
    evidence,
  };
}

// ---------------------------------------------------------------------------
// signalStatementIds — the five evidence shapes documented in CLAUDE.md.
// This is the parity guard: buildSignalMap used to inline this logic inside
// BODSGraph.tsx, and the scoping filter reading `evidence` even slightly
// differently would silently drop signals the graph would have badged.
// ---------------------------------------------------------------------------

describe("signalStatementIds", () => {
  it("reads evidence.statement_id (SANCTIONED, PEP)", () => {
    expect(signalStatementIds(signal("SANCTIONED", { statement_id: "s1" }))).toEqual(["s1"]);
  });

  it("reads evidence.subject_statement_id (RELATED_*)", () => {
    expect(signalStatementIds(signal("RELATED_PEP", { subject_statement_id: "s2" }))).toEqual(["s2"]);
  });

  it("reads evidence.matches[].statement_id (TRUST_OR_ARRANGEMENT, NOMINEE, AMLA)", () => {
    const sig = signal("TRUST_OR_ARRANGEMENT", {
      matches: [{ statement_id: "s3" }, { statement_id: "s4" }, { no_id: true }],
    });
    expect(signalStatementIds(sig)).toEqual(["s3", "s4"]);
  });

  it("reads evidence.jurisdictions[].statement_id (FATF_*, NON_EU_JURISDICTION)", () => {
    const sig = signal("FATF_GREY_LIST", { jurisdictions: [{ statement_id: "s5", code: "SY" }] });
    expect(signalStatementIds(sig)).toEqual(["s5"]);
  });

  it("reads evidence.longest_path[] (COMPLEX_OWNERSHIP_LAYERS)", () => {
    const sig = signal("COMPLEX_OWNERSHIP_LAYERS", { longest_path: ["s6", "s7", 42, "s8"] });
    expect(signalStatementIds(sig)).toEqual(["s6", "s7", "s8"]);
  });

  it("collects every shape a single signal happens to carry", () => {
    const sig = signal("RELATED_SANCTIONED", {
      statement_id: "a",
      subject_statement_id: "b",
      matches: [{ statement_id: "c" }],
      jurisdictions: [{ statement_id: "d" }],
      longest_path: ["e"],
    });
    expect(new Set(signalStatementIds(sig))).toEqual(new Set(["a", "b", "c", "d", "e"]));
  });

  it("survives missing, empty and wrong-typed evidence without throwing", () => {
    expect(signalStatementIds({ ...signal("PEP", {}), evidence: undefined as never })).toEqual([]);
    expect(signalStatementIds(signal("PEP", {}))).toEqual([]);
    expect(signalStatementIds(signal("PEP", { statement_id: 7, matches: "nope" }))).toEqual([]);
  });
});

describe("statementIdsIn", () => {
  it("collects statementIds and ignores malformed entries", () => {
    const ids = statementIdsIn([stmt("s1"), { recordType: "entity" }, { statementId: "" }, stmt("s2")]);
    expect(ids).toEqual(new Set(["s1", "s2"]));
  });
});

describe("isCrossSourceSignal", () => {
  it("is true for every RELATED_* code and false for subject-level ones", () => {
    for (const code of [
      "RELATED_PEP",
      "RELATED_SANCTIONED",
      "RELATED_SANCTIONS_LINKED",
      "RELATED_SANCTIONS_CONTROLLED",
      "RELATED_COUNTER_SANCTIONED",
      "RELATED_DEBARMENT",
      "RELATED_EXPORT_CONTROLLED",
      "RELATED_EXPORT_CONTROL_LINKED",
      "RELATED_EXPORT_RISK",
    ]) {
      expect(isCrossSourceSignal(signal(code, {}))).toBe(true);
    }
    for (const code of ["SANCTIONED", "PEP", "COMPLEX_OWNERSHIP_LAYERS", "OFFSHORE_LEAKS", "EXPORT_CONTROLLED"]) {
      expect(isCrossSourceSignal(signal(code, {}))).toBe(false);
    }
  });
});

// ---------------------------------------------------------------------------
// scopeCrossSourceSignals — the fix itself.
// ---------------------------------------------------------------------------

describe("scopeCrossSourceSignals", () => {
  const bundle = [stmt("in-bundle-1"), stmt("in-bundle-2")];

  it("keeps a RELATED_* signal scoped to a statement in this bundle — and it reaches the badge map", () => {
    // The regression this ticket exists to prevent: on the Rosneft page the
    // risk panel called a node "Related sanctions-linked" while the same node
    // in the OpenSanctions card's graph carried no badge at all.
    const sig = signal("RELATED_SANCTIONS_LINKED", { subject_statement_id: "in-bundle-1" });
    const scoped = scopeCrossSourceSignals([sig], bundle);
    expect(scoped).toEqual([sig]);

    // Asserted against the real badge machinery's input rather than a proxy:
    // BODSGraph renders a node's badge from exactly this map.
    expect(buildSignalMap(scoped).get("in-bundle-1")).toEqual([sig]);
  });

  it("drops a RELATED_* signal whose statement is not in this bundle", () => {
    const sig = signal("RELATED_SANCTIONED", { subject_statement_id: "some-other-bundle" });
    expect(scopeCrossSourceSignals([sig], bundle)).toEqual([]);
    // Passing it through unfiltered would be harmless (no node matches) but
    // illegible — the graph's signal list should say what it means.
    expect(buildSignalMap(scopeCrossSourceSignals([sig], bundle)).size).toBe(0);
  });

  it("drops subject-level codes even when their statement IS in this bundle", () => {
    // Deliberately narrow, not an oversight. COMPLEX_OWNERSHIP_LAYERS carries
    // a longest_path computed over the MERGED graph; a source bundle usually
    // holds only a fragment of it, so badging here would assert a structural
    // claim that is untrue of the graph on screen. Widening past RELATED_*
    // should require editing this test.
    const sanctioned = signal("SANCTIONED", { statement_id: "in-bundle-1" });
    const layers = signal("COMPLEX_OWNERSHIP_LAYERS", {
      longest_path: ["in-bundle-1", "in-bundle-2"],
    });
    expect(scopeCrossSourceSignals([sanctioned, layers], bundle)).toEqual([]);
  });

  it("keeps only the matching subset of a mixed top-level list", () => {
    const keep = signal("RELATED_PEP", { subject_statement_id: "in-bundle-2" });
    const dropWrongBundle = signal("RELATED_PEP", { subject_statement_id: "elsewhere" });
    const dropSubjectLevel = signal("PEP", { statement_id: "in-bundle-1" });
    expect(
      scopeCrossSourceSignals([dropSubjectLevel, keep, dropWrongBundle], bundle),
    ).toEqual([keep]);
  });

  it("matches on any evidence shape, not just subject_statement_id", () => {
    const viaMatches = signal("RELATED_DEBARMENT", { matches: [{ statement_id: "in-bundle-2" }] });
    expect(scopeCrossSourceSignals([viaMatches], bundle)).toEqual([viaMatches]);
  });

  it("returns [] for empty or absent inputs", () => {
    const sig = signal("RELATED_PEP", { subject_statement_id: "in-bundle-1" });
    expect(scopeCrossSourceSignals([], bundle)).toEqual([]);
    expect(scopeCrossSourceSignals([sig], [])).toEqual([]);
    expect(scopeCrossSourceSignals(undefined, bundle)).toEqual([]);
    expect(scopeCrossSourceSignals([sig], undefined)).toEqual([]);
    expect(scopeCrossSourceSignals([sig], [{ recordType: "entity" }])).toEqual([]);
  });
});

describe("buildSignalMap", () => {
  it("buckets several signals under one statement and one signal under several", () => {
    const a = signal("RELATED_PEP", { subject_statement_id: "s1" });
    const b = signal("RELATED_SANCTIONED", { subject_statement_id: "s1" });
    const c = signal("COMPLEX_OWNERSHIP_LAYERS", { longest_path: ["s1", "s2"] });
    const map = buildSignalMap([a, b, c]);
    expect(map.get("s1")).toEqual([a, b, c]);
    expect(map.get("s2")).toEqual([c]);
  });

  it("ignores signals whose evidence names no statement", () => {
    expect(buildSignalMap([signal("SANCTIONED", {})]).size).toBe(0);
  });
});
