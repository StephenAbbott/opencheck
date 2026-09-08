import { describe, it, expect } from "vitest";

import {
  dedupeFrontier,
  isEntityStatement,
  subjectLei,
  mergeStatements,
  frontierAnchors,
  mergeSignals,
  signalsBeyond,
  subjectRegisterId,
  type EdgeLite,
  type FrontierAnchor,
} from "./expand";
import type { RiskSignal } from "./api";

type Stmt = Record<string, unknown>;

const _LEI = "5493001KJTIIGC8Y1R12";

function entity(id: string, identifiers: Stmt[] = []): Stmt {
  return {
    statementId: id,
    recordType: "entity",
    recordDetails: { entityType: { type: "registeredEntity" }, name: id, identifiers },
  };
}

describe("isEntityStatement", () => {
  it("is true only for entity statements (people are terminal)", () => {
    expect(isEntityStatement(entity("e1"))).toBe(true);
    expect(isEntityStatement({ statementId: "p1", recordType: "person" })).toBe(false);
    expect(isEntityStatement(undefined)).toBe(false);
  });
});

describe("subjectLei", () => {
  it("reads an LEI from an LEI-scheme identifier", () => {
    const e = entity("e1", [{ id: _LEI, scheme: "XI-LEI", schemeName: "LEI" }]);
    expect(subjectLei(e)).toBe(_LEI);
  });

  it("falls back to an LEI-shaped value when no scheme says LEI", () => {
    const e = entity("e1", [{ id: _LEI, scheme: "", schemeName: "" }]);
    expect(subjectLei(e)).toBe(_LEI);
  });

  it("returns null for a company number with no LEI (a live dead-end)", () => {
    const e = entity("e1", [{ id: "00102498", scheme: "GB-COH", schemeName: "Companies House" }]);
    expect(subjectLei(e)).toBeNull();
  });
});

describe("frontierAnchors", () => {
  const A = entity("A", [{ id: _LEI, scheme: "XI-LEI", schemeName: "LEI" }]);
  const B = entity("B", [{ id: "5493004YR8F4DUF6C453", scheme: "XI-LEI", schemeName: "LEI" }]);
  const person = { statementId: "P", recordType: "person" } as Stmt;
  const noLei = entity("N", [{ id: "00102498", scheme: "GB-COH" }]);
  // A owns B (A → B), so B is "owned" and only A is on the frontier.
  const edges: EdgeLite[] = [{ source: "A", target: "B", category: "ownership" }];

  it("returns LEI-bearing entities that nobody shown owns yet", () => {
    const f = frontierAnchors([A, B, person, noLei], edges, new Set());
    expect(f).toEqual([{ lei: _LEI, anchor: "A" }]);
  });

  it("excludes people, no-LEI nodes, and already-expanded anchors", () => {
    // No edges → both A and B would be frontier; expanding A leaves only B.
    const f = frontierAnchors([A, B, person, noLei], [], new Set(["A"]));
    expect(f.map((x) => x.anchor)).toEqual(["B"]);
  });

  it("subsidiaries direction keeps the leaves (nodes that own nothing shown)", () => {
    // A owns B. Digging DOWN, the frontier is the leaf B (expand its children),
    // not A — the opposite of the owners direction.
    const f = frontierAnchors([A, B, person, noLei], edges, new Set(), "subsidiaries");
    expect(f.map((x) => x.anchor)).toEqual(["B"]);
  });

  it("ignores role edges when deciding the frontier", () => {
    // A director (role edge) pointing at A must not mark A as owned.
    const roleEdges: EdgeLite[] = [{ source: "P", target: "A", category: "role" }];
    const f = frontierAnchors([A, person], roleEdges, new Set());
    expect(f.map((x) => x.anchor)).toEqual(["A"]);
  });
});

describe("mergeSignals / signalsBeyond", () => {
  const sanctioned = { code: "SANCTIONED", source_id: "os", hit_id: "1" } as unknown as RiskSignal;
  const pep = { code: "PEP", source_id: "ep", hit_id: "2" } as unknown as RiskSignal;
  const pepDup = { code: "PEP", source_id: "ep", hit_id: "2" } as unknown as RiskSignal;

  it("mergeSignals drops exact duplicates", () => {
    expect(mergeSignals([sanctioned], [pep, pepDup])).toHaveLength(2);
  });

  it("signalsBeyond returns only extras not already in the base", () => {
    // base already has `sanctioned`; only PEP is new.
    expect(signalsBeyond([sanctioned], [sanctioned, pep]).map((s) => s.code)).toEqual(["PEP"]);
  });
});

describe("mergeStatements", () => {
  it("appends new statements and de-dupes by statementId (base wins)", () => {
    const base = [entity("e1"), entity("e2")];
    const extra = [
      { statementId: "e2", recordType: "entity", recordDetails: { name: "dup" } },
      entity("e3"),
    ];
    const merged = mergeStatements(base, extra);
    const ids = merged.map((s) => s.statementId);
    expect(ids).toEqual(["e1", "e2", "e3"]);
    // base copy of e2 is kept, not the duplicate.
    expect((merged.find((s) => s.statementId === "e2")!.recordDetails as Stmt).name).not.toBe("dup");
  });
});

describe("dedupeFrontier", () => {
  // Three raw anchors; g-shell and ch-shell are per-source duplicates of the
  // same reconciled entity, "other" stands alone (issue #25, item 3: the
  // "Add next layer — N" count must match the visible reconciled node count).
  const raw: FrontierAnchor[] = [
    { lei: "21380068P1DRHMJ8KU70", anchor: "g-shell" },
    { lei: "21380068P1DRHMJ8KU70", anchor: "ch-shell" },
    { lei: "5493001KJTIIGC8Y1R12", anchor: "other" },
  ];
  const remap = {
    "g-shell": "recon:LEI:21380068P1DRHMJ8KU70",
    "ch-shell": "recon:LEI:21380068P1DRHMJ8KU70",
  };

  it("collapses per-source duplicates of one entity to a single anchor", () => {
    const out = dedupeFrontier(raw, remap);
    expect(out).toHaveLength(2);
    // First raw anchor per canonical id survives (expansion bookkeeping
    // tracks raw statementIds).
    expect(out[0].anchor).toBe("g-shell");
    expect(out[1].anchor).toBe("other");
  });

  it("is a no-op when nothing reconciles", () => {
    expect(dedupeFrontier(raw, {})).toHaveLength(3);
  });

  it("handles an empty frontier", () => {
    expect(dedupeFrontier([], remap)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Phase 182 — the frontier keyed on register-scoped identifiers
// ---------------------------------------------------------------------------

describe("subjectRegisterId", () => {
  const HOPS: ReadonlySet<string> = new Set(["GB-COH"]);
  const ch = entity("ch", [{ id: "2999029", scheme: "GB-COH", schemeName: "UK Companies House" }]);

  it("reads the first identifier whose scheme the server can hop on, as filed", () => {
    // The dropped leading zero is the server's to restore (Phase 177), not ours.
    expect(subjectRegisterId(ch, HOPS)).toEqual({ scheme: "GB-COH", id: "2999029" });
  });

  it("matches the scheme case-insensitively and skips schemes with no hop", () => {
    const lower = entity("x", [{ id: "12345678", scheme: "gb-coh" }]);
    expect(subjectRegisterId(lower, HOPS)).toEqual({ scheme: "GB-COH", id: "12345678" });
    const jersey = entity("j", [{ id: "12345", scheme: "REG-JE" }]);
    expect(subjectRegisterId(jersey, HOPS)).toBeNull();
  });

  it("is null with no hop schemes at all — the LEI-only frontier of before", () => {
    expect(subjectRegisterId(ch, new Set())).toBeNull();
  });
});

describe("frontierAnchors with register hops", () => {
  const HOPS: ReadonlySet<string> = new Set(["GB-COH"]);
  const gleif = entity("G", [
    { id: _LEI, scheme: "XI-LEI", schemeName: "LEI" },
    { id: "00070274", scheme: "GB-COH" },
  ]);
  const psc = {
    ...entity("PSC", [{ id: "2999029", scheme: "GB-COH", schemeName: "UK Companies House" }]),
    recordDetails: {
      entityType: { type: "registeredEntity" },
      name: "BABCOCK DEFENCE SYSTEMS LIMITED",
      identifiers: [{ id: "2999029", scheme: "GB-COH", schemeName: "UK Companies House" }],
    },
  };
  const jersey = entity("JE", [{ id: "12345", scheme: "REG-JE" }]);
  // The PSC owns the subject: G is owned, PSC is on the frontier.
  const edges: EdgeLite[] = [{ source: "PSC", target: "G", category: "ownership" }];

  it("offers a company-number node for expansion, keyed on its scheme and id", () => {
    const f = frontierAnchors([gleif, psc, jersey], edges, new Set(), "owners", HOPS);
    expect(f).toEqual([
      { scheme: "GB-COH", id: "2999029", anchor: "PSC", name: "BABCOCK DEFENCE SYSTEMS LIMITED" },
    ]);
  });

  it("keys a node on its LEI when it has one, even if it also has a company number", () => {
    const f = frontierAnchors([gleif, psc], [], new Set(), "owners", HOPS);
    expect(f[0]).toEqual({ lei: _LEI, anchor: "G" });
    expect(f[0].scheme).toBeUndefined();
  });

  it("stays LEI-only without hop schemes — the company-number node is a dead end", () => {
    const f = frontierAnchors([gleif, psc], edges, new Set(), "owners");
    expect(f).toEqual([]);
  });

  it("never offers a company-number node when digging down (children need an LEI)", () => {
    const f = frontierAnchors([gleif, psc], [], new Set(), "subsidiaries", HOPS);
    expect(f.map((x) => x.anchor)).toEqual(["G"]);
  });
});

describe("dedupeFrontier prefers the LEI-keyed anchor", () => {
  it("expands a company reached by LEI and by company number once, on its LEI", () => {
    // Companies House's statement (number only) comes first in the raw
    // frontier; GLEIF's (LEI + number) reconciles to the same node.
    const raw: FrontierAnchor[] = [
      { scheme: "GB-COH", id: "00070274", anchor: "ch-vosper" },
      { lei: "213800W5454D8XRJ8J78", anchor: "g-vosper" },
      { scheme: "GB-COH", id: "2999029", anchor: "ch-babcock" },
    ];
    const remap = { "ch-vosper": "recon:LEI:213800W5454D8XRJ8J78", "g-vosper": "recon:LEI:213800W5454D8XRJ8J78" };
    const out = dedupeFrontier(raw, remap);
    expect(out).toEqual([
      { lei: "213800W5454D8XRJ8J78", anchor: "g-vosper" },
      { scheme: "GB-COH", id: "2999029", anchor: "ch-babcock" },
    ]);
  });
});
