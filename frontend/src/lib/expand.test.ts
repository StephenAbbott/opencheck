import { describe, it, expect } from "vitest";

import {
  isEntityStatement,
  subjectLei,
  mergeStatements,
  frontierAnchors,
  type EdgeLite,
} from "./expand";

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
