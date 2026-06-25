import { describe, it, expect } from "vitest";

import { isEntityStatement, subjectLei, mergeStatements } from "./expand";

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
