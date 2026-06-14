import { describe, it, expect } from "vitest";
import { bodsToGraph, searchNodes } from "./bodsGraph";

// Minimal BODS v0.4 bundle: an entity (with a declarationSubject alias and an
// identifier), a person, and a relationship whose endpoints reference the
// person by statementId and the entity by its declarationSubject alias.
const STATEMENTS: Record<string, unknown>[] = [
  {
    statementId: "n-acme",
    recordType: "entity",
    declarationSubject: "XI-LEI-ACME",
    recordDetails: {
      name: "ACME LIMITED",
      jurisdiction: { code: "GB" },
      identifiers: [{ id: "12345678", scheme: "GB-COH" }],
    },
  },
  {
    statementId: "n-bob",
    recordType: "person",
    recordDetails: { names: [{ fullName: "Bob Owner" }], personType: "knownPerson" },
  },
  {
    statementId: "r-1",
    recordType: "relationship",
    recordDetails: {
      interestedParty: "n-bob",
      subject: "XI-LEI-ACME",
      interests: [{ type: "shareholding", share: { exact: 75 }, beneficialOwnershipOrControl: true }],
    },
  },
];

describe("bodsToGraph", () => {
  const model = bodsToGraph(STATEMENTS);

  it("emits one node per entity/person statement", () => {
    expect(model.nodes.map((n) => n.id).sort()).toEqual(["n-acme", "n-bob"]);
  });

  it("resolves a relationship using statementId + declarationSubject alias", () => {
    expect(model.edges).toHaveLength(1);
    expect(model.edges[0]).toMatchObject({ source: "n-bob", target: "n-acme" });
  });

  it("categorises a shareholding as ownership and labels the share", () => {
    expect(model.edges[0].category).toBe("ownership");
    expect(model.edges[0].label).toContain("Owns 75%");
  });

  it("collects identifiers onto the node for search", () => {
    const acme = model.nodes.find((n) => n.id === "n-acme")!;
    expect(acme.identifiers).toContain("12345678");
  });

  it("drops relationships with an unresolvable endpoint", () => {
    const dangling = bodsToGraph([
      { statementId: "x", recordType: "entity", recordDetails: { name: "X" } },
      { statementId: "r", recordType: "relationship", recordDetails: { interestedParty: "ghost", subject: "x" } },
    ]);
    expect(dangling.edges).toHaveLength(0);
  });
});

describe("searchNodes", () => {
  const { nodes } = bodsToGraph(STATEMENTS);

  it("matches by name (case-insensitive substring)", () => {
    expect(searchNodes(nodes, "acme")).toEqual(["n-acme"]);
    expect(searchNodes(nodes, "BOB")).toEqual(["n-bob"]);
  });

  it("matches by identifier", () => {
    expect(searchNodes(nodes, "12345678")).toEqual(["n-acme"]);
  });

  it("returns no matches for a blank query", () => {
    expect(searchNodes(nodes, "   ")).toEqual([]);
  });

  it("returns no matches when nothing contains the query", () => {
    expect(searchNodes(nodes, "zzz")).toEqual([]);
  });
});
