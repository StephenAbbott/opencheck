import { describe, it, expect } from "vitest";
import {
  bodsToGraph,
  searchNodes,
  computeLevels,
  computeVisibility,
  autoCollapse,
  buildTree,
  type GraphModel,
} from "./bodsGraph";

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

// A DAG with a shared subsidiary (C reachable from both A and B) and a deep
// chain (C → D): R→A, R→B, A→C, B→C, C→D.
function diamondModel(): GraphModel {
  const ids = ["R", "A", "B", "C", "D"];
  return {
    nodes: ids.map((id) => ({ id, label: id, recordType: "entity", icon: "", identifiers: [] })),
    edges: ([["R", "A"], ["R", "B"], ["A", "C"], ["B", "C"], ["C", "D"]] as const).map(
      ([source, target], i) => ({ id: `e${i}`, source, target, label: `owns ${target}`, category: "ownership" as const })
    ),
  };
}

describe("computeLevels", () => {
  it("uses longest path so a shared node sits below both parents", () => {
    const L = computeLevels(diamondModel());
    expect([L.get("R"), L.get("A"), L.get("C"), L.get("D")]).toEqual([0, 1, 2, 3]);
  });
});

describe("computeVisibility (DAG-aware collapse)", () => {
  const model = diamondModel();

  it("hides nothing when a collapsed node's descendants stay reachable elsewhere", () => {
    const v = computeVisibility(model, new Set(["A"]));
    expect([...v.hidden]).toEqual([]); // C/D still reachable via B
    expect(v.hiddenCount.get("A")).toBe(0);
  });

  it("hides the shared subtree only when every path is collapsed", () => {
    const v = computeVisibility(model, new Set(["A", "B"]));
    expect([...v.hidden].sort()).toEqual(["C", "D"]);
    expect(v.hiddenCount.get("A")).toBe(2);
  });

  it("hides a node's descendants when that node is collapsed", () => {
    const v = computeVisibility(model, new Set(["C"]));
    expect([...v.hidden]).toEqual(["D"]);
    expect(v.hiddenCount.get("C")).toBe(1);
  });
});

describe("autoCollapse", () => {
  it("collapses deep-level nodes with children when the graph is deep", () => {
    expect([...autoCollapse(diamondModel())]).toEqual(["C"]); // level 2, has child D
  });

  it("collapses nothing for a shallow graph", () => {
    const shallow: GraphModel = {
      nodes: [
        { id: "X", label: "X", recordType: "entity", icon: "", identifiers: [] },
        { id: "Y", label: "Y", recordType: "entity", icon: "", identifiers: [] },
      ],
      edges: [{ id: "e", source: "X", target: "Y", label: "", category: "ownership" }],
    };
    expect([...autoCollapse(shallow)]).toEqual([]);
  });
});

describe("buildTree", () => {
  it("flattens the DAG; a shared node is full once then a repeat", () => {
    const rows = buildTree(diamondModel(), new Set());
    expect(rows.map((r) => r.id)).toEqual(["R", "A", "C", "D", "B", "C"]);
    expect(rows.map((r) => r.depth)).toEqual([0, 1, 2, 3, 1, 2]);
    expect(rows.filter((r) => r.id === "C").map((r) => r.isRepeat)).toEqual([false, true]);
  });

  it("carries the parent interest label onto the row", () => {
    const rows = buildTree(diamondModel(), new Set());
    expect(rows.find((r) => r.id === "A")!.interestLabel).toBe("owns A");
    expect(rows.find((r) => r.id === "R")!.interestLabel).toBeUndefined();
  });

  it("omits children of a collapsed node", () => {
    const rows = buildTree(diamondModel(), new Set(["C"]));
    expect(rows.map((r) => r.id)).toEqual(["R", "A", "C", "B", "C"]);
    expect(rows.find((r) => r.id === "C" && !r.isRepeat)!.collapsed).toBe(true);
  });
});
