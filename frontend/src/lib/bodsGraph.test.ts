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

// GLEIF Level-2 publishes a direct and an ultimate accounting-consolidation
// record per parent/child, each mapped to its own BODS statement. The two
// view-layer clean-ups collapse the resulting duplicate / skip-level edges.
function consolidationInterest(kind: "direct" | "ultimate") {
  return {
    type: "otherInfluenceOrControl",
    beneficialOwnershipOrControl: false,
    details: `Relationship Type: IS_${kind === "ultimate" ? "ULTIMATELY" : "DIRECTLY"}_CONSOLIDATED_BY`,
  };
}

/** parent P, child C, grandchild G. P is both direct & ultimate parent of C;
 *  C is direct parent of G and P is G's ultimate parent. */
function consolidationStatements(): Record<string, unknown>[] {
  const ent = (id: string) => ({
    statementId: id,
    recordType: "entity",
    recordDetails: { name: id },
  });
  const rel = (id: string, parent: string, child: string, kind: "direct" | "ultimate") => ({
    statementId: id,
    recordType: "relationship",
    recordDetails: {
      interestedParty: parent,
      subject: child,
      interests: [consolidationInterest(kind)],
    },
  });
  return [
    ent("P"),
    ent("C"),
    ent("G"),
    rel("r-pc-d", "P", "C", "direct"),
    rel("r-pc-u", "P", "C", "ultimate"), // duplicate of the P→C direct edge
    rel("r-cg-d", "C", "G", "direct"),
    rel("r-pg-u", "P", "G", "ultimate"), // skip-level: implied by P→C→G
  ];
}

describe("consolidation edge clean-up (B + C)", () => {
  it("collapses a duplicate direct/ultimate pair into one edge (defaults on)", () => {
    const { edges } = bodsToGraph(consolidationStatements());
    // P→C once, C→G once, and the skip-level P→G suppressed → 2 edges.
    const pairs = edges.map((e) => `${e.source}->${e.target}`).sort();
    expect(pairs).toEqual(["C->G", "P->C"]);
  });

  it("annotates a same-pair direct+ultimate child as one merged edge", () => {
    // The same entity is both a direct AND ultimate child. C keeps the ultimate
    // edge (its pair has a direct edge too), B merges them, and the single
    // surviving edge is annotated to reflect both flavours — both BODS
    // statements stay in the data, only the rendered edge is merged.
    const { edges } = bodsToGraph(consolidationStatements());
    const pc = edges.find((e) => e.source === "P" && e.target === "C")!;
    expect(pc.category).toBe("control");
    expect(pc.label).toBe("Controls (direct + ultimate)");
    expect(pc.details).toContain("IS_DIRECTLY_CONSOLIDATED_BY");
    expect(pc.details).toContain("IS_ULTIMATELY_CONSOLIDATED_BY");
  });

  it("when C is off, B merges the pair and pools both flavours into one edge", () => {
    const { edges } = bodsToGraph(consolidationStatements(), {
      suppressRedundantUltimateConsolidation: false,
    });
    const pc = edges.find((e) => e.source === "P" && e.target === "C")!;
    expect(pc.label).toBe("Controls (direct + ultimate)");
    expect(pc.details).toContain("IS_DIRECTLY_CONSOLIDATED_BY");
    expect(pc.details).toContain("IS_ULTIMATELY_CONSOLIDATED_BY");
  });

  it("keeps an ultimate edge that has no direct-consolidation path (no orphan)", () => {
    // G's only link to P is the ultimate edge; the C→G direct edge is removed.
    const stmts = consolidationStatements().filter((s) => s.statementId !== "r-cg-d");
    const { edges } = bodsToGraph(stmts);
    const pairs = edges.map((e) => `${e.source}->${e.target}`).sort();
    expect(pairs).toEqual(["P->C", "P->G"]); // P→G ultimate retained
  });

  it("merges parallel edges but does not suppress when C is disabled", () => {
    const { edges } = bodsToGraph(consolidationStatements(), {
      suppressRedundantUltimateConsolidation: false,
    });
    // P→C duplicate still merges (B), P→G skip-level edge stays (C off) → 3.
    const pairs = edges.map((e) => `${e.source}->${e.target}`).sort();
    expect(pairs).toEqual(["C->G", "P->C", "P->G"]);
  });

  it("leaves every raw edge separate when both clean-ups are disabled", () => {
    const { edges } = bodsToGraph(consolidationStatements(), {
      mergeParallelEdges: false,
      suppressRedundantUltimateConsolidation: false,
    });
    expect(edges).toHaveLength(4); // r-pc-d, r-pc-u, r-cg-d, r-pg-u
  });

  it("recognises the live GLEIF mapper detail form (direct-child / ultimate-child)", () => {
    // map_gleif_subsidiaries writes details "GLEIF Level 2 {direct,ultimate}-child
    // (accounting consolidation)" with directOrIndirect direct/indirect — the
    // subsidiary reveal must merge+annotate these exactly like the OO-bundle form.
    const childRel = (id: string, kind: "direct" | "ultimate") => ({
      statementId: id,
      recordType: "relationship",
      recordDetails: {
        interestedParty: "P",
        subject: "C",
        interests: [
          {
            type: "otherInfluenceOrControl",
            directOrIndirect: kind === "direct" ? "direct" : "indirect",
            beneficialOwnershipOrControl: false,
            details: `GLEIF Level 2 ${kind}-child (accounting consolidation)`,
          },
        ],
      },
    });
    const { edges } = bodsToGraph([
      { statementId: "P", recordType: "entity", recordDetails: { name: "P" } },
      { statementId: "C", recordType: "entity", recordDetails: { name: "C" } },
      childRel("r-d", "direct"),
      childRel("r-u", "ultimate"),
    ]);
    expect(edges).toHaveLength(1);
    expect(edges[0].label).toBe("Controls (direct + ultimate)");
  });

  it("does not merge edges between different pairs", () => {
    // A diamond of ownership edges must stay as four distinct edges.
    const { edges } = bodsToGraph([
      { statementId: "A", recordType: "entity", recordDetails: { name: "A" } },
      { statementId: "B", recordType: "entity", recordDetails: { name: "B" } },
      { statementId: "C", recordType: "entity", recordDetails: { name: "C" } },
      { statementId: "r1", recordType: "relationship", recordDetails: { interestedParty: "A", subject: "B", interests: [{ type: "shareholding" }] } },
      { statementId: "r2", recordType: "relationship", recordDetails: { interestedParty: "A", subject: "C", interests: [{ type: "shareholding" }] } },
    ]);
    expect(edges).toHaveLength(2);
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
    nodes: ids.map((id) => ({ id, label: id, recordType: "entity", icon: "", identifiers: [], sources: [] })),
    edges: ([["R", "A"], ["R", "B"], ["A", "C"], ["B", "C"], ["C", "D"]] as const).map(
      ([source, target], i) => ({ id: `e${i}`, source, target, label: `owns ${target}`, category: "ownership" as const, sources: [] })
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
        { id: "X", label: "X", recordType: "entity", icon: "", identifiers: [], sources: [] },
        { id: "Y", label: "Y", recordType: "entity", icon: "", identifiers: [], sources: [] },
      ],
      edges: [{ id: "e", source: "X", target: "Y", label: "", category: "ownership", sources: [] }],
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
