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

// A UK corporate-PSC chain after Fix 2 (Phase 183): the register's two hops
// (holding → subject, person → holding; isComponent true) plus one primary
// indirect relationship person → subject whose componentRecords list the
// intermediary entity record and both hop records.
function componentChainStatements(): Record<string, unknown>[] {
  const ent = (id: string, isComponent = false) => ({
    statementId: id,
    recordId: `rec-${id}`,
    recordType: "entity",
    recordDetails: { name: id, isComponent },
  });
  return [
    ent("SUB"),
    ent("HOLD", true),
    {
      statementId: "OWNER",
      recordId: "rec-OWNER",
      recordType: "person",
      recordDetails: { names: [{ fullName: "Ultimate Owner" }], personType: "knownPerson" },
    },
    {
      statementId: "r-hold-sub",
      recordId: "rec-r-hold-sub",
      recordType: "relationship",
      recordDetails: {
        isComponent: true,
        interestedParty: "HOLD",
        subject: "SUB",
        interests: [{ type: "shareholding", directOrIndirect: "unknown", beneficialOwnershipOrControl: false }],
      },
    },
    {
      statementId: "r-owner-hold",
      recordId: "rec-r-owner-hold",
      recordType: "relationship",
      recordDetails: {
        isComponent: true,
        interestedParty: "OWNER",
        subject: "HOLD",
        interests: [{ type: "shareholding", directOrIndirect: "unknown", beneficialOwnershipOrControl: true }],
      },
    },
    {
      statementId: "r-primary",
      recordId: "rec-r-primary",
      recordType: "relationship",
      recordDetails: {
        isComponent: false,
        interestedParty: "OWNER",
        subject: "SUB",
        interests: [{ type: "shareholding", directOrIndirect: "indirect", beneficialOwnershipOrControl: true }],
        componentRecords: ["rec-HOLD", "rec-r-hold-sub", "rec-r-owner-hold"],
      },
    },
  ];
}

describe("component-primary clean-up (D)", () => {
  it("hides the primary indirect edge when every hop it lists is drawn (default on)", () => {
    const { edges } = bodsToGraph(componentChainStatements());
    const pairs = edges.map((e) => `${e.source}->${e.target}`).sort();
    expect(pairs).toEqual(["HOLD->SUB", "OWNER->HOLD"]);
  });

  it("keeps every node — hiding the edge never drops the person", () => {
    const { nodes } = bodsToGraph(componentChainStatements());
    expect(nodes.map((n) => n.id).sort()).toEqual(["HOLD", "OWNER", "SUB"]);
  });

  it("draws the primary when D is off", () => {
    const { edges } = bodsToGraph(componentChainStatements(), {
      suppressRedundantComponentPrimary: false,
    });
    const pairs = edges.map((e) => `${e.source}->${e.target}`).sort();
    expect(pairs).toEqual(["HOLD->SUB", "OWNER->HOLD", "OWNER->SUB"]);
    const primary = edges.find((e) => e.source === "OWNER" && e.target === "SUB")!;
    expect(primary.category).toBe("ownership");
  });

  it("keeps the primary when one of its hop relationships is not in the graph", () => {
    // The person → holding hop is missing (a bundle trimmed elsewhere): the
    // chain no longer shows the primary, so the primary stays.
    const stmts = componentChainStatements().filter((s) => s.statementId !== "r-owner-hold");
    const { edges } = bodsToGraph(stmts);
    const pairs = edges.map((e) => `${e.source}->${e.target}`).sort();
    expect(pairs).toEqual(["HOLD->SUB", "OWNER->SUB"]);
  });

  it("ignores entity records in componentRecords — only hops decide", () => {
    // Drop the intermediary entity's recordId from the list: the two hops are
    // still there, so the primary is still redundant.
    const stmts = componentChainStatements();
    const primary = stmts.find((s) => s.statementId === "r-primary")!;
    (primary.recordDetails as Record<string, unknown>).componentRecords = [
      "rec-r-hold-sub",
      "rec-r-owner-hold",
    ];
    const { edges } = bodsToGraph(stmts);
    expect(edges.map((e) => `${e.source}->${e.target}`).sort()).toEqual(["HOLD->SUB", "OWNER->HOLD"]);
  });

  it("leaves a relationship without componentRecords alone", () => {
    const { edges } = bodsToGraph(STATEMENTS);
    expect(edges).toHaveLength(1);
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

describe("identity verification tick (Phase 203)", () => {
  const verifiedAnnotation = {
    statementPointerTarget: "/recordDetails",
    motivation: "commenting",
    identityVerification: { status: "verified", route: "companiesHouse" },
  };
  const withTick = [
    STATEMENTS[0],
    { ...STATEMENTS[1], annotations: [verifiedAnnotation] },
    {
      statementId: "n-eve",
      recordType: "person",
      recordDetails: { names: [{ fullName: "Eve Director" }], personType: "knownPerson" },
    },
    STATEMENTS[2],
    {
      statementId: "r-2",
      recordType: "relationship",
      recordDetails: {
        interestedParty: "n-eve",
        subject: "n-acme",
        interests: [{ type: "seniorManagingOfficial" }],
        // A role-level annotation alone does not tick the person node.
      },
      annotations: [{ ...verifiedAnnotation, statementPointerTarget: "/recordDetails/interestedParty" }],
    },
  ];

  it("ticks only the person whose statement carries the annotation", () => {
    const model = bodsToGraph(withTick);
    const byId = new Map(model.nodes.map((n) => [n.id, n]));
    expect(byId.get("n-bob")!.identityVerified).toBe(true);
    expect(byId.get("n-eve")).not.toHaveProperty("identityVerified");
    expect(byId.get("n-acme")).not.toHaveProperty("identityVerified");
  });

  it("carries the tick onto the tree row", () => {
    const rows = buildTree(bodsToGraph(withTick), new Set());
    expect(rows.find((r) => r.id === "n-bob")!.identityVerified).toBe(true);
    expect(rows.find((r) => r.id === "n-eve")!.identityVerified).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Phase 219 — ended relationships
// ---------------------------------------------------------------------------

describe("ended relationships (Phase 219)", () => {
  const AS_OF = "2026-09-16";
  const party = (id: string, kind: "entity" | "person" = "person") =>
    kind === "person"
      ? { statementId: id, recordType: "person", recordDetails: { names: [{ fullName: id }], personType: "knownPerson" } }
      : { statementId: id, recordType: "entity", recordDetails: { name: id } };
  const rel = (
    id: string,
    from: string,
    to: string,
    interests: Record<string, unknown>[],
    recordStatus = "new"
  ) => ({
    statementId: id,
    recordId: `rec-${id}`,
    recordType: "relationship",
    recordStatus,
    recordDetails: { interestedParty: from, subject: to, interests },
  });
  const psc75 = { type: "shareholding", share: { exclusiveMinimum: 75, maximum: 100 } };
  const psc25 = { type: "shareholding", share: { minimum: 25, maximum: 50 } };

  it("marks a closed PSC record ended, dated from its interest, and says so on the label", () => {
    // The Open Ownership UK PSC shape: recordStatus closed + endDate = ceased_on.
    const m = bodsToGraph(
      [party("co", "entity"), party("ann"), rel("r", "ann", "co", [{ ...psc75, endDate: "2024-11-30" }], "closed")],
      { asOf: AS_OF }
    );
    expect(m.edges[0]).toMatchObject({ ended: true, endedOn: "2024-11-30", category: "ownership" });
    expect(m.edges[0].label).toBe("Owns 75–100%\nended 30 November 2024");
    expect(m.edges[0].details).toMatch(/^Ended 30 November 2024\./);
  });

  it("marks a closed record with no endDate ended without inventing a date", () => {
    const m = bodsToGraph(
      [party("co", "entity"), party("ann"), rel("r", "ann", "co", [psc75], "closed")],
      { asOf: AS_OF }
    );
    expect(m.edges[0].ended).toBe(true);
    expect(m.edges[0].endedOn).toBeUndefined();
    expect(m.edges[0].label).toBe("Owns 75–100%\nended");
  });

  it("marks a relationship ended when every interest has a past endDate, even if the record is open", () => {
    // Companies House officer resignations carry endDate on a record never closed.
    const m = bodsToGraph(
      [party("co", "entity"), party("dir"), rel("r", "dir", "co", [{ type: "seniorManagingOfficial", endDate: "2021-03-01" }])],
      { asOf: AS_OF }
    );
    expect(m.edges[0]).toMatchObject({ ended: true, endedOn: "2021-03-01", category: "role" });
  });

  it("keeps a future endDate current", () => {
    const m = bodsToGraph(
      [party("co", "entity"), party("ann"), rel("r", "ann", "co", [{ ...psc75, endDate: "2030-01-01" }])],
      { asOf: AS_OF }
    );
    expect(m.edges[0].ended).toBeUndefined();
    expect(m.edges[0].label).toBe("Owns 75–100%");
  });

  it("never drops an ended edge or its party — BOVS completeness", () => {
    const m = bodsToGraph(
      [party("co", "entity"), party("ann"), rel("r", "ann", "co", [psc75], "closed")],
      { asOf: AS_OF }
    );
    expect(m.nodes.map((n) => n.id).sort()).toEqual(["ann", "co"]);
    expect(m.edges).toHaveLength(1);
  });

  it("draws a pair with a current and an ended record as one current edge labelled by what is current", () => {
    // A PSC whose holding moved from 25–50% to 75–100%: Companies House closes
    // one PSC record and opens another for the same pair.
    const m = bodsToGraph(
      [
        party("co", "entity"),
        party("ann"),
        rel("old", "ann", "co", [{ ...psc25, endDate: "2019-06-18" }], "closed"),
        rel("new", "ann", "co", [psc75]),
      ],
      { asOf: AS_OF }
    );
    expect(m.edges).toHaveLength(1);
    expect(m.edges[0].ended).toBeUndefined();
    expect(m.edges[0].label).toBe("Owns 75–100%");
    expect(m.edges[0].details).toContain("Ended 18 June 2019: Owns 25–50%");
  });

  it("keeps a closed record's undated interests ended once pooled with a current one", () => {
    const m = bodsToGraph(
      [
        party("co", "entity"),
        party("ann"),
        rel("old", "ann", "co", [{ type: "votingRights" }], "closed"),
        rel("new", "ann", "co", [psc75]),
      ],
      { asOf: AS_OF }
    );
    expect(m.edges[0].label).toBe("Owns 75–100%");
    expect(m.edges[0].details).toContain("Ended: Controls (votes)");
  });

  it("draws a pair whose pooled records have all ended as ended, dated with the latest end", () => {
    const m = bodsToGraph(
      [
        party("co", "entity"),
        party("ann"),
        rel("a", "ann", "co", [{ ...psc25, endDate: "2019-06-18" }], "closed"),
        rel("b", "ann", "co", [{ ...psc75, endDate: "2024-11-30" }], "closed"),
      ],
      { asOf: AS_OF }
    );
    expect(m.edges).toHaveLength(1);
    expect(m.edges[0]).toMatchObject({ ended: true, endedOn: "2024-11-30" });
  });

  it("leaves internal markers off the exposed edge", () => {
    const m = bodsToGraph(
      [
        party("co", "entity"),
        party("ann"),
        rel("old", "ann", "co", [psc25], "closed"),
        rel("new", "ann", "co", [psc75]),
      ],
      { asOf: AS_OF }
    );
    expect(JSON.stringify(m)).not.toContain("__closedRecord");
  });

  it("carries the end onto the tree row", () => {
    const m = bodsToGraph(
      [party("co", "entity"), party("ann"), rel("r", "co", "ann", [{ ...psc75, endDate: "2024-11-30" }], "closed")],
      { asOf: AS_OF }
    );
    const rows = buildTree(m, new Set());
    const child = rows.find((r) => r.depth === 1)!;
    expect(child).toMatchObject({ interestEnded: true, interestEndedOn: "2024-11-30" });
    expect(rows.find((r) => r.depth === 0)!.interestEnded).toBe(false);
  });
});

describe("consolidation clean-up (C) with ended relationships (Phase 219)", () => {
  const closeOne = (id: string) =>
    consolidationStatements().map((s) => (s.statementId === id ? { ...s, recordStatus: "closed" } : s));

  it("keeps a current ultimate edge when the direct chain that implied it has ended", () => {
    // C→G direct has lapsed, so P→C→G no longer says today who consolidates G.
    // Hiding P→G would leave G linked only by history.
    const { edges } = bodsToGraph(closeOne("r-cg-d"), { asOf: "2026-09-16" });
    const pairs = edges.map((e) => `${e.source}->${e.target}`).sort();
    expect(pairs).toEqual(["C->G", "P->C", "P->G"]);
    expect(edges.find((e) => e.source === "P" && e.target === "G")!.ended).toBeUndefined();
    expect(edges.find((e) => e.source === "C" && e.target === "G")!.ended).toBe(true);
  });

  it("still hides an ended ultimate edge the direct chain covers", () => {
    const { edges } = bodsToGraph(closeOne("r-pg-u"), { asOf: "2026-09-16" });
    expect(edges.map((e) => `${e.source}->${e.target}`).sort()).toEqual(["C->G", "P->C"]);
  });
});
