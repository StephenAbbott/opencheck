import { describe, expect, it } from "vitest";
import type { GraphEdge, GraphModel, GraphNode } from "./bodsGraph";
import {
  MIN_BADGE_FONT_PX,
  MIN_TARGET_PX,
  canvasHeightFor,
  collapsibleNodes,
  clusterLabel,
  findSiblingClusters,
  hiddenByCluster,
  hitBox,
  prefersTextFirst,
  readingOrder,
  rovingTarget,
  shortSourceName,
  wrapWideRanks,
  type PlacedNode,
} from "./graphScale";

function node(id: string): GraphNode {
  return { id, label: id, recordType: "entity", icon: "", identifiers: [], sources: [] };
}
function edge(source: string, target: string, src = "GLEIF", category: GraphEdge["category"] = "control", extra: Partial<GraphEdge> = {}): GraphEdge {
  return { id: `${source}->${target}`, source, target, label: "Controls", category, sources: [src], ...extra };
}

/** Shell's shape as read on 24 Sept 2026: owners over one hub over many leaves. */
function shellShape({ gleif = 94, os = 32, small = 6, owners = 16 } = {}): GraphModel {
  const nodes: GraphNode[] = [node("hub")];
  const edges: GraphEdge[] = [];
  for (let i = 0; i < owners; i++) {
    nodes.push(node(`owner${i}`));
    edges.push(edge(`owner${i}`, "hub", "UK Companies House", "role"));
  }
  for (let i = 0; i < gleif; i++) {
    nodes.push(node(`g${i}`));
    edges.push(edge("hub", `g${i}`));
  }
  for (let i = 0; i < os; i++) {
    nodes.push(node(`o${i}`));
    edges.push(edge("hub", `o${i}`, "OpenSanctions", "ownership", { label: `Owns ${i}%` }));
  }
  for (let i = 0; i < small; i++) {
    nodes.push(node(`s${i}`));
    edges.push(edge("hub", `s${i}`, "GLEIF", "ownership"));
  }
  return { nodes, edges };
}

describe("findSiblingClusters (Phase 243)", () => {
  it("groups Shell's 132 leaf subsidiaries by source and kind", () => {
    const clusters = findSiblingClusters(shellShape());
    expect(clusters.map((c) => c.label)).toEqual([
      "GLEIF: 94 subsidiaries",
      "OpenSanctions: 32 subsidiaries",
      "GLEIF: 6 subsidiaries",
    ]);
    expect(clusters.every((c) => c.hub === "hub" && c.direction === "below")).toBe(true);
    // One shared edge label is carried; differing labels are not guessed at.
    expect(clusters[0].edgeLabel).toBe("Controls");
    expect(clusters[1].edgeLabel).toBe("");
  });

  it("leaves a rank at or under the threshold alone", () => {
    expect(findSiblingClusters(shellShape({ gleif: 20, os: 10, small: 6 }))).toEqual([]);
  });

  it("never groups a node that carries a signal badge", () => {
    const flagged = new Set(["g3", "o7"]);
    const clusters = findSiblingClusters(shellShape(), { flagged });
    const members = new Set(clusters.flatMap((c) => c.members));
    expect(members.has("g3")).toBe(false);
    expect(members.has("o7")).toBe(false);
    expect(clusters[0].label).toBe("GLEIF: 93 subsidiaries");
  });

  it("keeps groups below the minimum size as individual nodes", () => {
    const clusters = findSiblingClusters(shellShape({ small: 4 }));
    expect(clusters.map((c) => c.members.length)).toEqual([94, 32]);
  });

  it("does not group a leaf with two parents, or a node with children", () => {
    const m = shellShape();
    m.edges.push(edge("owner0", "g0")); // g0 now has two parents
    m.nodes.push(node("grandchild"));
    m.edges.push(edge("g1", "grandchild")); // g1 is no longer a leaf
    const members = new Set(findSiblingClusters(m).flatMap((c) => c.members));
    expect(members.has("g0")).toBe(false);
    expect(members.has("g1")).toBe(false);
  });

  it("keeps ended siblings apart from current ones, and says so", () => {
    const m = shellShape({ gleif: 60, os: 0, small: 0 });
    for (const e of m.edges.slice(16, 26)) e.ended = true;
    const labels = findSiblingClusters(m).map((c) => c.label);
    expect(labels).toContain("GLEIF: 50 subsidiaries");
    expect(labels).toContain("GLEIF: 10 former subsidiaries");
  });

  it("groups leaf parents above a hub too, named by the edge kind", () => {
    const m: GraphModel = { nodes: [node("hub")], edges: [] };
    for (let i = 0; i < 45; i++) {
      m.nodes.push(node(`p${i}`));
      m.edges.push(edge(`p${i}`, "hub", "UK Companies House", "role"));
    }
    const [c] = findSiblingClusters(m);
    expect(c.direction).toBe("above");
    expect(c.label).toBe("UK Companies House: 45 officers");
  });

  it("has ids that survive the network growing around them", () => {
    const a = findSiblingClusters(shellShape()).map((c) => c.id);
    const grown = shellShape();
    grown.nodes.push(node("new-owner"));
    grown.edges.push(edge("new-owner", "owner0", "GLEIF", "ownership"));
    expect(findSiblingClusters(grown).map((c) => c.id)).toEqual(a);
  });

  it("hides members only while their cluster is closed", () => {
    const clusters = findSiblingClusters(shellShape());
    expect(hiddenByCluster(clusters, new Set()).size).toBe(132);
    expect(hiddenByCluster(clusters, new Set([clusters[0].id])).size).toBe(38);
  });
});

describe("cluster wording", () => {
  it("shortens long source names", () => {
    expect(shortSourceName("OECD-UNSD Multinational Enterprise Information Platform (MEIP), 'Group Register' sheet.")).toBe(
      "OECD-UNSD Multinational Enterpr…",
    );
    expect(shortSourceName("UK Companies House")).toBe("UK Companies House");
    expect(shortSourceName("")).toBe("Unattributed");
  });

  it("never says owns about a group", () => {
    // GLEIF Level 2 is consolidation, MEIP is group membership: a cluster
    // label must not turn either into ownership.
    for (const cat of ["ownership", "control", "role", "unknown"] as const) {
      expect(clusterLabel("GLEIF", 3, "below", cat, false)).not.toMatch(/\bowns?\b|%/i);
    }
  });
});

describe("wrapWideRanks (Phase 243)", () => {
  const row = (n: number, y: number, flags: { leaf: boolean; root: boolean }): PlacedNode[] =>
    Array.from({ length: n }, (_, i) => ({ id: `${y}-${i}`, x: i * 140, y, ...flags }));

  it("folds a wide rank of leaves into rows and moves nothing it need not", () => {
    const nodes = [
      ...row(1, 0, { leaf: false, root: true }),
      ...row(25, 100, { leaf: true, root: false }),
    ];
    const pos = wrapWideRanks(nodes, { maxCols: 10, rowSep: 150 });
    const ys = [...new Set(nodes.slice(1).map((n) => pos.get(n.id)!.y))].sort((a, b) => a - b);
    expect(ys).toEqual([100, 250, 400]);
    expect(pos.get("0-0")).toEqual({ x: 0, y: 0 });
    // At most ten to a row.
    for (const y of ys) expect(nodes.filter((n) => pos.get(n.id)!.y === y).length).toBeLessThanOrEqual(10);
  });

  it("folds a wide rank of roots upwards, pushing the ranks above it up", () => {
    const nodes = [
      ...row(16, 0, { leaf: false, root: true }),
      ...row(1, 100, { leaf: true, root: false }),
    ];
    const pos = wrapWideRanks(nodes, { maxCols: 8, rowSep: 150 });
    const ys = [...new Set(nodes.slice(0, 16).map((n) => pos.get(n.id)!.y))].sort((a, b) => a - b);
    expect(ys).toEqual([-150, 0]);
    expect(pos.get("100-0")!.y).toBe(100);
  });

  it("pushes lower ranks down when a middle rank folds", () => {
    const nodes = [
      ...row(12, 0, { leaf: true, root: true }), // isolated parties: fold down
      ...row(1, 100, { leaf: true, root: false }),
    ];
    const pos = wrapWideRanks(nodes, { maxCols: 6, rowSep: 150 });
    expect(pos.get("100-0")!.y).toBe(250);
  });

  it("folds a long rank to the canvas's proportions, spaced for its widest node", () => {
    const nodes = [
      ...row(1, 0, { leaf: false, root: true }),
      ...row(96, 100, { leaf: true, root: false }),
    ];
    nodes[1].w = 150; // a cluster box in the rank
    const pos = wrapWideRanks(nodes, { maxCols: 10, aspect: 1166 / 640, colSep: 140, rowSep: 150 });
    const leaves = nodes.slice(1).map((n) => pos.get(n.id)!);
    const rows = new Set(leaves.map((p) => p.y)).size;
    expect(rows).toBe(7); // 14 across, not 10 × 10
    const firstRow = leaves.filter((p) => p.y === 100).map((p) => p.x).sort((a, b) => a - b);
    expect(firstRow[1] - firstRow[0]).toBe(170);
  });

  it("leaves a rank that mixes parents and leaves alone", () => {
    const nodes = [
      ...row(20, 0, { leaf: false, root: false }),
    ];
    nodes[0].leaf = true;
    const pos = wrapWideRanks(nodes, { maxCols: 5 });
    for (const n of nodes) expect(pos.get(n.id)).toEqual({ x: n.x, y: n.y });
  });
});

describe("canvas size and hit targets", () => {
  it("takes the graph's proportions within limits", () => {
    expect(canvasHeightFor({ w: 3000, h: 300 }, 1000)).toBe(360);
    expect(canvasHeightFor({ w: 1000, h: 500 }, 1000)).toBe(500);
    expect(canvasHeightFor({ w: 500, h: 2000 }, 1000)).toBe(640);
    expect(canvasHeightFor({ w: 0, h: 0 }, 1000)).toBe(360);
  });

  it("never gives an overlay control less than 24px either way", () => {
    for (const [w, h] of [[8, 8], [32, 16], [26, 13], [40, 30]]) {
      const box = hitBox(100, 100, w, h);
      expect(box.width).toBeGreaterThanOrEqual(MIN_TARGET_PX);
      expect(box.height).toBeGreaterThanOrEqual(MIN_TARGET_PX);
      // Centred on the mark.
      expect(box.left + box.width / 2).toBe(100);
      expect(box.top + box.height / 2).toBe(100);
    }
    expect(MIN_BADGE_FONT_PX).toBeGreaterThanOrEqual(11);
  });
});

describe("the overlay as one tab stop", () => {
  it("orders controls the way the eye reads", () => {
    const items = [
      { id: "c", cx: 10, cy: 200 },
      { id: "b", cx: 300, cy: 12 },
      { id: "a", cx: 20, cy: 0 },
    ];
    expect(readingOrder(items).map((i) => i.id)).toEqual(["a", "b", "c"]);
  });

  it("moves with arrows, wraps, jumps with Home/End, and ignores Tab", () => {
    expect(rovingTarget(3, 0, "ArrowRight")).toBe(1);
    expect(rovingTarget(3, 2, "ArrowDown")).toBe(0);
    expect(rovingTarget(3, 0, "ArrowLeft")).toBe(2);
    expect(rovingTarget(3, 1, "Home")).toBe(0);
    expect(rovingTarget(3, 1, "End")).toBe(2);
    expect(rovingTarget(3, 1, "Tab")).toBeNull();
    expect(rovingTarget(0, 0, "ArrowRight")).toBeNull();
  });
});

describe("prefersTextFirst", () => {
  it("is true on a phone-width viewport only", () => {
    const at = (matches: boolean) => ({ matchMedia: () => ({ matches }) });
    expect(prefersTextFirst(at(true))).toBe(true);
    expect(prefersTextFirst(at(false))).toBe(false);
    expect(prefersTextFirst(undefined)).toBe(false);
    expect(prefersTextFirst({ matchMedia: () => { throw new Error("no"); } })).toBe(false);
  });
});

describe("collapsibleNodes", () => {
  it("offers a collapse only where collapsing hides something", () => {
    const m = shellShape({ gleif: 3, os: 0, small: 0, owners: 3 });
    const set = collapsibleNodes(m, new Set());
    // Each officer's only child (the hub) is reachable through the others.
    expect([...set]).toEqual(["hub"]);
    // A lone owner is different: collapsing it hides the hub and below.
    const lone = shellShape({ gleif: 3, os: 0, small: 0, owners: 1 });
    expect(collapsibleNodes(lone, new Set()).has("owner0")).toBe(true);
    // A collapsed node stays toggleable, so it can be opened again.
    expect(collapsibleNodes(m, new Set(["owner1"])).has("owner1")).toBe(true);
  });
});
