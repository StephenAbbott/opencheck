import { describe, expect, it } from "vitest";
import { bodsToGraph, buildTree } from "./bodsGraph";
import { buildGraphLegend } from "./graphStyle";
import { interestCellText } from "./relationshipStatus";
import { isUndatedInterests, UNDATED_LABEL } from "./undated";

describe("isUndatedInterests", () => {
  it("is true for a holding with no dates", () => {
    expect(isUndatedInterests([{ type: "shareholding", share: { exact: 19.75 } }])).toBe(true);
  });
  it("is false once any interest is dated", () => {
    expect(isUndatedInterests([{ type: "shareholding", startDate: "2013-03-21" }])).toBe(false);
    expect(isUndatedInterests([{ type: "shareholding" }, { type: "votingRights", endDate: "2099-01-01" }])).toBe(false);
  });
  it("never calls a role undated", () => {
    expect(isUndatedInterests([{ type: "seniorManagingOfficial" }])).toBe(false);
    expect(isUndatedInterests([{ type: "otherInfluenceOrControl", opencheckOfficerPosition: "secretary" }])).toBe(false);
    expect(isUndatedInterests([])).toBe(false);
  });

  it("never calls a GLEIF reporting exception undated: it is a filing, not a holding", () => {
    const bridge = {
      type: "otherInfluenceOrControl",
      directOrIndirect: "direct",
      details: "GLEIF reporting exception NO_KNOWN_PERSON: there is no known person controlling the entity",
    };
    expect(isUndatedInterests([bridge])).toBe(false);
    expect(isUndatedInterests([bridge, { type: "shareholding" }])).toBe(true);
  });
});

const ent = (id: string, name: string) => ({
  statementId: id,
  recordId: id,
  recordType: "entity",
  recordDetails: { name, entityType: { type: "registeredEntity" } },
});
const rel = (id: string, owner: string, interests: object[], recordStatus = "new") => ({
  statementId: id,
  recordId: id,
  recordType: "relationship",
  recordStatus,
  recordDetails: { subject: "rosneft", interestedParty: owner, interests },
});

describe("the graph and the tree say it", () => {
  const statements = [
    ent("rosneft", "Rosneft"),
    ent("bp", "BP Russian Investments Ltd"),
    ent("old", "Former Holder LLC"),
    rel("r-bp", "bp", [{ type: "shareholding", share: { exact: 19.75 } }]),
    rel("r-old", "old", [{ type: "shareholding", startDate: "2003-05-12", endDate: "2017-12-27" }], "closed"),
  ];
  const model = bodsToGraph(statements as never, { asOf: "2026-11-11" });

  it("labels the undated edge and nothing else", () => {
    const bp = model.edges.find((e) => e.source === "bp")!;
    const old = model.edges.find((e) => e.source === "old")!;
    expect(bp.undated).toBe(true);
    expect(bp.label.split("\n")).toEqual(["Owns 19.75%", UNDATED_LABEL]);
    expect(old.undated).toBeUndefined();
    expect(old.ended).toBe(true);
  });

  it("puts it in the tree's one line", () => {
    const rows = buildTree(model, new Set());
    // The tree hangs the subject under each owner; the first is BP's row.
    const bp = rows.find((r) => r.id === "rosneft" && r.interestLabel?.startsWith("Owns 19.75%"))!;
    expect(bp.interestUndated).toBe(true);
    expect(interestCellText(bp.interestLabel, bp.interestEnded, bp.interestEndedOn, bp.interestUndated)).toBe(
      `Owns 19.75% · ${UNDATED_LABEL}`
    );
  });

  it("names it in the legend only when drawn", () => {
    const base = { edgeCategories: ["ownership"], signalsByNode: new Map(), hasPeople: false, hasCollapsed: false, signalName: (c: string) => c };
    expect(buildGraphLegend({ ...base, hasUndated: true }).edgeModifiers.map((m) => m.key)).toEqual(["undated"]);
    expect(buildGraphLegend(base).edgeModifiers).toEqual([]);
  });
});
