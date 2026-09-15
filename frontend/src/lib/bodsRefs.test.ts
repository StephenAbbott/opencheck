import { describe, expect, it } from "vitest";
import { bodsToGraph, buildTree } from "./bodsGraph";
import { partyRef, refIndex, resolveRef } from "./bodsRefs";
import { extractConnectedPeople } from "./backgroundCheck";
import { reconcileBods } from "./reconcile";

// The shape the OECD publishes (Phase 208): statementId is a hash, recordId is
// `meip-entity-N`, the relationship references the recordIds — as BODS v0.4
// says — and declarationSubject is the group *head* on every statement. Every OpenCheck mapper sets the two equal, which is why this was
// the first bundle whose edge did not draw (Phase 210).
const HEAD = {
  statementId: "3f6e3c2a-7b1e-5c9a-9d0b-2e4f8a1c6d70",
  recordId: "meip-entity-100913",
  recordType: "entity",
  declarationSubject: "meip-entity-100913",
  recordDetails: { name: "SHELL PLC", jurisdiction: { code: "GB" }, identifiers: [{ id: "21380068P1DRHMJ8KU70", scheme: "XI-LEI" }] },
};
const MEMBER = {
  statementId: "a1d2b8ee-4c55-5a1a-8e37-0c9b2f0d1a11",
  recordId: "meip-entity-100959",
  recordType: "entity",
  // The OECD stamps the group head on every statement, the member's included.
  declarationSubject: "meip-entity-100913",
  recordDetails: { name: "A/S Norske Shell", jurisdiction: { code: "NO" }, identifiers: [{ id: "213800F4ETX85XLF5K47", scheme: "XI-LEI" }] },
};
const EDGE = {
  statementId: "c0ffee00-1111-5222-8333-444455556666",
  recordId: "meip-rel-100959",
  recordType: "relationship",
  declarationSubject: "meip-entity-100959",
  recordDetails: {
    subject: "meip-entity-100959",
    interestedParty: "meip-entity-100913",
    interests: [{ type: "unknownInterest", directOrIndirect: "unknown", details: "Hierarchy: Known" }],
  },
  source: { description: "OECD-UNSD MEIP" },
};
const MEIP = [MEMBER, EDGE, HEAD];

describe("refIndex / resolveRef", () => {
  it("maps recordId, declarationSubject and statementId to the statementId", () => {
    const idx = refIndex(MEIP);
    expect(idx.get("meip-entity-100913")).toBe(HEAD.statementId);
    expect(idx.get(HEAD.statementId)).toBe(HEAD.statementId);
    expect(resolveRef("meip-entity-100959", idx)).toBe(MEMBER.statementId);
    expect(resolveRef({ describedByEntityStatement: "meip-entity-100913" }, idx)).toBe(HEAD.statementId);
  });

  it("leaves an unknown reference as it is, and an unspecified record as nothing", () => {
    const idx = refIndex(MEIP);
    expect(resolveRef("meip-entity-999", idx)).toBe("meip-entity-999");
    expect(resolveRef({ reason: "noBeneficialOwners" }, idx)).toBeUndefined();
    expect(partyRef("")).toBeUndefined();
  });

  it("ranks declarationSubject below recordId — the OECD's alias names the head everywhere", () => {
    const idx = refIndex([MEMBER, HEAD]);
    expect(idx.get("meip-entity-100913")).toBe(HEAD.statementId);
  });

  it("never lets one statement's recordId shadow another's statementId", () => {
    const idx = refIndex([
      { statementId: "X", recordId: "Y", recordType: "entity", recordDetails: {} },
      { statementId: "Y", recordId: "Y", recordType: "entity", recordDetails: {} },
    ]);
    expect(idx.get("Y")).toBe("Y");
  });
});

describe("the OECD's statements draw as one edge, not two islands", () => {
  it("bodsToGraph links member to head through the recordId references", () => {
    const g = bodsToGraph(MEIP);
    expect(g.nodes.map((n) => n.label).sort()).toEqual(["A/S Norske Shell", "SHELL PLC"]);
    expect(g.edges).toHaveLength(1);
    expect(g.edges[0].source).toBe(HEAD.statementId);
    expect(g.edges[0].target).toBe(MEMBER.statementId);
  });

  it("buildTree hangs the member under the head rather than isolating both", () => {
    const rows = buildTree(bodsToGraph(MEIP), new Set<string>());
    expect(rows.some((r) => r.isolated)).toBe(false);
  });

  it("reconcileBods keeps the edge resolvable after the merge", () => {
    const g = bodsToGraph(reconcileBods(MEIP).statements);
    expect(g.edges).toHaveLength(1);
  });

  it("extractConnectedPeople reads a person's role through a recordId reference", () => {
    const person = {
      statementId: "p-hash",
      recordId: "pub-person-1",
      recordType: "person",
      recordDetails: { personType: "knownPerson", names: [{ fullName: "Ada Board" }] },
    };
    const role = {
      statementId: "r-hash",
      recordId: "pub-rel-1",
      recordType: "relationship",
      recordDetails: {
        subject: "meip-entity-100913",
        interestedParty: "pub-person-1",
        interests: [{ type: "seniorManagingOfficial" }],
      },
      source: { description: "A register" },
    };
    const people = extractConnectedPeople([HEAD, person, role]);
    expect(people).toHaveLength(1);
    expect(people[0].roles[0].subjectName).toBe("SHELL PLC");
  });
});
