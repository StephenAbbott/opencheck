import { describe, it, expect } from "vitest";
import { reconcileBods, remapSignals } from "./reconcile";
import { bodsToGraph } from "./bodsGraph";
import type { RiskSignal } from "./api";

type Stmt = Record<string, unknown>;

const LEI = "529900T8BM49AURSDO55";
const OWNER_LEI = "529900OWNER00ACME001"; // valid LEI shape (18 alnum + 2 digits)

// The same company asserted by two sources (different statementIds, same LEI),
// each with one owner, plus a person owner on the GLEIF copy.
function twoSourceBundle(): Stmt[] {
  return [
    {
      statementId: "gleif-acme",
      recordType: "entity",
      recordDetails: { name: "Acme Ltd", identifiers: [{ scheme: "XI-LEI", id: LEI }] },
      source: { description: "GLEIF" },
    },
    {
      statementId: "ch-acme",
      recordType: "entity",
      recordDetails: {
        name: "ACME LIMITED",
        identifiers: [
          { scheme: "GB-COH", id: "00102498" },
          { scheme: "XI-LEI", id: LEI },
        ],
      },
      source: { description: "Companies House" },
    },
    {
      statementId: "gleif-owner",
      recordType: "entity",
      recordDetails: { name: "Acme Holdings", identifiers: [{ scheme: "XI-LEI", id: OWNER_LEI }] },
      source: { description: "GLEIF" },
    },
    {
      statementId: "rel-gleif",
      recordType: "relationship",
      recordDetails: {
        subject: "gleif-acme",
        interestedParty: "gleif-owner",
        interests: [{ type: "shareholding" }],
      },
      source: { description: "GLEIF" },
    },
    // Companies House asserts the SAME ownership (owner→acme) — should dedupe.
    {
      statementId: "rel-ch",
      recordType: "relationship",
      recordDetails: {
        subject: "ch-acme",
        interestedParty: "gleif-owner",
        interests: [{ type: "shareholding" }],
      },
      source: { description: "Companies House" },
    },
  ];
}

describe("reconcileBods", () => {
  it("merges entity statements sharing an LEI into one canonical node", () => {
    const { statements, remap } = reconcileBods(twoSourceBundle());
    const entities = statements.filter((s) => s.recordType === "entity");
    // Acme's two copies collapse to one; the owner stays distinct → 2 entities.
    expect(entities).toHaveLength(2);
    const canonical = `recon:LEI:${LEI}`;
    expect(remap["gleif-acme"]).toBe(canonical);
    expect(remap["ch-acme"]).toBe(canonical);
    const merged = entities.find((e) => e.statementId === canonical)!;
    expect(merged).toBeDefined();
    // Identifiers from both copies are unioned (LEI + company number).
    const ids = (merged.recordDetails as Stmt).identifiers as Stmt[];
    expect(ids.map((i) => i.id)).toEqual(expect.arrayContaining([LEI, "00102498"]));
    // Provenance: both sources stamped.
    expect(merged._sources).toEqual(expect.arrayContaining(["GLEIF", "Companies House"]));
  });

  it("remaps relationship endpoints to the canonical id and dedupes the edge", () => {
    const { statements } = reconcileBods(twoSourceBundle());
    const rels = statements.filter((s) => s.recordType === "relationship");
    // The two identical ownership statements (one per source) collapse to one.
    expect(rels).toHaveLength(1);
    const rel = rels[0];
    const rd = rel.recordDetails as Stmt;
    expect(rd.subject).toBe(`recon:LEI:${LEI}`);
    // The owner is a singleton entity but still gets its own canonical id.
    expect(rd.interestedParty).toBe(`recon:LEI:${OWNER_LEI}`);
    // Both contributing sources are pooled on the surviving edge.
    expect(rel._sources).toEqual(expect.arrayContaining(["GLEIF", "Companies House"]));
  });

  it("produces a graph where the merged node carries both sources", () => {
    const { statements } = reconcileBods(twoSourceBundle());
    const model = bodsToGraph(statements);
    const acme = model.nodes.find((n) => n.id === `recon:LEI:${LEI}`)!;
    expect(acme).toBeDefined();
    expect(acme.sources.sort()).toEqual(["Companies House", "GLEIF"]);
    // One ownership edge, pooling both sources.
    const ownEdges = model.edges.filter((e) => e.target === acme.id);
    expect(ownEdges).toHaveLength(1);
    expect(ownEdges[0].sources.sort()).toEqual(["Companies House", "GLEIF"]);
  });

  it("does not merge entities with different identifiers", () => {
    const { statements } = reconcileBods([
      {
        statementId: "a",
        recordType: "entity",
        recordDetails: { name: "A", identifiers: [{ scheme: "GB-COH", id: "111" }] },
        source: { description: "S1" },
      },
      {
        statementId: "b",
        recordType: "entity",
        recordDetails: { name: "B", identifiers: [{ scheme: "GB-COH", id: "222" }] },
        source: { description: "S2" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(2);
  });

  it("merges a national reg. number tagged with different schemes by jurisdiction", () => {
    // Real Novo Nordisk case: DK company no. 24256790 arrives as scheme ""
    // (GLEIF), DK-COA (OpenCorporates) and DK-CVR (CVR) — no shared LEI on the
    // CVR copy, so only a jurisdiction+value key collapses them.
    const { statements } = reconcileBods([
      {
        statementId: "gleif",
        recordType: "entity",
        recordDetails: {
          name: "NOVO NORDISK A/S",
          jurisdiction: { code: "DK" },
          identifiers: [
            { id: "549300DAQ1CVT6CXN342", scheme: "XI-LEI" },
            { id: "24256790", scheme: "" },
          ],
        },
        source: { description: "GLEIF" },
      },
      {
        statementId: "cvr",
        recordType: "entity",
        recordDetails: {
          name: "NOVO NORDISK A/S",
          jurisdiction: { code: "DK" },
          identifiers: [{ id: "24256790", scheme: "DK-CVR" }],
        },
        source: { description: "CVR" },
      },
    ]);
    const entities = statements.filter((s) => s.recordType === "entity");
    expect(entities).toHaveLength(1);
    expect(entities[0]._sources).toEqual(expect.arrayContaining(["GLEIF", "CVR"]));
  });

  it("does not cross-merge same-value ids in different jurisdictions", () => {
    const { statements } = reconcileBods([
      {
        statementId: "dk",
        recordType: "entity",
        recordDetails: { name: "DK Co", jurisdiction: { code: "DK" }, identifiers: [{ id: "100", scheme: "DK-CVR" }] },
        source: { description: "S1" },
      },
      {
        statementId: "no",
        recordType: "entity",
        recordDetails: { name: "NO Co", jurisdiction: { code: "NO" }, identifiers: [{ id: "100", scheme: "NO-BRC" }] },
        source: { description: "S2" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(2);
  });

  it("leaves persons and identifier-less entities untouched", () => {
    const { statements, remap } = reconcileBods([
      { statementId: "p1", recordType: "person", recordDetails: { name: "Jane" }, source: { description: "S1" } },
      { statementId: "e-noid", recordType: "entity", recordDetails: { name: "Mystery" }, source: { description: "S1" } },
    ]);
    expect(remap["p1"]).toBeUndefined();
    // identifier-less entity gets its own group (canonical = recon:<root>).
    const person = statements.find((s) => s.statementId === "p1");
    expect(person).toBeDefined();
    expect(person!._sources).toEqual(["S1"]);
  });
});

describe("remapSignals", () => {
  it("rewrites evidence statement ids onto the merged node", () => {
    const remap = { "gleif-acme": "recon:LEI:X", "ch-acme": "recon:LEI:X" };
    const signals = [
      { code: "SANCTIONED", evidence: { statement_id: "ch-acme" } },
    ] as unknown as RiskSignal[];
    const out = remapSignals(signals, remap);
    expect((out[0] as unknown as { evidence: { statement_id: string } }).evidence.statement_id).toBe(
      "recon:LEI:X"
    );
  });

  it("is a no-op when there is nothing to remap", () => {
    const signals = [{ code: "PEP", evidence: {} }] as unknown as RiskSignal[];
    expect(remapSignals(signals, {})).toBe(signals);
  });
});
