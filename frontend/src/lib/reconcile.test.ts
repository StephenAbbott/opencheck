import { describe, it, expect } from "vitest";
import { reconcileBods, remapSignals, possiblySameAs } from "./reconcile";
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

  it("merges on a shared VAT number (scheme-scoped), no LEI needed", () => {
    const { statements } = reconcileBods([
      {
        statementId: "g", recordType: "entity",
        recordDetails: { name: "Müller GmbH", jurisdiction: { code: "DE" },
          identifiers: [{ scheme: "XI-VAT", id: "DE123456789" }] },
        source: { description: "GLEIF" },
      },
      {
        statementId: "o", recordType: "entity",
        recordDetails: { name: "MULLER GMBH", jurisdiction: { code: "DE" },
          identifiers: [{ scheme: "XI-VAT", id: "DE123456789" }] },
        source: { description: "OpenCorporates" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(1);
  });

  it("does not merge a VAT number that collides with a company number (same jurisdiction)", () => {
    // Coincidental bare-value collision across identifier TYPES must not merge.
    const { statements } = reconcileBods([
      {
        statementId: "a", recordType: "entity",
        recordDetails: { name: "Alpha A/S", jurisdiction: { code: "DK" },
          identifiers: [{ scheme: "DK-CVR", id: "12345678" }] },
        source: { description: "CVR" },
      },
      {
        statementId: "b", recordType: "entity",
        recordDetails: { name: "Beta A/S", jurisdiction: { code: "DK" },
          identifiers: [{ scheme: "DK-VAT", id: "12345678" }] },
        source: { description: "GLEIF" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(2);
  });

  // ── Identifier-TYPE collision guards (issue #25, item 1) ─────────────────
  // The jurisdiction bridge is whitelist-gated to national-register schemes:
  // a tax / VAT / CIK number that coincidentally equals a different entity's
  // company number in the same jurisdiction must never merge them.

  it("does not merge a Polish tax id (PL-NIP) with a same-digits register number (PL-KRS)", () => {
    // NIP and KRS are both 10 bare digits — the canonical cross-type collision.
    const { statements } = reconcileBods([
      {
        statementId: "krs", recordType: "entity",
        recordDetails: { name: "Spółka Jedna", jurisdiction: { code: "PL" },
          identifiers: [{ scheme: "PL-KRS", id: "0000123456" }] },
        source: { description: "KRS" },
      },
      {
        statementId: "nip", recordType: "entity",
        recordDetails: { name: "Spółka Druga", jurisdiction: { code: "PL" },
          identifiers: [{ scheme: "PL-NIP", id: "0000123456" }] },
        source: { description: "OpenCorporates" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(2);
  });

  it("does not merge a US SEC CIK with a same-digits state registration number", () => {
    const { statements } = reconcileBods([
      {
        statementId: "cik", recordType: "entity",
        recordDetails: { name: "Listed Corp", jurisdiction: { code: "US" },
          identifiers: [{ scheme: "US-SEC-CIK", id: "313807" }] },
        source: { description: "SEC EDGAR" },
      },
      {
        statementId: "state", recordType: "entity",
        // Unschemed number from GLEIF registeredAs — a state registry number.
        recordDetails: { name: "Unrelated LLC", jurisdiction: { code: "US" },
          identifiers: [{ scheme: "", id: "313807" }] },
        source: { description: "GLEIF" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(2);
  });

  it("does not bridge AT-UID (a VAT scheme that doesn't contain the string 'VAT')", () => {
    const { statements } = reconcileBods([
      {
        statementId: "fb", recordType: "entity",
        recordDetails: { name: "Alpen GmbH", jurisdiction: { code: "AT" },
          identifiers: [{ scheme: "AT-FB", id: "123456" }] },
        source: { description: "Firmenbuch" },
      },
      {
        statementId: "uid", recordType: "entity",
        recordDetails: { name: "Anders GmbH", jurisdiction: { code: "AT" },
          identifiers: [{ scheme: "AT-UID", id: "123456" }] },
        source: { description: "OpenCorporates" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(2);
  });

  it("bridges the same register number under different register labels (CA-CORP ↔ CA-CC)", () => {
    // Verified live on Canada Basketball: Corporations Canada emits CA-CORP,
    // OpenCorporates passes through CA-CC — same number, same entity. The
    // non-register denylist must not break this (a whitelist would have).
    const { statements } = reconcileBods([
      {
        statementId: "cc-corp", recordType: "entity",
        recordDetails: { name: "CANADA BASKETBALL", jurisdiction: { code: "CA" },
          identifiers: [{ scheme: "CA-CORP", id: "0343587" }] },
        source: { description: "Corporations Canada" },
      },
      {
        statementId: "oc", recordType: "entity",
        recordDetails: { name: "CANADA BASKETBALL", jurisdiction: { code: "CA" },
          identifiers: [{ scheme: "CA-CC", id: "0343587" }] },
        source: { description: "OpenCorporates" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(1);
  });

  it("does not bridge CA-BN (tax) but keeps NZ-NZBN (register) — segment matching, not substring", () => {
    const { statements } = reconcileBods([
      // CA-BN vs CA-CORP same digits → must NOT merge ("BN" segment is tax).
      {
        statementId: "ca-corp", recordType: "entity",
        recordDetails: { name: "Maple Corp", jurisdiction: { code: "CA" },
          identifiers: [{ scheme: "CA-CORP", id: "1067752" }] },
        source: { description: "Corporations Canada" },
      },
      {
        statementId: "ca-bn", recordType: "entity",
        recordDetails: { name: "Other Corp", jurisdiction: { code: "CA" },
          identifiers: [{ scheme: "CA-BN", id: "1067752" }] },
        source: { description: "OpenCorporates" },
      },
      // NZ-NZBN vs unschemed same digits → MUST merge (NZBN is the register
      // number; the "BN" rule must not substring-match it).
      {
        statementId: "nz-reg", recordType: "entity",
        recordDetails: { name: "Kiwi Ltd", jurisdiction: { code: "NZ" },
          identifiers: [{ scheme: "NZ-NZBN", id: "9429000000000" }] },
        source: { description: "NZ Companies" },
      },
      {
        statementId: "nz-gleif", recordType: "entity",
        recordDetails: { name: "KIWI LIMITED", jurisdiction: { code: "NZ" },
          identifiers: [{ scheme: "", id: "9429000000000" }] },
        source: { description: "GLEIF" },
      },
    ]);
    const ents = statements.filter((s) => s.recordType === "entity");
    // 4 raw → 3: the NZ pair merges, the CA pair stays apart.
    expect(ents).toHaveLength(3);
  });

  it("still bridges an unschemed registeredAs number to its register scheme", () => {
    // The whitelist must not break the bridge's purpose: "" (GLEIF) ↔ DK-CVR.
    const { statements } = reconcileBods([
      {
        statementId: "gleif", recordType: "entity",
        recordDetails: { name: "Novo Nordisk A/S", jurisdiction: { code: "DK" },
          identifiers: [{ scheme: "", id: "24256790" }] },
        source: { description: "GLEIF" },
      },
      {
        statementId: "cvr", recordType: "entity",
        recordDetails: { name: "NOVO NORDISK A/S", jurisdiction: { code: "DK" },
          identifiers: [{ scheme: "DK-CVR", id: "24256790" }] },
        source: { description: "CVR" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(1);
  });

  it("identical scheme+value still merges even for non-register schemes", () => {
    // Scheme-scoped merging is untouched by the bridge whitelist: two sources
    // asserting the SAME US-SEC-CIK are the same entity.
    const { statements } = reconcileBods([
      {
        statementId: "a", recordType: "entity",
        recordDetails: { name: "Listed Corp", jurisdiction: { code: "US" },
          identifiers: [{ scheme: "US-SEC-CIK", id: "313807" }] },
        source: { description: "SEC EDGAR" },
      },
      {
        statementId: "b", recordType: "entity",
        recordDetails: { name: "Listed Corp.", jurisdiction: { code: "US" },
          identifiers: [{ scheme: "US-SEC-CIK", id: "313807" }] },
        source: { description: "Wikirate" },
      },
    ]);
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(1);
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

describe("possiblySameAs", () => {
  const ent = (id: string, name: string, jur: string, date?: string): Stmt => ({
    statementId: id,
    recordType: "entity",
    recordDetails: {
      name,
      jurisdiction: { code: jur },
      ...(date ? { foundingDate: date } : {}),
      identifiers: [],
    },
  });

  it("flags same normalised name + jurisdiction (no shared id)", () => {
    const c = possiblySameAs([ent("a", "Acme Ltd", "GB", "1990-01-01"), ent("b", "ACME LTD.", "GB", "1990-06-02")]);
    expect(c).toHaveLength(1);
    expect([c[0].a, c[0].b].sort()).toEqual(["a", "b"]);
  });

  it("does not flag the same name in different jurisdictions", () => {
    expect(possiblySameAs([ent("a", "Acme Ltd", "GB"), ent("b", "Acme Ltd", "US")])).toHaveLength(0);
  });

  it("does not flag distinct same-name entities incorporated in different years (tiebreaker)", () => {
    expect(possiblySameAs([ent("a", "Acme Ltd", "GB", "1990-01-01"), ent("b", "Acme Ltd", "GB", "2005-01-01")])).toHaveLength(0);
  });

  it("treats a missing founding date as compatible", () => {
    expect(possiblySameAs([ent("a", "Acme Ltd", "GB", "1990"), ent("b", "Acme Ltd", "GB")])).toHaveLength(1);
  });

  it("does not flag genuinely different names (Alpha vs Beta)", () => {
    expect(possiblySameAs([
      ent("a", "BP Exploration (Alpha) Limited", "GB", "1990"),
      ent("b", "BP Exploration (Beta) Limited", "GB", "1990"),
    ])).toHaveLength(0);
  });

  it("flags two same-name entities that reconcileBods kept separate (different LEIs)", () => {
    const lei1 = "529900T8BM49AURSDO55";
    const lei2 = "213800LH1BZH3DI6G760";
    const { statements } = reconcileBods([
      {
        statementId: "x", recordType: "entity",
        recordDetails: { name: "Globex Holdings", jurisdiction: { code: "FR" }, foundingDate: "2001",
          identifiers: [{ scheme: "XI-LEI", id: lei1 }] },
        source: { description: "GLEIF" },
      },
      {
        statementId: "y", recordType: "entity",
        recordDetails: { name: "GLOBEX HOLDINGS", jurisdiction: { code: "FR" }, foundingDate: "2001",
          identifiers: [{ scheme: "XI-LEI", id: lei2 }] },
        source: { description: "OpenCorporates" },
      },
    ]);
    // different LEIs -> two canonical nodes; same name+jurisdiction+year -> a candidate
    expect(statements.filter((s) => s.recordType === "entity")).toHaveLength(2);
    expect(possiblySameAs(statements)).toHaveLength(1);
  });
});
