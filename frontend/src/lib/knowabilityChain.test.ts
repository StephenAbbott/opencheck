import { describe, expect, it } from "vitest";

import type { KnowabilityChain, KnowabilityStatement } from "./api";
import { chainCodes, chainSummary, mergeChain, statementJurisdiction } from "./knowabilityChain";

const LEI = "213800LH1BZH3DI6G760";

const entity = (id: string, code: string | null, lei?: string) => ({
  statementId: id,
  recordType: "entity",
  recordDetails: {
    ...(code ? { jurisdiction: { code } } : {}),
    identifiers: lei ? [{ scheme: "XI-LEI", id: lei }] : [],
  },
});
const person = (id: string) => ({ statementId: id, recordType: "person", recordDetails: {} });
// Edges run owner → owned.
const owns = (owner: string, owned: string, category = "ownership") => ({ source: owner, target: owned, category });

const stmt = (code: string, sentence = `${code} sentence.`): KnowabilityStatement => ({
  code,
  name: code,
  sentence,
  sentences: [sentence],
  stated_absence: false,
  review_status: "verified",
  fields: {},
  opencheck_reads: [],
  sources: [],
});

describe("statementJurisdiction", () => {
  it("reads the code, folds a subdivision to its country, skips people", () => {
    expect(statementJurisdiction(entity("a", "gb"))).toBe("GB");
    expect(statementJurisdiction(entity("a", "US-DE"))).toBe("US");
    expect(statementJurisdiction(person("p"))).toBeNull();
    expect(statementJurisdiction(entity("a", null))).toBeNull();
    expect(statementJurisdiction(entity("a", "not a code"))).toBeNull();
  });
  it("falls back to incorporatedInJurisdiction", () => {
    const s = { statementId: "a", recordType: "entity", recordDetails: { incorporatedInJurisdiction: { code: "KY" } } };
    expect(statementJurisdiction(s)).toBe("KY");
  });
});

describe("chainCodes (Phase 226)", () => {
  const statements = [
    entity("subj", "GB", LEI),
    entity("subj-coh", "GB", LEI), // the same company from Companies House, not yet reconciled
    entity("z-hold", "KY"),
    entity("a-top", "BM"),
    entity("former", "VG"),
    entity("sub", "FR"), // below the subject — never on the path
    person("p1"),
  ];
  const edges = [
    owns("z-hold", "subj-coh"),
    owns("a-top", "z-hold", "control"),
    owns("former", "subj"), // an ended link is still an edge; the walker does not care
    owns("subj", "sub"),
    owns("p1", "a-top"),
    { source: "a-top", target: "z-hold", category: "possiblySame" }, // review edge, ignored
  ];

  it("walks upward from every statement carrying the LEI, subject first, in path order", () => {
    // Rank 1 is the owners of both subject statements (VG via the GLEIF one,
    // KY via the Companies House one), rank 2 is BM above KY. Alphabetical
    // order would put BM (a-top) before KY (z-hold); path order does not.
    expect(chainCodes(statements, edges, LEI)).toEqual(["GB", "VG", "KY", "BM"]);
  });
  it("never includes what the subject owns", () => {
    expect(chainCodes(statements, edges, LEI)).not.toContain("FR");
  });
  it("is empty without a subject", () => {
    expect(chainCodes(statements, edges, null)).toEqual([]);
    expect(chainCodes(statements, edges, "529900T8BM49AURSDO55")).toEqual([]);
  });
});

describe("mergeChain", () => {
  const frozen: KnowabilityChain = {
    subject: "GB",
    codes: ["GB", "KY"],
    statements: [stmt("GB", "Frozen GB."), stmt("KY", "Frozen KY.")],
    as_of: "2026-09-16",
  };

  it("keeps the run's chain first and its sentences fixed, then appends what the graph found", () => {
    const fetched = new Map([["KY", stmt("KY", "Today's KY.")], ["BM", stmt("BM")]]);
    const out = mergeChain(frozen, ["GB", "KY", "BM", "VG"], fetched);
    expect(out.codes).toEqual(["GB", "KY", "BM", "VG"]);
    expect(out.statements.map((s) => s.sentence)).toEqual(["Frozen GB.", "Frozen KY.", "BM sentence."]);
    expect(out.pending).toEqual(["VG"]);
  });
  it("works with no frozen chain at all (an older saved report)", () => {
    const out = mergeChain(null, ["GB", "KY"], new Map());
    expect(out.codes).toEqual(["GB", "KY"]);
    expect(out.statements).toEqual([]);
    expect(out.pending).toEqual(["GB", "KY"]);
  });
});

describe("chainSummary", () => {
  it("counts jurisdictions other than the subject's", () => {
    expect(chainSummary(["GB", "KY", "BM"], "GB", false)).toBe(
      "The mapped ownership path runs through 2 other jurisdictions.",
    );
    expect(chainSummary(["GB", "KY"], "GB", false)).toBe("The mapped ownership path runs through 1 other jurisdiction.");
    expect(chainSummary(["GB"], "GB", false)).toBe(
      "Every company on the mapped path is in the subject's own jurisdiction.",
    );
    expect(chainSummary([], "GB", true)).toBe("Reading the ownership path.");
    expect(chainSummary([], "GB", false)).toBe("No ownership path mapped yet.");
  });
});
