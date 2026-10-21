import { describe, expect, it } from "vitest";
import {
  extractConnectedPeople,
  groupPersonRecords,
  type Stmt,
} from "./backgroundCheck";
import { clusterConnectedPeople } from "./clusterPeople";

/**
 * The person-merge rules from the "BackgroundCheck person-source data quality"
 * ticket (20 Oct 2026): identifiers first; name + birth year only when there
 * is a birth year to agree on; a name alone never merges across sources; two
 * different values of a one-person-per-value scheme are never merged.
 */

function person(
  id: string,
  fullName: string,
  opts: {
    year?: number;
    source?: string;
    sourceId?: string;
    ids?: { scheme: string; id: string }[];
    notes?: { scheme: string; id: string }[];
  } = {}
): Stmt {
  return {
    statementId: id,
    recordType: "person",
    source: { description: opts.source ?? "UK Companies House", ...(opts.sourceId ? { opencheckSourceId: opts.sourceId } : {}) },
    recordDetails: {
      personType: "knownPerson",
      names: [{ fullName }],
      ...(opts.year ? { birthDate: String(opts.year) } : {}),
      ...(opts.ids ? { identifiers: opts.ids } : {}),
    },
    ...(opts.notes
      ? {
          annotations: opts.notes.map((n) => ({
            statementPointerTarget: "/recordDetails",
            motivation: "identifying",
            description: `Identifier ${n.id} (scheme ${n.scheme}; register person key). BODS keeps a person's identifiers for identity documents, so this one is published here: it says which record the statement was matched on, not who the person is.`,
          })),
        }
      : {}),
  };
}

const EE = { source: "Estonian e-Business Register", sourceId: "ariregister" };
const hash = (n: number) => ({ scheme: "EE-ARIREGISTER-HASH", id: `00000000-0000-4000-8000-00000000000${n}` });

describe("Wikidata bare-name records (Unilever's Fernando Fernández)", () => {
  // Two Wikidata items found by name — a Mexican actor and another person —
  // plus the real CFO from Companies House and OpenCorporates, with a year.
  const bundle = [
    person("oc", "FERNANDO FERNANDEZ", { year: 1966, source: "OpenCorporates" }),
    person("ch", "FERNANDEZ, Fernando", { year: 1966 }),
    person("wd-1", "Fernando Fernández", { source: "Wikidata", sourceId: "wikidata", ids: [{ scheme: "WIKIDATA", id: "Q5444704" }] }),
    person("wd-2", "Fernando Fernández", { source: "Wikidata", sourceId: "wikidata", ids: [{ scheme: "WIKIDATA", id: "Q204277" }] }),
  ];
  const people = extractConnectedPeople(bundle);

  it("does not merge two Wikidata items into one card", () => {
    expect(people).toHaveLength(3);
    const wd = people.filter((p) => p.sources.includes("Wikidata"));
    expect(wd).toHaveLength(2);
    expect(wd.map((p) => p.identifiers)).toEqual([["wikidata:q5444704"], ["wikidata:q204277"]]);
    expect(new Set(people.map((p) => p.key)).size).toBe(3);
  });

  it("still merges the register records that agree on the birth year", () => {
    const cfo = people.find((p) => p.birthYear === 1966);
    expect(cfo?.statementIds).toEqual(["oc", "ch"]);
  });

  it("never offers the two Wikidata items as a possible match for each other", () => {
    const { clusters } = clusterConnectedPeople(people);
    const pairs = clusters.flatMap((c) => c.pairs);
    const wdKeys = people.filter((p) => p.sources.includes("Wikidata")).map((p) => p.key);
    expect(pairs.some((p) => wdKeys.includes(p.aKey) && wdKeys.includes(p.bKey))).toBe(false);
    // Each is still offered, at Medium, against the real CFO for review.
    expect(pairs.length).toBe(2);
    expect(clusters.every((c) => c.confidence === "medium")).toBe(true);
  });

  it("does not merge different Wikidata items even when the birth years agree", () => {
    const out = extractConnectedPeople([
      person("a", "Jane Example", { year: 1970, source: "Wikidata", ids: [{ scheme: "WIKIDATA", id: "Q1" }] }),
      person("b", "Jane Example", { year: 1970, source: "Wikidata", ids: [{ scheme: "WIKIDATA", id: "Q2" }] }),
    ]);
    expect(out).toHaveLength(2);
  });
});

describe("a name alone does not merge across sources", () => {
  it("keeps same-name records with no birth year from two sources apart, for review", () => {
    const people = extractConnectedPeople([
      person("oc", "Jane Example", { source: "OpenCorporates" }),
      person("wd", "Jane Example", { source: "Wikidata" }),
    ]);
    expect(people).toHaveLength(2);
    expect(people[0].key).not.toBe(people[1].key);
    const { clusters } = clusterConnectedPeople(people);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].confidence).toBe("medium");
    expect(clusters[0].pairs[0].evidence).toMatch(/no birth year on either record/);
  });

  it("still merges one source naming one person twice with no birth year", () => {
    const people = extractConnectedPeople([
      person("a", "Jane Example", { source: "New Zealand Companies Register (NZBN)" }),
      person("b", "EXAMPLE, Jane", { source: "New Zealand Companies Register (NZBN)" }),
    ]);
    expect(people).toHaveLength(1);
    expect(people[0].statementIds).toEqual(["a", "b"]);
  });

  it("does not merge a birth year on one side with none on the other (unchanged)", () => {
    const people = extractConnectedPeople([
      person("a", "Jane Example", { year: 1980 }),
      person("b", "Jane Example", { source: "Wikidata" }),
    ]);
    expect(people).toHaveLength(2);
  });
});

describe("the Estonian register's person key", () => {
  it("keeps two Estonians with the same name and different keys as two cards", () => {
    const people = extractConnectedPeople([
      person("ee-1", "Mari Tamm", { ...EE, notes: [hash(1)] }),
      person("ee-2", "Mari Tamm", { ...EE, notes: [hash(2)] }),
    ]);
    expect(people).toHaveLength(2);
    expect(clusterConnectedPeople(people).clusters).toHaveLength(0);
  });

  it("merges statements carrying the same key whatever the spelling", () => {
    const people = extractConnectedPeople([
      person("ee-1", "Mari Tamm", { ...EE, notes: [hash(1)] }),
      person("ee-2", "MARI-LIIS TAMM", { ...EE, notes: [hash(1)] }),
    ]);
    expect(people).toHaveLength(1);
    expect(people[0].identifiers).toEqual(["ee-ariregister-hash:00000000-0000-4000-8000-000000000001"]);
  });
});

describe("identifiers first", () => {
  it("merges records sharing an identifier even when the names differ", () => {
    const officer = { scheme: "GB-COH-OFFICER", id: "abc123" };
    const people = extractConnectedPeople([
      person("a", "SMITH, John Edward", { year: 1960, ids: [officer] }),
      person("b", "John Smith", { ids: [officer] }),
    ]);
    expect(people).toHaveLength(1);
    expect(people[0].birthYear).toBe(1960);
  });

  it("does not let a shared non-strict identifier bridge two different person keys", () => {
    const groups = groupPersonRecords([
      { nameKey: "a", identifiers: ["ee-ariregister-hash:1", "gb-coh-officer:x"] },
      { nameKey: "b", identifiers: ["ee-ariregister-hash:2", "gb-coh-officer:x"] },
    ]);
    expect(groups[0]).not.toBe(groups[1]);
  });

  it("does not let a name merge join a group to a different Wikidata item", () => {
    const groups = groupPersonRecords([
      { nameKey: "jane example", birthYear: 1970, identifiers: ["wikidata:q1"] },
      { nameKey: "jane example", birthYear: 1970, identifiers: [] },
      { nameKey: "jane example", birthYear: 1970, identifiers: ["wikidata:q2"] },
    ]);
    expect(groups[0]).toBe(groups[1]);
    expect(groups[2]).not.toBe(groups[0]);
  });
});
