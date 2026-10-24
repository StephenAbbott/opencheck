/**
 * Relationships reference parties by recordId, and every consumer resolves them.
 *
 * The mirror of `backend/tests/test_record_id_references.py`. BODS v0.4 says
 * `subject` / `interestedParty` hold a party's recordId; OpenCheck's mappers
 * have kept `statementId === recordId`, so a consumer that compares a
 * reference against statementIds still happens to work. Phase B of the
 * record-ID ticket makes `statementId` content-addressed. This test scrambles
 * a realistic bundle into that shape — a different `statementId` on every
 * entity and person, the `recordId` and every reference untouched — and runs
 * each consumer on both, expecting the same answer once ids are mapped back.
 */
import { describe, it, expect } from "vitest";
import { bodsToGraph, buildTree, computeLevels } from "./bodsGraph";
import { reconcileBods } from "./reconcile";
import { extractConnectedPeople, extractPersonSubgraph } from "./backgroundCheck";
import { computeIntegratedOwnership, subjectIdsFor } from "./integratedOwnership";
import { refIndex } from "./bodsRefs";

type Stmt = Record<string, unknown>;

const LEI = "213800F4ETX85XLF5K47";
const PARENT_LEI = "724500KDSUZ5NW8YBM07";
const HEAD_LEI = "21380068P1DRHMJ8KU70";
const PREFIX = "ver1-";

function entity(id: string, source: string, name: string, code: string, identifiers: Stmt[], type = "registeredEntity"): Stmt {
  return {
    statementId: id, recordId: id, declarationSubject: id, recordType: "entity", recordStatus: "new",
    statementDate: "2026-10-23",
    publicationDetails: { bodsVersion: "0.4", publicationDate: "2026-10-23", publisher: { name: "OpenCheck" } },
    source: { description: source, opencheckSourceId: source.toLowerCase() },
    recordDetails: { name, entityType: { type }, jurisdiction: { code, name: code }, identifiers },
  };
}

function person(id: string, source: string, name: string, birthDate: string): Stmt {
  return {
    statementId: id, recordId: id, declarationSubject: id, recordType: "person", recordStatus: "new",
    statementDate: "2026-10-23",
    publicationDetails: { bodsVersion: "0.4", publicationDate: "2026-10-23", publisher: { name: "OpenCheck" } },
    source: { description: source, opencheckSourceId: source.toLowerCase() },
    recordDetails: { personType: "knownPerson", names: [{ type: "legal", fullName: name }], birthDate },
  };
}

function rel(id: string, source: string, subject: Stmt, party: Stmt, interests: Stmt[], extra: Stmt = {}): Stmt {
  return {
    statementId: id, recordId: `rec-${id}`, declarationSubject: subject.recordId, recordType: "relationship",
    recordStatus: "new", statementDate: "2026-10-23",
    publicationDetails: { bodsVersion: "0.4", publicationDate: "2026-10-23", publisher: { name: "OpenCheck" } },
    source: { description: source, opencheckSourceId: source.toLowerCase() },
    recordDetails: { subject: subject.recordId, interestedParty: party.recordId, interests, ...extra },
  };
}

function bundle(): Stmt[] {
  const subjectG = entity("g-subject", "GLEIF", "A/S NORSKE SHELL", "NO", [
    { scheme: "XI-LEI", id: LEI }, { scheme: "NO-BRC", id: "914807077" },
  ]);
  const parentG = entity("g-parent", "GLEIF", "SHELL PETROLEUM N.V.", "NL", [{ scheme: "XI-LEI", id: PARENT_LEI }]);
  const headG = entity("g-head", "GLEIF", "SHELL PLC", "GB", [{ scheme: "XI-LEI", id: HEAD_LEI }]);
  const subjectB = entity("b-subject", "Brreg", "A/S NORSKE SHELL", "NO", [{ scheme: "NO-BRC", id: "914807077" }]);
  const kari = person("b-p1", "Brreg", "Kari Nordmann", "1970-05");
  const state = entity("b-state", "Brreg", "NÆRINGS- OG FISKERIDEPARTEMENTET", "NO", [{ scheme: "NO-BRC", id: "912660680" }], "stateBody");
  const former = entity("b-former", "Brreg", "OLD HOLDINGS AS", "NO", [{ scheme: "NO-BRC", id: "900000001" }]);
  const rDirect = rel("g-r1", "GLEIF", subjectG, parentG, [{ type: "otherInfluenceOrControl", directOrIndirect: "direct" }]);
  return [
    subjectG, parentG, headG,
    rDirect,
    rel("g-r2", "GLEIF", subjectG, headG, [{ type: "otherInfluenceOrControl", directOrIndirect: "indirect" }],
      { componentRecords: [rDirect.recordId, parentG.recordId] }),
    rel("g-r3", "GLEIF", parentG, headG, [{ type: "otherInfluenceOrControl", directOrIndirect: "direct" }]),
    subjectB, kari, state, former,
    rel("b-r1", "Brreg", subjectB, kari, [{ type: "shareholding", share: { exact: 10 }, beneficialOwnershipOrControl: true }]),
    rel("b-r2", "Brreg", subjectB, state, [{ type: "shareholding", share: { exact: 67 } }]),
    rel("b-r3", "Brreg", subjectB, former, [{ type: "shareholding", share: { exact: 50 }, startDate: "2010-01-01", endDate: "2020-01-01" }]),
  ];
}

/** Phase B's shape: a new statementId on every party, recordId and references untouched. */
function scramble(statements: Stmt[]): Stmt[] {
  return statements.map((s) =>
    s.recordType === "entity" || s.recordType === "person" ? { ...s, statementId: PREFIX + (s.statementId as string) } : { ...s },
  );
}

function unscramble<T>(value: T): T {
  if (typeof value === "string") return value.split(PREFIX).join("") as T;
  if (Array.isArray(value)) return value.map(unscramble) as T;
  if (value instanceof Map) return new Map([...value].map(([k, v]) => [unscramble(k), unscramble(v)])) as T;
  if (value instanceof Set) return new Set([...value].map(unscramble)) as T;
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([k, v]) => [unscramble(k), unscramble(v)])) as T;
  }
  return value;
}

const leiOf = (s: Stmt): string | null => {
  const ids = ((s.recordDetails as Stmt)?.identifiers ?? []) as Stmt[];
  return (ids.find((i) => i.scheme === "XI-LEI")?.id as string) ?? null;
};

describe("relationships reference parties by recordId (the phase B shape)", () => {
  it("the fixture references every party by recordId, and the resolver finds them in both shapes", () => {
    for (const b of [bundle(), scramble(bundle())]) {
      const idx = refIndex(b);
      for (const s of b) {
        if (s.recordType !== "relationship") continue;
        const rd = s.recordDetails as Stmt;
        expect(idx.has(rd.subject as string)).toBe(true);
        expect(idx.has(rd.interestedParty as string)).toBe(true);
      }
    }
  });

  it("the graph draws the same nodes and edges", () => {
    const plain = bodsToGraph(bundle());
    const scr = unscramble(bodsToGraph(scramble(bundle())));
    expect(scr.nodes.map((n) => n.id).sort()).toEqual(plain.nodes.map((n) => n.id).sort());
    expect(scr.edges.map((e) => `${e.source}>${e.target}:${e.category}:${e.label}`).sort())
      .toEqual(plain.edges.map((e) => `${e.source}>${e.target}:${e.category}:${e.label}`).sort());
    expect(plain.edges.length).toBeGreaterThanOrEqual(5);
  });

  it("the text tree lists the same rows at the same levels", () => {
    const run = (b: Stmt[]) => {
      const model = bodsToGraph(b);
      return { tree: buildTree(model, new Set()), levels: computeLevels(model) };
    };
    expect(unscramble(run(scramble(bundle())))).toEqual(run(bundle()));
  });

  it("the reconciler merges the same statements and remaps the same edges", () => {
    const plain = reconcileBods(bundle());
    const scr = unscramble(reconcileBods(scramble(bundle())));
    expect(scr.statements).toEqual(plain.statements);
    expect(scr.remap).toEqual(plain.remap);
  });

  it("BackgroundCheck finds the same people with the same roles", () => {
    const plain = extractConnectedPeople(bundle());
    const scr = unscramble(extractConnectedPeople(scramble(bundle())));
    expect(scr).toEqual(plain);
    expect(plain.length).toBe(1);
    const personIds = (b: Stmt[]) => b.filter((s) => s.recordType === "person").map((s) => s.statementId as string);
    expect(unscramble(extractPersonSubgraph(scramble(bundle()), personIds(scramble(bundle())))))
      .toEqual(extractPersonSubgraph(bundle(), personIds(bundle())));
  });

  it("integrated ownership computes the same figures", () => {
    const run = (b: Stmt[]) => computeIntegratedOwnership(b, subjectIdsFor(b, LEI, leiOf), "2026-10-23");
    expect(unscramble(run(scramble(bundle())))).toEqual(run(bundle()));
  });
});
