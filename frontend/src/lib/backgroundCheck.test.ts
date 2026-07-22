import { describe, expect, it } from "vitest";
import {
  birthYearOf,
  describeRoleInterest,
  extractConnectedPeople,
} from "./backgroundCheck";

const entity = (id: string, name: string) => ({
  statementId: id,
  recordType: "entity",
  recordDetails: { name },
  source: { description: "UK Companies House" },
});

const person = (
  id: string,
  fullName: string,
  extra: Record<string, unknown> = {},
  source = "UK Companies House"
) => ({
  statementId: id,
  recordType: "person",
  recordDetails: {
    personType: "knownPerson",
    names: [{ type: "individual", fullName }],
    ...extra,
  },
  source: { description: source },
});

const rel = (
  id: string,
  subject: string,
  party: string,
  interests: Record<string, unknown>[],
  source = "UK Companies House"
) => ({
  statementId: id,
  recordType: "relationship",
  recordDetails: { subject, interestedParty: party, interests },
  source: { description: source },
});

describe("extractConnectedPeople", () => {
  it("extracts a person with roles, subject names and sources", () => {
    const bundle = [
      entity("e1", "Acme Ltd"),
      person("p1", "Jane Example", {
        birthDate: "1980-06",
        nationalities: [{ name: "British" }],
      }),
      rel("r1", "e1", "p1", [
        { type: "seniorManagingOfficial", details: "Director" },
      ]),
    ];
    const people = extractConnectedPeople(bundle);
    expect(people).toHaveLength(1);
    const jane = people[0];
    expect(jane.name).toBe("Jane Example");
    expect(jane.birthYear).toBe(1980);
    expect(jane.nationalities).toEqual(["British"]);
    expect(jane.roles).toEqual([
      {
        label: "Director",
        subjectName: "Acme Ltd",
        source: "UK Companies House",
        startDate: undefined,
        endDate: undefined,
      },
    ]);
  });

  it("merges the same person across sources by name + birth year", () => {
    const bundle = [
      entity("e1", "Acme Ltd"),
      person("p1", "Jane Example", { birthDate: "1980-06" }),
      person("p2", "JANE EXAMPLE", { birthDate: "1980" }, "UK PSC bulk"),
      rel("r1", "e1", "p1", [{ details: "Director" }]),
      rel("r2", "e1", "p2", [{ details: "Ownership of shares — 25-50%" }], "UK PSC bulk"),
    ];
    const people = extractConnectedPeople(bundle);
    expect(people).toHaveLength(1);
    expect(people[0].statementIds).toEqual(["p1", "p2"]);
    expect(people[0].sources).toEqual(["UK Companies House", "UK PSC bulk"]);
    expect(people[0].roles.map((r) => r.label)).toEqual([
      "Director",
      "Ownership of shares — 25-50%",
    ]);
  });

  it("keeps same-name people with different birth years separate", () => {
    const bundle = [
      person("p1", "Jane Example", { birthDate: "1980" }),
      person("p2", "Jane Example", { birthDate: "1955" }),
    ];
    expect(extractConnectedPeople(bundle)).toHaveLength(2);
  });

  it("skips anonymous and unknown persons", () => {
    const bundle = [
      person("p1", "Withheld", { personType: "anonymousPerson" }),
    ];
    // personType overrides in extra don't apply because person() sets it
    // first — build explicitly:
    bundle[0].recordDetails.personType = "anonymousPerson";
    expect(extractConnectedPeople(bundle)).toHaveLength(0);
  });

  it("ignores entity-to-entity relationships", () => {
    const bundle = [
      entity("e1", "Acme Ltd"),
      entity("e2", "Holding BV"),
      rel("r1", "e1", "e2", [{ details: "Ownership of shares" }]),
    ];
    expect(extractConnectedPeople(bundle)).toHaveLength(0);
  });

  it("handles v0.4-style statementType field names", () => {
    const bundle = [
      {
        statementId: "p1",
        statementType: "personStatement",
        recordDetails: {
          personType: "knownPerson",
          names: [{ fullName: "Ola Nordmann" }],
        },
      },
    ];
    const people = extractConnectedPeople(bundle);
    expect(people).toHaveLength(1);
    expect(people[0].name).toBe("Ola Nordmann");
  });
});

describe("describeRoleInterest", () => {
  it("prefers details", () => {
    expect(describeRoleInterest({ type: "shareholding", details: "Director" })).toBe(
      "Director"
    );
  });
  it("falls back to humanised type with share bands", () => {
    expect(
      describeRoleInterest({
        type: "shareholding",
        share: { minimum: 25, maximum: 50 },
      })
    ).toBe("shareholding — 25–50%");
  });
  it("handles exact shares", () => {
    expect(
      describeRoleInterest({ type: "shareholding", share: { exact: 100 } })
    ).toBe("shareholding — 100%");
  });
});

describe("birthYearOf", () => {
  it("parses YYYY and YYYY-MM", () => {
    expect(birthYearOf("1980")).toBe(1980);
    expect(birthYearOf("1980-06")).toBe(1980);
    expect(birthYearOf(undefined)).toBeUndefined();
  });
});

describe("possiblySamePeople", () => {
  const person = (key: string, name: string, birthYear?: number) => ({
    key,
    name,
    birthYear,
    birthDate: birthYear ? String(birthYear) : undefined,
    nationalities: [],
    statementIds: [key],
    sources: [],
    roles: [],
  });

  it("flags same-name pairs when a birth year is missing", async () => {
    const { possiblySamePeople } = await import("./backgroundCheck");
    const pairs = possiblySamePeople([
      person("a", "Jane Example", 1980),
      person("b", "JANE EXAMPLE"),
    ]);
    expect(pairs).toHaveLength(1);
    expect(pairs[0].reason).toContain("birth year missing on one record");
  });

  it("does not flag same-name pairs with conflicting birth years", async () => {
    const { possiblySamePeople } = await import("./backgroundCheck");
    const pairs = possiblySamePeople([
      person("a", "Jane Example", 1980),
      person("b", "Jane Example", 1955),
    ]);
    expect(pairs).toHaveLength(0);
  });

  it("does not flag different names", async () => {
    const { possiblySamePeople } = await import("./backgroundCheck");
    const pairs = possiblySamePeople([
      person("a", "Jane Example"),
      person("b", "John Other"),
    ]);
    expect(pairs).toHaveLength(0);
  });
});

describe("extractPersonSubgraph", () => {
  it("returns entities, person and relationships in reference order", async () => {
    const { extractPersonSubgraph } = await import("./backgroundCheck");
    const bundle = [
      entity("e1", "Acme Ltd"),
      entity("e2", "Unrelated BV"),
      person("p1", "Jane Example"),
      person("p2", "Someone Else"),
      rel("r1", "e1", "p1", [{ details: "Director" }]),
      rel("r2", "e2", "p2", [{ details: "Director" }]),
    ];
    const sub = extractPersonSubgraph(bundle, ["p1"]);
    expect(sub.map((s) => s.statementId)).toEqual(["e1", "p1", "r1"]);
  });

  it("includes all statements for a cross-source-merged person", async () => {
    const { extractPersonSubgraph } = await import("./backgroundCheck");
    const bundle = [
      entity("e1", "Acme Ltd"),
      person("p1", "Jane Example"),
      person("p2", "JANE EXAMPLE", {}, "UK PSC bulk"),
      rel("r1", "e1", "p1", [{ details: "Director" }]),
      rel("r2", "e1", "p2", [{ details: "PSC" }], "UK PSC bulk"),
    ];
    const sub = extractPersonSubgraph(bundle, ["p1", "p2"]);
    expect(sub.map((s) => s.statementId)).toEqual(["e1", "p1", "p2", "r1", "r2"]);
  });
});
