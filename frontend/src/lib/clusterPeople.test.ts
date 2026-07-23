import { describe, it, expect } from "vitest";
import { clusterConnectedPeople, nameSimilarity, scorePair } from "./clusterPeople";
import type { ClusterablePerson } from "./clusterPeople";

/** Build a ConnectedPerson-like fixture. */
function P(
  name: string,
  birthYear: number | undefined,
  sources: string[],
  opts: { nat?: string[]; ids?: string[]; companies?: string[] } = {}
): ClusterablePerson {
  return {
    key: `${name.toLowerCase()}|${birthYear ?? ""}|${sources[0] ?? ""}`,
    name,
    birthYear,
    birthDate: birthYear ? String(birthYear) : undefined,
    nationalities: opts.nat ?? [],
    identifiers: opts.ids ?? [],
    roles: (opts.companies ?? []).map((c) => ({ label: "Director", subjectName: c })),
    sources,
    statementIds: [`${name}-${sources[0] ?? ""}`],
  };
}

describe("clusterConnectedPeople", () => {
  it("clusters a name variant (middle name) with matching year + nationality at High", () => {
    const { clusters, singletons } = clusterConnectedPeople([
      P("NELSON PELTZ", 1942, ["OpenCorporates"], { nat: ["American"] }),
      P("Nelson Augustus Peltz", 1942, ["Companies House"], { nat: ["American"] }),
    ]);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].size).toBe(2);
    expect(clusters[0].confidence).toBe("high");
    expect(clusters[0].pairs[0].evidence).toMatch(/middle name|initial/);
    expect(singletons).toHaveLength(0);
  });

  it("clusters an identical name with a missing birth year at Medium (today's flagged case)", () => {
    const { clusters } = clusterConnectedPeople([
      P("FERNANDO FERNANDEZ", 1966, ["OpenCorporates"]),
      P("Fernando Fernandez", undefined, ["Wikidata"]),
    ]);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].confidence).toBe("medium");
    expect(clusters[0].pairs[0].evidence).toMatch(/missing on one/);
  });

  it("surfaces a birth-year conflict instead of vetoing it (today's blind spot)", () => {
    const { clusters } = clusterConnectedPeople([
      P("Jane Smith", 1970, ["OpenCorporates"]),
      P("Jane Smith", 1971, ["OpenAleph"]),
    ]);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].confidence).toBe("medium");
    expect(clusters[0].pairs[0].evidence).toMatch(/birth years differ/);
  });

  it("lets a shared identifier dominate across a birth-year conflict (High)", () => {
    const { clusters } = clusterConnectedPeople([
      P("N. Peltz", 1942, ["OpenCorporates"], { ids: ["wikidata:q6990685"] }),
      P("Nelson Peltz", 1943, ["Wikidata"], { ids: ["wikidata:q6990685"] }),
    ]);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].confidence).toBe("high");
    expect(clusters[0].pairs[0].evidence).toMatch(/shared identifier/);
  });

  it("keeps a middle-initial variant of different people at Medium — never High on name alone", () => {
    const { clusters } = clusterConnectedPeople([
      P("Fernando Fernandez", 1966, ["OpenCorporates"], { nat: ["Spanish"] }),
      P("Fernando J. Fernandez", undefined, ["Wikidata"], { nat: ["Argentine"] }),
    ]);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].confidence).toBe("medium");
  });

  it("does not cluster genuinely different people", () => {
    const { clusters, singletons } = clusterConnectedPeople([
      P("Jane Smith", 1970, ["OpenCorporates"]),
      P("John Doe", 1980, ["OpenCorporates"]),
      P("Zoe Yujnovich", 1975, ["OpenCorporates"]),
    ]);
    expect(clusters).toHaveLength(0);
    expect(singletons).toHaveLength(3);
  });

  it("produces no false clusters on the real single-source Unilever bundle", () => {
    const people = (
      [
        ["SUSAN SALTZBART KILSBY", 1958],
        ["RONG LU", 1971],
        ["ADRIAN HENNAH", 1957],
        ["NELSON PELTZ", 1942],
        ["IAN KEITH MEAKINS", 1956],
        ["FERNANDO FERNANDEZ", 1966],
        ["JUDITH MCKENNA", 1966],
        ["BENOIT THIERRY POTIER", 1957],
        ["ZOE YUJNOVICH", 1975],
        ["SRINIVAS PHATAK", 1971],
        ["PRAKASH KAKKAD", undefined],
      ] as [string, number | undefined][]
    ).map(([n, y]) => P(n, y, ["OpenCorporates"]));
    const { clusters, singletons } = clusterConnectedPeople(people);
    expect(clusters).toHaveLength(0);
    expect(singletons).toHaveLength(11);
  });
});

describe("nameSimilarity", () => {
  it("scores an order swap highly", () => {
    expect(nameSimilarity("Peltz, Nelson", "Nelson Peltz").score).toBeGreaterThanOrEqual(0.95);
  });
  it("scores a middle-name expansion at 0.9", () => {
    expect(nameSimilarity("Nelson Peltz", "Nelson Augustus Peltz").score).toBe(0.9);
  });
  it("scores unrelated names low", () => {
    expect(nameSimilarity("Jane Smith", "John Doe").score).toBeLessThan(0.5);
  });
});

describe("scorePair", () => {
  it("returns null for non-candidates", () => {
    expect(
      scorePair(P("Jane Smith", 1970, ["x"]), P("John Doe", 1980, ["y"]))
    ).toBeNull();
  });
});
