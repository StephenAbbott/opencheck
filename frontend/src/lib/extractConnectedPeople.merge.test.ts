import { describe, it, expect } from "vitest";
import { extractConnectedPeople } from "./backgroundCheck";
import { clusterConnectedPeople } from "./clusterPeople";
import type { Stmt } from "./backgroundCheck";

/**
 * Order-insensitive merge key: the real Unilever capture (deepen_top=10, CH
 * live) has every UK director twice — OpenCorporates "FORENAME SURNAME" and
 * Companies House "SURNAME, Forename", same birth year. Sorting the name tokens
 * in the merge key collapses those pairs automatically, so clustering only has
 * to deal with the genuinely-uncertain residue (the Wikidata missing-year
 * Fernández record).
 *
 * Requires the nameMatchKey change in extractConnectedPeople (see INTEGRATION.md).
 */

function person(id: string, fullName: string, year?: number, source?: string): Stmt {
  return {
    statementId: id,
    recordType: "person",
    source: { description: source },
    recordDetails: {
      personType: "knownPerson",
      names: [{ fullName }],
      ...(year ? { birthDate: String(year) } : {}),
    },
  };
}
const oc = (n: string, y?: number) => person(`oc-${n}`, n, y, "OpenCorporates");
const ch = (n: string, y?: number) => person(`ch-${n}`, n, y, "UK Companies House");

const UNILEVER_22: Stmt[] = [
  oc("SUSAN SALTZBART KILSBY", 1958), oc("RONG LU", 1971), oc("ADRIAN HENNAH", 1957),
  oc("NELSON PELTZ", 1942), oc("IAN KEITH MEAKINS", 1956), oc("FERNANDO FERNANDEZ", 1966),
  oc("JUDITH MCKENNA", 1966), oc("BENOIT THIERRY POTIER", 1957), oc("ZOE YUJNOVICH", 1975),
  oc("SRINIVAS PHATAK", 1971), oc("PRAKASH KAKKAD", undefined),
  ch("KILSBY, Susan Saltzbart", 1958), ch("LU, Rong", 1971), ch("HENNAH, Adrian", 1957),
  ch("PELTZ, Nelson", 1942), ch("MEAKINS, Ian Keith", 1956), ch("FERNANDEZ, Fernando", 1966),
  ch("MCKENNA, Judith", 1966), ch("POTIER, Benoit Thierry", 1957), ch("YUJNOVICH, Zoe", 1975),
  ch("PHATAK, Srinivas", 1971),
  person("wd-1", "Fernando Fernández", undefined, "Wikidata"),
  person("wd-2", "Fernando Fernández", undefined, "Wikidata"),
];

describe("order-insensitive merge key on the real Unilever capture", () => {
  const people = extractConnectedPeople(UNILEVER_22);

  it("collapses the register-format duplicates to 12 people", () => {
    expect(people).toHaveLength(12);
  });

  it("merges each of the 10 UK directors across OpenCorporates + Companies House", () => {
    const merged = people.filter((p) => p.sources.length === 2);
    expect(merged).toHaveLength(10);
    for (const p of merged) {
      expect(p.sources).toEqual(
        expect.arrayContaining(["OpenCorporates", "UK Companies House"])
      );
    }
  });

  it("keeps Nelson Peltz as a single card carrying both source statements", () => {
    const peltz = people.find((p) => /peltz/i.test(p.name));
    expect(peltz?.sources).toHaveLength(2);
    expect(peltz?.statementIds).toHaveLength(2);
  });

  it("leaves only the Fernández/Wikidata missing-year pair for clustering", () => {
    const { clusters, singletons } = clusterConnectedPeople(people);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].confidence).toBe("medium");
    expect(clusters[0].pairs[0].evidence).toMatch(/missing on one/);
    expect(singletons).toHaveLength(10);
  });
});
