import { describe, expect, it } from "vitest";
import { gemChipHeading, gemOwnershipChips } from "./gemOwnership";

describe("gemOwnershipChips", () => {
  it("never shows a group's top entity as its own parent (Pertamina)", () => {
    const { owners, parents } = gemOwnershipChips({
      entity_id: "E100000000538",
      parents: [{ entity_id: "E100000000538", name: "PT Pertamina (Persero) PT", share: 100 }],
      owners: [{ entity_id: "E100001000084", name: "Government of Indonesia", share: 100 }],
    });
    expect(parents).toEqual([]);
    expect(owners).toEqual([{ id: "E100001000084", name: "Government of Indonesia", share: 100 }]);
  });

  it("shows a party that is both a parent and a direct owner once, as an owner", () => {
    const { owners, parents } = gemOwnershipChips({
      entity_id: "E100001000387",
      parents: [
        { entity_id: "E100001016502", name: "Mahanada Suppliers Pvt Ltd", share: 22.6 },
        { entity_id: "E100000000999", name: "Group Holding Ltd", share: 51 },
      ],
      owners: [{ entity_id: "E100001016502", name: "Mahanada Suppliers Pvt Ltd", share: 22.55 }],
    });
    expect(owners.map((o) => o.share)).toEqual([22.55]);
    expect(parents.map((p) => p.name)).toEqual(["Group Holding Ltd"]);
  });

  it("drops an owner row naming the entity itself, and keeps a missing share as null", () => {
    const { owners } = gemOwnershipChips({
      entity_id: "E1",
      owners: [
        { entity_id: "E1", name: "Itself", share: 100 },
        { entity_id: "E100000123261", name: "natural person(s) ", share: null },
      ],
    });
    expect(owners).toEqual([{ id: "E100000123261", name: "natural person(s)", share: null }]);
  });

  it("reads a stored pre-Phase-199 hit, which has no owners list", () => {
    const { owners, parents } = gemOwnershipChips({
      entity_id: "E2",
      parents: [{ entity_id: "E3", name: "Parent Plc", share: 55 }],
    });
    expect(owners).toEqual([]);
    expect(parents).toEqual([{ id: "E3", name: "Parent Plc", share: 55 }]);
  });
});

describe("gemChipHeading", () => {
  it("pluralises on count", () => {
    expect(gemChipHeading("owners", 1)).toBe("GEM direct owner");
    expect(gemChipHeading("owners", 4)).toBe("GEM direct owners");
    expect(gemChipHeading("parents", 2)).toBe("GEM parents");
  });
});
