import { describe, expect, it } from "vitest";

import {
  chipEntries,
  entryDetails,
  entryLabel,
  entryTone,
  notCoveredLine,
  type SourceDisclosure,
} from "./disclosureStatus";

const ch: SourceDisclosure = {
  values: ["declared", "not_provided"],
  ownership_source: true,
  entries: [
    { value: "declared", covers: "beneficial_owners", count: 3 },
    {
      value: "not_provided",
      covers: "beneficial_owners",
      details: ["at least one beneficial owner unidentified"],
      reasons: ["subjectUnableToConfirmOrIdentifyBeneficialOwner"],
    },
  ],
};

describe("disclosure status", () => {
  it("labels each entry with what it covers", () => {
    expect(entryLabel(ch.entries[0])).toBe("Beneficial owners: Declared (3)");
    expect(entryLabel(ch.entries[1])).toBe("Beneficial owners: Not provided");
    expect(entryLabel({ value: "exempt", covers: "parent" })).toBe("Parent: Exempt");
  });

  it("chips every value a source supports, in order", () => {
    expect(chipEntries(ch).map((e) => e.value)).toEqual(["declared", "not_provided"]);
  });

  it("never chips not_covered or no_record", () => {
    expect(chipEntries({ values: ["not_covered"], ownership_source: false,
      entries: [{ value: "not_covered", covers: null }] })).toEqual([]);
    expect(chipEntries({ values: ["no_record"], ownership_source: true,
      entries: [{ value: "no_record", covers: null }] })).toEqual([]);
  });

  it("drops Not read on a card that already says Did not answer", () => {
    const s: SourceDisclosure = { values: ["unreachable"], ownership_source: true,
      entries: [{ value: "unreachable", covers: "beneficial_owners" }] };
    expect(chipEntries(s, { cardFailed: true })).toEqual([]);
    expect(chipEntries(s)).toHaveLength(1);
  });

  it("is never a risk tone", () => {
    for (const e of ch.entries) expect(["neutral", "warn"]).toContain(entryTone(e));
    expect(entryTone({ value: "unreachable", covers: "parent" })).toBe("warn");
  });

  it("keeps the source's own words", () => {
    expect(entryDetails(ch.entries)).toEqual([
      "Beneficial owners — at least one beneficial owner unidentified",
    ]);
  });

  it("names the sources that are not ownership sources on one line", () => {
    const line = notCoveredLine(
      { sources: {}, not_covered: ["opensanctions", "ted_eu"], as_of: "2026-11-03" },
      (id) => ({ opensanctions: "OpenSanctions", ted_eu: "TED" })[id] ?? id,
    );
    expect(line).toBe("Not ownership sources: OpenSanctions, TED.");
    expect(notCoveredLine(null, (id) => id)).toBeNull();
  });
});
