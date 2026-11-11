import { describe, expect, it } from "vitest";

import {
  chipEntries,
  entryDetails,
  entryLabel,
  entryTone,
  notCoveredLine,
  otherTabLine,
  type DisclosureStatusPayload,
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

// Shell plc in production (4 Nov 2026): Climate TRACE is an ownership source
// whose card is on the Climate & ESG tab, not QuickCheck.
const shell: DisclosureStatusPayload = {
  as_of: "2026-11-04",
  not_covered: ["opensanctions", "eiti"],
  sources: {
    companies_house: {
      values: ["exempt"],
      ownership_source: true,
      entries: [{ value: "exempt", covers: "beneficial_owners" }],
    },
    climatetrace: {
      values: ["declared"],
      ownership_source: true,
      entries: [{ value: "declared", covers: "shareholders", count: 1 }],
    },
    eiti: {
      values: ["not_covered"],
      ownership_source: false,
      entries: [{ value: "not_covered", covers: null }],
    },
  },
};
const names: Record<string, string> = {
  climatetrace: "Global Energy Monitor / Climate TRACE",
  companies_house: "UK Companies House",
  eiti: "EITI",
};
const label = (id: string) => names[id] ?? id;

describe("ownership sources on another tab", () => {
  it("names Climate TRACE's value on QuickCheck", () => {
    expect(otherTabLine(shell, new Set(["climatetrace", "eiti"]), label)).toBe(
      "On the Climate & ESG tab: Global Energy Monitor / Climate TRACE — Shareholders: Declared (1).",
    );
  });

  it("says 'No record of the company' too", () => {
    const lilly: DisclosureStatusPayload = {
      ...shell,
      sources: {
        climatetrace: {
          values: ["no_record"],
          ownership_source: true,
          entries: [{ value: "no_record", covers: null }],
        },
      },
    };
    expect(otherTabLine(lilly, new Set(["climatetrace"]), label)).toBe(
      "On the Climate & ESG tab: Global Energy Monitor / Climate TRACE — No record of the company.",
    );
  });

  it("leaves out sources with a card here and sources that publish no owners", () => {
    expect(otherTabLine(shell, new Set(["eiti"]), label)).toBeNull();
    expect(otherTabLine(null, new Set(["climatetrace"]), label)).toBeNull();
  });

  it("drops no ownership source: each is chipped on a QuickCheck card or named on the line", () => {
    // Whatever is filtered off QuickCheck, every ownership source's answer is
    // still on QuickCheck — on its card or on this line.
    for (const offPage of [new Set<string>(), new Set(["climatetrace"]), new Set(["climatetrace", "companies_house"])]) {
      const line = otherTabLine(shell, offPage, label) ?? "";
      for (const [id, status] of Object.entries(shell.sources)) {
        if (!status.ownership_source) continue;
        const onCard = !offPage.has(id) && chipEntries(status).length > 0;
        expect(onCard || line.includes(label(id))).toBe(true);
      }
    }
  });
});
