/**
 * formerNames — the picker's "matched a former name" line and the subject
 * card's "Formerly …" line (Phase 309). Barrick Mining Corporation as GLEIF
 * files it on 8 Oct 2026: three PREVIOUS_LEGAL_NAME entries, one trading
 * name, renamed from Barrick Gold Corporation on 6 May 2025.
 */
import { describe, expect, it } from "vitest";

import {
  CARD_FORMER_NAMES_MAX,
  FORMERLY_LABEL,
  NAME_CHANGED_LABEL,
  formerlyLine,
  formerlyMatchedLine,
  matchedFormerName,
  nameKey,
  queryMatches,
} from "./formerNames";

const OTHER_NAMES = [
  { name: "American Barrick Resources Corporation", type: "PREVIOUS_LEGAL_NAME" },
  { name: "SOCIETE AURIFERE BARRICK", type: "PREVIOUS_LEGAL_NAME" },
  { name: "Barrick Gold Corporation", type: "PREVIOUS_LEGAL_NAME" },
  { name: "SOCIETE MINIERE BARRICK", type: "TRADING_OR_OPERATING_NAME" },
];
const MINING = "BARRICK MINING CORPORATION";

describe("nameKey / queryMatches", () => {
  it("folds case, diacritics and punctuation", () => {
    expect(nameKey("Société Aurifère Barrick, Inc.")).toBe("societe aurifere barrick inc");
    expect(queryMatches("barrick gold", "Barrick Gold Corporation")).toBe(true);
    expect(queryMatches("Barrick Gold Corporation", MINING)).toBe(false);
    expect(queryMatches("", MINING)).toBe(false);
  });
});

describe("matchedFormerName", () => {
  it("names the former legal name the query matched when the current name did not", () => {
    expect(matchedFormerName("Barrick Gold Corporation", MINING, OTHER_NAMES)).toBe("Barrick Gold Corporation");
    expect(matchedFormerName("american barrick", MINING, OTHER_NAMES)).toBe("American Barrick Resources Corporation");
  });

  it("says nothing when the current name matches, and never calls a trading name former", () => {
    expect(matchedFormerName("Barrick Mining", MINING, OTHER_NAMES)).toBeNull();
    expect(matchedFormerName("Barrick", MINING, OTHER_NAMES)).toBeNull();
    expect(matchedFormerName("Societe Miniere Barrick", MINING, OTHER_NAMES)).toBeNull();
    expect(matchedFormerName("Barrick Gold Corporation", MINING, undefined)).toBeNull();
    expect(matchedFormerName("Barrick Gold Corporation", MINING, [])).toBeNull();
  });

  it("words the row line", () => {
    expect(formerlyMatchedLine("Barrick Gold Corporation")).toBe(
      `${FORMERLY_LABEL} Barrick Gold Corporation — matched your search`,
    );
  });
});

describe("formerlyLine", () => {
  const profile = {
    former_names: [
      { name: "Barrick Gold Corporation", until: "2025-05-06", from: null, sources: ["gleif", "companies_house"] },
      { name: "American Barrick Resources Corporation", until: null, from: null, sources: ["gleif"] },
      { name: "SOCIETE AURIFERE BARRICK", until: null, from: null, sources: ["gleif"] },
    ],
    name_changed_on: "2025-05-06",
  };

  it("lists the first names, counts the rest and dates the change", () => {
    expect(CARD_FORMER_NAMES_MAX).toBe(2);
    expect(formerlyLine(profile)).toBe(
      `${FORMERLY_LABEL} Barrick Gold Corporation (until 6 May 2025), American Barrick Resources Corporation and 1 more · ${NAME_CHANGED_LABEL} 6 May 2025`,
    );
  });

  it("drops the clauses it has no fact for", () => {
    expect(formerlyLine({ former_names: profile.former_names.slice(1, 2), name_changed_on: null })).toBe(
      `${FORMERLY_LABEL} American Barrick Resources Corporation`,
    );
    expect(formerlyLine({ former_names: [], name_changed_on: "2025-05-06" })).toBeNull();
    expect(formerlyLine(null)).toBeNull();
    expect(formerlyLine(undefined)).toBeNull();
  });
});
