/**
 * formerNames — the picker's "matched a former name" line (Phase 309). The
 * subject's own former names are the identity band's "Former names" row,
 * pinned in subjectProfile.test.ts; Phase 311 took them off the subject card.
 * Barrick Mining Corporation as GLEIF
 * files it on 8 Oct 2026: three PREVIOUS_LEGAL_NAME entries, one trading
 * name, renamed from Barrick Gold Corporation on 6 May 2025.
 */
import { describe, expect, it } from "vitest";

import {
  FORMERLY_LABEL,
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
