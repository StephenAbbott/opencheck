import { describe, expect, it } from "vitest";
import { declaredLine, filingOf, FILING_PROPERTY } from "./filingProvenance";

const URL =
  "https://find-and-update.company-information.service.gov.uk/company/09446231/filing-history/MzM5/document?format=pdf&download=0";

function stmt(...facts: Record<string, unknown>[]) {
  return {
    annotations: [
      { statementPointerTarget: "/recordDetails/interests/0/type", motivation: "transformation" },
      ...facts.map((f) => ({
        statementPointerTarget: "/recordDetails/interests",
        motivation: "commenting",
        [FILING_PROPERTY]: f,
      })),
    ],
  };
}

describe("filingOf", () => {
  it("reads the notification and the cessation apart", () => {
    const s = stmt(
      { event: "notification", match: "filing", type: "PSC02", date: "2016-04-06" },
      { event: "cessation", match: "filing", type: "PSC07", date: "2021-07-07" },
    );
    expect(filingOf(s)?.type).toBe("PSC02");
    expect(filingOf(s, "cessation")?.type).toBe("PSC07");
  });

  it("ignores a statement with no filing, or an unknown match word", () => {
    expect(filingOf({})).toBeNull();
    expect(filingOf(stmt({ event: "notification", match: "guessed" }))).toBeNull();
  });
});

describe("declaredLine", () => {
  it("says when, on what form, and links the filing", () => {
    expect(
      declaredLine({
        event: "notification",
        match: "filing",
        type: "PSC02",
        date: "2023-10-30",
        documentUrl: URL,
      }),
    ).toEqual({
      text: "Declared on 30 Oct 2023 via PSC02, filed by the company",
      linkText: "filing",
      url: URL,
    });
  });

  it("names the incorporation and 2016 routes without calling them a PSC form", () => {
    expect(
      declaredLine({ event: "notification", match: "incorporation", type: "NEWINC", date: "2020-01-13" })?.text,
    ).toBe("Declared at incorporation, 13 Jan 2020 (NEWINC), filed by the company");
    expect(
      declaredLine({ event: "notification", match: "confirmation_statement", type: "CS01", date: "2017-02-20" })?.text,
    ).toBe(
      "On no PSC form; before June 2017 PSCs were given on the next confirmation statement — CS01, filed 20 Feb 2017, filed by the company",
    );
  });

  it("states an absence in the same voice, with no link", () => {
    expect(declaredLine({ event: "notification", match: "none" })).toEqual({
      text: "No notifying filing in the register's filing history",
    });
    expect(declaredLine({ event: "cessation", match: "none" })?.url).toBeUndefined();
    expect(declaredLine(null)).toBeNull();
  });
});
