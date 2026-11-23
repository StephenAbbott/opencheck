import { describe, expect, it } from "vitest";
import { readableDate, readableDatesIn } from "./readableDate";

describe("readableDate", () => {
  it("words a day, a month and a year at their own precision", () => {
    expect(readableDate("1964-02-25")).toBe("25 Feb 1964");
    expect(readableDate("2026-09")).toBe("Sept 2026");
    expect(readableDate("1964")).toBe("1964");
    expect(readableDate("2026-11-23T09:48:42Z")).toBe("23 Nov 2026");
  });

  it("leaves what is not a date alone", () => {
    expect(readableDate("")).toBe("");
    expect(readableDate(undefined)).toBe("");
    expect(readableDate("not a date")).toBe("not a date");
    expect(readableDate("2026-13")).toBe("2026-13");
  });
});

describe("readableDatesIn", () => {
  it("rewords every ISO date in a source's detail", () => {
    expect(
      readableDatesIn(
        "datos.jus.gob.ar Registro Nacional de Sociedades and IGJ, RNS cut 2026-09-18, IGJ 2026-09",
      ),
    ).toBe("datos.jus.gob.ar Registro Nacional de Sociedades and IGJ, RNS cut 18 Sept 2026, IGJ Sept 2026");
  });

  it("does not touch numbers that are not dates", () => {
    expect(readableDatesIn("Companies 2020-21 and 12345-678")).toBe("Companies 2020-21 and 12345-678");
  });
});
