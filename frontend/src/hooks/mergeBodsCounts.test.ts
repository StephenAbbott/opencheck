import { describe, expect, it } from "vitest";
import { mergeBodsCounts } from "./useLookupStream";

describe("mergeBodsCounts", () => {
  it("keeps the register-stage counts when the EveryPolitician event arrives", () => {
    const first = { "gleif:L1": 5, "companies_house:123": 9 };
    const second = { "everypolitician:Q1": 1, "everypolitician:Q2": 3 };
    expect(mergeBodsCounts(first, second)).toEqual({
      "gleif:L1": 5,
      "companies_house:123": 9,
      "everypolitician:Q1": 1,
      "everypolitician:Q2": 3,
    });
  });

  it("lets a later count for the same row win", () => {
    expect(mergeBodsCounts({ "x:1": 1 }, { "x:1": 4 })).toEqual({ "x:1": 4 });
  });
});
