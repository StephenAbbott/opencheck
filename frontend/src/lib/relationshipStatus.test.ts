/**
 * Phase 219 — the rule for "this relationship has ended".
 *
 * `backend/tests/test_bods_lifecycle.py` runs the same cases against
 * `opencheck/bods/lifecycle.py`. The two are parallel, not shared: change one
 * and the other must follow.
 */
import { describe, expect, it } from "vitest";
import {
  endedPhrase,
  interestCellText,
  interestEnded,
  relationshipLifecycle,
  todayIso,
} from "./relationshipStatus";

const AS_OF = "2026-09-16";

describe("interestEnded", () => {
  it("reads a past or same-day endDate as ended", () => {
    expect(interestEnded({ endDate: "2024-11-30" }, false, AS_OF)).toBe(true);
    expect(interestEnded({ endDate: AS_OF }, false, AS_OF)).toBe(true);
  });

  it("reads a future endDate as current — a scheduled end has not happened", () => {
    expect(interestEnded({ endDate: "2027-01-01" }, false, AS_OF)).toBe(false);
  });

  it("treats every interest on a closed record as ended, dated or not", () => {
    // Six closed PSC relationships in the demo set carry no endDate at all;
    // CAC Nigeria's INACTIVE rows are the live example.
    expect(interestEnded({}, true, AS_OF)).toBe(true);
    expect(interestEnded({ endDate: "2027-01-01" }, true, AS_OF)).toBe(true);
  });

  it("compares the date part of a date-time", () => {
    expect(interestEnded({ endDate: "2024-11-30T00:00:00Z" }, false, AS_OF)).toBe(true);
  });

  it("ignores an endDate it cannot read rather than guessing", () => {
    expect(interestEnded({ endDate: "sometime" }, false, AS_OF)).toBe(false);
  });
});

describe("relationshipLifecycle", () => {
  it("is ended when every interest has ended, dated with the latest end", () => {
    expect(
      relationshipLifecycle([{ endDate: "2019-06-18" }, { endDate: "2024-11-30" }], false, AS_OF)
    ).toEqual({ ended: true, endedOn: "2024-11-30" });
  });

  it("stays current while any interest is current", () => {
    expect(
      relationshipLifecycle([{ endDate: "2024-11-30" }, {}], false, AS_OF)
    ).toEqual({ ended: false });
  });

  it("is ended for a closed record and never invents a date for it", () => {
    expect(relationshipLifecycle([{}], true, AS_OF)).toEqual({ ended: true });
  });

  it("does not date a closed record with a future endDate", () => {
    expect(relationshipLifecycle([{ endDate: "2027-01-01" }], true, AS_OF)).toEqual({ ended: true });
  });

  it("reads a relationship with no interests as ended only when closed", () => {
    expect(relationshipLifecycle([], false, AS_OF)).toEqual({ ended: false });
    expect(relationshipLifecycle([], true, AS_OF)).toEqual({ ended: true });
  });
});

describe("the words", () => {
  it("spells the month out and parses the date by hand", () => {
    // new Date("2024-11-30") is midnight UTC — the 29th west of Greenwich.
    expect(endedPhrase("2024-11-30")).toBe("ended 30 November 2024");
    expect(endedPhrase("2018-01-05")).toBe("ended 5 January 2018");
  });

  it("says only 'ended' when no date was published or it is unreadable", () => {
    expect(endedPhrase(undefined)).toBe("ended");
    expect(endedPhrase("2024-13-01")).toBe("ended");
  });

  it("gives the tree cell the label's first line and the phrase, once", () => {
    expect(interestCellText("Owns 75–100%\nended 30 November 2024", true, "2024-11-30")).toBe(
      "Owns 75–100% · ended 30 November 2024"
    );
    expect(interestCellText("ended", true)).toBe("ended");
    expect(interestCellText(undefined, true)).toBe("ended");
    expect(interestCellText("Owns 75%\nControls (votes)", false)).toBe("Owns 75%");
  });

  it("formats today as a BODS date", () => {
    expect(todayIso(new Date("2026-09-16T23:30:00Z"))).toBe("2026-09-16");
  });
});
