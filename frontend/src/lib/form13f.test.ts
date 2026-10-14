import { describe, expect, it } from "vitest";
import { THOUSANDS, payload, pos, sec } from "../test/form13fFixture";
import {
  BANNED_VOCABULARY,
  VALUE_NOT_SHOWN,
  form13fView,
  hiddenRows,
  pageSentences,
  percentText,
  periodLine,
  securityFooter,
  securityLabel,
  sharesText,
  thousandsNote,
  toggleLabel,
  valueText,
} from "./form13f";

describe("form13fView", () => {
  it("renders nothing without a payload or without a listed security", () => {
    expect(form13fView(null)).toBeNull();
    expect(form13fView(undefined)).toBeNull();
    expect(form13fView(payload({ securities: [] }))).toBeNull();
    expect(form13fView(payload({ securities: [sec({ positions: [] })] }))).toBeNull();
  });

  it("says an out-of-date index in its own line, with no rows", () => {
    const v = form13fView(
      payload({ status: "out_of_date", securities: [], line: "The newest Form 13F data OpenCheck holds is for the quarter ending 2025-12-31, too old to show." }),
    );
    expect(v?.status).toBe("out_of_date");
    expect(v?.securities).toEqual([]);
    expect(v?.line).toMatch(/too old to show/);
  });

  it("dates the positions by the quarter end and the last filing day", () => {
    expect(periodLine(payload())).toBe(
      "Positions as at 30 June 2026, from reports filed up to 31 Aug 2026.",
    );
    expect(periodLine({ period: null, filed_through: null })).toBeNull();
  });
});

describe("rows", () => {
  it("formats shares exactly and rounds the value", () => {
    expect(sharesText(24_669_778)).toBe("24,669,778");
    expect(valueText({ value: 11_173_435_852, value_note: null })).toBe("$11.2B");
  });

  it("never shows a value reported in thousands", () => {
    const p = pos("T. Rowe Price", 1000, { value: 452, value_note: THOUSANDS, value_check: "thousands" });
    expect(valueText(p)).toBe(VALUE_NOT_SHOWN);
    const s = sec({ positions: [pos("A", 10), p] }, 2);
    expect(thousandsNote(s, 1)).toBeNull(); // collapsed: the row is not shown
    expect(thousandsNote(s, 20)).toBe(THOUSANDS);
  });

  it("gives a percentage only where the backend computed one", () => {
    expect(percentText({ percent: 14.24 })).toBe("14.24%");
    expect(percentText({ percent: 7.5 })).toBe("7.50%");
    expect(percentText({ percent: null })).toBeNull();
  });

  it("names the security as the filers do, with its class", () => {
    expect(securityLabel(sec())).toBe("MOODYS CORP · COM");
    expect(securityLabel(sec({ issuer_name: null, title_of_class: null }))).toBe("615369105");
  });
});

describe("collapsed and open", () => {
  it("collapsed shows the largest; opening shows the next nineteen", () => {
    expect(hiddenRows([sec()])).toBe(19);
    expect(toggleLabel(false, [sec()])).toBe("Show the next 19");
    expect(toggleLabel(true, [sec()])).toBe("Show only the largest");
    expect(toggleLabel(false, [sec({}, 2)])).toBe("Show the next manager");
    expect(toggleLabel(false, [sec({}, 1)])).toBe("");
  });

  it("says 'for each security' when several are listed alike", () => {
    const two = [sec(), sec({ cusip: "084670702", title_of_class: "CL B NEW" })];
    expect(toggleLabel(false, two)).toBe("Show the next 19 for each security");
    expect(toggleLabel(false, [sec(), sec({ cusip: "X" }, 5)])).toBe("Show all 25 rows");
  });

  it("states what is left out and the basis, never a total", () => {
    const lines = securityFooter(sec());
    expect(lines[0]).toBe("1,401 managers reported a position; the 20 largest by shares are shown.");
    expect(lines.some((l) => l.includes("computed by OpenCheck"))).toBe(true);
    expect(lines.some((l) => /7 reported positions were left out/.test(l))).toBe(true);
    expect(securityFooter(sec({ managers_reported: 1, left_out: 1 }, 1))).toEqual([
      "1 manager reported a position.",
      sec().shares_outstanding!.basis,
      "1 reported position was left out because its figures cannot all be right (for example shares and value transposed).",
    ]);
    expect(pageSentences(payload()).join(" ")).not.toMatch(/\btotal\b/i);
  });

  it("carries the ADR note on a depositary security", () => {
    const adr = sec({
      is_adr: true,
      adr_note: "American depositary shares traded in the US. No percentage is computed: one depositary share can stand for more or fewer than one underlying share.",
      shares_outstanding: null,
    });
    expect(securityFooter(adr)).toContain(adr.adr_note);
  });
});

describe("wording", () => {
  it("never reads a reported position as ownership (the disclaimer aside)", () => {
    const thousandsRow = pos("T. Rowe Price", 1, { value_note: THOUSANDS, value: null });
    const p = payload({
      not_listed_note: "1 more of the company's securities is not listed: fewer than 10 managers reported it, or it is not a share.",
      securities: [sec(), sec({ is_adr: true, positions: [thousandsRow] }, 1)],
    });
    const words = pageSentences(p).join(" ").toLowerCase();
    for (const w of BANNED_VOCABULARY) {
      // A substring, as the backend tests it: "controls" is out too.
      expect(words.includes(w), w).toBe(false);
    }
  });
});
