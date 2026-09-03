import { describe, expect, it } from "vitest";

import type { BatchRow } from "./api";
import {
  BATCH_CAP,
  costLine,
  csvFilename,
  leiCheckDigitsOk,
  parseLeiPaste,
  pasteSummary,
  rowCoverage,
  rowsToCsv,
  sortRows,
  type TableRow,
} from "./batch";

// Real LEIs: Shell plc, Bank of Valletta, Infosys — check digits verified upstream.
const SHELL = "21380068P1DRHMJ8KU70";
const BOV = "529900RWC8ZYB066JF16";
const INFOSYS = "335800TYLGG93MM7PR89";

function row(over: Partial<BatchRow> = {}): BatchRow {
  return {
    lei: SHELL,
    legal_name: "SHELL PLC",
    jurisdiction: "GB",
    register_status: { liveness: "live", since: null, raw: "active", source_id: "companies_house" },
    verdict: "No risk signals surfaced.",
    risk_count: 0,
    context_count: 1,
    risk_codes: [],
    context_codes: ["NON_EU_JURISDICTION"],
    coverage: { applicable: 11, answered: 11, applicable_ids: [], answered_ids: [] },
    degraded: false,
    degraded_sources: [],
    licensing: null,
    replayed: false,
    report_url: `/?lei=${SHELL}`,
    ...over,
  };
}

describe("leiCheckDigitsOk", () => {
  it("accepts real LEIs and rejects a one-character typo", () => {
    for (const lei of [SHELL, BOV, INFOSYS]) expect(leiCheckDigitsOk(lei)).toBe(true);
    expect(leiCheckDigitsOk(SHELL.slice(0, 19) + "1")).toBe(false);
    expect(leiCheckDigitsOk("2138000000000000A001")).toBe(false);
  });
});

describe("parseLeiPaste", () => {
  it("reads every separator and case — a spreadsheet column just works", () => {
    const p = parseLeiPaste(`${SHELL.toLowerCase()}\r\n${BOV}, ${INFOSYS};\t`);
    expect(p.leis).toEqual([SHELL, BOV, INFOSYS]);
    expect(p.rejected).toEqual([]);
    expect(p.overflow).toBe(0);
  });

  it("rejects in place, with a reason a reader can act on", () => {
    const typo = SHELL.slice(0, 19) + "1";
    const p = parseLeiPaste(`${SHELL}\nTOO-SHORT\n${SHELL}\n${typo}\nabcdefghijklmnopqr!!`);
    expect(p.leis).toEqual([SHELL]);
    expect(p.rejected.map((r) => [r.token, r.reason])).toEqual([
      ["TOO-SHORT", "9 characters — an LEI has 20"],
      [SHELL, "duplicate"],
      [typo, "check digits do not match — a typo?"],
      ["abcdefghijklmnopqr!!", "not an LEI: 18 letters or digits then two digits"],
    ]);
  });

  it("caps at twenty and counts what it did not take", () => {
    // Twenty-three distinct valid LEIs, by varying the stem and recomputing.
    const leis: string[] = [];
    for (let i = 0; leis.length < 23; i++) {
      const stem = `2138000000000000${String(i).padStart(2, "0")}`;
      for (let cd = 2; cd < 99; cd++) {
        const cand = `${stem}${String(cd).padStart(2, "0")}`;
        if (leiCheckDigitsOk(cand)) {
          leis.push(cand);
          break;
        }
      }
    }
    const p = parseLeiPaste(leis.join(" "));
    expect(p.leis).toEqual(leis.slice(0, BATCH_CAP));
    expect(p.overflow).toBe(3);
    expect(pasteSummary(p)).toBe("20 valid · 3 beyond the cap of 20");
  });

  it("summarises the count under the box", () => {
    expect(pasteSummary(parseLeiPaste(`${SHELL} nope`))).toBe("1 valid · 1 rejected");
    expect(pasteSummary(parseLeiPaste(""))).toBe("0 valid");
  });
});

describe("costLine", () => {
  it("says the ceiling in minutes, and how the queue works", () => {
    expect(costLine(0)).toBe("");
    expect(costLine(1)).toMatch(/^A few seconds/);
    expect(costLine(4)).toMatch(/^Under a minute for 4 companies/);
    expect(costLine(20)).toMatch(/^About a minute for 20 companies/);
    expect(costLine(20)).toContain("runs 2 at a time");
  });
});

describe("sortRows", () => {
  it("puts degraded and failed rows first, then risk count, then name; running rows last in paste order", () => {
    const rows: TableRow[] = [
      { state: "running", lei: "R2" },
      { state: "done", lei: "B", row: row({ lei: "B", legal_name: "Beta", risk_count: 2 }) },
      { state: "running", lei: "R1" },
      { state: "done", lei: "A", row: row({ lei: "A", legal_name: "Alpha", risk_count: 2 }) },
      { state: "degraded", lei: "F", failed: { lei: "F", status: 503, reason: "rate-limited", retryable: true, degraded: true } },
      { state: "done", lei: "C", row: row({ lei: "C", legal_name: "Gamma", risk_count: 5 }) },
      { state: "degraded", lei: "D", row: row({ lei: "D", legal_name: "Delta", degraded: true, degraded_sources: ["opensanctions"] }) },
    ];
    expect(sortRows(rows).map((r) => r.lei)).toEqual(["F", "D", "C", "A", "B", "R2", "R1"]);
  });
});

describe("rowCoverage", () => {
  it("hands the anchor back to coverageCopy rather than counting it twice", () => {
    const c = rowCoverage(row({ coverage: { applicable: 11, answered: 10, applicable_ids: [], answered_ids: [] } }), 40);
    expect(c.answered).toBe(10);
    expect(c.applicable).toBe(11);
    expect(c.aside).toBe("10 of 11 sources answered");
    expect(c.detail).toBe("11 of OpenCheck's 40 sources apply to a GB company; 10 answered.");
  });
});

describe("rowsToCsv", () => {
  it("writes every screened LEI, failed rows with their reason, and quotes commas", () => {
    const rows: TableRow[] = [
      { state: "done", lei: SHELL, row: row({ verdict: "Sanctions, on the company itself." , risk_count: 1, risk_codes: ["SANCTIONED"] }) },
      { state: "degraded", lei: BOV, failed: { lei: BOV, status: 404, reason: "No GLEIF record found", retryable: false, degraded: true } },
      { state: "running", lei: INFOSYS },
    ];
    const csv = rowsToCsv(rows, "https://opencheck.world");
    const lines = csv.trimEnd().split("\r\n");
    expect(lines).toHaveLength(3); // header + two finished rows; the running one is not a result
    expect(lines[0].split(",")).toContain("state");
    expect(lines[1]).toContain(`"Sanctions, on the company itself."`);
    expect(lines[1]).toContain(",SANCTIONED,");
    expect(lines[1]).toContain(`https://opencheck.world/?lei=${SHELL}`);
    expect(lines[2]).toContain(`${BOV},,,,,,,,,,,true,,not checked,No GLEIF record found,`);
  });

  it("names the file by the day", () => {
    expect(csvFilename(new Date("2026-09-03T15:00:00Z"))).toBe("opencheck-batch-2026-09-03.csv");
  });
});
