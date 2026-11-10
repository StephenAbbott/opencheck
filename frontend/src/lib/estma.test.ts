import { describe, expect, it } from "vitest";

import { estmaAndEitiNote, estmaTile, formatCad, ownReports, periodLine, type EstmaBundle } from "./estma";

function bundle(over: Partial<EstmaBundle> = {}): EstmaBundle {
  return {
    lei: "894500XO86FKXQGPYV71",
    gleif_legal_name: "Suncor Energy Inc.",
    latest_year: "2025",
    entities: [
      {
        estma_id: "E405435",
        name: "SUNCOR ENERGY INC.",
        reports_payments: true,
        includes: null,
        match: { method: "exact name, one live LEI", basis: "names agree", gleif_name: "Suncor Energy Inc.", note: "" },
        reports: [
          { year: "2025", period_start: "2025-01-01", period_end: "2025-12-31", report_type: "CON",
            report_type_label: "Consolidated Report", url: "https://example.com/r", included_in: null,
            payee_total_cad: 4_637_910_000, payee_count: 18, project_count: 10 },
          { year: "2024", period_start: "2024-01-01", period_end: "2024-12-31", report_type: "CON",
            report_type_label: "Consolidated Report", url: null, included_in: null, payee_total_cad: 10 },
        ],
      },
    ],
    ...over,
  };
}

describe("estma values", () => {
  it("formats Canadian dollars compactly", () => {
    expect(formatCad(4_637_910_000)).toBe("CAD 4.6bn");
    expect(formatCad(12_300_000)).toBe("CAD 12.3m");
    expect(formatCad(54_395)).toBe("CAD 54,395");
    expect(formatCad(-1_500)).toBe("−CAD 1,500");
  });

  it("states the reporting period as filed", () => {
    expect(periodLine(bundle().entities[0].reports[0])).toBe("1 Jan 2025 – 31 Dec 2025");
  });

  it("tiles the latest report as a sum of payee lines", () => {
    expect(estmaTile(bundle())).toEqual({ stat: "CAD 4.6bn", unit: "to governments", sub: "2025 · sum of payee lines" });
  });

  it("never adds two reports for one year on the tile", () => {
    const b = bundle();
    b.entities[0].reports = [
      { ...b.entities[0].reports[0], payee_total_cad: 20 },
      { ...b.entities[0].reports[0], payee_total_cad: 5, report_type: "IND_ST" },
    ];
    const tile = estmaTile(b);
    expect(tile.stat).toBe("CAD 20");
    expect(tile.sub).toBe("2025 · 2 reports, first shown");
  });

  it("says whose report an included entity's payments are in, never zero", () => {
    const b = bundle();
    b.entities[0].reports = [
      { year: "2025", period_start: null, period_end: null, report_type: "CON", report_type_label: null,
        url: null, included_in: { estma_id: "E405435", name: "SUNCOR ENERGY INC." } },
    ];
    expect(ownReports(b.entities[0])).toEqual([]);
    expect(estmaTile(b)).toEqual({
      stat: "2025",
      unit: "payments filed by another entity",
      sub: "in the report of SUNCOR ENERGY INC.",
    });
  });

  it("notes ESTMA beside EITI only when both are present", () => {
    expect(estmaAndEitiNote(["estma"])).toBeNull();
    expect(estmaAndEitiNote(["eiti"])).toBeNull();
    expect(estmaAndEitiNote(["estma", "eiti"])).toMatch(/not added together/);
    expect(estmaAndEitiNote(["estma", "eiti_zambia"])).toMatch(/Canadian dollars/);
  });
});
