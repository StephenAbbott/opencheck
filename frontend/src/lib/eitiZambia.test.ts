import { describe, expect, it } from "vitest";

import {
  formatZmw,
  latestSummedYear,
  matchBasis,
  womenShare,
  zambiaTile,
  type EitiZambiaBundle,
} from "./eitiZambia";

function bundle(over: Partial<EitiZambiaBundle> = {}): EitiZambiaBundle {
  return {
    lei: "2549008ZVFBSUO8W2L37",
    gleif_legal_name: "Kansanshi Mining PLC",
    tpins: ["1001602517"],
    names_as_filed: [],
    match: { method: "tpin_via_name", confidence: "medium", aliases: [] },
    zra_tax: [],
    eiti_reconciliation: [],
    employment: [],
    licences: [],
    water_offences: [],
    datasets: {},
    ...over,
  };
}

describe("formatZmw", () => {
  it("compacts kwacha", () => {
    expect(formatZmw(6_383_073_000)).toBe("ZMW 6.4bn");
    expect(formatZmw(12_300_000)).toBe("ZMW 12.3m");
    expect(formatZmw(54_395)).toBe("ZMW 54,395");
  });
});

describe("zambiaTile", () => {
  it("leads with the latest year that has a kwacha total", () => {
    const b = bundle({
      zra_tax: [
        { year: "2024", dataset: "zra-tax-revenue-2024", payments: 4, amounts_summed: false, by_tax_type: [] },
        { year: "2023", dataset: "x", payments: 107, amounts_summed: true, total_zmw: 4_388_223_358, by_tax_type: [] },
      ],
    });
    expect(latestSummedYear(b)?.year).toBe("2023");
    expect(zambiaTile(b)).toEqual({ stat: "ZMW 4.4bn", unit: "ZRA tax receipts", sub: "2023" });
  });

  it("counts payments, never invents a total, when no year has amounts", () => {
    const b = bundle({
      zra_tax: [{ year: "2024", dataset: "zra-tax-revenue-2024", payments: 1, amounts_summed: false, by_tax_type: [] }],
    });
    expect(zambiaTile(b)).toEqual({ stat: "1", unit: "ZRA tax payment", sub: "2024 · amounts not published" });
  });

  it("falls back to the rights count", () => {
    const lic = { code: "1", type: null, status: null, commodities: null, area: null, location: null,
      grant_date: null, expiry_date: null, holder_as_filed: "x", holder_share_pct: null };
    expect(zambiaTile(bundle({ zra_tax: [], licences: [lic] }))).toEqual({
      stat: "1", unit: "mining right", sub: "in the cadastre",
    });
  });
});

describe("matchBasis and womenShare", () => {
  it("names what the match rests on", () => {
    expect(matchBasis("tpin_via_name")).toContain("TPIN");
    expect(matchBasis("name_only")).toContain("cadastre");
  });
  it("keeps an absent share absent", () => {
    expect(womenShare(0.083)).toBe("8.3%");
    expect(womenShare(null)).toBeNull();
  });
});
