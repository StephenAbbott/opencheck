import { describe, expect, it } from "vitest";

import {
  eitiIdentLine,
  eitiLinkNote,
  formatZmw,
  latestSummedYear,
  matchBasis,
  twoEitiPublishersNote,
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

describe("two EITI publishers (Phase 306)", () => {
  it("speaks only when both EITI cards are present", () => {
    expect(twoEitiPublishersNote(["eiti", "eiti_zambia", "climatetrace"])).toMatch(/not added/);
    expect(twoEitiPublishersNote(["eiti"])).toBeNull();
    expect(twoEitiPublishersNote(["eiti_zambia"])).toBeNull();
    expect(twoEitiPublishersNote([])).toBeNull();
  });

  it("names the units of each publication", () => {
    const note = twoEitiPublishersNote(["eiti", "eiti_zambia"]) ?? "";
    expect(note).toContain("US dollars");
    expect(note).toContain("kwacha");
  });

  it("names a Zambian identification as the ZRA TPIN", () => {
    expect(eitiIdentLine("ZM", "1001602517")).toBe("ZM · ZRA TPIN 1001602517");
    expect(eitiIdentLine("GB", "01285743")).toBe("GB · national ID 01285743");
  });

  it("says when the link rests on the portal's name match", () => {
    expect(eitiLinkNote("zm_tpin")).toMatch(/name match/);
    expect(eitiLinkNote("registered_as")).toBeNull();
    expect(eitiLinkNote(undefined)).toBeNull();
  });
});
