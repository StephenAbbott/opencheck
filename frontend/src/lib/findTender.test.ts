import { describe, expect, it } from "vitest";

import { coverageNote, dateLabel, lotsLabel, statusLabel, valueLabel } from "./findTender";

const base = { ocid: "ocds-h6vhtk-1", award_id: "1" };

describe("findTender", () => {
  it("says what each date is", () => {
    expect(dateLabel({ ...base, date: "2026-03-10", date_basis: "awarded" })).toBe("awarded 2026-03-10");
    expect(dateLabel({ ...base, date: "2020-12-11", date_basis: "signed" })).toBe("signed 2020-12-11");
    expect(dateLabel({ ...base, date: "2026-01-01", date_basis: "updated" })).toBe(
      "notice updated 2026-01-01",
    );
    expect(dateLabel({ ...base })).toBe("");
  });

  it("shows a value with who shares it, never a total", () => {
    expect(valueLabel({ ...base, value: 9630343.49, currency: "GBP", suppliers: 1 })).toBe("£9,630,343");
    expect(valueLabel({ ...base, value: 1.95e9, currency: "GBP", suppliers: 20 })).toBe(
      "£1,950,000,000, shared by 20 suppliers",
    );
    expect(valueLabel({ ...base, value: 1000, currency: "EUR" })).toBe("1,000 EUR");
    expect(valueLabel({ ...base, value: null })).toBe("");
  });

  it("says a status only when it is not a live contract", () => {
    expect(statusLabel({ ...base, award_status: "active", contract_status: "active" })).toBe("");
    expect(statusLabel({ ...base, award_status: "active", contract_status: "cancelled" })).toBe(
      "contract cancelled",
    );
    expect(statusLabel({ ...base, award_status: "pending" })).toBe("awarded, contract not yet signed");
    expect(lotsLabel({ ...base, lots: 5 })).toBe("5 lots");
    expect(lotsLabel({ ...base, lots: 1 })).toBe("");
  });

  it("states the coverage limits", () => {
    const note = coverageNote(2);
    expect(note).toContain("Procurement Act");
    expect(note).toContain("not added up");
    expect(note).toContain("PPON");
    expect(coverageNote(0)).not.toContain("PPON");
  });
});
