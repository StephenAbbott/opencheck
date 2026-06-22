import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the API module so the timeline's data source is controlled. The
// component fetches via getHistory(lei, true); we assert the pure data-shaping
// helpers the component uses, fed by the mocked response.
vi.mock("../../lib/api", () => ({
  getHistory: vi.fn(),
}));

import { getHistory } from "../../lib/api";
import type { HistoryResponse } from "../../lib/api";
import {
  buildTimelineRows,
  noiseEventsOf,
  sourceUrl,
  basisLabel,
} from "./HistoryTimeline";

const _LEI = "213800IN6LSRGTZSOS29";

const RESP: HistoryResponse = {
  lei: _LEI,
  company_number: "00358949",
  available: true,
  sources: ["gleif", "companies_house"],
  notable_count: 3,
  notable: [
    {
      change_type: "LEGAL_FORM_CHANGE", label: "Legal form changed", tier: 2,
      record_type: "entity", date: "2022-01-11", date_basis: "effective",
      date_confidence: "high", value_old: "B6ES", value_new: "H0PO",
      sources: ["companies_house", "gleif"], corroborating_sources: ["gleif"],
      counterparty: null, interest_start_date: null, interest_end_date: null,
      boosted: false,
    },
    {
      change_type: "LEGAL_NAME_CHANGE", label: "Legal name changed", tier: 2,
      record_type: "entity", date: "2021-12-01", date_basis: "effective",
      date_confidence: "high", value_old: "WM MORRISON SUPERMARKETS P L C",
      value_new: "WM MORRISON SUPERMARKETS LIMITED",
      sources: ["companies_house", "gleif"], corroborating_sources: ["gleif"],
      counterparty: null, interest_start_date: null, interest_end_date: null,
      boosted: false,
    },
    {
      change_type: "OWNER_ADDED", label: "Owner / parent added", tier: 1,
      record_type: "relationship", date: "2023-11-25", date_basis: "recorded",
      date_confidence: "medium", value_old: null,
      value_new: "IS_DIRECTLY_CONSOLIDATED_BY", sources: ["gleif"],
      corroborating_sources: [], counterparty: "549300RKU7UEPSC42U63",
      interest_start_date: "2021-11-01", interest_end_date: null, boosted: false,
    },
  ],
  events: [
    {
      source_id: "gleif", record_type: "entity", raw_change_type: "UPDATE",
      raw_field: "/lei:.../lei:Registration/lei:NextRenewalDate",
      value_old: "2026-01-11", value_new: "2027-01-11", change_type: null,
      tier: 3, event_date: "2025-11-20", date_basis: "recorded",
    },
    {
      source_id: "companies_house", record_type: "entity", raw_change_type: "CS01",
      raw_field: "confirmation-statement", value_old: null, value_new: null,
      change_type: null, tier: 3, event_date: "2022-03-01", date_basis: "effective",
    },
    // A notable (tier-2) raw event — must NOT be treated as noise.
    {
      source_id: "gleif", record_type: "entity", raw_change_type: "UPDATE",
      raw_field: "/lei:.../lei:Entity/lei:LegalName", value_old: "x", value_new: "y",
      change_type: "LEGAL_NAME_CHANGE", tier: 2, event_date: "2021-12-09",
      date_basis: "recorded",
    },
  ],
};

beforeEach(() => {
  vi.mocked(getHistory).mockReset();
});

describe("getHistory mock wiring", () => {
  it("returns the mocked response", async () => {
    vi.mocked(getHistory).mockResolvedValue(RESP);
    const d = await getHistory(_LEI, true);
    expect(d).toBe(RESP);
    expect(getHistory).toHaveBeenCalledWith(_LEI, true);
  });
});

describe("noiseEventsOf", () => {
  it("keeps only Tier-3 events", () => {
    const noise = noiseEventsOf(RESP);
    expect(noise).toHaveLength(2);
    expect(noise.every((e) => e.tier === 3)).toBe(true);
    // The tier-2 LegalName raw event is excluded.
    expect(noise.some((e) => e.change_type === "LEGAL_NAME_CHANGE")).toBe(false);
  });
});

describe("buildTimelineRows", () => {
  it("shows only notable rows by default, oldest first", () => {
    const rows = buildTimelineRows(RESP, false);
    expect(rows).toHaveLength(3);
    expect(rows.every((r) => r.kind === "notable")).toBe(true);
    expect(rows.map((r) => r.date)).toEqual([
      "2021-12-01", "2022-01-11", "2023-11-25",
    ]);
  });

  it("interleaves noise rows by date when toggled on", () => {
    const rows = buildTimelineRows(RESP, true);
    expect(rows).toHaveLength(5); // 3 notable + 2 noise
    expect(rows.map((r) => r.date)).toEqual([
      "2021-12-01", "2022-01-11", "2022-03-01", "2023-11-25", "2025-11-20",
    ]);
    // The 2022-03-01 and 2025-11-20 rows are the noise ones.
    const noiseRows = rows.filter((r) => r.kind === "noise");
    expect(noiseRows.map((r) => r.date)).toEqual(["2022-03-01", "2025-11-20"]);
  });
});

describe("sourceUrl", () => {
  it("links GLEIF to the LEI record", () => {
    expect(sourceUrl("gleif", _LEI, null)).toBe(
      `https://search.gleif.org/#/record/${_LEI}`,
    );
  });

  it("links Companies House to the company's filing history", () => {
    expect(sourceUrl("companies_house", _LEI, "00358949")).toBe(
      "https://find-and-update.company-information.service.gov.uk/company/00358949/filing-history",
    );
  });

  it("returns null for Companies House without a company number", () => {
    expect(sourceUrl("companies_house", _LEI, null)).toBeNull();
  });
});

describe("basisLabel", () => {
  it("labels effective vs recorded honestly", () => {
    expect(basisLabel("effective")).toBe("as filed");
    expect(basisLabel("recorded")).toBe("as recorded by GLEIF");
  });
});
